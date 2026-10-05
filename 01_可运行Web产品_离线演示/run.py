#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GoldGuard AI · 离线演示服务（仅 Python 标准库，无需安装任何依赖）。

用途
----
把「黄金企业网络信息与合规风险排查」的看板做成一个可访问、可操作的 Web 产品：
评委无需 API Key、无需数据库、无需联网，双击 / 命令行即可在本机打开。

设计约束（与笔试「统一规则」一致）
--------------------------------
- 全部数据来自 data/fixtures.json，均为构造/脱敏数据，不含真实企业、密钥、登录态；
- 数据缺失 / 接口失败不得静默判定“正常”：缺失时输出“待核实”，模型不可用时降级为规则扫描并标注；
- 所有结论区分「事实证据 / 推断 / 不确定项 / 改变结论的条件」，并可回到来源与时点；
- 人工复核（采纳 / 修改 / 转人工）真正改变后续展示状态，形成人机协作闭环。

启动
----
    python run.py            # 默认 http://127.0.0.1:8765
    python run.py --port 9000 --host 0.0.0.0
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
UI_DIR = ROOT / "ui"
FIXTURE_PATH = ROOT / "data" / "fixtures.json"

# 阶段顺序与真实流水线一致（见 engine/AGENTS.md 的 PHASES 契约）
PHASES = [
    "WEB_ICP", "WEB_CRAWL", "IMAGE_DOWNLOAD", "OCR", "KEYWORD",
    "ACCOUNT_DISCOVERY", "ACCOUNT_RANK", "ACCOUNT_REVIEW", "ACCOUNT_VERIFY",
    "POST_CRAWL", "RISK_ANALYSIS", "RISK_EVIDENCE",
]
PHASE_ZH = {
    "WEB_ICP": "备案载体查询", "WEB_CRAWL": "官网抓取", "IMAGE_DOWNLOAD": "图片下载",
    "OCR": "图片文字识别", "KEYWORD": "关键词筛查", "ACCOUNT_DISCOVERY": "社媒账号发现",
    "ACCOUNT_RANK": "账号筛选排序", "ACCOUNT_REVIEW": "账号人工复核", "ACCOUNT_VERIFY": "账号身份核验",
    "POST_CRAWL": "社媒内容抓取", "RISK_ANALYSIS": "风险分析研判", "RISK_EVIDENCE": "风险证据整理",
}

with FIXTURE_PATH.open(encoding="utf-8") as fh:
    FIXTURE: dict[str, Any] = json.load(fh)
COMPANIES: list[dict[str, Any]] = FIXTURE["companies"]
SOURCES: dict[str, dict[str, Any]] = {s["id"]: s for s in FIXTURE["sources"]}
META: dict[str, Any] = FIXTURE["meta"]

# 运行时状态（内存态，重启即复位）
STATE: dict[str, Any] = {
    "running": False,
    "pid": None,
    "start_time": None,
    "phase_idx": 0,
    "thread": None,
    "log": [],
    "log_lock": threading.Lock(),
    "reviews": {},  # company -> human_review dict（人工复核结果覆盖 fixture）
}


def log(msg: str) -> None:
    stamp = time.strftime("%H:%M:%S")
    with STATE["log_lock"]:
        STATE["log"].append(f"[{stamp}] {msg}")


# --------------------------------------------------------------------------
# 数据组装：把 fixture 转换成看板前端需要的结构
# --------------------------------------------------------------------------
def _active_phase(company: dict[str, Any]) -> str:
    """运行中为 OPEN 企业返回当前推进到的阶段；否则返回自身状态。"""
    if STATE["running"]:
        idx = min(STATE["phase_idx"], len(PHASES) - 1)
        return PHASES[idx]
    return company.get("current_phase") or "RISK_EVIDENCE"


def _risk(company: dict[str, Any]) -> dict[str, Any]:
    risk = dict(company.get("risk") or {})
    if company["name"] in STATE["reviews"]:
        risk["human_review"] = STATE["reviews"][company["name"]]
    return risk


def _row(company: dict[str, Any]) -> dict[str, Any]:
    icp = company.get("icp") or {}
    risk = _risk(company)
    keywords: list[str] = []
    for finding in risk.get("risk_keyword_findings") or []:
        for kw in finding.get("keywords") or []:
            if kw not in keywords:
                keywords.append(kw)
    active = [d["domain"] for d in company.get("domain_activity") or [] if d.get("is_active") is True]
    return {
        "企业": company["name"],
        "状态": company["status"],
        "ICP网站数": len(icp.get("web") or []),
        "ICP小程序数": len(icp.get("mapp") or []),
        "ICP应用数": len(icp.get("app") or []),
        "ICP快应用数": len(icp.get("kapp") or []),
        "搜索候选官网数": len((company.get("website_search") or {}).get("candidates") or []),
        "互联网载体候选数": len((company.get("website_search") or {}).get("candidates") or []),
        "官网域名数": len((company.get("website_crawl") or {}).get("seen_urls") or []),
        "活跃域名": ",".join(sorted(active)) or "-",
        "社媒账号数": (company.get("social") or {}).get("count", 0),
        "有风险": risk.get("overall_risk_found"),
        "回购线索": (risk.get("gold_buyback") or {}).get("present"),
        "风险词": ",".join(keywords) or "-",
        "分析模式": risk.get("analysis_mode"),
        "生成时间": META["generated_at"],
    }


def _progress_company(company: dict[str, Any]) -> dict[str, Any]:
    status = "OPEN" if (STATE["running"] and company["status"] == "OPEN") else company["status"]
    phase = _active_phase(company) if status == "OPEN" else "已完成"
    total = 12 * 3  # 演示口径：每企业 3 类载体 × 12 阶段
    ok = total if status == "CLOSED" else max(1, int(total * (STATE["phase_idx"] + 1) / len(PHASES)))
    return {
        "企业": company["name"],
        "状态": status,
        "当前阶段": phase if status == "OPEN" else "-",
        "任务统计": {"total": total, "ok": ok, "failed": 0, "running": total - ok},
    }


def _dashboard() -> dict[str, Any]:
    rows = [_row(c) for c in COMPANIES]
    closed = sum(1 for c in COMPANIES if c["status"] == "CLOSED")
    open_ = len(COMPANIES) - closed
    review_pending = sum(
        1 for c in COMPANIES
        if (_risk(c).get("human_review") or {}).get("status") != "已复核"
    )
    files = {}
    for c in COMPANIES:
        files[c["name"]] = [
            {"kind": "截图", "path": f"demo/{c['name']}/evidence_01.png"},
            {"kind": "截图", "path": f"demo/{c['name']}/evidence_02.png"},
            {"kind": "过程文本", "path": f"demo/{c['name']}/phase_summary.md"},
        ]
    events = [
        {"time": META["generated_at"], "level": "info", "text": f"加载离线快照，共 {len(COMPANIES)} 家企业"},
        {"time": META["generated_at"], "level": "warning", "text": "示例企业G：备案/官网数据缺失，结论已降级为“待核实”"},
        {"time": META["generated_at"], "level": "warning", "text": "示例企业H：LLM 不可用，自动降级为本地关键词规则"},
        {"time": META["generated_at"], "level": "info", "text": f"待人工复核 {review_pending} 家"},
    ]
    return {
        "run_id": META["run_id"],
        "status": {
            "companies_by_status": {"CLOSED": closed, "OPEN": open_},
            "tasks_by_phase_status": {p: {"OK": len(COMPANIES)} for p in PHASES},
            "manifest": {"execution_summary": {
                "PROCESS": len(COMPANIES) * len(PHASES),
                "HISTORY_REUSE": max(0, len(COMPANIES) * len(PHASES) - (STATE["phase_idx"] + 1) * len(COMPANIES)),
            }},
        },
        "audit": {"by_status": {"FAILED": 0, "RETRY_WAIT": 0, "RUNNING": 0, "PENDING": max(0, review_pending)}},
        "companies": {"rows": rows},
        "process_files": {"totals": {"截图": len(COMPANIES) * 2, "过程文本": len(COMPANIES)}, "by_company": files},
        "events": events,
    }


def _company_section(name: str, section: str) -> dict[str, Any] | None:
    company = next((c for c in COMPANIES if c["name"] == name), None)
    if company is None:
        return None
    risk = _risk(company)
    if section == "icp":
        icp = company.get("icp") or {}
        return {
            "icp_counts": {k: len(icp.get(k) or []) for k in ("web", "mapp", "app", "kapp")},
            "icp_carriers": icp,
            "analysis_mode": risk.get("analysis_mode"),
        }
    if section == "website":
        return {
            "website_search": company.get("website_search") or {},
            "website_crawl": company.get("website_crawl") or {},
            "analysis_mode": risk.get("analysis_mode"),
        }
    if section == "social":
        social = company.get("social") or {}
        return {
            "social_account_verifications": social,
            "analysis_mode": risk.get("analysis_mode"),
        }
    if section == "risk":
        return {"risk": risk, "analysis_mode": risk.get("analysis_mode")}
    if section == "evidence":
        return {
            "executive_summary": risk.get("executive_summary"),
            "evidence": risk.get("evidence") or [],
            "inferences": risk.get("inferences") or [],
            "uncertainties": risk.get("uncertainties") or [],
            "change_conditions": risk.get("change_conditions") or [],
            "human_review": risk.get("human_review") or {},
            "analysis_mode": risk.get("analysis_mode"),
            "sources": SOURCES,
        }
    return None


# --------------------------------------------------------------------------
# 运行模拟：点击“开始排查”后按真实阶段顺序推进，产生日志与进度
# --------------------------------------------------------------------------
def _simulate() -> None:
    log("main_agent 启动（离线演示模式），加载 8 家脱敏企业")
    time.sleep(0.6)
    for idx, phase in enumerate(PHASES):
        STATE["phase_idx"] = idx
        log(f"[{idx + 1}/{len(PHASES)}] {phase}（{PHASE_ZH[phase]}）执行中…")
        time.sleep(1.1)
        log(f"[{idx + 1}/{len(PHASES)}] {phase} 完成")
    log("全部企业排查完成；示例企业G 因数据缺失标记“待核实”，示例企业H 触发降级")
    log("任务进入人工复核队列")
    STATE["running"] = False
    STATE["thread"] = None


def _start(mode: str) -> dict[str, Any]:
    if STATE["running"]:
        return {"ok": False, "error": "已有排查任务在运行中"}
    STATE["running"] = True
    STATE["phase_idx"] = 0
    STATE["pid"] = 40000 + int(time.time()) % 1000
    STATE["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
    log(f"启动参数 mode={mode}")
    thread = threading.Thread(target=_simulate, daemon=True)
    STATE["thread"] = thread
    thread.start()
    return {"ok": True, "pid": STATE["pid"], "log_file": "logs/demo_agent.log"}


# --------------------------------------------------------------------------
# HTTP 服务
# --------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "GoldGuardDemo/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # 静默
        pass

    # -- 工具 --
    def _json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _ok(self, data: Any = None, **extra: Any) -> None:
        payload = {"ok": True, **extra}
        if data is not None:
            payload["data"] = data
        self._json(payload)

    def _fail(self, error: str, status: int = 200) -> None:
        self._json({"ok": False, "error": error}, status=status)

    def _text(self, text: str, ctype: str = "text/plain; charset=utf-8", status: int = 200) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, path: Path) -> None:
        if not path.is_file():
            self._text("404 Not Found", status=404)
            return
        ctype = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        if path.suffix == ".js":
            ctype = "application/javascript; charset=utf-8"
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _query(self) -> dict[str, str]:
        parsed = urllib.parse.urlparse(self.path)
        return {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}

    # -- GET --
    def do_GET(self) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        q = self._query()
        try:
            if path in ("/", "/index.html"):
                self._serve_file(UI_DIR / "index.html")
            elif path == "/about":
                self._serve_file(UI_DIR / "about.html")
            elif path.startswith("/static/"):
                rel = path[len("/static/"):]
                target = (UI_DIR / rel).resolve()
                if UI_DIR.resolve() not in target.parents and target != UI_DIR.resolve():
                    self._text("403", status=403)
                else:
                    self._serve_file(target)
            elif path == "/api/runs/latest":
                self._ok(_dashboard())
            elif path == "/api/runs/latest/progress":
                companies = [_progress_company(c) for c in COMPANIES]
                self._json({"ok": True, "companies": companies, "summary": {"total": len(companies)}})
            elif path == "/api/agent/status":
                self._json({
                    "ok": True,
                    "running": STATE["running"],
                    "pid": STATE["pid"],
                    "start_time": STATE["start_time"],
                    "error_message": None,
                    "error_level": None,
                })
            elif path == "/api/company":
                section = q.get("section", "risk")
                data = _company_section(q.get("company", ""), section)
                if data is None:
                    self._fail("未找到该企业或该栏目")
                else:
                    self._ok(data)
            elif path == "/api/phase":
                # 离线演示：直接回落到企业自身的证据卡片
                if _company_section(q.get("company", ""), "evidence") is None:
                    self._fail("未找到阶段产物")
                else:
                    self._ok({"tasks": [
                        {"task_dir": f"{q.get('phase', 'PHASE')}/task_01", "status": "OK",
                         "summary": {"阶段": q.get("phase", "PHASE"), "说明": "离线演示产物"}}
                    ]})
            elif path == "/api/file":
                label = os.path.basename(q.get("path", "evidence.png"))
                svg = (
                    f'<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360">'
                    f'<rect width="100%" height="100%" fill="#f4f7f4"/>'
                    f'<text x="50%" y="46%" text-anchor="middle" font-size="20" fill="#147a5a">证据截图（构造占位）</text>'
                    f'<text x="50%" y="56%" text-anchor="middle" font-size="14" fill="#69756f">{label}</text></svg>'
                )
                self._text(svg, ctype="image/svg+xml; charset=utf-8")
            elif path == "/api/agent/log":
                self._stream_log()
            elif path == "/api/meta":
                self._ok(META)
            else:
                self._text("404 Not Found", status=404)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _stream_log(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        sent = 0
        idle = 0
        try:
            while True:
                with STATE["log_lock"]:
                    lines = STATE["log"][sent:]
                if lines:
                    sent += len(lines)
                    for line in lines:
                        self.wfile.write(f"data: {json.dumps({'text': line}, ensure_ascii=False)}\n\n".encode("utf-8"))
                    idle = 0
                else:
                    idle += 1
                    if idle % 10 == 0:
                        self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
                time.sleep(0.4)
        except (BrokenPipeError, ConnectionResetError):
            return

    # -- POST --
    def do_POST(self) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = {}
        try:
            if path == "/api/agent/start":
                self._json(_start("full"))
            elif path == "/api/agent/replay":
                self._json(_start("replay:" + ",".join(body.get("companies") or [])))
            elif path == "/api/agent/stop":
                STATE["running"] = False
                log("收到停止指令，任务已中止")
                self._json({"ok": True})
            elif path == "/api/review":
                name = (body.get("company") or "").strip()
                company = next((c for c in COMPANIES if c["name"] == name), None)
                if company is None:
                    self._fail("未找到该企业")
                else:
                    review = {
                        "status": "已复核",
                        "reviewer": body.get("reviewer") or "复核人（演示）",
                        "decision": body.get("decision") or "采纳",
                        "note": body.get("note") or "",
                        "reviewed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    STATE["reviews"][name] = review
                    log(f"人工复核[{name}] 决策={review['decision']}")
                    self._ok(review)
            else:
                self._text("404 Not Found", status=404)
        except (BrokenPipeError, ConnectionResetError):
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="GoldGuard AI 离线演示服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{'127.0.0.1' if args.host in ('0.0.0.0', '') else args.host}:{args.port}"
    print("=" * 68)
    print("  GoldGuard AI · 黄金企业风险雷达（离线演示）")
    print(f"  已启动：{url}")
    print("  数据：构造/脱敏，无需联网、无需 API Key。Ctrl+C 停止。")
    print("=" * 68)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
        server.shutdown()


if __name__ == "__main__":
    main()
