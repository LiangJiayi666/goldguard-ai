#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GoldGuard AI 主链路与边界冒烟测试（仅标准库）。

覆盖笔试要求的四类场景：
  1) 主链路：驾驶舱 → 企业列表 → 企业证据链 → 人工复核 → 启动/停止排查；
  2) 数据缺失：示例企业G 无备案/官网数据，必须输出“待核实”，不得判“正常”；
  3) 接口/模型失败降级：示例企业H LLM 不可用，降级为本地规则并标注；
  4) 合规边界：证据必须可回溯来源与时点；数据中不得出现收益承诺/买卖指令表述。

运行：
    python tests/smoke_test.py
返回码 0 表示全部通过。
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

WEB_DIR = Path(__file__).resolve().parents[1]
PORT = 8801
BASE = f"http://127.0.0.1:{PORT}"

results: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    results.append((bool(cond), name if cond else f"{name} —— {detail}"))


def get(path: str) -> dict:
    with urllib.request.urlopen(BASE + path, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post(path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(BASE + path, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def company_path(name: str, section: str) -> str:
    q = urllib.parse.urlencode({"company": name, "section": section})
    return f"/api/company?{q}"


def main() -> int:
    proc = subprocess.Popen([sys.executable, "run.py", "--port", str(PORT)], cwd=str(WEB_DIR),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        # 等待服务就绪
        for _ in range(40):
            try:
                get("/api/meta")
                break
            except Exception:
                time.sleep(0.25)
        else:
            print("服务未能在 10s 内启动")
            return 2

        meta = get("/api/meta")
        check(meta.get("ok") and "notice" in meta.get("data", {}), "服务启动并返回元数据")

        dash = get("/api/runs/latest")
        rows = dash["data"]["companies"]["rows"]
        check(dash.get("ok") and len(rows) == 8, "驾驶舱返回 8 家企业", f"got {len(rows)}")

        by_name = {r["企业"]: r for r in rows}
        a = next(r for r in rows if "黄金珠宝制造" in r["企业"])
        g = next(r for r in rows if "黄金投资咨询" in r["企业"])
        h = next(r for r in rows if "黄金加工" in r["企业"])

        # 1) 主链路：证据链可回溯
        ev = get(company_path(a["企业"], "evidence"))["data"]
        check(len(ev["evidence"]) >= 3, "企业A 至少 3 条事实证据")
        src_ok = all(e.get("source_id") in ev["sources"] for e in ev["evidence"])
        check(src_ok, "每条证据都能回到已登记来源")
        check(all(e.get("observed_at") for e in ev["evidence"]), "每条证据都有观察时点")
        check(all(e.get("status") in {"verified", "partial", "unavailable"} for e in ev["evidence"]),
              "证据状态取值合法（verified/partial/unavailable）")
        check(isinstance(ev.get("inferences"), list) and isinstance(ev.get("uncertainties"), list)
              and isinstance(ev.get("change_conditions"), list),
              "事实/推断/不确定项/改变结论条件分栏存在")

        # 2) 数据缺失：不得静默判“正常”
        g_ev = get(company_path(g["企业"], "evidence"))["data"]
        check(g["有风险"] is None, "示例企业G 风险结论为“待核实”（None），未误判为无风险",
              f"got {g['有风险']}")
        check(any(e["status"] == "unavailable" for e in g_ev["evidence"]), "示例企业G 存在 unavailable 证据节点")

        # 3) 模型失败降级
        h_ev = get(company_path(h["企业"], "evidence"))["data"]
        check(h_ev["analysis_mode"] == "conservative_local_fallback", "示例企业H 标记为降级模式")
        check(any("降级" in str(u) or "LLM" in str(u) for u in h_ev["uncertainties"]),
              "示例企业H 在不确定项中披露降级事实")

        # 4) 合规边界
        blob = json.dumps(get("/api/runs/latest"), ensure_ascii=False) + json.dumps(ev, ensure_ascii=False)
        forbidden = ["保证收益", "必涨", "稳赚", "推荐买入", "推荐卖出", "确定上涨"]
        hit = [w for w in forbidden if w in blob]
        check(not hit, "数据中不含收益承诺/买卖指令表述", f"命中 {hit}")
        check("不构成投资建议" in meta["data"]["notice"] or "不构成投资建议" in json.dumps(meta, ensure_ascii=False),
              "产品显著声明不构成投资建议")

        # 5) 人工复核闭环
        rv = post("/api/review", {"company": a["企业"], "decision": "标记误报", "note": "冒烟测试"})
        check(rv.get("ok") and rv["data"]["decision"] == "标记误报", "人工复核可提交并生效")
        ev2 = get(company_path(a["企业"], "evidence"))["data"]
        check(ev2["human_review"]["status"] == "已复核", "复核结果写回证据卡片")

        # 6) 启动/停止排查
        st = post("/api/agent/start")
        check(st.get("ok"), "可启动排查任务")
        time.sleep(0.5)
        check(get("/api/agent/status")["running"] is True, "启动后状态为运行中")
        check(post("/api/agent/stop").get("ok"), "可停止排查任务")
        check(get("/api/agent/status")["running"] is False, "停止后状态为已停止")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    passed = sum(1 for ok, _ in results if ok)
    print(f"\n冒烟测试：{passed}/{len(results)} 通过\n" + "-" * 60)
    for ok, name in results:
        print(("  [PASS] " if ok else "  [FAIL] ") + name)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
