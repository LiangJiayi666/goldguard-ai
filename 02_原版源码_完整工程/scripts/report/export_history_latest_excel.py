#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""导出所有企业「历史最新批次」排查结果到 Excel（每块标注数据实际来源批次）。

按企业取最新批次（run_id 最大）的 exports JSON。每个数据块（官网/app/小程序/
各平台账号/业务判定）单独标注「数据来源批次/运行时间」：HISTORY_REUSE 任务写
source_run_id，全新任务写本批次 id，多来源按运行时间旧→新以 ; 分隔。

用法（从项目根目录运行）：
    python .workbuddy/skills/pipeline-query/report/export_history_latest_excel.py
    python .workbuddy/skills/pipeline-query/report/export_history_latest_excel.py --out <路径.xlsx>

导出约定：默认输出到 outputs/<YYYYmmdd_HHMMSS>/ 时间戳目录下，避免覆盖旧文件；
--out 显式指定路径时跳过该约定。
"""
import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import common
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = Path(common.ROOT)
RUNS_DIR = ROOT / "pipeline_output" / "runs"
DB_PATH = ROOT / "pipeline_output" / "data" / "main_agent.db"

PRESENT_MAP = {"yes": "是", "no": "否", "unclear": "存疑", "warning": "预警"}
ACTIVITY_MAP = {"active": "活跃", "inactive": "不活跃", "unknown": "状态未知"}

PLATFORMS = [
    ("xhs", ("xhs", "xiaohongshu")),
    ("dy", ("douyin",)),
    ("ks", ("kuaishou",)),
]

# (块key, ICP字段, 是否按域名去重, 搜索/LLM的 carrier_type 集合)
CARRIER_BLOCKS = [
    ("website", "web", True, {"web", "website", "domain"}),
    ("app", "app", False, {"app"}),
    ("mapp", "mapp", False, {"mini_program", "miniprogram", "mini", "小程序"}),
]

CARRIER_LABELS = {"website": ("是否有官网或疑似官网", "官网list", "官网活跃度list"),
                  "app": ("是否有app", "app list", "app活跃度list"),
                  "mapp": ("是否有小程序", "小程序list", "小程序活跃度list")}

WEB_PHASES = {"WEB_ICP", "WEB_SEARCH", "WEB_CRAWL"}
SOCIAL_PHASES = {"ACCOUNT_DISCOVERY", "ACCOUNT_RANK", "ACCOUNT_REVIEW", "ACCOUNT_VERIFY", "POST_CRAWL"}
RISK_PHASE = {"RISK_ANALYSIS"}

VERIFICATION_ID_FIELDS = (
    "account_id", "account_name", "username", "nickname",
    "user_id", "sec_uid", "douyin_id", "kwai_id", "red_id",
)


def columns():
    cols = ["企业名"]
    for key, _, _, _ in CARRIER_BLOCKS:
        has, lst, act = CARRIER_LABELS[key]
        cols += [has, lst, act, "数据来源批次", "数据来源运行时间"]
    for key, _ in PLATFORMS:
        cols += [f"是否有{key}账号", f"{key}账号list", f"{key}账号活跃度list",
                 "数据来源批次", "数据来源运行时间"]
    cols += ["是否有业务", "业务介绍", "数据来源批次", "数据来源运行时间"]
    cols += ["是否黄金业务", "黄金业务判定依据", "数据来源批次", "数据来源运行时间"]
    cols += ["是否命中风险词", "命中风险词list", "数据来源批次", "数据来源运行时间"]
    cols += ["是否命中风险现象", "风险现象list", "数据来源批次", "数据来源运行时间"]
    return cols


def to_local(iso):
    try:
        return datetime.fromisoformat(str(iso)).astimezone().strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(iso or "")


def run_time_fallback(run_id):
    try:
        return datetime.strptime(str(run_id)[:15], "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def load_run_times():
    times = {}
    if DB_PATH.is_file():
        con = sqlite3.connect(DB_PATH)
        try:
            for run_id, started in con.execute("SELECT run_id, started_at FROM runs"):
                times[run_id] = to_local(started)
        finally:
            con.close()
    return times


def json_file(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def join_list(values):
    return "\n".join(str(v) for v in values if v)


def clean_profile_url(url):
    """源数据偶发同域名重复拼接（douyin.com//douyin.com/user/…），仅同域时折叠。"""
    url = str(url or "")
    m = re.match(r"(https?://[^/]+)/+([^/]+)/", url)
    if m and root_key(m.group(1)) == root_key(m.group(2)):
        url = f"{m.group(1)}/{url[m.end():]}"
    return url.split("?")[0]


def root_key(host):
    h = re.sub(r"^https?://", "", str(host or "").strip().lower())
    h = h.split("/")[0].split(":")[0]
    return h[4:] if h.startswith("www.") else h


def block_sources(data, run_id, phases, platform_aliases=None):
    """块内各 completed 任务的来源 run 集合：复用写 source_run_id，否则本批次。"""
    sources = set()
    for item in data.get("completed") or []:
        if item.get("phase") not in phases:
            continue
        plat = str(item.get("platform") or "").lower()
        if platform_aliases is not None and plat and plat not in platform_aliases:
            continue
        dg = (item.get("summary") or {}).get("diagnosis") or {}
        src = (dg.get("evidence") or {}).get("source_run_id") if dg.get("source") == "history_reuse" else None
        sources.add(str(src) if src else run_id)
    return sources or {run_id}


def source_pair(sources, run_times):
    ordered = sorted(sources, key=lambda r: (run_times.get(r) or run_time_fallback(r) or r))
    return (";".join(ordered),
            ";".join(run_times.get(r) or run_time_fallback(r) for r in ordered))


def carrier_merge(data, icp_field, carrier_types, is_domain):
    """ICP 备案 + 搜索候选 + LLM network_carrier_assessment 去重合并，状态取最强。"""
    merged = {}
    rank = {"活跃": 3, "不活跃": 2, "状态未知": 1, None: 0}

    def add(key, display, label, order, active=None):
        entry = merged.setdefault(key, {"display": display, "sources": [], "active": None})
        if label not in [lab for _, lab in entry["sources"]]:
            entry["sources"].append((order, label))
        if rank.get(active, 0) > rank.get(entry["active"], 0):
            entry["active"] = active

    for rec in (data.get("icp_carriers") or {}).get(icp_field) or []:
        key = root_key(rec.get("name")) if is_domain else str(rec.get("name") or rec.get("service_name") or "")
        if key:
            add(key, str(rec.get("service_name") or rec.get("name") or key), "备案", 3)

    ws = data.get("website_search") or {}
    for cand in (ws.get("candidates") or []) + (ws.get("carrier_candidates") or []):
        if str(cand.get("carrier_type") or "").lower() not in carrier_types:
            continue
        key = root_key(cand.get("domain")) if is_domain else str(cand.get("candidate_name") or cand.get("domain") or "")
        if key:
            add(key, str(cand.get("domain") or key), "疑似(搜索)", 2)

    carriers = ((data.get("enterprise_assessment") or {}).get("network_carrier_assessment") or {}).get("carriers") or []
    for c in carriers:
        if str(c.get("carrier_type") or "").lower() not in carrier_types:
            continue
        name = str(c.get("name") or "").strip()
        if not name:
            continue
        if is_domain:
            parts = [root_key(p) for p in name.split("/") if root_key(p)]
            key = parts[0] if parts else name
            display = " / ".join(sorted(set(parts))) or name
        else:
            key = display = name
        confirmed = c.get("official_status") == "confirmed"
        add(key, display, "LLM确认" if confirmed else "LLM未核实",
            4 if confirmed else 1,
            ACTIVITY_MAP.get(str(c.get("activity_status") or "").lower()))
    return merged


def render_carrier(merged):
    items, acts = [], []
    for entry in merged.values():
        labels = "+".join(lab for _, lab in sorted(entry["sources"], reverse=True))
        items.append(f"{entry['display']}({labels})")
        if entry["active"]:
            acts.append(f"{entry['display']}:{entry['active']}")
    return items, acts


def accepted_verifications(data, aliases):
    return [v for v in data.get("social_account_verifications") or []
            if isinstance(v, dict)
            and str(v.get("platform") or "").lower() in aliases
            and v.get("decision") != "rejected"
            and v.get("official_status") != "rejected"]


def verification_ids(item):
    return {str(item[k]) for k in VERIFICATION_ID_FIELDS if item.get(k)}


def account_activity(data, verification):
    want = verification_ids(verification)
    best = {}
    for item in data.get("social_account_activity") or []:
        if not isinstance(item, dict) or verification_ids(item) & want:
            plat = str(item.get("platform") or "").lower()
            key = (plat, str(item.get("account_name") or ""))
            prev = best.get(key)
            if not prev or int(item.get("round") or 0) >= int(prev.get("round") or 0):
                best[key] = item
    latest = max(best.values(), key=lambda i: int(i.get("round") or 0), default=None)
    if not latest:
        return ""
    post = latest.get("latest_post_time")
    return f"{'活跃账号' if latest.get('is_active') else '不活跃'}(最近发帖 {post})" if post else \
        f"{'活跃账号' if latest.get('is_active') else '不活跃'}"


def build_row(data, run_dir, run_id, fallback_name, run_times):
    """按 columns() 的顺序逐块追加值。来源列在多个块重名，必须位置序，不能用 dict。"""
    row = [normalize_name(data.get("company") or fallback_name)]

    def block(values, sources):
        row.extend(values)
        row.extend(source_pair(sources, run_times))

    for key, icp_field, is_domain, types in CARRIER_BLOCKS:
        merged = carrier_merge(data, icp_field, types, is_domain)
        items, acts = render_carrier(merged)
        has, lst, act = CARRIER_LABELS[key]
        block([("是" if items else "否"), join_list(items), join_list(acts)],
              block_sources(data, run_id, WEB_PHASES))

    ea = data.get("enterprise_assessment") or {}
    for key, aliases in PLATFORMS:
        verifications = accepted_verifications(data, aliases)
        entries, acts = [], []
        for v in verifications:
            name = v.get("account_name") or v.get("nickname") or v.get("account_id") or ""
            url = clean_profile_url(v.get("profile_url"))
            entries.append(f"{name} | {url}" if url and url != name else name)
            label = account_activity(data, v)
            if label:
                acts.append(f"{name}:{label}")
        block([("是" if verifications else "否"), join_list(entries), join_list(acts)],
              block_sources(data, run_id, SOCIAL_PHASES, set(aliases)))

    bi = ea.get("business_involvement") or {}
    bm = ea.get("business_model") or {}
    activities = bm.get("business_activities") or []
    intro = join_list(
        [f"{b.get('activity') or ''}{('｜' + b['reason']) if b.get('reason') else ''}" for b in activities]
    ) or str(bm.get("summary") or "")
    block([PRESENT_MAP.get(str(bi.get("present") or ""), "存疑"), intro],
          block_sources(data, run_id, RISK_PHASE))

    gold = ea.get("gold_business_involvement") or {}
    block([PRESENT_MAP.get(str(gold.get("present") or ""), "存疑"), str(gold.get("reason") or "")],
          block_sources(data, run_id, RISK_PHASE))

    keywords = [k for k in (ea.get("keyword_assessments") or ea.get("risk_keyword_findings") or [])
                if k.get("present") == "yes"]
    block(["是" if keywords else "否",
           join_list([f"{k.get('keyword')}{('｜' + k['reason']) if k.get('reason') else ''}" for k in keywords])],
          block_sources(data, run_id, RISK_PHASE))

    scenarios = [s for s in (ea.get("risk_scenarios") or [])
                 if s.get("present") in ("yes", "warning")]
    block(["是" if scenarios else "否",
           join_list([f"{s.get('scenario_id')}{('｜' + s['reason']) if s.get('reason') else ''}" for s in scenarios])],
          block_sources(data, run_id, RISK_PHASE))
    return row


def normalize_name(name):
    """统一企业名格式：全角括号、下划线、多余空格归一化为半角括号。"""
    s = str(name or "").strip()
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[\s_]+", " ", s)
    return s


def company_key(data, filename):
    # 以标准化企业名为去重主键：company_id 在跨批次不稳定，导致同一企业产生重复行。
    name = data.get("company") or filename or ""
    norm = re.sub(r"[\s　()（）_\-]", "", str(name)).lower()
    return f"name:{norm}"


def load_rows(run_times, run_id=None):
    latest = {}
    # 指定批次时只扫描该批次；未指定时跨所有批次取历史最新。
    if run_id:
        run_ids = [run_id]
    else:
        run_ids = sorted((p.name for p in RUNS_DIR.iterdir() if p.is_dir()), reverse=True)
    for rid in run_ids:
        export_dir = RUNS_DIR / rid / "exports"
        if not export_dir.is_dir():
            continue
        for entry in export_dir.iterdir():
            if not entry.is_file() or entry.suffix != ".json":
                continue
            data = json_file(entry)
            if not data:
                continue
            # 跳过无实质数据的旧 INCOMPLETE 导出，避免空壳记录覆盖有效批次。
            completed = data.get("completed") or []
            if data.get("status") == "INCOMPLETE" and not completed:
                continue
            latest.setdefault(company_key(data, entry.stem), (data, rid, entry.stem))
    return [build_row(data, RUNS_DIR / rid, rid, name, run_times)
            for data, rid, name in latest.values()]


def write_workbook(rows, output: Path):
    cols = columns()
    assert all(len(r) == len(cols) for r in rows), "row/column count mismatch"
    wb = Workbook()
    ws = wb.active
    ws.title = "历史最新批次结果"
    header_fill = PatternFill("solid", fgColor="1F4E79")

    for c_idx, name in enumerate(cols, 1):
        cell = ws.cell(row=1, column=c_idx, value=name)
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for r_idx, row in enumerate(rows, 2):
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    widths = {"企业名": 28}
    for c_idx, name in enumerate(cols, 1):
        ws.column_dimensions[get_column_letter(c_idx)].width = widths.get(
            name, 16 if name.startswith("是否有") or name.startswith("数据来源") or "批次" in name or "时间" in name else 42)
    for r_idx in range(2, len(rows) + 2):
        ws.row_dimensions[r_idx].height = 42
    ws.freeze_panes = "B2"
    wb.save(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", default="", help="指定批次 run_id（缺省=所有历史最新批次）")
    parser.add_argument("--out", default="", help="输出 xlsx 路径（缺省=outputs/<时间戳>/所有企业历史最新批次结果_带来源.xlsx）")
    args = parser.parse_args()

    run_times = load_run_times()
    rows = load_rows(run_times, args.run or None)
    rows.sort(key=lambda r: str(r[0] or ""))
    if args.out:
        output = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = ROOT / "outputs" / stamp / "所有企业历史最新批次结果_带来源.xlsx"
    output.parent.mkdir(parents=True, exist_ok=True)
    write_workbook(rows, output)
    print(json.dumps({"companies": len(rows), "output": str(output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
