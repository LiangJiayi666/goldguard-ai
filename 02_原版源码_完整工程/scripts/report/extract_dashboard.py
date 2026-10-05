# -*- coding: utf-8 -*-
"""看板数据包：组合既有原子提取脚本，并索引批次过程文件。

本脚本只读。它通过子进程调用 extract_run_status、extract_batch_table、
extract_audit 和 extract_events，避免重复实现这些原子查询逻辑。

用法：
  python .workbuddy/skills/pipeline-query/report/extract_dashboard.py --run latest
  python .workbuddy/skills/pipeline-query/report/extract_dashboard.py --run latest --events-tail 50
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter

import common


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def invoke_json(script: str, *args: str) -> dict:
    """调用一个既有原子脚本并读取其 JSON 标准输出。"""
    completed = subprocess.run(
        [sys.executable, os.path.join(SCRIPT_DIR, script), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{script} 未输出有效 JSON：{exc}") from exc


def process_file_index(run_dir: str, limit_per_company: int) -> dict:
    """按企业和类型建立过程文件索引；路径相对于项目根目录，便于 /api/file 直接读取。"""
    companies_dir = os.path.join(run_dir, "companies")
    index: dict[str, list[dict]] = {}
    totals: Counter[str] = Counter()
    if not os.path.isdir(companies_dir):
        return {"totals": {}, "by_company": index}

    for company in sorted(os.listdir(companies_dir)):
        company_dir = os.path.join(companies_dir, company)
        if not os.path.isdir(company_dir):
            continue
        items: list[dict] = []
        for root, _, files in os.walk(company_dir):
            for filename in files:
                # 路径以项目根目录为基准，确保 /api/file?path=... 能正确定位
                relative = os.path.relpath(os.path.join(root, filename), common.ROOT)
                extension = os.path.splitext(filename)[1].lower() or "[无扩展名]"
                if filename in {"final.json", "result.json"}:
                    kind = filename
                elif extension in {".png", ".jpg", ".jpeg", ".webp"}:
                    kind = "截图"
                elif extension in {".txt", ".md", ".csv"}:
                    kind = "过程文本"
                else:
                    kind = "其他"
                totals[kind] += 1
                if len(items) < limit_per_company:
                    items.append({"kind": kind, "path": relative.replace("\\", "/")})
        index[company] = items
    return {"totals": dict(sorted(totals.items())), "by_company": index}


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="组合原子脚本输出，生成运行看板数据包")
    parser.add_argument("--run", default="latest", help="批次 ID 或 latest")
    parser.add_argument("--events-tail", type=int, default=30, help="保留最近 N 条审计事件")
    parser.add_argument("--files-per-company", type=int, default=100, help="每家企业最多索引的过程文件数")
    args = parser.parse_args()
    if args.events_tail < 0 or args.files_per_company < 0:
        parser.error("数量参数不能小于 0")

    run_dir, run_id = common.resolve_run(args.run)
    run = ["--run", run_id]
    status = invoke_json("extract_run_status.py", *run)
    companies = invoke_json("extract_batch_table.py", *run, "--format", "json")
    audit = invoke_json("extract_audit.py", *run)
    events = invoke_json("extract_events.py", *run, "--tail", str(args.events_tail))

    dashboard = {
        "run_id": run_id,
        "generated_at": __import__("datetime").datetime.now().astimezone().isoformat(),
        "data_sources": {
            "status": "extract_run_status.py",
            "companies": "extract_batch_table.py --format json",
            "audit": "extract_audit.py",
            "events": "extract_events.py",
            "process_files": "runs/<run_id>/companies/**",
        },
        "status": status,
        "companies": companies,
        "audit": audit,
        "recent_events": events,
        "process_files": process_file_index(run_dir, args.files_per_company),
    }
    common.emit(dashboard)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
