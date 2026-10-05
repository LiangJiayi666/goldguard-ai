# -*- coding: utf-8 -*-
"""原子脚本 04：批次企业总览表（每家一行，用于批次报告表格）。

用法:
  python .workbuddy/skills/pipeline-query/report/extract_batch_table.py --run latest --format json
  python .workbuddy/skills/pipeline-query/report/extract_batch_table.py --run latest --format csv
  python .workbuddy/skills/pipeline-query/report/extract_batch_table.py --run latest --format md

列: 企业 | 状态 | ICP载体数(网站/小程序) | 搜索候选官网数 | 互联网载体候选数 | 官网域名数 | 活跃域名 | 社媒账号数 | 有风险 | 回购线索 | 关键词命中 | 生成时间
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os

import common

COLUMNS = [
    "企业", "状态", "ICP网站数", "ICP小程序数", "ICP应用数", "ICP快应用数",
    "搜索候选官网数", "互联网载体候选数", "官网域名数", "活跃域名",
    "社媒账号数", "有风险", "回购线索", "风险词", "分析模式", "生成时间",
]


def row_for(export_path: str, run_id: str) -> dict:
    export = common.load_json(export_path)
    name = export.get("company") or os.path.splitext(os.path.basename(export_path))[0]
    icp = export.get("icp_carriers") or {}
    icp_counts = {k: len(v or []) for k, v in icp.items()}
    website = export.get("website_crawl") or {}
    website_search = export.get("website_search") or {}
    active_domains = [
        d.get("domain") for d in (export.get("domain_activity") or [])
        if d.get("is_active") is True
    ]
    risk = export.get("enterprise_assessment") or {}
    buyback = risk.get("gold_buyback") or {}
    keyword_hits = []
    for f in (risk.get("risk_keyword_findings") or []):
        for kw in (f.get("keywords") or []):
            if kw not in keyword_hits:
                keyword_hits.append(kw)
    return {
        "企业": name,
        "状态": export.get("status"),
        "ICP网站数": icp_counts.get("web", 0),
        "ICP小程序数": icp_counts.get("mapp", 0),
        "ICP应用数": icp_counts.get("app", 0),
        "ICP快应用数": icp_counts.get("kapp", 0),
        "搜索候选官网数": len(website_search.get("candidates") or []),
        "互联网载体候选数": len(website_search.get("carrier_candidates") or []),
        "官网域名数": len(website.get("seen_urls") or []),
        "活跃域名": ",".join(sorted(active_domains)) or "-",
        "社媒账号数": len(export.get("social_account_verifications") or []),
        "有风险": risk.get("overall_risk_found"),
        "回购线索": buyback.get("present"),
        "风险词": ",".join(keyword_hits) or "-",
        "分析模式": risk.get("analysis_mode") or "llm",
        "生成时间": export.get("generated_at"),
    }


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="批次企业总览表")
    parser.add_argument("--run", default="latest")
    parser.add_argument("--format", choices=["json", "csv", "md"], default="json")
    args = parser.parse_args()

    run_dir, run_id = common.resolve_run(args.run)
    exports = common.list_exports(run_dir)
    if not exports:
        if args.format == "json":
            common.emit({"run_id": run_id, "rows": [], "note": "无导出文件"})
        elif args.format == "csv":
            print(",".join(COLUMNS))
        else:
            print("| " + " | ".join(COLUMNS) + " |")
            print("| " + " | ".join("---" for _ in COLUMNS) + " |")
        return 0
    rows = [row_for(p, run_id) for p in exports]

    if args.format == "json":
        common.emit({"run_id": run_id, "count": len(rows), "rows": rows})
        return 0

    if args.format == "csv":
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        print(out.getvalue(), end="")
        return 0

    # markdown
    cols = list(rows[0].keys())
    print("| " + " | ".join(cols) + " |")
    print("| " + " | ".join("---" for _ in cols) + " |")
    for r in rows:
        cells = [str(r[c]).replace("|", "\\|") for c in cols]
        print("| " + " | ".join(cells) + " |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
