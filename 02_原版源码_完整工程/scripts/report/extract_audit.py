# -*- coding: utf-8 -*-
"""原子脚本 05：批次任务审计（失败/重试/跳过/复用/缺陷）。

用法:
  python .workbuddy/skills/pipeline-query/report/extract_audit.py --run latest --failed
  python .workbuddy/skills/pipeline-query/report/extract_audit.py --run latest --company 金雅福
  python .workbuddy/skills/pipeline-query/report/extract_audit.py --run latest --phase WEB_CRAWL

默认输出统计摘要；加 --detail 输出明细。
"""
from __future__ import annotations

import argparse

import common

# 这些 error_code 属于“可自动恢复的非错误状态”，不应展示为错误归因。
IGNORED_ERROR_CODES = {"history_materialize_error"}


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="批次任务审计")
    parser.add_argument("--run", default="latest")
    parser.add_argument("--company", help="只看某企业（子串匹配）")
    parser.add_argument("--phase", help="只看某阶段，如 WEB_CRAWL")
    parser.add_argument("--failed", action="store_true", help="只看失败终态任务")
    parser.add_argument("--retried", action="store_true", help="只看重试过的任务")
    parser.add_argument("--detail", action="store_true", help="输出明细")
    args = parser.parse_args()

    _, run_id = common.resolve_run(args.run)
    name_map = common.company_names_from_db(run_id)

    where = ["t.run_id=?"]
    params: list = [run_id]
    if args.company:
        cids = [cid for cid, name in name_map.items() if args.company in name]
        if not cids:
            common.emit({"error": f"未找到包含「{args.company}」的企业"})
            return 1
        marks = ",".join("?" * len(cids))
        where.append(f"t.company_id IN ({marks})")
        params.extend(cids)
    if args.phase:
        where.append("t.phase=?")
        params.append(args.phase)
    if args.failed:
        where.append(
            "t.status='TERMINAL' AND ("
            "t.error_code IS NOT NULL OR "
            "COALESCE(t.result_json, '') = '' OR "
            "t.result_json LIKE '%FAILED_FINAL%'"
            ")"
        )
    if args.retried:
        where.append("t.retry_count > 0")

    sql = (
        "SELECT t.task_id, t.company_id, t.phase, t.round_no, t.platform, "
        "t.status, t.retry_count, t.error_code, t.error_message, t.created_at, t.updated_at, "
        "COALESCE(a.reuse_match_kind, '') AS reuse_match_kind, COALESCE(a.execution_mode, '') AS execution_mode "
        "FROM tasks t "
        "LEFT JOIN task_attempts a ON a.task_id = t.task_id AND a.status='SUCCEEDED' "
        f"WHERE {' AND '.join(where)} ORDER BY t.updated_at DESC"
    )
    with common.db() as con:
        rows = [dict(r) for r in con.execute(sql, params).fetchall()]
        for r in rows:
            if not r.get("error_code") and not r.get("result_json"):
                r["result_json"] = common.resolve_task_result(con, r["task_id"])

    if not args.detail:
        summary = {
            "run_id": run_id,
            "total": len(rows),
            "by_phase": {},
            "by_status": {},
            "by_error": {},
        }
        for r in rows:
            summary["by_phase"][r["phase"]] = summary["by_phase"].get(r["phase"], 0) + 1
            key = r["status"] if r["status"] != "TERMINAL" else ("FAILED" if r["error_code"] else "OK")
            summary["by_status"][key] = summary["by_status"].get(key, 0) + 1
            if r["error_code"] and r["error_code"] not in IGNORED_ERROR_CODES:
                summary["by_error"][r["error_code"]] = summary["by_error"].get(r["error_code"], 0) + 1
        common.emit(summary)
        return 0

    for r in rows:
        r["company_name"] = name_map.get(r["company_id"], r["company_id"])
    common.emit(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
