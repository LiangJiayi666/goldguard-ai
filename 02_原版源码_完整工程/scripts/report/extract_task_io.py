# -*- coding: utf-8 -*-
"""原子脚本 08：某企业某阶段（可指定轮次）任务的输入(payload)与输出(result)。

用法:
  python .workbuddy/skills/pipeline-query/report/extract_task_io.py --run latest --company 上善智能 --phase RISK_ANALYSIS
  python .workbuddy/skills/pipeline-query/report/extract_task_io.py --run latest --company 上善智能 --phase KEYWORD --round 2
  python .workbuddy/skills/pipeline-query/report/extract_task_io.py --run latest --company 上善智能 --phase KEYWORD --round last
"""
from __future__ import annotations

import argparse
import json

import common


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="任务输入输出")
    parser.add_argument("--run", default="latest")
    parser.add_argument("--company", required=True)
    parser.add_argument("--phase", required=True, help="如 KEYWORD/RISK_ANALYSIS/WEB_ICP")
    parser.add_argument("--round", default="last", help="轮次序号，或 last(时间最晚的一个)")
    parser.add_argument("--json-only", action="store_true", help="只输出原始 payload/result")
    args = parser.parse_args()

    _, run_id = common.resolve_run(args.run)
    name_map = common.company_names_from_db(run_id)
    hits = [cid for cid, name in name_map.items() if args.company in name]
    if not hits:
        common.emit({"error": f"未找到包含「{args.company}」的企业"})
        return 1
    cid = hits[0]
    real_name = name_map[cid]

    with common.db() as con:
        rows = [
            dict(r)
            for r in con.execute(
                "SELECT task_id, phase, round_no, platform, status, payload_json, result_json, "
                "error_code, error_message, created_at, updated_at FROM tasks "
                "WHERE run_id=? AND company_id=? AND phase=? ORDER BY updated_at",
                (run_id, cid, args.phase),
            ).fetchall()
        ]
    if not rows:
        common.emit({"error": f"企业「{real_name}」没有 {args.phase} 任务"})
        return 1

    if args.round == "last":
        row = rows[-1]
    else:
        try:
            target = int(args.round)
        except ValueError:
            common.emit({"error": f"--round 需为数字或 last，收到 {args.round}"})
            return 1
        matches = [r for r in rows if r["round_no"] == target]
        if not matches:
            common.emit({"error": f"没有 round={target} 的任务，可用: {sorted({r['round_no'] for r in rows})}"})
            return 1
        row = matches[-1]

    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else None
    except json.JSONDecodeError:
        payload = row["payload_json"]

    with common.db() as con:
        result_json = common.resolve_task_result(con, row["task_id"]) or row["result_json"]
    try:
        result = json.loads(result_json) if result_json else None
    except json.JSONDecodeError:
        result = result_json

    if args.json_only:
        common.emit({"payload": payload, "result": result})
        return 0

    common.emit({
        "run_id": run_id,
        "company": real_name,
        "phase": row["phase"],
        "round_no": row["round_no"],
        "platform": row["platform"],
        "status": row["status"],
        "error_code": row["error_code"],
        "error_message": row["error_message"],
        "task_id": row["task_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "input": payload,
        "output": result,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
