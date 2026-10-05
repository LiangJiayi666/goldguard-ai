# -*- coding: utf-8 -*-
"""原子脚本 06：批次审计事件时间线（events 表）。

用法:
  python .workbuddy/skills/pipeline-query/report/extract_events.py --run latest --tail 20
  python .workbuddy/skills/pipeline-query/report/extract_events.py --run latest --event attempt_finished --company 金雅福
"""
from __future__ import annotations

import argparse

import common


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="审计事件时间线")
    parser.add_argument("--run", default="latest")
    parser.add_argument("--tail", type=int, default=20, help="输出最近 N 条")
    parser.add_argument("--event", help="按事件类型过滤")
    parser.add_argument("--company", help="只看某企业相关（子串匹配）")
    args = parser.parse_args()

    _, run_id = common.resolve_run(args.run)
    name_map = common.company_names_from_db(run_id)
    rows = common.events_for_run(run_id)
    for r in rows:
        r["payload"] = r.pop("payload_json")
    if args.event:
        rows = [r for r in rows if r["event"] == args.event]
    if args.company:
        sub = {cid for cid, name in name_map.items() if args.company in name}
        if not sub:
            common.emit({"error": f"未找到包含「{args.company}」的企业"})
            return 1
        with common.db() as con:
            task_ids = {
                r["task_id"]
                for r in con.execute(
                    "SELECT task_id FROM tasks WHERE run_id=? AND company_id IN (%s)"
                    % ",".join("?" * len(sub)),
                    [run_id, *sub],
                ).fetchall()
            }
        ids = sub | task_ids
        rows = [r for r in rows if r["target_id"] in ids]
    rows.sort(key=lambda r: r["time"], reverse=True)
    common.emit({"run_id": run_id, "events": rows[: args.tail]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
