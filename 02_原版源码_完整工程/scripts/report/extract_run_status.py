# -*- coding: utf-8 -*-
"""原子脚本 02：批次进度总览（企业状态 + 任务按阶段/状态统计 + 事件数）。

用法: python .workbuddy/skills/pipeline-query/report/extract_run_status.py [--run <run_id|latest>]
"""
from __future__ import annotations

import argparse

import common


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="批次进度总览")
    parser.add_argument("--run", default="latest", help="批次 ID 或 latest")
    args = parser.parse_args()

    run_dir, run_id = common.resolve_run(args.run)
    with common.db() as con:
        company_rows = con.execute(
            "SELECT status, COUNT(*) AS n FROM companies WHERE run_id=? GROUP BY status",
            (run_id,),
        ).fetchall()
        phase_rows = con.execute(
            "SELECT phase, status, COUNT(*) AS n FROM tasks WHERE run_id=? "
            "GROUP BY phase, status ORDER BY phase, status",
            (run_id,),
        ).fetchall()
    event_rows_raw = common.events_for_run(run_id)
    event_by_type: dict[str, int] = {}
    for ev in event_rows_raw:
        event_by_type[ev["event"]] = event_by_type.get(ev["event"], 0) + 1

    manifest = {}
    try:
        manifest = common.load_json(f"{run_dir}/manifest.json")
    except Exception:
        pass

    result = {
        "run_id": run_id,
        "manifest": manifest,
        "companies_by_status": {r["status"]: r["n"] for r in company_rows},
        "tasks_by_phase_status": {
            f"{r['phase']}:{r['status']}": r["n"] for r in phase_rows
        },
        "events_by_type": event_by_type,
    }
    common.emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
