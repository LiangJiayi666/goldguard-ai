# -*- coding: utf-8 -*-
"""原子脚本 01：列出所有批次及状态。

用法: python .workbuddy/skills/pipeline-query/report/extract_runs.py [--format json|md]
"""
from __future__ import annotations

import argparse

import common


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="列出所有批次")
    parser.add_argument("--format", choices=["json", "md"], default="json")
    args = parser.parse_args()

    runs = common.list_runs()
    if args.format == "json":
        common.emit(runs)
        return 0

    print("| run_id | 状态 | 输入文件 | 开始时间 | 结束时间 |")
    print("| --- | --- | --- | --- | --- |")
    for r in runs:
        print(f"| {r['run_id']} | {r['status']} | {r['input_file']} | {r['started_at']} | {r['finished_at']} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
