# -*- coding: utf-8 -*-
"""原子脚本 07：单企业某阶段的任务产物（读取 companies/<企业>/<PHASE>/ 下的 final.json）。

用法:
  python .workbuddy/skills/pipeline-query/report/extract_phase.py --run latest --company 金雅福 --phase WEB_ICP
  python .workbuddy/skills/pipeline-query/report/extract_phase.py --run latest --company 金雅福 --phase POST_CRAWL --kind posts

--kind 可选: summary(默认,任务status+summary) | full(整份final.json) | posts(作品列表) | corpus(语料文本)
"""
from __future__ import annotations

import argparse
import glob
import json
import os

import common


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="单企业某阶段任务产物")
    parser.add_argument("--run", default="latest")
    parser.add_argument("--company", required=True)
    parser.add_argument("--phase", required=True, help="WEB_ICP/WEB_SEARCH/WEB_CRAWL/OCR/KEYWORD/ACCOUNT_*/POST_CRAWL/RISK_* 等")
    parser.add_argument("--kind", choices=["full", "summary", "posts", "corpus"], default="summary")
    args = parser.parse_args()

    run_dir, run_id = common.resolve_run(args.run)
    name, export_path = common.find_company(run_dir, args.company)
    real_name = name
    if os.path.isfile(export_path):
        real_name = common.load_json(export_path).get("company") or name
    company_dir = os.path.join(run_dir, "companies", common.safe_name(name))
    phase_dir = os.path.join(company_dir, args.phase)
    finals = sorted(glob.glob(os.path.join(phase_dir, "task_*", "attempt_*", "final.json")))
    if not finals:
        common.emit({"run_id": run_id, "company": real_name, "phase": args.phase, "tasks": [], "note": "无任务产物"})
        return 0

    out_tasks = []
    for f in finals:
        rel = os.path.relpath(os.path.dirname(f), phase_dir).replace("\\", "/")
        data = common.load_json(f)
        if args.kind == "full":
            out_tasks.append({"task_dir": rel, "final": data})
        elif args.kind == "posts":
            items = (data.get("summary") or {}).get("items") or []
            out_tasks.append({
                "task_dir": rel,
                "status": data.get("status"),
                "posts": [
                    {
                        "id": it.get("item_id") or it.get("photo_id") or it.get("id"),
                        "title": it.get("title") or it.get("caption"),
                        "author": it.get("author_name"),
                        "time": it.get("timestamp") or it.get("time"),
                        "likes": it.get("like_count") or it.get("likes"),
                        "views": it.get("view_count"),
                    }
                    for it in items
                ],
            })
        elif args.kind == "corpus":
            texts = []
            summary = data.get("summary") or {}
            text = summary.get("text")
            if text:
                texts.append(text)
            out_tasks.append({"task_dir": rel, "status": data.get("status"), "text": "\n".join(texts)})
        else:
            out_tasks.append({"task_dir": rel, "status": data.get("status"), "summary": data.get("summary")})

    corpus_extra = {}
    if args.kind == "corpus":
        for fname in ("corpus.txt", "posts_corpus.txt"):
            p = os.path.join(phase_dir, "_state", fname)
            if os.path.isfile(p):
                with open(p, encoding="utf-8", errors="replace") as f:
                    corpus_extra[fname] = f.read()
    result = {"run_id": run_id, "company": real_name, "phase": args.phase, "task_count": len(finals), "tasks": out_tasks}
    if corpus_extra:
        result["state_files"] = corpus_extra
    common.emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
