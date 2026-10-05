from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""快手 第2步：抓指定账号主页全部作品，单函数增量抓取，输出作品列表 JSON。

从首页开始持续滚动触发 /rest/v/profile/feed 分页：任一作品命中历史已爬集合即停
（增量锚点），否则 feed 停滞（滚到底）停。替代旧的 home + page 分页。

用法:
  .venv/Scripts/python.exe kuaishou_posts.py --kwai-id <id> \
      [--existing-ids id1 id2 ...] [--headless] [--output-dir output] [--output-file x.json]

输出: JSON {status, items:[{item_id,photo_id,video_id,href,title,...}], has_more, stopped_early, count, profile}
"""

import argparse
import json
import os
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from pipeline_runtime import resolve_output, safe_name, write_json
from ks_profile_extract import (
    capture_profile_responses, extract_dom_items,
    extract_profile_feed_batch, extract_profile_summary, extract_visible_profile,
    reconcile_profile_summary, item_key,
)
from kuaishou_search import check_login_required, human_pause, launch_browser, new_context

ROOT = Path(__file__).parent


def resolve_output_path(args: argparse.Namespace, kwai_id: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(kwai_id)}_ks_notes.json")


class LoginRequiredError(Exception):
    pass


def _atomic_write_ids(path: Path, ids) -> None:
    """原子写 checkpoint：先写 .tmp 再 os.replace，避免中断留下半截文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(sorted(ids)) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _load_checkpoint(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def fetch_one_account(
    page, kwai_id: str, existing_ids: set[str], max_items: int = 30,
    skip_ids: set[str] | None = None, resume_file: Path | None = None,
    checkpoint_every: int = 10,
) -> tuple[list[dict], bool, dict, dict[str, Any], int, int, bool]:
    """抓取一个快手账号主页作品。

    返回 (作品列表, 是否命中历史锚点提前停, profile, feed 响应数, 跳过的已见作品数, 是否权威到底)。
    ``skip_ids``（如 review 样本 / checkpoint 恢复）命中只跳过不收集、不触发锚点停，继续滚。
    权威到底信号 = 最后消费的 feed batch 的 pcursor ∈ {"", "no_more"}（has_more=False）。
    ``resume_file``：每 ``checkpoint_every`` 轮原子写已收集作品 id，供超时 kill 后恢复。
    """
    skip_ids = skip_ids or set()
    captured = capture_profile_responses(page)
    page.goto(f"https://www.kuaishou.com/profile/{kwai_id}", wait_until="domcontentloaded", timeout=60000)
    human_pause()
    page.wait_for_timeout(8000)
    if check_login_required(page):
        raise LoginRequiredError("快手主页需要登录")
    page_profile = extract_visible_profile(page, kwai_id)

    items: dict[str, dict] = {}
    profile_evidence: list[dict] = []
    stopped_early = False
    stagnant = 0
    processed = 0
    skipped = 0
    bottom_reached = False
    round_no = 0

    # 无轮数上限：终止依赖 权威到底信号（pcursor=no_more）、停滞检测 与 增量锚点。
    while True:
        round_no += 1
        for raw in captured["feeds"][processed:]:
            batch = extract_profile_feed_batch(raw)
            bottom_reached = not batch["has_more"]
            for item in batch["items"]:
                if not profile_evidence and item.get("author_id"):
                    profile_evidence.append(item)
                key = item_key(item)
                if not key:
                    continue
                if key in existing_ids:
                    stopped_early = True
                    break
                if key in skip_ids:
                    skipped += 1
                    continue
                items[key] = item
                if max_items and len(items) >= max_items:
                    break
            if stopped_early:
                break
        processed = len(captured["feeds"])
        if stopped_early or (max_items and len(items) >= max_items) or bottom_reached:
            break
        feed_count = len(captured["feeds"])
        if resume_file and round_no % checkpoint_every == 0:
            _atomic_write_ids(resume_file, items.keys())
        page.mouse.wheel(0, 2400)
        human_pause(1.0, 1.8)
        try:
            page.wait_for_load_state("networkidle", timeout=8000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(1500)
        if len(captured["feeds"]) == feed_count:
            stagnant += 1
            if stagnant >= 3:
                break
        else:
            stagnant = 0

    # API 未返回时退回 DOM 解析（无增量锚点，一次性抓可见卡片）。
    if not captured["feeds"]:
        for item in extract_dom_items(page):
            key = item_key(item)
            if not key or key in items:
                continue
            if key in existing_ids:
                stopped_early = True
                break
            if key in skip_ids:
                skipped += 1
                continue
            items[key] = item
            if max_items and len(items) >= max_items:
                break

    profile = reconcile_profile_summary(
        extract_profile_summary(captured["profile"]), profile_evidence or list(items.values()), kwai_id,
        page_profile,
    )
    if max_items:
        return list(items.values())[:max_items], stopped_early, profile, page_profile, len(captured["feeds"]), skipped, bottom_reached
    return list(items.values()), stopped_early, profile, page_profile, len(captured["feeds"]), skipped, bottom_reached


def build_summary(items: list[dict], stopped_early: bool, status: str = "ok",
                  profile: dict | None = None, feed_count: int = 0, skipped: int = 0,
                  reached_bottom: bool = False, page_profile: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "items": items,
        "has_more": not reached_bottom,
        "stopped_early": stopped_early,
        "reached_bottom": reached_bottom,
        "skipped": skipped,
        "count": len(items),
        "feed_count": feed_count,
        "source": "profile_feed_api" if feed_count else "dom_fallback",
    }
    if profile:
        payload["profile"] = profile
    visible_profile = page_profile if isinstance(page_profile, dict) else {}
    profile_url = str(visible_profile.get("profile_url") or "")
    if profile_url and visible_profile.get("profile_page_confirmed"):
        payload["profile_url"] = profile_url
    payload["profile_page_confirmed"] = bool(visible_profile.get("profile_page_confirmed"))
    if visible_profile:
        payload["page_profile"] = visible_profile
    return payload


def _crawl(browser, kwai_id: str, existing_ids: set[str], max_items: int = 30,
           skip_ids: set[str] | None = None, resume_file: Path | None = None) -> dict[str, Any]:
    context = new_context(browser)
    try:
        page = context.new_page()
        try:
            items, stopped_early, profile, page_profile, feed_count, skipped, reached_bottom = fetch_one_account(
                page, kwai_id, existing_ids, max_items, skip_ids, resume_file,
            )
            status_ok = bool(items or feed_count or profile)
            return build_summary(
                items, stopped_early, "ok" if status_ok else "login_required", profile,
                feed_count, skipped, reached_bottom, page_profile,
            )
        except LoginRequiredError:
            return build_summary([], False, status="login_required")
    finally:
        try:
            context.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Kuaishou profile notes: incremental single-pass crawl.")
    parser.add_argument("--kwai-id", help="目标账号 kwai_id（单条模式）")
    parser.add_argument("--existing-ids", nargs="*", default=[], help="历史已爬 photo_id 集合，命中任一即停")
    parser.add_argument("--skip-ids", nargs="*", default=[], help="已见 photo_id（如 review 样本），跳过不收集、不触发锚点停")
    parser.add_argument("--max-items", type=int, default=30, help="单账号最多抓取的作品卡片数；0=无上限（滚到底）")
    parser.add_argument("--resume-file", type=Path, default=None, help="中途 checkpoint 路径：启动读入已收作品 id 作跳过集，每 10 轮原子写入，正常完成删除")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--output-file")
    args = parser.parse_args()

    if args.max_items < 0:
        parser.error("--max-items 必须大于等于 0（0 表示无上限）")

    if not args.kwai_id:
        parser.error("单条模式需要 --kwai-id 参数")

    output_path = resolve_output_path(args, args.kwai_id)
    skip_ids = set(args.skip_ids or [])
    if args.resume_file and args.resume_file.is_file():
        skip_ids |= _load_checkpoint(args.resume_file)
    with sync_playwright() as p:
        browser = launch_browser(p, args.headless)
        try:
            summary = _crawl(browser, args.kwai_id, set(args.existing_ids or []), args.max_items, skip_ids, args.resume_file)
            if summary["status"] == "ok" and args.resume_file:
                args.resume_file.unlink(missing_ok=True)
            write_json(output_path, summary)
            if summary["status"] == "login_required":
                return 100
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
