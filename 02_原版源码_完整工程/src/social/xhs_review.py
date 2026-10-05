from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import json
from datetime import datetime
from pathlib import Path

from pipeline_runtime import resolve_output, safe_name, write_json
from social_screenshot import capture_page_screenshot
from xhs_login_state import has_login_state_lost_text

from xhs_search import check_login_required, check_rate_limited, human_pause, launch_browser, new_context
from xhs_detail import fetch_one_detail, LoginRequiredError as DetailLoginRequiredError, RateLimitedError as DetailRateLimitedError


ROOT = Path(__file__).resolve().parent


def resolve_output_path(args: argparse.Namespace, user_id: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(user_id)}_xhs_home.json")


def _detail_href(note_id: str, token: str) -> str | None:
    if not note_id or not token:
        return None
    return (
        f"https://www.xiaohongshu.com/search_result/{note_id}"
        f"?xsec_token={token}&xsec_source=pc_search"
    )


def fetch_home(page, user_id: str, xsec_token: str, max_items: int = 30) -> dict:
    profile_url = (
        f"https://www.xiaohongshu.com/user/profile/{user_id}"
        f"?xsec_token={xsec_token}&xsec_source=pc_search"
    )
    page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
    human_pause()
    if check_login_required(page):
        return {"status": "login_required", "items": [], "has_more": False, "next_cursor": "0"}
    if check_rate_limited(page):
        return {"status": "rate_limited", "items": [], "has_more": False, "next_cursor": "0"}
    items = page.evaluate(
        """(() => {
            const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
            const u = (window.__INITIAL_STATE__ || {}).user || {};
            const notes = uw(u.notes) || [];
            const flat = Array.isArray(notes[0]) ? notes.flat() : notes;
            return flat.map(raw => {
                const item = uw(raw) || {};
                const n = uw(item.noteCard || item.note_card || item) || {};
                return {
                    note_id: n.noteId || n.note_id,
                    title: n.displayTitle || n.display_title || n.title,
                    type: n.type,
                    likes: (uw(n.interactInfo || n.interact_info) || {}).likedCount
                        || (uw(n.interactInfo || n.interact_info) || {}).liked_count,
                    xsec_token: n.xsecToken || n.xsec_token || item.xsecToken || item.xsec_token,
                };
            }).filter(n => n.note_id);
        })()"""
    )
    for item in items:
        token = item.get("xsec_token") or xsec_token
        item["detail_href"] = _detail_href(item.get("note_id"), token)
    if has_login_state_lost_text(page):
        return {"status": "login_required", "items": [], "has_more": False, "next_cursor": "0"}
    total_items = len(items)
    return {
        "status": "ok",
        "items": items[:max_items],
        "cursor": "0",
        "next_cursor": "0",
        "has_more": total_items > max_items or total_items >= 20,
    }


def fetch_item_details(page, items: list[dict], default_token: str = "") -> list[dict]:
    """对复核抓到的每条笔记补抓详情页正文、点赞/收藏/评论数与 canonical_url。"""
    for item in items:
        note_id = item.get("note_id")
        token = item.get("xsec_token") or default_token
        href = item.get("detail_href")
        if not note_id:
            continue
        try:
            detail = fetch_one_detail(page, str(note_id), str(token), href or None)
        except (DetailLoginRequiredError, DetailRateLimitedError) as exc:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 小红书复核详情中断 {note_id}: {exc}", file=sys.stderr, flush=True)
            break
        except Exception as exc:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 小红书复核详情失败 {note_id}: {exc}", file=sys.stderr, flush=True)
            continue
        if not detail:
            continue
        for key in ("title", "desc", "likes", "collects", "comments", "time", "canonical_url", "keywords"):
            if detail.get(key) is not None:
                item[key] = detail[key]
        if detail.get("canonical_url") and not item.get("detail_href"):
            item["detail_href"] = detail["canonical_url"]
    return items


def main() -> int:
    parser = argparse.ArgumentParser(description="XHS home fetch one batch.")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--xsec-token", required=True)
    parser.add_argument("--max-items", type=int, default=30)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--output-file")
    parser.add_argument("--screenshot-file", help="账号主页截图文件")
    args = parser.parse_args()
    if args.max_items < 1:
        parser.error("--max-items must be greater than 0")
    output_path = resolve_output_path(args, args.user_id)
    with __import__("playwright.sync_api").sync_api.sync_playwright() as p:
        browser = launch_browser(p, args.headless)
        try:
            context = new_context(browser)
            page = context.new_page()
            result = fetch_home(page, args.user_id, args.xsec_token, args.max_items)
            capture_page_screenshot(page, args.screenshot_file, result)
            if result.get("status") == "ok" and result.get("items"):
                result["items"] = fetch_item_details(page, result["items"], args.xsec_token)
            write_json(output_path, result)
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
