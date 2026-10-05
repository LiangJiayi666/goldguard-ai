from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import json
import re
from pathlib import Path
from typing import Any

from pipeline_runtime import resolve_output, safe_name, write_json
from social_screenshot import capture_page_screenshot

from ks_profile_extract import (
    capture_profile_responses, extract_dom_items,
    extract_profile_feed_batch, extract_profile_summary, extract_visible_profile,
    reconcile_profile_summary, item_key,
)
from kuaishou_search import check_login_required, human_pause, launch_browser, new_context

ROOT = Path(__file__).resolve().parent


def resolve_output_path(args: argparse.Namespace, kwai_id: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(kwai_id)}_ks_home.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kuaishou profile home fetch.")
    parser.add_argument("--kwai-id", required=True)
    parser.add_argument("--max-items", type=int, default=30)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--output-file")
    parser.add_argument("--screenshot-file", help="账号主页截图文件")
    args = parser.parse_args()
    if args.max_items < 1:
        parser.error("--max-items must be greater than 0")
    output_path = resolve_output_path(args, args.kwai_id)

    from playwright.sync_api import sync_playwright

    payload: dict[str, Any]
    try:
        with sync_playwright() as p:
            browser = launch_browser(p, args.headless)
            try:
                context = new_context(browser)
                page = context.new_page()
                captured = capture_profile_responses(page)
                page.goto(f"https://www.kuaishou.com/profile/{args.kwai_id}", wait_until="domcontentloaded", timeout=60000)
                human_pause()
                page.wait_for_timeout(8000)
                page_profile = extract_visible_profile(page, args.kwai_id)

                items: list[dict[str, Any]] = []
                seen_keys: set[str] = set()
                last_cursor = ""
                has_more = False
                for raw_payload in captured["feeds"]:
                    batch = extract_profile_feed_batch(raw_payload)
                    cursor = str(batch.get("cursor") or "")
                    if cursor:
                        last_cursor = cursor
                    has_more = has_more or bool(batch.get("has_more"))
                    for item in batch["items"]:
                        key = item_key(item)
                        if key and key not in seen_keys:
                            seen_keys.add(key)
                            items.append(item)
                            if len(items) >= args.max_items:
                                break
                    if len(items) >= args.max_items:
                        break

                if not items:
                    for item in extract_dom_items(page):
                        key = item_key(item)
                        if key and key not in seen_keys:
                            seen_keys.add(key)
                            items.append(item)
                            if len(items) >= args.max_items:
                                break

                profile = reconcile_profile_summary(
                    extract_profile_summary(captured["profile"]), items, args.kwai_id, page_profile,
                )
                status_ok = bool(items or captured["feeds"] or profile or not check_login_required(page))
                payload = {
                    "status": "ok" if status_ok else "login_required",
                    "items": items,
                    "cursor": last_cursor or "0",
                    "has_more": has_more,
                    "feed_count": len(captured["feeds"]),
                    "source": "profile_feed_api" if captured["feeds"] else "dom_fallback",
                    "profile_page_confirmed": bool(page_profile.get("profile_page_confirmed")),
                }
                if page_profile.get("profile_page_confirmed"):
                    payload["profile_url"] = str(page_profile.get("profile_url") or page.url or "")
                if page_profile:
                    payload["page_profile"] = page_profile
                if profile:
                    payload["profile"] = profile
                if not status_ok:
                    payload["items"] = []
                    payload["cursor"] = "0"
                    payload["has_more"] = False
                capture_page_screenshot(page, args.screenshot_file, payload)
            finally:
                browser.close()
    except Exception as exc:
        payload = {"status": "error", "error": str(exc)[:500], "items": [], "cursor": "0", "has_more": False}

    write_json(output_path, payload)
    return 1 if payload.get("status") == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
