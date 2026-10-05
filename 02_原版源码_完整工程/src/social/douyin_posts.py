from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from pipeline_runtime import resolve_output, safe_name, write_json
from social_screenshot import capture_page_screenshot
from douyin_search import check_login_required, human_pause, launch_browser, new_context, signed_fetch


ROOT = Path(__file__).resolve().parent


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _truthy_has_more(value) -> bool:
    # 服务端 has_more 可能是 int 0/1、bool，或字符串 '0'/'1'/'false'/'true'。
    # 必须归一化为 bool：避免 bool('0')=True 误判"还有下一页"导致死循环。
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no"}
    return False


def resolve_output_path(args: argparse.Namespace, sec_uid: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(sec_uid)}_douyin_page.json")


def fetch_page(page, sec_uid: str, cursor: str, max_items: int = 18) -> dict:
    body = signed_fetch(page, "/aweme/v1/web/aweme/post/", {
        "device_platform": "webapp", "aid": "6383", "channel": "channel_pc_web",
        "sec_user_id": sec_uid, "max_cursor": cursor, "count": str(max_items),
        "publish_video_strategy_type": "2",
    })
    if isinstance(body, dict) and isinstance(body.get("aweme_list"), list):
        body["aweme_list"] = body["aweme_list"][:max_items]
    return body


def main() -> int:
    parser = argparse.ArgumentParser(description="Douyin fetch one page by cursor.")
    parser.add_argument("--sec-uid", required=True)
    parser.add_argument("--cursor", default="0")
    parser.add_argument("--max-items", type=int, default=18)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--output-file")
    parser.add_argument("--screenshot-file", help="账号主页截图文件")
    args = parser.parse_args()
    if args.max_items < 1:
        parser.error("--max-items must be greater than 0")
    output_path = resolve_output_path(args, args.sec_uid)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = launch_browser(p, args.headless)
        try:
            context = new_context(browser)
            page = context.new_page()
            page.goto(
                f"https://www.douyin.com/user/{args.sec_uid}",
                wait_until="domcontentloaded", timeout=60000,
            )
            human_pause()
            if check_login_required(page):
                write_json(output_path, {"status": "login_required", "aweme_list": [], "has_more": False, "next_cursor": args.cursor})
                print(f"{now_stamp()} douyin_page {args.sec_uid} LOGIN_REQUIRED", file=sys.stderr, flush=True)
                return 100
            body = fetch_page(page, args.sec_uid, args.cursor, args.max_items)
            if isinstance(body, dict):
                body["has_more"] = _truthy_has_more(body.get("has_more"))
                body.setdefault("next_cursor", str(body.get("max_cursor") or body.get("cursor") or args.cursor))
                body.setdefault("profile_url", page.url)
                capture_page_screenshot(page, args.screenshot_file, body)
            write_json(output_path, body)
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
