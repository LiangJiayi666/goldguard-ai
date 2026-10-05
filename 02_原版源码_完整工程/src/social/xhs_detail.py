from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""小红书 第3步：抓单条帖子详情，输出详情 JSON 到文件。

优先用 --href（第2步 detail_href，xsec_source 与真实来源一致）；
否则用 search_result/<note_id> 让站点自己 302 到 /explore/（不直接访问 /explore/）。
数据以 __INITIAL_STATE__ 为主，og/og:xhs 元数据兜底。

用法:
  .venv/Scripts/python.exe xhs_detail.py --note-id 664eb0520000000016013066 \
      --xsec-token "XXX" [--href "https://..."] [--headless] \
      [--output-dir output] [--output-file xxx.json]
输出: JSON 文件 {title, desc, likes, collects, comments, time, canonical_url, keywords}
"""

import argparse
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from pipeline_runtime import resolve_output, safe_name, write_json
from social_screenshot import capture_page_screenshot

from xhs_search import (
    COOKIES_FILE, STEALTH_JS, USER_AGENT, launch_browser, new_context,
    check_login_required, check_rate_limited, save_cookies, human_pause,
    XHS_DETAIL_PAUSE_LOW, XHS_DETAIL_PAUSE_HIGH,
)
from xhs_login_state import has_login_state_lost_text

DETAIL_PAUSE = (XHS_DETAIL_PAUSE_LOW, XHS_DETAIL_PAUSE_HIGH)

ROOT = Path(__file__).parent
TASK_LABEL = "小红书帖子详情"


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def resolve_output_path(args: argparse.Namespace, note_id: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(note_id)}_xhs_detail.json")


class LoginRequiredError(Exception):
    pass


class RateLimitedError(Exception):
    pass


def fetch_one_detail(page, note_id: str, xsec_token: str, href: str | None) -> dict[str, Any]:
    """抓取单条小红书帖子详情。"""
    url = href or (
        f"https://www.xiaohongshu.com/search_result/{note_id}"
        f"?xsec_token={xsec_token}&xsec_source=pc_search"
    )

    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    human_pause(*DETAIL_PAUSE)

    if check_login_required(page):
        raise LoginRequiredError("小红书详情页需要登录")
    if check_rate_limited(page):
        raise RateLimitedError("小红书详情页操作过于频繁")

    try:
        page.wait_for_url("**/explore/**", timeout=30000)
    except PlaywrightTimeoutError:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except PlaywrightTimeoutError:
        pass
    if has_login_state_lost_text(page):
        raise LoginRequiredError("小红书详情页登录态已失效")
    page.wait_for_function(
        """() => {
            const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
            const d = window.__INITIAL_STATE__ && window.__INITIAL_STATE__.note;
            if (!d) return false;
            const map = uw(d.noteDetailMap);
            return !!(map && Object.keys(map).length);
        }""",
        timeout=30000,
    )
    human_pause(*DETAIL_PAUSE)
    if has_login_state_lost_text(page):
        raise LoginRequiredError("小红书详情页登录态已失效")
    detail = page.evaluate(
        """(() => {
            const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
            const d = (window.__INITIAL_STATE__ || {}).note || {};
            const map = uw(d.noteDetailMap);
            const cur = map ? Object.values(map)[0] : null;
            const n = uw((cur || {}).note || cur) || {};
            const ii = uw(n.interactInfo || n.interact_info) || {};
            const meta = name => {
                const el = document.querySelector(`meta[name="${name}"], meta[property="${name}"]`);
                return el ? el.content : null;
            };
            return {
                title: n.title || meta('og:title'),
                desc: ((n.desc || '') || (meta('og:description') || meta('description') || '')).slice(0, 2000),
                likes: ii.likedCount || ii.liked_count || meta('og:xhs:note_like'),
                collects: ii.collectedCount || ii.collected_count || meta('og:xhs:note_collect'),
                comments: ii.commentCount || ii.comment_count || meta('og:xhs:note_comment'),
                time: n.time || n.lastUpdateTime || n.last_update_time,
                canonical_url: meta('og:url') || location.href,
                keywords: meta('keywords'),
            };
        })()"""
    )
    return detail or {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--note-id", help="目标帖子 note_id（单条模式）")
    parser.add_argument("--xsec-token", default="")
    parser.add_argument("--href", default="", help="第2步给出的 detail_href，优先使用")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output", help="输出目录")
    parser.add_argument("--output-file", help="输出 JSON 文件名（可选）")
    parser.add_argument("--screenshot-file", help="帖子正文页截图文件")
    args = parser.parse_args()

    if not args.note_id:
        parser.error("单条模式需要 --note-id 参数")

    output_path = resolve_output_path(args, args.note_id)
    print(f"{now_stamp()} {TASK_LABEL} {args.note_id} 开始", flush=True)

    try:
        with sync_playwright() as p:
            browser = launch_browser(p, args.headless)
            context = new_context(browser)
            page = context.new_page()
            detail = fetch_one_detail(page, args.note_id, args.xsec_token, args.href or None)
            capture_page_screenshot(page, args.screenshot_file, detail)
            try:
                save_cookies(context)
            except Exception:
                pass
            output_path.parent.mkdir(parents=True, exist_ok=True)
            write_json(output_path, detail)
            try:
                context.close()
            except Exception:
                pass
            browser.close()
    except LoginRequiredError as exc:
        write_json(output_path, {"status": "login_required", "note_id": args.note_id, "error": str(exc)})
        print(f"{now_stamp()} {TASK_LABEL} {args.note_id} LOGIN_REQUIRED 需要重新登录", file=sys.stderr, flush=True)
        return 100
    except RateLimitedError as exc:
        write_json(output_path, {"status": "rate_limited", "note_id": args.note_id, "error": str(exc)})
        print(f"{now_stamp()} {TASK_LABEL} {args.note_id} RATE_LIMITED 操作频繁，先冷却稍后重试", file=sys.stderr, flush=True)
        return 0
    except Exception as exc:
        print(f"{now_stamp()} {TASK_LABEL} {args.note_id} 失败 {exc}", file=sys.stderr, flush=True)
        return 1

    print(
        f"{now_stamp()} {TASK_LABEL} {args.note_id} 成功 {output_path} "
        f"标题={detail.get('title') or '无'}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
