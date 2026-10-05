from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import sys
from pathlib import Path
from typing import Any, Callable

from pipeline_runtime import atomic_result, write_json
from social_screenshot import capture_page_screenshot


ROOT = Path(__file__).resolve().parent


def _platform_browser(platform: str) -> tuple[Callable[..., Any], Callable[..., Any], Callable[..., bool]]:
    if platform == "xhs":
        from xhs_search import check_login_required, launch_browser, new_context
        return launch_browser, new_context, check_login_required
    if platform == "douyin":
        from douyin_search import check_login_required, launch_browser, new_context
        return launch_browser, new_context, check_login_required
    if platform == "kuaishou":
        from kuaishou_search import check_login_required, launch_browser, new_context
        return launch_browser, new_context, check_login_required
    raise ValueError(f"unsupported platform: {platform}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture one authenticated social-media evidence URL.")
    parser.add_argument("--platform", choices=("xhs", "douyin", "kuaishou"), required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--screenshot-file", type=Path, required=True)
    parser.add_argument("--output-file", type=Path, required=True)
    args = parser.parse_args()

    launch_browser, new_context, check_login_required = _platform_browser(args.platform)
    payload: dict[str, Any]
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser = launch_browser(playwright, args.headless)
            try:
                context = new_context(browser)
                page = context.new_page()
                response = page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    page.wait_for_timeout(2000)
                if check_login_required(page):
                    payload = atomic_result(
                        {
                            "status": "login_required",
                            "platform": args.platform,
                            "requested_url": args.url,
                            "final_url": page.url,
                        },
                        source="social_evidence_screenshot.login_check",
                    )
                    write_json(args.output_file, payload)
                    return 100
                payload = {
                    "status": "ok",
                    "platform": args.platform,
                    "requested_url": args.url,
                    "final_url": page.url,
                    "status_code": response.status if response is not None else None,
                }
                capture_page_screenshot(page, args.screenshot_file, payload, quality=85)
                write_json(args.output_file, payload)
            finally:
                browser.close()
    except Exception as exc:
        payload = atomic_result(
            {
                "status": "error",
                "platform": args.platform,
                "requested_url": args.url,
                "error": str(exc)[:500],
            },
            source="social_evidence_screenshot.exception",
            error_code="process_error",
        )
        write_json(args.output_file, payload)
        print(str(exc), file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
