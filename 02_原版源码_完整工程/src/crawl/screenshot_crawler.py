from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import hashlib
import re
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

from pipeline_runtime import write_json
from site_crawler import build_output as build_static_output
from site_fetch_common import (
    UnsafeUrlError,
    canonical_url,
    install_playwright_ssrf_guard,
    is_auth_page,
    now_stamp,
    resolve_public_url,
    skip_reason,
    ssrf_block_payload,
)


TASK_LABEL = "网页截图"
NETWORKIDLE_MS = 3000
HEADED_RETRY_DELAY_SECONDS = 1.0
OFFSCREEN_BROWSER_ARGS = ["--window-position=-32000,-32000"]
DYNAMIC_FETCH_FUNCTION = "screenshot_crawler.render_page"
STATIC_FETCH_FUNCTION = "site_crawler.fetch_page"


class Extractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.text: list[str] = []
        self.forms = 0
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        data = dict(attrs)
        if tag == "a" and data.get("href"):
            self.links.append(data["href"])
        if tag == "form":
            self.forms += 1
        if tag in ("script", "style", "template", "noscript"):
            self._skip += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "template", "noscript") and self._skip:
            self._skip -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip and data.strip():
            self.text.append(data.strip())


def accept_cookie_banner(page) -> None:
    candidates = [
        "button:has-text('接受所有Cookie')",
        "button:has-text('接受Cookie')",
        "button:has-text('接受')",
        "button:has-text('同意')",
        "button:has-text('允许')",
        "text=Accept all cookies",
        "text=Accept cookies",
        "text=Accept",
        "text=I agree",
    ]
    for selector in candidates:
        try:
            locator = page.locator(selector).first
            if locator.count() and locator.is_visible():
                locator.click(timeout=1500)
                page.wait_for_timeout(500)
                return
        except Exception:
            continue


def gentle_scroll(page) -> None:
    try:
        page.mouse.wheel(0, 1200)
        page.wait_for_timeout(500)
        page.mouse.wheel(0, 1200)
        page.wait_for_timeout(500)
        page.mouse.wheel(0, -600)
        page.wait_for_timeout(300)
    except Exception:
        pass


def default_name(url: str, suffix: str) -> Path:
    return Path(f"{hashlib.sha1(url.encode('utf-8')).hexdigest()[:16]}{suffix}")


def resolve_output_path(directory: Path, provided: str | None, fallback: Path) -> Path:
    if not provided:
        return directory / fallback
    candidate = Path(provided)
    if candidate.is_absolute() or candidate.parent != Path("."):
        return candidate
    return directory / candidate.name


def headed_screenshot_path(screenshot_path: Path) -> Path:
    return screenshot_path.with_name(
        f"{screenshot_path.stem}_headed{screenshot_path.suffix}"
    )


def save_page_screenshot(page, screenshot_path: Path, screenshot_quality: int) -> None:
    screenshot_options = {"path": str(screenshot_path), "full_page": True}
    if screenshot_path.suffix.lower() in {".jpg", ".jpeg"}:
        screenshot_options.update(type="jpeg", quality=screenshot_quality)
    page.screenshot(**screenshot_options)


def render_page(
    page, url: str, screenshot_path: Path, json_path: Path,
    url_timeout: int, screenshot_quality: int = 70,
) -> dict:
    """Render one URL on an existing page, write screenshot+json, return a result dict."""
    resolve_public_url(url)
    response = page.goto(url, wait_until="domcontentloaded", timeout=url_timeout * 1000)
    resolve_public_url(page.url)
    redirect_reason = skip_reason(page.url)
    if redirect_reason:
        return {"status": "skipped", "reason": redirect_reason}
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_MS)
    except Exception:
        page.wait_for_timeout(1500)
    accept_cookie_banner(page)
    gentle_scroll(page)
    try:
        page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_MS)
    except Exception:
        page.wait_for_timeout(1000)
    if response and response.status >= 400:
        return {"status": "failed", "reason": f"browser HTTP {response.status}"}
    status_code = response.status if response is not None else None

    page_html = page.content()
    extractor = Extractor()
    extractor.feed(page_html)
    text = "\n".join(extractor.text)
    links = [u for u in (canonical_url(v, page.url) for v in extractor.links) if u]
    if is_auth_page("", extractor):
        return {"status": "skipped", "reason": "登录"}

    payload = {
        "status": "ok",
        "url": url,
        "fetched_url": page.url,
        "status_code": status_code,
        "text": text,
        "links": list(dict.fromkeys(links)),
        "fetch_function": DYNAMIC_FETCH_FUNCTION,
        "fetch_mode": "dynamic",
        "image_download_required": False,
        "screenshot_file": str(screenshot_path),
        "content_type": "text/html;charset=utf-8",
        "content_fingerprint": hashlib.sha256(
            re.sub(r"\s+", " ", text).strip().encode("utf-8")
        ).hexdigest()
        if text.strip()
        else None,
        "content_fingerprint_version": 2,
    }
    guard = getattr(page.context, "_codex_ssrf_guard", None)
    if guard is not None and guard.blocked:
        payload["ssrf_blocked_requests"] = list(guard.blocked)

    save_page_screenshot(page, screenshot_path, screenshot_quality)
    write_json(json_path, payload)
    return {
        "status": "ok",
        "reason": "",
        "fetched_url": page.url,
        "status_code": status_code,
        "fetch_function": DYNAMIC_FETCH_FUNCTION,
    }


def static_fallback(
    url: str,
    json_path: Path,
    timeout: int,
    max_bytes: int,
    dynamic_attempt: dict[str, object],
) -> dict:
    """Use the static crawler after a technical dynamic-render failure."""
    try:
        payload = build_static_output(url, timeout, max_bytes)
        skip = payload.pop("_skip_reason", None)
        if skip:
            result = {
                "status": "skipped",
                "url": url,
                "reason": str(skip),
                "fetch_function": STATIC_FETCH_FUNCTION,
                "fetch_mode": "static",
                "image_download_required": False,
                "dynamic_attempt": dynamic_attempt,
            }
            write_json(json_path, result)
            return result
        payload.update({
            "status": "ok",
            "fetch_function": STATIC_FETCH_FUNCTION,
            "fetch_mode": "static",
            "image_download_required": True,
            "dynamic_attempt": dynamic_attempt,
        })
        write_json(json_path, payload)
        return {
            "status": "ok",
            "reason": "",
            "fetched_url": str(payload.get("fetched_url") or url),
            "status_code": payload.get("status_code"),
            "fetch_function": STATIC_FETCH_FUNCTION,
        }
    except UnsafeUrlError as exc:
        result = {
            **ssrf_block_payload(exc),
            "url": url,
            "fetch_function": STATIC_FETCH_FUNCTION,
            "fetch_mode": "static",
            "image_download_required": False,
            "dynamic_attempt": dynamic_attempt,
        }
    except TimeoutError as exc:
        result = {
            "status": "timeout",
            "url": url,
            "reason": str(exc) or "static fetch timeout",
            "fetch_function": STATIC_FETCH_FUNCTION,
            "fetch_mode": "static",
            "image_download_required": False,
            "dynamic_attempt": dynamic_attempt,
        }
    except Exception as exc:
        result = {
            "status": "error",
            "url": url,
            "error": str(exc)[:500],
            "fetch_function": STATIC_FETCH_FUNCTION,
            "fetch_mode": "static",
            "image_download_required": False,
            "dynamic_attempt": dynamic_attempt,
        }
    write_json(json_path, result)
    return result


def crawl_page(
    page,
    url: str,
    screenshot_path: Path,
    json_path: Path,
    timeout: int,
    max_bytes: int,
    screenshot_quality: int = 70,
) -> dict:
    """Render dynamically first; fall back to static fetch within one attempt."""
    try:
        outcome = render_page(
            page, url, screenshot_path, json_path, timeout, screenshot_quality,
        )
    except UnsafeUrlError as exc:
        outcome = ssrf_block_payload(exc)
    except TimeoutError as exc:
        outcome = {"status": "timeout", "reason": str(exc) or "dynamic render timeout"}
    except Exception as exc:
        outcome = {"status": "error", "reason": str(exc)[:500]}

    status = str(outcome.get("status") or "error")
    reason = str(outcome.get("reason") or "")
    if status == "ok":
        return outcome
    if status == "skipped":
        result = {
            "status": "skipped",
            "url": url,
            "reason": reason,
            "fetch_function": DYNAMIC_FETCH_FUNCTION,
            "fetch_mode": "dynamic",
            "image_download_required": False,
        }
        for key in ("blocked_url", "blocked_address"):
            if outcome.get(key):
                result[key] = outcome[key]
        write_json(json_path, result)
        return result
    return static_fallback(
        url,
        json_path,
        timeout,
        max_bytes,
        {"function": DYNAMIC_FETCH_FUNCTION, "status": status, "reason": reason},
    )


def run_dynamic_browser(
    playwright,
    *,
    headless: bool,
    url: str,
    screenshot_path: Path,
    json_path: Path,
    timeout: int,
    screenshot_quality: int,
) -> dict:
    """Run one isolated dynamic attempt and always close its browser."""
    launch_options: dict[str, object] = {"headless": headless}
    if not headless:
        launch_options["args"] = OFFSCREEN_BROWSER_ARGS

    browser = None
    page = None
    interrupted = False
    outcome: dict = {"status": "error", "reason": "browser did not start"}
    try:
        browser = playwright.chromium.launch(**launch_options)
        context = new_context(browser)
        page = context.new_page()
        outcome = render_page(
            page, url, screenshot_path, json_path, timeout, screenshot_quality,
        )
    except UnsafeUrlError as exc:
        outcome = ssrf_block_payload(exc)
    except TimeoutError as exc:
        outcome = {"status": "timeout", "reason": str(exc) or "dynamic render timeout"}
    except Exception as exc:
        outcome = {"status": "error", "reason": str(exc)[:500]}
    except KeyboardInterrupt:
        # Preserve the operator/host interruption.  The finally block must not
        # launch a second screenshot operation against a closing browser.
        interrupted = True
        raise
    finally:
        if not interrupted and page is not None and not outcome.get("screenshot_file"):
            try:
                save_page_screenshot(page, screenshot_path, screenshot_quality)
                outcome["screenshot_file"] = str(screenshot_path)
            except Exception as exc:
                outcome["screenshot_error"] = str(exc)[:500]
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
    return outcome


def crawl_with_browser_retry(
    playwright,
    url: str,
    screenshot_path: Path,
    json_path: Path,
    timeout: int,
    max_bytes: int,
    screenshot_quality: int = 70,
    headed_retry_delay: float = HEADED_RETRY_DELAY_SECONDS,
) -> dict:
    """Try headless, retry headed off-screen, then use the static fallback."""
    attempts: list[dict[str, str]] = []
    modes = (("headless", True), ("headed", False))

    for index, (mode, headless) in enumerate(modes):
        attempt_screenshot_path = (
            screenshot_path if headless else headed_screenshot_path(screenshot_path)
        )
        outcome = run_dynamic_browser(
            playwright,
            headless=headless,
            url=url,
            screenshot_path=attempt_screenshot_path,
            json_path=json_path,
            timeout=timeout,
            screenshot_quality=screenshot_quality,
        )
        status = str(outcome.get("status") or "error")
        reason = str(outcome.get("reason") or "")
        if status == "ok":
            return outcome
        if status == "skipped":
            result = {
                "status": "skipped",
                "url": url,
                "reason": reason,
                "fetch_function": DYNAMIC_FETCH_FUNCTION,
                "fetch_mode": "dynamic",
                "browser_mode": mode,
                "image_download_required": False,
            }
            for key in ("blocked_url", "blocked_address", "screenshot_file", "screenshot_error"):
                if outcome.get(key):
                    result[key] = outcome[key]
            write_json(json_path, result)
            return result

        attempt = {"mode": mode, "status": status, "reason": reason}
        for key in ("screenshot_file", "screenshot_error"):
            if outcome.get(key):
                attempt[key] = str(outcome[key])
        attempts.append(attempt)
        if index == 0 and headed_retry_delay > 0:
            time.sleep(headed_retry_delay)

    last_attempt = attempts[-1]
    return static_fallback(
        url,
        json_path,
        timeout,
        max_bytes,
        {
            "function": DYNAMIC_FETCH_FUNCTION,
            "status": last_attempt["status"],
            "reason": last_attempt["reason"],
            "attempts": attempts,
        },
    )


def new_context(browser):
    context = browser.new_context(
        ignore_https_errors=True,
        service_workers="block",
        viewport={"width": 1440, "height": 2400},
        user_agent="Mozilla/5.0 SiteScreenshot/1.0",
    )
    guard = install_playwright_ssrf_guard(context)
    setattr(context, "_codex_ssrf_guard", guard)
    return context


def main() -> int:
    parser = argparse.ArgumentParser(description="Render one website page and save screenshot plus page JSON.")
    parser.add_argument("domain", nargs="?", help="website domain or URL, e.g. yoostone.com or https://yoostone.com/")
    parser.add_argument("--screenshot-dir", help="directory to write the screenshot")
    parser.add_argument("--screenshot-file", help="optional screenshot file path; overrides default file name")
    parser.add_argument("--json-dir", help="directory to write the page JSON")
    parser.add_argument("--json-file", help="optional JSON file path; overrides default file name")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-bytes", type=int, default=20 * 1024 * 1024)
    parser.add_argument(
        "--screenshot-format", choices=("jpeg", "png"), default="jpeg",
        help="未指定截图文件名时使用的格式；默认jpeg",
    )
    parser.add_argument(
        "--screenshot-quality", type=int, choices=range(1, 101), default=70,
        help="JPEG截图质量1-100；PNG时忽略",
    )
    args = parser.parse_args()

    if not args.domain:
        parser.error("单页模式需要 domain 参数")
    if not args.screenshot_dir or not args.json_dir:
        parser.error("单页模式需要 --screenshot-dir 和 --json-dir")

    url = args.domain.strip()
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    domain = urlsplit(url).hostname or args.domain

    reason = skip_reason(url)
    if reason:
        print(f"{now_stamp()} {TASK_LABEL} {domain} 跳过:{reason}", flush=True)
        return 0

    screenshot_dir = Path(args.screenshot_dir)
    json_dir = Path(args.json_dir)
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".jpg" if args.screenshot_format == "jpeg" else ".png"
    screenshot_path = resolve_output_path(
        screenshot_dir, args.screenshot_file, default_name(url, suffix)
    )
    json_path = resolve_output_path(json_dir, args.json_file, default_name(url, ".json"))
    screenshot_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            outcome = crawl_with_browser_retry(
                playwright,
                url,
                screenshot_path,
                json_path,
                args.timeout,
                args.max_bytes,
                args.screenshot_quality,
            )
            if outcome["status"] == "skipped":
                print(
                    f"{now_stamp()} {TASK_LABEL} {domain} 跳过:{outcome['reason']}",
                    flush=True,
                )
                return 0
            if outcome["status"] != "ok":
                reason = outcome.get("reason") or outcome.get("error") or "官网抓取失败"
                print(f"{now_stamp()} {TASK_LABEL} {domain} 失败 {reason}", file=sys.stderr, flush=True)
                return 2 if outcome["status"] == "timeout" else 1
        print(
            f"{now_stamp()} {TASK_LABEL} {domain} 成功 "
            f"status_code={outcome.get('status_code')} "
            f"function={outcome.get('fetch_function')} final_url={outcome.get('fetched_url')} {json_path}",
            flush=True,
        )
        return 0
    except Exception as exc:
        outcome = static_fallback(
            url,
            json_path,
            args.timeout,
            args.max_bytes,
            {"function": DYNAMIC_FETCH_FUNCTION, "status": "error", "reason": str(exc)[:500]},
        )
        if outcome["status"] == "skipped":
            print(f"{now_stamp()} {TASK_LABEL} {domain} 跳过:{outcome.get('reason')}", flush=True)
            return 0
        if outcome["status"] == "ok":
            print(
                f"{now_stamp()} {TASK_LABEL} {domain} 成功 "
                f"status_code={outcome.get('status_code')} "
                f"function={outcome.get('fetch_function')} final_url={outcome.get('fetched_url')} {json_path}",
                flush=True,
            )
            return 0
        reason = outcome.get("reason") or outcome.get("error") or str(exc)
        print(f"{now_stamp()} {TASK_LABEL} {domain} 失败 {reason}", file=sys.stderr, flush=True)
        return 2 if outcome["status"] == "timeout" else 1


if __name__ == "__main__":
    raise SystemExit(main())
