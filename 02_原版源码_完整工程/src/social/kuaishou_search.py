from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""快手 第1步：按关键词搜索账号，输出账号列表 JSON 到文件。

搜索页切"用户"Tab（页内XHR，非跳转），从 DOM 的 .user-card 解析。

用法:
  .venv/Scripts/python.exe kuaishou_search.py "永盛鑫珠宝" \
      [--count 5] [--headless] [--output-dir output] [--output-file xxx.json]
输出: JSON 文件 [{kwai_id, nickname, desc}, ...]
"""

import argparse
import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

from pipeline_runtime import atomic_result, resolve_output, safe_name
from social_screenshot import capture_page_screenshot
from login_common import USER_AGENT, human_pause, now_stamp
from kuaishou_login_state import check_kuaishou_login_required
from social_browser import launch_browser as _launch_browser, new_context as _new_context

ROOT = Path(__file__).parent
COOKIES_FILE = ROOT / "kuaishou_cookies.json"

TASK_LABEL = "快手账号搜索"

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
window.chrome = {runtime: {}};
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
"""


def resolve_output_path(args: argparse.Namespace, keyword: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(keyword)}_ks_search.json")


def launch_browser(p, headless: bool):
    return _launch_browser(p, headless)


def new_context(browser):
    """为每家企业创建独立 context，隔离指纹/缓存，降低反爬识别概率。"""
    return _new_context(browser, cookies_file=COOKIES_FILE, stealth_js=STEALTH_JS)


def check_login_required(page) -> bool:
    """检测快手登录墙；普通页面上的“登录”入口不算登录态失效。"""
    return check_kuaishou_login_required(page)


def run_one(
    browser, keyword: str, count: int, output_path: Path,
    screenshot_file: str | Path | None = None,
) -> dict:
    """在已启动的 browser 内用独立 context 跑一家企业。"""
    context = new_context(browser)
    try:
        page = context.new_page()
        page.goto(
            f"https://www.kuaishou.com/search/video?searchKey={keyword}",
            wait_until="domcontentloaded", timeout=60000,
        )
        human_pause(4.0, 6.0)
        try:
            page.click('div.tab-item:has-text("用户")')
            human_pause(3.0, 4.5)
            page.wait_for_selector("div.user-card", timeout=30000)
        except Exception:
            # The wall may render after the initial page check.  Reclassify it
            # as login_required only when a definitive shared signal exists;
            # otherwise preserve the original processing error for diagnosis.
            if check_login_required(page):
                return atomic_result(
                    {"status": "login_required", "output_file": str(output_path), "count": 0},
                    source="kuaishou.search.login_check",
                )
            raise
        cards = page.evaluate(
            """(() => {
                const out = [];
                document.querySelectorAll('div.user-card').forEach(card => {
                    const text = card.innerText || '';
                    const name = (card.querySelector('.name') || {}).innerText || '';
                    const m = text.match(/快手号：([\\w]+)/);
                    const descEl = card.querySelector('[class*=desc], [class*=text]');
                    out.push({
                        nickname: name.trim(),
                        kwai_id: m ? m[1] : null,
                        desc: descEl ? descEl.innerText.trim() : '',
                    });
                });
                return out;
            })()"""
        )
        users = [c for c in cards if c.get("kwai_id")][:count]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"accounts": users, "search_url": page.url}
        capture_page_screenshot(page, screenshot_file, payload)
        output_path.write_text(json.dumps(atomic_result(payload, source="kuaishou.search"), ensure_ascii=False, indent=2), encoding="utf-8")
        return atomic_result({"status": "ok", "output_file": str(output_path), "count": len(users)}, source="kuaishou.search")
    except Exception as exc:
        return atomic_result({"status": "error", "output_file": str(output_path), "count": 0, "error": str(exc)[:500]}, source="kuaishou.search.exception", error_code="process_error")
    finally:
        try:
            context.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("keyword", nargs="?", help="搜索关键词（单条模式）")
    parser.add_argument("--count", type=int, default=50, help="最多返回几个账号，50 足够覆盖第一页")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output", help="输出目录")
    parser.add_argument("--output-file", help="输出 JSON 文件名（可选）")
    parser.add_argument("--screenshot-file", help="搜索结果页截图文件")
    args = parser.parse_args()

    if not args.keyword:
        parser.error("单条模式需要 keyword 参数")

    output_path = resolve_output_path(args, args.keyword)
    print(f"{now_stamp()} {TASK_LABEL} {args.keyword} 开始", flush=True)

    try:
        with sync_playwright() as p:
            browser = launch_browser(p, args.headless)
            try:
                result = run_one(browser, args.keyword, args.count, output_path, args.screenshot_file)
                if result["status"] == "login_required":
                    print(f"{now_stamp()} {TASK_LABEL} {args.keyword} LOGIN_REQUIRED 需要重新登录", file=sys.stderr, flush=True)
                    return 100
                if result["status"] == "error":
                    print(f"{now_stamp()} {TASK_LABEL} {args.keyword} 失败 {result['error']}", file=sys.stderr, flush=True)
                    return 1
            finally:
                browser.close()
    except Exception as exc:
        print(f"{now_stamp()} {TASK_LABEL} {args.keyword} 失败 {exc}", file=sys.stderr, flush=True)
        return 1

    print(
        f"{now_stamp()} {TASK_LABEL} {args.keyword} 成功 {output_path} 账号{result['count']}个",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
