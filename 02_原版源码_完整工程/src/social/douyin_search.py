from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""抖音 第1步：按关键词搜索账号，输出账号列表 JSON 到文件。

主路径：/search/<kw>?type=user 真实用户搜索页解析卡片；
备用路径：综合搜索API + 视频作者推导（风控环境下主路径常不可用）。
要点：真Chrome启动、route拦截风控SDK资源、API不带X-Bogus/msToken。

用法:
  .venv/Scripts/python.exe douyin_search.py "永盛鑫珠宝" \
      [--count 5] [--headless] [--output-dir output] [--output-file xxx.json]
输出: JSON 文件 {source, accounts: [{sec_uid, douyin_id, nickname, fans, works_count, profile_href}, ...]}
"""

import argparse
import json
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

from pipeline_runtime import atomic_result, resolve_output, safe_name
from social_screenshot import capture_page_screenshot
from login_common import USER_AGENT, human_pause, now_stamp, wait_for_any_selector
from social_browser import launch_browser as _launch_browser, new_context as _new_context

ROOT = Path(__file__).parent
COOKIES_FILE = ROOT / "douyin_cookies.json"

TASK_LABEL = "抖音账号搜索"

# 抖音风控SDK会把页面主线程拖死，拦掉这些资源页面才响应；API实测仍正常应答
BLOCK_PATTERNS = (
    "webmssdk", "verifycenter", "captcha", "rmc-",
    "security.zijieapi", "web/r/token", "secsdk",
)


def resolve_output_path(args: argparse.Namespace, keyword: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(keyword)}_douyin_search.json")


def launch_browser(p, headless: bool):
    """Chromium 启动（不依赖系统 Chrome）。原实现用 channel="chrome" 启动真 Chrome，
    对方机器没装系统 Chrome 会直接抛 Executable doesn't exist，抖音搜索/作品全挂；
    改用 Playwright 自带 chromium（与快手同款），不再要求系统装 Chrome。"""
    return _launch_browser(p, headless, extra_args=("--disable-blink-features=AutomationControlled",))


def new_context(browser):
    """为每家企业创建独立 context，隔离指纹/缓存，降低反爬识别概率。"""
    return _new_context(browser, cookies_file=COOKIES_FILE, inject_stealth=False, block_patterns=BLOCK_PATTERNS)


SIGNED_FETCH_JS = """async ({apiPath, params}) => {
    // 不传 msToken（空值触发风控空结果）、不加 X-Bogus（加了被静默吊死）
    const qs = new URLSearchParams(params).toString();
    const url = 'https://www.douyin.com' + apiPath + '?' + qs;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 15000);
    try {
        const resp = await fetch(url, {credentials: 'include', signal: ctrl.signal});
        return await resp.json();
    } finally {
        clearTimeout(timer);
    }
}"""


def signed_fetch(page, api_path: str, params: dict) -> dict:
    last_exc: Exception | None = None
    for _ in range(3):
        try:
            return page.evaluate(SIGNED_FETCH_JS, {"apiPath": api_path, "params": params})
        except Exception as exc:
            if "Execution context was destroyed" not in str(exc):
                raise
            last_exc = exc
            human_pause(2.0, 4.0)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=30000)
            except Exception:
                pass
            human_pause()
    raise last_exc  # type: ignore[misc]


LOGIN_WALL_SELECTORS = (
    'text=登录',
    'text=请完成验证',
)


def check_login_required(page) -> bool:
    """检测抖音登录/验证墙。"""
    if "/login" in page.url.lower():
        return True
    return wait_for_any_selector(page, LOGIN_WALL_SELECTORS)


def search_via_page(page, keyword: str, count: int) -> list[dict]:
    page.goto(f"https://www.douyin.com/search/{keyword}?type=user",
              wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_load_state("load", timeout=20000)
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    human_pause(2.0, 4.0)
    if check_login_required(page):
        return []  # 由外层判断后返回 LOGIN_REQUIRED
    try:
        page.wait_for_selector('a[href*="/user/"]', timeout=30000)
    except Exception:
        return []
    human_pause(1.5, 3.0)
    cards = page.evaluate(
        """() => {
            const seen = new Set();
            const out = [];
            document.querySelectorAll('a[href*="/user/"]').forEach(a => {
                if (a.closest('nav, aside, [class*=sidebar], [class*=SideBar], [class*=left-bar]')) return;
                const href = a.getAttribute('href') || '';
                const m = href.match(/\\/user\\/([\\w-]+)/);
                if (!m || seen.has(m[1])) return;
                seen.add(m[1]);
                const text = (a.innerText || '').trim();
                if (!text) return;
                const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
                if (!lines.length || lines[0] === '我的') return;
                out.push({
                    sec_uid: m[1], nickname: lines[0],
                    raw_stats_text: lines.slice(1).join(' | '),
                    profile_href: href.startsWith('http') ? href : 'https://www.douyin.com' + href,
                });
            });
            return out;
        }"""
    )
    for c in cards:
        c.update({"douyin_id": None, "signature": None, "fans": None, "works_count": None})
    return cards[:count]


def search_via_api(page, keyword: str, count: int) -> list[dict]:
    body = signed_fetch(page, "/aweme/v1/web/general/search/single/", {
        "device_platform": "webapp", "aid": "6383", "channel": "channel_pc_web",
        "search_channel": "aweme_user_web", "enable_history": "1",
        "keyword": keyword, "search_source": "tab_search",
        "query_correct_type": "1", "is_filter_search": "0",
        "offset": "0", "count": "15", "need_filter_settings": "1",
        "list_type": "multi",
    })
    users: dict[str, dict] = {}
    for item in body.get("data") or []:
        author = ((item.get("aweme_info") or {}).get("author")) or {}
        sec_uid = author.get("sec_uid")
        if sec_uid and sec_uid not in users:
            users[sec_uid] = {
                "sec_uid": sec_uid,
                "douyin_id": author.get("unique_id") or author.get("short_id"),
                "nickname": author.get("nickname"),
                "signature": author.get("signature"),
                "fans": author.get("follower_count"),
                "works_count": author.get("aweme_count"),
                "profile_href": f"https://www.douyin.com/user/{sec_uid}",
            }
    return list(users.values())[:count]


def run_one(
    browser, keyword: str, count: int, output_path: Path,
    screenshot_file: str | Path | None = None,
) -> dict:
    """在已启动的 browser 内用独立 context 跑一家企业。"""
    context = new_context(browser)
    try:
        page = context.new_page()
        users = search_via_page(page, keyword, count)
        source = "type=user页面"
        if check_login_required(page):
            return atomic_result({"status": "login_required", "output_file": str(output_path), "count": 0, "source": source}, source="douyin.search.login_check")
        if not users:
            source = "综合搜索API作者推导"
            page.goto(f"https://www.douyin.com/search/{keyword}",
                      wait_until="domcontentloaded", timeout=60000)
            human_pause()
            if check_login_required(page):
                return atomic_result({"status": "login_required", "output_file": str(output_path), "count": 0, "source": source}, source="douyin.search.login_check")
            users = search_via_api(page, keyword, count)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"source": source, "accounts": users, "search_url": page.url}
        capture_page_screenshot(page, screenshot_file, payload)
        result = atomic_result(payload, source="douyin.search")
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return atomic_result({"status": "ok", "output_file": str(output_path), "count": len(users), "source": source}, source="douyin.search")
    except Exception as exc:
        return atomic_result({"status": "error", "output_file": str(output_path), "count": 0, "error": str(exc)[:500]}, source="douyin.search.exception", error_code="process_error")
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
