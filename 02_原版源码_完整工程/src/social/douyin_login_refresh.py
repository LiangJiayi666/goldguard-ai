from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright

from login_common import USER_AGENT, human_pause, now_stamp, run_login_refresh
from douyin_search import signed_fetch

ROOT = Path(__file__).parent
COOKIES_FILE = ROOT / "douyin_cookies.json"
TASK_LABEL = "douyin login refresh"
DOMAIN = "https://www.douyin.com"
SOURCE = "douyin.login_refresh"
ENTRY_URL = f"https://www.douyin.com/search/{quote('珠宝')}"
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
window.chrome = {runtime: {}};
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
"""


# 会话级 cookie（扫码后才设、页面刷新不轮转）；指纹只跟它们，避开 passport_csrf_token 这类每次请求轮转的 cookie。
LOGIN_COOKIE_NAMES = ("sessionid", "sessionid_ss")


SEARCH_KEYWORD = "珠宝"


def _search_real_users(page) -> int:
    """登录态功能探针，返回 >0 真实账号数 / 0 命中登录墙(判定未登录) / -1 不确定(不据此失效)。"""
    try:
        page.goto(f"https://www.douyin.com/search/{quote(SEARCH_KEYWORD)}?type=user",
                  wait_until="domcontentloaded", timeout=60000)
    except Exception:
        return -1
    if "/login" in page.url.lower():
        return 0
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    human_pause(2.0, 4.0)
    # 主路径：type=user 页面 DOM 卡片（step1 实测最稳）。
    try:
        page.wait_for_selector('a[href*="/user/"]', timeout=20000)
        cards = page.evaluate(
            """() => {
                const seen = new Set();
                document.querySelectorAll('a[href*="/user/"]').forEach(a => {
                    if (a.closest('nav, aside, [class*=sidebar], [class*=SideBar], [class*=left-bar]')) return;
                    const m = (a.getAttribute('href') || '').match(/\\/user\\/([\\w-]+)/);
                    if (m && !seen.has(m[1]) && (a.innerText || '').trim()) seen.add(m[1]);
                });
                return seen.size;
            }"""
        )
    except Exception:
        cards = 0
    if cards:
        return int(cards)
    # 备用：综合搜索 API。先回一般搜索页（同 step1，同源同 Referer），再取作者。
    try:
        page.goto(f"https://www.douyin.com/search/{quote(SEARCH_KEYWORD)}",
                  wait_until="domcontentloaded", timeout=60000)
        body = signed_fetch(page, "/aweme/v1/web/general/search/single/", {
            "device_platform": "webapp", "aid": "6383", "channel": "channel_pc_web",
            "search_channel": "aweme_user_web", "enable_history": "1",
            "keyword": SEARCH_KEYWORD, "search_source": "tab_search",
            "query_correct_type": "1", "is_filter_search": "0",
            "offset": "0", "count": "15", "need_filter_settings": "1", "list_type": "multi",
        })
        n = sum(1 for item in (body.get("data") or [])
                if ((item.get("aweme_info") or {}).get("author") or {}).get("sec_uid"))
    except Exception:
        return -1
    return n if n else -1


def verify_login_by_search(page) -> tuple[bool, int]:
    """返回 (是否登录有效, 真实账号数)。拿到真实账号才算有效；否则 (False, 0)。
    二元判定：main_agent 按返回码累计失败/BLOCK，故"不确定"也判无效——避免死登录刷不出 BLOCK。"""
    for _ in range(2):
        n = _search_real_users(page)
        if n > 0:
            return True, n
        human_pause(3.0, 5.0)
    return False, 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expect-login", action="store_true", help="pipeline 检测到 login_required 后调用（非交互）")
    parser.add_argument("--interactive-wait", action="store_true",
                        help="由 main_agent 调用时保持二维码页并持续等待扫码")
    args = parser.parse_args()

    if not COOKIES_FILE.is_file():
        COOKIES_FILE.write_text("[]", encoding="utf-8")

    interactive = args.interactive_wait or (sys.stdin.isatty() and not args.expect_login)
    print(f"{now_stamp()} {TASK_LABEL} 开始（{'交互' if interactive else '管线/非交互'}）", flush=True)

    try:
        with sync_playwright() as p:
            qr_browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
            probe_browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--window-position=-32000,-32000"],
            )
            try:
                context = qr_browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1440, "height": 900},
                    locale="zh-CN",
                )
                context.add_init_script(STEALTH_JS)
                try:
                    context.add_cookies(json.loads(COOKIES_FILE.read_text(encoding="utf-8")))
                except Exception as exc:
                    print(f"{now_stamp()} {TASK_LABEL} cookie 文件异常: {exc}", file=sys.stderr, flush=True)

                qr_page = context.new_page()
                qr_page.goto(ENTRY_URL, wait_until="domcontentloaded", timeout=60000)
                human_pause()

                def make_probe_context():
                    ctx = probe_browser.new_context(
                        user_agent=USER_AGENT,
                        viewport={"width": 1440, "height": 900},
                        locale="zh-CN",
                    )
                    ctx.add_init_script(STEALTH_JS)
                    ctx.add_cookies(context.cookies(DOMAIN))
                    return ctx

                rc = run_login_refresh(
                    context, qr_page,
                    entry_url=ENTRY_URL,
                    verify_fn=verify_login_by_search, cookies_file=COOKIES_FILE,
                    domain=DOMAIN, source=SOURCE, task_label=TASK_LABEL, interactive=interactive,
                    login_cookie_names=LOGIN_COOKIE_NAMES,
                    make_probe_context=make_probe_context,
                )
            finally:
                probe_browser.close()
                qr_browser.close()
            return rc
    except Exception as exc:
        print(f"{now_stamp()} {TASK_LABEL} 失败 {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
