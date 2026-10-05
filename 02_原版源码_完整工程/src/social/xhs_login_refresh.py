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

from playwright.sync_api import sync_playwright

from login_common import USER_AGENT, human_pause, now_stamp, run_login_refresh
from xhs_login_state import has_login_state_lost_text

ROOT = Path(__file__).parent
COOKIES_FILE = ROOT / "xhs_cookies.json"
TASK_LABEL = "xhs login refresh"
DOMAIN = "https://www.xiaohongshu.com"
SOURCE = "xhs.login_refresh"
ENTRY_URL = "https://www.xiaohongshu.com/login"
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
window.chrome = {runtime: {}};
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
"""


def is_login_wall(page) -> bool:
    if has_login_state_lost_text(page):
        return True
    selectors = [
        'text=登录',
        'text=登录 / 注册',
        '[class*="login"]:visible',
        '[class*="mask"]:has([class*="login"]):visible',
        '.r-captcha-modal:visible',
        '.fe-captcha-app:visible',
        '[class*="captcha"]:visible',
        'img[src*="qrcode"]:visible',
    ]
    for selector in selectors:
        try:
            page.wait_for_selector(selector, timeout=3000)
            return True
        except Exception:
            pass
    return False


# 会话级 cookie（扫码后才设、页面刷新不轮转）；指纹只跟它们，避开每次请求轮转的 cookie。
LOGIN_COOKIE_NAMES = ("x-user-id", "web_session")


SEARCH_KEYWORD = "珠宝"


def _search_real_users(page) -> int:
    """登录态功能探针，返回 >0 真实账号数 / 0 命中登录墙(判定未登录) / -1 不确定(不据此失效)。"""
    try:
        page.goto(f"https://www.xiaohongshu.com/search_result?keyword={SEARCH_KEYWORD}",
                  wait_until="domcontentloaded", timeout=60000)
    except Exception:
        return 0 if is_login_wall(page) else -1
    if "/login" in page.url.lower():
        return 0
    human_pause(2.0, 3.0)
    if is_login_wall(page):
        return 0
    try:
        page.click('div.channel:has-text("用户")')
    except Exception:
        return 0 if is_login_wall(page) else -1
    try:
        page.wait_for_function(
            """() => {
                const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
                const s = (window.__INITIAL_STATE__ || {}).search || {};
                const ul = uw(s.userLists);
                const st = uw(s.fetchUserListsStatus);
                return (Array.isArray(ul) && ul.length > 0) || st === 'success' || st === 'error';
            }""",
            timeout=30000,
        )
    except Exception:
        return 0 if is_login_wall(page) else -1
    human_pause()
    if is_login_wall(page):
        return 0
    try:
        count = page.evaluate(
            """(() => {
                const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
                const list = uw((window.__INITIAL_STATE__ || {}).search.userLists) || [];
                return list.filter(raw => {
                    const item = uw(raw) || {};
                    return item.userid || item.user_id || item.id;
                }).length;
            })()"""
        )
    except Exception:
        return 0 if is_login_wall(page) else -1
    if count:
        return int(count)
    return 0 if is_login_wall(page) else -1


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
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-site-isolation-trials",
            ]
            qr_browser = p.chromium.launch(headless=False, args=launch_args)
            probe_browser = p.chromium.launch(
                headless=False,
                args=[*launch_args, "--window-position=-32000,-32000"],
            )
            try:
                context = qr_browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1440, "height": 900},
                    locale="zh-CN",
                    timezone_id="Asia/Shanghai",
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
                        timezone_id="Asia/Shanghai",
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
