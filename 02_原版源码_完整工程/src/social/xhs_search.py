from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""小红书 第1步：按关键词搜索账号，输出账号列表 JSON 到文件。

反爬约定：全程地址栏式新导航，搜索页"用户"Tab为页内XHR（非跳转），
所有请求由站点自己在真实浏览器里发出。
登录态失效时自动引导扫码登录并回写 cookie。

用法:
  .venv/Scripts/python.exe xhs_search.py "永盛鑫珠宝" \
      [--count 5] [--headless] [--output-dir output] [--output-file xxx.json]
输出: JSON 文件 [{user_id, red_id, nickname, fans, note_count, xsec_token}, ...]
"""

import argparse
import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

from pipeline_runtime import atomic_result, resolve_output, safe_name
from social_screenshot import capture_page_screenshot
from login_common import USER_AGENT, now_stamp, save_cookies as _save_cookies, wait_for_any_selector
from login_common import human_pause as _human_pause

# 小红书风控比抖音/快手敏感：操作间等待单独拉长（login_common 默认 0.5-1.5s）。
# xhs_detail / xhs_posts / xhs_review 均从本模块导入 human_pause，改这里一处即可全链路生效。
# 2026-08-19 起按业务要求放慢到 15-30s/操作，兼顾防限流与吞吐。
XHS_PAUSE_LOW = 15.0
XHS_PAUSE_HIGH = 30.0
# 详情页单独的等待区间（比列表页小）：全量正文量级大，详情走轻量等待。
XHS_DETAIL_PAUSE_LOW = 5.0
XHS_DETAIL_PAUSE_HIGH = 10.0


def human_pause(low: float = XHS_PAUSE_LOW, high: float = XHS_PAUSE_HIGH) -> None:
    _human_pause(low, high)
from social_browser import launch_browser as _launch_browser, new_context as _new_context
from xhs_login_state import has_login_state_lost_text

ROOT = Path(__file__).parent
COOKIES_FILE = ROOT / "xhs_cookies.json"

TASK_LABEL = "小红书账号搜索"

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
window.chrome = {runtime: {}};
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
"""


def resolve_output_path(args: argparse.Namespace, keyword: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(keyword)}_xhs_search.json")


def launch_browser(p, headless: bool):
    return _launch_browser(p, headless, extra_args=(
        "--disable-blink-features=AutomationControlled",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-site-isolation-trials",
    ))


def new_context(browser):
    """为每家企业创建独立 context，隔离指纹/缓存，降低反爬识别概率。"""
    return _new_context(browser, cookies_file=COOKIES_FILE, timezone_id="Asia/Shanghai", stealth_js=STEALTH_JS)


def save_cookies(context) -> None:
    """把浏览器当前 cookie 全量回写（登录后调用）。复用 login_common 的原子写实现。"""
    _save_cookies(context, COOKIES_FILE, "https://www.xiaohongshu.com")


LOGIN_WALL_SELECTORS = (
    'text=登录后查看搜索结果',
    'text=请通过验证',
    'text=登录 / 注册',
    'text=登录',
    '[class*="login"]:visible',
    '[class*="mask"]:has([class*="login"]):visible',
    # 小红书在 headless 下常弹出 QR/滑块验证码
    '.r-captcha-modal:visible',
    '.fe-captcha-app:visible',
    '[class*="captcha"]:visible',
    'img[src*="qrcode"]:visible',
)

RATE_LIMIT_SELECTORS = (
    'text=操作过于频繁',
    'text=访问太频繁',
    'text=请稍后再试',
    'text=休息一下',
)


def check_rate_limited(page) -> bool:
    """检测小红书是否提示操作过于频繁。"""
    return wait_for_any_selector(page, RATE_LIMIT_SELECTORS, timeout=2000)


def check_login_required(page) -> bool:
    """检测登录墙或反爬验证码；出现则报告 LOGIN_REQUIRED，由调用方决定是否刷新登录。

    限频弹窗（操作过于频繁/请稍后再试）常带登录入口、也会命中下面的宽松登录选择器，
    但它不是掉登录——此时若立刻弹扫码刷新，反而会被平台判高频把真登录态打掉。
    所以限频优先：限频页面一律不算 login_required，先冷却复核。
    """
    if check_rate_limited(page):
        return False
    if has_login_state_lost_text(page):
        return True
    return wait_for_any_selector(page, LOGIN_WALL_SELECTORS)


def run_one(
    browser, keyword: str, count: int, output_path: Path,
    screenshot_file: str | Path | None = None,
) -> dict:
    """在已启动的 browser 内用独立 context 跑一家企业。"""

    def _rate_limited_result() -> dict:
        # 限频不是崩溃：落盘 result.json 让主控拿到明确的 rate_limited（冷却重试），
        # 而不是 rc=0 + 缺产物的 missing_output。
        result = atomic_result({"status": "rate_limited", "output_file": str(output_path), "count": 0}, source="xhs.search.rate_limit")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result

    context = new_context(browser)
    try:
        page = context.new_page()
        page.goto(
            f"https://www.xiaohongshu.com/search_result?keyword={keyword}",
            wait_until="domcontentloaded", timeout=60000,
        )
        human_pause()
        if check_rate_limited(page):
            return _rate_limited_result()
        if check_login_required(page):
            return atomic_result({"status": "login_required", "output_file": str(output_path), "count": 0}, source="xhs.search.login_check")
        page.click('div.channel:has-text("用户")')
        human_pause()
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
        human_pause()
        if check_rate_limited(page):
            return _rate_limited_result()
        if has_login_state_lost_text(page):
            return atomic_result({"status": "login_required", "output_file": str(output_path), "count": 0}, source="xhs.search.login_check")
        raw = page.evaluate(
            """(() => {
                const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
                const list = uw(window.__INITIAL_STATE__.search.userLists) || [];
                return list.map(raw => {
                    const item = uw(raw) || {};
                    return {
                        user_id: item.userid || item.user_id || item.id,
                        red_id: item.red_id || item.redId,
                        nickname: item.nickname || item.nick_name || item.name,
                        desc: item.desc,
                        fans: item.fans,
                        note_count: item.note_count || item.notes_count || item.noteCount,
                        xsec_token: item.xsec_token || item.xsecToken,
                    };
                });
            })()"""
        )
        users = [u for u in raw if u.get("user_id")][:count]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"accounts": users, "search_url": page.url}
        capture_page_screenshot(page, screenshot_file, payload)
        output_path.write_text(json.dumps(atomic_result(payload, source="xhs.search"), ensure_ascii=False, indent=2), encoding="utf-8")
        return atomic_result({"status": "ok", "output_file": str(output_path), "count": len(users)}, source="xhs.search")
    except Exception as exc:
        return atomic_result({"status": "error", "output_file": str(output_path), "count": 0, "error": str(exc)[:500]}, source="xhs.search.exception", error_code="process_error")
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
                if result["status"] == "rate_limited":
                    print(f"{now_stamp()} {TASK_LABEL} {args.keyword} RATE_LIMITED 操作频繁，先冷却稍后重试", file=sys.stderr, flush=True)
                    return 0
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
