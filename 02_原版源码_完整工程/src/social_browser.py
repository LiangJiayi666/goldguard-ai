from __future__ import annotations
"""三平台共用浏览器/登录生命周期。

launch_browser / new_context 在抖音/小红书/快手是结构相同的近副本，差异全部是刻意的
反爬策略：抖音用 route 拦截风控资源、刻意不注入 stealth（注入假指纹反而触发 verify_check）；
小红书/快手注入各自 STEALTH_JS，小红书额外设 timezone_id=Asia/Shanghai。
这里用参数表显式保留这些差异，绝不合并成一套统一指纹。
check_login_required 依赖各平台特有的登录态检测（含 login_state 文本、URL 判断），
耦合过深，留在各自 step1 脚本。
"""


import json
from typing import Any

from login_common import USER_AGENT


def launch_browser(p, headless: bool, *, channel: str | None = None,
                   extra_args: tuple[str, ...] = ()) -> Any:
    """启动浏览器。channel 留作可选（当前无调用方传它，统一走 Playwright 自带 chromium）；
    extra_args 为平台专属启动参数。

    保持页面 1440x900 视口不变；仅把非登录任务的有头窗口移出桌面，避免遮挡用户操作。
    登录刷新脚本不复用此函数，仍正常显示二维码。
    """
    args = list(extra_args)
    if not headless:
        args.append("--window-position=-32000,-32000")
    kwargs: dict[str, Any] = {"headless": headless, "args": args}
    if channel:
        kwargs["channel"] = channel
    return p.chromium.launch(**kwargs)


def new_context(browser, *, cookies_file: Any, inject_stealth: bool = True,
                timezone_id: str | None = None,
                block_patterns: tuple[str, ...] = (), stealth_js: str = "") -> Any:
    """为每家企业创建独立 context，隔离指纹/缓存，降低反爬识别概率。

    cookies_file 必传（登录态）。inject_stealth=False 用于抖音（真 Chrome 不注入假指纹）；
    timezone_id 仅小红书设置；block_patterns 用于抖音 route 拦截风控资源。
    """
    kwargs: dict[str, Any] = {
        "user_agent": USER_AGENT,
        "viewport": {"width": 1440, "height": 900},
        "locale": "zh-CN",
    }
    if timezone_id:
        kwargs["timezone_id"] = timezone_id
    context = browser.new_context(**kwargs)
    if inject_stealth and stealth_js:
        context.add_init_script(stealth_js)
    context.add_cookies(json.loads(cookies_file.read_text(encoding="utf-8")))
    if block_patterns:
        context.route(
            "**/*",
            lambda route: route.abort()
            if any(pat in route.request.url for pat in block_patterns)
            else route.continue_(),
        )
    return context
