from __future__ import annotations
"""社媒脚本公共层：三平台 step1 搜索与 login_refresh 共享的 UA、时间戳、停顿与 cookie 回写。

wait_for_login / 登录墙检测 / STEALTH_JS / launch 配置因三平台控制流差异大，留在各自脚本；
这里只收敛逐字相同的小工具与 cookie 回写（cookie expires 修复也落在这里）。
"""


import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

from pipeline_runtime import atomic_result


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def human_pause(low: float = 0.5, high: float = 1.5) -> None:
    time.sleep(random.uniform(low, high))


def wait_for_any_selector(page, selectors, timeout: float = 3000) -> bool:
    """Return True when any selector appears; shared login/rate-wall detection.

    三平台的登录墙/风控文案各不相同，selector 列表留在各平台脚本；
    这里只收敛逐条 try/except 的样板，避免三份复制漂移。
    """
    for selector in selectors:
        try:
            page.wait_for_selector(selector, timeout=timeout)
            return True
        except Exception:
            continue
    return False


def print_atomic_result(payload: dict, source: str) -> None:
    print(json.dumps(atomic_result(payload, source=source), ensure_ascii=False), flush=True)


def save_cookies(context, cookies_file: Path, domain: str) -> None:
    """全量回写当前 cookie；会话级 cookie 保留服务端原 expires，不强制续命到 2027。
    原子写（tmp + os.replace），防中断留下截断 JSON 让下次启动降级成空 context。"""
    jar = context.cookies(domain)
    out = [
        {
            "name": c["name"], "value": c["value"], "domain": c["domain"],
            "path": c["path"],
            "expires": c.get("expires", -1),
            "secure": c.get("secure", False), "httpOnly": c.get("httpOnly", False),
        }
        for c in jar
    ]
    tmp = cookies_file.with_suffix(cookies_file.suffix + ".tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, cookies_file)


# --- 登录刷新主循环：三平台共享的"弹码 → 轮询 → 另窗验证 → 停泊"流程 ---
# 触发策略：会话级 cookie 变化（快路径，响应用户扫码）OR 定时兜底（防漏），二者 OR 才探针。
# 本地 cookie 采样高频（纯内存读，零联网零风控）；探针联网受 PROBE_MIN_INTERVAL 节流防风控。
STATE_CHECK_INTERVAL = 10.0        # 本地 cookie 签名采样间隔：不联网，高频低延迟捕获扫码
PROBE_MIN_INTERVAL = 180.0         # 探针（联网搜索）最小间隔：兜底节流，与会话 cookie 变化触发 OR
QR_REFRESH = 900.0                 # 仅在 15 分钟后更新一次可能过期的二维码


def _verify_in_new_page(context, verify_fn, make_probe_context=None) -> tuple[bool, int]:
    """另开新页跑验证（不打扰二维码页），完事关闭。

    双浏览器模式下 make_probe_context 提供屏幕外的探针 context（已同步二维码
    context 的最新 cookie），二维码窗口保持可见、不被探针抢前台。未提供时
    退化用二维码 context（向后兼容）。
    """
    if make_probe_context is not None:
        probe_ctx = make_probe_context()
        try:
            return verify_fn(probe_ctx.new_page())
        finally:
            try:
                probe_ctx.close()
            except Exception:
                pass
    page = context.new_page()
    try:
        return verify_fn(page)
    finally:
        try:
            page.close()
        except Exception:
            pass


def _login_state_signature(context, qr_page, domain: str, login_cookie_names=()) -> tuple:
    """A local-only login-state snapshot; no platform request is made here.

    只跟会话级 cookie（login_cookie_names），过滤掉每次请求轮转的干扰 cookie——
    轮转 cookie 变化不触发探针，避免误报和把定时兜底带飞。空名单时退化为全量。
    """
    cookies = context.cookies(domain)
    wanted = {str(name).lower() for name in login_cookie_names} if login_cookie_names else None
    cookie_values = tuple(sorted(
        (str(cookie.get("name") or "").lower(), str(cookie.get("value") or ""),
         str(cookie.get("domain") or ""), str(cookie.get("path") or ""))
        for cookie in cookies
        if wanted is None or str(cookie.get("name") or "").lower() in wanted
    ))
    return (str(qr_page.url or ""), cookie_values)


def _reopen_qr_page(context, entry_url: str, task_label: str):
    """Create a replacement QR page after the user closes the original one."""
    print(f"{now_stamp()} {task_label} 登录页已关闭，正在重新打开二维码页", flush=True)
    page = context.new_page()
    page.goto(entry_url, wait_until="domcontentloaded", timeout=60000)
    human_pause()
    return page


def run_login_refresh(context, qr_page, *, entry_url, verify_fn, cookies_file,
                      domain, source, task_label, interactive,
                      login_cookie_names=(), make_probe_context=None) -> int:
    """统一登录刷新主循环：展示二维码 → 高频本地采 cookie → 探针确认。

    触发探针的判据是 OR：会话级 cookie 变化（用户扫码的快路径）OR 定时兜底
    （PROBE_MIN_INTERVAL，独立计时，不被 cookie 变化重置）。探针（联网搜索）
    是登录成功的唯一真相；拿到真实账号才回写 cookie 返回 0，否则保持二维码页继续轮询。"""
    def _try_verify(tag: str) -> bool:
        print(f"{now_stamp()} {task_label} {tag}，开新搜索页验证", flush=True)
        verified, count = _verify_in_new_page(context, verify_fn, make_probe_context)
        if verified and count > 0:
            save_cookies(context, cookies_file, domain)
            print_atomic_result(
                {"status": "ok", "cookies_file": str(cookies_file), "verified": True, "verified_count": count},
                source,
            )
            print(f"{now_stamp()} {task_label} 登录成功，搜索探针返回 {count} 个真实账号，cookie 已回写到 {cookies_file}", flush=True)
            return True
        print(f"{now_stamp()} {task_label} 验证未通过（{tag}，未拿到真实内容），继续等待扫码", file=sys.stderr, flush=True)
        return False

    # 二维码页已经打开。直接用功能探针检查当前 cookie，不依赖特定 cookie 是否变化。
    if _try_verify("初始 cookie 验证"):
        return 0

    baseline = _login_state_signature(context, qr_page, domain, login_cookie_names)
    next_probe = time.monotonic() + PROBE_MIN_INTERVAL   # 定时兜底：独立计时，不被 cookie 变化重置
    next_qr_refresh = time.monotonic() + QR_REFRESH
    while True:
        time.sleep(STATE_CHECK_INTERVAL)
        now = time.monotonic()
        # Closing just the login tab should not end the refresh session.  Keep
        # the browser context (and its cookies), replace only that QR tab.
        if qr_page.is_closed():
            try:
                qr_page = _reopen_qr_page(context, entry_url, task_label)
                baseline = _login_state_signature(context, qr_page, domain, login_cookie_names)
                next_qr_refresh = now + QR_REFRESH
            except Exception as exc:
                print(f"{now_stamp()} {task_label} 重新打开二维码页失败: {exc}",
                      file=sys.stderr, flush=True)
                continue
        try:
            current = _login_state_signature(context, qr_page, domain, login_cookie_names)
        except Exception as exc:
            print(f"{now_stamp()} {task_label} 本地登录态检测失败: {exc}", file=sys.stderr, flush=True)
            current = baseline
        cookie_changed = current != baseline
        probe_due = now >= next_probe
        # OR：会话 cookie 变化（扫码快路径）OR 定时兜底（防漏）。任一为真才探针（联网）。
        if cookie_changed or probe_due:
            tag = "会话cookie变化" if cookie_changed else "定时兜底"
            if _try_verify(tag):
                return 0
            baseline = _login_state_signature(context, qr_page, domain, login_cookie_names)
            next_probe = now + PROBE_MIN_INTERVAL
        if now >= next_qr_refresh:
            try:
                qr_page.reload(wait_until="domcontentloaded", timeout=15000)
                baseline = _login_state_signature(context, qr_page, domain, login_cookie_names)
            except Exception:
                pass
            next_qr_refresh = now + QR_REFRESH
