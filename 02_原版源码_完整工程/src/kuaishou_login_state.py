from __future__ import annotations
"""Shared, conservative login-wall detection for Kuaishou browser pages."""


# Do not use the generic ``text=登录`` selector here.  Kuaishou may render a
# normal login entry on an otherwise usable search page, so that selector
# turns a healthy session into a false LOGIN_REQUIRED result.  These signals
# are specific to the login/verification wall used by the site.
LOGIN_WALL_SELECTORS = (
    "text=登录即可享受",
    'text="手机号登录"',
    'text="手机号码登录"',
    'text="扫码登录"',
    'text="立即登录"',
    "text=请完成验证",
    'input[placeholder*="手机号"]',
)


def check_kuaishou_login_required(page, *, timeout: float = 3000) -> bool:
    """Return True only when the current page has a definitive login wall.

    ``timeout`` is a total budget shared by all selectors, rather than a
    per-selector timeout.  This keeps the healthy-page path fast while still
    allowing a newly rendered login dialog to be detected.
    """
    try:
        if "/login" in str(page.url or "").lower():
            return True
    except Exception:
        pass

    per_selector_timeout = max(100.0, float(timeout) / len(LOGIN_WALL_SELECTORS))
    for selector in LOGIN_WALL_SELECTORS:
        try:
            page.wait_for_selector(selector, timeout=per_selector_timeout)
            return True
        except Exception:
            continue
    return False
