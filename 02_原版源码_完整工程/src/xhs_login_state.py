from __future__ import annotations
"""Shared login-state signals for Xiaohongshu browser pages."""



# Xiaohongshu displays this page slogan when the session has fallen back to
# its unauthenticated shell. Treat its presence as a definitive login loss.
LOGIN_STATE_LOST_TEXT = "3 亿人的生活经验，都在小红书"


def has_login_state_lost_text(page) -> bool:
    """Return whether the page body contains Xiaohongshu's login-loss text."""
    try:
        return bool(page.evaluate(
            "(text) => (document.body?.innerText || '').includes(text)",
            LOGIN_STATE_LOST_TEXT,
        ))
    except Exception:
        # Keep the signal usable during partial navigation/error pages where
        # evaluating the document is unavailable but page HTML is accessible.
        try:
            return LOGIN_STATE_LOST_TEXT in page.content()
        except Exception:
            return False
