from __future__ import annotations

from pathlib import Path
from typing import Any


def capture_page_screenshot(
    page: Any,
    screenshot_file: str | Path | None,
    payload: dict[str, Any] | None = None,
    *,
    quality: int = 80,
) -> str | None:
    """Save a full-page browser screenshot and attach its provenance to payload.

    截图是辅助证据，绝不能让整个任务陪葬：页面字体/资源不加载时
    Playwright 会等到 30s 超时（曾导致数据已抓到、任务却被判失败重试）。
    失败时降级为视口截图，再失败则记录 screenshot_error 并返回 None。
    """
    if not screenshot_file:
        return None
    path = Path(screenshot_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    options: dict[str, Any] = {"path": str(path), "full_page": True, "timeout": 15000}
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        options.update(type="jpeg", quality=quality)
    try:
        page.screenshot(**options)
    except Exception as full_page_exc:
        try:
            fallback = dict(options, full_page=False, timeout=8000)
            page.screenshot(**fallback)
        except Exception as fallback_exc:
            if payload is not None:
                payload["screenshot_error"] = (
                    f"full_page: {str(full_page_exc)[:160]}; "
                    f"viewport: {str(fallback_exc)[:160]}"
                )
            return None
    if payload is not None:
        payload["screenshot_file"] = str(path)
        payload["screenshot_url"] = str(getattr(page, "url", "") or "")
    return str(path)
