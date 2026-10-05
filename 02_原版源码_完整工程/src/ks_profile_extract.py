from __future__ import annotations

from typing import Any


PROFILE_DETAIL_URL_PREFIX = "https://www.kuaishou.com/short-video/"


def capture_profile_responses(page) -> dict[str, Any]:
    """挂 response 监听，捕获 profile/get 与 profile/feed 的 JSON 响应。

    快手主页的作品列表走 API 分页；review 与 posts 两个脚本共用。
    """
    captured: dict[str, Any] = {"profile": None, "feeds": []}

    def on_response(response) -> None:
        url = response.url
        if "/rest/v/profile/get" not in url and "/rest/v/profile/feed" not in url:
            return
        try:
            data = response.json()
        except Exception:
            return
        if not isinstance(data, dict):
            return
        if "/rest/v/profile/get" in url:
            captured["profile"] = data
        else:
            captured["feeds"].append(data)

    page.on("response", on_response)
    return captured


def extract_dom_items(page) -> list[dict[str, Any]]:
    """API 未返回时退回 DOM 解析：抓主页可见作品卡片。"""
    items = page.evaluate(
        """(() => {
            const seen = new Set();
            const out = [];
            const add = (payload) => {
                if (!payload) return;
                const key = payload.item_id || payload.photo_id || payload.video_id || payload.href || payload.title;
                if (!key || seen.has(key)) return;
                seen.add(key);
                out.push(payload);
            };
            const anchors = Array.from(document.querySelectorAll('a[href]'));
            for (const anchor of anchors) {
                const href = anchor.href || anchor.getAttribute('href') || '';
                if (!href) continue;
                let url;
                try {
                    url = new URL(href, window.location.href);
                } catch (_) {
                    continue;
                }
                if (!/(^|\\.)kuaishou\\.com$/i.test(url.hostname)) continue;
                // Navigation links such as /live and /live/match are not posts.
                // Keep only canonical work-detail links with a concrete item id.
                const match = url.pathname.match(/^\\/(?:short-video|photo|video)\\/([^\\/?#\\/]+)\\/?$/i);
                if (!match) continue;
                const text = (anchor.innerText || anchor.textContent || '').trim();
                const itemId = match[1];
                add({
                    item_id: itemId,
                    photo_id: itemId,
                    video_id: itemId,
                    href,
                    title: text.slice(0, 120),
                });
            }
            return out;
        })()"""
    )
    return [item for item in items if isinstance(item, dict)]


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def extract_visible_profile(page, expected_author_id: str) -> dict[str, Any]:
    """Extract identity evidence rendered by the requested Kuaishou profile page.

    The profile API can occasionally return the currently signed-in user while
    the browser is visibly on the requested account.  The page URL by itself is
    not enough evidence because an error page may retain that URL, so require the
    visible profile metadata as well.
    """
    expected = _text(expected_author_id)
    if not expected:
        return {}
    try:
        raw = page.evaluate(
            """(expectedId) => {
                let parsedUrl;
                try {
                    parsedUrl = new URL(window.location.href);
                } catch (_) {
                    return {};
                }
                const pathMatch = parsedUrl.pathname.match(/^\\/profile\\/([^\\/?#\\/]+)\\/?$/i);
                const pathAccountId = pathMatch ? decodeURIComponent(pathMatch[1]) : '';
                const requestedProfile = Boolean(
                    pathAccountId
                    && String(pathAccountId).toLowerCase() === String(expectedId).toLowerCase(),
                );
                const bodyText = String((document.body && document.body.innerText) || '').replace(/\\r/g, '');
                const lines = bodyText.split('\\n').map(line => line.trim()).filter(Boolean);
                // Some page variants use an icon or a different punctuation
                // character between the label and the account number. Accept a
                // single non-word delimiter instead of requiring one colon.
                const numberIndex = lines.findIndex(line => /^快手号\\s*(?:[^\\w\\s]\\s*)?[\\w-]+/.test(line));
                const numberLine = numberIndex >= 0 ? lines[numberIndex] : '';
                const numberMatch = numberLine.match(/^快手号\\s*(?:[^\\w\\s]\\s*)?([\\w-]+)/);
                let username = '';
                if (numberIndex > 0) {
                    for (let index = numberIndex - 1; index >= 0; index -= 1) {
                        const candidate = lines[index];
                        if (!/^(关注|粉丝|获赞|性别)/.test(candidate)) {
                            username = candidate;
                            break;
                        }
                    }
                }
                const loginWall = /(?:手机号登录|扫码登录|请完成验证|登录后)/.test(bodyText);
                return {
                    profile_url: parsedUrl.href,
                    profile_page_confirmed: Boolean(
                        requestedProfile && username && numberMatch && !loginWall,
                    ),
                    username,
                    visible_kwai_number: numberMatch ? numberMatch[1] : '',
                };
            }""",
            expected,
        )
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    return _compact(
        {
            "profile_url": _text(raw.get("profile_url")),
            "profile_page_confirmed": bool(raw.get("profile_page_confirmed")),
            "username": _text(raw.get("username")),
            "visible_kwai_number": _text(raw.get("visible_kwai_number")),
        }
    )


def _int_or_none(value: Any) -> int | None:
    text = _text(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _compact(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value is not None}


def item_key(item: dict[str, Any]) -> str:
    # 公共作品键：供 kuaishou_review 与 kuaishou_posts 共用。
    return str(item.get("item_id") or item.get("photo_id") or item.get("video_id") or item.get("href") or "")


def extract_profile_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return _compact(
        {
            "eid": _text(payload.get("eid")),
            "user_id": _text(payload.get("userId")),
            "user_define_id": _text(payload.get("userDefineId")),
            "username": _text(payload.get("userName")),
            "signature": _text(payload.get("userTex")),
            "avatar_url": _text(payload.get("userHead")),
            "sex": _text(payload.get("sex")),
            "fans": _int_or_none(payload.get("fans")),
            "follows": _int_or_none(payload.get("follows")),
            "likes": _int_or_none(payload.get("like")),
        }
    )


def reconcile_profile_summary(
    profile: dict[str, Any], items: list[dict[str, Any]], expected_author_id: str,
    visible_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Keep profile identity tied to the requested account, not the logged-in user.

    Kuaishou's ``/rest/v/profile/get`` response can describe the current login
    while the profile feed correctly describes the requested account.  Feed
    author fields are therefore the identity authority when their id matches
    the requested Kuaishou id.  Login-user-only counters and biography fields
    are deliberately discarded on a mismatch instead of producing a mixed
    profile.
    """
    expected = _text(expected_author_id)
    if not expected:
        return dict(profile)

    page_profile = visible_profile if isinstance(visible_profile, dict) else {}
    page_confirmed = bool(page_profile.get("profile_page_confirmed"))
    profile_ids = {
        _text(profile.get(field))
        for field in ("eid", "kwai_id", "user_id", "user_define_id")
        if _text(profile.get(field))
    }
    author = next(
        (
            item for item in items
            if isinstance(item, dict) and _text(item.get("author_id")) == expected
        ),
        None,
    )
    if author is None:
        # A mismatched /profile/get result describes the signed-in user.  When
        # the actual requested profile page is confirmed in the DOM, do not
        # leak that unrelated login identity into review or risk analysis.
        if page_confirmed and expected not in profile_ids:
            return _compact(
                {
                    "kwai_id": expected,
                    "username": _text(page_profile.get("username")),
                    "visible_kwai_number": _text(page_profile.get("visible_kwai_number")),
                    "identity_source": "visible_profile_page",
                }
            )
        return dict(profile)

    if expected not in profile_ids:
        return _compact(
            {
                "eid": expected,
                "username": _text(author.get("author_name")),
                "avatar_url": _text(author.get("author_avatar")),
                "identity_source": "feed_author",
            }
        )

    reconciled = dict(profile)
    author_name = _text(author.get("author_name"))
    author_avatar = _text(author.get("author_avatar"))
    if author_name:
        reconciled["username"] = author_name
    if author_avatar:
        reconciled["avatar_url"] = author_avatar
    return reconciled


def _first_media_url(photo_urls: Any) -> tuple[str, str]:
    if not isinstance(photo_urls, list):
        return "", ""
    for entry in photo_urls:
        if not isinstance(entry, dict):
            continue
        url = _text(entry.get("url"))
        if url:
            return url, _text(entry.get("cdn"))
    return "", ""


def _extract_tags(feed: dict[str, Any]) -> list[str]:
    tags = feed.get("tags")
    if not isinstance(tags, list):
        return []
    out: list[str] = []
    for tag in tags:
        if not isinstance(tag, dict):
            continue
        name = _text(tag.get("name"))
        if name:
            out.append(name)
    return out


def extract_profile_feed_item(feed: Any) -> dict[str, Any] | None:
    if not isinstance(feed, dict):
        return None

    photo = feed.get("photo")
    if not isinstance(photo, dict):
        return None

    photo_id = _text(photo.get("id") or feed.get("photoId") or feed.get("videoId") or feed.get("id"))
    if not photo_id:
        return None

    author = feed.get("author")
    if not isinstance(author, dict):
        author = {}

    manifest = photo.get("manifest")
    if not isinstance(manifest, dict):
        manifest = {}

    caption = _text(photo.get("caption"))
    media_url, media_cdn = _first_media_url(photo.get("photoUrls"))
    detail_url = f"{PROFILE_DETAIL_URL_PREFIX}{photo_id}"

    return _compact(
        {
            "item_id": photo_id,
            "photo_id": photo_id,
            "video_id": _text(manifest.get("videoId") or photo_id),
            "href": detail_url,
            "url": detail_url,
            "title": caption[:120],
            "caption": caption,
            "author_id": _text(author.get("id")),
            "author_name": _text(author.get("name")),
            "author_avatar": _text(author.get("headerUrl")),
            "cover_url": _text(photo.get("coverUrl") or photo.get("animatedCoverUrl")),
            "media_url": media_url,
            "media_cdn": media_cdn,
            "like_count": _int_or_none(photo.get("likeCount")),
            "view_count": _int_or_none(photo.get("viewCount")),
            "timestamp": _int_or_none(photo.get("timestamp")),
            "duration_ms": _int_or_none(photo.get("duration")),
            "feed_type": _text(feed.get("type")),
            "tags": _extract_tags(feed),
        }
    )


def extract_profile_feed_items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    feeds = payload.get("feeds")
    if not isinstance(feeds, list):
        return []
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for feed in feeds:
        item = extract_profile_feed_item(feed)
        if not item:
            continue
        key = _text(item.get("item_id") or item.get("photo_id") or item.get("video_id") or item.get("href"))
        if not key or key in seen:
            continue
        seen.add(key)
        items.append(item)
    return items


def extract_profile_feed_batch(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"items": [], "cursor": "", "has_more": False}

    cursor = _text(payload.get("pcursor") or payload.get("next_cursor") or payload.get("cursor"))
    has_more = cursor not in {"", "no_more"}
    return {
        "items": extract_profile_feed_items(payload),
        "cursor": cursor,
        "has_more": has_more,
    }
