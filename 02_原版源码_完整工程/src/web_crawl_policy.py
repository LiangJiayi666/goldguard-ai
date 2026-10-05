from __future__ import annotations
"""Pure URL, bounded-recursion, and image-dedup policies for website crawling.

The fetchers remain responsible for network access.  This module deliberately
contains no I/O so the control plane can apply the same rules to live results,
retried results, and restored runs.
"""


import posixpath
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


# None 表示无上限：官网 BFS 爬完整站（仍受 URL 去重与同站限制约束）。
DEFAULT_MAX_DEPTH = 6
DEFAULT_MAX_PAGES = 60

# Suffixes that unambiguously describe resources rather than navigable HTML.
# Extensionless URLs and ordinary web-page suffixes (.html/.htm/.php/...) stay
# eligible because many official sites use routes such as /about or /news.do.
NON_PAGE_SUFFIXES = frozenset({
    # Images and fonts.
    ".avif", ".bmp", ".gif", ".ico", ".jpeg", ".jpg", ".png", ".svg",
    ".tif", ".tiff", ".webp", ".woff", ".woff2", ".ttf", ".otf", ".eot",
    # Documents and tabular exports.
    ".csv", ".doc", ".docm", ".docx", ".epub", ".ods", ".odt", ".pdf",
    ".ppt", ".pptm", ".pptx", ".rtf", ".tsv", ".xls", ".xlsm", ".xlsx",
    # Archives, disk images, and executables.
    ".7z", ".apk", ".bz2", ".dmg", ".exe", ".gz", ".iso", ".msi", ".rar",
    ".tar", ".tgz", ".xz", ".zip",
    # Audio/video and non-HTML web assets/data.
    ".aac", ".avi", ".css", ".flac", ".js", ".json", ".m3u8", ".m4a",
    ".m4v", ".mkv", ".mov", ".mp3", ".mp4", ".mpeg", ".mpg", ".ogg",
    ".ogv", ".rss", ".wav", ".webm", ".xml",
})


def _normalized_path(path: str) -> str:
    """Normalize obvious path aliases without decoding reserved characters."""
    collapsed = re.sub(r"/{2,}", "/", path or "/")
    trailing_slash = collapsed.endswith("/")
    normalized = posixpath.normpath(collapsed)
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if trailing_slash and normalized != "/":
        normalized += "/"
    return normalized


def canonicalize_url(raw: object, base_url: str | None = None) -> str | None:
    """Return a stable HTTP(S) URL or ``None`` for an unusable value.

    Stability covers fragment removal, lower-case scheme/IDNA host, trailing
    DNS-dot removal, default-port removal, dot/repeated path segments, and query
    ordering.  Credentials are rejected: official-site crawl targets should not
    carry authority information in URLs.
    """
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        joined = urljoin(base_url or "", value)
        parts = urlsplit(joined)
        scheme = parts.scheme.lower()
        if scheme not in {"http", "https"} or not parts.hostname:
            return None
        if parts.username is not None or parts.password is not None:
            return None

        raw_host = parts.hostname.rstrip(".").lower()
        if not raw_host:
            return None
        if ":" in raw_host:  # urlsplit exposes an IPv6 literal without brackets.
            host = f"[{raw_host}]"
        else:
            host = raw_host.encode("idna").decode("ascii")

        port = parts.port
        if port is not None and not (
            (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
        ):
            host = f"{host}:{port}"

        query_pairs = parse_qsl(parts.query, keep_blank_values=True)
        query = urlencode(sorted(query_pairs, key=lambda item: (item[0], item[1])))
        return urlunsplit((scheme, host, _normalized_path(parts.path), query, ""))
    except (UnicodeError, ValueError):
        return None


def host_variant_url(url: object) -> str | None:
    """Return the alternate ``www``/apex host form of an HTTP(S) URL.

    The registrable apex and its ``www.`` host can serve entirely different
    sites (or only one of them may answer), so crawls should try both forms.
    Subdomains other than ``www`` and non-HTTP(S) or non-host inputs return
    ``None``; sibling subdomains are never synthesized.
    """
    normalized = canonicalize_url(url)
    if not normalized:
        return None
    parts = urlsplit(normalized)
    host = (parts.hostname or "").lower()
    if not host or ":" in host:  # empty host or IPv6 literal
        return None
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host):
        return None
    labels = host.split(".")
    if host.startswith("www."):
        variant_host = host[4:]
    elif len(labels) == 2 or (
        len(labels) == 3
        and labels[-2] in {"com", "net", "org", "gov", "edu", "ac"}
        and labels[-1] == "cn"
    ):
        variant_host = "www." + host
    else:
        return None
    port = parts.port
    netloc = f"{variant_host}:{port}" if port else variant_host
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, ""))


def site_host(url: object) -> str | None:
    """Return a same-site host key, treating only a leading ``www.`` as an alias."""
    normalized = canonicalize_url(url)
    if not normalized:
        return None
    host = (urlsplit(normalized).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def same_site(left: object, right: object) -> bool:
    """Whether two URLs belong to the same host tree.

    A root host and its child subdomains are treated as one site so navigation
    from ``example.com`` to ``shop.example.com`` remains crawlable.  Sibling
    hosts and look-alike suffixes remain isolated: ``a.example.com`` does not
    automatically trust ``b.example.com``, and ``example.com.evil.test`` is
    never considered part of ``example.com``.
    """
    left_host = site_host(left)
    right_host = site_host(right)
    if not left_host or not right_host:
        return False
    return (
        left_host == right_host
        or left_host.endswith("." + right_host)
        or right_host.endswith("." + left_host)
    )


def is_recursive_page_url(url: object, base_url: str | None = None) -> bool:
    """Return whether a URL is a plausible navigable HTTP(S) page."""
    normalized = canonicalize_url(url, base_url)
    if not normalized:
        return False
    suffix = posixpath.splitext(urlsplit(normalized).path)[1].lower()
    return suffix not in NON_PAGE_SUFFIXES


def dedupe_links(links: Iterable[object], base_url: str | None = None) -> list[str]:
    """Normalize links and preserve the first occurrence of each URL."""
    output: list[str] = []
    seen: set[str] = set()
    for raw in links:
        normalized = canonicalize_url(raw, base_url)
        if normalized and normalized not in seen:
            seen.add(normalized)
            output.append(normalized)
    return output


def dedupe_result_links(results: Iterable[Mapping[str, Any]]) -> list[str]:
    """Collect and stably deduplicate ``links`` from page result objects."""
    output: list[str] = []
    seen: set[str] = set()
    for result in results:
        base = str(result.get("fetched_url") or result.get("url") or "") or None
        raw_links = result.get("links")
        if not isinstance(raw_links, (list, tuple)):
            continue
        for normalized in dedupe_links(raw_links, base):
            if normalized not in seen:
                seen.add(normalized)
                output.append(normalized)
    return output


def dedupe_image_candidates(results: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Stably deduplicate image candidates across page results by canonical URL.

    The first candidate wins so scheduling order and its associated alt/size
    metadata remain deterministic.  String candidates are accepted for legacy
    result files, although current fetchers emit dictionaries.
    """
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for result in results:
        base = str(result.get("fetched_url") or result.get("url") or "") or None
        candidates = result.get("image_candidates")
        if not isinstance(candidates, (list, tuple)):
            continue
        for candidate in candidates:
            if isinstance(candidate, Mapping):
                item = dict(candidate)
                raw_url = item.get("url")
            else:
                item = {"url": candidate}
                raw_url = candidate
            normalized = canonicalize_url(raw_url, base)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            item["url"] = normalized
            output.append(item)
    return output


@dataclass(frozen=True, slots=True)
class CrawlTarget:
    """One BFS page target and the edge that discovered it."""

    url: str
    depth: int
    parent_url: str | None = None


@dataclass(slots=True)
class SiteCrawlFrontier:
    """Deterministic, bounded BFS queue for one official-site root.

    The root itself is depth 0 and counts toward ``max_pages``.  URLs are marked
    seen when enqueued, preventing concurrent dispatch of the same target.
    """

    root_url: str
    max_depth: int = DEFAULT_MAX_DEPTH
    max_pages: int = DEFAULT_MAX_PAGES
    _pending: deque[CrawlTarget] = field(default_factory=deque, init=False, repr=False)
    _seen: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_depth is not None and self.max_depth < 0:
            raise ValueError("max_depth must be >= 0")
        if self.max_pages is not None and self.max_pages < 1:
            raise ValueError("max_pages must be >= 1")
        root = canonicalize_url(self.root_url)
        if not root or not is_recursive_page_url(root):
            raise ValueError("root_url must be a navigable HTTP(S) URL")
        self.root_url = root
        self._seen.add(root)
        self._pending.append(CrawlTarget(root, 0, None))

    @property
    def seen_count(self) -> int:
        return len(self._seen)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def seen_urls(self) -> frozenset[str]:
        return frozenset(self._seen)

    def pop_next(self) -> CrawlTarget | None:
        return self._pending.popleft() if self._pending else None

    def offer_links(
        self,
        source_url: str,
        source_depth: int,
        links: Iterable[object],
    ) -> list[CrawlTarget]:
        """Add eligible children in discovery order and return newly queued targets."""
        source = canonicalize_url(source_url)
        if (
            not source
            or not same_site(source, self.root_url)
            or source_depth < 0
            or (self.max_depth is not None and source_depth >= self.max_depth)
        ):
            return []

        added: list[CrawlTarget] = []
        for link in dedupe_links(links, source):
            if self.max_pages is not None and len(self._seen) >= self.max_pages:
                break
            if not same_site(link, self.root_url) or not is_recursive_page_url(link):
                continue
            if link in self._seen:
                continue
            target = CrawlTarget(link, source_depth + 1, source)
            self._seen.add(link)
            self._pending.append(target)
            added.append(target)
        return added

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-serializable frontier state for checkpointing."""
        return {
            "root_url": self.root_url,
            "max_depth": self.max_depth,
            "max_pages": self.max_pages,
            "seen_urls": sorted(self._seen),
            "pending": [
                {"url": target.url, "depth": target.depth, "parent_url": target.parent_url}
                for target in self._pending
            ],
        }

    @classmethod
    def from_snapshot(cls, state: Mapping[str, Any]) -> "SiteCrawlFrontier":
        """Restore state emitted by :meth:`snapshot`, rejecting invalid entries."""

        def _optional_int(raw: Any) -> int | None:
            if raw in (None, "", "null"):
                return None
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        frontier = cls(
            str(state.get("root_url") or ""),
            max_depth=_optional_int(state.get("max_depth", DEFAULT_MAX_DEPTH)),
            max_pages=_optional_int(state.get("max_pages", DEFAULT_MAX_PAGES)),
        )
        restored_seen: set[str] = {frontier.root_url}
        raw_seen = state.get("seen_urls")
        if isinstance(raw_seen, (list, tuple)):
            for raw in raw_seen:
                normalized = canonicalize_url(raw)
                if normalized and same_site(normalized, frontier.root_url):
                    restored_seen.add(normalized)

        restored_pending: deque[CrawlTarget] = deque()
        restored_pending_urls: set[str] = set()
        raw_pending = state.get("pending")
        if isinstance(raw_pending, (list, tuple)):
            for item in raw_pending:
                if not isinstance(item, Mapping):
                    continue
                normalized = canonicalize_url(item.get("url"))
                try:
                    depth = int(item.get("depth", 0))
                except (TypeError, ValueError):
                    continue
                parent = canonicalize_url(item.get("parent_url")) if item.get("parent_url") else None
                if (
                    normalized
                    and normalized in restored_seen
                    and normalized not in restored_pending_urls
                    and same_site(normalized, frontier.root_url)
                    and is_recursive_page_url(normalized)
                    and (frontier.max_depth is None or 0 <= depth <= frontier.max_depth)
                ):
                    restored_pending.append(CrawlTarget(normalized, depth, parent))
                    restored_pending_urls.add(normalized)
        ordered_seen = [frontier.root_url, *sorted(restored_seen - {frontier.root_url})]
        frontier._seen = set(ordered_seen[: frontier.max_pages] if frontier.max_pages is not None else ordered_seen)
        frontier._pending = deque(
            target for target in restored_pending if target.url in frontier._seen
        )
        return frontier
