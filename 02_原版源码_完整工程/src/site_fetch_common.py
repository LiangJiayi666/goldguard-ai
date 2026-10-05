from __future__ import annotations
"""官网抓取公共层：登录/备案 URL 过滤、链接规整、登录页识别。

site_crawler（curl 抓取）与 screenshot_crawler（Playwright 截图）共享这套纯逻辑。
Extractor 类因两脚本字段不同（site 多 images/image_candidates），留在各自脚本。
"""


import ipaddress
import re
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from urllib.parse import urljoin, urlsplit, urlunsplit


SKIP_WORDS = ("login", "signin", "sign-in", "logout", "register", "signup", "password", "auth", "oauth", "account", "登录", "注册", "认证", "授权")
SKIP_HOSTS = {"beian.miit.gov.cn", "www.beian.miit.gov.cn"}
HTTP_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
MAX_REDIRECTS = 8


class UnsafeUrlError(ValueError):
    """Raised when a network URL can reach a non-public destination."""

    def __init__(self, url: str, reason: str, address: str | None = None) -> None:
        self.url = url
        self.reason = reason
        self.address = address
        evidence = f" address={address}" if address else ""
        super().__init__(f"SSRF blocked url={url!r}: {reason}{evidence}")


class UrlResolutionError(RuntimeError):
    """Raised when a URL cannot be resolved safely before a request."""


@dataclass(frozen=True)
class PublicUrlTarget:
    """A normalized URL plus the public IP addresses resolved for its authority."""

    url: str
    scheme: str
    host: str
    port: int
    addresses: tuple[str, ...]


Resolver = Callable[..., list[tuple]]


def _public_ip(value: str, url: str) -> str:
    """Return a normalized global IP or raise with the blocked address as evidence."""
    candidate = value.split("%", 1)[0]
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError as exc:
        raise UrlResolutionError(f"invalid resolved address {value!r} for {url!r}") from exc

    mapped = getattr(address, "ipv4_mapped", None)
    inspected = mapped or address
    unsafe = (
        not inspected.is_global
        or inspected.is_private
        or inspected.is_loopback
        or inspected.is_link_local
        or inspected.is_multicast
        or inspected.is_reserved
        or inspected.is_unspecified
    )
    if unsafe:
        raise UnsafeUrlError(url, "destination is not a public Internet address", str(address))
    return str(address)


def resolve_public_url(url: str, resolver: Resolver = socket.getaddrinfo) -> PublicUrlTarget:
    """Normalize and resolve one HTTP(S) URL, rejecting every non-public answer.

    All DNS answers must be public.  Rejecting mixed public/private answers avoids
    letting a client choose an unsafe address after this validation step.
    """
    raw = str(url or "").strip()
    if not raw or any(ord(char) < 32 or ord(char) == 127 for char in raw):
        raise UnsafeUrlError(raw, "empty URL or control character")
    try:
        parts = urlsplit(raw)
        scheme = parts.scheme.lower()
        if scheme not in {"http", "https"}:
            raise UnsafeUrlError(raw, f"unsupported scheme {scheme or '<missing>'}")
        if parts.username is not None or parts.password is not None:
            raise UnsafeUrlError(raw, "URL credentials are not allowed")
        host_raw = parts.hostname
        explicit_port = parts.port
    except ValueError as exc:
        raise UnsafeUrlError(raw, f"malformed URL: {exc}") from exc
    if not host_raw:
        raise UnsafeUrlError(raw, "missing hostname")

    host_raw = host_raw.rstrip(".")
    if not host_raw:
        raise UnsafeUrlError(raw, "missing hostname")
    if "%" in host_raw:
        raise UnsafeUrlError(raw, "scoped IP literals are not allowed")
    try:
        literal = ipaddress.ip_address(host_raw)
    except ValueError:
        try:
            host = host_raw.encode("idna").decode("ascii").lower()
        except UnicodeError as exc:
            raise UnsafeUrlError(raw, "invalid internationalized hostname") from exc
        if host == "localhost" or host.endswith(".localhost"):
            raise UnsafeUrlError(raw, "localhost hostname is not allowed")
        literal = None
    else:
        host = str(literal)

    port = explicit_port or (443 if scheme == "https" else 80)
    netloc_host = f"[{host}]" if ":" in host else host
    netloc = netloc_host + (f":{explicit_port}" if explicit_port else "")
    normalized = urlunsplit((scheme, netloc, parts.path or "/", parts.query, ""))

    if literal is not None:
        addresses = (_public_ip(str(literal), normalized),)
    else:
        try:
            answers = resolver(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise UrlResolutionError(f"could not resolve {host!r} for {normalized!r}: {exc}") from exc
        resolved: list[str] = []
        for answer in answers:
            try:
                value = str(answer[4][0])
            except (IndexError, TypeError) as exc:
                raise UrlResolutionError(f"invalid DNS answer for {normalized!r}: {answer!r}") from exc
            public = _public_ip(value, normalized)
            if public not in resolved:
                resolved.append(public)
        if not resolved:
            raise UrlResolutionError(f"no addresses resolved for {normalized!r}")
        addresses = tuple(resolved)
    return PublicUrlTarget(normalized, scheme, host, port, addresses)


def curl_resolve_args(target: PublicUrlTarget) -> list[str]:
    """Pin curl to the address that was validated, closing the DNS TOCTOU gap."""
    host = f"[{target.host}]" if ":" in target.host else target.host
    addresses = ",".join(
        f"[{address}]" if ":" in address else address
        for address in target.addresses
    )
    return ["--noproxy", "*", "--resolve", f"{host}:{target.port}:{addresses}"]


def redirected_url(current_url: str, location: str) -> str:
    """Resolve a Location header; safety is checked by resolve_public_url next."""
    target = urljoin(current_url, str(location or "").strip())
    if not target:
        raise UnsafeUrlError(current_url, "redirect has an empty target")
    return target


def ssrf_block_payload(exc: UnsafeUrlError) -> dict[str, str]:
    payload = {
        "status": "skipped",
        "reason": str(exc),
        "blocked_url": exc.url,
    }
    if exc.address:
        payload["blocked_address"] = exc.address
    return payload


class PlaywrightSSRFGuard:
    """Fail-closed Playwright route handler for navigations and subresources."""

    def __init__(self, validator: Callable[[str], PublicUrlTarget] = resolve_public_url) -> None:
        self.validator = validator
        self.blocked: list[dict[str, str]] = []

    def _validate(self, url: str) -> None:
        parts = urlsplit(url)
        scheme = parts.scheme.lower()
        if scheme in {"about", "blob", "data"}:
            return
        self.validator(url)

    def handle(self, route, request=None) -> None:
        request = request or route.request
        url = str(request.url)
        try:
            self._validate(url)
        except Exception as exc:
            self.blocked.append({"url": url, "reason": str(exc)[:500]})
            route.abort("blockedbyclient")
            return
        route.continue_()


def install_playwright_ssrf_guard(context) -> PlaywrightSSRFGuard:
    guard = PlaywrightSSRFGuard()
    context.route("**/*", guard.handle)
    return guard


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def canonical_url(raw: str, base: str) -> str | None:
    try:
        joined = urljoin(base, raw.strip())
        parts = urlsplit(joined)
        if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
            return None
        host = parts.hostname.encode("idna").decode("ascii").lower()
        if ":" in host:
            host = f"[{host}]"
        port = f":{parts.port}" if parts.port else ""
        path = re.sub(r"/{2,}", "/", parts.path or "/")
        return urlunsplit((parts.scheme.lower(), host + port, path, parts.query, ""))
    except (UnicodeError, ValueError):
        return None


def skip_reason(url: str) -> str | None:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    hay = (parts.path + "?" + parts.query).lower()
    if host in SKIP_HOSTS:
        return "工信"
    if any(word in hay or word in host for word in SKIP_WORDS):
        return "登录"
    return None


def is_auth_page(title: str, extractor) -> bool:
    # extractor 鸭子类型：需有 .text（list[str]）与 .forms（int）属性。
    hay = (title + "\n" + "\n".join(extractor.text[:120])).lower()
    strong = ("登录", "用户登录", "会员登录", "sign in", "log in", "forgot password", "忘记密码")
    return extractor.forms > 0 and any(word.lower() in hay for word in strong)
