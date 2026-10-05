from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import hashlib
import json
import re
import subprocess
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

from pipeline_runtime import atomic_result, resolve_output, write_json
from site_fetch_common import (
    HTTP_REDIRECT_STATUSES,
    MAX_REDIRECTS,
    PublicUrlTarget,
    UnsafeUrlError,
    canonical_url,
    curl_resolve_args,
    is_auth_page,
    now_stamp,
    redirected_url,
    resolve_public_url,
    skip_reason,
    ssrf_block_payload,
)
from web_crawl_policy import host_variant_url


TASK_LABEL = "网页抓取"
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "output"


class Extractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.images: list[str] = []
        self.image_candidates: list[dict[str, object]] = []
        self.text: list[str] = []
        self.forms = 0
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        data = dict(attrs)
        if tag == "a" and data.get("href"):
            self.links.append(data["href"])
        if tag == "form":
            self.forms += 1
        if tag in ("img", "source"):
            for key in ("src", "data-src", "data-original", "data-lazy-src"):
                if data.get(key):
                    self.images.append(data[key])
                    self.image_candidates.append(
                        {
                            "url": data[key],
                            "width": data.get("width"),
                            "height": data.get("height"),
                            "alt": data.get("alt"),
                        }
                    )
                    break
        if tag in ("script", "style", "template", "noscript"):
            self._skip += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "template", "noscript") and self._skip:
            self._skip -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip and data.strip():
            self.text.append(data.strip())


def decode_html(body: bytes, content_type: str) -> str:
    match = re.search(r"charset=([^\s;]+)", content_type, re.I)
    encodings = [match.group(1).strip("'\"")] if match else []
    encodings.extend(["utf-8", "gb18030"])
    for encoding in dict.fromkeys(encodings):
        try:
            return body.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            continue
    return body.decode("utf-8", errors="replace")


def useful_html(body: bytes) -> bool:
    parser = Extractor()
    try:
        parser.feed(body.decode("utf-8", errors="replace"))
    except Exception:
        return False
    return len("".join(parser.text)) >= 120 or bool(parser.links or parser.images)


def error_kind(message: object) -> str:
    text = str(message or "").lower()
    if "untrusted_root" in text:
        return "tls_untrusted_root"
    if "wrong_principal" in text:
        return "tls_hostname_mismatch"
    if "cert_expired" in text:
        return "tls_certificate_expired"
    if "handshake" in text or "ssl/tls connection failed" in text:
        return "tls_handshake_failed"
    if "connection timed out" in text:
        return "connect_timeout"
    if "operation timed out" in text or "timeoutexpired" in text:
        return "timeout"
    if "could not resolve" in text or "name_not_resolved" in text:
        return "dns_failure"
    if "returned error: 404" in text:
        return "http_404"
    return "fetch_error"


def is_tls_error(message: object) -> bool:
    text = str(message or "").lower()
    return any(
        marker in text
        for marker in (
            "schannel",
            "ssl",
            "tls",
            "certificate",
            "cert_",
            "untrusted_root",
            "wrong_principal",
            "handshake",
            "illegal_message",
            "invalid_token",
        )
    )


def _split_curl_response(raw: bytes) -> tuple[bytes, bytes]:
    """Split curl's one or more HTTP header blocks from its response body."""
    offset = 0
    headers = b""
    while raw[offset : offset + 5] == b"HTTP/":
        crlf = raw.find(b"\r\n\r\n", offset)
        lf = raw.find(b"\n\n", offset)
        if crlf >= 0 and (lf < 0 or crlf <= lf):
            end, separator_size = crlf, 4
        elif lf >= 0:
            end, separator_size = lf, 2
        else:
            break
        headers = raw[offset:end]
        offset = end + separator_size
        if raw[offset : offset + 5] != b"HTTP/":
            break
    return headers, raw[offset:]


def _curl_once(
    target: PublicUrlTarget,
    timeout: int,
    max_bytes: int,
    insecure: bool = False,
) -> tuple[bytes, str, int, str | None]:
    marker = "\n__CRAWLER_EFFECTIVE_URL__:"
    cmd = [
        "curl",
        "--globoff",
        "--fail",
        "--silent",
        "--show-error",
        "--compressed",
        "--proto",
        "=http,https",
        "--max-time",
        str(timeout),
        "--connect-timeout",
        str(min(timeout, 15)),
        "-A",
        "Mozilla/5.0 SiteCrawler/1.0",
        "-D",
        "-",
        "--write-out",
        marker + "%{url_effective}\n",
    ]
    if insecure:
        cmd.append("--insecure")
    cmd.extend(curl_resolve_args(target))
    cmd.append(target.url)
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"curl timed out after {timeout + 5} seconds") from exc
    if proc.returncode:
        raise RuntimeError(proc.stderr.decode("utf-8", "replace").strip())
    raw, found, effective_raw = proc.stdout.rpartition(marker.encode("ascii"))
    if not found:
        raw = proc.stdout
    else:
        effective = effective_raw.strip().decode("utf-8", "replace")
        if effective and effective.rstrip("/") != target.url.rstrip("/"):
            raise RuntimeError(
                f"curl changed URL without an audited redirect: {target.url!r} -> {effective!r}"
            )
    headers, body = _split_curl_response(raw)
    status_match = re.match(br"HTTP/\S+\s+(\d{3})", headers)
    if not status_match:
        raise RuntimeError(f"curl returned no valid HTTP status for {target.url!r}")
    status = int(status_match.group(1))
    content_type_match = re.search(br"(?im)^content-type:\s*([^\r\n]+)", headers)
    content_type = content_type_match.group(1).decode("latin1") if content_type_match else ""
    location_match = re.search(br"(?im)^location:\s*([^\r\n]+)", headers)
    location = location_match.group(1).decode("latin1").strip() if location_match else None
    if len(body) > max_bytes:
        raise ValueError(f"response exceeded {max_bytes} bytes")
    return body, content_type, status, location


def curl_response(
    url: str,
    timeout: int,
    max_bytes: int,
    insecure: bool = False,
) -> tuple[bytes, str, str, int]:
    """Fetch one final response while validating and pinning every redirect hop."""
    current = url
    visited: set[str] = set()
    for redirect_count in range(MAX_REDIRECTS + 1):
        target = resolve_public_url(current)
        if target.url in visited:
            raise RuntimeError(f"redirect loop detected at {target.url!r}")
        visited.add(target.url)
        body, content_type, status, location = _curl_once(
            target, timeout, max_bytes, insecure=insecure,
        )
        if status in HTTP_REDIRECT_STATUSES and location:
            if redirect_count >= MAX_REDIRECTS:
                raise RuntimeError(f"too many redirects (>{MAX_REDIRECTS}) from {url!r}")
            current = redirected_url(target.url, location)
            continue
        return body, content_type, target.url, status
    raise RuntimeError(f"too many redirects (>{MAX_REDIRECTS}) from {url!r}")


def curl_bytes(url: str, timeout: int, max_bytes: int, insecure: bool = False) -> tuple[bytes, str, str]:
    """Backward-compatible byte API; callers needing HTTP status use curl_response."""
    body, content_type, final_url, _status = curl_response(
        url, timeout, max_bytes, insecure=insecure,
    )
    return body, content_type, final_url


MAX_CLIENT_REDIRECTS = 2
_META_REFRESH_TAG_RE = re.compile(r"<meta[^>]+http-equiv=[\"']?refresh[\"']?[^>]*>", re.I)
_META_CONTENT_URL_RE = re.compile(r"content=[\"'][^\"']*?url\s*=\s*([^\"'>]+)", re.I)
_JS_LOCATION_RE = re.compile(r"(?:window\.)?location(?:\.href)?\s*=\s*[\"']([^\"']+)[\"']", re.I)
_JS_REPLACE_RE = re.compile(r"(?:window\.)?location\.replace\(\s*[\"']([^\"']+)[\"']\s*\)", re.I)


def client_redirect_url(body: bytes, base_url: str) -> str | None:
    """Extract a client-side redirect target (meta refresh or location.href).

    Some official sites serve a thin shell page whose only content is a
    JavaScript or meta-refresh redirect to the real host.  curl never executes
    it, so without this check the crawl stops at the shell.  Only http(s)
    targets are followed; SSRF validation happens per hop in curl_response.
    """
    head = body[:16384].decode("utf-8", "replace")
    candidates: list[str] = []
    for tag in _META_REFRESH_TAG_RE.findall(head):
        match = _META_CONTENT_URL_RE.search(tag)
        if match:
            candidates.append(match.group(1))
    for pattern in (_JS_LOCATION_RE, _JS_REPLACE_RE):
        match = pattern.search(head)
        if match:
            candidates.append(match.group(1))
    for candidate in candidates:
        target = urljoin(base_url, candidate.strip())
        if target != base_url and urlsplit(target).scheme.lower() in ("http", "https"):
            return target
    return None


def _strategy_responses(
    url: str,
    timeout: int,
    max_bytes: int,
    attempts: list[dict[str, str]],
):
    """Yield (strategy, body, content_type, final_url, status) per curl strategy.

    Strategy order mirrors the historical behaviour: plain curl, insecure curl
    when the failure looks TLS-related, then an http:// fallback for https://
    inputs.  A strategy that raises records its error and the cascade moves on;
    a strategy that answers yields its response for the caller to inspect.
    """
    try:
        yield ("curl", *curl_response(url, timeout, max_bytes))
    except UnsafeUrlError:
        raise
    except Exception as exc:
        attempts.append({"strategy": "curl", "kind": error_kind(exc), "error": str(exc)})
        if is_tls_error(exc):
            try:
                yield ("curl_insecure", *curl_response(url, timeout, max_bytes, insecure=True))
            except UnsafeUrlError:
                raise
            except Exception as exc2:
                attempts.append({"strategy": "curl_insecure", "kind": error_kind(exc2), "error": str(exc2)})
    if url.startswith("https://"):
        http_url = "http://" + url[8:]
        try:
            yield ("curl_http_fallback", *curl_response(http_url, timeout, max_bytes, insecure=True))
        except UnsafeUrlError:
            raise
        except Exception as exc:
            attempts.append({"strategy": "curl_http_fallback", "kind": error_kind(exc), "error": str(exc)})


def _fetch_candidate(
    candidate: str,
    original_url: str,
    timeout: int,
    max_bytes: int,
    attempts: list[dict[str, str]],
) -> dict | None:
    """Fetch one candidate URL, following sparse shell pages' client redirects."""
    current = candidate
    for hop in range(MAX_CLIENT_REDIRECTS + 1):
        redirect_target: str | None = None
        for strategy, body, content_type, final_url, status_code in _strategy_responses(
            current, timeout, max_bytes, attempts,
        ):
            if useful_html(body):
                return {
                    "url": original_url,
                    "fetched_url": final_url,
                    "body": body,
                    "content_type": content_type,
                    "status_code": status_code,
                    "strategy": strategy,
                }
            redirect_target = client_redirect_url(body, final_url)
            if redirect_target:
                attempts.append({
                    "strategy": strategy,
                    "kind": "client_redirect",
                    "error": f"sparse HTML; following client-side redirect to {redirect_target}",
                })
                break
            attempts.append({"strategy": strategy, "kind": "sparse_html", "error": "sparse HTML; trying fallback"})
        if not redirect_target:
            return None
        if hop >= MAX_CLIENT_REDIRECTS:
            attempts.append({
                "strategy": "client_redirect",
                "kind": "redirect_limit",
                "error": f"too many client-side redirects (>{MAX_CLIENT_REDIRECTS})",
            })
            return None
        current = redirect_target
    return None


def fetch_page(url: str, timeout: int, max_bytes: int) -> tuple[dict, list[dict[str, str]]]:
    attempts: list[dict[str, str]] = []
    # The registered root is not always the host that answers: the apex domain
    # may be parked while only www.<domain> serves the site (or vice versa).
    candidates = [url]
    variant = host_variant_url(url)
    if variant:
        candidates.append(variant)
    for index, candidate in enumerate(candidates):
        if index:
            attempts.append({
                "strategy": "host_variant",
                "kind": "host_fallback",
                "error": f"trying alternate host {candidate}",
            })
        page = _fetch_candidate(candidate, url, timeout, max_bytes, attempts)
        if page is not None:
            return page, attempts
    raise RuntimeError("all fetch strategies failed: " + " | ".join(f"{x['strategy']}: {x['error']}" for x in attempts))


def build_output(url: str, timeout: int, max_bytes: int) -> dict:
    page, attempts = fetch_page(url, timeout, max_bytes)
    body = page.pop("body")
    text = decode_html(body, page["content_type"])
    extractor = Extractor()
    extractor.feed(text)
    title_match = re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S)
    title = re.sub(r"<[^>]+>", " ", title_match.group(1)) if title_match else ""
    title = re.sub(r"\s+", " ", title).strip()
    if is_auth_page(title, extractor):
        return {"_skip_reason": "登录"}
    links = [u for u in (canonical_url(v, page["fetched_url"]) for v in extractor.links) if u]
    images = [u for u in (canonical_url(v, page["fetched_url"]) for v in extractor.images) if u]
    candidates = []
    for item in extractor.image_candidates:
        normalized = canonical_url(str(item["url"]), page["fetched_url"])
        if normalized:
            candidates.append({**item, "url": normalized})
    key = re.sub(r"[^A-Za-z0-9]+", "_", urlsplit(url).hostname or "site")[:80]
    return {
        "url": page["url"],
        "fetched_url": page["fetched_url"],
        "status_code": page["status_code"],
        "fetch_function": "site_crawler.fetch_page",
        "fetch_mode": "static",
        "image_download_required": True,
        "text": "\n".join(extractor.text),
        "links": list(dict.fromkeys(links)),
        "images": list(dict.fromkeys(images)),
        "image_candidates": list({item["url"]: item for item in candidates}.values()),
        "fetch_strategy": page["strategy"],
        "fetch_attempts": attempts,
        "title": title,
        "content_type": page["content_type"],
        "content_fingerprint": hashlib.sha256(re.sub(r"\s+", " ", " ".join(extractor.text)).strip().encode("utf-8")).hexdigest() if extractor.text else None,
        "content_fingerprint_version": 2,
        "_file_name": f"{key}.json",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch one website page and save a page-style JSON artifact.")
    parser.add_argument("domain", help="website domain or URL, e.g. yoostone.com or https://yoostone.com/")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录")
    parser.add_argument("--output-file", help="输出 JSON 文件路径；覆盖默认目录与文件名")
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--max-bytes", type=int, default=20 * 1024 * 1024)
    args = parser.parse_args()

    url = args.domain.strip()
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    domain = urlsplit(url).hostname or args.domain
    requested_output = Path(args.output_file) if args.output_file else None

    reason = skip_reason(url)
    if reason:
        if requested_output:
            write_json(requested_output, atomic_result({"status": "skipped", "url": url, "reason": reason}, source="site_crawler.skip"))
        print(f"{now_stamp()} {TASK_LABEL} {domain} 跳过:{reason}", flush=True)
        return 0

    try:
        result = build_output(url, args.timeout, args.max_bytes)
        reason = result.pop("_skip_reason", None)
        if reason:
            if requested_output:
                write_json(requested_output, atomic_result({"status": "skipped", "url": url, "reason": reason}, source="site_crawler.skip"))
            print(f"{now_stamp()} {TASK_LABEL} {domain} 跳过:{reason}", flush=True)
            return 0
        file_path = resolve_output(args.output_dir, args.output_file, DEFAULT_OUTPUT_DIR, result.pop("_file_name"))
        write_json(file_path, result)
        print(
            f"{now_stamp()} {TASK_LABEL} {domain} 成功 "
            f"status_code={result.get('status_code')} "
            f"function={result.get('fetch_function')} final_url={result.get('fetched_url')} {file_path}",
            flush=True,
        )
        return 0
    except UnsafeUrlError as exc:
        if requested_output:
            write_json(
                requested_output,
                atomic_result(
                    {**ssrf_block_payload(exc), "url": url},
                    source="site_crawler.ssrf_guard",
                ),
            )
        print(f"{now_stamp()} {TASK_LABEL} {domain} 跳过:{exc}", flush=True)
        return 0
    except TimeoutError:
        if requested_output:
            write_json(requested_output, atomic_result({"status": "timeout", "url": url, "reason": "fetch timeout"}, source="site_crawler.timeout"))
        print(f"{now_stamp()} {TASK_LABEL} {domain} 超时", flush=True)
        return 2
    except Exception as exc:
        if requested_output:
            write_json(requested_output, atomic_result({"status": "error", "url": url, "error": str(exc)[:500]}, source="site_crawler.exception"))
        print(f"{now_stamp()} {TASK_LABEL} {domain} 失败 {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
