from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path

_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

"""Search the public web for likely official-company website roots.

The output deliberately calls every result a candidate.  Search-engine ranking is
discovery evidence, not proof that a website is operated by the named company.
"""

import argparse
import html
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

from pipeline_runtime import resolve_output, safe_name, write_json
from web_crawl_policy import NON_PAGE_SUFFIXES, canonicalize_url, site_host


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_HTML_ENDPOINT = "https://cn.bing.com/search"
DEFAULT_RSS_ENDPOINT = "https://www.bing.com/search"
# Backward-compatible name for callers that still pass ``endpoint``.
DEFAULT_ENDPOINT = DEFAULT_RSS_ENDPOINT
DEFAULT_MAX_CANDIDATES = 3
DEFAULT_MAX_CARRIER_CANDIDATES = 3

# WEB_SEARCH also records these public-web discovery results, but they are not
# promoted to website crawl roots and are not treated as verified ownership.
CARRIER_QUERY_SPECS = (
    ("douyin_account", "douyin", "抖音"),
    ("xiaohongshu_account", "xhs", "小红书"),
    ("kuaishou_account", "kuaishou", "快手"),
    ("wechat_official_account", "wechat", "微信公众号"),
    ("wechat_mini_program", "wechat", "微信小程序"),
    ("app", "app", "APP"),
)

PLATFORM_HOST_CARRIER_TYPES = {
    "douyin.com": ("douyin_account", "douyin"),
    "iesdouyin.com": ("douyin_account", "douyin"),
    "xiaohongshu.com": ("xiaohongshu_account", "xhs"),
    "kuaishou.com": ("kuaishou_account", "kuaishou"),
    "gifshow.com": ("kuaishou_account", "kuaishou"),
    "weixin.qq.com": ("wechat_official_account", "wechat"),
}

# Search-engine result pages themselves are not useful carrier evidence. Other
# third-party pages may still be retained as unverified discovery clues.
CARRIER_EXCLUDED_HOST_SUFFIXES = frozenset({
    "baidu.com", "bing.com", "google.com", "so.com", "sogou.com",
})

# These are discovery/aggregation/platform hosts, not company-owned website roots.
# Suffix matching also excludes their ordinary subdomains.
EXCLUDED_HOST_SUFFIXES = frozenset({
    "163.com", "1688.com", "58.com", "alibaba.com", "aiqicha.baidu.com", "baidu.com", "baike.com",
    "bilibili.com", "bing.com", "chinaz.com", "douban.com", "douyin.com",
    "eastmoney.com", "google.com", "gov.cn", "hc360.com", "jd.com", "jobui.com",
    "kanzhun.com", "kuaishou.com", "liepin.com", "linkedin.com", "map.baidu.com",
    "people.com.cn", "qcc.com", "qq.com", "sina.com.cn", "so.com", "sogou.com",
    "sohu.com", "taobao.com", "thepaper.cn", "tianyancha.com", "tmall.com",
    "toutiao.com", "weibo.com", "weixin.qq.com", "wikipedia.org",
    "xiaohongshu.com", "x.com", "zhihu.com", "qixin.com", "11467.com",
    "zhaopin.com", "zhipin.com",
})

COMPANY_SUFFIXES = (
    "有限责任公司", "股份有限公司", "集团有限公司", "有限公司", "集团公司",
    "股份公司", "总公司", "集团",
)


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(value: object) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", text.casefold())


def company_alias(company: str) -> str:
    value = re.sub(r"\s+", "", company.strip())
    for suffix in COMPANY_SUFFIXES:
        if value.endswith(suffix) and len(value) - len(suffix) >= 4:
            return value[: -len(suffix)]
    return value


def build_queries(company: str) -> list[str]:
    escaped = company.replace('"', " ").strip()
    return [spec["query"] for spec in build_query_specs(escaped)]


def build_query_specs(company: str) -> list[dict[str, str]]:
    escaped = company.replace('"', " ").strip()
    specs = [
        # The domestic HTML result page handles an ordinary unquoted company
        # name materially better than Bing's legacy RSS endpoint.
        {"query": escaped, "candidate_kind": "website", "platform": "web"},
        {"query": f"{escaped} 官网", "candidate_kind": "website", "platform": "web"},
    ]
    specs.extend(
        {
            "query": f"{escaped} {label}",
            "candidate_kind": carrier_type,
            "platform": platform,
        }
        for carrier_type, platform, label in CARRIER_QUERY_SPECS
    )
    return specs


def _host_is_excluded(host: str) -> bool:
    value = host.lower().rstrip(".")
    return any(value == suffix or value.endswith("." + suffix) for suffix in EXCLUDED_HOST_SUFFIXES)


def candidate_root(raw_url: object) -> str | None:
    normalized = canonicalize_url(raw_url)
    if not normalized:
        return None
    parts = urlsplit(normalized)
    host = (parts.hostname or "").lower()
    if not host or _host_is_excluded(host):
        return None
    suffix = Path(parts.path).suffix.lower()
    if suffix in NON_PAGE_SUFFIXES:
        return None
    netloc = parts.netloc.lower()
    return urlunsplit((parts.scheme, netloc, "/", "", ""))


def carrier_result_url(raw_url: object) -> str | None:
    """Keep a carrier result's deep link while rejecting search pages/assets."""
    normalized = canonicalize_url(raw_url)
    if not normalized:
        return None
    parts = urlsplit(normalized)
    host = (parts.hostname or "").lower()
    if not host or any(
        host == suffix or host.endswith("." + suffix)
        for suffix in CARRIER_EXCLUDED_HOST_SUFFIXES
    ):
        return None
    if Path(parts.path).suffix.lower() in NON_PAGE_SUFFIXES:
        return None
    return normalized


def carrier_type_for_url(raw_url: object) -> tuple[str, str] | None:
    normalized = canonicalize_url(raw_url)
    host = (urlsplit(normalized).hostname or "").lower() if normalized else ""
    for suffix, carrier in PLATFORM_HOST_CARRIER_TYPES.items():
        if host == suffix or host.endswith("." + suffix):
            return carrier
    return None


def parse_rss(payload: bytes) -> list[dict[str, str]]:
    root = ET.fromstring(payload)
    records: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = item.findtext("title") or ""
        link = item.findtext("link") or ""
        description = item.findtext("description") or ""
        if link.strip():
            records.append({"title": title.strip(), "url": link.strip(), "snippet": description.strip()})
    return records


class _BingHTMLResultParser(HTMLParser):
    """Extract ordinary ``li.b_algo`` results from Bing's HTML response."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.records: list[dict[str, str]] = []
        self.current: dict[str, Any] | None = None
        self.in_h2 = False
        self.in_title_link = False
        self.caption_depth = 0
        self.in_caption_paragraph = False

    @staticmethod
    def _classes(attrs: list[tuple[str, str | None]]) -> set[str]:
        value = next((value or "" for key, value in attrs if key == "class"), "")
        return {part for part in value.split() if part}

    @staticmethod
    def _attr(attrs: list[tuple[str, str | None]], name: str) -> str:
        return next((value or "" for key, value in attrs if key == name), "")

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        classes = self._classes(attrs)
        if tag == "li" and "b_algo" in classes and self.current is None:
            self.current = {"title_parts": [], "snippet_parts": [], "url": ""}
            return
        if self.current is None:
            return
        if tag == "h2":
            self.in_h2 = True
        elif tag == "a" and self.in_h2 and not self.current["url"]:
            href = self._attr(attrs, "href").strip()
            if href:
                self.current["url"] = href
                self.in_title_link = True
        if tag == "div":
            if self.caption_depth:
                self.caption_depth += 1
            elif "b_caption" in classes:
                self.caption_depth = 1
        elif tag == "p" and self.caption_depth:
            self.in_caption_paragraph = True

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if self.current is None:
            return
        if tag == "a" and self.in_title_link:
            self.in_title_link = False
        elif tag == "h2":
            self.in_h2 = False
            self.in_title_link = False
        elif tag == "p" and self.in_caption_paragraph:
            self.in_caption_paragraph = False
        elif tag == "div" and self.caption_depth:
            self.caption_depth -= 1
            if not self.caption_depth:
                self.in_caption_paragraph = False
        elif tag == "li":
            title = re.sub(r"\s+", " ", "".join(self.current["title_parts"])).strip()
            snippet = re.sub(r"\s+", " ", "".join(self.current["snippet_parts"])).strip()
            url = str(self.current["url"] or "").strip()
            if title and url:
                self.records.append({"title": title, "url": url, "snippet": snippet})
            self.current = None
            self.in_h2 = False
            self.in_title_link = False
            self.caption_depth = 0
            self.in_caption_paragraph = False

    def handle_data(self, data: str) -> None:
        if self.current is None:
            return
        if self.in_title_link:
            self.current["title_parts"].append(data)
        if self.in_caption_paragraph:
            self.current["snippet_parts"].append(data)


def parse_bing_html(payload: bytes) -> list[dict[str, str]]:
    parser = _BingHTMLResultParser()
    parser.feed(payload.decode("utf-8", "replace"))
    parser.close()
    return parser.records


SO_360_ENDPOINT = "https://www.so.com/s"
# 每次搜索请求之间的间隔（秒）：360/百度系对密集机器人请求会弹验证码。
SEARCH_REQUEST_INTERVAL = 2.0


class _SoHTMLResultParser(HTMLParser):
    """Extract ``li.res-list`` results from 360 search HTML.

    360 wraps outbound links in ``so.com/link`` redirects but carries the real
    URL in ``data-mdurl``/``data-cache``, so no redirect-resolution request is
    needed. Bing's bot-variant pages relax niche Chinese company queries into
    character-level noise; 360 answers them with genuinely relevant results.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.records: list[dict[str, str]] = []
        self.current: dict[str, Any] | None = None
        self.in_title_h3 = False
        self.in_title_link = False
        self.in_snippet = False

    @staticmethod
    def _classes(attrs: list[tuple[str, str | None]]) -> set[str]:
        value = next((value or "" for key, value in attrs if key == "class"), "")
        return {part for part in value.split() if part}

    @staticmethod
    def _attr(attrs: list[tuple[str, str | None]], name: str) -> str:
        return next((value or "" for key, value in attrs if key == name), "")

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        classes = self._classes(attrs)
        if tag == "li" and "res-list" in classes and self.current is None:
            self.current = {"title_parts": [], "snippet_parts": [], "url": ""}
            return
        if self.current is None:
            return
        if tag == "h3" and "res-title" in classes:
            self.in_title_h3 = True
        elif tag == "a" and self.in_title_h3 and not self.current["url"]:
            url = (
                self._attr(attrs, "data-mdurl").strip()
                or self._attr(attrs, "data-cache").strip()
                or self._attr(attrs, "href").strip()
            )
            if url:
                self.current["url"] = url
                self.in_title_link = True
        elif tag in {"p", "div"} and any("res-desc" in cls for cls in classes):
            self.in_snippet = True

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if self.current is None:
            return
        if tag == "a" and self.in_title_link:
            self.in_title_link = False
        elif tag == "h3":
            self.in_title_h3 = False
            self.in_title_link = False
        elif tag in {"p", "div"} and self.in_snippet:
            self.in_snippet = False
        elif tag == "li":
            title = re.sub(r"\s+", " ", "".join(self.current["title_parts"])).strip()
            snippet = re.sub(r"\s+", " ", "".join(self.current["snippet_parts"])).strip()
            url = str(self.current["url"] or "").strip()
            if url.startswith("//"):
                url = "https:" + url
            if title and url:
                self.records.append({"title": title, "url": url, "snippet": snippet})
            self.current = None
            self.in_title_h3 = False
            self.in_title_link = False
            self.in_snippet = False

    def handle_data(self, data: str) -> None:
        if self.current is None:
            return
        if self.in_title_link:
            self.current["title_parts"].append(data)
        if self.in_snippet:
            self.current["snippet_parts"].append(data)


def parse_so_html(payload: bytes) -> list[dict[str, str]]:
    parser = _SoHTMLResultParser()
    parser.feed(payload.decode("utf-8", "replace"))
    parser.close()
    return parser.records


def fetch_so_360(
    query: str,
    *,
    endpoint: str = SO_360_ENDPOINT,
    timeout: int = 20,
) -> list[dict[str, str]]:
    separator = "&" if "?" in endpoint else "?"
    url = endpoint + separator + urllib.parse.urlencode({"q": query})
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        records = parse_so_html(response.read(4 * 1024 * 1024))
    for record in records:
        record["source"] = "so_360"
    return records


def fetch_bing_html_cn(
    query: str,
    *,
    endpoint: str = DEFAULT_HTML_ENDPOINT,
    timeout: int = 20,
) -> list[dict[str, str]]:
    separator = "&" if "?" in endpoint else "?"
    url = endpoint + separator + urllib.parse.urlencode({
        "q": query,
        "ensearch": "0",
        "setlang": "zh-cn",
        "cc": "cn",
    })
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        records = parse_bing_html(response.read(4 * 1024 * 1024))
    for record in records:
        record["source"] = "bing_html_cn"
    return records


def fetch_bing_rss(query: str, *, endpoint: str = DEFAULT_ENDPOINT, timeout: int = 20) -> list[dict[str, str]]:
    separator = "&" if "?" in endpoint else "?"
    url = endpoint + separator + urllib.parse.urlencode({"q": query, "format": "rss", "setlang": "zh-Hans"})
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OfficialSiteDiscovery/1.0",
            "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.5",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        records = parse_rss(response.read(4 * 1024 * 1024))
    for record in records:
        record["source"] = "bing_rss"
    return records


def company_relevance_count(company: str, records: Iterable[dict[str, str]]) -> int:
    """Count results whose title/snippet contains the full name or usable alias."""
    company_key = normalize_text(company)
    alias_key = normalize_text(company_alias(company))
    count = 0
    for record in records:
        combined = normalize_text(record.get("title")) + normalize_text(record.get("snippet"))
        if company_key and company_key in combined:
            count += 1
        elif alias_key != company_key and len(alias_key) >= 4 and alias_key in combined:
            count += 1
    return count


def score_result(company: str, record: dict[str, str], *, rank: int) -> tuple[int, list[str]]:
    company_key = normalize_text(company)
    alias_key = normalize_text(company_alias(company))
    title_key = normalize_text(record.get("title"))
    snippet_key = normalize_text(record.get("snippet"))
    score = max(0, 2 - rank // 3)
    reasons: list[str] = [f"search_rank={rank + 1}"]

    full_name_matched = False
    if company_key and company_key in title_key:
        full_name_matched = True
        score += 8
        reasons.append("full_company_name_in_title")
        if title_key == company_key:
            score += 3
            reasons.append("title_is_exact_company_name")
    elif company_key and company_key in snippet_key:
        full_name_matched = True
        score += 5
        reasons.append("full_company_name_in_snippet")

    if not full_name_matched and alias_key != company_key and len(alias_key) >= 4:
        if alias_key in title_key:
            score += 3
            reasons.append("company_alias_in_title")
        elif alias_key in snippet_key:
            score += 2
            reasons.append("company_alias_in_snippet")

    combined = title_key + snippet_key
    if "官网" in combined or "官方网站" in combined:
        score += 2
        reasons.append("official_website_wording")

    result_url = canonicalize_url(record.get("url"))
    if result_url:
        path_parts = [part.casefold() for part in urlsplit(result_url).path.split("/") if part]
        ordinary_company_pages = {"about", "aboutus", "company", "contact", "home", "index", "intro", "profile"}
        if path_parts and path_parts[0].split(".", 1)[0] not in ordinary_company_pages:
            score -= 6
            reasons.append("deep_result_path_penalty")
    return score, reasons


def score_carrier_result(
    company: str,
    record: dict[str, str],
    *,
    rank: int,
    carrier_type: str,
    platform: str,
) -> tuple[int, list[str]]:
    company_key = normalize_text(company)
    alias_key = normalize_text(company_alias(company))
    title_key = normalize_text(record.get("title"))
    snippet_key = normalize_text(record.get("snippet"))
    score = max(0, 2 - rank // 3)
    reasons: list[str] = [f"search_rank={rank + 1}", f"carrier_type={carrier_type}"]

    full_name_matched = False
    if company_key and company_key in title_key:
        full_name_matched = True
        score += 8
        reasons.append("full_company_name_in_title")
        if title_key == company_key:
            score += 3
            reasons.append("title_is_exact_company_name")
    elif company_key and company_key in snippet_key:
        full_name_matched = True
        score += 5
        reasons.append("full_company_name_in_snippet")

    if not full_name_matched and alias_key != company_key and len(alias_key) >= 4:
        if alias_key in title_key:
            score += 3
            reasons.append("company_alias_in_title")
        elif alias_key in snippet_key:
            score += 2
            reasons.append("company_alias_in_snippet")

    combined = title_key + snippet_key
    labels = {
        "douyin_account": ("抖音",),
        "xiaohongshu_account": ("小红书",),
        "kuaishou_account": ("快手",),
        "wechat_official_account": ("微信公众号", "公众号", "微信公众"),
        "wechat_mini_program": ("微信小程序", "小程序"),
        "app": ("app", "应用", "客户端"),
    }.get(carrier_type, ())
    if any(normalize_text(label) in combined for label in labels):
        score += 2
        reasons.append("carrier_wording")

    inferred = carrier_type_for_url(record.get("url"))
    if inferred and inferred == (carrier_type, platform):
        score += 4
        reasons.append("platform_host_match")
    return score, reasons


def rank_candidates(
    company: str,
    result_sets: Iterable[tuple[str, list[dict[str, str]]]],
    *,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_score: int = 5,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_site: dict[str, dict[str, Any]] = {}
    excluded: list[dict[str, Any]] = []
    for query, records in result_sets:
        for rank, record in enumerate(records):
            root = candidate_root(record.get("url"))
            if not root:
                excluded.append({"url": record.get("url"), "title": record.get("title"), "reason": "excluded_host_or_url"})
                continue
            score, reasons = score_result(company, record, rank=rank)
            candidate = {
                "url": root,
                "result_url": canonicalize_url(record.get("url")),
                "domain": urlsplit(root).hostname,
                "title": record.get("title"),
                "snippet": re.sub(r"\s+", " ", html.unescape(record.get("snippet") or "")).strip()[:800],
                "score": score,
                "reasons": reasons,
                "query": query,
                "source": record.get("source") or "bing_rss",
                "official_status": "unverified_candidate",
            }
            key = site_host(root) or root
            existing = by_site.get(key)
            better_score = existing is None or int(candidate["score"]) > int(existing["score"])
            prefer_https = (
                existing is not None
                and int(candidate["score"]) == int(existing["score"])
                and str(candidate["url"]).startswith("https://")
                and not str(existing["url"]).startswith("https://")
            )
            if better_score or prefer_https:
                by_site[key] = candidate
    ranked = sorted(by_site.values(), key=lambda item: (-int(item["score"]), str(item["url"])))
    selected = [item for item in ranked if int(item["score"]) >= min_score][:max_candidates]
    return selected, excluded


def rank_carrier_candidates(
    company: str,
    result_sets: Iterable[tuple[str, str, str, list[dict[str, str]]]],
    *,
    max_candidates_per_type: int = DEFAULT_MAX_CARRIER_CANDIDATES,
    min_score: int = 5,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_candidate: dict[tuple[str, str], dict[str, Any]] = {}
    excluded: list[dict[str, Any]] = []
    for carrier_type, platform, query, records in result_sets:
        for rank, record in enumerate(records):
            if company_relevance_count(company, [record]) == 0:
                excluded.append({
                    "url": record.get("url"),
                    "title": record.get("title"),
                    "carrier_type": carrier_type,
                    "reason": "company_name_not_matched",
                })
                continue
            result_url = carrier_result_url(record.get("url"))
            if not result_url:
                excluded.append({
                    "url": record.get("url"),
                    "title": record.get("title"),
                    "carrier_type": carrier_type,
                    "reason": "excluded_host_or_url",
                })
                continue
            score, reasons = score_carrier_result(
                company,
                record,
                rank=rank,
                carrier_type=carrier_type,
                platform=platform,
            )
            candidate = {
                "carrier_type": carrier_type,
                "platform": platform,
                "candidate_name": record.get("title"),
                "url": result_url,
                "domain": urlsplit(result_url).hostname,
                "title": record.get("title"),
                "snippet": re.sub(r"\s+", " ", html.unescape(record.get("snippet") or "")).strip()[:800],
                "score": score,
                "reasons": reasons,
                "query": query,
                "source": record.get("source") or "bing_rss",
                "official_status": "unverified_candidate",
                "record_only": True,
            }
            key = (carrier_type, result_url)
            existing = by_candidate.get(key)
            if existing is None or int(candidate["score"]) > int(existing["score"]):
                by_candidate[key] = candidate

    ranked = sorted(
        (item for item in by_candidate.values() if int(item["score"]) >= min_score),
        key=lambda item: (str(item["carrier_type"]), -int(item["score"]), str(item["url"])),
    )
    selected: list[dict[str, Any]] = []
    per_type: dict[str, int] = {}
    for item in ranked:
        carrier_type = str(item["carrier_type"])
        if per_type.get(carrier_type, 0) >= max_candidates_per_type:
            continue
        selected.append(item)
        per_type[carrier_type] = per_type.get(carrier_type, 0) + 1
    return selected, excluded


def search_company(
    company: str,
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    html_endpoint: str = DEFAULT_HTML_ENDPOINT,
    timeout: int = 20,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    min_score: int = 5,
    request_interval: float = SEARCH_REQUEST_INTERVAL,
) -> dict[str, Any]:
    query_specs = build_query_specs(company)
    queries = [spec["query"] for spec in query_specs]
    website_result_sets: list[tuple[str, list[dict[str, str]]]] = []
    carrier_result_sets: list[tuple[str, str, str, list[dict[str, str]]]] = []
    errors: list[dict[str, str]] = []
    query_attempts: list[dict[str, Any]] = []
    providers_used: set[str] = set()
    fallback_count = 0
    successful_queries = 0
    fetches_done = 0
    for spec in query_specs:
        query = spec["query"]
        records: list[dict[str, str]] = []
        query_succeeded = False
        query_relevant = 0
        # Provider chain: 360 (best coverage for niche Chinese company names,
        # real URL via data-mdurl) -> Bing CN HTML -> Bing legacy RSS.
        # Fall through whenever the previous provider failed or returned
        # nothing company-relevant. 每次请求间隔数秒：360/百度系对密集机器人
        # 请求会弹验证码（qcaptcha.so.com），慢一点才能用得久。
        for provider, fetch in (
            ("so_360", lambda q, **kw: fetch_so_360(q, **kw)),
            ("bing_html_cn", lambda q, **kw: fetch_bing_html_cn(q, endpoint=html_endpoint, **kw)),
            ("bing_rss", lambda q, **kw: fetch_bing_rss(q, endpoint=endpoint, **kw)),
        ):
            if query_relevant > 0:
                break
            if fetches_done:
                time.sleep(request_interval)
            fetches_done += 1
            try:
                fetched = fetch(query, timeout=timeout)
                relevant = company_relevance_count(company, fetched)
                records.extend(fetched)
                query_succeeded = True
                providers_used.add(provider)
                query_attempts.append({
                    "query": query,
                    "provider": provider,
                    "status": "ok" if fetched else "empty_success",
                    "result_count": len(fetched),
                    "relevant_count": relevant,
                })
                if relevant > 0:
                    query_relevant = relevant
                else:
                    fallback_count += 1
            except Exception as exc:
                errors.append({"query": query, "provider": provider, "error": str(exc)[:500]})
                query_attempts.append({
                    "query": query,
                    "provider": provider,
                    "status": "failed",
                    "result_count": 0,
                    "relevant_count": 0,
                    "error": str(exc)[:500],
                })
                fallback_count += 1

        if not query_succeeded:
            continue
        successful_queries += 1
        if spec["candidate_kind"] == "website":
            website_result_sets.append((query, records))
            # Generic search sometimes directly surfaces a platform account.
            for record in records:
                inferred = carrier_type_for_url(record.get("url"))
                if inferred:
                    carrier_result_sets.append((*inferred, query, [record]))
        else:
            carrier_result_sets.append((
                spec["candidate_kind"], spec["platform"], query, records,
            ))
    if not successful_queries:
        return {
            "schema_version": 3,
            "record_type": "official_website_search",
            "company": company,
            "status": "failed",
            "provider": "so_360_with_bing_fallback",
            "providers_used": sorted(providers_used),
            "query_attempts": query_attempts,
            "queries": queries,
            "candidates": [],
            "carrier_candidates": [],
            "selected_roots": [],
            "errors": errors,
            "searched_at": now_stamp(),
        }
    selected, excluded = rank_candidates(
        company,
        website_result_sets,
        max_candidates=max_candidates,
        min_score=min_score,
    )
    carrier_candidates, carrier_excluded = rank_carrier_candidates(
        company,
        carrier_result_sets,
        min_score=min_score,
    )
    return {
        "schema_version": 3,
        "record_type": "official_website_search",
        "company": company,
        "status": "ok" if selected or carrier_candidates else "empty_success",
        "provider": "so_360_with_bing_fallback",
        "providers_used": sorted(providers_used),
        "fallback_count": fallback_count,
        "queries": queries,
        "query_specs": query_specs,
        "query_attempts": query_attempts,
        "candidates": selected,
        "carrier_candidates": carrier_candidates,
        "selected_roots": [item["url"] for item in selected],
        "excluded_count": len(excluded),
        "carrier_excluded_count": len(carrier_excluded),
        "errors": errors,
        "searched_at": now_stamp(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Search the public Internet for likely official-company websites.")
    parser.add_argument("company", help="enterprise full legal name")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-file")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--max-candidates", type=int, default=DEFAULT_MAX_CANDIDATES)
    parser.add_argument("--min-score", type=int, default=5)
    parser.add_argument("--request-interval", type=float, default=SEARCH_REQUEST_INTERVAL,
                        help="每次搜索请求之间的间隔秒数（防搜索引擎验证码）")
    args = parser.parse_args()

    company = args.company.strip().lstrip("\ufeff")
    if not company:
        parser.error("company must not be empty")
    output_file = resolve_output(
        args.output_dir,
        args.output_file,
        DEFAULT_OUTPUT_DIR,
        f"{safe_name(company)}_official_website_search.json",
    )
    print(f"{now_stamp()} official website search {company} start", flush=True)
    result = search_company(
        company,
        timeout=args.timeout,
        max_candidates=max(1, args.max_candidates),
        min_score=max(0, args.min_score),
        request_interval=max(0.0, args.request_interval),
    )
    write_json(output_file, result)
    print(
        f"{now_stamp()} official website search {company} end "
        f"status={result['status']} selected={len(result['selected_roots'])} "
        f"carrier_candidates={len(result.get('carrier_candidates') or [])} {output_file}",
        flush=True,
    )
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
