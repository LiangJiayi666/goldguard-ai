from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline_runtime import iso_utc, resolve_output, safe_name, write_json
from llm_client import call_llm, load_llm_config, parse_llm_json


ROOT = Path(__file__).parent
DEFAULT_OUTPUT_DIR = ROOT / "output"
TASK_LABEL = "social account rank"
PLATFORMS = {"douyin": "douyin", "xhs": "xhs", "kuaishou": "kuaishou"}


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def resolve_output_path(args: argparse.Namespace, company: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(company)}_social_top_accounts.json")


def normalize_account(platform: str, raw: dict[str, Any], aliases: Any = None) -> dict[str, Any]:
    if platform == "douyin":
        raw_stats = str(raw.get("raw_stats_text") or "")
        visible_id_match = re.search(r"抖音号\s*[:：]\s*(.+?)(?=获赞|粉丝|\||\s|$)", raw_stats)
        visible_id = visible_id_match.group(1) if visible_id_match else None
        # Douyin's rendered search card can concatenate the public account ID
        # and the like count (for example ``qxy7588882797获赞``).  A confirmed
        # ID is authoritative when it is the prefix of that rendered segment.
        for alias in _normalized_aliases(aliases, platform):
            if alias["alias_type"] != "platform_account_id" or not alias["is_confirmed"]:
                continue
            if visible_id and _normalize_signal(visible_id).startswith(alias["normalized_value"]):
                visible_id = alias["alias_value"]
                break
        return {
            "user_id": raw.get("sec_uid"),
            "account_id": raw.get("douyin_id") or visible_id,
            "username": raw.get("nickname"),
            "signature": raw.get("signature"),
            "fans": raw.get("fans"),
            "works_count": raw.get("works_count"),
            "profile_url": raw.get("profile_href"),
            "raw_stats_text": raw.get("raw_stats_text"),
        }
    if platform == "xhs":
        return {
            "user_id": raw.get("user_id"),
            "account_id": raw.get("red_id"),
            "username": raw.get("nickname"),
            "signature": raw.get("desc"),
            "fans": raw.get("fans"),
            "works_count": raw.get("note_count"),
            "profile_url": raw.get("profile_url"),
            "raw_stats_text": raw.get("raw_stats_text"),
        }
    kwai_id = raw.get("kwai_id")
    return {
        "user_id": kwai_id,
        "account_id": kwai_id,
        "username": raw.get("nickname"),
        "signature": raw.get("desc"),
        "fans": raw.get("fans"),
        "works_count": raw.get("works_count"),
        "profile_url": raw.get("profile_url") or (f"https://www.kuaishou.com/profile/{kwai_id}" if kwai_id else None),
        "raw_stats_text": raw.get("raw_stats_text"),
    }


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _company_short(company: str) -> str:
    name = re.sub(r"（.*?）|\(.*?\)", "", company).strip()
    return re.sub(r"(有限公司|有限责任公司|股份有限公司)$", "", name).strip()


def _normalize_signal(value: Any) -> str:
    return re.sub(r"\s+", "", _text(value)).casefold()


def _normalized_aliases(aliases: Any, platform: str) -> list[dict[str, Any]]:
    if not isinstance(aliases, list):
        return []
    result: list[dict[str, Any]] = []
    for raw in aliases:
        if not isinstance(raw, dict):
            continue
        alias_platform = str(raw.get("platform") or "").strip()
        if alias_platform and alias_platform != platform:
            continue
        value = str(raw.get("alias_value") or raw.get("value") or "").strip()
        alias_type = str(raw.get("alias_type") or raw.get("type") or "").strip()
        if not value or not alias_type:
            continue
        result.append({
            "alias_type": alias_type,
            "alias_value": value,
            "normalized_value": _normalize_signal(value),
            "platform": alias_platform,
            "confidence": float(raw.get("confidence") or 0.0),
            "is_confirmed": bool(raw.get("is_confirmed") or raw.get("confirmed")),
            "source_type": str(raw.get("source_type") or raw.get("source") or "unknown"),
        })
    return result


def deterministic_protection_reasons(
    company: str,
    platform: str,
    account: dict[str, Any],
    aliases: Any,
) -> list[str]:
    reasons: list[str] = []
    account_name = _normalize_signal(
        account.get("username") or account.get("nickname") or account.get("account_name")
    )
    signature = _normalize_signal(account.get("signature"))
    raw_stats = _normalize_signal(account.get("raw_stats_text"))
    account_ids = {
        _normalize_signal(account.get(key))
        for key in ("account_id", "douyin_id", "red_id", "kwai_id")
        if _normalize_signal(account.get(key))
    }
    full = _normalize_signal(company)
    short = _normalize_signal(_company_short(company))
    platform_aliases = _normalized_aliases(aliases, platform)

    confirmed_ids = {
        item["normalized_value"]
        for item in platform_aliases
        if item["alias_type"] == "platform_account_id" and item["is_confirmed"]
    }
    if account_ids & confirmed_ids:
        reasons.append("confirmed_platform_account_id_exact")
    elif any(confirmed_id and confirmed_id in raw_stats for confirmed_id in confirmed_ids):
        reasons.append("confirmed_platform_account_id_exact")

    certification_text = " ".join((_text(account.get("raw_stats_text")), _text(account.get("signature"))))
    if full and full in _normalize_signal(certification_text) and re.search(r"认证徽章|认证主体|企业认证", certification_text):
        reasons.append("certification_subject_exact")

    if short and len(short) >= 4 and short in account_name:
        reasons.append("account_name_contains_short_name")

    if full and full in signature + raw_stats:
        reasons.append("bio_contains_legal_name")

    for item in platform_aliases:
        signal = item["normalized_value"]
        if item["alias_type"] == "account_name":
            matched = signal == account_name
        else:
            matched = signal in account_name or signal in signature or signal in raw_stats
        if (
            item["alias_type"] in {"brand_name", "account_name", "website_brand", "icp_service_name"}
            and item["is_confirmed"]
            and item["confidence"] >= 0.6
            and len(signal) >= 2
            and matched
        ):
            reasons.append(f"confirmed_{item['alias_type']}_exact")

    return list(dict.fromkeys(reasons))


def _evidence_from_account(company: str, account: dict[str, Any]) -> list[str]:
    evidence: list[str] = []
    short = _company_short(company)
    text_fields = {
        "username": _text(account.get("username")),
        "signature": _text(account.get("signature")),
        "raw_stats_text": _text(account.get("raw_stats_text")),
        "profile_url": _text(account.get("profile_url")),
    }
    for label, text in text_fields.items():
        if not text:
            continue
        lower = text.lower()
        if company and company in text:
            evidence.append(f"{label} 包含完整公司名")
        elif short and short in text:
            evidence.append(f"{label} 包含公司简称")
        elif any(token in lower for token in ("official", "verified", "shop", "brand", "account")):
            evidence.append(f"{label} 包含官方/品牌信号")
    if not evidence:
        for label, text in text_fields.items():
            if text:
                evidence.append(f"{label}: {text[:80]}")
    return evidence[:3]


def _reason_from_account(company: str, account: dict[str, Any]) -> str:
    evidence = _evidence_from_account(company, account)
    if evidence:
        return f"账号与公司或品牌信号匹配，依据：{evidence[0]}"
    if _text(account.get("username")) or _text(account.get("signature")):
        return "账号名称或简介与企业相关，因此保留为较高置信候选。"
    return "可用信号较弱，但搜索结果仍有相关性。"


def _deterministic_fallback_accounts(company: str, candidates: list[dict[str, Any]], top_k: int) -> list[dict[str, Any]]:
    """Keep only literal enterprise-name matches when LLM ranking is unavailable.

    Search engines commonly return accounts sharing one or two characters with
    the enterprise.  Passing the first N results through on an LLM outage
    turns that weak recall list into an apparent shortlist, which is unsafe.
    The fallback is intentionally conservative: it accepts only the full name
    or its legal-suffix-free form as a contiguous text match.
    """
    signals = tuple(dict.fromkeys(value for value in (company.strip(), _company_short(company)) if value))
    selected: list[dict[str, Any]] = []
    for candidate in candidates:
        text = " ".join(
            _text(candidate.get(key))
            for key in ("username", "signature", "raw_stats_text", "profile_url")
        )
        if any(signal in text for signal in signals):
            selected.append(candidate)
        if len(selected) >= max(1, top_k):
            break
    return selected


def _match_candidate_index(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in (
            candidate.get("sec_uid"),
            candidate.get("user_id"),
            candidate.get("kwai_id"),
            candidate.get("red_id"),
            candidate.get("douyin_id"),
            candidate.get("account_id"),
            candidate.get("username"),
            candidate.get("nickname"),
            candidate.get("profile_url"),
            candidate.get("profile_href"),
        ):
            value = _text(key)
            if value and value not in index:
                index[value] = candidate
    return index


def normalize_rank_item(company: str, candidate_index: dict[str, dict[str, Any]], raw: Any, rank: int) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    item = dict(raw)
    matched_candidate = None
    for lookup_key in (item.get("user_id"), item.get("account_id"), item.get("username"), item.get("profile_url")):
        matched_candidate = candidate_index.get(_text(lookup_key))
        if matched_candidate:
            break
    if matched_candidate:
        for key in ("user_id", "account_id", "username", "signature", "fans", "works_count", "profile_url", "raw_stats_text"):
            value = matched_candidate.get(key)
            if value not in {None, ""}:
                item[key] = value
    for key in ("user_id", "account_id", "username", "signature", "fans", "works_count", "profile_url", "raw_stats_text"):
        if key not in item or item.get(key) in {None, ""}:
            for lookup_key in (item.get("user_id"), item.get("account_id"), item.get("username"), item.get("profile_url")):
                candidate = candidate_index.get(_text(lookup_key))
                if candidate and candidate.get(key) not in {None, ""}:
                    item[key] = candidate.get(key)
                    break
    item["rank"] = item.get("rank") or rank
    item["reason"] = _reason_from_account(company, item)
    evidence = item.get("evidence")
    if isinstance(evidence, list):
        normalized_evidence = [_text(x) for x in evidence if _text(x)]
    elif isinstance(evidence, str):
        normalized_evidence = [_text(evidence)] if _text(evidence) else []
    else:
        normalized_evidence = []
    if not normalized_evidence:
        normalized_evidence = _evidence_from_account(company, item)
    item["evidence"] = normalized_evidence
    return item


def _rank_identity(item: dict[str, Any]) -> str:
    for key in ("user_id", "account_id", "profile_url", "username"):
        value = _normalize_signal(item.get(key))
        if value:
            return f"{key}:{value}"
    return ""


def normalize_ranked_accounts(
    company: str,
    candidates: list[dict[str, Any]],
    parsed: Any,
    top_k: int,
    *,
    platform: str = "",
    aliases: Any = None,
) -> list[dict[str, Any]]:
    confirmed_account_names = {
        item["normalized_value"]
        for item in _normalized_aliases(aliases, platform)
        if item["alias_type"] == "account_name" and item["is_confirmed"]
    }

    def confirmed_name_key(item: dict[str, Any]) -> str:
        name = _normalize_signal(item.get("username") or item.get("nickname") or item.get("account_name"))
        return name if name in confirmed_account_names else ""

    has_explicit_llm_decision = False
    if isinstance(parsed, dict):
        has_explicit_llm_decision = "top_accounts" in parsed
        raw_accounts = parsed.get("top_accounts") or []
    elif isinstance(parsed, list):
        has_explicit_llm_decision = True
        raw_accounts = parsed
    else:
        raw_accounts = []
    if not raw_accounts and not has_explicit_llm_decision:
        raw_accounts = _deterministic_fallback_accounts(company, candidates, top_k)
    candidate_index = _match_candidate_index(candidates)
    protected: list[dict[str, Any]] = []
    for candidate in candidates:
        reasons = deterministic_protection_reasons(company, platform, candidate, aliases)
        if not reasons:
            continue
        item = normalize_rank_item(company, candidate_index, candidate, len(protected) + 1)
        if item is None:
            continue
        item["selection_source"] = "deterministic_protection"
        item["protection_reasons"] = reasons
        item["llm_selected"] = False
        protected.append(item)

    protection_priority = {
        "confirmed_platform_account_id_exact": 0,
        "certification_subject_exact": 1,
        "confirmed_account_name_exact": 2,
        "confirmed_brand_name_exact": 3,
        "confirmed_website_brand_exact": 4,
        "confirmed_icp_service_name_exact": 5,
        "bio_contains_legal_name": 6,
        "account_name_contains_short_name": 7,
    }
    protected.sort(key=lambda item: (
        min((protection_priority.get(reason, 99) for reason in item.get("protection_reasons") or []), default=99),
        str(item.get("username") or ""),
        _rank_identity(item),
    ))
    deduped_protected: list[dict[str, Any]] = []
    seen_confirmed_names: set[str] = set()
    for item in protected:
        name_key = confirmed_name_key(item)
        if name_key and name_key in seen_confirmed_names:
            continue
        deduped_protected.append(item)
        if name_key:
            seen_confirmed_names.add(name_key)
        if len(deduped_protected) >= max(1, top_k):
            break
    protected = deduped_protected
    normalized: list[dict[str, Any]] = list(protected)
    seen = {_rank_identity(item) for item in protected if _rank_identity(item)}
    for i, raw in enumerate(raw_accounts[: max(1, top_k)], start=1):
        item = normalize_rank_item(company, candidate_index, raw, i)
        if item is None:
            continue
        identity = _rank_identity(item)
        if identity and identity in seen:
            for existing in normalized:
                if _rank_identity(existing) == identity:
                    existing["llm_selected"] = True
                    if existing.get("selection_source") != "deterministic_protection":
                        existing["selection_source"] = "llm"
                    break
            continue
        name_key = confirmed_name_key(item)
        if name_key and name_key in seen_confirmed_names:
            continue
        item["selection_source"] = "llm" if has_explicit_llm_decision else "deterministic_fallback"
        item["protection_reasons"] = []
        item["llm_selected"] = has_explicit_llm_decision
        normalized.append(item)
        if identity:
            seen.add(identity)
        if name_key:
            seen_confirmed_names.add(name_key)
    normalized = normalized[:max(1, top_k)]
    for rank, item in enumerate(normalized, start=1):
        item["rank"] = rank
    return normalized


def build_prompt(company: str, platform_accounts: dict[str, list[dict[str, Any]]], aliases: Any = None) -> str:
    sections = []
    for platform, accounts in platform_accounts.items():
        if not accounts:
            continue
        sections.append(f"{platform}: " + json.dumps(accounts[:20], ensure_ascii=False))
    return (
        f"Shortlist social accounts for a later recent-post ownership review for {company}.\n"
        f"Known aliases and confirmed platform identifiers: {json.dumps(aliases or [], ensure_ascii=False)}\n"
        "Preserve recall only among candidates with an exact textual company or brand-name match. "
        "For Chinese company or brand names, every character used as the company/brand signal must match "
        "the target name exactly, character by character. Homophones, near-homophones, similar-looking characters, "
        "variant characters, typos, approximate spellings, or translated/transliterated forms are not matches and "
        "must not be used as evidence of company or brand relevance. For example, different characters such as "
        "\u201c君豪\u201d, \u201c俊豪\u201d, \u201c君浩\u201d, and \u201c俊濠\u201d must be treated as different names. "
        "A matching location such as Shenzhen, an official shop, a local business account, or a brand-linked "
        "employee/sales account may support a candidate only after the exact company/brand text match is present. "
        "Do not claim that shortlist membership proves official ownership. Exclude candidates whose only apparent "
        "connection is a homophone, similar character, personal name, or unrelated industry/profile.\n"
        "Return strict JSON with this shape:\n"
        "{"
        "\"douyin\":{\"top_accounts\":[{\"user_id\":\"...\",\"account_id\":\"...\",\"username\":\"...\",\"profile_url\":\"...\",\"rank\":1,\"score\":0.98,\"reason\":\"...\",\"evidence\":[\"...\"]}]},"  # noqa: E501
        "\"xhs\":{\"top_accounts\":[...]},"  # noqa: E501
        "\"kuaishou\":{\"top_accounts\":[...]}"
        "}.\n"
        "Use short reason/evidence strings that explain the selection, not internal thinking.\n"
        + "\n".join(sections)
    )


def normalize_platform_accounts(raw: dict[str, Any], aliases: Any = None) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {p: [] for p in PLATFORMS}
    for platform in PLATFORMS:
        items = raw.get(platform) or []
        if isinstance(items, list):
            result[platform] = [normalize_account(platform, item, aliases) for item in items if isinstance(item, dict)]
    return result


def load_platform_accounts(value: str) -> dict[str, Any]:
    path = Path(value)
    if path.is_file():
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def main() -> int:
    config = load_llm_config()
    parser = argparse.ArgumentParser(description="Rank social media accounts by likelihood of being official.")
    parser.add_argument("company", help="enterprise name")
    parser.add_argument("platform_accounts", help='JSON file path or inline JSON like {"douyin": [...], "xhs": [...], "kuaishou": [...]}')
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="output directory")
    parser.add_argument("--output-file", help="output JSON file name")
    parser.add_argument("--top-k", type=int, default=5, help="review-candidate limit per platform")
    parser.add_argument("--model", default=os.environ.get("LLM_MODEL", config["model"]), help="LLM model name")
    parser.add_argument("--llm-timeout", type=int, default=180, help="LLM timeout in seconds")
    args = parser.parse_args()

    if not config["api_key"]:
        print(f"{now_stamp()} {TASK_LABEL} {args.company} failed: missing LLM API key", file=sys.stderr, flush=True)
        return 1

    raw_input = load_platform_accounts(args.platform_accounts)
    raw_platform_accounts = raw_input.get("platforms") if isinstance(raw_input.get("platforms"), dict) else raw_input
    aliases = raw_input.get("aliases") if isinstance(raw_input, dict) else []
    platform_accounts = normalize_platform_accounts(raw_platform_accounts, aliases)
    prompt = build_prompt(args.company, platform_accounts, aliases)
    try:
        content = call_llm(prompt, config["base_url"], args.model, config["api_key"], args.llm_timeout, system_message="Return only JSON with reason and evidence fields when possible.")
        parsed = parse_llm_json(content)
    except Exception as exc:
        print(f"{now_stamp()} {TASK_LABEL} {args.company} llm failed: {exc}", file=sys.stderr, flush=True)
        parsed = {}

    results: dict[str, Any] = {}
    for platform, accounts in platform_accounts.items():
        ranked = parsed.get(platform) if isinstance(parsed, dict) else None
        top_accounts = normalize_ranked_accounts(
            args.company, accounts, ranked, args.top_k, platform=platform, aliases=aliases,
        )
        protected_count = sum(bool(item.get("protection_reasons")) for item in top_accounts)
        results[platform] = {
            "candidate_count": len(accounts),
            "protected_count": protected_count,
            "llm_selected_count": sum(bool(item.get("llm_selected")) for item in top_accounts),
            "final_shortlist_count": len(top_accounts),
            "guard_version": 1,
            "top_accounts": top_accounts,
        }

    result = {
        "schema_version": 2,
        "record_type": "final",
        "company": args.company,
        "short_name": args.company,
        "query_time": now_iso(),
        "generated_at": iso_utc(),
        "results": results,
    }
    output_path = resolve_output_path(args, args.company)
    write_json(output_path, result)
    print(f"{now_stamp()} {TASK_LABEL} {args.company} success {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
