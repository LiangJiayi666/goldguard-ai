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
from datetime import datetime
from pathlib import Path
from typing import Any

from llm_client import call_llm, load_llm_config, parse_llm_json
from pipeline_runtime import iso_utc, resolve_output, safe_name, write_json


ROOT = Path(__file__).parent
DEFAULT_OUTPUT_DIR = ROOT / "output"
TASK_LABEL = "social account review"
PLATFORMS = ("douyin", "xhs", "kuaishou")
VALID_ROLES = {
    "main_official",
    "official_business",
    "local_business",
    "employee_or_sales",
    "high_relevance_candidate",
    "unrelated",
}
VALID_OFFICIAL_STATUSES = {"verified", "suspected", "related", "rejected", "unknown"}
VALID_DECISIONS = {"accept_main", "accept_related", "reject", "pending_review"}
BUSINESS_TERMS = (
    "黄金", "珠宝", "首饰", "钻石", "钻戒", "铂金", "白金", "贵金属", "水贝", "培育钻", "足金", "金价",
    "回收", "定制", "镶嵌", "手镯", "项链", "戒指", "耳饰", "吊坠", "珠宝店", "展厅",
)


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def resolve_output_path(args: argparse.Namespace, company: str) -> Path:
    return resolve_output(
        args.output_dir,
        args.output_file,
        DEFAULT_OUTPUT_DIR,
        f"{safe_name(company)}_social_account_review.json",
    )


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _normalize_identity(value: Any) -> str:
    return re.sub(r"[\W_]+", "", _text(value), flags=re.UNICODE).casefold()


def _identity_values(account: dict[str, Any]) -> set[str]:
    return {
        _text(account.get(key))
        for key in (
            "sec_uid",
            "user_id",
            "kwai_id",
            "account_id",
            "douyin_id",
            "username",
            "nickname",
            "account_name",
            "profile_url",
            "profile_href",
        )
        if _text(account.get(key))
    }


def _account_name(account: dict[str, Any]) -> str:
    return _text(
        account.get("account_name")
        or account.get("username")
        or account.get("nickname")
    ) or "unknown"


def _post_text(item: dict[str, Any]) -> str:
    for key in ("title", "desc", "caption", "content", "text", "display_title"):
        value = _text(item.get(key))
        if value:
            return re.sub(r"\s+", " ", value)[:240]
    return ""


def compact_review_record(raw: dict[str, Any], sample_limit: int = 8) -> dict[str, Any]:
    raw_account = raw.get("account") if isinstance(raw.get("account"), dict) else {}
    account = {
        key: value
        for key, value in raw_account.items()
        if "token" not in str(key).lower() and "cookie" not in str(key).lower()
    }
    sample = raw.get("sample") if isinstance(raw.get("sample"), dict) else {}
    posts = sample.get("posts") if isinstance(sample.get("posts"), list) else []
    titles = [_post_text(item) for item in posts if isinstance(item, dict)]
    titles = [title for title in titles if title][:sample_limit]
    return {
        "account": account,
        "sample": {
            "status": sample.get("status"),
            "profile": sample.get("profile") if isinstance(sample.get("profile"), dict) else {},
            "author": sample.get("author") if isinstance(sample.get("author"), dict) else {},
            "available_post_count": int(sample.get("available_post_count") or len(posts)),
            "sample_titles": titles,
        },
    }


def normalize_evidence(raw: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    source = raw.get("platforms") if isinstance(raw.get("platforms"), dict) else raw
    result: dict[str, list[dict[str, Any]]] = {platform: [] for platform in PLATFORMS}
    for platform in PLATFORMS:
        records = source.get(platform) if isinstance(source, dict) else None
        if not isinstance(records, list):
            continue
        result[platform] = [
            compact_review_record(record)
            for record in records
            if isinstance(record, dict)
        ]
    return result


def _company_signals(company: str, aliases: Any = None) -> list[tuple[str, bool]]:
    """Return exact company/brand signals, with confirmation strength."""
    name = _text(company)
    no_suffix = re.sub(r"(有限责任公司|有限公司|股份有限公司)$", "", name).strip()
    signals: list[tuple[str, bool]] = []
    for value, strong in ((name, True), (no_suffix, False)):
        if value and value not in [item[0] for item in signals]:
            signals.append((value, strong))
    # A short name without the administrative prefix is useful for recall, but
    # must be supported by profile/post evidence before it can pass.
    short = re.sub(r"^(中国|广东省?|深圳市?|深圳|北京市?|上海市?)", "", no_suffix).strip()
    if len(short) >= 3 and short not in [item[0] for item in signals]:
        signals.append((short, False))
    # Legal names often append descriptors such as “文化科技/实业”; retain
    # the recognizable brand stem so an employee or brand account is not lost.
    for marker in ("珠宝", "黄金", "首饰", "贵金属"):
        if marker in short:
            stem = short.split(marker, 1)[0].strip()
            brand = f"{stem}{marker}" if stem else marker
            for value in (brand, stem):
                if len(value) >= 3 and value not in [item[0] for item in signals]:
                    signals.append((value, False))
    if isinstance(aliases, list):
        for raw in aliases:
            if not isinstance(raw, dict):
                continue
            value = _text(raw.get("alias_value") or raw.get("value"))
            if not value:
                continue
            try:
                confidence = float(raw.get("confidence") or 0)
            except (TypeError, ValueError):
                confidence = 0
            strong = bool(raw.get("is_confirmed") or raw.get("confirmed")) and confidence >= 0.8
            if len(value) >= 3 and value not in [item[0] for item in signals]:
                signals.append((value, strong))
    return signals


def _joined_text(account: dict[str, Any], sample: dict[str, Any]) -> tuple[str, str, list[str]]:
    profile = " ".join(
        _text(account.get(key))
        for key in ("username", "nickname", "account_name", "signature", "raw_stats_text", "profile_url", "profile_href")
    )
    profile += " " + json.dumps(sample.get("profile") or {}, ensure_ascii=False)
    profile += " " + json.dumps(sample.get("author") or {}, ensure_ascii=False)
    titles = [
        _text(title)
        for title in (sample.get("sample_titles") or [])
        if _text(title)
    ]
    return profile, " ".join(titles), titles


def _has_business_content(text: str) -> bool:
    return any(term in text for term in BUSINESS_TERMS)


def _is_certified(profile: str, company: str) -> bool:
    return _text(company) in profile and any(token in profile for token in ("认证徽章", "认证主体", "企业认证", "enterprise_verify"))


def _apply_ownership_gate(
    company: str,
    account: dict[str, Any],
    sample: dict[str, Any],
    raw: dict[str, Any] | None,
    aliases: Any = None,
) -> dict[str, Any]:
    """Turn the LLM's recall-oriented opinion into a safer crawl decision.

    Main-account recall is protected by accepting certified accounts and exact
    company/confirmed-brand accounts with business-consistent samples. Generic
    industry accounts, missing samples, and unmatched LLM decisions do not pass.
    """
    raw_present = isinstance(raw, dict) and bool(raw)
    decision = dict(raw or {})
    profile, title_text, titles = _joined_text(account, sample)
    all_text = f"{profile} {title_text}"
    signals = _company_signals(company, aliases)
    exact_matches = [(value, strong) for value, strong in signals if value in all_text]
    name_text = " ".join(_text(account.get(key)) for key in ("username", "nickname", "account_name"))
    name_matches = [(value, strong) for value, strong in signals if value in name_text]
    profile_matches = [(value, strong) for value, strong in signals if value in profile]
    title_matches = [(value, strong) for value, strong in signals if value in title_text]
    certified = _is_certified(profile, company)
    business_content = _has_business_content(title_text) or _has_business_content(profile)
    sample_available = bool(titles) or int(sample.get("available_post_count") or 0) > 0
    raw_role = _text(decision.get("account_role"))
    person_like = any(token in name_text for token in ("老板", "老师", "经理", "客服", "助理", "小苏", "小小", "姝思", "老苗", "攀哥"))
    llm_confidence = decision.get("confidence", 0.0)
    try:
        llm_confidence = max(0.0, min(1.0, float(llm_confidence)))
    except (TypeError, ValueError):
        llm_confidence = 0.0

    normalized_account_ids = {
        _text(account.get(key)).casefold()
        for key in ("account_id", "douyin_id", "red_id", "kwai_id", "user_id", "sec_uid")
        if _text(account.get(key))
    }
    confirmed_ids = {
        _text(raw_alias.get("alias_value") or raw_alias.get("value")).casefold()
        for raw_alias in (aliases if isinstance(aliases, list) else [])
        if isinstance(raw_alias, dict)
        and str(raw_alias.get("alias_type") or raw_alias.get("type") or "") == "platform_account_id"
        and bool(raw_alias.get("is_confirmed") or raw_alias.get("confirmed"))
        and (
            not _text(raw_alias.get("platform"))
            or not _text(account.get("platform"))
            or _text(raw_alias.get("platform")) == _text(account.get("platform"))
        )
    }
    confirmed_id_match = bool(normalized_account_ids & confirmed_ids)
    account_name_identity = _normalize_identity(_account_name(account))
    confirmed_account_names = {
        _normalize_identity(raw_alias.get("alias_value") or raw_alias.get("value"))
        for raw_alias in (aliases if isinstance(aliases, list) else [])
        if isinstance(raw_alias, dict)
        and str(raw_alias.get("alias_type") or raw_alias.get("type") or "") == "account_name"
        and bool(raw_alias.get("is_confirmed") or raw_alias.get("confirmed"))
        and (
            not _text(raw_alias.get("platform"))
            or not _text(account.get("platform"))
            or _text(raw_alias.get("platform")) == _text(account.get("platform"))
        )
    }
    confirmed_name_match = bool(account_name_identity and account_name_identity in confirmed_account_names)
    legal_name_match = account_name_identity == _normalize_identity(company)

    # Deterministic trusted identifiers and exact certification survive an
    # omitted/malformed LLM decision.  These are the highest-value accounts to
    # spend POST_CRAWL budget on.
    if confirmed_id_match:
        final_decision = "accept_main"
        role = "official_business"
        status = "verified"
        eligible = True
        reason = "账号平台标识与已确认企业账号标识精确一致。"
    elif certified:
        final_decision = "accept_main"
        role = "main_official" if raw_role not in {"employee_or_sales", "local_business"} else raw_role
        status = "verified"
        eligible = True
        reason = "企业认证主体与目标企业全称一致。"
    elif confirmed_name_match:
        final_decision = "accept_main"
        role = "official_business"
        status = "verified"
        eligible = True
        reason = "账号名称与已确认企业账号名称精确匹配。"
    elif legal_name_match:
        final_decision = "accept_main"
        role = "official_business"
        status = "verified"
        eligible = True
        reason = "账号名称与目标企业法定全称精确匹配。"
    # A missing/malformed decision remains fail-closed for untrusted accounts.
    elif not raw_present:
        final_decision = "pending_review"
        role = "high_relevance_candidate"
        status = "unknown"
        eligible = False
        reason = "LLM未返回该账号的明确判断，转待复核，不自动进入正式帖子抓取。"
    elif name_matches and business_content and (profile_matches or title_matches):
        if person_like or raw_role in {"employee_or_sales", "local_business"}:
            final_decision = "accept_related"
            role = raw_role if raw_role in {"employee_or_sales", "local_business"} else "employee_or_sales"
            status = "related"
            eligible = False
            reason = "账号名称、企业/品牌信号与珠宝业务内容相关，但更像员工、销售或门店账号。"
        else:
            final_decision = "accept_main"
            role = "official_business" if raw_role == "official_business" else "high_relevance_candidate"
            status = "suspected" if llm_confidence < 0.85 else "suspected"
            eligible = False
            reason = "账号名称含精确企业/品牌信号，且主页或近期帖子同时出现对应业务证据。"
    elif name_matches and not sample_available and (certified or any(strong for _, strong in name_matches)):
        final_decision = "accept_main"
        role = "official_business" if raw_role == "official_business" else "high_relevance_candidate"
        status = "suspected"
        eligible = False
        reason = "账号名称含强企业/品牌信号，帖子样本暂不可用，保留以避免漏筛主账号。"
    elif exact_matches and business_content and llm_confidence >= 0.85 and not person_like:
        final_decision = "accept_main"
        role = "official_business" if raw_role == "official_business" else "high_relevance_candidate"
        status = "suspected"
        eligible = False
        reason = "企业/品牌信号与珠宝业务内容一致，且LLM置信度较高；保留为主账号候选。"
    elif exact_matches and (person_like or raw_role in {"employee_or_sales", "local_business"}) and business_content:
        final_decision = "accept_related"
        role = raw_role if raw_role in {"employee_or_sales", "local_business"} else "employee_or_sales"
        status = "related"
        eligible = False
        reason = "有企业/品牌信号和行业内容，但账号角色更像员工、销售或门店账号。"
    else:
        final_decision = "reject" if sample_available else "pending_review"
        role = "unrelated" if sample_available else "high_relevance_candidate"
        status = "rejected" if sample_available else "unknown"
        eligible = False
        reason = (
            "样本内容仅体现泛珠宝/泛行业相关，未形成目标企业归属证据。"
            if sample_available else
            "账号样本不足，无法确认企业归属，转待复核。"
        )

    evidence = decision.get("evidence")
    if isinstance(evidence, str):
        evidence = [evidence]
    if not isinstance(evidence, list):
        evidence = []
    if certified:
        evidence.insert(0, "企业认证主体与目标企业全称一致")
    if confirmed_id_match:
        evidence.insert(0, "平台账号标识与已确认标识精确一致")
    if confirmed_name_match:
        evidence.insert(0, "账号名称与已确认企业账号名称精确匹配")
    if legal_name_match:
        evidence.insert(0, "账号名称与目标企业法定全称精确匹配")
    if name_matches:
        evidence.append("账号名称含精确企业/品牌信号：" + ", ".join(value for value, _ in name_matches))
    if title_matches:
        evidence.append("近期帖子出现企业/品牌信号：" + ", ".join(value for value, _ in title_matches))
    if business_content:
        evidence.append("主页或近期帖子包含珠宝业务内容")
    return {
        **account,
        "account_name": _account_name(account),
        "eligible_for_post_crawl": eligible,
        "decision": final_decision,
        "account_role": role,
        "official_status": status,
        "confidence": llm_confidence,
        "reason": reason,
        "evidence": [_text(item) for item in evidence if _text(item)][:5],
        "review_sample": sample,
    }


def build_prompt(company: str, evidence: dict[str, list[dict[str, Any]]], aliases: Any = None) -> str:
    return (
        f"Review social-account ownership for target company: {company}.\n"
        "The accounts were already shortlisted from search metadata. Decide whether each account is a main official account, "
        "a related employee/store/sales account, unrelated, or pending review. Main-account recall matters, but generic "
        "jewelry accounts must not be treated as belonging to the target company.\n"
        "Rules:\n"
        "1. Certification whose subject exactly matches the target company is sufficient for accept_main.\n"
        "2. Without certification, require at least two independent ownership signals: exact company/confirmed brand signal "
        "plus profile/bio/company contact/website evidence or repeated recent posts containing the exact target signal.\n"
        "3. Generic jewelry, gold, Shenzhen, Shuibei, shop, live-commerce, or same-industry content is not ownership evidence.\n"
        "4. A person, employee, salesperson, manager, customer-service, branch, or local-store account may be accept_related, "
        "but must not be classified as main_official.\n"
        "5. If recent samples are empty, unavailable, or mostly unrelated, do not accept merely because the username is similar. "
        "Use pending_review when evidence is insufficient; use reject when the sampled content conflicts or is clearly generic.\n"
        "6. An exact brand/account alias from the supplied alias list may protect recall, but it still needs a profile or post "
        "business signal unless it is a confirmed platform account ID.\n"
        "Return strict JSON keyed by platform. Each platform must contain decisions for every supplied account:\n"
        "{\"douyin\":{\"decisions\":[{\"user_id\":\"...\",\"account_id\":\"...\","
        "\"username\":\"...\",\"eligible_for_post_crawl\":true,"
        "\"decision\":\"accept_main|accept_related|reject|pending_review\","
        "\"account_role\":\"main_official|official_business|local_business|employee_or_sales|high_relevance_candidate|unrelated\","
        "\"official_status\":\"verified|suspected|related|rejected|unknown\",\"confidence\":0.0,"
        "\"reason\":\"...\",\"evidence\":[\"...\"]}]},\"xhs\":{\"decisions\":[]},"
        "\"kuaishou\":{\"decisions\":[]}}.\n"
        "Return one decision for every supplied account. Missing or malformed decisions must be treated as pending_review, not accepted.\n"
        + "Known aliases and confirmed identifiers:\n" + json.dumps(aliases or [], ensure_ascii=False) + "\n"
        + json.dumps(evidence, ensure_ascii=False)
    )


def _decision_index(parsed_platform: Any) -> list[dict[str, Any]]:
    if isinstance(parsed_platform, dict):
        decisions = parsed_platform.get("decisions") or parsed_platform.get("accounts") or []
    elif isinstance(parsed_platform, list):
        decisions = parsed_platform
    else:
        decisions = []
    return [item for item in decisions if isinstance(item, dict)]


def _match_decision(account: dict[str, Any], decisions: list[dict[str, Any]]) -> dict[str, Any] | None:
    identities = _identity_values(account)
    name = _account_name(account)
    for decision in decisions:
        decision_ids = _identity_values(decision)
        if identities & decision_ids or name in decision_ids:
            return decision
    return None


def normalize_decision(account: dict[str, Any], sample: dict[str, Any], raw: dict[str, Any] | None) -> dict[str, Any]:
    decision = dict(raw or {})
    eligible = decision.get("eligible_for_post_crawl")
    if not isinstance(eligible, bool):
        eligible = True
    role = _text(decision.get("account_role"))
    if role not in VALID_ROLES:
        role = "high_relevance_candidate" if eligible else "unrelated"
    official_status = _text(decision.get("official_status"))
    if official_status not in VALID_OFFICIAL_STATUSES:
        official_status = "suspected" if eligible else "rejected"
    try:
        confidence = max(0.0, min(1.0, float(decision.get("confidence", 0.55))))
    except (TypeError, ValueError):
        confidence = 0.55
    evidence = decision.get("evidence")
    if isinstance(evidence, str):
        evidence = [evidence]
    if not isinstance(evidence, list):
        evidence = []
    titles = sample.get("sample_titles") if isinstance(sample.get("sample_titles"), list) else []
    if not evidence and titles:
        evidence = [f"近期标题样本：{_text(titles[0])[:100]}"]
    reason = _text(decision.get("reason"))
    if not reason:
        reason = "元数据初筛已入围；复核证据不足时按高召回策略进入有界正式抓取。"
    merged = {
        **account,
        "account_name": _account_name(account),
        "eligible_for_post_crawl": eligible,
        "account_role": role,
        "official_status": official_status,
        "confidence": confidence,
        "reason": reason,
        "evidence": [_text(item) for item in evidence if _text(item)][:5],
        "review_sample": sample,
    }
    return merged


def review_accounts(
    company: str,
    evidence: dict[str, list[dict[str, Any]]],
    parsed: Any,
    aliases: Any = None,
) -> dict[str, dict[str, Any]]:
    parsed = parsed if isinstance(parsed, dict) else {}
    results: dict[str, dict[str, Any]] = {}
    for platform in PLATFORMS:
        decisions = _decision_index(parsed.get(platform))
        accepted: list[dict[str, Any]] = []
        related: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for record in evidence.get(platform, []):
            account = record.get("account") if isinstance(record.get("account"), dict) else {}
            sample = record.get("sample") if isinstance(record.get("sample"), dict) else {}
            normalized = _apply_ownership_gate(
                company, account, sample, _match_decision(account, decisions), aliases,
            )
            if normalized["decision"] == "accept_main":
                accepted.append(normalized)
            elif normalized["decision"] == "accept_related":
                related.append(normalized)
            else:
                rejected.append(normalized)
        results[platform] = {
            "reviewed_count": len(evidence.get(platform, [])),
            "accepted_accounts": accepted,
            "related_accounts": related,
            "rejected_accounts": rejected,
        }
    return results


def load_review_evidence(value: str) -> dict[str, Any]:
    path = Path(value)
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            return loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}
    try:
        loaded = json.loads(value)
        return loaded if isinstance(loaded, dict) else {}
    except json.JSONDecodeError:
        return {}


def main() -> int:
    config = load_llm_config()
    parser = argparse.ArgumentParser(description="Review shortlisted accounts using profile and recent-post samples.")
    parser.add_argument("company")
    parser.add_argument("review_evidence", help="review evidence JSON file path or inline JSON")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-file")
    parser.add_argument("--model", default=os.environ.get("LLM_MODEL", config["model"]))
    parser.add_argument("--llm-timeout", type=int, default=180)
    args = parser.parse_args()

    raw_evidence = load_review_evidence(args.review_evidence)
    evidence = normalize_evidence(raw_evidence)
    parsed: Any = {}
    if config["api_key"]:
        try:
            content = call_llm(
                build_prompt(args.company, evidence, raw_evidence.get("aliases") if isinstance(raw_evidence, dict) else []),
                config["base_url"],
                args.model,
                config["api_key"],
                args.llm_timeout,
                system_message="Return only strict JSON. Protect exact certified/company accounts, but do not accept generic industry accounts; missing decisions are pending_review.",
            )
            parsed = parse_llm_json(content)
        except Exception as exc:
            print(f"{now_stamp()} {TASK_LABEL} {args.company} llm fallback: {exc}", file=sys.stderr, flush=True)
    else:
        print(f"{now_stamp()} {TASK_LABEL} {args.company} no API key; using recall-safe fallback", file=sys.stderr, flush=True)

    aliases = raw_evidence.get("aliases") if isinstance(raw_evidence, dict) else []
    result = {
        "schema_version": 1,
        "record_type": "account_review",
        "company": args.company,
        "generated_at": iso_utc(),
        "results": review_accounts(args.company, evidence, parsed, aliases),
    }
    output_path = resolve_output_path(args, args.company)
    write_json(output_path, result)
    print(f"{now_stamp()} {TASK_LABEL} {args.company} success {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
