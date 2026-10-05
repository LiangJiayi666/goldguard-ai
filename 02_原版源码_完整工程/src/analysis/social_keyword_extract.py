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

from pipeline_runtime import atomic_result, iso_utc, resolve_output, safe_name, write_json
from llm_client import call_llm, load_llm_config, parse_llm_json


ROOT = Path(__file__).parent
DEFAULT_OUTPUT_DIR = ROOT / "output"
TASK_LABEL = "social keyword extract"
PLATFORMS = {
    "douyin": {"label": "douyin", "keywords": ("douyin", "dy")},
    "xhs": {"label": "xhs", "keywords": ("xiaohongshu", "xhs")},
    "kuaishou": {"label": "kuaishou", "keywords": ("kuaishou", "ks")},
}
MAX_KEYWORDS_PER_PLATFORM = 10
# 收口子：LLM 关键词在全平台去重后最多保留这么多，避免查询任务与候选账号爆炸。
MAX_TOTAL_KEYWORDS = 3
ALIAS_KEYWORD_TYPES = {
    "legal_name", "short_name", "brand_name", "account_name",
    "platform_account_id", "icp_service_name", "website_brand",
}
_GENERIC_CANDIDATES = {
    "小程序", "公众号", "服务号", "订阅号", "视频号", "官方号", "账号",
    "微信", "抖音", "小红书", "快手", "微博", "app", "官网", "商城", "旗舰店",
    "详情", "查看详情", "点击查看", "搜索", "搜一搜", "扫码", "二维码",
}
_GENERIC_PREFIXES = (
    "可以", "可在", "即可", "用于", "提供", "支持", "进行", "我们", "用户",
    "详情", "查看", "点击", "进入", "打开", "搜索", "扫码", "关注", "购买", "下单",
    "访问", "了解", "办理", "使用", "通过",
)
_NON_ENTITY_PHRASES = re.compile(
    r"(?:出现异常|查找网点|前往体验|欢迎体验|轻松查询|点击查看|查看详情|"
    r"了解详情|立即下单|还我工资|恶意欠薪|拖欠工资|垃圾|骗子|打工人)"
)
_NON_ENTITY_ENDINGS = (
    "出现异常了", "异常了", "前往体验", "欢迎体验", "轻松查询",
    "查找网点", "点击查看", "查看详情", "了解详情", "立即下单",
)
_EXTRACTABLE_ALIAS_TYPES = {
    "brand_name", "account_name", "platform_account_id",
    "icp_service_name", "website_brand",
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def short_name(full: str) -> str:
    name = re.sub(r"\(.*?\)|（.*?）", "", full).strip()
    name = re.sub(r"(有限公司|有限责任公司|股份有限公司)$", "", name).strip()
    return name or full


def resolve_output_path(args: argparse.Namespace, company: str, suffix: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(company)}_{suffix}")


def _alias_value(item: Any) -> str:
    return str(item.get("alias_value") or item.get("value") or "").strip() if isinstance(item, dict) else ""


def _normalized_aliases(aliases: Any) -> list[dict[str, Any]]:
    if not isinstance(aliases, list):
        return []
    result: list[dict[str, Any]] = []
    for raw in aliases:
        if not isinstance(raw, dict):
            continue
        value = _alias_value(raw)
        alias_type = str(raw.get("alias_type") or raw.get("type") or "").strip()
        if not value or alias_type not in ALIAS_KEYWORD_TYPES:
            continue
        result.append({
            "alias_type": alias_type,
            "alias_value": value,
            "platform": str(raw.get("platform") or "").strip(),
            "source_type": str(raw.get("source_type") or raw.get("source") or "unknown"),
            "source_ref": raw.get("source_ref"),
            "confidence": float(raw.get("confidence") or 0.0),
            "is_confirmed": bool(raw.get("is_confirmed") or raw.get("confirmed")),
        })
    return result


def prepare_texts_for_llm(texts: list[str], limit: int = 40) -> list[str]:
    """Normalize input while retaining discovered-account clues for the AI to judge."""
    result: list[str] = []
    seen: set[str] = set()
    for raw in texts:
        text = str(raw or "").strip()
        legacy_account = re.match(r"^当前已发现账号\s*[：:]\s*(.+)$", text)
        if legacy_account:
            text = f"【已发现账号线索｜仅供判断，不等于企业载体】{legacy_account.group(1).strip()}"
        text = re.sub(r"#[^#\s]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def build_prompt(platform_label: str, company: str, texts: list[str], aliases: Any = None) -> str:
    prepared = prepare_texts_for_llm(texts)
    context = "\n".join(f"[正文{i + 1}] {text}" for i, text in enumerate(prepared)) or "（无正文）"
    known = [
        {
            "type": item["alias_type"],
            "value": item["alias_value"],
            "platform": item["platform"],
            "source": item["source_type"],
            "confidence": item["confidence"],
            "confirmed": item["is_confirmed"],
        }
        for item in _normalized_aliases(aliases)
    ]
    return (
        "你是企业社交账号检索词编辑器。企业名、历史别名、当前已发现账号和正文都是供你判断的输入线索；"
        "最终 keywords 只采用你在 JSON 中明确列出的内容，系统不会确定性补词。\n\n"
        f"目标企业：{company}\n"
        f"历史名称/账号线索（含未确认项，必须由你判断，不能直接照抄）：{json.dumps(known, ensure_ascii=False)}\n"
        f"目标平台：{platform_label}\n\n"
        "以下是内容正文。正文作者、发帖账号和负面评论者不一定属于目标企业：\n"
        f"{context}\n\n"
        "只返回合法 JSON，不要解释，结构必须为：\n"
        '{"keywords":{"douyin":["词1","词2"],"xhs":["词1","词2"],"kuaishou":["词1","词2"]},'
        '"alias_candidates":[{"value":"名称","type":"brand_name|account_name|platform_account_id|icp_service_name|website_brand",'
        '"evidence":"含该名称的正文原句"}]}\n\n'
        f"每个平台给 1-3 个最终搜索词；整个企业去重后最多 {MAX_TOTAL_KEYWORDS} 个不同关键词，"
        "最好只给 1 个最适合搜索该企业的简称，并严格遵守：\n"
        "1. 关键词只能是可独立检索的专有名称或账号 ID：企业简称、品牌、门店、官方账号、"
        "小程序、公众号、视频号、APP、商城。不要输出行业词、风险词、宣传动作或句子片段。\n"
        "2. 每个平台都要包含一个最适合搜索该企业的简称；可从企业登记名中做常规去地域/去公司后缀的简称判断。\n"
        "3. 其他名称必须逐字出现在正文或可信名称中；不得联想、改写、补全，也不得把名称和后续动作拼在一起。\n"
        "4. 必须理解句法：‘进入兑金通小程序查找网点’中的实体是‘兑金通’，"
        "不是‘查找网点’或‘兑金通小程序查找网点’。\n"
        "5. 排除投诉者、维权者、普通个人、正文作者及负面账号名。"
        "例如‘被金雅福恶意欠薪的打工人’不是企业载体。\n"
        "标为‘已发现账号线索’的名称仍要阅读其语义和正文证据：只有确属企业品牌、门店、"
        "官方/业务账号且值得跨平台继续搜索时才可输出；投诉者和个人账号必须排除。\n"
        "6. 明确禁止这些类型的残片：‘都出现异常了’、‘查找网点前往体验’、"
        "‘上可轻松查询’、‘点击查看详情’。\n"
        "7. 同一名称可跨三个平台搜索；没有平台专属差异时，三个列表应保持一致；"
        f"三个平台合计的不同关键词不超过 {MAX_TOTAL_KEYWORDS} 个（最好 1 个）。\n"
        "8. alias_candidates 只列 keywords 中除企业登记名/简称外的新实体，并准确填写类型和正文证据。\n\n"
        "正例：正文‘详情可以看小程序：煌泰珠宝’ → 关键词可含‘煌泰珠宝’。\n"
        "正例：正文‘可进入兑金通小程序查找网点’ → 关键词可含‘兑金通’。\n"
        "反例：正文‘我的账号都出现异常了’ → 不能输出‘都出现异常了’。"
    )


def _clean_candidate(value: Any) -> str:
    candidate = re.sub(r"\s+", " ", str(value or "")).strip()
    candidate = candidate.strip(" \t\r\n'\"“”‘’《》【】[]()（）<>：:，,。.!！?？;；")
    if not (2 <= len(candidate) <= 30):
        return ""
    if candidate in _GENERIC_CANDIDATES:
        return ""
    if candidate.startswith(_GENERIC_PREFIXES):
        return ""
    if candidate.startswith(("被", "我的", "我们", "当前")):
        return ""
    if _NON_ENTITY_PHRASES.search(candidate) or candidate.endswith(_NON_ENTITY_ENDINGS):
        return ""
    if candidate.endswith(("了", "吗", "呢", "吧")):
        return ""
    if re.search(r"https?://|www\.", candidate, re.I):
        return ""
    return candidate


def _clean_alias_candidate(value: Any) -> str:
    """Apply only structural cleanup; the model owns alias semantics."""
    candidate = re.sub(r"\s+", " ", str(value or "")).strip()
    return candidate.strip(" \t\r\n'\"“”‘’《》【】[]()（）<>：:，,。.!！?？;；")


def _unique_candidates(values: list[Any], limit: int = MAX_KEYWORDS_PER_PLATFORM) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = _clean_candidate(value)
        key = candidate.casefold()
        if candidate and key not in seen:
            seen.add(key)
            result.append(candidate)
        if len(result) >= limit:
            break
    return result


def _cap_total_keywords(by_platform: dict[str, list[str]], limit: int) -> dict[str, list[str]]:
    """把 LLM 关键词按全平台去重后压到 limit 个，优先保留各平台首个词，避免平台被清零。"""
    anchors: list[str] = []
    seen: set[str] = set()
    for platform in PLATFORMS:
        values = by_platform.get(platform) or []
        if not values:
            continue
        first = values[0]
        key = first.casefold()
        if key not in seen:
            seen.add(key)
            anchors.append(first)

    ordered = list(anchors[:limit])
    for platform in PLATFORMS:
        for value in by_platform.get(platform) or []:
            key = value.casefold()
            if key not in seen:
                seen.add(key)
                ordered.append(value)

    keep = {value.casefold() for value in ordered[:limit]}
    return {
        platform: [value for value in (by_platform.get(platform) or []) if value.casefold() in keep]
        for platform in PLATFORMS
    }


def _corpus_supports(
    company: str,
    keyword: str,
    texts: list[str],
    aliases: Any = None,
    platform: str = "",
) -> bool:
    """Validate an AI-produced keyword without creating or appending any keyword."""
    keyword = keyword.strip()
    if not keyword:
        return False
    if keyword in {company, short_name(company)}:
        return True
    # 企业简称由 AI 决定；本地只验证它确实是登记名中的连续片段，不负责生成。
    if len(keyword) >= 3 and keyword.casefold() in company.casefold():
        return True
    lowered = keyword.lower()
    if any(lowered in text.lower() for text in prepare_texts_for_llm(texts)):
        return True
    return any(
        lowered == item["alias_value"].casefold()
        and (not item["platform"] or item["platform"] == platform)
        for item in _normalized_aliases(aliases)
    )


def _confirmed_search_terms(aliases: Any, platform: str) -> list[str]:
    """Return user/verification-confirmed names and IDs that must be searched."""
    result: list[str] = []
    for item in _normalized_aliases(aliases):
        if not item["is_confirmed"]:
            continue
        if item["platform"] and item["platform"] != platform:
            continue
        if item["alias_type"] not in {
            "brand_name", "account_name", "platform_account_id",
            "website_brand", "icp_service_name",
        }:
            continue
        value = _clean_candidate(item["alias_value"])
        if value and value.casefold() not in {existing.casefold() for existing in result}:
            result.append(value)
    return result


def extract_keyword(
    platform: str,
    company: str,
    texts: list[str],
    base_url: str,
    model: str,
    api_key: str | None,
    timeout: int,
) -> tuple[list[str], list[str]]:
    keywords, evidence, _ = extract_keywords_once(
        company, texts, base_url, model, api_key, timeout,
    )
    return keywords.get(platform, []), evidence.get(platform, [])


def extract_keywords_once(
    company: str,
    texts: list[str],
    base_url: str,
    model: str,
    api_key: str | None,
    timeout: int,
    aliases: Any = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, bool]]:
    keywords, evidence, fallback, _ = extract_keywords_once_detailed(
        company, texts, base_url, model, api_key, timeout, aliases=aliases,
    )
    return keywords, evidence, fallback


def extract_keywords_once_detailed(
    company: str,
    texts: list[str],
    base_url: str,
    model: str,
    api_key: str | None,
    timeout: int,
    aliases: Any = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, bool], list[dict[str, Any]]]:
    prompt = build_prompt("all platforms", company, texts, aliases)
    raw = call_llm(
        prompt, base_url, model, api_key, timeout,
        system_message=(
            "Return only valid JSON. Every keyword must be your final entity-level search query; "
            "never output prose fragments, complaint-user names, or explanatory text."
        ),
    )
    payload = parse_llm_json(raw)
    values = payload.get("keywords") if isinstance(payload, dict) else None
    if not isinstance(values, dict):
        raise ValueError("LLM response missing keywords object")

    llm_by_platform: dict[str, list[str]] = {}
    for platform in PLATFORMS:
        raw_candidates = values.get(platform)
        if not isinstance(raw_candidates, list):
            raise ValueError(f"LLM keywords.{platform} must be a JSON list")
        llm_by_platform[platform] = [
            candidate for candidate in _unique_candidates(raw_candidates)
            if _corpus_supports(company, candidate, texts, aliases, platform)
        ]
    llm_by_platform = _cap_total_keywords(llm_by_platform, MAX_TOTAL_KEYWORDS)

    keywords: dict[str, list[str]] = {}
    for platform in PLATFORMS:
        accepted = _unique_candidates([
            *_confirmed_search_terms(aliases, platform),
            *llm_by_platform.get(platform, []),
        ])
        if not accepted:
            raise ValueError(f"LLM returned no valid grounded keywords for {platform}")
        keywords[platform] = accepted

    llm_aliases: list[dict[str, Any]] = []
    seen_aliases: set[str] = set()
    for item in payload.get("alias_candidates") or []:
        if not isinstance(item, dict):
            continue
        # ``alias_candidates`` is the model's semantic verdict.  Keep local
        # handling deliberately structural: normalize presentation, validate
        # the schema/type, and deduplicate.  Do not second-guess the verdict
        # with phrase blacklists, corpus substring checks, or keyword coupling.
        value = _clean_alias_candidate(item.get("value"))
        folded = value.casefold()
        alias_type = str(item.get("type") or item.get("alias_type") or "").strip()
        platform = str(item.get("platform") or "").strip()
        if (
            not value
            or folded in seen_aliases
            or alias_type not in _EXTRACTABLE_ALIAS_TYPES
        ):
            continue
        seen_aliases.add(folded)
        evidence_text = next(
            (text for text in prepare_texts_for_llm(texts) if folded in text.casefold()),
            str(item.get("evidence") or ""),
        )
        # A discovered-account label is circular evidence, not independent
        # proof that the name belongs to the enterprise.  Never persist it as
        # a reusable alias unless another corpus line independently supports it.
        independent_support = any(
            folded in text.casefold()
            and not text.startswith("【已发现账号线索｜")
            for text in prepare_texts_for_llm(texts)
        )
        if not independent_support:
            continue
        llm_aliases.append({
            "alias_type": alias_type,
            "alias_value": value,
            "platform": platform,
            "evidence": evidence_text[:500],
            "confidence": 0.7,
            "is_confirmed": False,
            "extraction_source": "llm",
        })

    prepared = prepare_texts_for_llm(texts)
    evidence = {
        platform: list(dict.fromkeys(
            text
            for keyword in platform_keywords
            for text in prepared
            if keyword.casefold() in text.casefold()
        ))[:5]
        for platform, platform_keywords in keywords.items()
    }
    fallback = {platform: False for platform in PLATFORMS}
    return keywords, evidence, fallback, llm_aliases


def build_keyword_metadata(
    company: str,
    texts: list[str],
    keywords: dict[str, list[str]],
    aliases: Any,
    llm_aliases: list[dict[str, Any]],
    *,
    corpus_kind: str,
    source_ref: str | None,
) -> tuple[dict[str, dict[str, str]], list[dict[str, Any]]]:
    candidates: dict[str, dict[str, Any]] = {
        item["alias_value"].casefold(): dict(item) for item in llm_aliases
    }
    sources = {
        platform: {keyword: "llm" for keyword in keywords.get(platform) or []}
        for platform in PLATFORMS
    }
    for item in candidates.values():
        item["source_type"] = "website" if corpus_kind == "website" else "post_corpus"
        item["source_ref"] = source_ref
    return sources, list(candidates.values())


def parse_texts(args: argparse.Namespace) -> list[str]:
    texts = []
    if getattr(args, "corpus_file", None):
        path = Path(args.corpus_file)
        if path.is_file():
            raw = path.read_text(encoding="utf-8-sig")
            if path.suffix.lower() == ".json":
                try:
                    payload = json.loads(raw)
                    if isinstance(payload, list):
                        texts.extend(str(item) for item in payload if str(item).strip())
                    elif isinstance(payload, dict):
                        for key in ("texts", "corpus", "items"):
                            value = payload.get(key)
                            if isinstance(value, list):
                                texts.extend(str(item) for item in value if str(item).strip())
                                break
                except Exception:
                    texts.extend(line.strip() for line in raw.splitlines() if line.strip())
            else:
                texts.extend(line.strip() for line in raw.splitlines() if line.strip())
    if getattr(args, "corpus_text", None):
        texts.append(args.corpus_text)
    return prepare_texts_for_llm([text for text in texts if text.strip()])


def load_keyword_input(value: str) -> dict[str, Any]:
    path = Path(value)
    if path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def main() -> int:
    config = load_llm_config()
    parser = argparse.ArgumentParser(description="Extract social media search keywords from company text.")
    parser.add_argument("company", help="enterprise name")
    parser.add_argument("corpus_input", nargs="?", default="", help="JSON file path or explicit source text")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="output directory")
    parser.add_argument("--output-file", help="output JSON file name")
    parser.add_argument("--corpus-file", help="deprecated: optional file containing additional corpus text")
    parser.add_argument("--round", type=int, default=1, help="keyword round number")
    parser.add_argument("--corpus-kind", default="explicit_corpus", help="source corpus kind")
    parser.add_argument("--model", default=os.environ.get("LLM_MODEL", config["model"]), help="LLM model name")
    parser.add_argument("--llm-timeout", type=int, default=60, help="LLM timeout in seconds")
    args = parser.parse_args()

    input_payload = load_keyword_input(args.corpus_input) if args.corpus_input else {}
    if input_payload:
        args.round = int(input_payload.get("round") or args.round)
        args.corpus_kind = str(input_payload.get("corpus_kind") or args.corpus_kind)
        corpus_file = str(input_payload.get("corpus_file") or "")
        if corpus_file:
            args.corpus_file = corpus_file
        corpus_text = str(input_payload.get("corpus_text") or "")
        if corpus_text:
            args.corpus_text = corpus_text

    texts = parse_texts(args)
    aliases = input_payload.get("aliases") if isinstance(input_payload, dict) else []
    short = short_name(args.company)
    keywords: dict[str, list[str]] = {}
    evidence: dict[str, list[str]] = {}
    fallback: dict[str, bool] = {}

    try:
        keywords, evidence, fallback, llm_aliases = extract_keywords_once_detailed(
            args.company,
            texts,
            config["base_url"],
            args.model,
            config["api_key"],
            args.llm_timeout,
            aliases=aliases,
        )
    except Exception as exc:
        output_path = resolve_output_path(args, args.company, "social_keywords.json")
        write_json(output_path, atomic_result({
            "schema_version": 2,
            "record_type": "final",
            "status": "error",
            "error_code": "llm_keyword_generation_failed",
            "reason": str(exc)[:1000],
            "company": args.company,
            "round": args.round,
            "corpus_kind": args.corpus_kind,
            "keywords": {platform: [] for platform in PLATFORMS},
            "source": "llm",
            "generation_mode": "llm_only",
        }, source="social_keyword_extract.llm", error_code="llm_keyword_generation_failed"))
        print(f"{now_stamp()} {TASK_LABEL} {args.company} llm failed: {exc}", file=sys.stderr, flush=True)
        return 1
    keyword_sources, alias_candidates = build_keyword_metadata(
        args.company,
        texts,
        keywords,
        aliases,
        llm_aliases,
        corpus_kind=args.corpus_kind,
        source_ref=str(args.corpus_file or input_payload.get("corpus_file") or "") or None,
    )

    result = {
        "schema_version": 2,
        "record_type": "final",
        "company": args.company,
        "short_name": short,
        "round": args.round,
        "corpus_kind": args.corpus_kind,
        "query_time": now_iso(),
        "generated_at": iso_utc(),
        "keywords": keywords,
        "keyword_sources": keyword_sources,
        "alias_candidates": alias_candidates,
        "evidence": evidence,
        "fallback": fallback,
        "source": "llm",
        "generation_mode": "llm_only",
    }
    output_path = resolve_output_path(args, args.company, "social_keywords.json")
    write_json(output_path, result)
    print(f"{now_stamp()} {TASK_LABEL} {args.company} success {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
