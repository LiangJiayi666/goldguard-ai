from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

"""One enterprise-level, evidence-grounded precious-metals risk review."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from llm_client import call_llm, load_llm_config, parse_llm_json
from pipeline_runtime import iso_utc, resolve_output, safe_name, write_json


ROOT = Path(__file__).parent
RISK_TERMS = [
    "黄金期货", "黄金TD交易", "黄金T+D交易", "黄金预定价", "黄金预订价", "上海黄金交易所会员", "黄金买涨买跌", "黄金锁价延期交易",
    "高收益", "稳赚不赔", "保证金", "补仓", "追保", "售后返租", "黄金交易平台", "售后保管", "代管",
    "寄售", "溢价回购", "黄金保管", "黄金委托租赁", "共享黄金", "黄金预售", "积分返利", "低价出售黄金",
    "黄金托管", "黄金寄存", "黄金理财", "黄金委托理财", "存金生息", "售后回租", "黄金质押", "保本",
]
REPORT_KEYWORDS = list(dict.fromkeys(RISK_TERMS + ["黄金回购"]))


class LLMResponseFormatError(RuntimeError):
    """The provider responded, but the model did not return a usable JSON object."""


EVIDENCE_PROMPT_BUDGET_CHARS = 100_000
EVIDENCE_TEXT_MAX_CHARS = 800


def _fit_evidence_to_budget(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Keep the evidence payload within the model context window.

    A company with thousands of keyword hits serializes to megabytes, the
    provider rejects the prompt outright (context window exceeded), and every
    run silently degrades to the conservative fallback.  When the payload is
    over budget, risk-term hits are kept first (they drive the S1-S5 review),
    each text is capped, and the remaining tail is dropped.  Returns the kept
    records (original order within each tier) and the dropped count.
    """
    if len(json.dumps(records, ensure_ascii=False)) <= EVIDENCE_PROMPT_BUDGET_CHARS:
        return records, 0
    risk_terms = set(REPORT_KEYWORDS) | {"锁价", "提前锁价", "延期交易", "预订价"}

    def is_risk_hit(item: dict[str, Any]) -> bool:
        return any(str(k) in risk_terms for k in (item.get("matched_keywords") or []))

    ordered = [r for r in records if isinstance(r, dict) and is_risk_hit(r)]
    ordered += [r for r in records if not (isinstance(r, dict) and is_risk_hit(r))]
    kept: list[dict[str, Any]] = []
    total = 2  # account for the surrounding "[]"
    for item in ordered:
        trimmed = dict(item)
        text = str(trimmed.get("text") or "")
        if len(text) > EVIDENCE_TEXT_MAX_CHARS:
            trimmed["text"] = text[:EVIDENCE_TEXT_MAX_CHARS] + "…"
        size = len(json.dumps(trimmed, ensure_ascii=False)) + 1
        if kept and total + size > EVIDENCE_PROMPT_BUDGET_CHARS:
            continue
        kept.append(trimmed)
        total += size
    return kept, len(records) - len(kept)


def prompt(company: str, keyword_hit_evidence: list[dict[str, Any]], non_keyword_evidence: list[dict[str, Any]],
           network_carriers: list[dict[str, Any]], coverage: dict[str, Any] | None = None) -> str:
    return f"""你是一名审慎的黄金业务合规风险分析师。只能依据下方证据作结论；关键词命中本身不是违法或风险事实。
直接输出 JSON，不要输出任何解释、思考过程或代码块标记；输出必须从 {{{{ 开始、以 }}}} 结束。

企业：{company}
证据规则：每个 finding 必须引用至少一个 evidence_id；不得杜撰网址、文字、交易方式或资质情况。若证据不足，结论必须为“无法判断”。

判定总原则（必须严格执行）：
1. 关键词只是召回入口，不能因为出现关键词就自动判风险；但必须阅读整段上下文做语义判断。若上下文已经表达风险机制、收益暗示、异常交易方式或风险业务功能，就必须判为“相关风险线索”，不能因尚未满足完整违法要件而归为普通业务。
2. 总体判定采用“任一有效风险线索即命中”，不是多数表决。大量普通回收、金价或实物交易证据，不能抵消另一条独立存在的“锁价+稳定收益”、代保管收益、价差结算、保证金交易等风险线索。
3. overall_risk_found 表示“是否发现至少一条有证据支持的风险线索”，不表示已经定性违法。出现以下任一情况时，overall_risk_found 必须为 true：
   a. 任一 S1-S5 的 present 为 yes；
   b. 任一 S1-S5 的 present 为 warning，且 evidence_ids 非空、reason 说明已出现部分风险要件；unclear 不算风险命中；
   c. 任一 risk_keyword_findings 的 assessment 为“相关风险线索”；
   d. 任一 finding 有非空 risk_signals，且 assessment 明确为风险线索或待核查线索；
   e. 有具体 evidence_id 的第三方投诉直接指称吸收资金、保本收益、非吸、资金挪用等涉金资金风险；此类投诉只算待核查线索，不得单独把 S1-S5 判为 yes。
4. 只有 S1-S5 全部为 no、全部关键词均属于普通业务表述、没有任何待核查线索时，overall_risk_found 才能为 false。
5. 语义一致性是硬约束：如果正文中写了“相关风险线索”“需进一步核实”“机制不明”并引用了有效证据，就禁止同时输出 overall_risk_found=false，也禁止在 executive_summary 中写“未发现明确风险线索”。
6. 对来源归属保持审慎：未被证据明确证明为官方的账号，只能写“疑似关联账号/品牌发布”；第三方账号只能写“第三方投诉称”。不得把疑似关联账号内容直接概括成目标企业已经确认开展的全部业务。
7. 某些渠道呈现实物交付、即时付款，只能说明这些已观察渠道的业务特征，不能据此排除同一企业或疑似关联品牌的其他锁价、预订价、托管或资金业务。

关键示例：
- “锁价”单独出现，且只是即时实物回收操作，可以是普通业务或无法判断；
- “锁价功能”同时出现“稳定收益、保本、赚取收益、金价波动获利、保障收益、收益保障、锁价超放心”等把锁价当作收益保障卖点的语义：必须认定为相关风险线索，S3 至少为 warning，overall_risk_found=true；回收场景中以上述卖点宣传“锁价”同样适用，不得仅因上下文出现“回收”就降级为普通业务或 unclear；
- “上海黄金交易所Au9999”仅作为回收报价参考：普通业务表述；
- 企业或平台明确提供黄金期货、黄金TD/T+D开户或交易服务：S5=yes，overall_risk_found=true；
- 第三方仅笼统辱骂不算风险；第三方具体指称“涉嫌非吸、吸收客户资金、承诺固定收益”等，且有证据原文时，计为待核查风险线索，但不得直接定性。

请审查：
1. 是否存在以下风险词/同义表述：{', '.join(REPORT_KEYWORDS)}。keyword_assessments 必须逐项覆盖上述每一个关键词：有明确证据为 yes；出现部分相关线索且有 evidence_ids 为 warning；明确未命中为 no；LLM 未完成为 unclear。
2. 企业是否有可核实的经营业务：必须单独填写 business_involvement，present 只能为 yes|no|unclear；不得使用 warning。没有抓到可核实证据时填 unclear，不得把证据缺失写成风险告警。
3. 企业是否涉及黄金业务：必须单独填写 gold_business_involvement，present 只能为 yes|no|unclear；不得使用 warning。只有明确业务证据才填 yes，证据不足或 LLM 未完成填 unclear。
4. 是否含“黄金回购”；如有，判断是否明确为“一手交钱一手交货”。若未明确，结论为“无法判断”，不能推定不是。
5. 是否存在五类现象：
   S1 买金后不提货、代保管且承诺利息/收益；
   S2 买金后签黄金租赁合同并给租金回报；
   S3 预定价交易、按金价差结算且无实物交割（含微信群下单、回购小程序）；
   S4 境内机构或个人违规代理境外黄金交易，或声称代理境外交易但实际未对接境外市场并挪用客户资金；上海黄金交易所会员或代理资质真实性不属于 S4；
   S5 满足以下任一口径即可判定：
      a. 证据明确显示企业或其平台开展、提供黄金期货、黄金TD/黄金T+D的开户或交易服务；此类直接业务表述本身足以判定 S5=yes，不再要求另行证明“不以实物交收为目的”；
      b. 只缴一定比例保证金、可买可卖、集中竞价/电子撮合等，并允许对冲平仓而非实物交收。

返回 JSON，且只能使用这个结构：
{{
  "overall_risk_found": true,
  "executive_summary": "不超过200字；必须与overall_risk_found一致。true时以'发现风险线索'或'发现待核查风险线索'开头，false时才可写'未发现明确风险线索'；明确是证据线索而非定性违法",
  "business_involvement": {{"present":"yes|no|unclear","evidence_ids":[],"reason":"企业是否有可核实经营业务；证据不足时为unclear"}},
  "gold_business_involvement": {{"present":"yes|no|unclear","evidence_ids":["E0001"],"reason":"说明企业是否涉及黄金业务；证据不足时为unclear"}},
  "keyword_assessments": [{{"keyword":"黄金期货","present":"yes|no|warning|unclear","evidence_ids":[],"reason":"逐项回答该关键词是否涉及"}}],
  "risk_keyword_findings": [{{"keywords":["词"],"evidence_ids":["E0001"],"assessment":"相关风险线索/仅普通业务表述/无法判断","reason":"..."}}],
  "gold_buyback": {{"present": false,"delivery_assessment":"not_present|one_hand_money_one_hand_goods|not_one_hand_money_one_hand_goods|unclear","evidence_ids":[],"reason":"..."}},
  "risk_scenarios": [{{"scenario_id":"S1","present":"yes|warning|unclear","evidence_ids":[],"reason":"..."}}],
  "findings": [{{"evidence_id":"E0001","url":"证据中的 url 或 null","quoted_text":"证据文字中与结论直接相关的原文摘录","matched_keywords":["..."],"risk_signals":["..."],"assessment":"..."}}]
}}

命中既有关键词的证据（JSON）：
{json.dumps(keyword_hit_evidence, ensure_ascii=False)}

未命中既有关键词、但必须一并审阅的证据（JSON）：
{json.dumps(non_keyword_evidence, ensure_ascii=False)}

证据采集覆盖情况（JSON）：
{json.dumps(coverage or {}, ensure_ascii=False)}

最终分析只允许使用上面的“命中既有关键词的证据”。未命中关键词的正文不属于本次风险分析输入。

Observed network carriers (JSON):
{json.dumps(network_carriers, ensure_ascii=False)}

Also return three additional top-level JSON objects:
"business_model": {{"summary":"evidence-grounded description or unable to determine","business_activities":[{{"activity":"...","evidence_ids":["E0001"],"reason":"..."}}],"unknowns":["..."]}},
"network_carrier_assessment": {{"summary":"only observed carriers; do not infer official ownership","carriers":[{{"carrier_type":"website|domain|app|mini_program|social_account|purchase_channel|other","name":"...","url":null,"platform":null,"official_status":"confirmed|unverified|not_applicable","activity_status":"active|inactive|unknown","evidence_ids":[],"source_ref":"...","reason":"..."}}],"unknowns":["..."]}}.
For social accounts, use official_status=unverified unless the evidence explicitly proves ownership. A reachable domain is active, not proof of every service being usable. Do not invent carriers or purchase channels."""


def _clean_ids(value: Any, valid_ids: set[str]) -> list[str]:
    return [str(item) for item in value if str(item) in valid_ids] if isinstance(value, list) else []


def validate(payload: dict[str, Any], evidence: list[dict[str, Any]]) -> dict[str, Any]:
    valid_ids = {str(item["evidence_id"]) for item in evidence}
    evidence_by_id = {str(item["evidence_id"]): item for item in evidence}
    for key in ("risk_keyword_findings", "keyword_assessments", "risk_scenarios", "findings"):
        if not isinstance(payload.get(key), list):
            payload[key] = []
    buyback = payload.get("gold_buyback")
    if not isinstance(buyback, dict):
        payload["gold_buyback"] = {"present": False, "delivery_assessment": "not_present", "evidence_ids": [], "reason": "LLM未返回有效字段"}
        buyback = payload["gold_buyback"]
    gold_business = payload.get("gold_business_involvement")
    if not isinstance(gold_business, dict):
        gold_business = {"present": "unclear", "evidence_ids": [], "reason": "LLM未返回企业黄金业务判断字段"}
    if gold_business.get("present") not in {"yes", "no", "unclear"}:
        gold_business["present"] = "unclear"
    gold_business["evidence_ids"] = _clean_ids(gold_business.get("evidence_ids"), valid_ids)
    payload["gold_business_involvement"] = gold_business
    business = payload.get("business_involvement")
    if not isinstance(business, dict):
        business = {"present": "unclear", "evidence_ids": [], "reason": "LLM未返回企业经营业务判断字段"}
    if business.get("present") not in {"yes", "no", "unclear"}:
        business["present"] = "unclear"
    business["evidence_ids"] = _clean_ids(business.get("evidence_ids"), valid_ids)
    payload["business_involvement"] = business
    keyword_items = {str(item.get("keyword")): item for item in payload["keyword_assessments"] if isinstance(item, dict) and item.get("keyword")}
    if payload.get("analysis_mode") == "conservative_local_fallback":
        term_ids: dict[str, list[str]] = {}
        for finding in payload["risk_keyword_findings"]:
            if not isinstance(finding, dict):
                continue
            finding_ids = _clean_ids(finding.get("evidence_ids"), valid_ids)
            for term in finding.get("keywords") or []:
                bucket = term_ids.setdefault(str(term), [])
                bucket.extend(eid for eid in finding_ids if eid not in bucket)
        keyword_items = {
            term: {
                "keyword": term,
                "present": "warning" if term_ids.get(term) else "unclear",
                "evidence_ids": term_ids.get(term, []),
                "reason": "本地规则检出该关键词，LLM 未完成上下文语义判定。" if term_ids.get(term) else "LLM 服务不可用，未完成该关键词判断。",
            }
            for term in REPORT_KEYWORDS
        }
    payload["keyword_assessments"] = [
        keyword_items.get(term) or {
            "keyword": term,
            "present": "unclear",
            "evidence_ids": [],
            "reason": "LLM未返回该关键词的逐项判断，需复核。",
        }
        for term in REPORT_KEYWORDS
    ]
    scenario_items = {str(item.get("scenario_id")): item for item in payload["risk_scenarios"] if isinstance(item, dict) and item.get("scenario_id")}
    payload["risk_scenarios"] = [
        scenario_items.get(sid) or {
            "scenario_id": sid,
            "present": "unclear",
            "evidence_ids": [],
            "reason": "LLM未返回该风险场景的逐项判断，需复核。",
        }
        for sid in ("S1", "S2", "S3", "S4", "S5")
    ]
    for item in payload["keyword_assessments"]:
        if item.get("present") not in {"yes", "no", "warning", "unclear"}:
            item["present"] = "unclear"
        if item.get("present") == "warning" and not _clean_ids(item.get("evidence_ids"), valid_ids):
            item["present"] = "unclear"
    for item in payload["risk_scenarios"]:
        if item.get("present") not in {"yes", "warning"}:
            item["present"] = "unclear"
        if item.get("present") == "warning" and not _clean_ids(item.get("evidence_ids"), valid_ids):
            item["present"] = "unclear"
    for group in (payload["risk_keyword_findings"], payload["keyword_assessments"], payload["risk_scenarios"], payload["findings"], [buyback, gold_business, business]):
        for item in group:
            if isinstance(item, dict):
                item["evidence_ids"] = [x for x in item.get("evidence_ids", []) if str(x) in valid_ids]
                if "evidence_id" in item and str(item["evidence_id"]) not in valid_ids:
                    item["evidence_id"] = None
                if item.get("evidence_id"):
                    # URLs are evidence metadata, not an LLM inference.
                    item["url"] = evidence_by_id[str(item["evidence_id"])].get("url")
    # A positive risk conclusion without any surviving evidence reference is
    # invalid. Preserve the narrative for review, but never publish it as a hit.
    has_supported_finding = any(
        item.get("evidence_id") or item.get("evidence_ids")
        for item in payload["findings"] + payload["risk_keyword_findings"] + payload["risk_scenarios"]
        if isinstance(item, dict)
    )
    payload["overall_risk_found"] = bool(payload.get("overall_risk_found")) and bool(valid_ids) and has_supported_finding
    business = payload.get("business_model")
    if not isinstance(business, dict):
        business = {"summary": "unable to determine", "business_activities": [], "unknowns": ["missing analysis"]}
    activities = business.get("business_activities") if isinstance(business.get("business_activities"), list) else []
    business["business_activities"] = [{**item, "evidence_ids": _clean_ids(item.get("evidence_ids"), valid_ids)} for item in activities if isinstance(item, dict)]
    business["unknowns"] = business.get("unknowns") if isinstance(business.get("unknowns"), list) else []
    payload["business_model"] = business
    carriers = payload.get("network_carrier_assessment")
    if not isinstance(carriers, dict):
        carriers = {"summary": "unable to determine", "carriers": [], "unknowns": ["missing analysis"]}
    rows = carriers.get("carriers") if isinstance(carriers.get("carriers"), list) else []
    carriers["carriers"] = [{**item, "evidence_ids": _clean_ids(item.get("evidence_ids"), valid_ids),
        "official_status": item.get("official_status") if item.get("official_status") in {"confirmed", "unverified", "not_applicable"} else "unverified",
        "activity_status": item.get("activity_status") if item.get("activity_status") in {"active", "inactive", "unknown"} else "unknown"}
        for item in rows if isinstance(item, dict)]
    carriers["unknowns"] = carriers.get("unknowns") if isinstance(carriers.get("unknowns"), list) else []
    payload["network_carrier_assessment"] = carriers
    return payload


def _fallback_analysis(evidence: list[dict[str, Any]], network_carriers: list[dict[str, Any]], reason: str) -> dict[str, Any]:
    """Return a conservative, evidence-only result when the LLM is unavailable.

    A gateway outage must not turn into a false "no risk" conclusion or block
    the whole enterprise run.  This fallback records only literal risk-term
    hits and explicitly leaves semantic judgement to later manual/LLM review.
    """
    keyword_findings: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    buyback_ids: list[str] = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        evidence_id = str(item.get("evidence_id") or "").strip()
        text = str(item.get("text") or "")
        reported = item.get("matched_keywords") if isinstance(item.get("matched_keywords"), list) else []
        terms = list(dict.fromkeys(
            str(term) for term in reported if str(term) in RISK_TERMS
        ))
        terms.extend(term for term in RISK_TERMS if term in text and term not in terms)
        if "黄金回购" in text and evidence_id:
            buyback_ids.append(evidence_id)
        if not terms or not evidence_id:
            continue
        quoted = next((text[max(0, text.find(term) - 80): text.find(term) + len(term) + 160] for term in terms if term in text), text[:240])
        keyword_findings.append({
            "keywords": terms,
            "evidence_ids": [evidence_id],
            "assessment": "仅本地规则命中，需人工或 LLM 语义复核",
            "reason": "LLM 服务不可用，未对上下文完成风险语义判定。",
        })
        findings.append({
            "evidence_id": evidence_id,
            "url": item.get("url"),
            "quoted_text": quoted,
            "matched_keywords": terms,
            "risk_signals": terms,
            "assessment": "仅关键词线索，尚未完成语义判定。",
        })

    carrier_rows = []
    for carrier in network_carriers:
        if not isinstance(carrier, dict):
            continue
        activity = str(carrier.get("activity_status") or "").lower()
        carrier_rows.append({
            "carrier_type": str(carrier.get("carrier_type") or "other"),
            "name": str(carrier.get("name") or ""),
            "url": carrier.get("url"),
            "platform": carrier.get("platform"),
            "official_status": "unverified",
            "activity_status": activity if activity in {"active", "inactive", "unknown"} else "unknown",
            "evidence_ids": [],
            "source_ref": str(carrier.get("source") or ""),
            "reason": "仅保留已观察到的网络载体，未推断官方归属。",
        })

    observed_ids = [str(item.get("evidence_id")) for item in evidence if item.get("evidence_id")]
    observed_term_ids: dict[str, list[str]] = {}
    for item in keyword_findings:
        for term in item.get("keywords", []):
            bucket = observed_term_ids.setdefault(str(term), [])
            bucket.extend(eid for eid in item.get("evidence_ids", []) if eid not in bucket)
    keyword_assessments = [
        {"keyword": term, "present": "warning" if observed_term_ids.get(term) else "unclear",
         "evidence_ids": observed_term_ids.get(term, []),
         "reason": "本地规则检出该关键词，LLM 未完成上下文语义判定。" if observed_term_ids.get(term) else "LLM 服务不可用，未完成该关键词判断。"}
        for term in REPORT_KEYWORDS
    ]
    return {
        "overall_risk_found": False,
        "executive_summary": "LLM 服务不可用，未完成语义风险判定；本地规则仅保留明确风险词线索，不能据此判定无风险。",
        "business_involvement": {
            "present": "unclear",
            "evidence_ids": [],
            "reason": "LLM 服务不可用，未完成企业经营业务语义判断。",
        },
        "gold_business_involvement": {
            "present": "unclear",
            "evidence_ids": [],
            "reason": "LLM 服务不可用，无法完成企业是否涉及黄金业务的语义判断；仅凭现有证据待复核。" if evidence else "未获得可用证据。",
        },
        "keyword_assessments": keyword_assessments,
        "risk_keyword_findings": keyword_findings,
        "gold_buyback": {
            "present": bool(buyback_ids),
            "delivery_assessment": "unclear" if buyback_ids else "not_present",
            "evidence_ids": buyback_ids,
            "reason": "仅检测到“黄金回购”字面表述，未完成交付方式语义审查。" if buyback_ids else "未发现“黄金回购”字面表述。",
        },
        "risk_scenarios": [
            {"scenario_id": sid, "present": "unclear", "evidence_ids": [], "reason": "LLM 服务不可用，未完成该风险场景判断。"}
            for sid in ("S1", "S2", "S3", "S4", "S5")
        ],
        "findings": findings,
        "business_model": {
            "summary": "LLM 服务不可用，无法完成业务模式语义归纳。",
            "business_activities": [],
            "unknowns": ["需要恢复 LLM 后进行证据语义审查。"],
        },
        "network_carrier_assessment": {
            "summary": "仅列示已观察到的网络载体；未完成归属或业务用途推断。",
            "carriers": carrier_rows,
            "unknowns": ["LLM 服务不可用，尚未完成综合研判。"],
        },
        "analysis_mode": "conservative_local_fallback",
        "analysis_warning": f"LLM unavailable: {reason[:500]}",
    }


def _request_risk_analysis(
    company: str,
    keyword_hit_evidence: list[dict[str, Any]],
    non_keyword_evidence: list[dict[str, Any]],
    network_carriers: list[dict[str, Any]],
    coverage: dict[str, Any],
    config: dict[str, str],
    model: str,
    timeout: int,
) -> dict[str, Any]:
    """Retry only model-format failures; transport failures keep their normal path."""
    base_prompt = prompt(
        company, keyword_hit_evidence, non_keyword_evidence, network_carriers, coverage,
    )
    system_message = "Return only valid JSON. Be evidence-grounded and conservative."
    format_errors: list[str] = []
    for attempt in range(2):
        request_prompt = base_prompt
        request_system = system_message
        if attempt:
            request_prompt += (
                "\n\nYour previous response was not valid JSON. Return exactly one valid JSON "
                "object, with correctly escaped strings and no markdown or prose."
            )
            request_system += " Return one syntactically valid JSON object only."
        raw = call_llm(
            request_prompt,
            config["base_url"],
            model,
            config["api_key"],
            timeout,
            max_tokens=8192,
            system_message=request_system,
        )
        try:
            parsed = parse_llm_json(raw)
        except json.JSONDecodeError as exc:
            format_errors.append(f"attempt {attempt + 1}: {exc}")
            continue
        if isinstance(parsed, dict):
            return parsed
        # Some providers wrap the requested object in a one-element JSON
        # array. The payload is still complete and unambiguous, so unwrap only
        # that exact shape. Multiple objects must not be merged implicitly.
        if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
            return parsed[0]
        format_errors.append(f"attempt {attempt + 1}: JSON root is {type(parsed).__name__}, expected object")
    raise LLMResponseFormatError(
        "LLM returned invalid JSON after retry: " + "; ".join(format_errors)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze keyword-hit enterprise text for precious-metals risks.")
    parser.add_argument("company")
    parser.add_argument("evidence_file", type=Path)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--output-file")
    parser.add_argument("--model")
    parser.add_argument("--llm-timeout", type=int, default=180)
    args = parser.parse_args()
    output = resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(args.company)}_risk_analysis.json")
    try:
        source = json.loads(args.evidence_file.read_text(encoding="utf-8-sig"))
        keyword_hit_evidence = source.get("keyword_hit_evidence", []) if isinstance(source, dict) else []
        non_keyword_evidence = source.get("non_keyword_evidence", []) if isinstance(source, dict) else []
        network_carriers = source.get("network_carriers", []) if isinstance(source, dict) else []
        coverage = source.get("coverage", {}) if isinstance(source, dict) else {}
        # Compatibility with evidence files created before the two-group format.
        if isinstance(source, dict) and "evidence" in source:
            keyword_hit_evidence = source.get("evidence", [])
            non_keyword_evidence = []
        if not isinstance(keyword_hit_evidence, list) or not isinstance(non_keyword_evidence, list):
            raise ValueError("evidence groups must be lists")
        if not isinstance(network_carriers, list):
            raise ValueError("network_carriers must be a list")
        keyword_hit_evidence, dropped_hits = _fit_evidence_to_budget(keyword_hit_evidence)
        non_keyword_evidence, dropped_non_keyword = _fit_evidence_to_budget(non_keyword_evidence)
        if dropped_hits or dropped_non_keyword:
            coverage = dict(coverage) if isinstance(coverage, dict) else {}
            coverage["evidence_trimmed_for_context"] = {
                "dropped_keyword_hits": dropped_hits,
                "dropped_non_keyword": dropped_non_keyword,
                "budget_chars": EVIDENCE_PROMPT_BUDGET_CHARS,
                "text_max_chars": EVIDENCE_TEXT_MAX_CHARS,
            }
        evidence = keyword_hit_evidence + non_keyword_evidence
        config = load_llm_config()
        try:
            parsed = _request_risk_analysis(
                args.company,
                keyword_hit_evidence,
                non_keyword_evidence,
                network_carriers,
                coverage,
                config,
                args.model or os.environ.get("LLM_MODEL", config["model"]),
                args.llm_timeout,
            )
            analysis = validate(parsed, evidence)
        except Exception as exc:
            format_error = isinstance(exc, LLMResponseFormatError)
            failure_kind = "LLM response format invalid after retry" if format_error else "LLM unavailable"
            print(
                f"enterprise risk analysis {args.company}: {failure_kind}; "
                f"using conservative local fallback: {exc}",
                file=sys.stderr,
                flush=True,
            )
            analysis = validate(_fallback_analysis(evidence, network_carriers, str(exc)), evidence)
            if format_error:
                analysis["analysis_warning_code"] = "response_format_invalid"
                analysis["analysis_warning"] = f"LLM response format invalid after retry: {str(exc)[:500]}"
                analysis["executive_summary"] = (
                    "\u6a21\u578b\u5df2\u54cd\u5e94\uff0c\u4f46\u8fde\u7eed\u4e24\u6b21"
                    "\u672a\u8fd4\u56de\u5408\u6cd5 JSON\uff1b\u672a\u5b8c\u6210\u8bed\u4e49"
                    "\u98ce\u9669\u5224\u5b9a\uff0c\u672c\u5730\u89c4\u5219\u4ec5\u4fdd\u7559"
                    "\u660e\u786e\u98ce\u9669\u8bcd\u7ebf\u7d22\u3002"
                )
        # The prompt is grouped to help the LLM compare both populations, but
        # consumers receive one evidence list and can filter on this flag.
        output_evidence = [
            {**item, "keyword_matched": True} for item in keyword_hit_evidence
        ] + [
            {**item, "keyword_matched": False} for item in non_keyword_evidence
        ]
        result = {"schema_version": 1, "record_type": "enterprise_risk_analysis", "company": args.company,
                  "generated_at": iso_utc(), "evidence_count": len(evidence), "evidence": output_evidence,
                  "coverage": coverage,
                  "network_carriers": network_carriers, "analysis": analysis,
                  "analysis_mode": analysis.get("analysis_mode", "llm")}
        write_json(output, result)
        return 0
    except Exception as exc:
        print(f"enterprise risk analysis failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
