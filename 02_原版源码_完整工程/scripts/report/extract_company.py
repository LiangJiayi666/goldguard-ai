# -*- coding: utf-8 -*-
"""原子脚本 03：单企业报告素材（从 exports JSON 提取全部要点）。

用法:
  python .workbuddy/skills/pipeline-query/report/extract_company.py --run latest --company 金雅福
  python .workbuddy/skills/pipeline-query/report/extract_company.py --run <run_id> --company <名称> --section risk

--section 可选: all|icp|website|social|activity|risk|evidence|defects|completed|raw
"""
from __future__ import annotations

import argparse

import os

import common


SECTIONS = ("all", "icp", "website", "social", "activity", "risk", "evidence", "defects", "completed", "raw")


def summarize_social(export: dict) -> dict:
    verifications = export.get("social_account_verifications") or []
    return {
        "count": len(verifications),
        "accounts": [
            {
                "platform": v.get("platform"),
                "name": v.get("account_name") or v.get("username"),
                "role": v.get("account_role"),
                "official_status": v.get("official_status"),
                "confidence": v.get("confidence"),
                "decision": v.get("decision"),
                "reason": v.get("reason"),
                "works": v.get("works_count"),
                "fans": v.get("fans"),
            }
            for v in verifications
        ],
    }


def summarize_activity(export: dict) -> dict:
    return {
        "social_accounts": export.get("social_account_activity") or [],
        "domains": export.get("domain_activity") or [],
    }


def summarize_risk(export: dict) -> dict:
    a = export.get("enterprise_assessment") or {}
    return {
        "analysis_mode": a.get("analysis_mode"),
        "analysis_warning": a.get("analysis_warning"),
        "overall_risk_found": a.get("overall_risk_found"),
        "executive_summary": a.get("executive_summary"),
        "gold_business_involvement": a.get("gold_business_involvement"),
        "business_model": a.get("business_model"),
        "findings": a.get("findings") or [],
        "gold_buyback": a.get("gold_buyback"),
        "risk_keyword_findings": a.get("risk_keyword_findings") or [],
        "risk_scenarios": a.get("risk_scenarios") or [],
        "network_carrier_assessment": a.get("network_carrier_assessment"),
    }


def _icp_from_phases(run_dir: str, name: str) -> dict:
    """企业尚未导出时，从 WEB_ICP 阶段 final.json 汇总备案载体数据。"""
    import glob

    company_dir = os.path.join(run_dir, "companies", common.safe_name(name))
    phase_dir = os.path.join(company_dir, "WEB_ICP")
    carriers: dict[str, list[dict]] = {"web": [], "mapp": [], "app": [], "kapp": []}
    if not os.path.isdir(phase_dir):
        return carriers
    seen: set[tuple[str, str]] = set()
    for final_path in sorted(glob.glob(os.path.join(phase_dir, "task_*", "attempt_*", "final.json"))):
        try:
            data = common.load_json(final_path)
        except Exception:
            continue
        summary = data.get("summary") or {}
        carrier = str(summary.get("carrier") or summary.get("carrier_type") or "web")
        for record in summary.get("records") or []:
            key = (
                carrier,
                str(record.get("serviceLicence") or record.get("mainLicence") or record.get("serviceName") or record.get("domain") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            label = record.get("serviceName") or record.get("appName") or record.get("domain") or "未知"
            carriers.setdefault(carrier, []).append({
                "carrier_type": carrier,
                "name": label,
                "service_name": record.get("serviceName") or record.get("appName") or record.get("domain"),
                "licence": record.get("serviceLicence") or record.get("mainLicence"),
                "unit_name": record.get("unitName"),
                "update_time": record.get("updateRecordTime"),
            })
    return carriers


def main() -> int:
    common.setup_utf8()
    parser = argparse.ArgumentParser(description="单企业报告素材")
    parser.add_argument("--run", default="latest")
    parser.add_argument("--company", required=True, help="企业名称（支持子串匹配）")
    parser.add_argument("--section", choices=SECTIONS, default="all")
    args = parser.parse_args()

    run_dir, run_id = common.resolve_run(args.run)
    name, export_path = common.find_company(run_dir, args.company)
    section = args.section
    if not os.path.exists(export_path):
        # 企业尚未 CLOSED 导出时，部分标签可从阶段产物回退展示
        if section == "icp":
            carriers = _icp_from_phases(run_dir, name)
            common.emit({
                "company": name,
                "company_id": "",
                "status": "OPEN",
                "run_id": run_id,
                "generated_at": None,
                "export_file": None,
                "icp_carriers": carriers,
                "icp_counts": {k: len(v) for k, v in carriers.items()},
                "note": "企业尚未完成，数据来自 WEB_ICP 阶段产物",
            })
            return 0
        section_phase = {
            "icp": "WEB_ICP",
            "website": "WEB_CRAWL",
            "social": "ACCOUNT_VERIFY",
            "risk": "RISK_ANALYSIS",
            "activity": "POST_CRAWL",
            "evidence": "RISK_EVIDENCE",
            "defects": "RISK_ANALYSIS",
            "completed": "RISK_ANALYSIS",
        }.get(section, "RISK_ANALYSIS")
        common.emit({
            "error": f"企业「{name}」在本批次还没有导出文件（未完成/未 CLOSED）",
            "hint": f"可用 extract_phase.py 按阶段提取任务产物，如: extract_phase.py --company \"{name}\" --phase {section_phase}",
        })
        return 1
    export = common.load_json(export_path)

    icp = export.get("icp_carriers") or {}
    carriers = {
        kind: (items or [])
        for kind, items in icp.items()
    }
    website = export.get("website_crawl") or {}

    payload = {
        "company": export.get("company"),
        "company_id": export.get("company_id"),
        "status": export.get("status"),
        "run_id": export.get("run_id"),
        "generated_at": export.get("generated_at"),
        "export_file": export_path,
    }
    if section == "raw":
        common.emit(export)
        return 0
    if section in ("all", "icp"):
        payload["icp_carriers"] = carriers
        payload["icp_counts"] = {k: len(v) for k, v in carriers.items()}
    if section in ("all", "website"):
        payload["website_search"] = export.get("website_search") or {
            "provider": None,
            "official_status": "unverified_candidates",
            "candidates": [],
            "carrier_candidates": [],
        }
        payload["website_crawl"] = {
            "max_depth": website.get("max_depth"),
            "max_pages": website.get("max_pages"),
            "pages_scheduled": website.get("pages_scheduled"),
            "seen_urls": website.get("seen_urls") or [],
            "stats": website.get("stats") or {},
        }
    if section in ("all", "social"):
        payload["social_account_verifications"] = summarize_social(export)
    if section in ("all", "activity"):
        payload["activity"] = summarize_activity(export)
    if section in ("all", "risk"):
        payload["risk"] = summarize_risk(export)
    if section in ("all", "evidence"):
        payload["risk_evidence_screenshots"] = export.get("risk_evidence_screenshots") or []
        payload["screenshots"] = export.get("screenshots") or []
    if section in ("all", "defects"):
        payload["defects"] = export.get("defects") or []
    if section in ("all", "completed"):
        completed = export.get("completed") or []
        payload["completed"] = {
            "count": len(completed),
            "phases": sorted({c.get("phase") for c in completed}),
            "items": [
                {
                    "phase": c.get("phase"),
                    "platform": c.get("platform"),
                    "summary": c.get("summary"),
                    "diagnosis_status": (c.get("diagnosis") or {}).get("status"),
                }
                for c in completed
            ],
        }
    common.emit(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
