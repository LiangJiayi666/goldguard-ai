#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量生成企业截图证据 Word 稿（每企业一个 docx）。

三类截图分区：官网 / 社媒 / 风险证据。截图与 OCR 文字按「每个阶段取历史最新
一批真实产物」跨所有批次扫描文件系统（不依赖 exports 的 completed 引用，规避
空壳 exports 漏数据），LLM 结论（摘要/回购/关键词/账号复核/风险判定）取自该
企业历史最新 exports JSON。

用法（从项目根目录运行）：
    python .workbuddy/skills/pipeline-query/report/batch_evidence_docx.py
    python .workbuddy/skills/pipeline-query/report/batch_evidence_docx.py --companies 金雅福,简戒
    python .workbuddy/skills/pipeline-query/report/batch_evidence_docx.py --companies-file 名单.txt
    python .workbuddy/skills/pipeline-query/report/batch_evidence_docx.py --sections website,risk
    python .workbuddy/skills/pipeline-query/report/batch_evidence_docx.py --run <run_id> --out-dir <目录>

--companies / --companies-file 缺省 = 全部企业；企业名支持子串匹配。
--sections 缺省 website,social,risk 全选。

导出约定：默认输出到 outputs/<YYYYmmdd_HHMMSS>/evidence_docx/ 时间戳目录下，
避免覆盖旧文件；--out-dir 显式指定目录时跳过该约定。
"""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

import common
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor
from export_history_latest_excel import ROOT, RUNS_DIR, company_key, json_file

PLATFORM_NAMES = {"douyin": "抖音", "kuaishou": "快手", "xhs": "小红书", "weixin": "微信"}
ALL_SECTIONS = ("website", "social", "risk")
IMG_EXTS = (".jpg", ".jpeg", ".png", ".webp")


def safe_name(value: str, limit: int = 80) -> str:
    """与 src/pipeline_runtime.py:209 一致：companies/exports 目录名的统一生成器。"""
    cleaned = []
    for ch in str(value).strip():
        cleaned.append(ch if ch.isalnum() or ch in {"_", "-", "."} else "_")
    return ("".join(cleaned).strip("_.") or "item")[:limit]


# ---------------------------------------------------------------- 企业定位

def resolve_company_set():
    """{company_key: (data, run_id, stem)}，按企业取历史最新有 exports 的批次。"""
    latest: dict = {}
    for run_id in sorted((p.name for p in RUNS_DIR.iterdir() if p.is_dir()), reverse=True):
        export_dir = RUNS_DIR / run_id / "exports"
        if not export_dir.is_dir():
            continue
        for entry in export_dir.iterdir():
            if not entry.is_file() or entry.suffix != ".json":
                continue
            data = json_file(entry)
            if data:
                latest.setdefault(company_key(data, entry.stem), (data, run_id, entry.stem))
    return latest


def build_dir_index():
    """safe_name -> {run_id: companies_dir}，run 新→旧（目录名时间戳前缀排序）。"""
    index: dict[str, dict] = {}
    for run_id in sorted((p.name for p in RUNS_DIR.iterdir() if p.is_dir()), reverse=True):
        comps = RUNS_DIR / run_id / "companies"
        if not comps.is_dir():
            continue
        for d in comps.iterdir():
            if d.is_dir():
                index.setdefault(d.name, {})[run_id] = d
    return index


def match_companies(company_set: dict, queries: list[str]):
    if not queries:
        return sorted(company_set.values(), key=lambda t: t[2]), []
    hits, misses = [], list(queries)
    for val in company_set.values():
        name = val[2]
        for q in queries:
            if q and q in name:
                hits.append(val)
                misses = [m for m in misses if m != q]
                break
    hits.sort(key=lambda t: t[2])
    return hits, misses


# ---------------------------------------------------------------- 截图收集（跨批次扫文件系统）

def _latest_shots(dirs: dict, phase: str) -> list[dict]:
    """取该阶段「最新有截图的 run」的全部截图；返回 [{resolved, path, url, platform, run_id}]。"""
    for run_id, cd in dirs.items():
        phase_dir = cd / phase
        if not phase_dir.is_dir():
            continue
        got = []
        for task in sorted(phase_dir.iterdir()):
            if not task.is_dir():
                continue
            for att in sorted(task.iterdir()):
                if not att.is_dir():
                    continue
                shot_dir = att / "screenshots"
                if not shot_dir.is_dir():
                    continue
                url = platform = None
                rj = att / "result.json"
                if rj.is_file():
                    try:
                        r = json.loads(rj.read_text(encoding="utf-8"))
                    except Exception:
                        r = {}
                    url = r.get("url") or r.get("fetched_url") or r.get("final_url") \
                        or r.get("screenshot_url") or r.get("profile_url") or r.get("search_url")
                    platform = r.get("platform")
                for img in sorted(shot_dir.glob("*")):
                    if img.suffix.lower() not in IMG_EXTS:
                        continue
                    got.append({"resolved": str(img),
                                "path": str(img.relative_to(RUNS_DIR)).replace("\\", "/"),
                                "url": url, "platform": platform, "run_id": run_id})
        if got:
            return got
    return []


def collect_website_shots(dirs: dict) -> list[dict]:
    return [{"type": "website", "url": s["url"], "mode": None, "status": None, "reason": None,
             "path": s["path"], "resolved": s["resolved"], "missing_reason": None,
             "source_run_id": s["run_id"]} for s in _latest_shots(dirs, "WEB_CRAWL")]


def collect_search_shots(dirs: dict) -> list[dict]:
    return [{"type": "search", "platform": s["platform"], "url": s["url"], "status": None,
             "path": s["path"], "resolved": s["resolved"], "missing_reason": None,
             "source_run_id": s["run_id"]} for s in _latest_shots(dirs, "ACCOUNT_DISCOVERY")]


def collect_profile_shots(dirs: dict, verifications: list) -> list[dict]:
    def match_v(profile_url):
        if not profile_url:
            return None
        norm = profile_url.split("?", 1)[0]
        tail = norm.rsplit("/user/", 1)[-1] if "/user/" in norm else None
        for v in verifications:
            vp = (v.get("profile_url") or "").split("?", 1)[0]
            if vp == norm or (tail and vp.rsplit("/user/", 1)[-1] == tail):
                return v
        return None

    items = []
    for s in _latest_shots(dirs, "ACCOUNT_REVIEW"):
        v = match_v(s["url"])
        items.append({"type": "profile", "platform": s["platform"], "url": s["url"],
                      "status": None, "account_name": (v or {}).get("account_name"),
                      "official_status": (v or {}).get("official_status"),
                      "confidence": (v or {}).get("confidence"),
                      "reason": (v or {}).get("reason"),
                      "path": s["path"], "resolved": s["resolved"], "missing_reason": None,
                      "source_run_id": s["run_id"]})
    return items


def collect_ocr(dirs: dict) -> list[dict]:
    """取「最新有非空 OCR 文字的 run」的全部 OCR 结果（文字 + 对应原图）。"""
    for run_id, cd in dirs.items():
        ocr_dir = cd / "OCR"
        if not ocr_dir.is_dir():
            continue
        items = []
        for rj in sorted(ocr_dir.rglob("result.json")):
            try:
                d = json.loads(rj.read_text(encoding="utf-8"))
            except Exception:
                continue
            text = ""
            tf = d.get("text_file")
            if tf and os.path.isfile(tf):
                text = Path(tf).read_text(encoding="utf-8", errors="replace").strip()
            if not text:
                continue
            resolved = d.get("image") if d.get("image") and os.path.isfile(d["image"]) else None
            items.append({"type": "ocr", "image": d.get("image"), "text": text,
                          "resolved": resolved,
                          "missing_reason": None if resolved else "原图不存在",
                          "source_run_id": run_id,
                          "path": str(Path(tf).relative_to(RUNS_DIR)).replace("\\", "/") if tf and os.path.isfile(tf) else ""})
        if items:
            return items
    return []


def collect_risk_evidence(export: dict, dirs: dict) -> list[dict]:
    """优先 exports 的 risk_evidence_screenshots（带 evidence_ids→findings 解析），空则回退扫文件系统。"""
    items = export.get("risk_evidence_screenshots") or []
    if not items:
        items = (export.get("enterprise_assessment") or {}).get("evidence_screenshots") or []
    findings = (export.get("enterprise_assessment") or {}).get("findings") or []

    def related(eids):
        ids = set(eids or [])
        return [f for f in findings if f.get("evidence_id") in ids]

    if items:
        out = []
        for it in items:
            sp = it.get("screenshot_path")
            resolved = sp if sp and os.path.isfile(sp) else None
            if not resolved and sp and not os.path.isabs(sp):
                for cand in (RUNS_DIR / sp, ROOT / sp):
                    if cand.is_file():
                        resolved = str(cand)
                        break
            out.append({"type": "risk_evidence", "evidence_ids": it.get("evidence_ids") or [],
                        "url": it.get("url"), "platform": it.get("platform"),
                        "status": it.get("status"), "error": it.get("error"),
                        "path": sp, "resolved": resolved,
                        "missing_reason": None if resolved else "文件不存在",
                        "source_run_id": None,
                        "findings": [{"evidence_id": f.get("evidence_id"),
                                      "matched_keywords": f.get("matched_keywords") or [],
                                      "quoted_text": f.get("quoted_text"),
                                      "assessment": f.get("assessment")} for f in related(it.get("evidence_ids") or [])]})
        return out

    # 回退：扫文件系统，只有截图 + final_url，无 findings 解析
    return [{"type": "risk_evidence", "evidence_ids": [], "url": s["url"],
             "platform": s["platform"], "status": None, "error": None,
             "path": s["path"], "resolved": s["resolved"], "missing_reason": None,
             "source_run_id": s["run_id"], "findings": []}
            for s in _latest_shots(dirs, "RISK_EVIDENCE")]


def collect_all(export: dict, dirs: dict, sections: tuple) -> dict:
    a = export.get("enterprise_assessment") or {}
    out = {"meta": {
        "run_id": export.get("run_id"), "company": export.get("company"),
        "status": export.get("status"), "generated_at": export.get("generated_at"),
        "overall_risk_found": a.get("overall_risk_found"),
        "analysis_mode": a.get("analysis_mode"),
        "executive_summary": a.get("executive_summary"),
        "gold_buyback": a.get("gold_buyback"),
    }}
    verifications = export.get("social_account_verifications") or []
    if "website" in sections:
        out["website"] = collect_website_shots(dirs)
        out["ocr"] = collect_ocr(dirs)
    if "social" in sections:
        out["search"] = collect_search_shots(dirs)
        out["profile"] = collect_profile_shots(dirs, verifications)
    if "risk" in sections:
        out["risk_evidence"] = collect_risk_evidence(export, dirs)
    return out


# ---------------------------------------------------------------- docx 构建

def _set_font(run, name_zh: str, size: float, bold: bool = False, color=None):
    run.font.name = "Calibri"
    run.font.size = Pt(size)
    run.font.bold = bold
    if color:
        run.font.color.rgb = RGBColor(*color)
    rpr = run._element.get_or_add_rPr()
    rfonts = rpr.find(qn("w:rFonts"))
    if rfonts is None:
        rfonts = rpr.makeelement(qn("w:rFonts"), {})
        rpr.append(rfonts)
    rfonts.set(qn("w:eastAsia"), name_zh)


def _heading(doc, text: str):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(12)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    _set_font(p.add_run(text), "微软雅黑", 14, bold=True, color=(0x1F, 0x3B, 0x63))


def _para(doc, text: str, size: float = 10.5, bold: bool = False, indent: bool = False, color=None):
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(3)
    if indent:
        p.paragraph_format.left_indent = Inches(0.2)
    _set_font(p.add_run(text), "宋体", size, bold=bold, color=color)


def _caption(doc, text: str):
    p = doc.add_paragraph()
    p.paragraph_format.keep_with_next = True
    p.paragraph_format.space_before = Pt(8)
    p.paragraph_format.space_after = Pt(2)
    _set_font(p.add_run(text), "微软雅黑", 10, bold=True, color=(0x40, 0x40, 0x40))


def _note(doc, text: str, color=(0x80, 0x00, 0x00)):
    _para(doc, text, size=9.5, color=color)


def _kv_table(doc, rows: list[tuple[str, str]]):
    t = doc.add_table(rows=len(rows), cols=2)
    t.style = "Table Grid"
    t.autofit = False
    widths = (Inches(1.6), Inches(4.9))
    for i, (k, v) in enumerate(rows):
        for j, cell in enumerate(t.rows[i].cells):
            cell.width = widths[j]
            cell.paragraphs[0].text = ""
            _set_font(cell.paragraphs[0].add_run(k if j == 0 else (v or "")), "宋体", 10.5, bold=(j == 0))
            cell.paragraphs[0].paragraph_format.space_after = Pt(1)
    return t


def _fit_image_size(path: str):
    from PIL import Image
    with Image.open(path) as im:
        w, h = im.size
    scale = min(5.5 / w, 8.0 / h, 1.0)
    return Inches(w * scale), Inches(h * scale)


def _embed_image(doc, item: dict, label: str):
    if not item.get("resolved"):
        _note(doc, f"[{label}] 截图缺失：{item.get('missing_reason') or '未知原因'}")
        return False
    try:
        w, h = _fit_image_size(item["resolved"])
        doc.add_paragraph().add_run().add_picture(item["resolved"], width=w, height=h)
        return True
    except Exception as exc:  # noqa: BLE001
        _note(doc, f"[{label}] 截图无法嵌入：{exc}")
        return False


def _src_line(item: dict) -> str:
    parts = [item.get("path") or ""]
    if item.get("source_run_id"):
        parts.append(f"源批次 {item['source_run_id']}")
    return " | ".join(p for p in parts if p)


def _embed_block(doc, items: list[dict], section_title: str, empty_text: str,
                 caption_fn, extra_fn=None):
    _heading(doc, section_title)
    if not items:
        _para(doc, empty_text)
        return 0
    embedded = 0
    for i, it in enumerate(items, 1):
        _caption(doc, caption_fn(it, i, len(items)))
        if _embed_image(doc, it, f"{section_title} {i}"):
            embedded += 1
        if extra_fn:
            extra_fn(doc, it)
        _note(doc, f"来源：{_src_line(it)}", color=(0x40, 0x40, 0x40))
    return embedded


def build_docx(data: dict, out_path: Path, sections: tuple) -> dict:
    doc = Document()
    for sec in doc.sections:
        sec.left_margin = sec.right_margin = Inches(0.9)
        sec.top_margin = sec.bottom_margin = Inches(0.9)

    meta = data["meta"]
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    _set_font(p.add_run("企业截图证据册"), "微软雅黑", 20, bold=True, color=(0x1F, 0x3B, 0x63))
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_after = Pt(10)
    _set_font(p.add_run(meta["company"] or ""), "微软雅黑", 14, bold=True)

    risk_text = "命中风险" if meta.get("overall_risk_found") else "未命中风险"
    if meta.get("analysis_mode") == "conservative_local_fallback":
        risk_text += "（LLM 服务不可用，保守本地判定）"
    _kv_table(doc, [("批次", meta.get("run_id")), ("企业状态", meta.get("status")),
                    ("风险结论", risk_text), ("生成时间", meta.get("generated_at"))])

    counts: dict[str, int] = {}

    _heading(doc, "一、排查结论摘要")
    if meta.get("executive_summary"):
        _para(doc, str(meta["executive_summary"]))
    gb = meta.get("gold_buyback")
    if isinstance(gb, dict) and gb.get("reason"):
        _para(doc, f"黄金回购判定：{gb.get('present')}｜{gb['reason']}")

    if "website" in sections:
        counts["website"] = _embed_block(
            doc, data["website"], "二、官网截图", "无官网截图记录。",
            lambda it, i, n: f"图 {i}/{n}  官网页面 | {it.get('url') or '（无 URL）'}")

        ocr = data.get("ocr") or []
        _heading(doc, "三、官网图片文字识别（OCR）")
        if not ocr:
            _para(doc, "本批次无 OCR 文字记录。")
        else:
            for i, it in enumerate(ocr, 1):
                _caption(doc, f"OCR {i}/{len(ocr)}  图片文字识别结果")
                if _embed_image(doc, it, f"OCR {i}"):
                    counts["ocr"] = counts.get("ocr", 0) + 1
                _para(doc, it["text"], size=10, indent=True)
                _note(doc, f"来源：{_src_line(it)}", color=(0x40, 0x40, 0x40))

    if "social" in sections:
        counts["search"] = _embed_block(
            doc, data["search"], "四、社媒账号搜索页截图", "无账号搜索页截图记录。",
            lambda it, i, n: f"图 {i}/{n}  搜索页 | {PLATFORM_NAMES.get(it.get('platform'), it.get('platform') or '未知平台')} | {it.get('url') or ''}")
        counts["profile"] = _embed_block(
            doc, data["profile"], "五、账号主页截图", "无账号主页截图记录。",
            lambda it, i, n: f"图 {i}/{n}  账号主页 | {PLATFORM_NAMES.get(it.get('platform'), it.get('platform') or '未知平台')} | {it.get('account_name') or '（账号名未知）'}{' | 认证 ' + it['official_status'] if it.get('official_status') else ''}",
            lambda doc, it: (_para(doc, f"复核理由：{it['reason']}", size=10, indent=True) if it.get("reason") else None,
                             _para(doc, f"主页地址：{it['url']}", size=10, indent=True) if it.get("url") else None))

    if "risk" in sections:
        def risk_extra(doc, it):
            if it.get("url"):
                _para(doc, f"页面地址：{it['url']}", size=10, indent=True)
            for f in it.get("findings") or []:
                kw = "、".join(f.get("matched_keywords") or [])
                if kw:
                    _para(doc, f"命中关键词：{kw}", size=10, indent=True)
                if f.get("quoted_text"):
                    _para(doc, f"页面原文：{f['quoted_text']}", size=10, indent=True)
                if f.get("assessment"):
                    _para(doc, f"判定：{f['assessment']}", size=10, indent=True)
            if it.get("error"):
                _note(doc, f"采集错误：{it['error']}")
        counts["risk_evidence"] = _embed_block(
            doc, data["risk_evidence"], "六、风险现象证据页截图",
            "无风险证据截图记录（未命中风险或未采集到证据页）。",
            lambda it, i, n: f"图 {i}/{n}  风险证据页 | {PLATFORM_NAMES.get(it.get('platform'), it.get('platform') or '')} | 证据ID {', '.join(it.get('evidence_ids') or [])} | {it.get('status')}",
            risk_extra)

    doc.save(str(out_path))
    return counts


# ---------------------------------------------------------------- 主流程

def safe_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name).strip() or "企业"


def parse_sections(raw: str) -> tuple:
    chosen = [s.strip().lower() for s in raw.replace("，", ",").split(",") if s.strip()]
    bad = [s for s in chosen if s not in ALL_SECTIONS]
    if bad:
        raise SystemExit(f"错误：未知分区 {bad}，可选 {list(ALL_SECTIONS)}")
    return tuple(s for s in ALL_SECTIONS if s in chosen)


def parse_queries(args) -> list[str]:
    queries = [q.strip() for q in args.companies.replace("，", ",").split(",") if q.strip()]
    if args.companies_file:
        p = Path(args.companies_file)
        if not p.is_file():
            raise SystemExit(f"错误：企业名单文件不存在 {p}")
        queries += [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()
                    if ln.strip() and not ln.startswith("#")]
    return queries


def main() -> int:
    common.setup_utf8()
    ap = argparse.ArgumentParser(description="批量生成企业截图证据 Word 稿")
    ap.add_argument("--companies", default="", help="企业名，逗号分隔（支持子串匹配）")
    ap.add_argument("--companies-file", default="", help="企业名单文件，每行一个")
    ap.add_argument("--sections", default=",".join(ALL_SECTIONS), help="website,social,risk 可组合")
    ap.add_argument("--run", default="", help="指定批次 run_id（缺省=按企业历史最新批次）")
    ap.add_argument("--out-dir", default="", help="输出目录（缺省=outputs/<时间戳>/evidence_docx/）")
    args = ap.parse_args()

    sections = parse_sections(args.sections)
    queries = parse_queries(args)
    dir_index = build_dir_index()
    company_set = resolve_company_set()
    if not company_set:
        raise SystemExit("错误：未找到任何已导出企业")

    if args.run:
        company_set = {k: v for k, v in company_set.items() if v[1] == args.run}
        if not company_set:
            raise SystemExit(f"错误：批次 {args.run} 无已导出企业")

    targets, misses = match_companies(company_set, queries)
    if misses:
        print(f"提示：未匹配到企业：{', '.join(misses)}", file=__import__("sys").stderr)

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = ROOT / "outputs" / stamp / "evidence_docx"
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for data, run_id, stem in targets:
        dirs = dir_index.get(stem, {})
        payload = collect_all(data, dirs, sections)
        docx_path = out_dir / f"{safe_filename(data.get('company') or stem)}.docx"
        counts = build_docx(payload, docx_path, sections)
        results.append({"company": data.get("company") or stem, "run_id": run_id,
                        "docx": str(docx_path), "counts": counts})

    common.emit({"companies": len(results), "output_dir": str(out_dir),
                 "sections": list(sections), "results": results})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
