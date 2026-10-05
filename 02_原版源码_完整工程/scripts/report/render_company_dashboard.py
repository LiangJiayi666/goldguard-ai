#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成企业排查看板 HTML（自包含、可离线双击打开）。

口径与 export_history_latest_excel.py 完全一致：按企业取「历史最新一次有 exports
的批次」。看板在其上增加：
  - 是否最新批次（该企业历史最新 run 是否等于全局最新 run）
  - 进度：当前阶段 + 阶段内终态任务/总任务
  - 每阶段耗时（该阶段 attempt 最早开始 → 最晚结束）+ 复用任务数
  - 详情页纵向罗列 Excel 全部字段 + 各阶段中间文件链接

数据只读：DB（runs/companies/tasks/task_attempts）+ exports JSON + 过程文件。

用法（从项目根目录运行）：
    python .workbuddy/skills/pipeline-query/report/render_company_dashboard.py [--out <路径.html>]
"""
import argparse
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import common
from export_history_latest_excel import (
    build_row, columns, load_run_times, company_key, json_file,
)

ROOT = Path(common.ROOT)
RUNS_DIR = ROOT / "pipeline_output" / "runs"
DB_PATH = ROOT / "pipeline_output" / "data" / "main_agent.db"

PHASES = [
    "WEB_ICP", "WEB_SEARCH", "WEB_CRAWL", "IMAGE_DOWNLOAD", "OCR", "KEYWORD",
    "ACCOUNT_DISCOVERY", "ACCOUNT_RANK", "ACCOUNT_REVIEW", "ACCOUNT_VERIFY",
    "POST_CRAWL", "RISK_ANALYSIS", "RISK_EVIDENCE",
]
PHASE_LABELS = {
    "WEB_ICP": "ICP备案查询", "WEB_SEARCH": "互联网载体搜索", "WEB_CRAWL": "官网文字抓取",
    "IMAGE_DOWNLOAD": "网页图片下载", "OCR": "图片文字识别", "KEYWORD": "LLM关键词精选",
    "ACCOUNT_DISCOVERY": "社媒账号查询", "ACCOUNT_RANK": "账号候选初筛",
    "ACCOUNT_REVIEW": "账号样本复核", "ACCOUNT_VERIFY": "账号归属定级",
    "POST_CRAWL": "账号作品抓取", "RISK_ANALYSIS": "企业风险判定", "RISK_EVIDENCE": "风险证据截图",
    "EXPORT": "结果导出",
}

# 详情字段分组：columns() 的第 1 项是企业名，随后 11 组（3 载体 + 3 平台 + 5 判定）。
DETAIL_BLOCKS = [
    ("官网载体", 5), ("APP载体", 5), ("小程序载体", 5),
    ("小红书账号", 5), ("抖音账号", 5), ("快手账号", 5),
    ("业务判定", 4), ("黄金业务", 4), ("风险词", 4), ("风险现象", 4),
]
CARRIER_SUMMARY_KEY = 1  # columns 里「官网list」的下标


def fmt_dur(sec):
    if sec is None or sec < 0:
        return ""
    sec = int(sec)
    if sec < 60:
        return f"{sec}秒"
    if sec < 3600:
        return f"{sec // 60}分{sec % 60}秒"
    return f"{sec // 3600}时{(sec % 3600) // 60}分"


def parse_ts(s):
    try:
        return datetime.fromisoformat(str(s))
    except Exception:
        return None


def load_db():
    """一次性读全量进度数据，避免逐企业查询。"""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        global_latest = con.execute(
            "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        companies = {f"{r['run_id']}|{r['company_id']}": r
                     for r in con.execute(
                         "SELECT run_id, company_id, company_name, status, phase FROM companies")}
        tasks = {}
        for r in con.execute(
                "SELECT run_id, company_id, phase, COUNT(*) n, "
                "SUM(CASE WHEN status='TERMINAL' THEN 1 ELSE 0 END) done "
                "FROM tasks GROUP BY run_id, company_id, phase"):
            tasks.setdefault(f"{r['run_id']}|{r['company_id']}", {})[r["phase"]] = (r["done"], r["n"])
        timings = {}
        for r in con.execute(
                "SELECT t.run_id, t.company_id, t.phase, "
                "MIN(ta.started_at) first_start, MAX(ta.finished_at) last_finish, "
                "SUM(CASE WHEN ta.execution_mode='HISTORY_REUSE' THEN 1 ELSE 0 END) reused "
                "FROM task_attempts ta JOIN tasks t ON t.task_id=ta.task_id "
                "JOIN runs r ON r.run_id=t.run_id "
                "WHERE ta.started_at >= r.started_at "
                "GROUP BY t.run_id, t.company_id, t.phase"):
            first = parse_ts(r["first_start"])
            last = parse_ts(r["last_finish"])
            dur = (last - first).total_seconds() if first and last else None
            timings.setdefault(f"{r['run_id']}|{r['company_id']}", {})[r["phase"]] = {
                "duration": dur, "reused": r["reused"] or 0,
            }
        return (global_latest["run_id"] if global_latest else None,
                companies, tasks, timings)
    finally:
        con.close()


def index_files(company_dir: Path, output_dir: Path):
    """索引企业过程文件；uri 是相对 HTML 输出目录的路径，避免 file:// 体积爆炸。"""
    out = []
    for phase_dir in sorted(company_dir.iterdir()):
        if not phase_dir.is_dir():
            continue
        phase = phase_dir.name
        label = PHASE_LABELS.get(phase, phase)
        files = [p for p in phase_dir.rglob("*") if p.is_file()]
        def rank(p):
            if p.name in ("final.json", "result.json"):
                return 0
            if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp"):
                return 1
            return 2
        files.sort(key=rank)
        for p in files:
            if len(out) >= 45:
                break
            kind = ("关键产物" if p.name in ("final.json", "result.json")
                    else "截图" if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")
                    else "过程文件")
            try:
                rel = Path(os.path.relpath(p, output_dir)).as_posix()
            except Exception:
                rel = str(p.relative_to(ROOT)).replace("\\", "/")
            out.append({"phase": phase, "label": label, "kind": kind,
                        "path": str(p.relative_to(ROOT)).replace("\\", "/"),
                        "uri": rel})
    return out

def load_companies(run_times, companies_db, tasks_db, timings_db, global_latest, html_dir: Path):
    latest = {}
    for run_id in sorted((p.name for p in RUNS_DIR.iterdir() if p.is_dir()), reverse=True):
        export_dir = RUNS_DIR / run_id / "exports"
        if not export_dir.is_dir():
            continue
        for entry in export_dir.iterdir():
            if not entry.is_file() or entry.suffix != ".json":
                continue
            data = json_file(entry)
            if not data:
                continue
            latest.setdefault(company_key(data, entry.stem),
                              (data, run_id, entry.stem))

    rows = []
    cols = columns()
    for data, run_id, name in latest.values():
        run_dir = RUNS_DIR / run_id
        company_id = str(data.get("company_id") or "")
        key = f"{run_id}|{company_id}"
        comp = companies_db.get(key)
        phase = (comp["phase"] if comp else "EXPORT") or "EXPORT"
        status = comp["status"] if comp else "OPEN"
        done_total = tasks_db.get(key, {}).get(phase, (None, None))
        tim = timings_db.get(key, {}).get(phase, {})
        row = build_row(data, run_dir, run_id, name, run_times)
        kv = dict(zip(cols, row))

        timings = []
        for ph in PHASES:
            t = timings_db.get(key, {}).get(ph, {})
            dn, tn = tasks_db.get(key, {}).get(ph, (None, None))
            timings.append({
                "phase": ph, "label": PHASE_LABELS[ph],
                "duration": fmt_dur(t.get("duration")),
                "reused": t.get("reused", 0),
                "done": dn, "total": tn,
            })

        files = index_files(run_dir / "companies" / name, html_dir)

        # 主表摘要：官网、社媒、风险三列
        social = [kv.get(f"是否有{p}账号") == "是" for p in ("xhs", "dy", "ks")]
        risk = kv.get("是否命中风险词") == "是" or kv.get("是否命中风险现象") == "是"

        rows.append({
            "name": kv.get("企业名") or name,
            "run_id": run_id,
            "run_time": run_times.get(run_id) or run_id[:15],
            "is_latest": run_id == global_latest,
            "status": status,
            "phase": phase,
            "phase_label": PHASE_LABELS.get(phase, phase),
            "phase_done": done_total[0],
            "phase_total": done_total[1],
            "phase_duration": fmt_dur(tim.get("duration")),
            "summary": {
                "官网": kv.get("官网list") or "",
                "社媒": sum(social),
                "风险": "有风险" if risk else "无风险",
            },
            "detail": detail_fields(kv, cols),
            "timings": timings,
            "files": files,
        })
    rows.sort(key=lambda r: r["name"])
    return rows


def detail_fields(kv, cols):
    """将 47 列按块分组，供详情页纵向罗列。"""
    blocks = []
    idx = 1  # 跳过第 0 列企业名
    for label, width in DETAIL_BLOCKS:
        fields = [[cols[idx + i], kv.get(cols[idx + i], "")] for i in range(width)]
        blocks.append({"label": label, "fields": fields})
        idx += width
    return blocks


# --------------------------------------------------------------------------- HTML
PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>企业排查看板</title>
<style>
:root{--ink:#15201d;--muted:#69756f;--line:#dbe2dc;--bg:#f4f7f4;--paper:#fff;--green:#147a5a;--green-soft:#e5f4ed;--amber:#b96d00;--amber-soft:#fff3d9;--red:#b42318;--red-soft:#fcedea;--blue:#2668bd;--blue-soft:#eaf1ff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.55 "Microsoft YaHei UI","Microsoft YaHei",system-ui,sans-serif}
.shell{max-width:1440px;margin:auto;padding:26px clamp(14px,3vw,44px) 48px}
.masthead{display:flex;justify-content:space-between;align-items:flex-end;gap:20px;padding-bottom:20px;border-bottom:1px solid var(--line)}
h1{font-size:26px;margin:0;letter-spacing:-.03em}
.sub{color:var(--muted);margin:4px 0 0;font-size:13px}
.fresh{color:var(--muted);font-size:12px;text-align:right}
.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0;border:1px solid var(--line);background:var(--paper);margin:20px 0;box-shadow:0 8px 22px rgba(21,32,29,.05)}
.metric{padding:14px 16px;border-right:1px solid var(--line)}
.metric:last-child{border:0}
.metric .l{color:var(--muted);font-size:12px}
.metric .v{font-size:26px;font-weight:750;margin-top:2px}
.metric .h{font-size:12px;color:var(--muted)}
.toolbar{display:flex;gap:10px;align-items:center;padding:14px 0;flex-wrap:wrap}
.toolbar input,.toolbar select{font:inherit;border:1px solid #cbd5ce;background:#fff;padding:7px 10px}
.toolbar input{flex:1 1 220px;outline-color:var(--green)}
.count{margin-left:auto;color:var(--muted);font-size:12px}
section{background:var(--paper);border:1px solid var(--line);box-shadow:0 8px 22px rgba(21,32,29,.05)}
table{width:100%;border-collapse:collapse;min-width:920px}
th{background:#f3f7f4;color:#4d5a53;font-size:12px;font-weight:650;text-align:left;padding:10px 12px;white-space:nowrap;position:sticky;top:0}
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:var(--green)}
th.sortable::after{content:'';margin-left:4px;color:var(--muted);font-size:10px}
th.sortable[data-dir="asc"]::after{content:'▲';color:var(--green)}
th.sortable[data-dir="desc"]::after{content:'▼';color:var(--green)}
td{border-top:1px solid #edf1ee;padding:10px 12px;vertical-align:middle}
tr.row{cursor:pointer}tr.row:hover{background:#f5faf7}
.name{font-weight:650;max-width:240px}
.tag{display:inline-flex;align-items:center;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:650;white-space:nowrap}
.latest{background:var(--green-soft);color:var(--green)}
.history{background:#eef1ee;color:#5a6660}
.risk{background:var(--red-soft);color:var(--red)}
.norisk{background:var(--green-soft);color:var(--green)}
.status-closed{background:var(--green-soft);color:var(--green)}
.status-open{background:var(--amber-soft);color:var(--amber)}
.bar{height:7px;overflow:hidden;background:#edf1ee;border-radius:4px}
.bar>i{display:block;height:100%;background:var(--green);transition:width .4s}
.pct{font-variant-numeric:tabular-nums;color:var(--muted);font-size:12px}
.pagination{display:flex;align-items:center;justify-content:flex-end;gap:8px;padding:12px 14px;border-top:1px solid var(--line)}
.pagination button{border:1px solid #cbd5ce;background:#fff;padding:5px 10px;cursor:pointer}
.pagination button:disabled{opacity:.4;cursor:default}
dialog{width:min(940px,calc(100vw - 28px));max-height:min(820px,calc(100vh - 28px));border:0;padding:0;box-shadow:0 24px 80px rgba(0,0,0,.25)}
dialog::backdrop{background:rgba(19,30,26,.45)}
.dhead{position:sticky;top:0;background:var(--paper);z-index:2;padding:16px 20px;display:flex;align-items:flex-start;justify-content:space-between;gap:14px;border-bottom:1px solid var(--line)}
.dhead h2{margin:0;font-size:19px;padding-right:16px}
.dhead .meta{color:var(--muted);font-size:12px;margin-top:3px}
.close{background:none;border:0;font-size:24px;line-height:1;cursor:pointer;color:var(--muted)}
.dbody{padding:16px 20px 26px}
h3{font-size:13px;margin:20px 0 8px;color:#4d5a53}
.timing{display:grid;grid-template-columns:130px 1fr 88px 60px;gap:10px;align-items:center;padding:6px 0;border-bottom:1px solid #f0f3f0;font-size:13px}
.timing .pl{font:12px ui-monospace,Consolas,monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.timing .pt{font-variant-numeric:tabular-nums;color:var(--muted)}
.reuse{color:var(--amber);font-size:12px}
.kv{border:1px solid var(--line);margin-bottom:18px}
.kv-head{background:#f3f7f4;padding:8px 12px;font-weight:650;font-size:13px}
.kv-row{display:grid;grid-template-columns:190px 1fr;border-top:1px solid #edf1ee}
.kv-row:first-of-type{border-top:0}
.kv-row .k{padding:8px 12px;color:var(--muted);font-size:13px;border-right:1px solid #edf1ee}
.kv-row .vv{padding:8px 12px;white-space:pre-wrap;overflow-wrap:anywhere}
.files{border:1px solid var(--line)}
.files .f{display:flex;gap:12px;align-items:flex-start;padding:8px 12px;border-top:1px solid #f0f3f0;font-size:13px}
.files .f:first-child{border-top:0}
.files .k{flex:0 0 82px;color:var(--muted);padding-top:2px}
.files a{color:var(--blue);text-decoration:none;overflow-wrap:anywhere}
.files a:hover{text-decoration:underline}
.files .g{background:#f8faf8;font-weight:650;padding:6px 12px;font-size:12px;color:#4d5a53}
.thumb{max-width:180px;max-height:130px;border:1px solid var(--line);border-radius:6px;display:block;cursor:zoom-in}
.thumb-grid{display:flex;flex-wrap:wrap;gap:8px;align-items:flex-start}
.vv a{color:var(--blue);text-decoration:none;overflow-wrap:anywhere}
.vv a:hover{text-decoration:underline}
.empty{color:var(--muted);padding:20px;text-align:center}
@media (max-width:760px){.metrics{grid-template-columns:repeat(2,1fr)}.metric:nth-child(2n){border-right:0}.metric:nth-child(n+3){border-top:1px solid var(--line)}.timing{grid-template-columns:100px 1fr 70px}.timing .pt{display:none}}
</style>
</head>
<body>
<main class="shell">
  <header class="masthead">
    <div><h1>企业排查看板</h1><p class="sub">黄金企业网络载体风险排查 · 按企业取历史最新批次</p></div>
    <div class="fresh" id="fresh"></div>
  </header>
  <section class="metrics" id="metrics"></section>
  <section>
    <div class="toolbar">
      <input id="q" placeholder="搜索企业名 / 官网 / 风险词…" autocomplete="off">
      <select id="batch-filter"><option value="">全部批次</option></select>
      <select id="page-size"><option value="20">每页20</option><option value="50" selected>每页50</option><option value="100">每页100</option></select>
      <span class="count" id="count"></span>
    </div>
    <div style="overflow:auto">
      <table><thead><tr><th class="sortable" data-key="name">企业</th><th class="sortable" data-key="run">批次</th><th class="sortable" data-key="status">状态</th><th class="sortable" data-key="phase">当前阶段</th><th class="sortable" data-key="progress">进度</th><th class="sortable" data-key="website">官网</th><th class="sortable" data-key="social">社媒</th><th class="sortable" data-key="risk">风险</th></tr></thead>
      <tbody id="tbody"></tbody></table>
    </div>
    <div class="pagination"><button id="prev">上一页</button><span id="plabel" style="color:var(--muted);font-size:12px"></span><button id="next">下一页</button></div>
  </section>
</main>
<dialog id="d"><div class="dhead"><div><h2 id="dname"></h2><div class="meta" id="dmeta"></div></div><button class="close" id="dclose">×</button></div><div class="dbody" id="dbody"></div></dialog>
<script id="data" type="application/json">__DATA__</script>
<script>
(() => {
  const data = JSON.parse(document.getElementById('data').textContent);
  const rows = data.companies || [];
  const $ = id => document.getElementById(String(id).replace(/^#/, ''));
  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const tag = (t, k) => `<span class="tag ${k}">${esc(t)}</span>`;
  const n = v => Number(v ?? 0);
  const st = { page: 1, size: 50, sortKey: 'name', sortDir: 1 };
  const linkify = s => String(s ?? '').split(/(https?:\/\/[^\s<>"']+)/g).map(part =>
    /^https?:\/\//.test(part)
      ? `<a href="${part}" target="_blank" rel="noopener">${part}</a>`
      : esc(part)).join('');
  const isImg = p => /\.(png|jpe?g|webp)$/i.test(p);

  $('#fresh').textContent = `数据生成于 ${(data.generated_at||'').replace('T',' ').slice(0,19)}`;
  const latestN = rows.filter(r=>r.is_latest).length;
  $('#metrics').innerHTML = [
    ['企业总数', rows.length, `${latestN} 家最新 · ${rows.length-latestN} 家历史`],
    ['最新批次', data.latest_run, data.latest_run_time || ''],
    ['已完成', rows.filter(r=>r.status==='CLOSED').length, '状态 CLOSED'],
    ['含风险', rows.filter(r=>r.summary && r.summary['风险']==='有风险').length, '风险词或风险现象命中'],
  ].map(m=>`<div class="metric"><div class="l">${m[0]}</div><div class="v">${m[1]}</div><div class="h">${m[2]}</div></div>`).join('');

  const bf = $('#batch-filter');
  [...new Set(rows.map(r=>r.run_id))].sort().forEach(id=>{
    const r=rows.find(x=>x.run_id===id);
    bf.insertAdjacentHTML('beforeend',`<option value="${esc(id)}">${esc(id)}${r&&r.is_latest?'（最新）':''}</option>`);
  });

  const filtered = () => {
    const q=$('#q').value.trim().toLowerCase(), b=bf.value;
    return rows.filter(r=>{
      const hay=[r.name, r.summary?.['官网']||'', r.run_id].join(' ').toLowerCase();
      if(q && !hay.includes(q)) return false;
      if(b && r.run_id!==b) return false;
      return true;
    });
  };

  const phasePct = r => (r.phase_total ? Math.round(n(r.phase_done)/n(r.phase_total)*100) : (r.status==='CLOSED'?100:0));

  const sortVal = (r, k) => {
    switch(k){
      case 'name': return r.name || '';
      case 'run': return r.is_latest ? 0 : 1;
      case 'status': return r.status || '';
      case 'phase': return r.phase_label || '';
      case 'progress': return phasePct(r);
      case 'website': return r.summary?.['官网'] || '';
      case 'social': return n(r.summary?.['社媒']);
      case 'risk': return r.summary?.['风险']==='有风险' ? 1 : 0;
    }
  };
  const sorted = () => {
    const arr = filtered().slice(), k = st.sortKey, dir = st.sortDir;
    arr.sort((a,b)=>{
      const va = sortVal(a,k), vb = sortVal(b,k);
      if (typeof va === 'number' && typeof vb === 'number') return (va-vb)*dir;
      return String(va).localeCompare(String(vb), 'zh')*dir;
    });
    return arr;
  };

  document.querySelectorAll('th.sortable').forEach(th=>{
    th.addEventListener('click', ()=>{
      const k = th.dataset.key;
      if (st.sortKey === k) st.sortDir *= -1;
      else { st.sortKey = k; st.sortDir = 1; }
      st.page = 1;
      document.querySelectorAll('th.sortable').forEach(x=>x.removeAttribute('data-dir'));
      th.setAttribute('data-dir', st.sortDir>0?'asc':'desc');
      render();
    });
  });

  function render(){
    const all=sorted(), pages=Math.max(1,Math.ceil(all.length/st.size));
    st.page=Math.min(st.page,pages);
    const page=all.slice((st.page-1)*st.size, st.page*st.size);
    $('#count').textContent=`${all.length} / ${rows.length} 家企业`;
    $('#tbody').innerHTML = page.map((r,i)=>{
      const pct=phasePct(r);
      const risk=r.summary&&r.summary['风险']==='有风险'?tag('有风险','risk'):tag('无风险','norisk');
      const dur=r.phase_duration?` · ${esc(r.phase_duration)}`:'';
      return `<tr class="row" data-i="${rows.indexOf(r)}">
        <td class="name">${esc(r.name)}</td>
        <td>${r.is_latest?tag('最新','latest'):tag('历史','history')}</td>
        <td><span class="tag ${r.status==='CLOSED'?'status-closed':'status-open'}">${esc(r.status)}</span></td>
        <td>${esc(r.phase_label)}${r.phase_total?` <span class="pct">${n(r.phase_done)}/${n(r.phase_total)}</span>`:''}</td>
        <td style="min-width:120px"><div class="bar"><i style="width:${pct}%"></i></div><div class="pct">${pct}%${dur}</div></td>
        <td>${esc(r.summary?.['官网']||'—')}</td>
        <td>${n(r.summary?.['社媒'])}</td>
        <td>${risk}</td>
      </tr>`;
    }).join('') || `<tr><td colspan="8" class="empty">没有符合条件的企业</td></tr>`;
    $('#plabel').textContent=`${st.page} / ${pages}`;
    $('#prev').disabled=st.page===1; $('#next').disabled=st.page===pages;
  }

  function show(i){
    const r=rows[i];
    $('#dname').textContent=r.name;
    $('#dmeta').innerHTML=`${r.is_latest?tag('最新批次','latest'):tag('历史批次','history')} · ${esc(r.run_id)} · ${esc(r.run_time)} · 状态 ${esc(r.status)}`;
    const timings = (r.timings||[]).map(t=>{
      const pct=t.total?Math.round(n(t.done)/n(t.total)*100):0;
      const cnt=t.total?`${n(t.done)}/${n(t.total)}`:t.done==null?'—':`${n(t.done)}/${n(t.total)}`;
      return `<div class="timing"><span class="pl">${esc(t.label)}</span><span class="bar"><i style="width:${pct}%"></i></span><span class="pt">${t.duration?esc(t.duration):'—'} ${t.reused?`<span class="reuse">复用${t.reused}</span>`:''}</span><span class="pt">${cnt}</span></div>`;
    }).join('');
    const detail=(r.detail||[]).map(b=>`
      <div class="kv"><div class="kv-head">${esc(b.label)}</div>
      ${b.fields.map(([k,v])=>`<div class="kv-row"><div class="k">${esc(k)}</div><div class="vv">${linkify(v)||'—'}</div></div>`).join('')}
      </div>`).join('');
    let files='';
    if(r.files && r.files.length){
      const groups={};
      r.files.forEach(f=>{ (groups[f.phase]=groups[f.phase]||[]).push(f); });
      const mkItem = f => {
        if(f.kind==='截图'){
          return `<span class="k">${esc(f.label)}</span><div class="thumb-grid">` +
            `<img class="thumb" src="${esc(f.uri)}" loading="lazy" title="${esc(f.path)}" ` +
            `onclick="window.open('${esc(f.uri)}','_blank')">` +
            `<div><a href="${esc(f.uri)}" target="_blank">打开原图</a></div></div>`;
        }
        return `<span class="k">${esc(f.kind)}</span><a href="${esc(f.uri)}" target="_blank">${esc(f.path)}</a>`;
      };
      files=`<h3>证据截图与中间文件</h3><div class="files">` +
        Object.entries(groups).map(([ph, arr])=>
          `<div class="g">${esc(arr[0].label)}（${arr.length}）</div>` +
          arr.map(mkItem).map(x=>`<div class="f">${x}</div>`).join('')).join('') +
        `</div>`;
    }
    $('#dbody').innerHTML = `<h3>阶段进度与耗时</h3>${timings}${detail}${files}`;
    $('#d').showModal();
  }

  $('#tbody').addEventListener('click',e=>{const tr=e.target.closest('tr[data-i]');if(tr)show(n(tr.dataset.i));});
  ['q','batch-filter'].forEach(id=>$(id).addEventListener(id==='q'?'input':'change',()=>{st.page=1;render();}));
  $('#page-size').addEventListener('change',e=>{st.size=n(e.target.value);st.page=1;render();});
  $('#prev').onclick=()=>{st.page--;render();};
  $('#next').onclick=()=>{st.page++;render();};
  $('#dclose').onclick=()=>$('#d').close();
  $('#d').addEventListener('click',e=>{if(e.target===$('#d'))$('#d').close();});
  render();
})();
</script>
</body></html>"""


def main():
    common.setup_utf8()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(ROOT / "outputs" / "企业排查看板.html"))
    args = parser.parse_args()

    out = Path(args.out)
    run_times = load_run_times()
    global_latest, companies_db, tasks_db, timings_db = load_db()
    rows = load_companies(run_times, companies_db, tasks_db, timings_db, global_latest, out.parent)

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "latest_run": global_latest,
        "latest_run_time": run_times.get(global_latest, "") if global_latest else "",
        "companies": rows,
    }
    html = PAGE.replace("__DATA__", json.dumps(payload, ensure_ascii=False).replace("</", "<\\/"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8", newline="\n")
    print(json.dumps({"companies": len(rows), "output": str(out)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
