# -*- coding: utf-8 -*-
"""把离线演示产品构建为纯静态站点（供 GitHub Pages 发布）。

做法：
1. 复用 run.py 的数据组装函数，生成 api-bundle.js（保证与本地服务版形状一致）；
2. 复制 ui/ 前端文件；
3. 注入 mock-api.js 垫片：在浏览器内把 /api/* 的 fetch 路由到 bundle，并模拟运行/复核。

用法：
    python tools/build_static_site.py
输出：
    <仓库根>/site/   （交给 GitHub Pages 发布）
"""
from __future__ import annotations

import importlib.util
import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent          # .../01_可运行Web产品_离线演示/tools
DEMO = HERE.parent                               # .../01_可运行Web产品_离线演示
REPO = DEMO.parent                               # 提交包根目录
SITE = REPO / "site"

# 动态导入同目录的 run.py
spec = importlib.util.spec_from_file_location("gg_run", DEMO / "run.py")
gg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gg)  # type: ignore[union-attr]

NAMES = [c["name"] for c in gg.COMPANIES]
SECTIONS = ["icp", "website", "social", "risk", "evidence"]
PHASES = gg.PHASES


def build_bundle() -> dict:
    progress = {"ok": True, "companies": [gg._progress_company(c) for c in gg.COMPANIES],
                "summary": {"total": len(gg.COMPANIES)}}
    company: dict[str, dict] = {}
    phase: dict[str, dict] = {}
    for name in NAMES:
        company[name] = {sec: gg._company_section(name, sec) for sec in SECTIONS}
        phase[name] = {
            ph: {"ok": True, "data": {"tasks": [
                {"task_dir": f"{ph}/task_01", "status": "OK",
                 "summary": {"阶段": ph, "说明": "静态演示产物"}}]}}
            for ph in PHASES
        }
    log_lines = [
        "[静态演示] 已加载构造/脱敏数据，共 %d 家企业" % len(NAMES),
        "[静态演示] 示例企业G 因数据缺失标记“待核实”；示例企业H 触发本地规则降级",
        "[静态演示] 点击「开始排查」可查看 12 阶段推进（浏览器内模拟）",
    ]
    return {
        "meta": gg.META,
        "dashboard": {"ok": True, "data": gg._dashboard()},
        "progress": progress,
        "company": company,
        "phase": phase,
        "log_lines": log_lines,
    }


def main() -> None:
    if SITE.exists():
        shutil.rmtree(SITE)
    (SITE / "data").mkdir(parents=True)

    bundle = build_bundle()
    (SITE / "data" / "api-bundle.js").write_text(
        "window.__GG_BUNDLE__ = " + json.dumps(bundle, ensure_ascii=False) + ";\n",
        encoding="utf-8")
    (SITE / "data" / "api-bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=1), encoding="utf-8")

    # 复制前端
    shutil.copy2(DEMO / "ui" / "app.js", SITE / "app.js")
    shutil.copy2(DEMO / "ui" / "mock-api.js", SITE / "mock-api.js")

    # index.html：路径改相对、注入垫片
    html = (DEMO / "ui" / "index.html").read_text(encoding="utf-8")
    html = html.replace('href="/about"', 'href="about.html"')
    html = html.replace(
        '<script src="/static/app.js"></script>',
        '<script src="data/api-bundle.js"></script>\n'
        '<script src="mock-api.js"></script>\n'
        '<script src="app.js"></script>')
    (SITE / "index.html").write_text(html, encoding="utf-8")

    # about.html：路径改相对
    about = (DEMO / "ui" / "about.html").read_text(encoding="utf-8")
    about = about.replace('href="/"', 'href="index.html"')
    (SITE / "about.html").write_text(about, encoding="utf-8")

    print(f"静态站点已生成：{SITE}")
    print("  index.html / app.js / mock-api.js / about.html / data/api-bundle.js")


if __name__ == "__main__":
    main()
