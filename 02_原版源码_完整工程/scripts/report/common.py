# -*- coding: utf-8 -*-
"""报告提取公共基座：run/企业定位、DB 连接、JSON 输出。只读，无第三方依赖。"""
from __future__ import annotations

import json
import os
import sqlite3
import sys

def _find_root() -> str:
    """从本文件向上逐级查找第一个含有 src/control/main_agent.py 的目录作为项目根；
    找不到时回退到按原 .workbuddy/skills/pipeline-query/report 目录层级的推算。"""
    current = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.isfile(os.path.join(current, "src", "control", "main_agent.py")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


ROOT = _find_root()
RUNS_DIR = os.path.join(ROOT, "pipeline_output", "runs")
DB_PATH = os.path.join(ROOT, "pipeline_output", "data", "main_agent.db")


def setup_utf8() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def safe_name(value: str, limit: int = 80) -> str:
    """与 src/pipeline_runtime.py 保持一致的目录名规范化。"""
    cleaned = []
    for ch in value.strip():
        if ch.isalnum() or ch in {"_", "-", "."}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    name = "".join(cleaned).strip("_.")
    return (name[:limit] or "item")


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def list_runs() -> list[dict]:
    """所有批次，按开始时间倒序。"""
    with db() as con:
        rows = con.execute(
            "SELECT run_id, status, input_file, started_at, finished_at "
            "FROM runs ORDER BY started_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def resolve_run(run_id: str | None) -> tuple[str, str]:
    """返回 (run_dir, run_id)。run_id 为 latest 时取最近开始的批次。"""
    runs = list_runs()
    if not runs:
        sys.exit("错误：台账中没有批次记录")
    if run_id in (None, "", "latest"):
        chosen = runs[0]
    else:
        hits = [r for r in runs if r["run_id"] == run_id]
        if not hits:
            sys.exit(f"错误：找不到批次 {run_id}。可用批次：{', '.join(r['run_id'] for r in runs)}")
        chosen = hits[0]
    return os.path.join(RUNS_DIR, chosen["run_id"]), chosen["run_id"]


def list_exports(run_dir: str) -> list[str]:
    """批次 exports 目录下的企业导出 JSON 绝对路径。"""
    exports_dir = os.path.join(run_dir, "exports")
    if not os.path.isdir(exports_dir):
        return []
    return sorted(
        os.path.join(exports_dir, f)
        for f in os.listdir(exports_dir)
        if f.endswith(".json")
    )


def find_company(run_dir: str, query: str) -> tuple[str, str]:
    """按名称模糊匹配企业，返回 (company_name, export_path)。

    企业目录名经过 safe_name 规范化（特殊字符转下划线），因此匹配时同时比较原始名与规范化名。
    """
    exports = list_exports(run_dir)
    names = [os.path.splitext(os.path.basename(p))[0] for p in exports]
    # 兜底：未导出企业也可用 companies 目录名匹配
    companies_dir = os.path.join(run_dir, "companies")
    if os.path.isdir(companies_dir):
        names += [n for n in os.listdir(companies_dir) if os.path.isdir(os.path.join(companies_dir, n))]
    names = list(dict.fromkeys(names))

    query_safe = safe_name(query)
    # 先精确匹配原始名
    if query in names:
        chosen = query
    else:
        # 再按原始名子串 / 规范化名子串匹配
        hits = [n for n in names if query in n or query_safe in n or safe_name(n) == query_safe]
        if not hits:
            sys.exit(f"错误：在 {len(names)} 家企业中找不到包含「{query}」的企业")
        if len(hits) > 1:
            print(f"提示：匹配到多家，取第一家：{hits}", file=sys.stderr)
        chosen = hits[0]
    export_path = os.path.join(run_dir, "exports", chosen + ".json")
    return chosen, export_path


def load_json(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def company_names_from_db(run_id: str) -> dict[str, str]:
    """run_id 下 company_id -> company_name 映射。"""
    with db() as con:
        rows = con.execute(
            "SELECT company_id, company_name FROM companies WHERE run_id=?",
            (run_id,),
        ).fetchall()
    return {r["company_id"]: r["company_name"] for r in rows}


def resolve_task_result(con: sqlite3.Connection, task_id: str) -> str | None:
    """读取 tasks.result_json；若已清空则回退到 attempt_output_items.data_json。"""
    row = con.execute(
        "SELECT result_json FROM tasks WHERE task_id=?", (task_id,)
    ).fetchone()
    if row and row["result_json"]:
        return row["result_json"]
    row = con.execute(
        """SELECT i.data_json
           FROM attempt_output_items i
           JOIN task_attempts ta ON ta.attempt_id = i.attempt_id
           WHERE ta.task_id=? AND ta.status IN ('SUCCEEDED','EMPTY_SUCCESS')
           ORDER BY ta.attempt_no DESC, i.seq DESC LIMIT 1""",
        (task_id,),
    ).fetchone()
    return row["data_json"] if row else None


def task_ids_from_db(run_id: str, chunk: int = 800) -> list[str]:
    """run_id 下全部 task_id（分块防 SQLite 参数上限）。"""
    with db() as con:
        rows = con.execute(
            "SELECT task_id FROM tasks WHERE run_id=?", (run_id,)
        ).fetchall()
    return [r["task_id"] for r in rows]


def events_for_run(run_id: str, chunk: int = 800) -> list[dict]:
    """查询某批次的全部审计事件（task 级 + company 级，按时间窗口防跨批污染）。"""
    out: list[dict] = []
    with db() as con:
        run = con.execute(
            "SELECT started_at, finished_at FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        started = run["started_at"] if run else None
        finished = run["finished_at"] if run else None
        task_ids = [
            r["task_id"]
            for r in con.execute(
                "SELECT task_id FROM tasks WHERE run_id=?", (run_id,)
            ).fetchall()
        ]
        company_ids = [
            r["company_id"]
            for r in con.execute(
                "SELECT company_id FROM companies WHERE run_id=?", (run_id,)
            ).fetchall()
        ]
        for i in range(0, len(task_ids), chunk):
            part = task_ids[i : i + chunk]
            marks = ",".join("?" * len(part))
            rows = con.execute(
                f"SELECT event_id, time, scope, target_id, event, payload_json "
                f"FROM events WHERE target_id IN ({marks})",
                part,
            ).fetchall()
            out.extend(dict(r) for r in rows)
        if company_ids and started:
            end = finished or "9999-12-31T23:59:59+00:00"
            for i in range(0, len(company_ids), chunk):
                part = company_ids[i : i + chunk]
                marks = ",".join("?" * len(part))
                rows = con.execute(
                    f"SELECT event_id, time, scope, target_id, event, payload_json "
                    f"FROM events WHERE target_id IN ({marks}) AND time >= ? AND time <= ?",
                    [*part, started, end],
                ).fetchall()
                out.extend(dict(r) for r in rows)
    return out
