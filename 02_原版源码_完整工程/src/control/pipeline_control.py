from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

"""Runtime control CLI for a live ``main_agent.py`` run."""

import argparse
import sys
from pathlib import Path
from uuid import uuid4

from pipeline_store import PipelineStore


PHASE_LABELS = {
    "WEB_ICP": "ICP备案查询",
    "WEB_SEARCH": "互联网载体搜索",
    "WEB_CRAWL": "官网文字抓取",
    "IMAGE_DOWNLOAD": "网页图片下载",
    "OCR": "图片文字识别",
    "KEYWORD": "LLM账号关键词精选",
    "ACCOUNT_DISCOVERY": "社媒账号查询",
    "ACCOUNT_RANK": "账号候选初筛",
    "ACCOUNT_REVIEW": "账号样本帖子复核",
    "ACCOUNT_VERIFY": "账号归属定级",
    "POST_CRAWL": "账号作品抓取",
    "RISK_ANALYSIS": "企业风险判定",
    "EXPORT": "结果导出",
}
# 主流程的 DB 在项目根 pipeline_output/data/main_agent.db（main_agent 以 output_dir 为根）。
# 不能用 __file__.with_name：那会指向 src/control/pipeline_output，建出一个空库再报"没有正在运行的批次"。
DEFAULT_DB = Path(__file__).resolve().parents[2] / "pipeline_output" / "data" / "main_agent.db"


def normalize_phase(value: str) -> str:
    raw = value.strip()
    code = raw.upper().replace("-", "_")
    if code in PHASE_LABELS:
        return code
    matches = [phase for phase, label in PHASE_LABELS.items() if raw == label]
    if len(matches) == 1:
        return matches[0]
    valid = "、".join(f"{phase}（{label}）" for phase, label in PHASE_LABELS.items())
    raise ValueError(f"未知环节 {value!r}；可用值：{valid}")


def resolve_run(store: PipelineStore, selector: str):
    row = store.latest_running_run() if selector == "latest" else store.get_run(selector)
    if row is None:
        raise ValueError("没有找到正在运行的批次" if selector == "latest" else f"批次不存在：{selector}")
    if str(row["status"]) != "RUNNING":
        raise ValueError(f"批次 {row['run_id']} 当前状态为 {row['status']}，不能发送运行时指令")
    return row


def resolve_company(store: PipelineStore, run_id: str, selector: str):
    companies = store.run_companies(run_id)
    exact = [
        row for row in companies
        if str(row["company_id"]) == selector or str(row["company_name"]) == selector
    ]
    if len(exact) == 1:
        return exact[0]
    folded = selector.casefold()
    casefold_matches = [row for row in companies if str(row["company_name"]).casefold() == folded]
    if len(casefold_matches) == 1:
        return casefold_matches[0]
    raise ValueError(f"批次 {run_id} 中找不到企业（请使用企业全称或 company_id）：{selector}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="向运行中的企业流水线发送控制指令")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="main_agent 使用的 SQLite 路径")
    subparsers = parser.add_subparsers(dest="action", required=True)
    skip = subparsers.add_parser("skip", help="跳过指定企业当前正在等待或执行的某个环节")
    skip.add_argument("--company", required=True, help="企业全称或 company_id")
    skip.add_argument("--phase", required=True, help="环节代码或中文名，例如 WEB_CRAWL 或 官网文字抓取")
    skip.add_argument("--run", default="latest", help="运行批次 ID；默认取最新 RUNNING 批次")
    skip.add_argument("--reason", default="operator_request", help="审计记录中的跳过原因")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = PipelineStore(args.db.resolve())
    store.init()
    try:
        run = resolve_run(store, args.run)
        run_id = str(run["run_id"])
        company = resolve_company(store, run_id, args.company)
        phase = normalize_phase(args.phase)
        command_id = uuid4().hex
        store.create_control_command(
            command_id, run_id, "SKIP_PHASE", str(company["company_id"]), phase, args.reason,
        )
        print(
            f"已提交跳过指令 {command_id}\n"
            f"批次：{run_id}\n"
            f"企业：{company['company_name']}\n"
            f"环节：{phase}（{PHASE_LABELS[phase]}）\n"
            "主流程通常会在 1 秒内确认；若该企业已离开此环节，指令会安全拒绝。",
            flush=True,
        )
        return 0
    except ValueError as exc:
        print(f"无法提交指令：{exc}", file=sys.stderr)
        return 2
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
