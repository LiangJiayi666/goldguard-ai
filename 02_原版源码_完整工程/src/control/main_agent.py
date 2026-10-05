from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

"""控制层主文件。

负责三件事：把公司包装成稳定的子状态机、调度重试与登录刷新和资源租约、
按故事线把各阶段结果推进到下一阶段。是整个流水线的大脑。
"""

import argparse
import concurrent.futures
import copy
import dataclasses
import hashlib
import json
import re
import subprocess
import sys
import threading
import time
import traceback
import unicodedata
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit
from uuid import uuid4
import functools
import os
import random
import shutil


from pipeline_runtime import (
    AttemptDiagnosis,
    build_manifest,
    canonical_attempt_diagnosis,
    extract_atomic_diagnosis,
    iso_utc,
    safe_name,
    stable_json,
    request_fingerprint,
    unwrap_atomic_data,
)
from pipeline_store import PipelineStore
from keyword_matcher import keyword_records, write_keyword_records
from web_crawl_policy import (
    DEFAULT_MAX_DEPTH,
    DEFAULT_MAX_PAGES,
    canonicalize_url,
    dedupe_image_candidates,
    dedupe_links,
    host_variant_url,
    is_recursive_page_url,
    same_site,
    site_host,
)


DEFAULT_REPLAY_COMPANIES_FILE = Path(__file__).resolve().parents[2] / "config" / "replay_companies.txt"
DEFAULT_SOCIAL_ACCOUNT_SEEDS_FILE = Path(__file__).resolve().parents[2] / "config" / "social_account_seeds.json"


# 阶段名与代码一一对应的故事线。
PHASES = [
    "WEB_ICP", "WEB_SEARCH", "WEB_CRAWL", "IMAGE_DOWNLOAD", "OCR", "KEYWORD",
    "ACCOUNT_DISCOVERY", "ACCOUNT_RANK", "ACCOUNT_REVIEW", "ACCOUNT_VERIFY",
    "POST_CRAWL", "RISK_ANALYSIS", "RISK_EVIDENCE",
]
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
    "RISK_EVIDENCE": "风险证据截图",
    "EXPORT": "结果导出",
}
PLATFORM_LABELS = {"douyin": "抖音", "xhs": "小红书", "kuaishou": "快手"}
ICP_CARRIER_LABELS = {"web": "网站", "app": "APP", "mapp": "小程序", "kapp": "快应用"}
# 三平台。所有平台分叉都围绕这三个名字。
PLATFORMS = ("douyin", "xhs", "kuaishou")
# 不同资源类型的最小发放间隔，避免同类请求扎堆。
# 第一轮 ICP 备案查询覆盖四类载体。只有 web 的结果会作为官网抓取入口；
# app / mapp / kapp 结果仍然保留在 ICP 产物中，但不进入 WEB_CRAWL。
ICP_CARRIERS = ("web", "app", "mapp", "kapp")
MAX_IMAGE_CANDIDATES = 500
# 资源节流：控制层 best-effort 约束，非全局硬锁。
# 旨在显著降低同类请求的发频，但不保证"始终大于等于最小值"。
# 以下路径可能绕过节流：未挂 resource_type/resource_key 的请求、历史复用、空任务、内部 fan-out、重排路径。
RESOURCE_GAP_SECONDS = {"ICP": 20.0, "OCR": 0.5, "LLM": 5.0, "PLATFORM": 20.0, "HOSTNAME": 3.0}
# 熔断冷却基础值：lane 命中瞬时故障(rate_limit/captcha/超时/断网)后停发许可，到点放一个探测任务，成功才恢复。
# 外部目标(ICP/HOSTNAME/PLATFORM)给 4~6min；内部服务(OCR/LLM)短些。实际冷却 = base + 0..JITTER 抖动，错峰探测。
RESOURCE_COOLDOWN_SECONDS = {"ICP": 240.0, "OCR": 20.0, "LLM": 30.0, "PLATFORM": 600.0, "HOSTNAME": 30.0}
RESOURCE_COOLDOWN_JITTER_SECONDS = {"PLATFORM": 300.0, "HOSTNAME": 30.0}
COOLDOWN_JITTER_SECONDS = 120.0
# 单个任务在队列里等许可的最大秒数：超时则跳过该任务、企业推进。这是 run 一定能跑完的兜底（方案一）。
TASK_EXECUTION_TIMEOUT = 600  # 单次子进程执行上限（秒），到点 kill。
SOCIAL_LOGIN_WAIT_TIMEOUT = 600  # 社媒因登录失效无法获租约时的任务级等待上限。
# 首次执行失败后，允许再重试 3 次；第 4 次失败才写 FAILED_FINAL。
MAX_TASK_RETRIES = 3
LOGIN_REFRESH_TIMEOUT = 600
# 社媒任务首次报 login_required 时先冷却复核的秒数：限频页面（"操作过于频繁，请稍后再试"）
# 常带登录入口，容易被误判成掉登录；此时立刻弹扫码刷新会被平台判高频，把真登录态打掉。
# 先冷却 ~90s 再让原任务重试，仍报 login_required 才真正启动登录刷新。
LOGIN_REFRESH_CONFIRM_COOLDOWN_SECONDS = 90.0
PLATFORM_REFRESH_SCRIPTS = {"douyin": "douyin_login_refresh.py", "xhs": "xhs_login_refresh.py", "kuaishou": "kuaishou_login_refresh.py"}
# 这些终态的作品结果不算有效覆盖，去重时跳过。
POST_FAIL_STATUSES = {"login_required", "cookie_invalid", "login_expired", "rate_limited", "captcha_required", "error", "failed", "failed_final", "missing_output"}
ACCOUNT_ACTIVITY_WINDOW_DAYS = 30
ACCOUNT_REVIEW_CANDIDATES_PER_PLATFORM = 5
ACCOUNT_REVIEW_SAMPLE_SIZE = 8
# 复核只看账号主页第一页（不翻页、不滚动、不开详情页）：自然呈现几条采几条，
# max-items 取各平台首页自然条数（douyin API 单页 18；xhs 首屏约 20；ks 首批 feed 约 30）。
ACCOUNT_REVIEW_PAGE_SIZE = {"douyin": 18, "xhs": 20, "kuaishou": 30}
MAX_POST_ITEMS_PER_ACCOUNT = 30
MAX_POST_DETAILS_PER_ACCOUNT = 8
# 每企业社媒搜索任务（ACCOUNT_DISCOVERY）全批次累计上限；分级轮转取词，见 KEYWORD 入队处。
MAX_DISCOVERY_TASKS_PER_COMPANY = 8
# 全量抓取不再设页数上限：翻页终止靠 has_more=false、游标不前进检测与库存全命中。
MAX_POST_PAGES_PER_ACCOUNT = 2       # 仅采样模式（full_crawl=False）的翻页预算
SOCIAL_SCREENSHOT_VERSION = 1
# 兼容上一版内部常量名；新逻辑已覆盖三平台。
XHS_ACTIVITY_WINDOW_DAYS = ACCOUNT_ACTIVITY_WINDOW_DAYS
PLATFORM_POST_TIME_FIELDS: dict[str, tuple[str, ...]] = {
    "douyin": ("create_time", "publish_time", "timestamp", "time"),
    "xhs": ("time", "last_update_time", "lastUpdateTime", "create_time", "timestamp"),
    "kuaishou": ("timestamp", "photo_timestamp", "create_time", "publish_time", "time"),
}


def _parse_post_datetime(value: Any, *, now: datetime | None = None) -> datetime | None:
    """Parse XHS post times (epoch seconds/milliseconds or common date strings)."""
    if value is None or isinstance(value, bool):
        return None
    reference = now or datetime.now().astimezone()
    local_tz = reference.tzinfo or timezone.utc
    numeric: float | None = None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
            numeric = float(text)
        else:
            normalized = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                parsed = None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
                            "%Y年%m月%d日 %H:%M:%S", "%Y年%m月%d日"):
                    try:
                        parsed = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue
            if parsed is None:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=local_tz)
            return parsed.astimezone(local_tz)
    if numeric is None:
        return None
    absolute = abs(numeric)
    if absolute >= 100_000_000_000_000:
        numeric /= 1_000_000  # microseconds
    elif absolute >= 100_000_000_000:
        numeric /= 1_000  # milliseconds
    try:
        return datetime.fromtimestamp(numeric, tz=timezone.utc).astimezone(local_tz)
    except (OverflowError, OSError, ValueError):
        return None


def _post_activity(value: Any, *, now: datetime | None = None) -> dict[str, Any]:
    """Return the persisted 30-day activity judgement for one post time."""
    checked_at = now or datetime.now().astimezone()
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    post_time = _parse_post_datetime(value, now=checked_at)
    if post_time is None:
        status, active = "帖子时间未知", None
    else:
        active = checked_at - timedelta(days=ACCOUNT_ACTIVITY_WINDOW_DAYS) <= post_time <= checked_at
        status = "活跃帖子" if active else "不活跃帖子"
    return {
        "latest_post_time_raw": value,
        "latest_post_time": post_time.isoformat(timespec="seconds") if post_time else None,
        "activity_status": status,
        "is_active": active,
        "activity_window_days": ACCOUNT_ACTIVITY_WINDOW_DAYS,
        "activity_checked_at": checked_at.isoformat(timespec="seconds"),
    }


def _account_activity(platform: str, posts: Iterable[dict[str, Any]], *, now: datetime | None = None) -> dict[str, Any]:
    """Pick the newest post in one crawl result and judge the account's 30-day activity."""
    checked_at = now or datetime.now().astimezone()
    post_list = [post for post in posts if isinstance(post, dict)]
    latest_time: datetime | None = None
    latest_raw: Any = None
    latest_post_id: str | None = None
    posts_with_time = 0
    fields = PLATFORM_POST_TIME_FIELDS.get(platform, ("time", "timestamp", "create_time", "publish_time"))
    id_fields = {
        "douyin": ("aweme_id", "video_id", "item_id"),
        "xhs": ("note_id", "item_id"),
        "kuaishou": ("photo_id", "video_id", "item_id"),
    }.get(platform, ("item_id",))
    for post in post_list:
        raw = next((post.get(field) for field in fields if post.get(field) is not None), None)
        parsed = _parse_post_datetime(raw, now=checked_at)
        if parsed is None:
            continue
        posts_with_time += 1
        if latest_time is None or parsed > latest_time:
            latest_time, latest_raw = parsed, raw
            latest_post_id = next((str(post.get(field)) for field in id_fields if post.get(field)), None)
    judgement = _post_activity(latest_raw, now=checked_at)
    if judgement["is_active"] is None:
        status = "活跃状态未知"
    else:
        status = "活跃账号" if judgement["is_active"] else "不活跃账号"
    return {
        **judgement,
        "activity_status": status,
        "latest_post_id": latest_post_id,
        "posts_checked": len(post_list),
        "posts_with_time": posts_with_time,
    }


def _domain_activity(url: str, summary: dict[str, Any], *, error_text: str = "",
                     probe_text: str = "", now: datetime | None = None) -> dict[str, Any]:
    """Classify reachability without treating anti-bot failures as a dead domain."""
    checked_at = now or datetime.now().astimezone()
    domain = (urlsplit(url).hostname or url).lower()
    combined = "\n".join((stable_json(summary), error_text, probe_text)).lower()
    http_codes = [int(code) for code in re.findall(r"(?:browser http|http(?:/\S+)?|returned error:)\s*(\d{3})", combined)]
    http_status = http_codes[-1] if http_codes else None
    anti_bot_markers = (
        "captcha", "验证码", "机器人", "robot", "access denied", "forbidden",
        "too many requests", "rate limit", "rate_limited", "cloudflare",
    )
    anti_bot = (http_status in {401, 403, 406, 418, 429}) or any(marker in combined for marker in anti_bot_markers)
    fetched = bool(summary.get("fetched_url")) and str(summary.get("status") or "").lower() not in {
        "error", "failed", "timeout", "missing_output",
    }
    sparse_response = "sparse html" in combined
    dns_failure = any(marker in combined for marker in (
        "could not resolve", "name_not_resolved", "err_name_not_resolved", "dns_failure", "nxdomain",
        "no such host", "name or service not known",
    ))
    connection_refused = any(marker in combined for marker in (
        "connection refused", "actively refused", "couldn't connect to server", "could not connect to server",
    )) or bool(re.search(r"curl:\s*\(7\)", combined))
    timed_out = any(marker in combined for marker in ("timed out", "timeout", "connect_timeout", "err_connection_timed_out"))
    tls_failure = any(marker in combined for marker in (
        "ssl", "tls", "certificate", "cert_", "err_cert_", "schannel", "handshake",
    ))

    if fetched:
        is_active, status, access_type = True, "活跃域名", "normal"
        reason = "浏览器或普通抓取成功访问"
    elif anti_bot:
        is_active, status, access_type = True, "活跃域名", "suspected_anti_bot"
        reason = "服务器有响应，但疑似存在反爬、验证码或访问限制"
    elif http_status is not None or sparse_response:
        is_active, status, access_type = True, "活跃域名", "server_responded"
        reason = f"服务器返回 HTTP {http_status}" if http_status is not None else "服务器返回了页面内容，但正文稀疏"
    elif dns_failure:
        is_active, status, access_type = False, "不活跃域名", "dns_failure"
        reason = "域名无法解析"
    elif connection_refused:
        is_active, status, access_type = False, "不活跃域名", "connection_refused"
        reason = "HTTPS/HTTP 连接被拒绝"
    elif timed_out:
        is_active, status, access_type = None, "活跃状态未知", "timeout"
        reason = "访问超时，可能是网络波动、地域限制或服务器无响应"
    elif tls_failure:
        is_active, status, access_type = None, "活跃状态未知", "tls_failure"
        reason = "TLS/证书异常，不能据此确认域名已经失效"
    else:
        is_active, status, access_type = None, "活跃状态未知", "unclassified_failure"
        reason = "抓取失败证据不足，无法可靠区分反爬与失效"
    return {
        "domain": domain,
        "url": url,
        "activity_status": status,
        "is_active": is_active,
        "access_type": access_type,
        "suspected_anti_bot": anti_bot,
        "http_status": http_status,
        "reason": reason,
        "checked_at": checked_at.isoformat(timespec="seconds"),
    }

# stderr 分类契约：原子脚本崩溃（没写出 result.json）时，main_agent 靠 stderr 文本匹配判状态。
# 改任何脚本的报错措辞前先同步这里——这是隐式契约，单点定义、_diagnose_attempt 共用。
STDERR_CLASSIFIERS: tuple[tuple[tuple[str, ...], str, str], ...] = (
    (("login_required", "cookie_invalid", "需要重新登录", "登录失效", "login expired"), "RETRY_WAIT", "login_required"),
    # 配置缺失不是 argv 契约问题。此前宽泛匹配 ``missing`` 会把
    # "missing LLM API key" 伪报成 argparse 缺陷并白白重试四次。
    (("missing llm api key", "llm api key is required"), "FAILED_FINAL", "configuration_error"),
    # 这里只保留 argparse 的明确措辞；通用的 invalid/missing 可能来自
    # 业务数据、上游服务或运行配置，不能据此认定 main_agent 造错命令。
    (("bad_command", "unrecognized argument", "unrecognized arguments", "the following arguments are required", "invalid choice", "invalid int value", "invalid float value"), "RETRY_WAIT", "invalid_input"),
    # DNS 解析失败是确定性故障：备案域名已过期/弃用时 getaddrinfo 会一直失败，
    # 重试只会白耗 4 次执行 + 4~6 分钟冷却，直接判终态。
    (("could not resolve", "getaddrinfo failed", "name or service not known", "no such host", "temporary failure in name resolution"), "FAILED_FINAL", "deterministic_dns_error"),
    (("timeout", "timed out", "connection", "reset", "omp:", "browser", "worker"), "RETRY_WAIT", "retryable_error"),
)


def _classify_stderr(stderr: str) -> tuple[str, str]:
    """按 stderr 文本判 (status, error_code)；无命中 → (RETRY_WAIT, process_error)：未分类崩溃默认可重试。"""
    lowered = stderr.lower()
    for tokens, status, error_code in STDERR_CLASSIFIERS:
        if any(tok in lowered for tok in tokens):
            return status, error_code
    return "RETRY_WAIT", "process_error"


def _suggest_invalid_input_fix(stderr: str) -> str:
    """把 argparse 契约错误的 stderr 翻成"嫌疑在哪"的维护指针。"""
    text = (stderr or "").lower()
    if "unrecognized argument" in text:
        return "main_agent 发了脚本不认的参数——核对 _command_for / PlatformSpec 与该脚本 argparse"
    if "are required" in text:
        return "脚本必填参数缺失——核对 main_agent 是否填充（上游可能给了空值）"
    if "invalid" in text:
        return "参数取值不被脚本接受——核对取值来源与脚本 argparse 类型/枚举"
    return "argparse 契约不匹配——比对 main_agent 造命令与脚本 argparse 定义"


def _build_script_defect(agent: EnterpriseSubAgent, request: Request, command: list[str] | None,
                         stderr: str, attempt_no: int) -> dict[str, Any]:
    """invalid_input 的结构化缺陷记录：脚本契约被违反的完整证据，供 export / defects.md 上报。"""
    argv = list(command or [])
    return {
        "kind": "invalid_input",
        "company": agent.name,
        "phase": request.phase,
        "platform": request.platform,
        "script": Path(str(argv[1])).name if len(argv) > 1 else None,
        "command": argv,
        "stderr_tail": (stderr or "").strip()[-1000:],
        "attempt_no": attempt_no,
        "suggested_fix": _suggest_invalid_input_fix(stderr),
    }


_SCRIPT_DIR = Path(__file__).parent                       # src/control
_PROJECT_ROOT = _SCRIPT_DIR.resolve().parents[1]          # 项目根
_SERVICE_DIR = _PROJECT_ROOT / "scripts"                  # 服务启停脚本

# 原子脚本按功能分目录存放；main_agent 从这里解析实际路径。
_SCRIPT_SUBDIRS: dict[str, str] = {
    "screenshot_crawler.py": "crawl",
    "image_downloader.py": "crawl",
    "web_search_official.py": "crawl",
    "xhs_search.py": "social",
    "xhs_review.py": "social",
    "xhs_posts.py": "social",
    "xhs_detail.py": "social",
    "xhs_login_refresh.py": "social",
    "douyin_search.py": "social",
    "douyin_posts.py": "social",
    "douyin_login_refresh.py": "social",
    "kuaishou_search.py": "social",
    "kuaishou_review.py": "social",
    "kuaishou_posts.py": "social",
    "kuaishou_login_refresh.py": "social",
    "social_evidence_screenshot.py": "social",
    "social_account_rank.py": "analysis",
    "social_account_review.py": "analysis",
    "social_keyword_extract.py": "analysis",
    "enterprise_risk_analysis.py": "analysis",
    "ocr_image.py": "ocr",
    "icp_query_one.py": "icp",
}


@functools.lru_cache(maxsize=None)
def _script_path(name: str) -> str:
    subdir = _SCRIPT_SUBDIRS.get(name, "")
    return str(_SCRIPT_DIR.parent / subdir / name) if subdir else str(_SCRIPT_DIR / name)


_SUPPORT_SERVICE_START_SCRIPTS = (
    ("ICP", "open_icp_service.ps1"),
    ("OCR", "open_ocr_service.ps1"),
)
_SUPPORT_SERVICE_STOP_SCRIPT = "close_services.ps1"


def _powershell_executable() -> str:
    """Return a PowerShell executable suitable for the existing service launchers."""
    executable = shutil.which("powershell.exe") or shutil.which("powershell")
    if not executable:
        raise RuntimeError("PowerShell was not found; cannot manage ICP/OCR services")
    return executable


def start_support_services() -> None:
    """Start ICP and OCR before any pipeline work is bootstrapped."""
    powershell = _powershell_executable()
    for service_name, script_name in _SUPPORT_SERVICE_START_SCRIPTS:
        script = _SERVICE_DIR / script_name
        if not script.is_file():
            raise RuntimeError(f"{service_name} service launcher not found: {script}")
        print(f"[service] starting {service_name} ...", flush=True)
        completed = subprocess.run(
            [
                powershell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                str(script), "-PythonExecutable", sys.executable,
            ],
            cwd=_SERVICE_DIR,
            stdin=subprocess.DEVNULL,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"{service_name} service failed to start (exit {completed.returncode})")


def stop_support_services() -> bool:
    """Stop both managed services; return False when cleanup could not complete.

    停止统一走 PowerShell 链路（close_services.ps1 → close_managed_service.ps1）；
    bash 版本的 close_*.sh 已不再需要（Windows 主环境）。
    """
    script = _SERVICE_DIR / _SUPPORT_SERVICE_STOP_SCRIPT
    if not script.is_file():
        print(f"[service] cleanup script not found: {script}", file=sys.stderr, flush=True)
        return False

    print("[service] stopping OCR and ICP ...", flush=True)
    try:
        powershell = _powershell_executable()
        completed = subprocess.run(
            [
                powershell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                str(script),
            ],
            cwd=_SERVICE_DIR,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, RuntimeError) as exc:
        print(f"[service] cleanup could not be launched: {exc}", file=sys.stderr, flush=True)
        return False
    if completed.returncode != 0:
        print(f"[service] cleanup failed (exit {completed.returncode})", file=sys.stderr, flush=True)
        return False
    return True


@dataclass(slots=True, frozen=True)
class PlatformSpec:
    """三平台的差异全部收敛到这张表，命令/字段/payload 构造都从它派生。"""

    name: str
    search_script: str
    review_script: str                     # 候选复核只抓主页首批，不滚动、不分叉详情
    home_script: str | None                # home 阶段脚本；skip_home=True 的平台为 None
    list_script: str | None                # douyin: page；xhs/ks 全量改单趟滚动，无独立翻页脚本
    detail_script: str | None              # 仅 xhs 有详情子任务
    account_id_field: str                  # sec_uid / user_id / kwai_id
    list_stage: str                        # "page" | "scroll"
    item_id_fields: tuple[str, ...]
    id_fallback_fields: tuple[str, ...]    # 优选账号 ID 回填的备选字段
    id_from_user_prefix: str | None        # douyin: 从 user_id 前缀恢复 sec_uid
    needs_token: bool                      # xhs 需要 xsec_token
    extra_cursor_fields: tuple[str, ...]   # 翻页 cursor 的额外来源（douyin: max_cursor）
    skip_home: bool                        # douyin 无独立 home 阶段，ACCOUNT_RANK 直接排首页翻页任务
    home_skips_if_seen: bool               # home 成功后是否历史跳过；xhs=False 以支持增量重爬


PLATFORM_SPECS: dict[str, PlatformSpec] = {
    "douyin": PlatformSpec(
        "douyin", "douyin_search.py", "douyin_posts.py", None, "douyin_posts.py",
        None, "sec_uid", "page", ("aweme_id", "video_id", "item_id"),
        (), "MS4", False, ("max_cursor",), True, True),
    "xhs": PlatformSpec(
        "xhs", "xhs_search.py", "xhs_review.py", "xhs_posts.py", None,
        "xhs_detail.py", "user_id", "scroll", ("note_id", "item_id"),
        ("account_id",), None, True, (), False, False),
    "kuaishou": PlatformSpec(
        "kuaishou", "kuaishou_search.py", "kuaishou_review.py", "kuaishou_posts.py", None,
        None, "kwai_id", "page", ("photo_id", "video_id", "item_id"),
        ("user_id", "account_id"), None, False, (), False, False),
}


def _id_flag(spec: PlatformSpec) -> str:
    return f"--{spec.account_id_field.replace('_', '-')}"


@functools.lru_cache(maxsize=None)
def company_id(name: str) -> str:
    # 公司名压成短 ID，供数据库主键和文件路径使用。
    return hashlib.sha256(name.strip().encode("utf-8")).hexdigest()[:16]


def read_companies(path: Path) -> list[str]:
    # 读取企业输入：支持 UTF-8 BOM、去空行、去重但保留原始顺序。
    raw = path.read_text(encoding="utf-8-sig")
    result: list[str] = []
    seen: set[str] = set()
    for line in raw.splitlines():
        name = line.strip()
        if name and name not in seen:
            result.append(name)
            seen.add(name)
    if not result:
        raise ValueError(f"企业输入为空: {path}")
    return result


def load_social_account_seeds(
    path: Path = DEFAULT_SOCIAL_ACCOUNT_SEEDS_FILE,
) -> dict[str, dict[str, list[dict[str, str]]]]:
    """Load user-confirmed account names/IDs used as mandatory search seeds."""
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    companies = payload.get("companies") if isinstance(payload, dict) else None
    if not isinstance(companies, dict):
        raise ValueError(f"社媒账号种子文件格式错误: {path}")
    normalized: dict[str, dict[str, list[dict[str, str]]]] = {}
    for company, platform_map in companies.items():
        if not isinstance(company, str) or not isinstance(platform_map, dict):
            continue
        normalized[company] = {}
        for platform, accounts in platform_map.items():
            if platform not in PLATFORMS or not isinstance(accounts, list):
                continue
            normalized[company][platform] = [
                {
                    "account_name": str(item.get("account_name") or "").strip(),
                    "account_id": str(item.get("account_id") or "").strip(),
                }
                for item in accounts
                if isinstance(item, dict)
                and (
                    str(item.get("account_name") or "").strip()
                    or str(item.get("account_id") or "").strip()
                )
            ]
    return normalized


def select_replay_companies(source_companies: list[tuple[str, str]], selectors: list[str]) -> list[tuple[str, str]]:
    """Select historical companies by exact name or an unambiguous name fragment."""
    selected_ids: set[str] = set()
    unmatched: list[str] = []
    ambiguous: dict[str, list[str]] = {}
    for selector in selectors:
        matches = [(cid, name) for cid, name in source_companies if selector == name or selector in name]
        if not matches:
            unmatched.append(selector)
        elif len(matches) > 1:
            ambiguous[selector] = [name for _, name in matches]
        else:
            selected_ids.add(matches[0][0])
    if unmatched or ambiguous:
        details: list[str] = []
        if unmatched:
            details.append(f"未在历史批次中找到: {', '.join(unmatched)}")
        if ambiguous:
            details.extend(
                f"匹配到多家企业: {selector} -> {', '.join(names)}"
                for selector, names in ambiguous.items()
            )
        raise ValueError("重放企业名称无效；" + "；".join(details))
    selected = [(cid, name) for cid, name in source_companies if cid in selected_ids]
    if not selected:
        raise ValueError("重放企业名称与历史记录没有交集")
    return selected


_JSON_CACHE: dict[Path, tuple[int, Any]] = {}


def _cached_json_read(path: Path) -> Any:
    # 带 mtime 失效的 JSON 读取缓存：同文件 mtime 不变则复用解析结果。
    # 子进程重写文件会改 mtime，下次读取自动失效，不读到旧数据。
    resolved = path.resolve()
    mtime = resolved.stat().st_mtime_ns
    cached = _JSON_CACHE.get(resolved)
    if cached and cached[0] == mtime:
        return cached[1]
    value = json.loads(resolved.read_text(encoding="utf-8-sig"))
    _JSON_CACHE[resolved] = (mtime, value)
    return value


def json_load(path: Path) -> dict[str, Any]:
    # 容错版 JSON 读取：失败返回空 dict。带 mtime memo；返回 deepcopy 防调用方修改污染缓存。
    try:
        value = _cached_json_read(path)
    except (OSError, json.JSONDecodeError):
        return {}
    return copy.deepcopy(value) if isinstance(value, dict) else {}


def json_load_any(path: Path) -> Any:
    # 比 json_load 更宽松，允许返回 list / dict / 其他 JSON 值。同样带 memo + deepcopy。
    try:
        value = _cached_json_read(path)
    except (OSError, json.JSONDecodeError):
        return None
    return copy.deepcopy(value) if isinstance(value, (dict, list)) else value


def nested_records(value: Any) -> list[dict[str, Any]]:
    # 兼容上游 {records: [...]} 结构。
    if isinstance(value, dict):
        records = value.get("records")
        if isinstance(records, list):
            return [x for x in records if isinstance(x, dict)]
    return []


def record_url(record: dict[str, Any]) -> str | None:
    # 从不同字段名里找出一个可访问 URL，这是进入 WEB_CRAWL 的入口。
    for key in ("url", "website", "webSite", "serviceUrl", "service_url", "domain"):
        value = str(record.get(key) or "").strip()
        if value:
            if not re.match(r"^https?://", value, re.I):
                value = "https://" + value
            if urlsplit(value).hostname:
                return value
    return None


def icp_record_label(record: dict[str, Any]) -> str:
    """Pick a human-facing carrier name across ICP website/app variants."""
    for key in (
        "domain", "serviceName", "service_name", "appName", "app_name",
        "miniAppName", "mini_app_name", "quickAppName", "quick_app_name",
        "websiteName", "website_name", "name", "unitName",
    ):
        value = str(record.get(key) or "").strip()
        if value:
            return value
    return "未命名载体"


@dataclass(frozen=True)
class DedupScope:
    """统一去重配置：make_key 从一条记录/请求提取去重 key；load_history 可选，给出 DB 历史 item 来源。
    seen 的生命周期由调用方决定（持久 self._seen_registry 或临时 set），scope 只负责 key 与历史来源。"""
    make_key: Callable[[Any], str | None]
    load_history: Callable[["PipelineStore", str], Iterable[Any]] | None = None


def _account_dedup_key(account: dict[str, Any]) -> str | None:
    for field in ("sec_uid", "user_id", "kwai_id", "nickname"):
        value = str(account.get(field) or "").strip()
        if value:
            return f"account:{value}"
    return None


def _keyword_dedup_key(rec: dict[str, Any]) -> str | None:
    platform = str(rec.get("platform") or "")
    normalized = str(rec.get("normalized") or "")
    return f"keyword:{platform}:{normalized}" if platform and normalized else None


# url/account/keyword 共用这套配置；post 因 _post_item_key 依赖 spec 且有平台差异/existing_ids 下推，自管。
DEDUP_SCOPES: dict[str, DedupScope] = {
    "url": DedupScope(record_url, None),
    "account": DedupScope(_account_dedup_key),
    "keyword": DedupScope(_keyword_dedup_key),
}


def _pick_text(value: Any) -> str:
    # 递归取文本，把字典/列表里最像标题/名称/内容的字段挖出来。
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "title", "name", "keyword", "domain", "url", "nickname", "account_name", "account", "content"):
            text = _pick_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        for item in value:
            text = _pick_text(item)
            if text:
                return text
        return ""
    if value is None:
        return ""
    return str(value).strip()


def _pick_name(item: dict[str, Any]) -> str:
    # 账号展示名的统一提取顺序，日志摘要共用。
    for key in ("nickname", "username", "name", "account_name", "title"):
        text = _pick_text(item.get(key))
        if text:
            return text
    return ""


def _brief_items(items: list[str], limit: int = 3) -> str:
    # 把长列表压成便于日志阅读的短字符串。
    picked = [item for item in items if item]
    if not picked:
        return ""
    if len(picked) > limit:
        return ", ".join(picked[:limit]) + f" ...(+{len(picked) - limit})"
    return ", ".join(picked)


_IMAGE_MAGIC_PREFIXES: tuple[tuple[bytes, str], ...] = (
    (b"\xff\xd8\xff", "jpg"),
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"GIF87a", "gif"),
    (b"GIF89a", "gif"),
    (b"RIFF", "webp"),
    (b"BM", "bmp"),
)


def _looks_like_image(path: Path) -> bool:
    """Magic bytes 快检：文件看起来是不是真实图片。坏/HTML/JSON 错误页返回 False。"""
    try:
        head = path.read_bytes()[:12]
    except OSError:
        return False
    return any(head.startswith(magic) for magic, _ in _IMAGE_MAGIC_PREFIXES)


def _iter_items(result: Any) -> list[dict[str, Any]]:
    # 从分页结果里取出作品/账号列表。现版脚本只写 items / aweme_list。
    if not isinstance(result, dict):
        return []
    for key in ("items", "records", "works", "videos", "notes", "aweme_list"):
        value = result.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _review_post_text(item: dict[str, Any]) -> str:
    for key in ("title", "desc", "caption", "content", "text", "display_title"):
        value = _pick_text(item.get(key))
        if value:
            return re.sub(r"\s+", " ", value)[:240]
    return ""


def _compact_account_review(summary: dict[str, Any], limit: int = ACCOUNT_REVIEW_SAMPLE_SIZE) -> dict[str, Any]:
    """Keep only ownership/profile signals and a bounded recent-title sample for account verification."""
    items = _iter_items(summary)
    compact_posts: list[dict[str, Any]] = []
    for item in items[:limit]:
        author = item.get("author") if isinstance(item.get("author"), dict) else {}
        compact_posts.append({
            "post_id": next((item.get(key) for key in ("aweme_id", "note_id", "photo_id", "video_id", "item_id") if item.get(key)), None),
            "title": _review_post_text(item),
            "author": {
                key: author.get(key)
                for key in ("nickname", "unique_id", "account_cert_info", "enterprise_verify_reason", "custom_verify")
                if author.get(key) not in {None, ""}
            },
        })
    first_author = next((item.get("author") for item in items if isinstance(item.get("author"), dict)), {})
    profile = summary.get("profile") if isinstance(summary.get("profile"), dict) else {}
    return {
        "status": summary.get("status"),
        "profile": profile,
        "author": {
            key: first_author.get(key)
            for key in ("nickname", "unique_id", "account_cert_info", "enterprise_verify_reason", "custom_verify")
            if first_author.get(key) not in {None, ""}
        },
        "posts": compact_posts,
        "available_post_count": len(items),
        "sample_limit": limit,
    }


@dataclass(slots=True)
class Request:
    # 控制层里的最小可调度单元：跑什么阶段、针对哪个平台、带什么参数、
    # 用什么命令、是否需要资源租约。
    phase: str
    platform: str | None
    payload: dict[str, Any]
    command: list[str] | None
    resource_type: str | None
    resource_key: str | None
    empty_reason: str | None = None
    task_id: str = field(default_factory=lambda: uuid4().hex)
    deadline: float = 0.0  # 仅社媒 LOGIN_REFRESH 等待的失败期限；0 表示未进入该等待状态。
    history_checked: bool = field(default=False, repr=False)
    history_source: dict[str, Any] | None = field(default=None, repr=False)


@dataclass(slots=True)
class EnterpriseSubAgent:
    """每家公司一个持久化逻辑子代理，即公司级剧情状态机。

    它不直接执行外部进程，也不直接改数据库；只维护当前走到哪一幕、
    下一幕应该排什么请求。
    """

    name: str
    run_id: str
    root: Path
    python: str
    db: PipelineStore
    web_max_depth: int | None = DEFAULT_MAX_DEPTH
    web_max_pages: int | None = DEFAULT_MAX_PAGES
    full_crawl: bool = False
    requests: deque[Request] = field(default_factory=deque)
    completed: list[dict[str, Any]] = field(default_factory=list)
    defects: list[dict[str, Any]] = field(default_factory=list)
    # 本流程每条 attempt 的终态分布；整波完成后供分类器读取。key=phase，value=按完成顺序的 status 列表。
    phase_status: dict[str, list[str]] = field(default_factory=dict)
    # 统一去重的持久 seen 缓存：key=namespace（url/account/keyword/...），value=key 集合。临时去重由调用方传 set()。
    _seen_registry: dict[str, set[str]] = field(default_factory=dict, repr=False, compare=False)
    state: str = "OPEN"
    round_no: int = 1
    round2_started: bool = False
    export_requested: bool = False
    inflight: int = 0
    _seen_post_accounts: set[str] | None = field(default=None, repr=False, compare=False)
    _seen_post_items: set[str] | None = field(default=None, repr=False, compare=False)
    _seen_posts_by_account: dict[str, set[str]] | None = field(default=None, repr=False, compare=False)
    # ACCOUNT_REVIEW 样本的帖子 id（按账号聚合），仅用于 home 任务 --skip-ids 去重。
    # 与 POST_CRAWL 的 seen 库存物理隔离：绝不进入任何"到底/锚点"终止判定。
    _seen_review_items: dict[str, set[str]] | None = field(default=None, repr=False, compare=False)
    # on_finished 正在处理的请求 payload；seen 首次构建时跳过它，使当前页不计入库存（早停/去重/existing_ids 均适用）。
    _processing_payload: dict[str, Any] | None = field(default=None, repr=False, compare=False)
    # 三平台按账号汇总出的最新发帖时间与 30 天活跃度；会进入企业导出。
    social_account_activity: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 账号样本复核后的分层结论；包含通过与拒绝项，供正式抓取、风险分析和导出复用。
    account_verifications: list[dict[str, Any]] = field(default_factory=list)
    risk_evidence_screenshots: list[dict[str, Any]] = field(default_factory=list)
    _reported_social_activity: set[str] = field(default_factory=set, repr=False, compare=False)
    domain_activity: dict[str, dict[str, Any]] = field(default_factory=dict)
    web_seen_urls: set[str] = field(default_factory=set, repr=False, compare=False)
    web_pages_scheduled: int = 0
    web_crawl_stats: Counter = field(default_factory=Counter, repr=False, compare=False)
    log: Any | None = field(default=None, repr=False, compare=False)
    _artifact_dir: Path | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        # artifact_dir 在 run 内不变，预算一次复用，避免每次拼路径 + safe_name。
        self._artifact_dir = self.root / "companies" / safe_name(self.name)

    def artifact_dir(self) -> Path:
        return self._artifact_dir

    def queue_initial(self) -> None:
        for carrier in ICP_CARRIERS:
            self.requests.append(Request(
                "WEB_ICP", None, {"carrier": carrier, "page": 1},
                [self.python, _script_path("icp_query_one.py"), self.name, "--carrier", carrier, "--page", "1"],
                "ICP", "global",
            ))

    def _files_for(self, phase: str) -> list[Path]:
        """Return result files from successful canonical attempts only."""
        files: list[Path] = []
        for attempt_dir in self._successful_attempt_dirs(phase):
            result_path = attempt_dir / "result.json"
            if result_path.is_file():
                files.append(result_path)
        return files

    def _successful_attempt_dirs(self, phase: str) -> list[Path]:
        # 成功判据以 DB 为权威（task_attempts.status），final.json 降级为调试产物；不再 glob 它。
        phase_dir = self.artifact_dir() / phase
        attempts: list[Path] = []
        for row in self.db.successful_attempts(company_id(self.name), phase):
            path = phase_dir / f"task_{row['task_id']}" / f"attempt_{row['attempt_no']}"
            if path.is_dir():
                attempts.append(path)
        return sorted(attempts, key=lambda path: path.as_posix())

    def _downloaded_images(self) -> list[Path]:
        """Return image payloads from successful download attempts only.

        Magic bytes 校验：历史批次里存在"进程返回 0 但产物损坏"的假成功
        （旧版下载器校验宽松，坏文件以 .bin 落盘，PIL 打不开 → OCR 必失败）。
        坏文件不送入 OCR，避免浪费 4 次重试与冷却。
        """
        supported_suffixes = {".bin", ".png", ".jpg", ".jpeg", ".webp", ".bmp"}
        images: list[Path] = []
        rejected = 0
        for attempt_dir in self._successful_attempt_dirs("IMAGE_DOWNLOAD"):
            for path in attempt_dir.rglob("*"):
                if not path.is_file() or path.suffix.lower() not in supported_suffixes:
                    continue
                if _looks_like_image(path):
                    images.append(path)
                else:
                    rejected += 1
        if rejected and callable(self.log):
            self.log("IMAGE_REJECTED_BAD_CONTENT", None, count=rejected)
        return sorted(images, key=lambda path: path.as_posix())

    def _content_files_for(self, phase: str) -> list[Path]:
        """Return canonical JSON/TXT payload files for corpus construction."""
        ignored = {"final.json", "stdout.log", "stderr.raw.log", "raw_result.json",
                   "keyword_input.json", "platform_accounts.json"}
        files: list[Path] = []
        for attempt_dir in self._successful_attempt_dirs(phase):
            for path in attempt_dir.rglob("*"):
                if path.is_file() and path.name not in ignored and path.suffix.lower() in {".json", ".txt"}:
                    files.append(path)
        return sorted(files, key=lambda path: path.as_posix())

    def _icp_roots(self) -> list[str]:
        # ICP 四类载体都属于 WEB_ICP 阶段，但只有 web 备案记录提供官网 URL。
        # 缺少 carrier_type 的旧产物按 web 兼容处理；新产物会显式携带 carrier_type，
        # 因此 app / mapp / kapp 不会误触发官网抓取。
        records = [
            r
            for path in self._files_for("WEB_ICP")
            for r in nested_records(json_load(path))
            if str(r.get("carrier_type") or "web").lower() == "web"
        ]
        return [u for u in (record_url(r) for r in self._dedup_filter("url", records, set())) if u][:60]

    def _website_search_candidates(self) -> list[dict[str, Any]]:
        """Collect selected search candidates without treating them as confirmed official sites."""
        by_root: dict[str, dict[str, Any]] = {}
        for path in self._files_for("WEB_SEARCH"):
            payload = json_load(path)
            candidates = payload.get("candidates")
            if not isinstance(candidates, list):
                continue
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                root = canonicalize_url(item.get("url"))
                if not root:
                    continue
                candidate = dict(item)
                candidate["url"] = root
                candidate["official_status"] = "unverified_candidate"
                existing = by_root.get(root)
                if existing is None or int(candidate.get("score") or 0) > int(existing.get("score") or 0):
                    by_root[root] = candidate
        return sorted(by_root.values(), key=lambda item: (-int(item.get("score") or 0), str(item.get("url") or "")))

    def _website_search_carrier_candidates(self) -> list[dict[str, Any]]:
        """Collect record-only carrier clues; never promote them to crawl roots."""
        by_candidate: dict[tuple[str, str], dict[str, Any]] = {}
        for path in self._files_for("WEB_SEARCH"):
            payload = json_load(path)
            candidates = payload.get("carrier_candidates")
            if not isinstance(candidates, list):
                continue
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                carrier_type = str(item.get("carrier_type") or "").strip()
                url = canonicalize_url(item.get("url"))
                if not carrier_type or not url:
                    continue
                candidate = dict(item)
                candidate["url"] = url
                candidate["official_status"] = "unverified_candidate"
                candidate["record_only"] = True
                key = (carrier_type, url)
                existing = by_candidate.get(key)
                if existing is None or int(candidate.get("score") or 0) > int(existing.get("score") or 0):
                    by_candidate[key] = candidate
        return sorted(
            by_candidate.values(),
            key=lambda item: (
                str(item.get("carrier_type") or ""),
                -int(item.get("score") or 0),
                str(item.get("url") or ""),
            ),
        )

    def _website_search_metadata(self) -> dict[str, Any]:
        """Expose provider/fallback audit fields from the latest successful search."""
        keys = (
            "provider", "providers_used", "fallback_count", "queries",
            "query_specs", "query_attempts", "errors", "searched_at",
        )
        for path in reversed(self._files_for("WEB_SEARCH")):
            payload = json_load(path)
            if isinstance(payload, dict):
                return {key: copy.deepcopy(payload.get(key)) for key in keys if key in payload}
        return {}

    def _roots(self) -> list[str]:
        """Merge strong ICP roots with search-discovered candidates, preserving provenance order."""
        icp_roots = self._icp_roots()
        search_roots = [str(item.get("url") or "") for item in self._website_search_candidates()]
        roots: list[str] = []
        seen_sites: set[str] = set()
        for root in [*icp_roots, *search_roots]:
            normalized = canonicalize_url(root)
            site_key = site_host(normalized) if normalized else None
            if normalized and site_key and site_key not in seen_sites:
                seen_sites.add(site_key)
                roots.append(normalized)
        # ICP roots come from bare registered domains.  The apex and its www
        # host can serve different sites (or only one answers), so crawl both
        # forms explicitly instead of trusting the www-alias site dedupe.
        queued_urls = set(roots)
        for root in icp_roots:
            normalized = canonicalize_url(root)
            variant = host_variant_url(normalized) if normalized else None
            if variant and variant not in queued_urls:
                queued_urls.add(variant)
                roots.append(variant)
        icp_sites = {site_host(root) for root in icp_roots if site_host(root)}
        self.web_crawl_stats["roots_from_icp"] = len(icp_sites)
        self.web_crawl_stats["roots_from_search"] = len([root for root in roots if site_host(root) not in icp_sites])
        return roots[:60]

    def _queue_web_search(self) -> None:
        self.requests.append(Request(
            "WEB_SEARCH", None, {
                "company": self.name,
                "provider": "bing_html_cn_with_rss_fallback",
                "search_schema_version": 3,
            },
            [self.python, _script_path("web_search_official.py"), self.name],
            "HOSTNAME", "cn.bing.com",
        ))

    def _queue_web_page(
        self,
        url: object,
        *,
        depth: int,
        crawl_root: object,
        parent_url: object | None = None,
    ) -> bool:
        """Queue one canonical page once, within the company-wide crawl bounds."""
        normalized = canonicalize_url(url)
        scope = canonicalize_url(crawl_root)
        parent = canonicalize_url(parent_url) if parent_url else None
        if (
            not normalized
            or not scope
            or depth < 0
            or (self.web_max_depth is not None and depth > self.web_max_depth)
            or not is_recursive_page_url(normalized)
            or not same_site(normalized, scope)
        ):
            self.web_crawl_stats["filtered_pages"] += 1
            return False
        if normalized in self.web_seen_urls:
            self.web_crawl_stats["duplicate_pages"] += 1
            return False
        if self.web_max_pages is not None and self.web_pages_scheduled >= self.web_max_pages:
            self.web_crawl_stats["page_limit_drops"] += 1
            return False
        self.web_seen_urls.add(normalized)
        self.web_pages_scheduled += 1
        self.web_crawl_stats["pages_scheduled"] += 1
        fetch_url = normalized
        raw_url = str(url or "").strip()
        raw_parts = urlsplit(raw_url)
        if raw_parts.scheme.lower() in {"http", "https"} and not raw_parts.path and not raw_parts.query:
            fetch_url = normalized.rstrip("/")
        host = (urlsplit(fetch_url).hostname or "unknown").lower()
        self.requests.append(Request(
            "WEB_CRAWL", None,
            {
                "url": fetch_url,
                "crawl_root": scope,
                "depth": depth,
                "parent_url": parent,
            },
            [self.python, _script_path("screenshot_crawler.py"), fetch_url],
            "HOSTNAME", host,
        ))
        return True

    def _fanout_web_page(self, request: Request, summary: dict[str, Any]) -> None:
        """Schedule same-site child pages immediately after one page completes."""
        if request.phase != "WEB_CRAWL":
            return
        try:
            depth = int(request.payload.get("depth") or 0)
        except (TypeError, ValueError):
            depth = 0
        requested = canonicalize_url(request.payload.get("url"))
        fetched = canonicalize_url(
            summary.get("fetched_url") or summary.get("url") or requested,
            requested,
        )
        scope = canonicalize_url(request.payload.get("crawl_root") or requested)
        if requested and requested not in self.web_seen_urls:
            self.web_seen_urls.add(requested)
            self.web_pages_scheduled += 1
        # A registered root commonly redirects to its canonical public hostname.
        # Treat that final hostname as the recursion scope only for depth zero;
        # deeper cross-site redirects are fetched but do not expand the crawl.
        if depth == 0 and fetched and scope and not same_site(fetched, scope):
            scope = fetched
        if fetched:
            self.web_seen_urls.add(fetched)
        raw_links = summary.get("links")
        if not isinstance(raw_links, (list, tuple)) or not fetched or not scope:
            return
        links = dedupe_links(raw_links, fetched)
        self.web_crawl_stats["links_discovered"] += len(links)
        queued = 0
        depth_dropped = 0
        offsite_dropped = 0
        asset_dropped = 0
        duplicate_dropped = 0
        page_limit_dropped = 0
        for link in links:
            if self.web_max_depth is not None and depth >= self.web_max_depth:
                self.web_crawl_stats["depth_limit_drops"] += 1
                depth_dropped += 1
                continue
            if not same_site(link, scope):
                self.web_crawl_stats["offsite_or_asset_links"] += 1
                offsite_dropped += 1
                continue
            if not is_recursive_page_url(link):
                self.web_crawl_stats["offsite_or_asset_links"] += 1
                asset_dropped += 1
                continue
            if link in self.web_seen_urls:
                self.web_crawl_stats["duplicate_pages"] += 1
                duplicate_dropped += 1
                continue
            if self.web_max_pages is not None and self.web_pages_scheduled >= self.web_max_pages:
                self.web_crawl_stats["page_limit_drops"] += 1
                page_limit_dropped += 1
                continue
            if self._queue_web_page(
                link, depth=depth + 1, crawl_root=scope, parent_url=fetched,
            ):
                queued += 1
        if callable(self.log):
            self.log(
                "WEB_LINK_FANOUT", self, request,
                url=fetched, depth=depth, discovered=len(links), queued=queued,
                seen=len(self.web_seen_urls), page_limit=self.web_max_pages,
                duplicate_dropped=duplicate_dropped,
                offsite_dropped=offsite_dropped,
                asset_dropped=asset_dropped,
                depth_dropped=depth_dropped,
                page_limit_dropped=page_limit_dropped,
            )

    def _corpus_path(self) -> Path:
        """Build account-keyword corpus from all historical platform-hit正文."""
        return self._platform_keyword_corpus_path("corpus.txt")

    def _platform_keyword_corpus_path(self, filename: str) -> Path:
        records, _ = self._historical_keyword_records()
        path = self.artifact_dir() / "KEYWORD" / "_state" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        seen: set[str] = set()
        # 企业名和历史别名会作为独立 prompt 字段；正文保留原始内容。
        # 已发现账号继续作为 AI 判断线索，但必须带“不等于企业载体”标签，
        # 防止投诉者/普通个人账号被模型直接当成品牌或官方账号。
        # 不能只认抖音/小红书/快手：例如“小程序：煌泰珠宝”仍须进入 LLM，
        # 由模型判断“煌泰珠宝”是否是可独立检索的载体名称。
        lines: list[str] = []
        for record in records:
            if not record.get("platforms") and not record.get("discovery_cues"):
                continue
            account_name = str(record.get("account_name") or "").strip()
            if account_name and account_name.lower() != "unknown":
                clue_line = f"【已发现账号线索｜仅供判断，不等于企业载体】{account_name}"
                if clue_line not in seen:
                    seen.add(clue_line)
                    lines.append(clue_line)
            text = str(record.get("text") or "").strip()
            if text and text not in seen:
                seen.add(text)
                lines.append(text)
        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def _historical_keyword_records(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Rebuild keyword records from every successful historical web/post attempt."""
        # ACCOUNT_REVIEW 是未验证采样，盲目回流关键词扩展会造成循环别名
        # (candidate -> alias -> protected candidate)；但小账号的全部帖子可能只在
        # 复核阶段被收集（POST_CRAWL 因 skip-ids 合法返回 0）。因此复核内容仅当
        # 其账号已被 POST_CRAWL 验证（_review_matches_post_scope）时才入库，
        # 堵住"复核抓到帖、证据零覆盖"的漏洞，同时不放开未验证样本。
        phases = ("WEB_CRAWL", "OCR", "POST_CRAWL", "ACCOUNT_REVIEW")
        rows = self.db.historical_content_attempts(company_id(self.name), phases)
        post_scopes = self._post_content_scopes(rows)
        seen: dict[tuple[str, str, str, str, tuple[str, ...]], dict[str, Any]] = {}
        coverage: dict[str, Any] = {
            "requested_phases": ["WEB_CRAWL", "OCR", "POST_CRAWL", "ACCOUNT_REVIEW"],
            "included_review_fallback": False,
            "attempts_seen": 0,
            "attempts_with_artifacts": 0,
            "texts_seen": 0,
            "keyword_hit_texts": 0,
            "platform_keyword_hit_texts": 0,
            "discovery_cue_hit_texts": 0,
            "source_runs": [],
            "missing_artifacts": [],
        }
        source_by_phase = {
            "WEB_CRAWL": "web", "OCR": "ocr",
            "POST_CRAWL": "post", "ACCOUNT_REVIEW": "post",
        }
        for row in rows:
            phase = str(row["phase"] or "")
            platform = str(row["platform"] or "")
            try:
                row_payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                row_payload = {}
            if not isinstance(row_payload, dict):
                row_payload = {}
            candidate_payload = row_payload.get("candidate") if isinstance(row_payload.get("candidate"), dict) else {}
            account_name = str(
                row_payload.get("account_name")
                or candidate_payload.get("account_name")
                or candidate_payload.get("nickname")
                or candidate_payload.get("username")
                or ""
            ).strip()
            if phase == "ACCOUNT_REVIEW" and not self._review_matches_post_scope(row, post_scopes):
                continue
            coverage["attempts_seen"] += 1
            run_id = str(row["run_id"] or "")
            if run_id and run_id not in coverage["source_runs"]:
                coverage["source_runs"].append(run_id)
            attempt_dir = self._historical_attempt_dir(row)
            if not attempt_dir.is_dir():
                coverage["missing_artifacts"].append({
                    "run_id": run_id, "task_id": str(row["task_id"]),
                    "phase": phase, "attempt_no": int(row["attempt_no"]),
                    "attempt_dir": str(attempt_dir),
                })
                continue
            coverage["attempts_with_artifacts"] += 1
            source_run = str(row["source_run_id"] or row["run_id"] or "")
            for file in self._content_files_from_attempt(attempt_dir):
                artifact_path = self._historical_artifact_path(file)
                texts = self._content_texts_from_file(
                    file, phase=phase, platform=platform,
                    artifact_path=artifact_path,
                )
                if not texts:
                    continue
                records = keyword_records(texts, source_by_phase[phase])
                for record in records:
                    text_value = str(record.get("text") or "").strip()
                    if not text_value:
                        continue
                    keywords = [str(x) for x in record.get("keywords", []) if str(x).strip()]
                    platforms = [str(x) for x in record.get("platforms", []) if str(x).strip()]
                    discovery_cues = [str(x) for x in record.get("discovery_cues", []) if str(x).strip()]
                    coverage["texts_seen"] += 1
                    if keywords:
                        coverage["keyword_hit_texts"] += 1
                    if platforms:
                        coverage["platform_keyword_hit_texts"] += 1
                    if discovery_cues:
                        coverage["discovery_cue_hit_texts"] += 1
                    if not keywords and not platforms and not discovery_cues:
                        continue
                    url = str(record.get("source_url") or "").strip()
                    key = (phase, str(record.get("platform") or platform), url, text_value, tuple(keywords))
                    existing = seen.get(key)
                    if existing is not None:
                        for field, value in (("source_run_ids", source_run), ("artifact_paths", artifact_path)):
                            if value and value not in existing[field]:
                                existing[field].append(value)
                        continue
                    seen[key] = {
                        "evidence_id": f"E{len(seen) + 1:04d}",
                        "url": url or None,
                        "text": text_value,
                        "matched_keywords": keywords,
                        "platforms": platforms,
                        "discovery_cues": discovery_cues,
                        "source": record.get("source"),
                        "source_phase": phase,
                        "source_run_ids": [source_run] if source_run else [],
                        "platform": record.get("platform") or platform or None,
                        "account_name": account_name or None,
                        "artifact_path": artifact_path,
                        "artifact_paths": [artifact_path],
                    }
        return list(seen.values()), coverage

    def _write_keyword_records(self, request: Request, result: dict[str, Any]) -> dict[str, Any]:
        """Index one attempt's text and return a terminal-friendly hit distribution."""
        distribution: dict[str, Any] = {
            "texts": 0,
            "hit_texts": 0,
            "discovery_hit_texts": 0,
            "keywords": Counter(),
            "platforms": Counter(),
            "discovery_cues": Counter(),
        }
        if request.phase not in {"WEB_CRAWL", "OCR", "POST_CRAWL"}:
            return distribution
        attempt_dir_value = str(result.get("attempt_dir") or "").strip()
        if not attempt_dir_value:
            return distribution
        attempt_dir = Path(attempt_dir_value)
        if not attempt_dir.is_dir():
            return distribution
        source = {"OCR": "ocr", "WEB_CRAWL": "web", "POST_CRAWL": "post"}[request.phase]
        ignored = {"final.json", "raw_result.json", "keyword_input.json", "platform_accounts.json"}
        for file in sorted(attempt_dir.rglob("*"), key=lambda p: p.as_posix()):
            if not file.is_file() or file.name.endswith(".keywords.json") or file.name in ignored:
                continue
            texts: list[dict[str, Any]] = []
            try:
                artifact_path = str(file.relative_to(self.root)).replace("\\", "/")
            except ValueError:
                # Unit tests and legacy callers may supply a result path outside
                # this run directory; preserve it rather than failing the crawl.
                artifact_path = str(file)

            def add_text(value: Any, *, source_url: str = "", title: str = "") -> None:
                text = str(value or "").strip()
                if text:
                    texts.append({"text": text, "source_url": source_url, "artifact_path": artifact_path,
                                  "platform": request.platform or "web", "title": title})

            def post_url(item: dict[str, Any], fallback: str) -> str:
                explicit = str(item.get("canonical_url") or item.get("share_url") or item.get("detail_href") or item.get("href") or fallback)
                if explicit:
                    return explicit
                if request.platform == "douyin":
                    item_id = str(item.get("aweme_id") or item.get("video_id") or item.get("item_id") or "")
                    return f"https://www.douyin.com/video/{item_id}" if item_id else ""
                if request.platform == "xhs":
                    item_id = str(item.get("note_id") or item.get("item_id") or "")
                    return f"https://www.xiaohongshu.com/explore/{item_id}" if item_id else ""
                if request.platform == "kuaishou":
                    item_id = str(item.get("photo_id") or item.get("video_id") or item.get("item_id") or "")
                    return f"https://www.kuaishou.com/short-video/{item_id}" if item_id else ""
                return ""

            if file.suffix.lower() == ".txt":
                add_text(file.read_text(encoding="utf-8", errors="replace"))
            elif file.suffix.lower() == ".json":
                payload = json_load(file)
                if isinstance(payload, dict):
                    source_url = str(payload.get("canonical_url") or payload.get("fetched_url") or payload.get("url") or payload.get("href") or "")
                    title = str(payload.get("title") or "")
                    for key in ("text", "body", "title", "desc", "caption"):
                        add_text(payload.get(key), source_url=source_url, title=title)
                    for value in payload.get("records", []):
                        if isinstance(value, str):
                            add_text(value, source_url=source_url, title=title)
                    for item in _iter_items(payload):
                        item_url = post_url(item, source_url)
                        item_title = str(item.get("title") or title)
                        for key in ("text", "body", "title", "desc", "caption"):
                            add_text(item.get(key), source_url=item_url, title=item_title)
            if texts:
                records = write_keyword_records(texts, source, file.with_name(f"{file.stem}.keywords.json"))
                for record in records:
                    distribution["texts"] += 1
                    keywords = [str(item) for item in record.get("keywords", []) if str(item).strip()]
                    platforms = [str(item) for item in record.get("platforms", []) if str(item).strip()]
                    discovery_cues = [str(item) for item in record.get("discovery_cues", []) if str(item).strip()]
                    if keywords:
                        distribution["hit_texts"] += 1
                    if discovery_cues:
                        distribution["discovery_hit_texts"] += 1
                    distribution["keywords"].update(keywords)
                    distribution["platforms"].update(platforms)
                    distribution["discovery_cues"].update(discovery_cues)
        distribution["keywords"] = dict(distribution["keywords"].most_common())
        distribution["platforms"] = dict(distribution["platforms"].most_common())
        distribution["discovery_cues"] = dict(distribution["discovery_cues"].most_common())
        return distribution

    def _risk_evidence_path(self) -> Path:
        """Collect historical keyword-hit website/post正文 for risk analysis.

        This deliberately rebuilds matches from every successful historical
        content attempt instead of relying on the current run's sidecar files.
        A current incremental crawl can return zero *new* posts while older
        successful runs still contain the relevant正文.
        """
        path = self.artifact_dir() / "RISK_ANALYSIS" / "risk_evidence.json"
        keyword_hit_evidence, coverage = self._historical_keyword_records()
        keyword_hit_evidence = [
            item for item in keyword_hit_evidence if item.get("matched_keywords")
        ]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "schema_version": 3, "company": self.name,
            "keyword_hit_evidence": keyword_hit_evidence,
            "non_keyword_evidence": [],
            "coverage": coverage,
            "network_carriers": self._network_carriers_for_analysis(),
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return path

    def _historical_attempt_dir(self, row: Any) -> Path:
        runs_root = self.root.parent
        return (
            runs_root / str(row["run_id"]) / "companies" / safe_name(self.name)
            / str(row["phase"]) / f"task_{row['task_id']}"
            / f"attempt_{row['attempt_no']}"
        )

    def _historical_artifact_path(self, file: Path) -> str:
        try:
            return str(file.relative_to(self.root.parent)).replace("\\", "/")
        except ValueError:
            return str(file)

    def _content_files_from_attempt(self, attempt_dir: Path) -> list[Path]:
        ignored = {
            "final.json", "stdout.log", "stderr.raw.log", "raw_result.json",
            "keyword_input.json", "platform_accounts.json",
        }
        return sorted(
            path for path in attempt_dir.rglob("*")
            if path.is_file()
            and path.name not in ignored
            and not path.name.endswith(".keywords.json")
            and path.suffix.lower() in {".json", ".txt"}
        )

    def _post_content_scopes(self, rows: Iterable[Any]) -> dict[str, list[tuple[str, str]]]:
        scopes: dict[str, list[tuple[str, str]]] = {platform: [] for platform in PLATFORMS}
        for row in rows:
            if str(row["phase"] or "") != "POST_CRAWL":
                continue
            platform = str(row["platform"] or "")
            if platform not in PLATFORM_SPECS:
                continue
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                payload = {}
            if not isinstance(payload, dict):
                continue
            identity = self._activity_request_identity(platform, payload)
            if identity != ("", "") and identity not in scopes[platform]:
                scopes[platform].append(identity)
        return scopes

    def _review_matches_post_scope(self, row: Any, scopes: dict[str, list[tuple[str, str]]]) -> bool:
        platform = str(row["platform"] or "")
        if platform not in PLATFORM_SPECS:
            return False
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            return False
        review_id, review_name = self._activity_request_identity(platform, payload)
        for post_id, post_name in scopes.get(platform, []):
            if review_id and post_id and review_id == post_id:
                return True
            if not review_id and not post_id and review_name and post_name and review_name == post_name:
                return True
        return False

    def _content_texts_from_file(
        self, file: Path, *, phase: str, platform: str, artifact_path: str,
    ) -> list[dict[str, Any]]:
        texts: list[dict[str, Any]] = []

        def add_text(value: Any, *, source_url: str = "", title: str = "") -> None:
            text_value = str(value or "").strip()
            if text_value:
                texts.append({
                    "text": text_value, "source_url": source_url,
                    "artifact_path": artifact_path, "platform": platform or "web",
                    "title": title,
                })

        def post_url(item: dict[str, Any], fallback: str) -> str:
            explicit = str(
                item.get("canonical_url") or item.get("share_url")
                or item.get("detail_href") or item.get("href") or fallback
            )
            if explicit:
                return explicit
            if platform == "douyin":
                item_id = str(item.get("aweme_id") or item.get("video_id") or item.get("item_id") or "")
                return f"https://www.douyin.com/video/{item_id}" if item_id else ""
            if platform == "xhs":
                item_id = str(item.get("note_id") or item.get("item_id") or "")
                return f"https://www.xiaohongshu.com/explore/{item_id}" if item_id else ""
            if platform == "kuaishou":
                item_id = str(item.get("photo_id") or item.get("video_id") or item.get("item_id") or "")
                return f"https://www.kuaishou.com/short-video/{item_id}" if item_id else ""
            return ""

        if file.suffix.lower() == ".txt":
            add_text(file.read_text(encoding="utf-8", errors="replace"))
            return texts
        payload = json_load(file)
        if not isinstance(payload, dict):
            return texts
        source_url = str(
            payload.get("canonical_url") or payload.get("fetched_url")
            or payload.get("url") or payload.get("href") or ""
        )
        title = str(payload.get("title") or "")
        for key in ("text", "body", "title", "desc", "caption"):
            add_text(payload.get(key), source_url=source_url, title=title)
        for value in payload.get("records", []):
            if isinstance(value, str):
                add_text(value, source_url=source_url, title=title)
        for item in _iter_items(payload):
            item_url = post_url(item, source_url)
            item_title = str(item.get("title") or title)
            for key in ("text", "body", "title", "desc", "caption"):
                add_text(item.get(key), source_url=item_url, title=item_title)
        return texts

    def _network_carriers_for_analysis(self) -> list[dict[str, Any]]:
        """Build a factual carrier inventory for the final enterprise assessment."""
        carriers: list[dict[str, Any]] = []
        observed_accounts: set[str] = set()
        for item in self._website_search_candidates():
            carriers.append({
                "carrier_type": "website",
                "name": item.get("domain"),
                "url": item.get("url"),
                "official_status": "unverified",
                "activity_status": "unknown",
                "source": "WEB_SEARCH",
                "reason": "; ".join(str(reason) for reason in (item.get("reasons") or [])),
            })
        for item in self.domain_activity.values():
            carriers.append({
                "carrier_type": "domain", "name": item.get("domain"), "url": item.get("url"),
                "activity_status": "active" if item.get("is_active") is True else "inactive" if item.get("is_active") is False else "unknown",
                "access_type": item.get("access_type"), "source": "WEB_CRAWL",
            })
        for item in self.social_account_activity.values():
            observed_accounts.update(self._account_identity(item))
            carriers.append({
                "carrier_type": "social_account", "name": item.get("account_name"), "platform": item.get("platform"),
                "account_id": item.get("account_id"), "official_status": item.get("official_status") or "unverified",
                "account_role": item.get("account_role"),
                "verification_confidence": item.get("verification_confidence"),
                "activity_status": "active" if item.get("is_active") is True else "inactive" if item.get("is_active") is False else "unknown",
                "latest_post_time": item.get("latest_post_time"), "source": "POST_CRAWL",
            })
        # Account discovery bios are source text. Include them only for accounts
        # that were actually selected/crawled, so broad search noise cannot become
        # business or risk evidence.
        for platform, accounts in self._accounts().items():
            for account in accounts:
                if not (self._account_identity(account) & observed_accounts):
                    continue
                bio = str(account.get("desc") or account.get("signature") or "").strip()
                if bio:
                    carriers.append({
                        "carrier_type": "social_account", "platform": platform,
                        "name": account.get("nickname") or account.get("username"),
                        "account_id": next(iter(self._account_identity(account)), None),
                        "official_status": "unverified", "activity_status": "unknown",
                        "profile_bio": bio[:800], "source": "ACCOUNT_DISCOVERY",
                    })
        for completed in self.completed:
            if completed.get("phase") != "WEB_ICP":
                continue
            request = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            for record in summary.get("records") or []:
                if not isinstance(record, dict):
                    continue
                carrier_type = str(record.get("carrier_type") or request.get("carrier") or "other")
                name = record.get("domain") or record.get("appName") or record.get("name")
                if name:
                    carriers.append({"carrier_type": carrier_type, "name": name, "source": "WEB_ICP"})
        seen: set[tuple[str, str, str]] = set()
        return [item for item in carriers if (key := (str(item.get("carrier_type") or ""), str(item.get("platform") or ""), str(item.get("name") or item.get("url") or ""))) not in seen and not seen.add(key)]

    def _posts_corpus_path(self) -> Path:
        return self._platform_keyword_corpus_path("posts_corpus.txt")

    # --- 关键词历史：跨轮去重查 DB 的 KEYWORD TERMINAL 任务，与作品/账号去重同源；不再用文件账本 ---

    def _normalize_keyword(self, keyword: str) -> str:
        value = unicodedata.normalize("NFKC", str(keyword or "")).strip().casefold()
        return re.sub(r"\s+", " ", value)

    def _load_used_keywords(self) -> list[dict[str, Any]]:
        # 已用于搜索的关键词来自 DB（KEYWORD TERMINAL 任务的 result_json.keywords），按 round_no 区分轮次。
        records: list[dict[str, Any]] = []
        for item in self.db.historical_used_keywords(company_id(self.name)):
            normalized = self._normalize_keyword(str(item.get("keyword") or ""))
            if normalized:
                records.append({"round": int(item.get("round") or 0), "platform": str(item.get("platform") or ""), "normalized": normalized})
        return records

    def _keyword_payload(self, result: Any) -> dict[str, Any]:
        # summary 可能套一两层 summary，统一在这里解包。
        if not isinstance(result, dict):
            return {}
        inner = result.get("summary")
        inner = inner if isinstance(inner, dict) else result
        nested = inner.get("summary") if isinstance(inner, dict) else None
        return nested or inner

    def _keyword_map(self, result: Any) -> dict[str, list[str]]:
        payload = self._keyword_payload(result)
        keywords = payload.get("keywords") if isinstance(payload, dict) else None
        if not isinstance(keywords, dict):
            return {}
        mapped: dict[str, list[str]] = {}
        for platform in PLATFORMS:
            value = keywords.get(platform)
            raw_values = value if isinstance(value, list) else [value]
            seen: set[str] = set()
            items: list[str] = []
            for raw in raw_values:
                keyword = str(raw or "").strip()
                normalized = self._normalize_keyword(keyword)
                if keyword and normalized not in seen:
                    seen.add(normalized)
                    items.append(keyword)
            if items:
                mapped[platform] = items
        return mapped

    def _keyword_was_used(self, platform: str, keyword: str, before_round: int | None = None) -> bool:
        target = self._dedup_key("keyword", {"platform": platform, "normalized": self._normalize_keyword(keyword)})
        if not target:  # 空 platform/keyword 短路，避免 None==None 误匹配
            return False
        return any(
            self._dedup_key("keyword", rec) == target
            and (before_round is None or rec["round"] < before_round)
            for rec in self._load_used_keywords()
        )

    def _enterprise_alias_snapshot(self, platform: str | None = None) -> list[dict[str, Any]]:
        return self.db.list_enterprise_aliases(company_id(self.name), platform=platform)

    @staticmethod
    def _keyword_priority(keyword: str, aliases: list[dict[str, Any]]) -> int:
        """搜索关键词分级：0=已确认平台账号ID 1=已确认账号名 2=简称/法定名/品牌 3=其他LLM词。"""
        key = keyword.strip().casefold()
        for alias in aliases:
            if not isinstance(alias, dict):
                continue
            value = str(alias.get("normalized_value") or alias.get("alias_value") or "").strip().casefold()
            if not value or value != key:
                continue
            alias_type = str(alias.get("alias_type") or "")
            confirmed = bool(alias.get("is_confirmed"))
            if alias_type == "platform_account_id" and confirmed:
                return 0
            if alias_type == "account_name":
                return 1 if confirmed else 2
            if alias_type in {"short_name", "legal_name", "brand_name", "website_brand", "icp_service_name"}:
                return 2
        return 3

    def _persist_keyword_alias_candidates(self, summary: dict[str, Any]) -> None:
        candidates = summary.get("alias_candidates") if isinstance(summary, dict) else None
        if not isinstance(candidates, list):
            return
        allowed_types = {
            "brand_name", "account_name", "platform_account_id",
            "website_brand", "icp_service_name",
        }
        stored = 0
        for item in candidates:
            if not isinstance(item, dict):
                continue
            alias_type = str(item.get("alias_type") or item.get("type") or "brand_name").strip()
            value = str(item.get("alias_value") or item.get("value") or "").strip()
            if alias_type not in allowed_types or not value:
                continue
            if self.db.upsert_enterprise_alias(
                company_id(self.name), alias_type, value,
                platform=str(item.get("platform") or "") or None,
                source_type=str(item.get("source_type") or "website"),
                source_ref=str(item.get("source_ref") or "") or None,
                confidence=float(item.get("confidence") or 0.65),
                is_confirmed=bool(item.get("is_confirmed") or item.get("confirmed")),
            ):
                stored += 1
        if stored:
            self.db.event(uuid4().hex, "company", company_id(self.name), "enterprise_aliases_upserted", {
                "source_phase": "KEYWORD", "stored_count": stored, "round": self.round_no,
            })

    def _emit_skip(self, request: Request | None, **fields: Any) -> None:
        if callable(self.log):
            self.log("TASK_SKIPPED", self, request, **fields)

    # --- 账号发现与筛选 ---

    def _accounts(self) -> dict[str, list[dict[str, Any]]]:
        result = {p: [] for p in PLATFORMS}
        for file in self._files_for("ACCOUNT_DISCOVERY"):
            payload = json_load(file)
            platform = str(payload.get("platform") or "")
            if platform in result:
                records = payload.get("records") or payload.get("accounts") or []
                result[platform].extend(x for x in records if isinstance(x, dict))
        # 每平台一个临时 seen（绝不持久化：_accounts 在 run 内被调三次，持久 seen 会让后次调用丢号）。
        # batch 先过 filter 先占 seen（batch 优先），historical 复用同一 seen 去重；batch 内也 dedup，修 round1+round2 同账号重复。
        for platform in PLATFORMS:
            seen: set[str] = set()
            result[platform] = self._dedup_filter("account", result[platform], seen)
            # historical 前置过滤 id 全空项：_dedup_filter 对无 key 项一律保留，而 id 全空（sec_uid/user_id/kwai_id/nickname 都没）
            # 的历史项不能靠下游兜底——_account_identity 含 nickname/username 会误匹配污染候选池。
            historical = [h for h in self.db.historical_discovered_accounts(company_id(self.name), platform)
                          if self._dedup_key("account", h)]
            for item in self._dedup_filter("account", historical, seen):
                result[platform].append(item)
        return result

    def _round_accounts(self, round_no: int) -> dict[str, list[dict[str, Any]]]:
        """Return only accounts newly discovered in this round.

        Round 1 intentionally keeps the broader historical candidate pool through
        ``_accounts``.  Later rounds must not feed that pool back into ranking,
        otherwise a new keyword on one platform re-ranks and re-reviews accounts
        that were already handled in the previous round.
        """
        current: dict[str, list[dict[str, Any]]] = {platform: [] for platform in PLATFORMS}
        prior_seen: dict[str, set[str]] = {platform: set() for platform in PLATFORMS}

        for completed in self.completed:
            phase = str(completed.get("phase") or "")
            request_payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            completed_round = int(request_payload.get("round") or 1)

            if phase == "ACCOUNT_RANK" and completed_round < round_no:
                platform_pools = request_payload.get("platforms")
                if not isinstance(platform_pools, dict):
                    continue
                for platform, accounts in platform_pools.items():
                    if platform not in prior_seen or not isinstance(accounts, list):
                        continue
                    for account in accounts:
                        if isinstance(account, dict):
                            key = self._dedup_key("account", account)
                            if key:
                                prior_seen[platform].add(key)
                continue

            if phase != "ACCOUNT_DISCOVERY":
                continue
            platform = str(completed.get("platform") or "")
            if platform not in current:
                continue
            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            accounts = summary.get("records") or summary.get("accounts") or summary.get("results") or []
            if not isinstance(accounts, list):
                continue
            valid_accounts = [account for account in accounts if isinstance(account, dict)]
            if completed_round < round_no:
                for account in valid_accounts:
                    key = self._dedup_key("account", account)
                    if key:
                        prior_seen[platform].add(key)
            elif completed_round == round_no:
                current[platform].extend(valid_accounts)

        result: dict[str, list[dict[str, Any]]] = {}
        for platform in PLATFORMS:
            novel = self._dedup_filter("account", current[platform], prior_seen[platform])
            if novel:
                result[platform] = novel
        return result

    def _latest_rank_fallback(self, platform: str) -> list[dict[str, Any]]:
        row = self.db.latest_successful_rankings(company_id(self.name), platform)
        if not row:
            return []
        try:
            payload = json.loads(row["result_json"] or "{}")
        except json.JSONDecodeError:
            return []
        ranked = payload.get("results", {}).get(platform, {})
        top_accounts = ranked.get("top_accounts") if isinstance(ranked, dict) else []
        return [x for x in top_accounts if isinstance(x, dict)]

    def _rank_results(self, summary: dict[str, Any]) -> dict[str, Any]:
        # 适配器结果可能套一层 summary，解包后再消费。
        direct = summary.get("results")
        if isinstance(direct, dict):
            return direct
        nested = summary.get("summary")
        if isinstance(nested, dict) and isinstance(nested.get("results"), dict):
            return nested["results"]
        return {}

    def _account_identity(self, account: dict[str, Any]) -> set[str]:
        return {
            str(account.get(key)).strip()
            for key in ("sec_uid", "user_id", "kwai_id", "account_id", "douyin_id", "username", "nickname")
            if account.get(key)
        }

    def _selected_account(self, spec: PlatformSpec, selected: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
        # 把 LLM 筛选字段与原始发现记录合并，发现记录是抓取凭证的事实来源。
        selected_ids = self._account_identity(selected)
        selected_name = str(selected.get("username") or selected.get("nickname") or "").strip()
        matched: dict[str, Any] = {}
        for candidate in candidates:
            candidate_ids = self._account_identity(candidate)
            if selected_ids & candidate_ids or (selected_name and selected_name in candidate_ids):
                matched = candidate
                break
        merged = {**matched, **selected}
        id_value = matched.get(spec.account_id_field) or selected.get(spec.account_id_field)
        for fb in spec.id_fallback_fields:
            id_value = id_value or selected.get(fb)
        if not id_value and spec.id_from_user_prefix:
            uid = str(selected.get("user_id") or "")
            if uid.startswith(spec.id_from_user_prefix):
                id_value = selected.get("user_id")
        merged[spec.account_id_field] = id_value
        if spec.needs_token:
            merged["xsec_token"] = matched.get("xsec_token") or selected.get("xsec_token")
        merged["account_name"] = (
            selected.get("username") or selected.get("nickname") or
            matched.get("nickname") or matched.get("username") or "unknown"
        )
        return merged

    def _append_account_review_request(self, spec: PlatformSpec, account: dict[str, Any]) -> bool:
        """Queue one bounded homepage sample for a metadata-shortlisted account."""
        id_value = str(account.get(spec.account_id_field) or "").strip()
        if not id_value or (spec.needs_token and not account.get("xsec_token")):
            return False
        stage = "page" if spec.skip_home else "home"
        review_page_size = ACCOUNT_REVIEW_PAGE_SIZE.get(spec.name, ACCOUNT_REVIEW_SAMPLE_SIZE)
        payload: dict[str, Any] = {
            "stage": stage,
            spec.account_id_field: id_value,
            "account_name": account.get("account_name") or "unknown",
            "candidate": account,
            "round": self.round_no,
            "sample_limit": review_page_size,
            "screenshot_version": SOCIAL_SCREENSHOT_VERSION,
        }
        if spec.needs_token:
            payload["xsec_token"] = account.get("xsec_token")
        if spec.skip_home:
            payload["cursor"] = "0"
            command = [
                self.python, _script_path(spec.review_script), _id_flag(spec), id_value,
                "--cursor", "0", "--max-items", str(review_page_size),
            ]
        else:
            command = [
                self.python, _script_path(spec.review_script), _id_flag(spec), id_value,
                "--max-items", str(review_page_size),
            ]
            if spec.needs_token:
                command += ["--xsec-token", str(account.get("xsec_token") or "")]
        self.requests.append(Request("ACCOUNT_REVIEW", spec.name, payload, command, "PLATFORM", spec.name))
        return True

    def _account_review_evidence(self) -> dict[str, list[dict[str, Any]]]:
        evidence: dict[str, list[dict[str, Any]]] = {platform: [] for platform in PLATFORMS}
        seen: set[tuple[str, str]] = set()
        for completed in self.completed:
            if completed.get("phase") != "ACCOUNT_REVIEW":
                continue
            payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            if int(payload.get("round") or 1) != self.round_no:
                continue
            platform = str(completed.get("platform") or "")
            if platform not in evidence:
                continue
            candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
            spec = PLATFORM_SPECS[platform]
            identity = str(candidate.get(spec.account_id_field) or payload.get(spec.account_id_field) or payload.get("account_name") or "")
            key = (platform, identity)
            if key in seen:
                continue
            seen.add(key)
            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            safe_candidate = {
                key: value
                for key, value in candidate.items()
                if "token" not in str(key).lower() and "cookie" not in str(key).lower()
            }
            evidence[platform].append({
                "account": safe_candidate,
                "sample": _compact_account_review(summary, limit=ACCOUNT_REVIEW_PAGE_SIZE.get(platform, ACCOUNT_REVIEW_SAMPLE_SIZE)),
            })
        return evidence

    def _queue_account_verify(self) -> bool:
        evidence = self._account_review_evidence()
        if not any(evidence.values()):
            return False
        self.requests.append(Request(
            "ACCOUNT_VERIFY", None, {
                "platforms": evidence,
                "aliases": self._enterprise_alias_snapshot(),
                "round": self.round_no,
                "ownership_gate_version": 3,
            },
            [self.python, _script_path("social_account_review.py"), self.name],
            "LLM", "global",
        ))
        return True

    def _verification_results(self, summary: dict[str, Any]) -> dict[str, Any]:
        direct = summary.get("results")
        if isinstance(direct, dict):
            return direct
        nested = summary.get("summary")
        if isinstance(nested, dict) and isinstance(nested.get("results"), dict):
            return nested["results"]
        return {}

    def _remember_account_verifications(self, results: dict[str, Any]) -> None:
        for platform in PLATFORMS:
            platform_result = results.get(platform) if isinstance(results.get(platform), dict) else {}
            for decision_name in ("accepted_accounts", "related_accounts", "rejected_accounts"):
                for account in platform_result.get(decision_name) or []:
                    if not isinstance(account, dict):
                        continue
                    self.account_verifications.append({
                        **account,
                        "platform": platform,
                        "round": self.round_no,
                        "decision": (
                            "accepted" if decision_name == "accepted_accounts"
                            else "related" if decision_name == "related_accounts"
                            else "rejected"
                        ),
                    })
                    if decision_name != "accepted_accounts":
                        continue
                    confirmed = str(account.get("official_status") or "") == "verified"
                    account_name = str(
                        account.get("account_name") or account.get("username") or account.get("nickname") or ""
                    ).strip()
                    if account_name:
                        self.db.upsert_enterprise_alias(
                            company_id(self.name), "account_name", account_name,
                            platform=platform, source_type="account_verified",
                            confidence=float(account.get("confidence") or 0.7),
                            is_confirmed=confirmed,
                        )
                    visible_id = str(
                        account.get("account_id") or account.get("douyin_id")
                        or account.get("red_id") or account.get("kwai_id") or ""
                    ).strip()
                    if visible_id:
                        self.db.upsert_enterprise_alias(
                            company_id(self.name), "platform_account_id", visible_id,
                            platform=platform, source_type="account_verified",
                            confidence=float(account.get("confidence") or 0.7),
                            is_confirmed=confirmed,
                        )

    # --- 作品去重：seen-keys 缓存，懒加载，避免每次全量重扫 ---

    def _post_account_key(self, spec: PlatformSpec, payload: dict[str, Any]) -> str | None:
        text = str(payload.get(spec.account_id_field) or "").strip()
        return f"{spec.name}:home:{text}" if text else None

    def _post_item_key(self, spec: PlatformSpec, payload: dict[str, Any]) -> str | None:
        for key in spec.item_id_fields:
            text = str(payload.get(key) or "").strip()
            if text:
                return f"{spec.name}:post:{text}"
        stage = str(payload.get("stage") or "")
        if stage in {"page", "scroll"}:
            account_text = str(
                payload.get(spec.account_id_field) or payload.get("sec_uid")
                or payload.get("user_id") or payload.get("kwai_id") or payload.get("account_id") or ""
            ).strip()
            cursor = str(payload.get("cursor") or payload.get("next_cursor") or "").strip()
            if account_text and cursor:
                return f"{spec.name}:{stage}:{account_text}:{cursor}"
        return None

    def _dedup_key(self, ns: str, item: Any) -> str | None:
        return DEDUP_SCOPES[ns].make_key(item)

    def _dedup_filter(self, ns: str, items: Iterable[Any], seen: set[str]) -> list[Any]:
        # 首次出现的保留（含无 key 的），就地填 seen；seen 持久还是临时由调用方决定。
        out: list[Any] = []
        for item in items:
            key = DEDUP_SCOPES[ns].make_key(item)
            if key:
                if key in seen:
                    continue
                seen.add(key)
            out.append(item)
        return out

    def _ensure_seen_post_keys(self) -> None:
        # post 自管不接 registry：make_key 依赖 spec（item_id_fields/account_id_field）、消费是点查非流式 filter、
        # _seen_post_accounts 生命周期由 home_skips_if_seen 分支决定、_seen_posts_by_account 是 existing_ids 下推非去重 gate。
        # fence（重构必保）：_processing_payload 排除当前页（使当前页不计库存）、懒加载须早于首个 page 完成（_inventory_hit 才准）。
        if self._seen_post_accounts is not None:
            return
        accounts: set[str] = set()
        items: set[str] = set()
        by_account: dict[str, set[str]] = {}
        rows: list[tuple[PlatformSpec, dict[str, Any], dict[str, Any], bool]] = []
        # 本轮已完成：completed 的 summary 可能套一层，需 unwrap。
        for completed in self.completed:
            if completed.get("phase") != "POST_CRAWL":
                continue
            spec = PLATFORM_SPECS.get(str(completed.get("platform") or ""))
            if not spec:
                continue
            payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            if payload is self._processing_payload:
                continue
            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            if str(summary.get("status") or "").lower() in POST_FAIL_STATUSES:
                continue
            rows.append((spec, payload, summary, True))
        # 历史 DB 记录：result_json 已是最终 summary，不再 unwrap。
        for row in self.db.historical_post_crawl_results(company_id(self.name)):
            spec = PLATFORM_SPECS.get(str(row.get("platform") or ""))
            if not spec:
                continue
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            result = row.get("result") if isinstance(row.get("result"), dict) else {}
            rows.append((spec, payload, result, False))
        for spec, payload, summary, unwrap in rows:
            if payload.get("stage") == "home":
                key = self._post_account_key(spec, payload)
                if key and (spec.home_skips_if_seen or unwrap):
                    # home_skips_if_seen=False（xhs）只做本轮同批次去重（unwrap=True），历史 home 不计入 → 跨 run 增量重爬。
                    accounts.add(key)
            # 游标/页码级任务 key 仅做同 run 内去重（防同页重复入队）。
            # 历史 run 的任务 key（如 douyin:page:{uid}:0）跨 run 会逐字命中，
            # 导致新 run 的首 page 被误判为"已抓"直接跳过——即使上轮只抓了寥寥几条。
            # 跨 run 覆盖度由作品级 id 跟踪；page 级 key 历史不入库。
            item_key = self._post_item_key(spec, payload)
            stage = str(payload.get("stage") or "")
            if item_key and stage in {"detail", "page", "scroll"} and (unwrap or stage == "detail"):
                # detail 的作品级 id（如 note_id）跨 run 入 seen，保证详情不重抓；
                # page/scroll 的 cursor 级 key 仅本轮入 seen，防历史逐字命中误判首页已抓。
                items.add(item_key)
            if spec.name == "douyin":
                target = summary.get("summary") if (unwrap and isinstance(summary, dict) and isinstance(summary.get("summary"), dict)) else summary
                for item in _iter_items(target):
                    key = self._post_item_key(spec, item)
                    if key:
                        items.add(key)
            elif spec.name in {"xhs", "kuaishou"}:
                # 按 user_id 聚合历史作品 id，作为下次 home 增量重爬的停止锚点。
                uid = str(payload.get(spec.account_id_field) or "").strip()
                if uid:
                    target = summary.get("summary") if (unwrap and isinstance(summary, dict) and isinstance(summary.get("summary"), dict)) else summary
                    for item in _iter_items(target):
                        nid = ""
                        for id_field in spec.item_id_fields:
                            nid = str(item.get(id_field) or "").strip()
                            if nid:
                                break
                        if nid:
                            by_account.setdefault(uid, set()).add(nid)
        self._seen_post_accounts = accounts
        self._seen_post_items = items
        self._seen_posts_by_account = by_account

    def _ensure_review_skip_ids(self) -> None:
        # review 样本去重：从 completed 的 ACCOUNT_REVIEW 结果按账号聚合帖子 id。
        # 只喂给 home 任务的 --skip-ids（跳过不收集、不触发锚点停），与 POST_CRAWL
        # 终止库存(_seen_post_items/_seen_posts_by_account/_inventory_hit)物理隔离。
        if self._seen_review_items is not None:
            return
        by_account: dict[str, set[str]] = {}
        for completed in self.completed:
            if completed.get("phase") != "ACCOUNT_REVIEW":
                continue
            spec = PLATFORM_SPECS.get(str(completed.get("platform") or ""))
            if not spec:
                continue
            payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            account_id = str(payload.get(spec.account_id_field) or "").strip()
            if not account_id:
                continue
            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            ids = by_account.setdefault(account_id, set())
            for item in _iter_items(summary):
                for field in spec.item_id_fields:
                    value = str(item.get(field) or "").strip()
                    if value:
                        ids.add(value)
                        break
        self._seen_review_items = by_account

    def _list_pagination(self, spec: PlatformSpec, summary: Any) -> tuple[bool, str]:
        # 统一按 has_more + cursor 翻页。xhs/ks 无独立翻页脚本（list_script=None），
        # 该函数只服务 skip_home 平台（douyin）；kuaishou 曾经的"只取一页"特判已随死代码删除。
        if not isinstance(summary, dict):
            return False, ""
        has_more = bool(summary.get("has_more"))
        cursor = ""
        for field in ("next_cursor", *spec.extra_cursor_fields, "cursor"):
            cursor = str(summary.get(field) or "").strip()
            if cursor:
                break
        return has_more, cursor

    def _inventory_hit_for_page(self, spec: PlatformSpec, summary: Any) -> bool:
        # skip_home 平台（douyin）翻页停检：仅当本页全部作品都已在历史库存里才停止翻页。
        # 部分重叠说明上轮抓取不完整（如只爬到几条就断了）；继续翻页给补全机会。
        # _seen_post_items 在本页入队前已懒加载完成（首次构建早于首个 page 完成），故不含本页自身作品。
        if not spec.skip_home:
            return False
        self._ensure_seen_post_keys()
        items = _iter_items(summary)
        if not items:
            return False
        seen_count = 0
        for item in items:
            if isinstance(item, dict):
                key = self._post_item_key(spec, item)
                if key and key in self._seen_post_items:
                    seen_count += 1
        return seen_count == len(items)

    def _append_post_request(self, spec: PlatformSpec, payload: dict[str, Any]) -> bool:
        # 作品入队统一入口：按 stage 决定 key 与命令，命中 seen 则跳过。
        self._ensure_seen_post_keys()
        stage = str(payload.get("stage") or "")
        if stage == "home":
            key = self._post_account_key(spec, payload)
            seen, kind = self._seen_post_accounts, "post_home"
            command = [self.python, _script_path(spec.home_script), _id_flag(spec), str(payload.get(spec.account_id_field) or "")]
            if spec.needs_token:
                command += ["--xsec-token", str(payload.get("xsec_token") or "")]
            if spec.name in {"xhs", "kuaishou"}:
                # full_crawl 传 0（无上限，脚本滚到权威到底信号）；采样模式用 MAX_POST_ITEMS_PER_ACCOUNT。
                max_items = 0 if self.full_crawl else MAX_POST_ITEMS_PER_ACCOUNT
                command += ["--max-items", str(max_items)]
            if not spec.home_skips_if_seen:
                existing = (self._seen_posts_by_account or {}).get(str(payload.get(spec.account_id_field) or ""), set())
                if existing:
                    command += ["--existing-ids", *sorted(existing)]
            # review 样本去重分流：跳过不收集、不触发锚点停。独立库存，绝不影响终止判定。
            self._ensure_review_skip_ids()
            review_skip = (self._seen_review_items or {}).get(str(payload.get(spec.account_id_field) or ""), set())
            if review_skip:
                command += ["--skip-ids", *sorted(review_skip)]
        elif stage == "detail":
            key = self._post_item_key(spec, payload)
            seen, kind = self._seen_post_items, "post_item"
            command = [self.python, _script_path(spec.detail_script), "--note-id", str(payload.get("note_id") or ""),
                       "--xsec-token", str(payload.get("xsec_token") or ""), "--href", str(payload.get("href") or "")]
        else:  # page / scroll
            if not spec.list_script:
                return False
            key = self._post_item_key(spec, payload)
            seen, kind = self._seen_post_items, "post_item"
            command = [self.python, _script_path(spec.list_script), _id_flag(spec), str(payload.get(spec.account_id_field) or "")]
            if spec.needs_token:
                command += ["--xsec-token", str(payload.get("xsec_token") or "")]
            command += ["--cursor", str(payload.get("cursor") or "")]
        if key and key in seen:
            self._emit_skip(None, kind=kind, platform=spec.name, account_name=payload.get("account_name"),
                            key=key, reason=f"historical_{kind}_seen")
            return False
        if key:
            seen.add(key)
        self.requests.append(Request("POST_CRAWL", spec.name, payload, command, "PLATFORM", spec.name))
        return True

    def _queue_website_keyword(self) -> None:
        """Advance to social-keyword extraction when the website asset chain ends."""
        corpus = self._corpus_path()
        self.requests.append(Request(
            "KEYWORD", None, {
                "corpus_file": str(corpus), "round": 1, "corpus_kind": "website",
                "aliases": self._enterprise_alias_snapshot(),
            },
            [self.python, _script_path("social_keyword_extract.py"), self.name, "--corpus-file", str(corpus)],
            "LLM", "global",
        ))

    def on_finished(self, request: Request, result: dict[str, Any], diagnosis: dict[str, Any] | None = None) -> None:
        # 三层分离：
        # ① _diagnose_attempt — 状态解释（子进程 exit code/stderr → 可重试/致命/登录失效）
        # ② _summarize_result — 人工可读摘要（仅用于日志/progress）
        # ③ on_finished — 状态推进（将已放行的 summary 推向下游 fan-out / 阶段切换）
        # ①→②→③ 严格单向：②/③ 不改①的判定，③ 不改②的展示。
        """消费一个完成结果：诊断负责放行，summary 负责业务推进。"""
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else result
        summary = dict(summary)
        diagnosis = diagnosis or (result.get("diagnosis") if isinstance(result.get("diagnosis"), dict) else None)
        if diagnosis and "diagnosis" not in summary:
            summary["diagnosis"] = diagnosis
        self._processing_payload = request.payload
        _status = (diagnosis or {}).get("status") or str(result.get("status") or "SUCCEEDED")
        terminal_status = str(_status).upper()
        self.phase_status.setdefault(request.phase, []).append(_status)
        self.completed.append({
            "request": request.payload,
            "phase": request.phase,
            "platform": request.platform,
            "summary": summary,
        })
        self._capture_domain_activity(request, summary)
        accepted_output = terminal_status in {"SUCCEEDED", "EMPTY_SUCCESS", "OK"}
        if accepted_output:
            self._capture_social_account_activity(request, summary)
            if request.phase == "KEYWORD":
                self._persist_keyword_alias_candidates(summary)
        # Failed or identity-contaminated output remains auditable in the attempt
        # directory, but must never enter keyword/risk evidence.
        keyword_distribution = self._write_keyword_records(request, result) if accepted_output else {}
        if keyword_distribution.get("texts") and callable(self.log):
            self.log(
                "KEYWORD_DISTRIBUTION",
                self,
                request,
                distribution=keyword_distribution,
                account=request.payload.get("account_name"),
                stage=request.payload.get("stage"),
            )
        # 每个 POST_CRAWL 任务一完成就分叉自己的 detail/翻页（wave guard 之前，不受 inflight 阻断）。
        if request.phase == "POST_CRAWL" and accepted_output:
            self._fanout_post_crawl(request, summary)
        # WEB_ICP 单页完成即按 has_next_page 决定是否排下一页（同上，wave guard 之前）。
        if request.phase == "WEB_ICP" and accepted_output:
            self._fanout_icp_page(request, summary)
        # 官网每页完成后立即把同站子页面加入 BFS 队列；只有整波真正排空才进入图片阶段。
        if request.phase == "WEB_CRAWL" and terminal_status in {"SUCCEEDED", "OK"}:
            self._fanout_web_page(request, summary)
        # 一个阶段可能 fan-out 出多个独立请求；等整波完成后再派生下一阶段。
        if self.inflight:
            return
        if request.phase == "WEB_ICP" and not self.requests:
            # ICP 是强证据，但不再是官网发现的唯一入口；每家企业都补一次互联网搜索。
            self._queue_web_search()
        elif request.phase == "WEB_SEARCH" and not self.requests:
            # 合并 ICP 与搜索候选。搜索失败时仍可使用 ICP 结果继续，不阻塞整条流水线。
            roots = self._roots()
            if roots:
                for root in roots:
                    self._queue_web_page(root, depth=0, crawl_root=root)
            if roots and not self.requests:
                self.web_crawl_stats["invalid_roots"] += len(roots)
            if not self.requests:
                self._emit_skip(request, kind="web_crawl", reason="no_crawlable_web_root")
                self._emit_skip(request, kind="image_download", reason="no_crawlable_web_root")
                self._emit_skip(request, kind="ocr", reason="no_crawlable_web_root")
                self._queue_website_keyword()
        elif request.phase == "WEB_CRAWL" and not self.requests:
            eligible_results: list[dict[str, Any]] = []
            raw_candidate_count = 0
            dynamic_pages = 0
            static_pages = 0
            for file in self._files_for("WEB_CRAWL"):
                payload = json_load(file)
                if str(payload.get("fetch_function") or "") == "screenshot_crawler.render_page":
                    dynamic_pages += 1
                    continue
                if payload.get("image_download_required") is False:
                    continue
                static_pages += 1
                raw = payload.get("image_candidates")
                if isinstance(raw, (list, tuple)):
                    raw_candidate_count += len(raw)
                eligible_results.append(payload)
            candidates = dedupe_image_candidates(eligible_results)
            selected = candidates[:MAX_IMAGE_CANDIDATES]
            for item in selected:
                url = str(item.get("url") or "")
                host = (urlsplit(url).hostname or "unknown").lower()
                if url:
                    self.requests.append(Request("IMAGE_DOWNLOAD", None, {"url": url},
                        [self.python, _script_path("image_downloader.py"), url], "HOSTNAME", host))
            self.web_crawl_stats["dynamic_pages"] = dynamic_pages
            self.web_crawl_stats["static_pages"] = static_pages
            self.web_crawl_stats["raw_image_candidates"] = raw_candidate_count
            self.web_crawl_stats["unique_image_candidates"] = len(candidates)
            self.web_crawl_stats["image_duplicates_removed"] = max(
                0, raw_candidate_count - len(candidates),
            )
            self.web_crawl_stats["image_limit_drops"] = max(
                0, len(candidates) - len(selected),
            )
            if callable(self.log):
                self.log(
                    "WEB_CRAWL_SUMMARY", self, request,
                    pages=self.web_pages_scheduled,
                    dynamic=dynamic_pages,
                    static=static_pages,
                    links=self.web_crawl_stats.get("links_discovered", 0),
                    images_raw=raw_candidate_count,
                    images_unique=len(candidates),
                    images_queued=len(selected),
                    duplicates=max(0, raw_candidate_count - len(candidates)),
                )
            if not self.requests:
                self._emit_skip(request, kind="image_download", reason="no_usable_image_candidates")
                self._emit_skip(request, kind="ocr", reason="no_usable_image_candidates")
                self._queue_website_keyword()
        elif request.phase == "IMAGE_DOWNLOAD" and not self.requests:
            images = self._downloaded_images()
            for image in images:
                self.requests.append(Request("OCR", None, {"image": str(image)},
                    [self.python, _script_path("ocr_image.py"), str(image)], "OCR", "global"))
            if not images:
                self._emit_skip(request, kind="ocr", reason="no_downloaded_images")
                self._queue_website_keyword()
        elif request.phase == "OCR" and not self.requests:
            self._queue_website_keyword()
        elif request.phase == "KEYWORD" and not self.requests:
            if int(request.payload.get("round") or 1) == 2:
                self.round_no = 2
                if str(_status).upper() == "EMPTY_SUCCESS":
                    self._queue_risk_analysis()
                    return
            # 关键词搜索队列只消费本次成功结果；该结果已经显式标注并合并
            # LLM 正文实体与确定性的企业/品牌/平台账号 ID 种子。
            current_keywords = self._keyword_map(summary) if accepted_output else {}
            aliases_snapshot = self._enterprise_alias_snapshot()
            # 当轮 LLM 新发现的实体（alias_candidates）：名称类里插队到最前——
            # 老词历史上多半搜过，新线索的信息增量最大。
            new_clues = {
                str(item.get("alias_value") or item.get("value") or "").strip().casefold()
                for item in ((summary.get("alias_candidates") if isinstance(summary, dict) else None) or [])
                if isinstance(item, dict) and str(item.get("alias_value") or item.get("value") or "").strip()
            }

            def effective_priority(kw: str) -> int:
                p = self._keyword_priority(kw, aliases_snapshot)
                if p >= 1 and kw.strip().casefold() in new_clues:
                    return 1
                return p

            # 搜索任务额度：按台账确认度分级（确认ID>确认账号名>简称/品牌>其他LLM词），
            # 平台间轮转取词，污染词（含万/亿/空白）直接丢弃；全批次累计超限即跳过。
            discovery_used = (
                sum(1 for item in self.requests if item.phase == "ACCOUNT_DISCOVERY")
                + sum(1 for item in self.completed if item.get("phase") == "ACCOUNT_DISCOVERY")
            )
            per_platform: list[tuple[str, list[str]]] = []
            for platform, keywords in current_keywords.items():
                ranked: list[str] = []
                for index, keyword in enumerate(keywords):
                    if re.search(r"[万亿\s]", keyword):
                        self._emit_skip(request, kind="keyword", platform=platform, keyword=keyword,
                                        round=self.round_no, reason="polluted_keyword_dropped")
                        continue
                    ranked.append(keyword)
                # 同级内当轮新线索排前（稳定排序保原顺序，新线索插队到同级最前）。
                ranked.sort(key=lambda kw: (effective_priority(kw), 0 if kw.strip().casefold() in new_clues else 1))
                per_platform.append((platform, ranked))
            # 每平台保底 1 个名称类词（P1/P2 最优者，不含 P0 账号ID）：LLM 新发现的简称/品牌
            # 也是名称类，不能被确认 ID 占满额度而轮空。保底先占位，剩余词再按优先级轮转。
            ordered: list[tuple[str, str]] = []
            reserved: set[tuple[str, str]] = set()
            for platform, ranked in per_platform:
                for kw in ranked:
                    if 1 <= effective_priority(kw) <= 2:
                        ordered.append((platform, kw))
                        reserved.add((platform, kw))
                        break
            depth = 0
            while True:
                added = False
                for platform, ranked in per_platform:
                    candidates = [kw for kw in ranked if (platform, kw) not in reserved]
                    if depth < len(candidates):
                        ordered.append((platform, candidates[depth]))
                        added = True
                if not added:
                    break
                depth += 1
            for platform, keyword in ordered:
                if discovery_used >= MAX_DISCOVERY_TASKS_PER_COMPANY:
                    self._emit_skip(request, kind="keyword", platform=platform, keyword=keyword,
                                    round=self.round_no, reason="discovery_task_budget_exceeded")
                    continue
                if self.round_no == 2 and self._keyword_was_used(platform, keyword, before_round=self.round_no):
                    self._emit_skip(request, kind="keyword", platform=platform, keyword=keyword,
                                    round=self.round_no, reason="historical_keyword_used")
                    continue
                spec = PLATFORM_SPECS[platform]
                self.requests.append(Request("ACCOUNT_DISCOVERY", platform, {
                    "keyword": keyword, "round": self.round_no,
                    "screenshot_version": SOCIAL_SCREENSHOT_VERSION,
                },
                    [self.python, _script_path(spec.search_script), keyword], "PLATFORM", platform))
                discovery_used += 1
            if not self.requests:
                if self.round_no == 2:
                    self._queue_risk_analysis()
                else:
                    self.requests.append(Request("ACCOUNT_RANK", None, {
                        "platforms": self._accounts(), "round": self.round_no,
                        "aliases": self._enterprise_alias_snapshot(), "guard_version": 3,
                    },
                        [self.python, _script_path("social_account_rank.py"), self.name], "LLM", "global"))
        elif request.phase == "ACCOUNT_DISCOVERY" and not self.requests:
            accounts = self._round_accounts(self.round_no) if self.round_no == 2 else self._accounts()
            if accounts:
                self.requests.append(Request("ACCOUNT_RANK", None, {
                    "platforms": accounts, "round": self.round_no,
                    "aliases": self._enterprise_alias_snapshot(), "guard_version": 3,
                },
                    [self.python, _script_path("social_account_rank.py"), self.name], "LLM", "global"))
            elif self.round_no == 2:
                self._queue_risk_analysis()
            elif not self.round2_started:
                self._queue_round2_keyword(self._corpus_path(), "website")
        elif request.phase == "ACCOUNT_RANK" and not self.requests:
            ranked = self._rank_results(summary) if isinstance(summary, dict) else {}
            requested_accounts = request.payload.get("platforms") if isinstance(request.payload, dict) else None
            candidate_pools = requested_accounts if isinstance(requested_accounts, dict) else self._accounts()
            for platform in PLATFORMS:
                if platform not in candidate_pools:
                    continue
                accounts = candidate_pools.get(platform) or []
                spec = PLATFORM_SPECS[platform]
                top_accounts: list[dict[str, Any]] = []
                plat = ranked.get(platform) if isinstance(ranked, dict) else None
                if isinstance(plat, dict):
                    top_accounts = plat.get("top_accounts") or []
                selected = top_accounts
                if not selected and self.round_no == 1:
                    selected = self._latest_rank_fallback(platform)
                if not top_accounts and selected:
                    self.db.event(uuid4().hex, "company", company_id(self.name), "stage_fallback", {
                        "stage": "ACCOUNT_RANK", "platform": platform,
                        "source": "history_latest_rank", "selected_count": len(selected),
                    })
                if not selected:
                    self._emit_skip(request, kind="account_review", platform=platform, reason="no_shortlisted_account")
                    continue
                protected = [
                    item for item in selected
                    if isinstance(item, dict) and item.get("protection_reasons")
                ]
                ordinary = [item for item in selected if item not in protected]
                review_selected = [
                    *protected[:ACCOUNT_REVIEW_CANDIDATES_PER_PLATFORM],
                    *ordinary[:max(0, ACCOUNT_REVIEW_CANDIDATES_PER_PLATFORM - len(protected))],
                ][:ACCOUNT_REVIEW_CANDIDATES_PER_PLATFORM]
                for raw_account in review_selected:
                    account = self._selected_account(spec, raw_account, accounts)
                    self._append_account_review_request(spec, account)
            if not self.requests and not self.round2_started:
                self._queue_round2_keyword(self._corpus_path(), "website")
            elif not self.requests and self.round_no == 2:
                self._queue_risk_analysis()
        elif request.phase == "ACCOUNT_REVIEW" and not self.requests:
            if not self._queue_account_verify():
                if self.round_no == 1 and not self.round2_started:
                    self._queue_round2_keyword(self._corpus_path(), "website")
                elif self.round_no == 2:
                    self._queue_risk_analysis()
        elif request.phase == "ACCOUNT_VERIFY" and not self.requests:
            verified = self._verification_results(summary) if isinstance(summary, dict) else {}
            self._remember_account_verifications(verified)
            discovered = self._accounts()
            for platform in PLATFORMS:
                spec = PLATFORM_SPECS[platform]
                platform_result = verified.get(platform) if isinstance(verified.get(platform), dict) else {}
                crawl_accounts = [
                    *(platform_result.get("accepted_accounts") or []),
                    *(platform_result.get("related_accounts") or []),
                ]
                for raw_account in crawl_accounts:
                    if not isinstance(raw_account, dict) or raw_account.get("eligible_for_post_crawl") is False:
                        continue
                    account = self._selected_account(spec, raw_account, discovered.get(platform, []))
                    id_value = account.get(spec.account_id_field)
                    if not id_value or (spec.needs_token and not account.get("xsec_token")):
                        continue
                    common = {
                        spec.account_id_field: id_value,
                        "account_name": account["account_name"],
                        "round": self.round_no,
                        "account_role": raw_account.get("account_role"),
                        "official_status": raw_account.get("official_status"),
                        "verification_confidence": raw_account.get("confidence"),
                    }
                    if spec.skip_home:
                        payload: dict[str, Any] = {
                            **common,
                            "stage": spec.list_stage,
                            "cursor": "0",
                            "page_no": 1,
                        }
                    else:
                        payload = {**common, "stage": "home"}
                    if spec.needs_token:
                        payload["xsec_token"] = account["xsec_token"]
                    self._append_post_request(spec, payload)
            if not self.requests and self.round_no == 1 and not self.round2_started:
                self._queue_round2_keyword(self._corpus_path(), "website")
            elif not self.requests and self.round_no == 2:
                self._queue_risk_analysis()
        elif request.phase == "POST_CRAWL" and not self.requests:
            self._drain_post_crawl(request, summary)
        elif request.phase == "RISK_ANALYSIS" and not self.requests:
            if not self._queue_risk_evidence(summary):
                self._finalize_risk_evidence()
                self.export_requested = True
                self.state = "CLOSED"
        elif request.phase == "RISK_EVIDENCE" and not self.requests:
            self._finalize_risk_evidence()
            self.export_requested = True
            self.state = "CLOSED"

    def _fanout_post_crawl(self, request: Request, summary: dict[str, Any]) -> None:
        """单个 POST_CRAWL 任务完成即推进：分叉 detail / 翻页。只 append self.requests，不做 round 切换。"""
        stage = str(request.payload.get("stage") or "")
        spec = PLATFORM_SPECS.get(request.platform) if request.platform else None
        if spec and stage == "home":
            id_value = str(request.payload.get(spec.account_id_field) or "")
            token_ok = not spec.needs_token or request.payload.get("xsec_token")
            if id_value and token_ok and not spec.home_skips_if_seen:
                # home_skips_if_seen=False（xhs/ks）：home 已一次抓全，对每条帖子分叉 detail。
                if spec.detail_script and isinstance(summary, dict):
                    account_name = request.payload.get("account_name")
                    token = str(request.payload.get("xsec_token") or "")
                    detail_items = summary.get("items") or []
                    if not self.full_crawl:
                        detail_items = detail_items[:MAX_POST_DETAILS_PER_ACCOUNT]
                    for item in detail_items:
                        if isinstance(item, dict) and item.get("note_id"):
                            self._append_post_request(spec, {
                                "stage": "detail", "note_id": str(item["note_id"]),
                                "xsec_token": str(item.get("xsec_token") or token or ""),
                                "href": str(item.get("detail_href") or ""), "account_name": account_name,
                                "account_id": id_value, "round": request.payload.get("round") or self.round_no,
                                "account_role": request.payload.get("account_role"),
                                "official_status": request.payload.get("official_status"),
                                "verification_confidence": request.payload.get("verification_confidence"),
                            })
        elif spec and stage in {"page", "scroll"}:
            id_value = str(request.payload.get(spec.account_id_field) or "")
            token = str(request.payload.get("xsec_token") or "") if spec.needs_token else ""
            if id_value and (token or not spec.needs_token):
                account_name = request.payload.get("account_name")
                # xhs scroll 对每条帖子分叉详情子任务。
                if spec.detail_script and isinstance(summary, dict):
                    detail_items = summary.get("items") or []
                    if not self.full_crawl:
                        detail_items = detail_items[:MAX_POST_DETAILS_PER_ACCOUNT]
                    for item in detail_items:
                        if isinstance(item, dict) and item.get("note_id"):
                            self._append_post_request(spec, {
                                "stage": "detail", "note_id": str(item["note_id"]),
                                "xsec_token": str(item.get("xsec_token") or token or ""),
                                "href": str(item.get("detail_href") or ""), "account_name": account_name,
                                "account_id": id_value, "round": request.payload.get("round") or self.round_no,
                                "account_role": request.payload.get("account_role"),
                                "official_status": request.payload.get("official_status"),
                                "verification_confidence": request.payload.get("verification_confidence"),
                            })
                has_more, next_cursor = self._list_pagination(spec, summary)
                page_no = int(request.payload.get("page_no") or 1)
                current_cursor = str(request.payload.get("cursor") or "").strip()
                # 采样模式保留页数预算；全量模式无上限（防死循环靠游标不前进检测）。
                page_budget = None if self.full_crawl else MAX_POST_PAGES_PER_ACCOUNT
                within_page_budget = page_budget is None or page_no < page_budget
                # 停摆护栏：服务端返回的下页游标与当前游标相同 → 不推进，防 cursor 不收敛死循环。
                if has_more and next_cursor and current_cursor and next_cursor == current_cursor:
                    self._emit_skip(request, kind="post_page", platform=spec.name,
                                    account_name=account_name, reason="cursor_not_advancing")
                elif has_more and next_cursor and within_page_budget and not self._inventory_hit_for_page(spec, summary):
                    payload = {"stage": spec.list_stage, spec.account_id_field: id_value,
                               "cursor": next_cursor, "account_name": account_name,
                               "round": request.payload.get("round") or self.round_no,
                               "page_no": page_no + 1,
                               "account_role": request.payload.get("account_role"),
                               "official_status": request.payload.get("official_status"),
                               "verification_confidence": request.payload.get("verification_confidence")}
                    if spec.needs_token:
                        payload["xsec_token"] = token
                    self._append_post_request(spec, payload)
                elif has_more and next_cursor and not within_page_budget:
                    self._emit_skip(request, kind="post_page", platform=spec.name,
                                    account_name=account_name, reason="account_page_budget_reached")
                elif spec.skip_home and has_more and next_cursor:
                    self._emit_skip(request, kind="post_page", platform=spec.name,
                                    account_name=account_name, reason="inventory_hit_early_stop")

    def _fanout_icp_page(self, request: Request, summary: dict[str, Any]) -> None:
        """单页 WEB_ICP 完成即推进：has_next_page=true 时排下一页（carrier 沿用，页号 +1）。"""
        if not isinstance(summary, dict) or not summary.get("has_next_page"):
            return
        carrier = str(request.payload.get("carrier") or "web")
        next_page = int(request.payload.get("page") or 1) + 1
        self.requests.append(Request(
            "WEB_ICP", None, {"carrier": carrier, "page": next_page},
            [self.python, _script_path("icp_query_one.py"), self.name, "--carrier", carrier, "--page", str(next_page)],
            "ICP", "global"))

    def _drain_post_crawl(self, request: Request, summary: dict[str, Any]) -> None:
        """整波 POST_CRAWL 排空后收尾：round1→round2（posts 语料）/ round2→export。"""
        stage = str(request.payload.get("stage") or "")
        if not self.requests and stage in {"home", "scroll", "detail", "page"}:
            self._report_social_account_activity(self.round_no)
            if self.round_no == 1 and not self.round2_started:
                self._queue_round2_keyword(self._posts_corpus_path(), "round2_posts")
            elif self.round_no == 2:
                self._queue_risk_analysis()

    def _activity_request_identity(self, platform: str, payload: dict[str, Any]) -> tuple[str, str]:
        spec = PLATFORM_SPECS[platform]
        candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
        account_id = next(
            (
                str(value).strip()
                for value in (
                    payload.get("account_id"),
                    payload.get(spec.account_id_field),
                    candidate.get("account_id"),
                    candidate.get(spec.account_id_field),
                    *(payload.get(field) for field in spec.id_fallback_fields),
                    *(candidate.get(field) for field in spec.id_fallback_fields),
                )
                if str(value or "").strip()
            ),
            "",
        )
        account_name = next(
            (
                str(value).strip()
                for value in (
                    payload.get("account_name"), candidate.get("account_name"),
                    candidate.get("username"), candidate.get("nickname"),
                )
                if str(value or "").strip()
            ),
            "",
        )
        return account_id, account_name

    def _activity_evidence_posts(
        self, request: Request, current_summary: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Collect and deduplicate review/crawl posts for the requested account."""
        platform = str(request.platform or "")
        if platform not in PLATFORM_SPECS:
            return []
        spec = PLATFORM_SPECS[platform]
        target_id, target_name = self._activity_request_identity(platform, request.payload)
        target_round = int(request.payload.get("round") or self.round_no)
        posts: list[dict[str, Any]] = []
        seen: set[str] = set()

        observations = list(self.completed)
        if current_summary is not None:
            observations.append({
                "phase": request.phase,
                "platform": request.platform,
                "request": request.payload,
                "summary": current_summary,
            })
        for completed in observations:
            if completed.get("phase") not in {"ACCOUNT_REVIEW", "POST_CRAWL"}:
                continue
            if str(completed.get("platform") or "") != platform:
                continue
            payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            if int(payload.get("round") or 1) != target_round:
                continue
            source_id, source_name = self._activity_request_identity(platform, payload)
            if target_id and source_id:
                if target_id != source_id:
                    continue
            elif target_name and source_name:
                if target_name != source_name:
                    continue
            else:
                continue

            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            diagnosis = extract_atomic_diagnosis(summary) or {}
            diagnosis_status = str(diagnosis.get("status") or "").upper()
            if (
                str(summary.get("status") or "").lower() in POST_FAIL_STATUSES
                or diagnosis_status in {"RETRY_WAIT", "FAILED_FINAL", "SKIPPED"}
            ):
                continue
            if platform == "xhs" and payload.get("stage") == "detail":
                candidates = [summary]
            else:
                candidates = _iter_items(summary)
            for post in candidates:
                post_id = next(
                    (str(post.get(field)).strip() for field in (*spec.item_id_fields, "post_id") if post.get(field)),
                    "",
                )
                key = post_id or str(post.get("href") or post.get("url") or "").strip()
                if not key:
                    key = stable_json({
                        "time": next((post.get(field) for field in PLATFORM_POST_TIME_FIELDS.get(platform, ()) if post.get(field) is not None), None),
                        "text": _review_post_text(post),
                    })
                if key in seen:
                    continue
                seen.add(key)
                posts.append(post)
        return posts

    def _capture_social_account_activity(self, request: Request, summary: dict[str, Any]) -> None:
        """Merge page/detail observations into one latest-post judgement per platform account and round."""
        if request.phase != "POST_CRAWL" or request.platform not in PLATFORM_SPECS:
            return
        platform = str(request.platform)
        spec = PLATFORM_SPECS[platform]
        evidence_posts = self._activity_evidence_posts(request)
        activity = (
            _account_activity(platform, evidence_posts)
            if evidence_posts
            else summary.get("account_activity") if isinstance(summary.get("account_activity"), dict) else {}
        )
        account_name = str(request.payload.get("account_name") or "unknown")
        account_id = str(request.payload.get("account_id") or request.payload.get(spec.account_id_field) or "")
        round_no = int(request.payload.get("round") or self.round_no)
        key = f"{round_no}:{platform}:{account_id or account_name}"
        candidate = {
            "platform": platform,
            "account_name": account_name,
            "account_id": account_id or None,
            "round": round_no,
            "account_role": request.payload.get("account_role"),
            "official_status": request.payload.get("official_status") or "unverified",
            "verification_confidence": request.payload.get("verification_confidence"),
            **activity,
        }
        if not candidate.get("latest_post_id") and request.payload.get("stage") == "detail":
            candidate["latest_post_id"] = str(request.payload.get("note_id") or "") or None
        previous = self.social_account_activity.get(key)
        if evidence_posts:
            # Evidence is rebuilt from all completed review/crawl observations,
            # so replacing avoids double-counting the same review posts or XHS details.
            self.social_account_activity[key] = candidate
            self._save_social_account_activity()
            return
        candidate_time = _parse_post_datetime(candidate.get("latest_post_time"))
        previous_time = _parse_post_datetime(previous.get("latest_post_time")) if previous else None
        total_checked = int((previous or {}).get("posts_checked") or 0) + int(candidate.get("posts_checked") or 0)
        total_with_time = int((previous or {}).get("posts_with_time") or 0) + int(candidate.get("posts_with_time") or 0)
        if previous is None or (candidate_time is not None and (previous_time is None or candidate_time > previous_time)):
            candidate["posts_checked"] = total_checked
            candidate["posts_with_time"] = total_with_time
            self.social_account_activity[key] = candidate
        else:
            previous["posts_checked"] = total_checked
            previous["posts_with_time"] = total_with_time
        self._save_social_account_activity()

    def _save_social_account_activity(self) -> None:
        state_dir = self.artifact_dir() / "POST_CRAWL" / "_state"
        state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "activity_window_days": ACCOUNT_ACTIVITY_WINDOW_DAYS,
            "accounts": sorted(
                self.social_account_activity.values(),
                key=lambda item: (int(item.get("round") or 0), str(item.get("platform") or ""), str(item.get("account_name") or "")),
            ),
        }
        (state_dir / "social_account_activity.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    def _report_social_account_activity(self, round_no: int) -> None:
        if not callable(self.log):
            return
        for key, activity in sorted(self.social_account_activity.items()):
            if key in self._reported_social_activity or int(activity.get("round") or 0) != round_no:
                continue
            self.log("SOCIAL_ACCOUNT_ACTIVITY", self, None, **activity)
            self._reported_social_activity.add(key)

    def _capture_domain_activity(self, request: Request, summary: dict[str, Any]) -> None:
        if request.phase != "WEB_CRAWL" or not isinstance(summary.get("domain_activity"), dict):
            return
        activity = dict(summary["domain_activity"])
        key = str(activity.get("domain") or request.payload.get("url") or "unknown")
        self.domain_activity[key] = activity
        state_dir = self.artifact_dir() / "WEB_CRAWL" / "_state"
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / "domain_activity.json").write_text(
            json.dumps({"domains": sorted(self.domain_activity.values(), key=lambda item: str(item.get("domain") or ""))},
                       ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def _queue_risk_analysis(self) -> None:
        if any(item.phase == "RISK_ANALYSIS" for item in self.requests):
            return
        evidence_path = self._risk_evidence_path()
        self.requests.append(Request(
            "RISK_ANALYSIS", None, {"evidence_file": str(evidence_path)},
            [self.python, _script_path("enterprise_risk_analysis.py"), self.name, str(evidence_path)],
            "LLM", "global"))

    def _artifact_ref(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(self.root.resolve())).replace("\\", "/")
        except (OSError, ValueError):
            return str(path)

    def _existing_evidence_screenshot(self, evidence: dict[str, Any]) -> str | None:
        artifact_value = str(evidence.get("artifact_path") or "").strip()
        if not artifact_value:
            return None
        artifact = Path(artifact_value)
        if not artifact.is_absolute():
            artifact = self.root / artifact
        base = artifact.parent
        result_path = base / "result.json"
        payload = json_load(result_path) if result_path.is_file() else {}
        if isinstance(payload, dict) and payload.get("screenshot_file"):
            evidence_url = str(evidence.get("url") or "").strip()
            page_urls = [
                str(payload.get(key) or "").strip()
                for key in (
                    "screenshot_url", "fetched_url", "final_url", "canonical_url",
                    "url", "profile_url", "search_url",
                )
                if str(payload.get(key) or "").strip()
            ]
            if evidence_url:
                expected = urlsplit(evidence_url)
                same_page = any(
                    (urlsplit(value).hostname or "").lower() == (expected.hostname or "").lower()
                    and urlsplit(value).path.rstrip("/") == expected.path.rstrip("/")
                    for value in page_urls
                )
                if not same_page:
                    return None
            candidate = Path(str(payload["screenshot_file"]))
            if not candidate.is_absolute():
                candidate = base / candidate
            if candidate.is_file():
                return self._artifact_ref(candidate)
        return None

    def _risk_reference_ids(self, analysis: dict[str, Any]) -> set[str]:
        if not analysis.get("overall_risk_found"):
            return set()
        referenced: set[str] = set()
        groups: list[Any] = [
            analysis.get("risk_keyword_findings"),
            analysis.get("risk_scenarios"),
            analysis.get("findings"),
            [analysis.get("gold_buyback")],
        ]
        for group in groups:
            for item in group or []:
                if not isinstance(item, dict):
                    continue
                one = str(item.get("evidence_id") or "").strip()
                if one:
                    referenced.add(one)
                referenced.update(str(value) for value in item.get("evidence_ids") or [] if str(value).strip())
        return referenced

    def _evidence_platform(self, evidence: dict[str, Any], url: str) -> str | None:
        platform = str(evidence.get("platform") or "").strip().lower()
        if platform in PLATFORMS:
            return platform
        host = (urlsplit(url).hostname or "").lower()
        if "xiaohongshu.com" in host:
            return "xhs"
        if "douyin.com" in host:
            return "douyin"
        if "kuaishou.com" in host:
            return "kuaishou"
        return None

    def _queue_risk_evidence(self, risk_summary: dict[str, Any]) -> bool:
        analysis = risk_summary.get("analysis") if isinstance(risk_summary.get("analysis"), dict) else {}
        reference_ids = self._risk_reference_ids(analysis)
        self.risk_evidence_screenshots = []
        evidence_items = risk_summary.get("evidence") if isinstance(risk_summary.get("evidence"), list) else []
        evidence_by_id = {
            str(item.get("evidence_id")): item for item in evidence_items
            if isinstance(item, dict) and str(item.get("evidence_id") or "")
        }
        grouped: dict[tuple[str | None, str], dict[str, Any]] = {}
        for evidence_id in sorted(reference_ids):
            evidence = evidence_by_id.get(evidence_id)
            if not evidence:
                self.risk_evidence_screenshots.append({
                    "evidence_ids": [evidence_id], "status": "evidence_not_found",
                })
                continue
            url = str(evidence.get("url") or "").strip()
            existing = self._existing_evidence_screenshot(evidence)
            if existing:
                self.risk_evidence_screenshots.append({
                    "evidence_ids": [evidence_id], "url": url or None,
                    "platform": evidence.get("platform"), "status": "existing",
                    "screenshot_path": existing, "source_artifact_path": evidence.get("artifact_path"),
                })
                continue
            if not url.startswith(("http://", "https://")):
                self.risk_evidence_screenshots.append({
                    "evidence_ids": [evidence_id], "url": url or None,
                    "platform": evidence.get("platform"), "status": "missing_url",
                    "source_artifact_path": evidence.get("artifact_path"),
                })
                continue
            platform = self._evidence_platform(evidence, url)
            key = (platform, canonicalize_url(url) or url)
            target = grouped.setdefault(key, {
                "evidence_ids": [], "url": url, "platform": platform,
                "source_artifact_paths": [],
            })
            target["evidence_ids"].append(evidence_id)
            if evidence.get("artifact_path"):
                target["source_artifact_paths"].append(evidence.get("artifact_path"))

        for (_, _), target in grouped.items():
            url = str(target["url"])
            platform = target.get("platform")
            if platform:
                command = [
                    self.python, _script_path("social_evidence_screenshot.py"),
                    "--platform", str(platform), "--url", url,
                ]
                resource_type, resource_key = "PLATFORM", str(platform)
            else:
                command = [self.python, _script_path("screenshot_crawler.py"), url]
                resource_type = "HOSTNAME"
                resource_key = (urlsplit(url).hostname or "unknown").lower()
            self.requests.append(Request(
                "RISK_EVIDENCE", platform, target, command, resource_type, resource_key,
            ))
        return bool(self.requests)

    def _finalize_risk_evidence(self) -> Path:
        entries = list(self.risk_evidence_screenshots)
        for completed in self.completed:
            if completed.get("phase") != "RISK_EVIDENCE":
                continue
            request_payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            summary = completed.get("summary") if isinstance(completed.get("summary"), dict) else {}
            screenshot = str(summary.get("screenshot_file") or "").strip()
            if screenshot and Path(screenshot).is_file():
                screenshot = self._artifact_ref(Path(screenshot))
            entries.append({
                "evidence_ids": request_payload.get("evidence_ids") or [],
                "url": request_payload.get("url"),
                "platform": request_payload.get("platform"),
                "status": "captured" if screenshot else str(summary.get("status") or "capture_failed"),
                "screenshot_path": screenshot or None,
                "source_artifact_paths": request_payload.get("source_artifact_paths") or [],
                "error": summary.get("error") or summary.get("reason"),
            })
        self.risk_evidence_screenshots = entries
        path = self.artifact_dir() / "RISK_EVIDENCE" / "_state" / "evidence_screenshots.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "schema_version": 1,
            "company": self.name,
            "generated_at": iso_utc(),
            "entries": entries,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        risk_summary = next((
            item.get("summary") for item in reversed(self.completed)
            if item.get("phase") == "RISK_ANALYSIS" and isinstance(item.get("summary"), dict)
        ), None)
        if isinstance(risk_summary, dict):
            enriched = copy.deepcopy(risk_summary)
            enriched["evidence_screenshots"] = entries
            (path.parent / "risk_analysis_with_screenshots.json").write_text(
                json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
        return path

    def _queue_round2_keyword(self, corpus_path: Path, corpus_kind: str = "round2_posts") -> None:
        if self.round2_started:
            return
        self.requests.append(Request(
            "KEYWORD", None, {
                "round": 2, "corpus_file": str(corpus_path), "corpus_kind": corpus_kind,
                "aliases": self._enterprise_alias_snapshot(),
            },
            [self.python, _script_path("social_keyword_extract.py"), self.name,
             "--round", "2", "--corpus-kind", corpus_kind, "--corpus-file", str(corpus_path)],
            "LLM", "global"))
        self.round2_started = True


class MainAgent:
    def __init__(self, input_file: Path, output_dir: Path, python: str | None = None,
                 db_path: Path | None = None,
                 replay: bool = False,
                 replay_companies_file: Path = DEFAULT_REPLAY_COMPANIES_FILE,
                 crawl_max_depth: int | None = DEFAULT_MAX_DEPTH,
                 crawl_max_pages: int | None = DEFAULT_MAX_PAGES,
                 full_crawl: bool = True) -> None:
        if crawl_max_depth is not None and crawl_max_depth < 0:
            raise ValueError("crawl_max_depth must be >= 0")
        if crawl_max_pages is not None and crawl_max_pages < 1:
            raise ValueError("crawl_max_pages must be >= 1")
        self.input_file = input_file.resolve()
        self.output_dir = output_dir.resolve()
        self.python = python or sys.executable
        self.crawl_max_depth = crawl_max_depth
        self.crawl_max_pages = crawl_max_pages
        self.full_crawl = full_crawl
        self.db = PipelineStore(db_path or (self.output_dir / "data" / "main_agent.db"))
        self.db.init()
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
        self.run_dir = self.output_dir / "runs" / self.run_id
        self.replay_enabled = replay
        self.replay_companies_file = replay_companies_file.resolve()
        self.replay_company_selectors: list[str] = []
        self.agents: dict[str, EnterpriseSubAgent] = {}
        self.ready: deque[str] = deque()
        self.last_grant: dict[tuple[str, str], float] = {}
        self.attempt_counts: dict[str, int] = {}
        self.platform_refreshing: dict[str, bool] = {p: False for p in PLATFORMS}
        # 各平台连续 login_required 次数：第 1 次只冷却复核（见 LOGIN_REFRESH_CONFIRM_COOLDOWN_SECONDS），
        # 冷却后再探仍报失效才 _ensure_login_refresh；任一平台任务成功即清零。
        self._login_required_strikes: dict[str, int] = {}
        self.login_refresh_interactive = bool(sys.stdin and sys.stdin.isatty())
        self.worker_count = 4
        self._resource_condition = threading.Condition()
        self._resource_active: dict[tuple[str, str], int] = {}
        # lane 熔断状态：未列=key 缺省 HEALTHY；COOLDOWN=瞬时故障停发、到点放探测；LOGIN_REFRESH=社媒登录失效、后台刷新中。
        self._lane_state: dict[tuple[str, str], str] = {}
        self._lane_probe_at: dict[tuple[str, str], float] = {}
        # lane 冷却/登录刷新等待的剩余时间播报节流（monotonic 秒，每 60s 一次）
        self._last_wait_report: float = 0.0
        self._released_tasks: set[str] = set()
        self._active_leases: dict[str, str] = {}
        self._active_requests: dict[str, tuple[str, str]] = {}
        # task_id -> control command payload. Workers poll this map while a
        # subprocess is running; the scheduler also uses it to suppress
        # fan-out when a task finishes concurrently with a skip command.
        self._manual_skips: dict[str, dict[str, Any]] = {}
        self._state_lock = threading.Lock()
        self._console_lock = threading.Lock()

    def console(self, event: str, agent: EnterpriseSubAgent | None = None,
                request: Request | None = None, **fields: Any) -> None:
        """Print business-facing lines; technical lease/task events remain in SQLite."""
        stamp = datetime.now().astimezone().strftime("%H:%M:%S")
        company = agent.name if agent is not None else "全局"
        phase = PHASE_LABELS.get(request.phase, request.phase) if request is not None else ""
        line: str | None = None
        if event == "RUN_STARTED":
            line = f"[{stamp}] [流水线启动] 共 {fields.get('fields', 0)} 家企业｜运行批次 {self.run_id}"
            if self.replay_enabled:
                line += "｜按企业复用最新历史结果"
        elif event == "WEB_LINK_FANOUT":
            line = (
                f"[{stamp}] [站内递归] {company}｜深度 {fields.get('depth', 0)}｜"
                f"发现链接 {fields.get('discovered', 0)}｜新派发 {fields.get('queued', 0)}｜"
                f"过滤：重复 {fields.get('duplicate_dropped', 0)}、外站 {fields.get('offsite_dropped', 0)}、"
                f"资源 {fields.get('asset_dropped', 0)}、深度 {fields.get('depth_dropped', 0)}、"
                f"页数 {fields.get('page_limit_dropped', 0)}｜"
                f"已见 URL {fields.get('seen', 0)}｜页面上限 {fields.get('page_limit', 0)}"
            )
        elif event == "WEB_CRAWL_SUMMARY":
            line = (
                f"[{stamp}] [官网抓取汇总] {company}｜页面 {fields.get('pages', 0)}｜"
                f"动态 {fields.get('dynamic', 0)}｜静态 {fields.get('static', 0)}｜"
                f"页面链接 {fields.get('links', 0)}｜图片候选 {fields.get('images_raw', 0)}→"
                f"去重 {fields.get('images_unique', 0)}→派发 {fields.get('images_queued', 0)}"
            )
        elif event == "HISTORY_REUSED":
            line = (
                f"[{stamp}] [历史复用] {company}｜{phase}｜来源批次 {fields.get('source_run')}｜"
                f"来源 attempt {fields.get('source_attempt')}"
            )
        elif event == "PHASE_PROGRESS":
            progress = fields.get("progress") if isinstance(fields.get("progress"), dict) else {}
            line = (
                f"[{stamp}] [进度] {company}｜{phase}｜当前任务 {progress.get('total', 0)}｜"
                f"已处理 {progress.get('done', 0)}｜进行中 {progress.get('running', 0)}｜"
                f"等待 {progress.get('waiting', 0)}｜失败 {progress.get('failed', 0)}｜"
                f"成功 {progress.get('success', 0)}｜空成功 {progress.get('empty_success', 0)}"
            )
            note = str(fields.get("note") or "").strip()
            if note:
                line += f"｜{note}"
        elif event == "RESULT_BRIEF":
            brief = str(fields.get("brief") or "").strip()
            if brief:
                line = f"[{stamp}] [关键结论] {company}｜{brief}"
        elif event == "ICP_SUMMARY":
            icp_summary = str(fields.get("summary") or "无结果").strip()
            line = f"[{stamp}] [关键结论] {company}｜ICP备案查询汇总｜{icp_summary}"
        elif event == "KEYWORD_DISTRIBUTION":
            dist = fields.get("distribution") if isinstance(fields.get("distribution"), dict) else {}
            keyword_counts = dist.get("keywords") if isinstance(dist.get("keywords"), dict) else {}
            platform_counts = dist.get("platforms") if isinstance(dist.get("platforms"), dict) else {}
            keyword_items = list(keyword_counts.items())
            keyword_text = "、".join(f"{key}×{value}" for key, value in keyword_items[:12]) or "无命中"
            if len(keyword_items) > 12:
                keyword_text += f" 等{len(keyword_items)}种"
            platform_items = list(platform_counts.items())
            platform_text = "、".join(
                f"{PLATFORM_LABELS.get(key, key)}×{value}" for key, value in platform_items[:6]
            ) or "无"
            target = str(fields.get("account") or "").strip()
            target_text = f"｜账号 {target}" if target else ""
            line = (
                f"[{stamp}] [关键词分布] {company}｜{phase}{target_text}｜文本 {dist.get('texts', 0)} 条｜"
                f"命中 {dist.get('hit_texts', 0)} 条｜未命中 {max(0, int(dist.get('texts', 0)) - int(dist.get('hit_texts', 0)))} 条｜"
                f"关键词：{keyword_text}｜平台词：{platform_text}"
            )
        elif event == "SOCIAL_ACCOUNT_ACTIVITY":
            account = str(fields.get("account_name") or "unknown")
            platform = PLATFORM_LABELS.get(str(fields.get("platform") or ""), str(fields.get("platform") or "未知平台"))
            post_time = str(fields.get("latest_post_time") or fields.get("latest_post_time_raw") or "未知")
            status = str(fields.get("activity_status") or "活跃状态未知")
            line = f"[{stamp}] [账号活跃度] {company}｜{platform}｜账号 {account}｜最新帖子时间 {post_time}｜{status}"
        elif event == "DOMAIN_ACTIVITY":
            domain = str(fields.get("domain") or "未知域名")
            status = str(fields.get("activity_status") or "活跃状态未知")
            access_type = str(fields.get("access_type") or "")
            suffix = "（疑似反爬）" if access_type == "suspected_anti_bot" else ""
            reason = str(fields.get("reason") or "")
            line = f"[{stamp}] [域名活跃度] {company}｜{domain}｜{status}{suffix}｜{reason}"
        elif event in {"LOGIN_REFRESH_START", "LOGIN_REFRESH_WAIT", "LOGIN_REFRESH_QUEUED",
                       "LOGIN_REFRESH_RETRY", "LOGIN_REFRESH_OK", "LOGIN_REFRESH_MISSING",
                       "LOGIN_REFRESH_DEFERRED"}:
            platform = PLATFORM_LABELS.get(str(fields.get("platform") or ""), str(fields.get("platform") or "未知平台"))
            action = {
                "LOGIN_REFRESH_START": "登录已失效，正在刷新",
                "LOGIN_REFRESH_WAIT": "已打开登录页；请扫码/验证，登录态变化会自动检测",
                "LOGIN_REFRESH_QUEUED": "等待前一个平台完成登录刷新",
                "LOGIN_REFRESH_RETRY": "登录尚未恢复，继续等待",
                "LOGIN_REFRESH_OK": "登录已恢复",
                "LOGIN_REFRESH_MISSING": "缺少登录刷新脚本",
                "LOGIN_REFRESH_DEFERRED": f"疑似限频误判为登录失效，先冷却 {fields.get('cooldown', 90)} 秒后复核，暂不刷新登录",
            }[event]
            line = f"[{stamp}] [平台状态] {platform}｜{action}"
        elif event in {"SCRIPT_DEFECT", "WORKER_FAILED", "SCRIPT_DEFECTS_FOUND"}:
            detail = fields.get("fix") or fields.get("error") or fields.get("see") or "请查看运行日志"
            line = f"[{stamp}] [异常] {company}{f'｜{phase}' if phase else ''}｜{detail}"
        elif event == "MANUAL_SKIP_APPLIED":
            line = (
                f"[{stamp}] [手动跳过] {company}｜{phase}｜"
                f"等待任务 {fields.get('queued', 0)} 个｜运行任务 {fields.get('running', 0)} 个｜"
                f"原因 {fields.get('reason') or 'operator_request'}"
            )
        elif event == "MANUAL_SKIP_REJECTED":
            line = (
                f"[{stamp}] [跳过未执行] {company}｜{phase or fields.get('phase', '')}｜"
                f"{fields.get('reason') or '当前环节不匹配'}"
            )
        elif event == "LANE_WAIT":
            line = f"[{stamp}] [等待中] {fields.get('waits', '')}"
        elif event == "RUN_FINISHED":
            line = f"[{stamp}] [流水线结束] 状态 {fields.get('status')}｜企业 {len(self.agents)} 家｜脚本缺陷 {fields.get('defects', 0)}"
        # RESOURCE/LEASE/ATTEMPT heartbeat 等技术事件默认不刷终端；均已持久化到 DB。
        if line is None:
            return
        with self._console_lock:
            print(line, flush=True)

    def _phase_progress(self, agent: EnterpriseSubAgent, phase: str) -> dict[str, int]:
        statuses = [str(status).upper() for status in agent.phase_status.get(phase, [])]
        success = statuses.count("SUCCEEDED")
        empty_success = statuses.count("EMPTY_SUCCESS")
        skipped = statuses.count("SKIPPED")
        failed = sum(status not in {"SUCCEEDED", "EMPTY_SUCCESS", "SKIPPED"} for status in statuses)
        waiting = sum(request.phase == phase for request in agent.requests)
        cid = company_id(agent.name)
        with self._state_lock:
            running = sum(active_cid == cid and active_phase == phase
                          for active_cid, active_phase in self._active_requests.values())
        done = success + empty_success + skipped + failed
        return {
            "total": done + running + waiting,
            "done": done,
            "running": running,
            "waiting": waiting,
            "failed": failed,
            "success": success,
            "empty_success": empty_success,
        }

    def _report_progress(self, agent: EnterpriseSubAgent, request: Request, note: str = "") -> None:
        self.console("PHASE_PROGRESS", agent, request, progress=self._phase_progress(agent, request.phase), note=note)

    def _icp_summary(self, agent: EnterpriseSubAgent) -> str:
        findings: dict[str, list[str]] = {carrier: [] for carrier in ICP_CARRIERS}
        for completed in agent.completed:
            if completed.get("phase") != "WEB_ICP":
                continue
            payload = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            carrier = str(payload.get("carrier") or "web")
            summary = completed.get("summary")
            for record in nested_records(summary):
                label = icp_record_label(record)
                if label not in findings.setdefault(carrier, []):
                    findings[carrier].append(label)
        parts: list[str] = []
        for carrier in ICP_CARRIERS:
            names = findings.get(carrier, [])
            label = ICP_CARRIER_LABELS.get(carrier, carrier)
            parts.append(f"{label} {len(names)} 条" + (f"（{_brief_items(names, 6)}）" if names else ""))
        return "｜".join(parts)

    def bootstrap(self) -> None:
        if self.replay_enabled:
            source_companies = self.db.historical_companies()
            if not source_companies:
                raise ValueError("没有可供重放的企业历史记录: 全部历史批次")
            if not self.replay_companies_file.is_file():
                # 旧批次记录的路径可能是重构前的项目根路径；回退到 config/ 下同名文件。
                fallback = _PROJECT_ROOT / "config" / self.replay_companies_file.name
                if fallback.is_file():
                    self.replay_companies_file = fallback
                else:
                    raise ValueError(f"重试企业清单不存在: {self.replay_companies_file}")
            self.replay_company_selectors = read_companies(self.replay_companies_file)
            companies = select_replay_companies(
                [(str(row["company_id"]), str(row["company_name"])) for row in source_companies],
                self.replay_company_selectors,
            )
            names = [name for _, name in companies]
        else:
            if not self.input_file.is_file():
                fallback = _PROJECT_ROOT / "config" / self.input_file.name
                if fallback.is_file():
                    self.input_file = fallback
                else:
                    raise ValueError(f"企业清单不存在: {self.input_file}")
            names = read_companies(self.input_file)
            companies = [(company_id(name), name) for name in names]
        self.run_dir.mkdir(parents=True, exist_ok=True)
        config = {
            "python": self.python,
            "phases": PHASES,
            "website_crawl": {
                "mode": "dynamic_then_static",
                "max_depth": self.crawl_max_depth,
                "max_pages": self.crawl_max_pages,
            },
        }
        if self.replay_enabled:
            config.update({
                "mode": "REPLAY",
                "replay_strategy": "latest_company_history",
                "replay_companies_file": str(self.replay_companies_file),
                "replay_company_selectors": self.replay_company_selectors,
                "replay_companies": names,
            })
        self.db.create_run(self.run_id, str(self.input_file), config, companies)
        account_seeds = load_social_account_seeds()
        seeded_aliases = 0
        for cid, name in companies:
            for platform, accounts in account_seeds.get(name, {}).items():
                for account in accounts:
                    account_name = str(account.get("account_name") or "").strip()
                    account_id_value = str(account.get("account_id") or "").strip()
                    if account_name and self.db.upsert_enterprise_alias(
                        cid, "account_name", account_name,
                        platform=platform,
                        source_type="user_confirmed_standard",
                        source_ref=str(DEFAULT_SOCIAL_ACCOUNT_SEEDS_FILE),
                        confidence=1.0,
                        is_confirmed=True,
                    ):
                        seeded_aliases += 1
                    if account_id_value and self.db.upsert_enterprise_alias(
                        cid, "platform_account_id", account_id_value,
                        platform=platform,
                        source_type="user_confirmed_standard",
                        source_ref=str(DEFAULT_SOCIAL_ACCOUNT_SEEDS_FILE),
                        confidence=1.0,
                        is_confirmed=True,
                    ):
                        seeded_aliases += 1
        for _, name in companies:
            agent = EnterpriseSubAgent(
                name, self.run_id, self.run_dir, self.python, self.db,
                web_max_depth=self.crawl_max_depth,
                web_max_pages=self.crawl_max_pages,
                full_crawl=self.full_crawl,
            )
            agent.log = self.console
            agent.queue_initial()
            self.agents[company_id(name)] = agent
            self.ready.append(company_id(name))
        self.db.event(uuid4().hex, "run", self.run_id, "run_started", {
            "companies": names,
            "input_file": str(self.input_file),
            "mode": "REPLAY" if self.replay_enabled else "PROCESS",
            "replay_strategy": "latest_company_history" if self.replay_enabled else None,
            "replay_companies": names if self.replay_enabled else None,
            "confirmed_social_account_aliases_seeded": seeded_aliases,
        })
        self.console("RUN_STARTED", fields=len(names))
        for agent in self.agents.values():
            if agent.requests:
                self._report_progress(agent, agent.requests[0], "已进入查询队列")

    def _attempt_dir(self, agent: EnterpriseSubAgent, request: Request, attempt_no: int) -> Path:
        path = agent.artifact_dir() / request.phase / f"task_{request.task_id}" / f"attempt_{attempt_no}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _request_fingerprints(self, agent: EnterpriseSubAgent, request: Request) -> tuple[str, str]:
        cid = company_id(agent.name)
        exact_payload = dict(request.payload)
        if request.empty_reason:
            exact_payload["__empty_reason"] = request.empty_reason
        exact = request_fingerprint(cid, request.phase, request.platform, exact_payload, hash_files=True)
        legacy = request_fingerprint(cid, request.phase, request.platform, request.payload, hash_files=False)
        return exact, legacy

    def _register_task(self, agent: EnterpriseSubAgent, request: Request,
                       store: PipelineStore | None = None) -> tuple[dict[str, Any], int, str]:
        """Durably register a request before any waiter, lease, or attempt child row."""
        target = store or self.db
        payload = dict(request.payload)
        payload["company"] = agent.name
        task_round = int(payload.get("round") or payload.get("round_no") or 1)
        exact_fingerprint, _ = self._request_fingerprints(agent, request)
        target.create_task(
            request.task_id, self.run_id, company_id(agent.name), request.phase,
            task_round, request.platform, payload, request_fingerprint=exact_fingerprint,
        )
        return payload, task_round, exact_fingerprint

    def _source_attempt_dir(self, source: dict[str, Any]) -> Path:
        runs_root = (self.output_dir / "runs").resolve()
        path = (
            runs_root
            / str(source["source_run_id"])
            / "companies"
            / safe_name(str(source["company_name"]))
            / str(source["phase"])
            / f"task_{source['task_id']}"
            / f"attempt_{source['attempt_no']}"
        ).resolve()
        try:
            path.relative_to(runs_root)
        except ValueError as exc:
            raise ValueError(f"history attempt escapes runs root: {path}") from exc
        return path

    def _prepare_history_reuse(self, agent: EnterpriseSubAgent, request: Request) -> None:
        if request.history_checked:
            return
        request.history_checked = True
        if not self.replay_enabled:
            return
        exact, legacy = self._request_fingerprints(agent, request)
        source = self.db.reusable_attempt(
            company_id(agent.name), request.phase, request.platform, exact, legacy,
            exclude_run_id=self.run_id,
        )
        if source is None:
            return
        source_dir = self._source_attempt_dir(source)
        if not source_dir.is_dir():
            return
        if request.command and not (source_dir / "result.json").is_file():
            return
        # 旧批次里可能存在“进程返回 0、原子脚本写 ok，但业务产物实际无效”的
        # 假成功。历史复用不能绕过当前总控的产物验收，否则修复后仍会继续沿用
        # 错误结果。这里只拒绝复用，随后按正常资源调度重新执行当前任务。
        try:
            source_summary = json.loads(str(source.get("summary_json") or "{}"))
        except json.JSONDecodeError:
            source_summary = {}
        if isinstance(source_summary, dict):
            quality = self._kuaishou_output_quality_gate(agent, request, source_summary)
            if quality and quality["status"] == "RETRY_WAIT":
                return
            # A conservative local fallback keeps the run moving during a
            # provider or response-format failure, but it is not equivalent to
            # a completed semantic risk analysis.  Replay it only by running
            # the current LLM path again, so a recovered provider can replace
            # the provisional result.
            if request.phase == "RISK_ANALYSIS":
                prior_analysis = (
                    source_summary.get("analysis")
                    if isinstance(source_summary.get("analysis"), dict)
                    else source_summary
                )
                if str(prior_analysis.get("analysis_mode") or "").strip() == "conservative_local_fallback":
                    return
        # IMAGE_DOWNLOAD 复用同样要过产物验收：旧批次假成功可能带损坏图片
        # （PIL 打不开的 .bin），复用会让 OCR 白重试。坏图不复用、重新下载。
        if request.phase == "IMAGE_DOWNLOAD":
            payload_files = [
                path for path in source_dir.rglob("*")
                if path.is_file() and path.suffix.lower() in {".bin", ".png", ".jpg", ".jpeg", ".webp", ".bmp"}
            ]
            if not payload_files or not any(_looks_like_image(path) for path in payload_files):
                return
        source["request_fingerprint"] = exact
        source["attempt_dir"] = str(source_dir)
        request.history_source = source

    def _materialize_history_attempt(self, source: dict[str, Any], attempt_dir: Path) -> None:
        """Copy a source attempt into this run without retaining source-path dependencies."""
        source_dir = Path(str(source["attempt_dir"])).resolve()
        stage = attempt_dir.parent / f".{attempt_dir.name}_history_{uuid4().hex}"
        try:
            shutil.copytree(source_dir, stage)
            # _attempt_dir created an empty target. Rename the complete staging
            # tree into place so a copy failure cannot leave a half-reused attempt.
            attempt_dir.rmdir()
            stage.replace(attempt_dir)
        finally:
            if stage.exists():
                shutil.rmtree(stage)

    def _lane_grantable(self, request: Request, now: float) -> bool:
        # 内存态 lane 判定：非阻塞、只读、与 DB 租约/调度队列完全隔离。
        # 三态分离设计——_lane_grantable + _acquire_lane 管"这轮能不能发"，
        # _release_resource/_release_lease 管"用完了归还"。三者在独立 code path 中，
        # 存在内存计数(_resource_active/last_grant)、DB 租约(acquire_resource_lease)、
        # 调度决策(_lane_grantable)三者不同步的窗口。保护分支越多，越容易一处改、另处漏。
        """非阻塞判定 lane 当前可否派发：无 lane→恒真；并发位已占→否；LOGIN_REFRESH→否；
        COOLDOWN→仅到探测点后是；HEALTHY(缺省)→看 gap 节流。只读不改。"""
        if request.history_source:
            return True
        if not request.resource_type or not request.resource_key:
            return True
        key = (request.resource_type, request.resource_key)
        with self._resource_condition:
            if self._resource_active.get(key, 0) >= 1:
                return False
            state = self._lane_state.get(key, "HEALTHY")
            if state == "LOGIN_REFRESH":
                return False
            if state == "COOLDOWN":
                return now >= self._lane_probe_at.get(key, 0.0)
            gap = RESOURCE_GAP_SECONDS.get(request.resource_type, 0.0)
            return (now - self.last_grant.get(key, 0.0)) >= gap

    def _acquire_lane(self, request: Request, now: float, store: PipelineStore) -> str | None:
        """假定可派发，落账占用 lane（内存并发位 + gap + DB lease）。无 lane 请求返回 None（视为已占、无 lease）。
        极少数情况下与 _lane_grantable 之间被别线程改了状态→返回 None 表示没占上，调用方把请求塞回队首。"""
        if request.history_source:
            return None
        if not request.resource_type or not request.resource_key:
            return None
        key = (request.resource_type, request.resource_key)
        with self._resource_condition:
            if self._resource_active.get(key, 0) >= 1:
                return None
            state = self._lane_state.get(key, "HEALTHY")
            if state == "LOGIN_REFRESH":
                return None
            if state == "COOLDOWN":
                if now < self._lane_probe_at.get(key, 0.0):
                    return None
            else:
                gap = RESOURCE_GAP_SECONDS.get(request.resource_type, 0.0)
                if (now - self.last_grant.get(key, 0.0)) < gap:
                    return None
            previous_last_grant = self.last_grant.get(key)
            self._resource_active[key] = self._resource_active.get(key, 0) + 1
            self.last_grant[key] = now
            self._released_tasks.discard(request.task_id)
        waiter_id = uuid4().hex
        lease_id = uuid4().hex
        expires = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(timespec="seconds")
        lease_committed = False
        try:
            store.acquire_resource_lease(
                waiter_id, lease_id, request.task_id,
                request.resource_type, request.resource_key, 1, expires,
            )
            lease_committed = True
            self.console("RESOURCE_QUEUED", request=request, resource=f"{request.resource_type}:{request.resource_key}", waiter=waiter_id)
            store.event(uuid4().hex, "task", request.task_id, "lease_granted", {"lease_id": lease_id, "resource": list(key)})
            self.console("LEASE_GRANTED", request=request, resource=f"{request.resource_type}:{request.resource_key}", lease=lease_id)
        except Exception as exc:
            # Roll back process-owned state even if post-commit audit/console reporting fails.
            if lease_committed:
                try:
                    store.release_lease(lease_id, "ACQUIRE_ABORTED")
                    store.set_resource(request.resource_type, request.resource_key, "AVAILABLE", 1)
                except Exception as cleanup_exc:  # Preserve the triggering error and attach cleanup evidence.
                    if hasattr(exc, "add_note"):
                        exc.add_note(f"lease cleanup also failed: {cleanup_exc}")
            with self._resource_condition:
                active = self._resource_active.get(key, 0)
                if active <= 1:
                    self._resource_active.pop(key, None)
                else:
                    self._resource_active[key] = active - 1
                if previous_last_grant is None:
                    self.last_grant.pop(key, None)
                else:
                    self.last_grant[key] = previous_last_grant
                self._resource_condition.notify_all()
            raise
        return lease_id

    def _set_lane(self, key: tuple[str, str], state: str, probe_at: float | None = None) -> None:
        """写 lane 熔断态并广播。HEALTHY→清掉记录（回归缺省）；COOLDOWN 带 probe_at；LOGIN_REFRESH 不带。"""
        with self._resource_condition:
            if state == "HEALTHY":
                self._lane_state.pop(key, None)
                self._lane_probe_at.pop(key, None)
            else:
                self._lane_state[key] = state
                if probe_at is not None:
                    self._lane_probe_at[key] = probe_at
                else:
                    self._lane_probe_at.pop(key, None)
            self._resource_condition.notify_all()

    def _report_lane_waits(self, now: float) -> None:
        """lane 处于冷却/登录刷新等待时，每 60 秒报一次剩余等待时间。"""
        with self._resource_condition:
            cooldowns = [
                (key, self._lane_probe_at.get(key, 0.0) - now)
                for key, state in self._lane_state.items()
                if state == "COOLDOWN"
            ]
            refreshing = [key for key, state in self._lane_state.items() if state == "LOGIN_REFRESH"]
        if not cooldowns and not refreshing:
            self._last_wait_report = 0.0  # 等待结束即复位，下次进入等待立即播报
            return
        if now - self._last_wait_report < 60.0:
            return
        self._last_wait_report = now
        parts = [
            f"{key[0]}/{key[1]} 重试等待剩余 {max(0, int(remaining))} 秒"
            for key, remaining in cooldowns
        ]
        parts.extend(f"{key[0]}/{key[1]} 等待登录刷新" for key in refreshing)
        self.console("LANE_WAIT", waits="；".join(parts))

    def _release_resource(self, request: Request) -> None:
        if not request.resource_type or not request.resource_key:
            return
        key = (request.resource_type, request.resource_key)
        with self._resource_condition:
            if request.task_id in self._released_tasks:
                return
            self._released_tasks.add(request.task_id)
            active = self._resource_active.get(key, 0)
            if active <= 1:
                self._resource_active.pop(key, None)
            else:
                self._resource_active[key] = active - 1
            self._resource_condition.notify_all()

    def _release_lease(self, store: PipelineStore, request: Request, lease_id: str | None) -> None:
        """Release the DB lease and memory guard from one ownership point."""
        if not lease_id:
            return
        try:
            store.release_lease(lease_id)
            store.set_resource(request.resource_type or "", request.resource_key or "", "AVAILABLE", 1)
        finally:
            self._release_resource(request)

    def _command_for(self, request: Request, attempt_dir: Path) -> list[str] | None:
        if not request.command:
            return None
        command = list(request.command)
        if request.phase in {"WEB_ICP", "WEB_SEARCH"}:
            command += ["--output-file", str(attempt_dir / "result.json")]
        elif request.phase == "WEB_CRAWL":
            script_name = Path(str(command[1])).name if len(command) > 1 else ""
            if script_name == "site_crawler.py":
                command += ["--output-dir", str(attempt_dir), "--output-file", str(attempt_dir / "result.json")]
            else:
                command += [
                    "--screenshot-dir", str(attempt_dir / "screenshots"),
                    "--json-dir", str(attempt_dir),
                    "--json-file", str(attempt_dir / "result.json"),
                ]
        elif request.phase == "IMAGE_DOWNLOAD":
            command += ["--output-dir", str(attempt_dir / "images"), "--output-file", str(attempt_dir / "image.bin"),
                        "--result-file", str(attempt_dir / "result.json")]
        elif request.phase == "OCR":
            command += ["--output-file", str(attempt_dir / "result.json"),
                        "--text-file", str(attempt_dir / "result.txt")]
        elif request.phase in {"ACCOUNT_DISCOVERY", "ACCOUNT_RANK", "ACCOUNT_VERIFY"}:
            if request.phase == "ACCOUNT_RANK":
                accounts_path = attempt_dir / "platform_accounts.json"
                accounts = request.payload.get("platforms") if isinstance(request.payload, dict) else {}
                aliases = request.payload.get("aliases") if isinstance(request.payload, dict) else []
                accounts_path.write_text(stable_json({
                    "schema_version": 2,
                    "platforms": accounts if isinstance(accounts, dict) else {},
                    "aliases": aliases if isinstance(aliases, list) else [],
                    "guard_version": int(request.payload.get("guard_version") or 1),
                }), encoding="utf-8")
                # 位置参数 JSON 必须插在公司名之后、可选 flag 之前，否则 argparse 拒绝。
                command = command[:3] + [str(accounts_path)] + command[3:]
            elif request.phase == "ACCOUNT_VERIFY":
                evidence_path = attempt_dir / "review_evidence.json"
                evidence = request.payload.get("platforms") if isinstance(request.payload, dict) else {}
                aliases = request.payload.get("aliases") if isinstance(request.payload, dict) else []
                evidence_path.write_text(stable_json({
                    "schema_version": 2,
                    "platforms": evidence if isinstance(evidence, dict) else {},
                    "aliases": aliases if isinstance(aliases, list) else [],
                    "ownership_gate_version": int(request.payload.get("ownership_gate_version") or 1),
                }), encoding="utf-8")
                command = command[:3] + [str(evidence_path)] + command[3:]
            command += ["--output-file", str(attempt_dir / "result.json"), "--output-dir", str(attempt_dir)]
            if request.phase == "ACCOUNT_DISCOVERY":
                command += ["--screenshot-file", str(attempt_dir / "screenshots" / "search.jpg")]
        elif request.phase == "KEYWORD":
            input_path = attempt_dir / "keyword_input.json"
            input_path.write_text(stable_json(request.payload if isinstance(request.payload, dict) else {}), encoding="utf-8")
            command = command[:3] + [str(input_path)] + command[3:]
            command += ["--output-file", str(attempt_dir / "result.json"), "--output-dir", str(attempt_dir)]
        elif request.phase in {"ACCOUNT_REVIEW", "POST_CRAWL"}:
            command += ["--output-file", str(attempt_dir / "result.json"), "--output-dir", str(attempt_dir)]
            script_name = Path(str(command[1])).name if len(command) > 1 else ""
            if request.phase == "ACCOUNT_REVIEW":
                command += ["--screenshot-file", str(attempt_dir / "screenshots" / "profile.jpg")]
            elif script_name == "xhs_detail.py":
                command += ["--screenshot-file", str(attempt_dir / "screenshots" / "post.jpg")]
            elif request.phase == "POST_CRAWL" and script_name in {"xhs_posts.py", "kuaishou_posts.py"}:
                # 全量滚动的中途 checkpoint：任务级路径（跨 attempt 共享），超时 kill 后重试从已收 ids 恢复。
                command += ["--resume-file", str(attempt_dir.parent / "resume_ids.txt")]
        elif request.phase == "RISK_ANALYSIS":
            command += ["--output-file", str(attempt_dir / "result.json"), "--output-dir", str(attempt_dir)]
        elif request.phase == "RISK_EVIDENCE":
            script_name = Path(str(command[1])).name if len(command) > 1 else ""
            if script_name == "screenshot_crawler.py":
                command += [
                    "--screenshot-dir", str(attempt_dir / "screenshots"),
                    "--json-dir", str(attempt_dir),
                    "--json-file", str(attempt_dir / "result.json"),
                ]
            else:
                command += [
                    "--screenshot-file", str(attempt_dir / "screenshots" / "evidence.jpg"),
                    "--output-file", str(attempt_dir / "result.json"),
                ]
        return command

    def _summary_status(self, summary: Any) -> str:
        if isinstance(summary, dict):
            diagnosis = extract_atomic_diagnosis(summary)
            if isinstance(diagnosis, dict) and diagnosis.get("summary_status"):
                return str(diagnosis["summary_status"]).strip().lower()
            status = str(summary.get("status") or "").strip().lower()
            if status:
                return status
            nested = summary.get("summary")
            if isinstance(nested, dict):
                return str(nested.get("status") or "").strip().lower()
        return ""

    def _kuaishou_output_quality_gate(
        self,
        agent: EnterpriseSubAgent,
        request: Request,
        summary: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Validate successful Kuaishou/XHS/Douyin review/crawl output at the orchestration boundary.

        A child process exit code only proves that the script ran; it does not prove
        that the platform returned the requested profile.  In particular,
        ``/rest/v/profile/get`` can describe the logged-in user while the requested
        feed is empty, and Douyin's page API returns whatever the signed session
        yields.  The main agent therefore verifies target identity and checks an
        empty result against the account-review evidence before allowing the state
        machine to advance.

        Scope: kuaishou/xhs ``home`` stage (their single-pass scroll crawl) and
        douyin ``page`` stage (douyin's review and pagination both run per page).

        ``None`` means the reported success is usable.  A returned mapping overrides
        the child diagnosis with either a retryable integrity failure or a verified
        empty success.
        """
        if (
            request.phase not in {"ACCOUNT_REVIEW", "POST_CRAWL"}
            or request.platform not in {"kuaishou", "xhs", "douyin"}
        ):
            return None
        stage = str(request.payload.get("stage") or "home")
        if request.platform == "douyin":
            if stage != "page":
                return None
        elif stage != "home":
            return None

        spec = PLATFORM_SPECS.get(str(request.platform or ""))
        expected_id = str(
            request.payload.get(spec.account_id_field if spec else "kwai_id")
            or request.payload.get("account_id")
            or ""
        ).strip()
        items = _iter_items(summary)
        # Douyin requests use ``sec_uid``; each returned author also has a
        # separate numeric ``uid``.  Keep its comparable ids and numeric ids in
        # separate sets, otherwise a correct response looks like a multi-author
        # feed.
        author_ids: set[str] = set()
        author_numeric_ids: set[str] = set()
        for item in items:
            author = item.get("author") if isinstance(item.get("author"), dict) else {}
            if request.platform == "douyin":
                sec_uid = str(author.get("sec_uid") or "").strip()
                if sec_uid:
                    author_ids.add(sec_uid)
                # Preserve native numeric identifiers as audit evidence, but do
                # not compare them with the requested sec_uid.
                for value in (
                    item.get("author_id"),
                    item.get("author_user_id"),
                    item.get("user_id"),
                    author.get("uid"),
                    author.get("author_id"),
                ):
                    text = str(value or "").strip()
                    if text:
                        author_numeric_ids.add(text)
                continue
            for value in (
                item.get("author_id"),
                item.get("user_id"),
                author.get("sec_uid"),
                author.get("uid"),
                author.get("author_id"),
            ):
                text = str(value or "").strip()
                if text:
                    author_ids.add(text)
        profile = summary.get("profile") if isinstance(summary.get("profile"), dict) else {}
        profile_ids = {
            str(profile.get(field) or "").strip()
            for field in ("eid", "kwai_id", "account_id", "user_id", "user_define_id")
            if str(profile.get(field) or "").strip()
        }
        # Douyin's page script records the final profile URL. Kuaishou also
        # reports a URL, but it is identity evidence only after its script has
        # confirmed the visible page metadata; an error page can retain the
        # requested URL while showing no account.
        page_profile = summary.get("page_profile") if isinstance(summary.get("page_profile"), dict) else {}
        profile_url = str(
            summary.get("profile_url")
            or (summary.get("screenshot_url") if request.platform == "kuaishou" else "")
            or ""
        ).strip()
        profile_page_confirmed = bool(
            summary.get("profile_page_confirmed") or page_profile.get("profile_page_confirmed")
        )
        url_is_identity_evidence = (
            request.platform != "kuaishou" or profile_page_confirmed
        )
        if expected_id and profile_url and expected_id in profile_url and url_is_identity_evidence:
            profile_ids.add(expected_id)
        prior_posts = agent._activity_evidence_posts(request)
        command = [str(part) for part in (request.command or [])]
        has_incremental_anchor = request.phase == "POST_CRAWL" and "--existing-ids" in command
        identity_verified = bool(expected_id and (expected_id in author_ids or expected_id in profile_ids))
        try:
            feed_count = int(summary.get("feed_count") or 0)
        except (TypeError, ValueError):
            feed_count = 0
        evidence = {
            "expected_account_id": expected_id or None,
            "observed_author_ids": sorted(author_ids),
            "observed_author_numeric_ids": sorted(author_numeric_ids),
            "observed_profile_ids": sorted(profile_ids),
            "item_count": len(items),
            "review_post_count": len(prior_posts),
            "feed_count": feed_count,
            "stopped_early": bool(summary.get("stopped_early")),
            "has_incremental_anchor": has_incremental_anchor,
            "profile_url": profile_url or None,
            "profile_page_confirmed": profile_page_confirmed,
        }

        def retry(code: str, reason: str) -> dict[str, Any]:
            return {
                "status": "RETRY_WAIT",
                "error_code": code,
                "reason": reason,
                "source": "main_agent_kuaishou_quality_gate",
                "evidence": evidence,
            }

        if not expected_id:
            return retry("invalid_input", "social post crawl has no target account id")

        if items:
            # Feed author ids are the strongest identity evidence.  A mixed or
            # different-author feed must never become enterprise risk evidence.
            if author_ids and author_ids != {expected_id}:
                return retry(
                    "target_identity_mismatch",
                    "crawled items belong to an account other than the requested target",
                )
            # DOM fallback items have no author id.  In that case an explicitly
            # contradictory profile is enough to reject the response; an absent
            # profile is tolerated because the visible target page still yielded posts.
            if not author_ids and profile_ids and expected_id not in profile_ids:
                return retry(
                    "target_identity_mismatch",
                    "profile identity does not match the requested target",
                )
            return None

        # Incremental crawling may legitimately return no *new* posts when the first
        # target-author item is an existing anchor.  Require verified target identity
        # so a logged-in-user response cannot masquerade as that condition.
        if has_incremental_anchor and summary.get("stopped_early") and identity_verified:
            return {
                "status": "EMPTY_SUCCESS",
                "error_code": None,
                "reason": "target reached an existing post anchor; no new posts",
                "source": "main_agent_kuaishou_quality_gate",
                "evidence": evidence,
            }

        # 全部可见帖子都命中 review 样本跳过集（skipped>0 且零收集）：
        # 小账号的帖子在复核阶段已抓全，属合法"无新增"，不能按不完整无限重试。
        # 跳过集是按账号聚合的复核样本 ID，页面上能命中它们本身就证明了身份，
        # 因此 xhs（无 author/profile 元数据）也适用。
        try:
            skipped_count = int(summary.get("skipped") or 0)
        except (TypeError, ValueError):
            skipped_count = 0
        if skipped_count > 0:
            return {
                "status": "EMPTY_SUCCESS",
                "error_code": None,
                "reason": "all visible posts were already collected as review samples",
                "source": "main_agent_kuaishou_quality_gate",
                "evidence": {**evidence, "skipped": skipped_count},
            }

        # ACCOUNT_REVIEW just observed posts for this same account and round.  A later
        # zero-item result is therefore an incomplete/transient response, not a
        # valid empty account.
        if prior_posts:
            return retry(
                "post_collection_incomplete",
                "post crawl returned zero items although account review observed target posts",
            )
        # xhs 卡片不暴露 author_id、主页无 profile 元数据，无法做身份校验；
        # 0 条且 review 无证据时按已验证空账号处理（真空账号或 review/crawl 均 0 条）。
        if request.platform != "xhs":
            if profile_ids and expected_id not in profile_ids:
                return retry(
                    "target_identity_mismatch",
                    "empty response contains the logged-in or another account profile",
                )
            if not identity_verified:
                return retry(
                    "target_identity_unverified",
                    "returned zero items without evidence for the requested account",
                )

        # The requested profile was positively identified and neither review nor the
        # current crawl found posts.  This is the only ordinary zero-item success.
        return {
            "status": "EMPTY_SUCCESS",
            "error_code": None,
            "reason": "target identity verified and no posts were found",
            "source": "main_agent_kuaishou_quality_gate",
            "evidence": evidence,
        }

    def _summarize_result(self, request: Request, result: dict[str, Any]) -> str | None:
        # 状态展示层（仅供 console/progress 输出；不做判定、不改状态）。
        if str(result.get("status") or "").upper() not in {"SUCCEEDED", "EMPTY_SUCCESS"}:
            return None
        summary = result.get("summary") if isinstance(result, dict) else None
        if request.phase == "WEB_ICP":
            carrier = str(request.payload.get("carrier") or "web")
            carrier_label = ICP_CARRIER_LABELS.get(carrier, carrier)
            records = nested_records(summary)
            names = [icp_record_label(record) for record in records]
            if names:
                return f"ICP网络载体｜{carrier_label}查询到{len(names)}条：{_brief_items(names, 6)}"
            return f"ICP网络载体｜{carrier_label}未查询到结果"
        if request.phase == "WEB_SEARCH":
            candidates = summary.get("candidates") if isinstance(summary, dict) else []
            candidates = candidates if isinstance(candidates, list) else []
            domains = [str(item.get("domain") or item.get("url") or "") for item in candidates if isinstance(item, dict)]
            carrier_candidates = summary.get("carrier_candidates") if isinstance(summary, dict) else []
            carrier_candidates = carrier_candidates if isinstance(carrier_candidates, list) else []
            if domains or carrier_candidates:
                return (
                    f"互联网载体候选｜官网{len(domains)}个"
                    f"｜其他载体线索{len(carrier_candidates)}个"
                    f"｜{_brief_items(domains, 5) if domains else '无官网候选'}"
                )
            return "互联网载体候选｜未找到高置信候选"
        if request.phase == "WEB_CRAWL":
            if isinstance(summary, dict):
                function = str(summary.get("fetch_function") or "unknown")
                status_code = summary.get("status_code")
                final_url = str(summary.get("fetched_url") or summary.get("url") or request.payload.get("url") or "")
                depth = request.payload.get("depth", 0)
                return (
                    f"官网页面｜深度 {depth}｜HTTP {status_code if status_code is not None else '未知'}｜"
                    f"函数 {function}｜{final_url}"
                )
            return "官网页面｜抓取完成"
        if request.phase == "OCR":
            text = ""
            if isinstance(summary, dict):
                text = _pick_text(summary.get("text")) or _pick_text(summary.get("text_preview")) or _pick_text(summary.get("recognized_text"))
            if not text and isinstance(summary, dict):
                text = _pick_text(summary)
            return f"OCR文本: {text[:80]}" if text else "OCR文本: 无"
        if request.phase == "KEYWORD":
            if result.get("status") == "EMPTY_SUCCESS":
                return "LLM账号搜索关键词｜没有新增关键词，跳过重复搜索"
            if isinstance(summary, dict):
                keywords = summary.get("keywords")
                if isinstance(keywords, dict):
                    brief = "｜".join(
                        f"{PLATFORM_LABELS.get(k, k)}：{'、'.join(str(x) for x in (v if isinstance(v, list) else [v]) if str(x).strip())[:60]}"
                        for k, v in keywords.items() if v
                    )
                    if brief:
                        return f"第{int(request.payload.get('round') or 1)}轮 LLM账号搜索关键词｜{brief}"
            return "LLM账号搜索关键词｜无有效结果"
        if request.phase == "ACCOUNT_DISCOVERY":
            platform = PLATFORM_LABELS.get(request.platform or "", request.platform or "未知平台")
            accounts: list[str] = []
            if isinstance(summary, dict):
                records = summary.get("records") or summary.get("accounts") or []
                if isinstance(records, list):
                    accounts = [_pick_name(item) for item in records if isinstance(item, dict)]
            accounts = [a for a in accounts if a]
            return f"{platform}账号: {_brief_items(accounts, 4)}" if accounts else f"{platform}账号: 无"
        if request.phase == "ACCOUNT_RANK":
            if isinstance(summary, dict):
                results = summary.get("results")
                if isinstance(results, dict):
                    parts: list[str] = []
                    for platform, value in results.items():
                        if not isinstance(value, dict):
                            continue
                        names: list[str] = []
                        reasons: list[str] = []
                        for item in value.get("top_accounts") or []:
                            if isinstance(item, dict):
                                name = _pick_name(item)
                                if name:
                                    names.append(name)
                                reason = _pick_text(item.get("reason"))
                                if reason:
                                    reasons.append(reason)
                        if names:
                            brief = _brief_items(names, 3)
                            if reasons:
                                brief = f"{brief} [{_brief_items(reasons, 1)}]"
                            parts.append(f"{PLATFORM_LABELS.get(platform, platform)}：{brief}")
                        elif value.get("candidate_count") is not None:
                            parts.append(f"{PLATFORM_LABELS.get(platform, platform)}：入围0（候选{value.get('candidate_count')}）")
                    if parts:
                        return "账号复核候选｜" + "｜".join(parts)
            return "账号复核候选｜无"
        if request.phase == "ACCOUNT_REVIEW":
            platform = PLATFORM_LABELS.get(request.platform or "", request.platform or "未知平台")
            account = _pick_text(request.payload.get("account_name"))
            compact = _compact_account_review(summary if isinstance(summary, dict) else {})
            titles = [
                _pick_text(item.get("title"))
                for item in compact.get("posts") or []
                if isinstance(item, dict) and _pick_text(item.get("title"))
            ]
            return (
                f"账号样本复核｜{platform}｜{account or 'unknown'}｜"
                f"取样 {len(titles)} 条" + (f"｜{_brief_items(titles, 2)}" if titles else "")
            )
        if request.phase == "ACCOUNT_VERIFY":
            results = summary.get("results") if isinstance(summary, dict) else None
            if not isinstance(results, dict):
                return "账号归属定级｜无有效结果"
            parts: list[str] = []
            for platform, platform_result in results.items():
                if not isinstance(platform_result, dict):
                    continue
                accepted = platform_result.get("accepted_accounts") or []
                rejected = platform_result.get("rejected_accounts") or []
                names = [_pick_name(item) for item in accepted if isinstance(item, dict)]
                parts.append(
                    f"{PLATFORM_LABELS.get(platform, platform)}：通过{len(accepted)}、拒绝{len(rejected)}"
                    + (f"（{_brief_items([name for name in names if name], 3)}）" if names else "")
                )
            return "账号归属定级｜" + "｜".join(parts) if parts else "账号归属定级｜无"
        if request.phase == "POST_CRAWL":
            platform = PLATFORM_LABELS.get(request.platform or "", request.platform or "未知平台")
            account = _pick_text(request.payload.get("account_name")) if isinstance(request.payload, dict) else ""
            status_text = self._summary_status(summary)
            if request.platform == "xhs" and request.payload.get("stage") == "detail" and isinstance(summary, dict):
                title = _pick_text(summary.get("title"))
                desc = _pick_text(summary.get("desc"))
                activity = summary.get("account_activity") if isinstance(summary.get("account_activity"), dict) else {}
                post_time = _pick_text(activity.get("latest_post_time")) or _pick_text(summary.get("time")) or "未知"
                text = title or desc[:60] or "正文为空"
                return f"{platform}帖子正文已获取｜账号 {account or 'unknown'}｜{text[:60]}｜帖子时间 {post_time}"
            items = summary if isinstance(summary, list) else _iter_items(summary)
            if items:
                sample = _pick_text(items[0]) if isinstance(items[0], dict) else str(items[0])
                return f"{platform}作品: {account or 'unknown'} {len(items)}条 {sample[:60]}"
            if status_text:
                return f"{platform}作品: {account or 'unknown'} {status_text}"
            return f"{platform}作品: {account or 'unknown'} 无"
        if request.phase == "RISK_EVIDENCE":
            url = _pick_text(request.payload.get("url"))
            screenshot = _pick_text(summary.get("screenshot_file")) if isinstance(summary, dict) else ""
            status_text = self._summary_status(summary)
            if screenshot:
                return f"风险证据截图｜已保存｜{url or screenshot}"
            return f"风险证据截图｜{status_text or '未获取'}｜{url or '无URL'}"
        if request.phase == "RISK_ANALYSIS":
            analysis = summary.get("analysis") if isinstance(summary, dict) else None
            if not isinstance(analysis, dict):
                return "最终风险判定｜无有效分析结果"
            if analysis.get("analysis_mode") == "conservative_local_fallback":
                if _pick_text(analysis.get("analysis_warning_code")) == "response_format_invalid":
                    verdict = "LLM 输出格式无效，风险语义判定待复核"
                else:
                    verdict = "LLM 不可用，风险语义判定待复核"
            else:
                verdict = "发现风险线索" if analysis.get("overall_risk_found") else "未发现明确风险线索"
            executive_summary = _pick_text(analysis.get("executive_summary"))
            risk_terms: list[str] = []
            for item in analysis.get("risk_keyword_findings") or []:
                if isinstance(item, dict):
                    risk_terms.extend(str(term) for term in item.get("keywords") or [] if str(term).strip())
            scenarios = [
                str(item.get("scenario_id"))
                for item in analysis.get("risk_scenarios") or []
                if isinstance(item, dict) and str(item.get("present") or "").lower() == "yes"
            ]
            details: list[str] = [verdict]
            if risk_terms:
                details.append(f"涉及词：{_brief_items(list(dict.fromkeys(risk_terms)), 8)}")
            if scenarios:
                details.append(f"命中场景：{', '.join(scenarios)}")
            if executive_summary:
                details.append(executive_summary[:200])
            return "最终风险判定｜" + "｜".join(details)
        return None

    def _refresh_script_for(self, platform: str | None) -> Path | None:
        if not platform:
            return None
        script = PLATFORM_REFRESH_SCRIPTS.get(platform)
        # Login refresh scripts live with the social collectors.  Resolve them
        # through the same registry used for every child-process command so a
        # login failure launches the actual script instead of leaving the lane
        # stuck in LOGIN_REFRESH.
        return Path(_script_path(script)) if script else None

    def _ensure_login_refresh(self, platform: str) -> None:
        """社媒登录失效：置该平台 lane 为 LOGIN_REFRESH（停发许可），并启一个后台刷新线程循环到成功。
        刷新"只有成功/没回复"：子进程非 0 退出或超时都视为"还没刷完"，重启续刷，永不返回失败。
        platform_refreshing 互斥保证同平台同时只有一个刷新线程。"""
        self._set_lane(("PLATFORM", platform), "LOGIN_REFRESH")
        with self._state_lock:
            if self.platform_refreshing.get(platform):
                return
            self.platform_refreshing[platform] = True
        script = self._refresh_script_for(platform)
        if script is None or not script.is_file():
            self.console("LOGIN_REFRESH_MISSING", platform=platform)
            # Do not leave all work for this platform permanently blocked when
            # deployment is incomplete.  Let the failed request use its normal
            # retry path, which remains durable and visible in the run record.
            self._set_lane(("PLATFORM", platform), "HEALTHY")
            with self._state_lock:
                self.platform_refreshing[platform] = False
            return
        self.console("LOGIN_REFRESH_START", platform=platform, script=script.name)
        threading.Thread(target=self._login_refresh_runner, args=(platform, script), daemon=True).start()

    def _login_refresh_runner(self, platform: str, script: Path) -> None:
        try:
            attempt = 0
            while True:
                attempt += 1
                returncode, stdout, stderr = self._run_refresh_once(platform, script)
                self._write_refresh_log(platform, attempt, returncode, stdout, stderr)
                if returncode == 0:
                    break  # 成功
                # 没刷完（脚本崩 / 超时 / 人还没在浏览器里登进来）→ 续刷，不计失败
                time.sleep(5)
            self._set_lane(("PLATFORM", platform), "HEALTHY")
            self.console("LOGIN_REFRESH_OK", platform=platform, attempts=attempt)
        finally:
            with self._state_lock:
                self.platform_refreshing[platform] = False

    def _run_refresh_once(self, platform: str, script: Path) -> tuple[int, str, str]:
        cmd = [self.python, str(script), "--expect-login"]
        if self.login_refresh_interactive:
            cmd.append("--interactive-wait")
            # Refresh scripts do not read stdin: each platform may keep its own
            # QR browser open concurrently and poll its local browser state.
            self.console("LOGIN_REFRESH_WAIT", platform=platform)
        try:
            proc = subprocess.run(cmd, cwd=_SCRIPT_DIR, capture_output=True, text=True,
                                  stdin=subprocess.DEVNULL, timeout=LOGIN_REFRESH_TIMEOUT)
            return proc.returncode, proc.stdout or "", proc.stderr or ""
        except subprocess.TimeoutExpired as exc:
            out = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", "replace")
            err = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", "replace")
            self.console("LOGIN_REFRESH_RETRY", platform=platform, reason="timeout", attempt_timeout=LOGIN_REFRESH_TIMEOUT)
            return 124, out, err
        except Exception as exc:
            self.console("LOGIN_REFRESH_RETRY", platform=platform, reason=type(exc).__name__)
            return 1, "", str(exc)

    def _write_refresh_log(self, platform: str, attempt: int, returncode: int, stdout: str, stderr: str) -> None:
        refresh_dir = self.run_dir / "login_refresh"
        refresh_dir.mkdir(parents=True, exist_ok=True)
        (refresh_dir / f"{platform}.json").write_text(json.dumps({
            "platform": platform, "attempt": attempt, "returncode": returncode,
            "status": "SUCCEEDED" if returncode == 0 else "RETRYING",
            "stdout_tail": (stdout or "")[-2000:], "stderr_tail": (stderr or "")[-2000:],
            "finished_at": iso_utc(),
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _execute_subprocess(self, agent: EnterpriseSubAgent, request: Request, command: list[str] | None,
                            attempt_dir: Path, attempt_no: int, lease_id: str | None) -> tuple[int | None, str]:
        # 跑子进程（command 为 None 时写 marker 占位），含 screenshot 产物校验。不碰 DB。
        stdout_path = attempt_dir / "stdout.log"
        stderr_path = attempt_dir / "stderr.raw.log"
        returncode: int | None = 0
        error_text = ""
        if command:
            with stdout_path.open("w", encoding="utf-8", newline="\n") as stdout, stderr_path.open("w", encoding="utf-8", newline="\n") as stderr:
                child_env = os.environ.copy()
                child_env["PYTHONIOENCODING"] = "utf-8"
                child_env["PYTHONUTF8"] = "1"
                proc = subprocess.Popen(command, stdout=stdout, stderr=stderr, cwd=_SCRIPT_DIR, env=child_env)
                started_at = time.monotonic()
                heartbeat = time.monotonic()
                while (returncode := proc.poll()) is None:
                    with self._state_lock:
                        manual_skip = self._manual_skips.get(request.task_id)
                    if manual_skip is not None:
                        proc.terminate()
                        try:
                            returncode = proc.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            returncode = proc.wait()
                        stderr.write(
                            f"main_agent manual skip: {manual_skip.get('reason') or 'operator_request'}\n"
                        )
                        break
                    if time.monotonic() - heartbeat >= 15:
                        self.console("ATTEMPT_RUNNING", agent, request, attempt=attempt_no, lease=lease_id or "none")
                        heartbeat = time.monotonic()
                    if time.monotonic() - started_at > TASK_EXECUTION_TIMEOUT:
                        proc.kill()
                        returncode = 124
                        stderr.write("main_agent timeout\n")
                        break
                    time.sleep(0.2)
            error_text = stderr_path.read_text(encoding="utf-8", errors="replace")
        else:
            stdout_path.write_text(json.dumps({"record_type": "final", "status": "empty_success", "reason": request.empty_reason or "no work"}, ensure_ascii=False) + "\n", encoding="utf-8")
            stderr_path.write_text("", encoding="utf-8")
        result_path = attempt_dir / "result.json"
        if request.phase == "WEB_CRAWL" and command and len(command) > 1 and Path(str(command[1])).name == "screenshot_crawler.py" and not result_path.is_file():
            if returncode == 0:
                returncode = 1
            error_text = (error_text + "\n" if error_text else "") + "screenshot crawl produced no result.json"
        return returncode, error_text

    def _build_summary(self, request: Request, attempt_dir: Path, base_status: str) -> dict[str, Any]:
        # 构造 attempt summary：读 result.json；ACCOUNT_DISCOVERY/POST_CRAWL 把 list 归一为 dict；OCR 文本在 result.txt、诊断在 result.json。
        result_path = attempt_dir / "result.json"
        if request.command and not result_path.is_file():
            return {"status": "missing_output", "expected_output": "result.json"}
        raw_result = json_load_any(result_path) if result_path.is_file() else None
        atomic_diagnosis = extract_atomic_diagnosis(raw_result)
        data = unwrap_atomic_data(raw_result)
        if isinstance(data, dict):
            summary = dict(data)
            if request.platform:
                summary["platform"] = request.platform
            if atomic_diagnosis:
                summary["diagnosis"] = atomic_diagnosis
        elif isinstance(data, list):
            item_key = "accounts" if request.phase == "ACCOUNT_DISCOVERY" else "items"
            summary = {"platform": request.platform, item_key: data, "status": "ok"}
            if atomic_diagnosis:
                summary["diagnosis"] = atomic_diagnosis
        else:
            summary = {"status": base_status}
            if request.platform:
                summary["platform"] = request.platform
            if atomic_diagnosis:
                summary["diagnosis"] = atomic_diagnosis
        if result_path.is_file() and isinstance(raw_result, (dict, list)):
            # 覆盖前先冻存脚本原始产物（字节级），result.json 才好做归一；归一后原始 payload 仍有处可查。
            raw_path = attempt_dir / "raw_result.json"
            if not raw_path.is_file():
                raw_path.write_bytes(result_path.read_bytes())
            result_path.write_text(stable_json(summary) + "\n", encoding="utf-8")
        return summary

    def _diagnose_attempt(
        self,
        agent: EnterpriseSubAgent,
        request: Request,
        returncode: int | None,
        stderr: str,
        summary: dict[str, Any],
        attempt_no: int,
    ) -> AttemptDiagnosis:
        # 状态解释层（不推进状态、不改 summary，仅为 on_finished 提供放行/拒绝判定）。
        """合并原子诊断与进程事实，生成 MainAgent 使用的最终诊断。"""
        atomic = extract_atomic_diagnosis(summary) or {}
        evidence = dict(atomic.get("evidence") or {}) if isinstance(atomic, dict) else {}
        evidence.update({"stderr_nonempty": bool(stderr.strip()), "attempt_no": attempt_no})

        if request.empty_reason:
            return AttemptDiagnosis(
                status="EMPTY_SUCCESS", error_code=None,
                reason=request.empty_reason, source="empty_reason",
                returncode=returncode, attempt_no=attempt_no,
                summary_status="empty_success", retryable=False,
                terminal=True, refresh_login=False, evidence=evidence,
            )

        status = "SUCCEEDED"
        error_code: str | None = None
        reason = "process exited successfully"
        source = "returncode"
        if returncode != 0:
            status, error_code = _classify_stderr(stderr)
            if error_code == "login_required":
                reason, source = "process stderr reports login failure", "stderr"
            elif error_code == "configuration_error":
                reason, source = "process stderr reports a missing or invalid runtime configuration", "stderr"
            elif error_code == "invalid_input":
                reason, source = "process stderr reports invalid input", "stderr"
            elif error_code == "retryable_error":
                reason, source = "process stderr reports a retryable failure", "stderr"
            else:
                reason, source = "process exited with an unclassified error", "returncode"

        atomic_status = str(atomic.get("status") or "") if isinstance(atomic, dict) else ""
        if atomic_status in {"SUCCEEDED", "RETRY_WAIT", "EMPTY_SUCCESS", "FAILED_FINAL"}:
            # stderr 分类得到的确定性终态（如 DNS 解析失败）优先于原子脚本的自述：
            # 脚本自己写 status=error 只表示"它认为可重试"，但 DNS 失败重试无意义。
            deterministic = status == "FAILED_FINAL" and str(error_code or "").startswith("deterministic")
            if not deterministic and (status == "SUCCEEDED" or atomic_status != "SUCCEEDED"):
                status = atomic_status
                error_code = atomic.get("error_code")
                reason = str(atomic.get("reason") or reason)
                source = str(atomic.get("source") or "atomic")

        summary_status = self._summary_status(summary)
        if status == "SUCCEEDED" and summary_status:
            payload_diag = canonical_attempt_diagnosis(summary_status, source="summary_status")
            if payload_diag.status != "SUCCEEDED":
                status, error_code = payload_diag.status, payload_diag.error_code
                reason, source = f"result status is {summary_status}", "summary_status"

        if status in {"SUCCEEDED", "EMPTY_SUCCESS"}:
            quality = self._kuaishou_output_quality_gate(agent, request, summary)
            if quality:
                status = str(quality["status"])
                error_code = quality.get("error_code")
                reason = str(quality.get("reason") or reason)
                source = str(quality.get("source") or "kuaishou_output_quality_gate")
                evidence.update(quality.get("evidence") or {})

        # round-2 KEYWORD 空判交给 on_finished（主线程 _keyword_was_used）：此处原在 worker 线程调
        # _keyword_empty_reason→self.db，sqlite 跨线程抛 ProgrammingError，round-2 被兜成 worker_error 全废。
        # 所有失败都先重试：首次执行 + 3 次重试均失败后，才写最终失败。
        if status == "RETRY_WAIT" and attempt_no > MAX_TASK_RETRIES:
            status = "FAILED_FINAL"
            reason, source = "retry budget exhausted after initial attempt plus 3 retries", "retry_budget"
        refresh_login = error_code == "login_required"
        return AttemptDiagnosis(
            status=status,
            error_code=error_code,
            reason=reason,
            source=source,
            returncode=returncode,
            attempt_no=attempt_no,
            summary_status=summary_status or None,
            retryable=status == "RETRY_WAIT",
            terminal=status in {"SUCCEEDED", "EMPTY_SUCCESS", "FAILED_FINAL"} and not refresh_login,
            refresh_login=refresh_login,
            evidence=evidence,
        )

    def _record_attempt(self, store: PipelineStore, agent: EnterpriseSubAgent, request: Request, attempt_id: str,
                        attempt_dir: Path, lease_id: str | None, attempt_no: int, diagnosis: AttemptDiagnosis,
                        returncode: int | None, error_text: str, summary: dict[str, Any]) -> None:
        # 落盘 final.json + manifest（marker 跳过全树扫描）+ DB 记录 + lease 释放 + 完成事件。
        (attempt_dir / "final.json").write_text(json.dumps({
            "status": diagnosis.status,
            "error_code": diagnosis.error_code,
            "diagnosis": diagnosis.to_dict(),
            "summary": summary,
            "finished_at": iso_utc(),
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if request.command:
            manifest = [dataclasses.asdict(x) for x in build_manifest(self.run_dir, [p for p in attempt_dir.rglob("*") if p.is_file()])]
        else:
            manifest = []  # marker 无产物，跳过 build_manifest 的空目录扫描
        store.output_item(attempt_id, 1, f"{request.phase}:{request.task_id}", "final", summary)
        store.finish_attempt(attempt_id, request.task_id, diagnosis.status, returncode, diagnosis.error_code, error_text[-4000:], summary, manifest)
        if lease_id:
            self._release_lease(store, request, lease_id)
            with self._state_lock:
                self._active_leases.pop(request.task_id, None)
        store.event(uuid4().hex, "task", request.task_id, "attempt_finished", {"attempt_id": attempt_id, "status": diagnosis.status, "error_code": diagnosis.error_code, "diagnosis": diagnosis.to_dict()})
        self.console("ATTEMPT_FINISHED", agent, request, attempt=attempt_no, status=diagnosis.status, error_code=diagnosis.error_code, returncode=returncode, lease=lease_id or "none")

    def _run_request(self, agent: EnterpriseSubAgent, request: Request, lease_id: str | None = None,
                     db: PipelineStore | None = None) -> dict[str, Any]:
        store = db or self.db
        payload, task_round, exact_fingerprint = self._register_task(agent, request, store)
        with self._state_lock:
            attempt_no = self.attempt_counts.get(request.task_id, 0) + 1
            self.attempt_counts[request.task_id] = attempt_no
        attempt_dir = self._attempt_dir(agent, request, attempt_no)
        attempt_id = uuid4().hex
        source = request.history_source
        execution_mode = "HISTORY_REUSE" if source else "PROCESS"
        start_kwargs = {
            "execution_mode": execution_mode,
            "request_fingerprint": exact_fingerprint,
            "source_run_id": str(source["source_run_id"]) if source else None,
            "source_task_id": str(source["task_id"]) if source else None,
            "source_attempt_id": str(source["attempt_id"]) if source else None,
            "reuse_match_kind": str(source["reuse_match_kind"]) if source else None,
        }
        if lease_id:
            store.start_attempt(attempt_id, request.task_id, attempt_no, lease_id, 1, **start_kwargs)
        else:
            store.start_attempt(attempt_id, request.task_id, attempt_no, "", 0, **start_kwargs)
        self.console("ATTEMPT_STARTED", agent, request, attempt=attempt_no, task=request.task_id, lease=lease_id or "none")

        if source:
            try:
                self._materialize_history_attempt(source, attempt_dir)
            except (OSError, ValueError) as exc:
                attempt_dir.mkdir(parents=True, exist_ok=True)
                exhausted = attempt_no > MAX_TASK_RETRIES
                diagnosis = AttemptDiagnosis(
                    status="FAILED_FINAL" if exhausted else "RETRY_WAIT", error_code="history_materialize_error",
                    reason=str(exc), source="history_reuse",
                    returncode=None, attempt_no=attempt_no, retryable=not exhausted, terminal=exhausted,
                    evidence={
                        "source_run_id": source["source_run_id"],
                        "source_attempt_id": source["attempt_id"],
                    },
                )
                summary = {"status": "error", "diagnosis": diagnosis.to_dict()}
                self._record_attempt(
                    store, agent, request, attempt_id, attempt_dir, lease_id,
                    attempt_no, diagnosis, None, str(exc), summary,
                )
                if diagnosis.retryable:
                    store.set_task_retry(request.task_id, diagnosis.error_code or "history_materialize_error", str(exc))
                request.history_source = None
                return {
                    "status": diagnosis.status,
                    "summary": summary,
                    "diagnosis": diagnosis.to_dict(),
                    "error_code": diagnosis.error_code,
                    "attempt_dir": str(attempt_dir),
                }

            try:
                source_summary = json.loads(str(source.get("summary_json") or "{}"))
            except json.JSONDecodeError:
                source_summary = {}
            summary = dict(source_summary) if isinstance(source_summary, dict) else {}
            if not summary:
                summary = self._build_summary(request, attempt_dir, str(source["attempt_status"]))
            summary.pop("diagnosis", None)
            diagnosis = AttemptDiagnosis(
                status=str(source["attempt_status"]), error_code=None,
                reason="reused successful attempt from replay source run",
                source="history_reuse", returncode=0, attempt_no=attempt_no,
                summary_status=self._summary_status(summary) or None,
                retryable=False, terminal=True, refresh_login=False,
                evidence={
                    "source_run_id": source["source_run_id"],
                    "source_task_id": source["task_id"],
                    "source_attempt_id": source["attempt_id"],
                    "reuse_match_kind": source["reuse_match_kind"],
                    "request_fingerprint": exact_fingerprint,
                },
            )
            summary["diagnosis"] = diagnosis.to_dict()
            (attempt_dir / "result.json").write_text(stable_json(summary) + "\n", encoding="utf-8")
            (attempt_dir / "stdout.log").write_text(stable_json({
                "execution_mode": "HISTORY_REUSE",
                "source_run_id": source["source_run_id"],
                "source_attempt_id": source["attempt_id"],
            }) + "\n", encoding="utf-8")
            (attempt_dir / "stderr.raw.log").write_text("", encoding="utf-8")
            self._record_attempt(
                store, agent, request, attempt_id, attempt_dir, lease_id,
                attempt_no, diagnosis, 0, "", summary,
            )
            store.event(uuid4().hex, "task", request.task_id, "history_reused", {
                "source_run_id": source["source_run_id"],
                "source_task_id": source["task_id"],
                "source_attempt_id": source["attempt_id"],
                "reuse_match_kind": source["reuse_match_kind"],
                "request_fingerprint": exact_fingerprint,
            })
            self.console(
                "HISTORY_REUSED", agent, request,
                source_run=source["source_run_id"], source_attempt=source["attempt_id"],
            )
            brief = self._summarize_result(request, {"status": diagnosis.status, "summary": summary})
            if brief:
                self.console("RESULT_BRIEF", agent, request, brief=brief)
                if request.phase == "ACCOUNT_RANK":
                    self.console("LLM_SELECTED", agent, request, selected=brief)
            return {
                "status": diagnosis.status,
                "summary": summary,
                "diagnosis": diagnosis.to_dict(),
                "error_code": None,
                "attempt_dir": str(attempt_dir),
                "execution_mode": "HISTORY_REUSE",
            }

        command = self._command_for(request, attempt_dir)
        returncode, error_text = self._execute_subprocess(agent, request, command, attempt_dir, attempt_no, lease_id)
        with self._state_lock:
            manual_skip = dict(self._manual_skips.get(request.task_id) or {})
        if manual_skip:
            diagnosis = AttemptDiagnosis(
                status="SKIPPED", error_code="manual_skip",
                reason=str(manual_skip.get("reason") or "operator requested phase skip"),
                source="control_command", returncode=returncode, attempt_no=attempt_no,
                summary_status="skipped", retryable=False, terminal=True,
                refresh_login=False,
                evidence={
                    "command_id": manual_skip.get("command_id"),
                    "company_id": company_id(agent.name),
                    "phase": request.phase,
                },
            )
            summary = {
                "status": "skipped",
                "reason": diagnosis.reason,
                "command_id": manual_skip.get("command_id"),
                "diagnosis": diagnosis.to_dict(),
            }
            (attempt_dir / "result.json").write_text(stable_json(summary) + "\n", encoding="utf-8")
            self._record_attempt(
                store, agent, request, attempt_id, attempt_dir, lease_id,
                attempt_no, diagnosis, returncode, error_text, summary,
            )
            return {
                "status": "SKIPPED", "summary": summary,
                "diagnosis": diagnosis.to_dict(), "error_code": "manual_skip",
                "attempt_dir": str(attempt_dir),
            }
        summary = self._build_summary(request, attempt_dir, "SUCCEEDED" if returncode == 0 else "FAILED_FINAL")
        summary_changed = False
        if request.phase == "KEYWORD" and task_round == 2 and returncode == 0:
            keywords = agent._keyword_map(summary)
            used = {
                (str(item.get("platform") or ""), agent._normalize_keyword(str(item.get("keyword") or "")))
                for item in store.historical_used_keywords(company_id(agent.name))
                if int(item.get("round") or 0) < task_round
            }
            keyword_pairs = [
                (platform, keyword)
                for platform, platform_keywords in keywords.items()
                for keyword in platform_keywords
            ]
            if keyword_pairs and all(
                (platform, agent._normalize_keyword(keyword)) in used
                for platform, keyword in keyword_pairs
            ):
                summary["status"] = "empty_success"
                summary["reason"] = "all round-2 keywords were already used"
                summary["duplicate_platforms"] = sorted(keywords)
                # The keyword script includes its own successful atomic
                # diagnosis.  Mark the request empty explicitly so that the
                # post-processing dedupe result wins over that earlier "ok".
                request.empty_reason = summary["reason"]
                summary_changed = True
        if request.phase == "WEB_CRAWL":
            url = str(request.payload.get("url") or summary.get("url") or "")
            summary["domain_activity"] = _domain_activity(
                url,
                summary,
                error_text=error_text,
                probe_text=str(request.payload.get("browser_probe_error") or ""),
            )
            summary_changed = True
        if request.phase == "POST_CRAWL" and request.platform in PLATFORM_SPECS:
            posts = agent._activity_evidence_posts(request, current_summary=summary)
            if not posts:
                posts = [summary] if request.platform == "xhs" and request.payload.get("stage") == "detail" else _iter_items(summary)
            summary["account_activity"] = _account_activity(str(request.platform), posts)
            # 保留小红书单帖旧字段，兼容已经按上一版结果结构读取的调用方。
            if request.platform == "xhs" and request.payload.get("stage") == "detail":
                summary["post_activity"] = _post_activity(summary.get("time"))
            summary_changed = True
        result_path = attempt_dir / "result.json"
        if summary_changed and result_path.is_file():
            result_path.write_text(stable_json(summary) + "\n", encoding="utf-8")
        diagnosis = self._diagnose_attempt(agent, request, returncode, error_text, summary, attempt_no)
        # invalid_input = 脚本契约 bug：建结构化缺陷，进 evidence/final.json + agent.defects，并实时 console。
        defect = _build_script_defect(agent, request, command, error_text, attempt_no) if diagnosis.error_code == "invalid_input" else None
        if defect:
            diagnosis.evidence["script_defect"] = defect
            agent.defects.append(defect)
        summary["diagnosis"] = diagnosis.to_dict()
        self._record_attempt(store, agent, request, attempt_id, attempt_dir, lease_id, attempt_no, diagnosis, returncode, error_text, summary)
        brief = self._summarize_result(request, {"status": diagnosis.status, "summary": summary})
        if brief:
            self.console("RESULT_BRIEF", agent, request, brief=brief)
            if request.phase == "ACCOUNT_RANK":
                self.console("LLM_SELECTED", agent, request, selected=brief)
        if defect:
            self.console("SCRIPT_DEFECT", agent, request, script=defect["script"], fix=defect["suggested_fix"])
        if diagnosis.retryable:
            store.set_task_retry(request.task_id, diagnosis.error_code or "retryable_error", error_text[-4000:])
        return {
            "status": diagnosis.status,
            "summary": summary,
            "diagnosis": diagnosis.to_dict(),
            "error_code": diagnosis.error_code,
            "attempt_dir": str(attempt_dir),
        }

    def _run_request_worker(self, agent: EnterpriseSubAgent, request: Request, lease_id: str | None) -> dict[str, Any]:
        # sqlite 连接是线程私有的；每个 worker 自建连接，主线程保留调度连接。
        store = PipelineStore(self.db.path)
        store.init()
        try:
            return self._run_request(agent, request, lease_id, store)
        except Exception as exc:
            store.interrupt_running_attempts(
                request.task_id, "worker_exception", str(exc)[-4000:],
            )
            raise
        finally:
            # 进程启动或文件 IO 抛错时，也要释放内存里的资源守卫。
            with self._state_lock:
                lease_id = self._active_leases.pop(request.task_id, None)
            if lease_id:
                store.release_lease(lease_id)
                store.set_resource(request.resource_type or "", request.resource_key or "", "AVAILABLE", 1)
            self._release_resource(request)
            store.close()

    def _icp_carrier_records(self, agent: EnterpriseSubAgent) -> dict[str, list[dict[str, Any]]]:
        """按载体类型收集 ICP 备案记录，供企业导出展示网站/小程序/APP/快应用等网络载体。"""
        found: dict[str, list[dict[str, Any]]] = {carrier: [] for carrier in ICP_CARRIERS}
        seen: set[tuple[str, str]] = set()
        for completed in agent.completed:
            if completed.get("phase") != "WEB_ICP":
                continue
            request = completed.get("request") if isinstance(completed.get("request"), dict) else {}
            carrier = str(request.get("carrier") or "web")
            for record in nested_records(completed.get("summary")):
                key = (carrier, str(record.get("serviceLicence") or record.get("serviceName") or record.get("domain") or ""))
                if key in seen:
                    continue
                seen.add(key)
                found.setdefault(carrier, []).append({
                    "carrier_type": carrier,
                    "name": icp_record_label(record),
                    "service_name": record.get("serviceName") or record.get("appName") or record.get("domain"),
                    "licence": record.get("serviceLicence") or record.get("mainLicence"),
                    "unit_name": record.get("unitName"),
                    "update_time": record.get("updateRecordTime"),
                })
        return found

    _SENSITIVE_KEY_PATTERNS = ("token", "cookie", "password", "secret", "authorization")
    _SENSITIVE_FLAGS = ("--xsec-token", "--cookie", "--password")

    @staticmethod
    def _redact_sensitive(value: Any) -> Any:
        """递归抹掉导出 JSON 里的凭据字段（xsec_token / cookies 等），值替换为 [REDACTED]。

        completed 原样进导出会带上 xhs 的 xsec_token（payload 与 candidate 里都有），
        defects 的 command 数组也含 --xsec-token 参数值；下游查询/报告脚本读这份 JSON，
        凭据不应随之扩散。业务字段不含这些词根，不会误伤。
        """
        if isinstance(value, dict):
            cleaned: dict[str, Any] = {}
            for key, item in value.items():
                if (
                    isinstance(key, str)
                    and any(pattern in key.lower() for pattern in MainAgent._SENSITIVE_KEY_PATTERNS)
                    and item not in (None, "")
                ):
                    cleaned[key] = "[REDACTED]"
                else:
                    cleaned[key] = MainAgent._redact_sensitive(item)
            return cleaned
        if isinstance(value, list):
            cleaned: list[Any] = []
            skip_next = False
            for item in value:
                if skip_next:
                    skip_next = False
                    cleaned.append("[REDACTED]")
                    continue
                if isinstance(item, str) and item in MainAgent._SENSITIVE_FLAGS:
                    cleaned.append(item)
                    skip_next = True
                    continue
                cleaned.append(MainAgent._redact_sensitive(item))
            return cleaned
        return value

    def _write_company_export(self, agent: EnterpriseSubAgent) -> None:
        export_dir = self.run_dir / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        final_assessment = next((
            item.get("summary", {}).get("analysis") for item in reversed(agent.completed)
            if item.get("phase") == "RISK_ANALYSIS" and isinstance(item.get("summary"), dict)
            and isinstance(item["summary"].get("analysis"), dict)
        ), None)
        if isinstance(final_assessment, dict):
            final_assessment = copy.deepcopy(final_assessment)
            final_assessment["evidence_screenshots"] = agent.risk_evidence_screenshots
        website_search_metadata = agent._website_search_metadata()
        payload = {
            "schema_version": 3,
            "record_type": "enterprise_result",
            "run_id": self.run_id,
            "company": agent.name,
            "company_id": company_id(agent.name),
            "status": agent.state,
            "completed": agent.completed,
            "social_account_verifications": sorted(
                agent.account_verifications,
                key=lambda item: (
                    int(item.get("round") or 0),
                    str(item.get("platform") or ""),
                    str(item.get("account_name") or item.get("username") or ""),
                ),
            ),
            "social_account_activity": sorted(
                agent.social_account_activity.values(),
                key=lambda item: (int(item.get("round") or 0), str(item.get("platform") or ""), str(item.get("account_name") or "")),
            ),
            "domain_activity": sorted(agent.domain_activity.values(), key=lambda item: str(item.get("domain") or "")),
            "icp_carriers": self._icp_carrier_records(agent),
            "website_search": {
                **website_search_metadata,
                "provider": website_search_metadata.get("provider") or "bing_html_cn_with_rss_fallback",
                "official_status": "unverified_candidates",
                "candidates": agent._website_search_candidates(),
                "carrier_candidates": agent._website_search_carrier_candidates(),
            },
            "website_crawl": {
                "max_depth": agent.web_max_depth,
                "max_pages": agent.web_max_pages,
                "pages_scheduled": agent.web_pages_scheduled,
                "seen_urls": sorted(agent.web_seen_urls),
                "stats": dict(agent.web_crawl_stats),
            },
            "enterprise_assessment": final_assessment,
            "risk_evidence_screenshots": agent.risk_evidence_screenshots,
            "defects": agent.defects,
            "artifacts": [str(path.relative_to(self.run_dir)).replace("\\", "/") for path in agent.artifact_dir().rglob("final.json")],
            "screenshots": [
                str(path.relative_to(self.run_dir)).replace("\\", "/")
                for path in agent.artifact_dir().rglob("*")
                if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
            ],
            "generated_at": iso_utc(),
        }
        payload = self._redact_sensitive(payload)
        (export_dir / f"{safe_name(agent.name)}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    def _write_run_defects(self, defects: list[dict[str, Any]]) -> None:
        """invalid_input 缺陷的维护工单：defects.json 全量 + defects.md 按脚本归类。无缺陷不写。"""
        if not defects:
            return
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "defects.json").write_text(
            json.dumps(defects, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        by_script: dict[str | None, list[dict[str, Any]]] = {}
        for defect in defects:
            by_script.setdefault(defect.get("script"), []).append(defect)
        lines = [
            f"# 脚本契约缺陷工单（run {self.run_id}）", "",
            f"共 {len(defects)} 条 invalid_input，涉及 {len(by_script)} 个脚本。每条都是 main_agent 造的命令与脚本 argparse 对不上的确定性 bug，需改代码。",
            "嫌疑判定：'main_agent 造命令' 看 _command_for / PlatformSpec；'脚本 argparse' 看对应 *_step*.py。", "",
        ]
        for script, items in by_script.items():
            companies = sorted({d.get("company") for d in items if d.get("company")})
            sample = items[0]
            cmd = " ".join(str(c) for c in (sample.get("command") or []))
            lines += [
                f"## {script or '(未知脚本)'}（{len(items)} 次，{len(companies)} 家公司）",
                f"- 建议修复: {sample.get('suggested_fix')}",
                f"- 公司: {', '.join(companies) or '(无)'}",
                f"- 样例命令: `{cmd}`",
                f"- 样例 stderr: {(sample.get('stderr_tail') or '').strip()[:400] or '(空)'}",
                "",
            ]
        (self.run_dir / "defects.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _cooldown_for(self, resource_type: str | None) -> float:
        base = RESOURCE_COOLDOWN_SECONDS.get(resource_type or "", 240.0)
        jitter = RESOURCE_COOLDOWN_JITTER_SECONDS.get(resource_type or "", COOLDOWN_JITTER_SECONDS)
        return base + random.uniform(0.0, jitter)

    def _apply_lane_result(self, request: Request, status: str, error_code: str | None) -> str:
        """一个 attempt 跑完后按结果更新其 lane 的熔断态，返回动作：retry(任务重排，等 lane 恢复) / terminal(放行 on_finished)。"""
        if error_code == "history_materialize_error" and status != "FAILED_FINAL":
            return "retry"
        if request.history_source:
            return "terminal"
        if not request.resource_type or not request.resource_key:
            return "terminal"  # 无 lane（marker 等）不参与熔断
        key = (request.resource_type, request.resource_key)
        # 登录失效优先于一般失败处理：停止该平台发租约并刷新登录，绝不
        # 误入普通冷却。达到重试预算后才允许作为最终失败放行。
        # 但首次报告先按"疑似限频误判"冷却复核一次：限频页常被脚本当成登录墙，
        # 立刻弹扫码刷新会被平台判高频、把真登录态打掉（2026-08-18 xhs 实测）。
        if error_code == "login_required" and request.resource_type == "PLATFORM" and status != "FAILED_FINAL":
            platform = request.platform or ""
            with self._state_lock:
                strikes = self._login_required_strikes.get(platform, 0) + 1
                self._login_required_strikes[platform] = strikes
            if strikes <= 1:
                self.console("LOGIN_REFRESH_DEFERRED", platform=platform,
                             cooldown=int(LOGIN_REFRESH_CONFIRM_COOLDOWN_SECONDS))
                self._set_lane(key, "COOLDOWN", time.monotonic() + LOGIN_REFRESH_CONFIRM_COOLDOWN_SECONDS)
                return "retry"
            self._ensure_login_refresh(platform)
            return "retry"
        if status in ("SUCCEEDED", "EMPTY_SUCCESS"):
            if request.resource_type == "PLATFORM":
                with self._state_lock:
                    self._login_required_strikes.pop(request.platform or "", None)
            self._set_lane(key, "HEALTHY")
            return "terminal"
        if status == "RETRY_WAIT":  # 瞬时：rate_limit/captcha/超时/断网/worker_error → 停发、到点探测
            self._set_lane(key, "COOLDOWN", time.monotonic() + self._cooldown_for(request.resource_type))
            return "retry"
        # FAILED_FINAL 只可能由重试预算耗尽产生。
        return "terminal"

    def _pick_grantable(self, agent: EnterpriseSubAgent, now: float) -> tuple[Request | None, list[Request]]:
        """非阻塞扫企业队列：过 wait-deadline 的收集到 expired（交还调用方按跳过推进），返回首个可派发请求。
        未选中的请求保持原序回填队首；deadline 首次见到时盖戳。不在扫描中调 on_finished（避免边遍历边改 deque）。"""
        held: list[Request] = []
        expired: list[Request] = []
        picked: Request | None = None
        while agent.requests:
            req = agent.requests.popleft()
            # A request is a durable task before resource arbitration.  This makes
            # blocked social work auditable and recoverable instead of invisible.
            self._register_task(agent, req, self.db)
            self._prepare_history_reuse(agent, req)
            is_social_login_wait = (
                req.resource_type == "PLATFORM"
                and req.resource_key in PLATFORMS
                and self._lane_state.get((req.resource_type, req.resource_key)) == "LOGIN_REFRESH"
            )
            if is_social_login_wait and req.deadline == 0.0:
                req.deadline = now + SOCIAL_LOGIN_WAIT_TIMEOUT
            elif not is_social_login_wait:
                req.deadline = 0.0
            if req.deadline and now > req.deadline:
                expired.append(req)
                continue
            if picked is None and self._lane_grantable(req, now):
                picked = req
                break
            held.append(req)
        agent.requests.extendleft(reversed(held))
        return picked, expired

    def _skip_request(self, agent: EnterpriseSubAgent, request: Request) -> None:
        """A social-login resource wait timed out: record one ordinary failed attempt."""
        self.console("TASK_FAILED", agent, request, reason="social_login_wait_timeout")
        _, _, exact_fingerprint = self._register_task(agent, request, self.db)
        with self._state_lock:
            attempt_no = self.attempt_counts.get(request.task_id, 0) + 1
            self.attempt_counts[request.task_id] = attempt_no
        exhausted = attempt_no > MAX_TASK_RETRIES
        diagnosis = {"status": "FAILED_FINAL" if exhausted else "RETRY_WAIT", "error_code": "social_login_wait_timeout",
                     "reason": "social login did not recover before task deadline", "source": "resource_wait",
                     "retryable": not exhausted, "terminal": exhausted, "refresh_login": False}
        result = {"status": diagnosis["status"], "summary": {"status": "failed", "reason": "social_login_wait_timeout"},
                  "diagnosis": diagnosis}
        attempt_id = uuid4().hex
        self.db.start_attempt(
            attempt_id, request.task_id, attempt_no, "", 0,
            execution_mode="RESOURCE_WAIT_TIMEOUT", request_fingerprint=exact_fingerprint,
        )
        self.db.finish_attempt(
            attempt_id, request.task_id, diagnosis["status"], 124, "social_login_wait_timeout", diagnosis["reason"],
            result["summary"], [],
        )
        self.db.event(uuid4().hex, "task", request.task_id, "task_skipped", {
            "reason": "social_login_wait_timeout",
            "phase": request.phase,
            "platform": request.platform,
            "attempt_id": attempt_id,
            "attempt_no": attempt_no,
        })
        if not exhausted:
            self.db.set_task_retry(request.task_id, "social_login_wait_timeout", diagnosis["reason"])
            request.deadline = 0.0
            agent.requests.appendleft(request)
            self._report_progress(agent, request, "登录等待超时，准备重试")
            return
        self.db.set_task_final_failure(request.task_id, "social_login_wait_timeout", diagnosis["reason"])
        agent.on_finished(request, result, diagnosis)
        self._report_progress(agent, request, "登录等待超时，最终失败")

    @staticmethod
    def _manual_skip_result(request: Request, control: dict[str, Any]) -> dict[str, Any]:
        reason = str(control.get("reason") or "operator requested phase skip")
        diagnosis = AttemptDiagnosis(
            status="SKIPPED", error_code="manual_skip", reason=reason,
            source="control_command", retryable=False, terminal=True,
            refresh_login=False,
            evidence={
                "command_id": control.get("command_id"),
                "phase": request.phase,
            },
        ).to_dict()
        return {
            "status": "SKIPPED",
            "error_code": "manual_skip",
            "diagnosis": diagnosis,
            "summary": {
                "status": "skipped", "reason": reason,
                "command_id": control.get("command_id"),
                "diagnosis": diagnosis,
            },
        }

    def _record_queued_manual_skip(self, agent: EnterpriseSubAgent, request: Request,
                                   control: dict[str, Any]) -> dict[str, Any]:
        """Persist a never-started request as a terminal, auditable skip attempt."""
        _, _, exact_fingerprint = self._register_task(agent, request, self.db)
        with self._state_lock:
            attempt_no = self.attempt_counts.get(request.task_id, 0) + 1
            self.attempt_counts[request.task_id] = attempt_no
        attempt_dir = self._attempt_dir(agent, request, attempt_no)
        result = self._manual_skip_result(request, control)
        diagnosis = AttemptDiagnosis(**result["diagnosis"])
        (attempt_dir / "stdout.log").write_text("", encoding="utf-8")
        (attempt_dir / "stderr.raw.log").write_text("", encoding="utf-8")
        (attempt_dir / "result.json").write_text(stable_json(result["summary"]) + "\n", encoding="utf-8")
        attempt_id = uuid4().hex
        self.db.start_attempt(
            attempt_id, request.task_id, attempt_no, "", 0,
            execution_mode="MANUAL_SKIP", request_fingerprint=exact_fingerprint,
        )
        self._record_attempt(
            self.db, agent, request, attempt_id, attempt_dir, None,
            attempt_no, diagnosis, None, "", result["summary"],
        )
        result["attempt_dir"] = str(attempt_dir)
        self.db.event(uuid4().hex, "task", request.task_id, "manually_skipped", {
            "command_id": control.get("command_id"),
            "company_id": company_id(agent.name),
            "phase": request.phase,
            "reason": control.get("reason"),
            "execution_mode": "MANUAL_SKIP",
        })
        return result

    def _apply_manual_skip_command(self, row: Any) -> None:
        command_id = str(row["command_id"])
        company_key = str(row["company_id"])
        phase = str(row["phase"]).upper()
        reason = str(row["reason"] or "operator_request")
        agent = self.agents.get(company_key)
        request_for_console = Request(phase, None, {}, None, None, None)
        if agent is None:
            result = {"reason": "company is not part of this run", "company_id": company_key, "phase": phase}
            self.db.finish_control_command(command_id, "REJECTED", result)
            self.console("MANUAL_SKIP_REJECTED", request=request_for_console, reason=result["reason"])
            return
        request_for_console.payload["company"] = agent.name
        if phase not in PHASES:
            result = {"reason": "unknown phase", "company_id": company_key, "phase": phase}
            self.db.finish_control_command(command_id, "REJECTED", result)
            self.console("MANUAL_SKIP_REJECTED", agent, request_for_console, reason=result["reason"])
            return

        with self._state_lock:
            running_ids = [
                task_id for task_id, (active_company, active_phase) in self._active_requests.items()
                if active_company == company_key and active_phase == phase
            ]
        queued = [request for request in agent.requests if request.phase == phase]
        if agent.state == "CLOSED" or (not queued and not running_ids):
            current = agent.requests[0].phase if agent.requests else "EXPORT"
            result = {
                "reason": f"phase is not currently pending or running (current: {current})",
                "company_id": company_key, "company": agent.name,
                "phase": phase, "current_phase": current,
            }
            self.db.finish_control_command(command_id, "REJECTED", result)
            self.console("MANUAL_SKIP_REJECTED", agent, request_for_console, reason=result["reason"])
            return

        control = {
            "command_id": command_id, "reason": reason,
            "company_id": company_key, "company": agent.name, "phase": phase,
        }
        with self._state_lock:
            for task_id in running_ids:
                self._manual_skips[task_id] = dict(control)

        # Remove and finalize one queued request at a time. Leaving the other
        # requests in the deque until their turn preserves the state machine's
        # wave guard, so only the final skipped item advances the company.
        for request in queued:
            agent.requests.remove(request)
            result = self._record_queued_manual_skip(agent, request, control)
            agent.on_finished(request, result, result["diagnosis"])
            self._report_progress(agent, request, "已手动跳过")

        outcome = {
            "company_id": company_key, "company": agent.name, "phase": phase,
            "queued_skipped": len(queued), "running_cancel_requested": len(running_ids),
            "reason": reason,
        }
        self.db.finish_control_command(command_id, "APPLIED", outcome)
        self.db.event(uuid4().hex, "company", company_key, "manual_phase_skip", {
            "command_id": command_id, **outcome,
        })
        self.console(
            "MANUAL_SKIP_APPLIED", agent, request_for_console,
            queued=len(queued), running=len(running_ids), reason=reason,
        )
        if agent.state != "CLOSED" and company_key not in self.ready:
            self.ready.append(company_key)

    def _poll_control_commands(self) -> None:
        for row in self.db.pending_control_commands(self.run_id):
            action = str(row["action"]).upper()
            if action == "STOP":
                self.db.finish_control_command(str(row["command_id"]), "APPLIED",
                                               {"reason": "operator requested shutdown"})
                raise KeyboardInterrupt("看板请求停止排查")
            if action != "SKIP_PHASE":
                result = {"reason": f"unsupported action: {row['action']}"}
                self.db.finish_control_command(str(row["command_id"]), "REJECTED", result)
                continue
            try:
                self._apply_manual_skip_command(row)
            except Exception as exc:
                result = {"reason": f"control command failed: {type(exc).__name__}: {exc}"}
                self.db.finish_control_command(str(row["command_id"]), "REJECTED", result)
                self.console("MANUAL_SKIP_REJECTED", phase=str(row["phase"]), reason=result["reason"])

    def _next_wake_timeout(self, now: float) -> float:
        """离最近一个 COOLDOWN lane 探测点的秒数，用作 wait 超时；无 COOLDOWN lane 时默认 1s（refresh 完成会 notify）。"""
        earliest: float | None = None
        with self._resource_condition:
            for key, state in self._lane_state.items():
                if state == "COOLDOWN":
                    wait = self._lane_probe_at.get(key, 0.0) - now
                    earliest = wait if earliest is None or wait < earliest else earliest
        if earliest is None:
            return 0.5
        # Control commands are polled by the scheduler loop too, so never sleep
        # for an entire resource cooldown before noticing an operator request.
        return max(0.1, min(earliest, 0.5))

    def run(self) -> int:
        self.bootstrap()
        futures: dict[concurrent.futures.Future[dict[str, Any]], tuple[str, EnterpriseSubAgent, Request]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.worker_count, thread_name_prefix="pipeline-worker") as pool:
            while self.ready or futures:
                self._poll_control_commands()
                # --- 派发阶段：非阻塞扫描各企业队列，把可派发任务填进线程池 ---
                deferred: deque[str] = deque()
                while self.ready and len(futures) < self.worker_count:
                    company_key = self.ready.popleft()
                    agent = self.agents[company_key]
                    if agent.state == "CLOSED":
                        self.db.set_company_state(self.run_id, company_key, "CLOSED", "EXPORT")
                        continue
                    picked, expired = self._pick_grantable(agent, time.monotonic())
                    if picked is None:
                        for req in expired:
                            self._skip_request(agent, req)
                        # 当前无可派发：队列空且无在途→CLOSED；否则下轮再来
                        if not agent.requests and agent.inflight == 0:
                            agent.state = "CLOSED"
                            self.db.set_company_state(self.run_id, company_key, "CLOSED", "EXPORT")
                        else:
                            deferred.append(company_key)
                        continue
                    # Child rows in resource_waiters/resource_leases reference tasks.
                    # Persist and validate the parent synchronously before leasing.
                    self._register_task(agent, picked, self.db)
                    lease_id = self._acquire_lane(picked, time.monotonic(), self.db)
                    if lease_id is None and picked.resource_type and not picked.history_source:
                        # 极少数竞态没占上 lane：picked 塞回队首，本轮不清 expired（下轮 picked 入航后再清）
                        agent.requests.appendleft(picked)
                        deferred.append(company_key)
                        continue
                    if lease_id:
                        with self._state_lock:
                            self._active_leases[picked.task_id] = lease_id
                    with self._state_lock:
                        self._active_requests[picked.task_id] = (company_key, picked.phase)
                    agent.inflight += 1
                    # picked 已入航：现在清 expired 的 on_finished 不会被 wave guard 误触发提前换幕
                    for req in expired:
                        self._skip_request(agent, req)
                    self.db.event(uuid4().hex, "company", company_key, "resource_requested", {"phase": picked.phase, "payload": picked.payload})
                    self.console("RESOURCE_REQUESTED", agent, request=picked, task=picked.task_id)
                    if picked.phase == "POST_CRAWL" and picked.platform and picked.payload.get("account_name"):
                        self.console("POST_WORKS_TARGET", agent, request=picked, account=picked.payload.get("account_name"), stage=picked.payload.get("stage"))
                    self._report_progress(agent, picked, "开始执行")
                    future = pool.submit(self._run_request_worker, agent, picked, lease_id)
                    futures[future] = (company_key, agent, picked)
                    # 还有请求或在途→回队尾，本轮可继续派发同企业的下一个
                    if agent.requests or agent.inflight > 0:
                        self.ready.append(company_key)
                self.ready.extend(deferred)
                # --- 等待阶段：等任一在途任务完成，或等到最近一个 COOLDOWN 探测点 / 登录刷新完成 ---
                wake_timeout = self._next_wake_timeout(time.monotonic())
                self._report_lane_waits(time.monotonic())
                if futures:
                    done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED, timeout=wake_timeout)
                    for future in done:
                        company_key, agent, request = futures.pop(future)
                        agent.inflight -= 1
                        with self._state_lock:
                            self._active_requests.pop(request.task_id, None)
                        try:
                            result = future.result()
                        except Exception as exc:
                            # worker 线程抛异常（子进程都没跑完）多为基础设施抖动，归瞬时 → lane 冷却重试。
                            worker_diagnosis = AttemptDiagnosis(
                                status="RETRY_WAIT", error_code="worker_error",
                                reason=str(exc), source="worker_exception",
                                retryable=True, terminal=False, evidence={"exception": type(exc).__name__},
                            ).to_dict()
                            result = {
                                "status": "RETRY_WAIT",
                                "error_code": "worker_error",
                                "diagnosis": worker_diagnosis,
                                "summary": {"error": str(exc), "diagnosis": worker_diagnosis},
                            }
                            self.console("WORKER_FAILED", agent, request, error=str(exc))
                        with self._state_lock:
                            manual_skip = self._manual_skips.pop(request.task_id, None)
                        if manual_skip and str(result.get("status") or "").upper() != "SKIPPED":
                            # The subprocess may have completed in the narrow
                            # interval between polling the command and polling
                            # its process. The operator command still wins for
                            # state-machine fan-out, and the race is auditable.
                            result = self._manual_skip_result(request, manual_skip)
                            self.db.event(uuid4().hex, "task", request.task_id, "manual_skip_completion_race", {
                                "command_id": manual_skip.get("command_id"),
                                "phase": request.phase,
                            })
                        diagnosis = result.get("diagnosis") if isinstance(result.get("diagnosis"), dict) else {}
                        result_status = str(diagnosis.get("status") or result.get("status") or "FAILED_FINAL")
                        result_error = diagnosis.get("error_code") or result.get("error_code")
                        action = self._apply_lane_result(request, result_status, result_error)
                        if action == "retry":
                            # Every failed attempt consumes the same durable budget.
                            # RETRY_WAIT attempts were recorded by the worker; login
                            # failures are converted here because they trigger refresh.
                            if result_status != "RETRY_WAIT":
                                self.db.set_task_retry(request.task_id, result_error or "retryable_error", "retry requested by lane")
                            task = self.db.get_task(request.task_id)
                            failures = int(task["retry_count"]) if task is not None else MAX_TASK_RETRIES + 1
                            if failures > MAX_TASK_RETRIES:
                                self.db.set_task_final_failure(request.task_id, result_error or "retryable_error", "failure budget exhausted")
                                diagnosis = dict(diagnosis)
                                diagnosis.update({"status": "FAILED_FINAL", "retryable": False, "terminal": True})
                                result["status"] = "FAILED_FINAL"
                                result["diagnosis"] = diagnosis
                                agent.on_finished(request, result, diagnosis)
                                self._report_progress(agent, request, "失败次数已达上限")
                                if agent.state != "CLOSED":
                                    self.ready.append(company_key)
                                next_phase = agent.requests[0].phase if agent.requests else request.phase
                                self.db.set_company_state(self.run_id, company_key, agent.state, next_phase)
                                continue
                            # 统一官网抓取任务内部已完成动态→静态回退；两者都失败才在这里记一次 RETRY。
                            agent.requests.appendleft(request)
                            self.console("RETRY_QUEUED", agent, request, attempt=self.attempt_counts.get(request.task_id, 0), error=result_error or result_status)
                            self._report_progress(agent, request, f"本次未成功，等待重试（{result_error or result_status}）")
                        else:
                            # 放行态（SUCCEEDED / EMPTY_SUCCESS，或重试预算耗尽后的 FAILED_FINAL）：
                            # 全部走 on_finished 推进，企业永不卡死。EXPORT 是死代码，留兜底。
                            agent.on_finished(request, result, diagnosis)
                            if request.phase == "WEB_CRAWL":
                                web_summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
                                activity = web_summary.get("domain_activity") if isinstance(web_summary.get("domain_activity"), dict) else {}
                                if activity:
                                    self.console("DOMAIN_ACTIVITY", agent, request, **activity)
                            self._report_progress(agent, request)
                            progress = self._phase_progress(agent, request.phase)
                            if request.phase == "WEB_ICP" and progress["waiting"] == 0 and progress["running"] == 0:
                                self.console("ICP_SUMMARY", agent, request, summary=self._icp_summary(agent))
                            if agent.export_requested and agent.state == "CLOSED":
                                self._write_company_export(agent)
                        if agent.state != "CLOSED" and (agent.requests or agent.inflight == 0):
                            self.ready.append(company_key)
                        next_phase = agent.requests[0].phase if agent.requests else request.phase
                        self.db.set_company_state(self.run_id, company_key, agent.state, next_phase)
                else:
                    if not self.ready:
                        break  # 全 CLOSED 且无在途，结束
                    # 无在途、全部 lane 在冷却/刷新中：等到探测点或刷新完成（notify）再回来
                    with self._resource_condition:
                        self._resource_condition.wait(timeout=wake_timeout)
        status = "COMPLETED" if all(agent.state == "CLOSED" for agent in self.agents.values()) else "INCOMPLETE"
        for company_key, agent in self.agents.items():
            self.db.set_company_state(
                self.run_id, company_key, agent.state,
                agent.requests[0].phase if agent.requests else "EXPORT",
            )
        all_defects = [d for a in self.agents.values() for d in a.defects]
        self._write_run_defects(all_defects)
        if all_defects:
            self.console("SCRIPT_DEFECTS_FOUND", count=len(all_defects), see=str((self.run_dir / "defects.md").relative_to(self.output_dir)))
        self.db.finish_run(self.run_id, status)
        execution_summary = self.db.execution_summary(self.run_id)
        self.db.event(uuid4().hex, "run", self.run_id, "run_finished", {
            "status": status,
            "summary": self.db.summary(self.run_id),
            "execution_summary": execution_summary,
            "replay_strategy": "latest_company_history" if self.replay_enabled else None,
            "defect_count": len(all_defects),
        })
        self.console("RUN_FINISHED", status=status, summary=self.db.summary(self.run_id), defects=len(all_defects))
        (self.run_dir / "manifest.json").write_text(json.dumps({
            "run_id": self.run_id,
            "status": status,
            "summary": self.db.summary(self.run_id),
            "execution_summary": execution_summary,
            "replay_strategy": "latest_company_history" if self.replay_enabled else None,
            "has_script_defects": bool(all_defects),
            "defect_count": len(all_defects),
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self.db.close()
        return 0 if status == "COMPLETED" else 2


def _single_instance_lock_path() -> Path:
    return _PROJECT_ROOT / "pipeline_output" / "main_agent.lock"


def _pid_alive(pid: int) -> bool:
    """Windows/Linux 通用进程存活检测（纯标准库）。"""
    if os.name == "nt":
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                return False
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_single_instance_lock() -> bool:
    """单实例锁：已有存活实例则返回 False（调用方直接退出）。

    崩溃/强杀留下的陈旧锁文件会被进程存活检测识别并覆盖，不会卡死重启。
    """
    lock_path = _single_instance_lock_path()
    try:
        if lock_path.is_file():
            try:
                pid = int(lock_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                pid = None
            if pid is not None and _pid_alive(pid):
                print(f"另一个 main_agent 实例正在运行 (PID {pid})，已退出。", file=sys.stderr)
                return False
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(str(os.getpid()), encoding="utf-8")
        return True
    except OSError as exc:
        print(f"[lock] 无法写入单实例锁：{exc}", file=sys.stderr)
        return False


def release_single_instance_lock() -> None:
    try:
        lock_path = _single_instance_lock_path()
        if lock_path.is_file():
            try:
                pid = int(lock_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                pid = None
            if pid == os.getpid():
                lock_path.unlink(missing_ok=True)
    except OSError:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Main agent for multi-enterprise leased pipeline")
    parser.add_argument("--input", type=Path, default=Path(__file__).resolve().parents[2] / "config" / "companies.txt")
    parser.add_argument(
        "--replay", action="store_true",
        help="replay companies listed in config/replay_companies.txt from their latest matching historical results",
    )
    parser.add_argument(
        "--replay-companies-file", type=Path, default=DEFAULT_REPLAY_COMPANIES_FILE,
        help="company selector file used with --replay",
    )
    parser.add_argument(
        "--skip-support-services", action="store_true",
        help="do not start ICP/OCR helpers (for replay/debug runs whose reusable history does not need them)",
    )
    args = parser.parse_args()

    if not acquire_single_instance_lock():
        return 1

    result = 1
    agent: MainAgent | None = None
    support_services_started = False
    try:
        if not args.skip_support_services:
            start_support_services()
            support_services_started = True
        agent = MainAgent(
            args.input, _PROJECT_ROOT / "pipeline_output", sys.executable,
            db_path=None, replay=args.replay,
            replay_companies_file=args.replay_companies_file,
            full_crawl=True,
        )
        result = agent.run()
    except KeyboardInterrupt:
        # Ctrl+C/host interruption is resumable work, not a clean completed
        # run.  Persist that state before releasing the single-instance lock.
        if agent is not None:
            try:
                agent.db.mark_run_incomplete(agent.run_id)
            except Exception as state_exc:
                print(f"main agent interrupt cleanup failed: {state_exc}", file=sys.stderr)
        result = 130
    except Exception as exc:
        if agent is not None:
            try:
                agent.db.mark_run_incomplete(agent.run_id)
            except Exception as state_exc:
                if hasattr(exc, "add_note"):
                    exc.add_note(f"failed to mark run incomplete: {state_exc}")
        print(f"main agent failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        result = 1
    finally:
        if agent is not None:
            try:
                agent.db.close()
            except Exception:
                if result == 0:
                    result = 1
        if support_services_started and not stop_support_services() and result == 0:
            result = 1
        release_single_instance_lock()
    return result


if __name__ == "__main__":
    raise SystemExit(main())
