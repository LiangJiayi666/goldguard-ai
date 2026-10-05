# -*- coding: utf-8 -*-
"""企业排查控制台看板后端。

提供：
- 静态看板页面服务
- 批次数据 API（复用 pipeline-query report 脚本）
- main_agent 进程启停控制
- 实时日志 SSE 推送
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Any

from aiohttp import web


# 项目根目录（server.py 位于 dashboard/ 下）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
LOG_DIR = PROJECT_ROOT / "logs"
LOCK_PATH = PROJECT_ROOT / "pipeline_output" / "main_agent.lock"
MAIN_AGENT = PROJECT_ROOT / "src" / "control" / "main_agent.py"
PYTHON_EXE = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
REPLAY_BAT = PROJECT_ROOT / "replay.bat"
REPORT_DIR = PROJECT_ROOT / "scripts" / "report"

# 全局运行状态
_state: dict[str, Any] = {
    "proc": None,              # subprocess.Popen 对象
    "log_file": None,          # 当前日志文件路径
    "log_handle": None,        # 当前日志文件句柄
    "start_time": None,
    "run_id": None,
    "error_message": None,
    "error_level": None,       # None / "warning" / "critical"
    "silent_fail_count": 0,    # 连续"静默失败"（rc!=0 且零输出）次数
}

# 启动参数（供自动重启时原样传给新实例）
_START_ARGS: list[str] = []


def _log(msg: str) -> None:
    """控制台日志，用于后端自身调试。"""
    print(f"[dashboard] {msg}", flush=True)


def _is_silent_fail(completed) -> bool:
    """判断是否为"静默失败"：returncode!=0 且 stdout/stderr 全空。

    Windows 上 aiohttp 服务进程在特定启动方式/运行一段时间后，spawn 的子进程
    会在 Python 初始化阶段静默退出（rc=1、零输出）。该状态无法在进程内恢复，
    但重启进程即可恢复，因此由 _record_silent_fail 计数并触发自动重启。
    """
    if completed.returncode == 0:
        return False
    out = (completed.stdout or "").strip()
    err = (completed.stderr or "").strip()
    return not out and not err


def _record_silent_fail(script: str, completed) -> None:
    """累计静默失败；连续 2 次即自动重启服务进程（重启可恢复该状态）。"""
    if not _is_silent_fail(completed):
        _state["silent_fail_count"] = 0
        return
    _state["silent_fail_count"] = _state.get("silent_fail_count", 0) + 1
    _log(f"{script} 子进程静默失败（rc={completed.returncode}，零输出），"
         f"累计 {_state['silent_fail_count']}/2 次")
    if _state["silent_fail_count"] >= 2:
        _restart_self()


def _restart_self() -> None:
    """启动新实例并退出当前进程。仅用于修复"子进程静默失败"的损坏状态。"""
    count = int(os.environ.get("DASHBOARD_RESTART_COUNT", "0")) + 1
    if count > 3:
        _log("自动重启次数超过上限（3 次），停止自愈，请手动重启看板")
        return
    _log(f"服务进程疑似损坏，自动重启看板服务（第 {count} 次）...")
    script = Path(__file__).resolve()
    args = [sys.executable, str(script), *_START_ARGS]
    env = os.environ.copy()
    env["DASHBOARD_RESTART_COUNT"] = str(count)
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_handle = open(LOG_DIR / "dashboard_restart.log", "w", encoding="utf-8")
        subprocess.Popen(
            args,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            # 不设 CREATE_NO_WINDOW：新实例需继承当前控制台，否则会复现"子进程静默失败"
        )
        _log(f"新实例已启动: {' '.join(args)}")
    except Exception as exc:
        _log(f"自动重启失败: {exc}")
        return
    time.sleep(2)
    os._exit(0)


def _running_pid() -> int | None:
    """若 lock 文件存在且指向存活进程，返回 PID；否则返回 None。"""
    if not LOCK_PATH.is_file():
        return None
    try:
        pid = int(LOCK_PATH.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    try:
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return None
        ctypes.windll.kernel32.CloseHandle(handle)
        return pid
    except Exception:
        return None


def _is_our_python(pid: int) -> bool:
    """简单校验：lock 中的 PID 是否为本项目 .venv 的 python.exe。

    仅用于避免 Conda/其他 Python 进程被误判为 main_agent 实例。
    """
    if sys.platform != "win32":
        return True
    try:
        import ctypes
        from ctypes import wintypes
        psapi = ctypes.WinDLL("psapi")
        k32 = ctypes.windll.kernel32
        h = k32.OpenProcess(0x1000 | 0x0010, False, pid)
        if not h:
            return False
        try:
            buf = ctypes.create_unicode_buffer(1024)
            size = wintypes.DWORD(1024)
            if psapi.GetProcessImageFileNameW(h, buf, size):
                exe = buf.value.replace("\\Device\\HarddiskVolume", "").lstrip("0123456789/\\")
                expected = str(PYTHON_EXE).replace("/", "\\").lstrip("C:\\").lower()
                return exe.lower().endswith(expected) or expected.endswith(exe.lower())
        finally:
            k32.CloseHandle(h)
    except Exception:
        pass
    return False


def _cleanup_stale_lock() -> dict[str, Any]:
    """清理陈旧的单实例锁，返回清理结果。"""
    if not LOCK_PATH.is_file():
        return {"had_lock": False, "removed": False, "reason": "无锁文件"}
    pid = _running_pid()
    if pid is None:
        try:
            LOCK_PATH.unlink()
            return {"had_lock": True, "removed": True, "reason": f"PID 已死，自动删除"}
        except OSError as exc:
            return {"had_lock": True, "removed": False, "reason": f"删除失败: {exc}"}
    if not _is_our_python(pid):
        try:
            LOCK_PATH.unlink()
            return {"had_lock": True, "removed": True, "reason": f"PID {pid} 不是本项目 python，自动删除"}
        except OSError as exc:
            return {"had_lock": True, "removed": False, "reason": f"删除失败: {exc}"}
    return {"had_lock": True, "removed": False, "reason": f"PID {pid} 正在运行"}


def _child_env() -> dict[str, str]:
    """构造给 report 子进程的干净环境。

    问题背景：看板若由 Conda 的 pythonw 启动，os.environ 会携带 Conda 的
    PYTHONPATH / PATH(DLL) 等，导致 venv 的 python 子进程加载到 Conda 的
    不兼容原生库（如 PIL / python-docx 的 C 扩展），表现为静默崩溃
    （returncode!=0、stdout/stderr 全空）。这里剥离污染项，仅保留 venv 与
    系统必要路径，确保子进程始终在纯净 venv 环境下运行。
    """
    env = os.environ.copy()
    for k in ("PYTHONPATH", "PYTHONHOME", "CONDA_PREFIX", "CONDA_DEFAULT_ENV",
              "CONDA_PYTHON_EXE", "VIRTUAL_ENV", "PYTHONSTARTUP"):
        env.pop(k, None)
    # PATH：去掉 conda / LenovoSoftstore 相关目录，确保 venv Scripts 在最前
    venv_scripts = str(PROJECT_ROOT / ".venv" / "Scripts")
    parts = env.get("PATH", "").split(os.pathsep)
    kept: list[str] = []
    for p in parts:
        pl = p.lower()
        if "conda" in pl or "lenovosoftstore" in pl:
            continue
        kept.append(p)
    seen: set[str] = set()
    dedup: list[str] = []
    for p in [venv_scripts] + kept:
        if p and p not in seen:
            seen.add(p)
            dedup.append(p)
    env["PATH"] = os.pathsep.join(dedup)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def _debug_dump(script: str, completed) -> None:
    """把子进程的输出落盘，便于排查「静默失败」（stdout/stderr 全空）。"""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_DIR / "export_debug.log", "a", encoding="utf-8") as fh:
            fh.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {script} rc={completed.returncode}\n")
            fh.write(f"STDOUT:\n{completed.stdout}\n")
            fh.write(f"STDERR:\n{completed.stderr}\n")
            fh.write("-" * 60 + "\n")
    except Exception:
        pass


def _invoke_report(script: str, *args: str) -> dict[str, Any]:
    """调用 scripts/report 脚本并读取 JSON。"""
    env = _child_env()
    completed = subprocess.run(
        [str(PYTHON_EXE), str(REPORT_DIR / script), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(PROJECT_ROOT),
        env=env,
        stdin=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    if completed.returncode != 0:
        _record_silent_fail(script, completed)
        raise RuntimeError(f"{script} 失败: {completed.stderr}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{script} 输出非法 JSON: {exc}") from exc


async def _run_report_async(script: str, *args: str) -> dict[str, Any]:
    """异步执行 report 脚本（避免阻塞 aiohttp 事件循环）。"""
    _log(f"开始执行脚本: {script} {' '.join(args)}")
    loop = asyncio.get_running_loop()

    def _run():
        env = _child_env()
        return subprocess.run(
            [str(PYTHON_EXE), str(REPORT_DIR / script), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )

    completed = await loop.run_in_executor(None, _run)
    _log(f"脚本 {script} 执行完成，returncode={completed.returncode}")
    if completed.returncode != 0:
        _debug_dump(script, completed)
    # 某些脚本（如 extract_company.py）在"数据尚未就绪"时返回非零，但仍输出合法 JSON 提示信息；
    # 优先尝试解析 stdout，能解析则返回，解析失败再按错误处理。
    try:
        parsed = json.loads(completed.stdout)
        if completed.returncode != 0 and isinstance(parsed, dict) and "error" in parsed:
            return parsed
        return parsed
    except json.JSONDecodeError as exc:
        if completed.returncode != 0:
            _record_silent_fail(script, completed)
            raise RuntimeError(f"{script} 失败: {completed.stderr}") from exc
        raise RuntimeError(f"{script} 输出非法 JSON: {exc}") from exc


def _safe_download_path(raw: str) -> Path | None:
    """校验下载路径，防止目录遍历；仅允许 project root 内的文件。"""
    try:
        target = (PROJECT_ROOT / raw.lstrip("/\\")).resolve()
        if not str(target).startswith(str(PROJECT_ROOT.resolve())):
            return None
        return target
    except Exception:
        return None


def _active_log_file() -> str | None:
    """main_agent 已先行运行（看板后开）时，自动发现其日志文件。

    看板点"开始/复跑"启动的进程会把 log_file 写进 _state；但若 main_agent 是
    通过 run.bat 先启动、之后才打开看板，_state 里没有日志路径。此时只要
    lock 文件指向存活进程，就取 logs/ 下最新写入的 main_agent_*.log 作为
    实时日志来源（main_agent 每次启动只写一个日志文件）。
    """
    if _running_pid() is None:
        return None
    if not LOG_DIR.is_dir():
        return None
    candidates = sorted(
        LOG_DIR.glob("main_agent_*.log"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return str(candidates[0]) if candidates else None


async def api_runs_latest(request: web.Request) -> web.Response:
    """返回最新批次的看板数据包。

    数据由 dashboard_worker.py（普通 python 进程）定时生成到
    pipeline_output/dashboard_latest.json，这里只读文件、不 spawn 子进程，
    规避 Windows 上 aiohttp 进程 spawn 子进程的"静默失败"问题。
    """
    try:
        latest = PROJECT_ROOT / "pipeline_output" / "dashboard_latest.json"
        if not latest.is_file():
            return web.json_response(
                {"ok": False, "error": "看板数据尚未生成（worker 未运行），请重启 dashboard.bat"}, status=503)
        data = json.loads(latest.read_text(encoding="utf-8"))
        return web.json_response({"ok": True, "data": data})
    except Exception as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_runs(request: web.Request) -> web.Response:
    """返回所有批次列表（读 worker 生成的 runs_list.json）。"""
    try:
        runs_file = PROJECT_ROOT / "pipeline_output" / "runs_list.json"
        if not runs_file.is_file():
            return web.json_response(
                {"ok": False, "error": "批次列表尚未生成（worker 未运行），请重启 dashboard.bat"}, status=503)
        data = json.loads(runs_file.read_text(encoding="utf-8"))
        return web.json_response({"ok": True, "data": data})
    except Exception as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_run_detail(request: web.Request) -> web.Response:
    """返回指定批次看板数据（latest 读 worker 文件；其他批次回退原逻辑）。"""
    run_id = request.match_info.get("run_id", "latest")
    if run_id in (None, "", "latest"):
        return await api_runs_latest(request)
    try:
        data = await _run_report_async("extract_dashboard.py", "--run", run_id)
        if not data or not data.get("run_id"):
            return web.json_response({"ok": False, "error": "extract_dashboard.py 未返回有效数据"}, status=500)
        return web.json_response({"ok": True, "data": data})
    except Exception as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_run_progress(request: web.Request) -> web.Response:
    """直接从 DB 查询某批次的实时企业进度（不依赖 exports/*.json）。"""
    run_id = request.match_info.get("run_id", "latest")
    db_path = PROJECT_ROOT / "pipeline_output" / "data" / "main_agent.db"
    if not db_path.is_file():
        return web.json_response({"ok": False, "error": "数据库不存在"}, status=404)

    import sqlite3

    try:
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row

        # 如果请求 latest，先解析为真实 run_id
        if run_id in (None, "", "latest"):
            row = con.execute(
                "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if not row:
                con.close()
                return web.json_response({"ok": False, "error": "无批次记录"}, status=404)
            run_id = row["run_id"]

        companies = con.execute(
            "SELECT company_id, company_name, status FROM companies WHERE run_id=?",
            (run_id,),
        ).fetchall()

        tasks = con.execute(
            "SELECT company_id, phase, status, COUNT(*) AS n "
            "FROM tasks WHERE run_id=? GROUP BY company_id, phase, status",
            (run_id,),
        ).fetchall()

        # 统计每家企业任务状态，并按阶段聚合
        task_counts: dict[str, dict[str, int]] = {}
        phase_counts: dict[str, dict[str, dict[str, int]]] = {}
        company_phase: dict[str, str] = {}
        for t in tasks:
            cid = t["company_id"]
            phase = t["phase"]
            status = t["status"]
            task_counts.setdefault(cid, {})
            task_counts[cid][status] = task_counts[cid].get(status, 0) + int(t["n"])
            phase_stats = phase_counts.setdefault(cid, {}).setdefault(phase, {})
            phase_stats[status] = phase_stats.get(status, 0) + int(t["n"])
            # 当前阶段取 "最新的非终态阶段"，若全部终态则取最后阶段
            if status in ("RUNNING", "PENDING", "RETRY_WAIT"):
                company_phase[cid] = phase

        # 为尚未开始任务的企业兜底阶段
        company_rows = []
        total = closed = failed = running = pending = 0
        for c in companies:
            cid = c["company_id"]
            counts = task_counts.get(cid, {})
            status = c["status"] or "OPEN"
            phase = company_phase.get(cid, "-" if not counts else "待判定")
            phase_stats = phase_counts.get(cid, {}).get(phase, {}) if phase not in ("-", "待判定") else {}
            phase_total = sum(phase_stats.values())
            phase_ok = phase_stats.get("OK", 0)
            total += 1
            if status == "CLOSED":
                closed += 1
            elif status == "FAILED":
                failed += 1
            elif status == "RUNNING":
                running += 1
            elif status == "PENDING":
                pending += 1
            company_rows.append({
                "企业": c["company_name"],
                "状态": status,
                "当前阶段": phase,
                "当前阶段进度": {"total": phase_total, "ok": phase_ok},
                "任务统计": {
                    "total": sum(counts.values()),
                    "ok": counts.get("OK", 0),
                    "failed": counts.get("FAILED", 0),
                    "running": counts.get("RUNNING", 0),
                    "pending": counts.get("PENDING", 0),
                    "retry_wait": counts.get("RETRY_WAIT", 0),
                },
            })

        con.close()
        return web.json_response({
            "ok": True,
            "run_id": run_id,
            "summary": {"total": total, "closed": closed, "open": total - closed, "failed": failed, "running": running, "pending": pending},
            "companies": company_rows,
        })
    except Exception as exc:
        return web.json_response({"ok": False, "error": f"查询实时进度失败: {exc}"}, status=500)


async def api_agent_status(request: web.Request) -> web.Response:
    """返回 main_agent 进程状态。"""
    proc = _state["proc"]
    pid = _running_pid()
    running = False
    if proc is not None and proc.poll() is None:
        running = True
    elif pid is not None:
        running = True

    return web.json_response({
        "ok": True,
        "running": running,
        "pid": pid,
        "run_id": _state.get("run_id"),
        "start_time": _state.get("start_time"),
        "error_message": _state.get("error_message"),
        "error_level": _state.get("error_level"),
    })


async def api_agent_start(request: web.Request) -> web.Response:
    """启动 main_agent；若已有旧实例/残留 lock，先自动停止旧实例再启动。"""
    resolved = _force_stop_running_instance("开始排查前自动清理")
    if resolved["stopped"]:
        _log(f"启动前自动清理旧实例：{resolved['details']}")

    # 检查必要配置
    companies_file = PROJECT_ROOT / "config" / "companies.txt"
    llm_config = PROJECT_ROOT / "config" / "llm_config.txt"
    if not companies_file.is_file() or not companies_file.read_text(encoding="utf-8").strip():
        _state["error_message"] = "config/companies.txt 为空或缺失，请填写待排查企业"
        _state["error_level"] = "critical"
        return web.json_response({"ok": False, "error": _state["error_message"]}, status=400)
    if not llm_config.is_file() or "<在此填入你的API密钥>" in llm_config.read_text(encoding="utf-8"):
        _state["error_message"] = "config/llm_config.txt 尚未填写 API key"
        _state["error_level"] = "critical"
        return web.json_response({"ok": False, "error": _state["error_message"]}, status=400)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"main_agent_{timestamp}.log"
    _state["log_file"] = str(log_file)
    _state["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _state["error_message"] = None
    _state["error_level"] = None

    try:
        handle = open(log_file, "w", encoding="utf-8")
        _state["log_handle"] = handle
        # 强制子进程使用 UTF-8 输出中文，避免 Windows 控制台默认 GBK 导致日志乱码
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        proc = subprocess.Popen(
            [str(PYTHON_EXE), str(MAIN_AGENT)],
            stdout=handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            cwd=str(PROJECT_ROOT),
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
        )
        _state["proc"] = proc
        _state["run_id"] = None  # 启动后由日志或状态接口推断
        _log(f"启动 main_agent，PID={proc.pid}，日志={log_file}")
        return web.json_response({"ok": True, "pid": proc.pid, "log_file": str(log_file)})
    except Exception as exc:
        _state["error_message"] = f"启动失败: {exc}"
        _state["error_level"] = "critical"
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_agent_replay(request: web.Request) -> web.Response:
    """启动 replay.bat（复跑企业排查）。

    支持通过 JSON body 传入 {companies: ["企业1", "企业2"]}，会自动写入
    config/replay_companies.txt 后再启动；若 body 为空则读取现有文件。
    """
    resolved = _force_stop_running_instance("复跑排查前自动清理")
    if resolved["stopped"]:
        _log(f"复跑前自动清理旧实例：{resolved['details']}")

    replay_companies_file = PROJECT_ROOT / "config" / "replay_companies.txt"
    llm_config = PROJECT_ROOT / "config" / "llm_config.txt"

    # 若前端传入企业列表，先写入 replay_companies.txt
    try:
        body = await request.json()
    except Exception:
        body = {}
    companies = body.get("companies") if isinstance(body, dict) else None
    if isinstance(companies, list) and companies:
        replay_companies_file.parent.mkdir(parents=True, exist_ok=True)
        replay_companies_file.write_text("\n".join(str(c).strip() for c in companies if str(c).strip()) + "\n", encoding="utf-8")
        _log(f"已写入复跑企业 {len(companies)} 家到 {replay_companies_file}")

    # 检查必要配置
    if not replay_companies_file.is_file() or not replay_companies_file.read_text(encoding="utf-8").strip():
        _state["error_message"] = "config/replay_companies.txt 为空或缺失，请填写待复跑企业"
        _state["error_level"] = "critical"
        return web.json_response({"ok": False, "error": _state["error_message"]}, status=400)
    if not llm_config.is_file() or "<在此填入你的API密钥>" in llm_config.read_text(encoding="utf-8"):
        _state["error_message"] = "config/llm_config.txt 尚未填写 API key"
        _state["error_level"] = "critical"
        return web.json_response({"ok": False, "error": _state["error_message"]}, status=400)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"main_agent_replay_{timestamp}.log"
    _state["log_file"] = str(log_file)
    _state["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _state["error_message"] = None
    _state["error_level"] = None

    try:
        handle = open(log_file, "w", encoding="utf-8")
        _state["log_handle"] = handle
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        proc = subprocess.Popen(
            [str(REPLAY_BAT)],
            stdout=handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            cwd=str(PROJECT_ROOT),
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
        )
        _state["proc"] = proc
        _state["run_id"] = None
        _log(f"启动复跑排查，PID={proc.pid}，日志={log_file}")
        return web.json_response({"ok": True, "pid": proc.pid, "log_file": str(log_file)})
    except Exception as exc:
        _state["error_message"] = f"复跑启动失败: {exc}"
        _state["error_level"] = "critical"
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


def _latest_run_id() -> str | None:
    """返回台账中最近开始的批次 run_id。"""
    db_path = PROJECT_ROOT / "pipeline_output" / "data" / "main_agent.db"
    if not db_path.is_file():
        return None
    import sqlite3
    try:
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        con.close()
        return row["run_id"] if row else None
    except Exception:
        return None


def _send_stop_command(run_id: str | None = None) -> dict[str, Any]:
    """向 main_agent 写入 STOP 控制命令；未传 run_id 时取最新批次。"""
    db_path = PROJECT_ROOT / "pipeline_output" / "data" / "main_agent.db"
    if not db_path.is_file():
        return {"ok": False, "reason": "数据库不存在"}
    rid = run_id or _latest_run_id()
    if not rid:
        return {"ok": False, "reason": "未找到运行中批次"}
    import sqlite3
    import uuid
    try:
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        # 检查该批次是否已有未处理 STOP，避免重复写入
        existing = con.execute(
            "SELECT 1 FROM control_commands WHERE run_id=? AND action='STOP' AND status='PENDING'",
            (rid,),
        ).fetchone()
        if not existing:
            con.execute(
                """INSERT INTO control_commands
                (command_id, run_id, action, company_id, phase, reason, status, created_at)
                VALUES (?, ?, 'STOP', '', '', 'dashboard stop button', 'PENDING', ?)""",
                (str(uuid.uuid4()), rid, time.strftime("%Y-%m-%dT%H:%M:%S")),
            )
            con.commit()
        con.close()
        return {"ok": True, "run_id": rid}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}


def _kill_process_tree(pid: int) -> bool:
    """强制终止指定 PID 及其子进程。Windows 下使用 taskkill /F /T，其余平台用 SIGTERM。"""
    if sys.platform != "win32":
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except Exception:
            return False
    try:
        completed = subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        # taskkill 返回 128 等也可能已经终止，stdout 含“已经终止”即算成功
        return completed.returncode == 0 or "已经终止" in (completed.stdout or "")
    except Exception:
        return False


def _terminate_proc(proc) -> None:
    """尽量优雅终止子进程：先尝试 CTRL_BREAK（失败忽略），超时则 taskkill /F /T。

    Windows 下 pythonw 进程无法可靠投递 CTRL_BREAK_EVENT（会抛异常），因此该步骤
    仅作 best-effort；真正的兜底由 taskkill /F /T 完成。
    """
    try:
        if sys.platform == "win32":
            try:
                os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
            except Exception:
                pass
        else:
            proc.terminate()
    except Exception:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc.pid)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    except Exception:
        pass


def _force_stop_running_instance(reason: str = "启动前自动清理") -> dict[str, Any]:
    """强制停止任何运行中的 main_agent 实例（看板持有的子进程或 lock 指向的 PID）。

    用于「开始排查」/「复跑排查」前自动清理旧实例，避免多实例并发写同一批数据。
    返回 {"stopped": bool, "details": [str, ...]}。
    """
    details: list[str] = []
    stopped_any = False

    # 1) 看板直接持有的子进程
    proc = _state.get("proc")
    if proc is not None and proc.poll() is None:
        try:
            _send_stop_command(_state.get("run_id"))
        except Exception:
            pass
        try:
            _terminate_proc(proc)
        except Exception as exc:
            details.append(f"停止持有进程 PID {proc.pid} 异常: {exc}")
        finally:
            if _state.get("log_handle"):
                try:
                    _state["log_handle"].close()
                except Exception:
                    pass
                _state["log_handle"] = None
            _state["proc"] = None
        stopped_any = True
        details.append(f"已停止看板持有的主进程 PID {proc.pid}")

    # 2) lock 指向的 PID（看板重启后 _state 丢失的场景）
    pid = _running_pid()
    if pid is not None:
        _kill_process_tree(pid)
        stopped_any = True
        details.append(f"已按 lock 停止 PID {pid}")

    # 3) 清理残留 lock 文件
    lock_info = _cleanup_stale_lock()
    if lock_info.get("had_lock") and lock_info.get("removed"):
        details.append("已清理残留 lock 文件")
    elif lock_info.get("had_lock") and not lock_info.get("removed"):
        details.append(f"lock 清理失败: {lock_info.get('reason')}")

    return {"stopped": stopped_any, "details": details}


async def api_agent_stop(request: web.Request) -> web.Response:
    """停止 main_agent：先写入 STOP 控制命令让 main_agent 优雅退出，再兜底杀进程。"""
    proc = _state["proc"]
    killed_pid = None

    # 统一手段1：写入 STOP 控制命令；main_agent 的 _poll_control_commands 会识别并退出。
    stop_cmd = _send_stop_command(_state.get("run_id"))
    _log(f"写入 STOP 控制命令: {stop_cmd}")

    # 情况1：dashboard 直接持有子进程对象
    if proc is not None and proc.poll() is None:
        try:
            _terminate_proc(proc)
            killed_pid = proc.pid
        except Exception as exc:
            _log(f"终止持有进程异常: {exc}")
        finally:
            if _state["log_handle"]:
                try:
                    _state["log_handle"].close()
                except Exception:
                    pass
                _state["log_handle"] = None
            _state["proc"] = None

    # 情况2：dashboard 重启后 _state 丢失，按 lock PID 兜底
    pid = _running_pid()
    if pid is not None and pid != killed_pid:
        _kill_process_tree(pid)

    lock_info = _cleanup_stale_lock()
    messages = []
    if killed_pid:
        messages.append(f"已停止主进程 PID {killed_pid}")
    if pid and pid != killed_pid:
        messages.append(f"已按 lock 尝试停止 PID {pid}")
    if stop_cmd.get("ok"):
        messages.append(f"已向批次 {stop_cmd.get('run_id')} 发送 STOP 命令")
    if not messages:
        messages.append("进程已停止或不存在")
    return web.json_response({"ok": True, "message": "；".join(messages), "lock": lock_info})


async def api_agent_log(request: web.Request) -> web.StreamResponse:
    """SSE 实时日志流（纯 aiohttp StreamResponse 实现，无需额外依赖）。"""
    response = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )
    await response.prepare(request)

    log_file = _state.get("log_file") or _active_log_file()
    last_size = 0
    if log_file and os.path.exists(log_file):
        last_size = os.path.getsize(log_file)

    try:
        while True:
            current_file = _state.get("log_file") or _active_log_file()
            if current_file and os.path.exists(current_file):
                if current_file != log_file:
                    log_file = current_file
                    last_size = 0
                size = os.path.getsize(log_file)
                if size > last_size:
                    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(last_size)
                        chunk = f.read(size - last_size)
                    last_size = size
                    if chunk:
                        payload = f"data: {json.dumps({'text': chunk}, ensure_ascii=False)}\n\n"
                        await response.write(payload.encode("utf-8"))
            await asyncio.sleep(1)
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    return response


async def api_company(request: web.Request) -> web.Response:
    """单企业完整结论（调用 extract_company.py）。"""
    run_id = request.query.get("run", "latest")
    company = request.query.get("company", "").strip()
    section = request.query.get("section", "all")
    if not company:
        return web.json_response({"ok": False, "error": "缺少 company 参数"}, status=400)
    try:
        result = await _run_report_async("extract_company.py", "--run", run_id, "--company", company, "--section", section)
        if isinstance(result, dict) and result.get("error"):
            return web.json_response({"ok": False, "error": result["error"], "hint": result.get("hint"), "data": result}, status=404)
        return web.json_response({"ok": True, "data": result})
    except Exception as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_phase(request: web.Request) -> web.Response:
    """单企业某阶段产物（调用 extract_phase.py）。"""
    run_id = request.query.get("run", "latest")
    company = request.query.get("company", "").strip()
    phase = request.query.get("phase", "").strip()
    kind = request.query.get("kind", "summary")
    if not company or not phase:
        return web.json_response({"ok": False, "error": "缺少 company 或 phase 参数"}, status=400)
    try:
        result = await _run_report_async("extract_phase.py", "--run", run_id, "--company", company, "--phase", phase, "--kind", kind)
        return web.json_response({"ok": True, "data": result})
    except Exception as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_task_io(request: web.Request) -> web.Response:
    """单企业某阶段任务输入输出（调用 extract_task_io.py）。"""
    run_id = request.query.get("run", "latest")
    company = request.query.get("company", "").strip()
    phase = request.query.get("phase", "").strip()
    round_no = request.query.get("round", "last")
    if not company or not phase:
        return web.json_response({"ok": False, "error": "缺少 company 或 phase 参数"}, status=400)
    try:
        result = await _run_report_async("extract_task_io.py", "--run", run_id, "--company", company, "--phase", phase, "--round", round_no)
        return web.json_response({"ok": True, "data": result})
    except Exception as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=500)


async def api_diag(request: web.Request) -> web.Response:
    """诊断端点：在服务进程内分别跑简单子进程与 extract_dashboard.py。"""
    import traceback
    results: dict[str, Any] = {}
    # 1) 简单 python 子进程（不嵌套）
    try:
        c = subprocess.run(
            [str(PYTHON_EXE), "-c", "print('hello-world')"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(PROJECT_ROOT), env=_child_env(), stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW, timeout=10,
        )
        results["simple"] = {"rc": c.returncode, "out": c.stdout[:50], "err": c.stderr[:100]}
    except Exception as exc:
        results["simple"] = {"error": str(exc), "tb": traceback.format_exc().splitlines()[-3:]}
    # 2) extract_dashboard.py（内部会再嵌套 spawn 4 个子脚本）
    try:
        c = subprocess.run(
            [str(PYTHON_EXE), str(REPORT_DIR / "extract_dashboard.py"), "--run", "latest"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(PROJECT_ROOT), env=_child_env(), stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW, timeout=30,
        )
        results["dashboard"] = {"rc": c.returncode, "stdout_len": len(c.stdout), "stderr_len": len(c.stderr), "stderr_head": c.stderr[:200]}
    except Exception as exc:
        results["dashboard"] = {"error": str(exc), "tb": traceback.format_exc().splitlines()[-3:]}
    # 3) simple + 默认环境（不传 env）
    try:
        c = subprocess.run(
            [str(PYTHON_EXE), "-c", "print('hello')"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(PROJECT_ROOT), stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW, timeout=10,
        )
        results["simple_default_env"] = {"rc": c.returncode, "out": c.stdout[:30], "err": c.stderr[:100]}
    except Exception as exc:
        results["simple_default_env"] = {"error": str(exc)}
    # 4) simple + 不带 CREATE_NO_WINDOW
    try:
        c = subprocess.run(
            [str(PYTHON_EXE), "-c", "print('hello')"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            cwd=str(PROJECT_ROOT), env=_child_env(), stdin=subprocess.DEVNULL,
            timeout=10,
        )
        results["simple_no_flag"] = {"rc": c.returncode, "out": c.stdout[:30], "err": c.stderr[:100]}
    except Exception as exc:
        results["simple_no_flag"] = {"error": str(exc)}
    return web.json_response(results)


async def api_file(request: web.Request) -> web.StreamResponse:
    """内联查看项目内文件（企业详情弹窗使用）。"""
    raw_path = request.query.get("path", "")
    target = _safe_download_path(raw_path)
    if not target or not target.is_file():
        return web.json_response({"ok": False, "error": "文件不存在或路径非法"}, status=404)
    mime = "image/png" if target.suffix.lower() in {".png"} else "image/jpeg" if target.suffix.lower() in {".jpg", ".jpeg"} else "application/octet-stream"
    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": mime},
    )
    await response.prepare(request)
    with open(target, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            await response.write(chunk)
    return response


async def index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(DASHBOARD_DIR / "index.html")


@web.middleware
async def _no_cache_static(request: web.Request, handler):
    """对首页与 /static/* 强制关缓存，避免改 app.js/index.html 后用户端依赖浏览器缓存而看不到新版本。"""
    response = await handler(request)
    path = request.path
    if path == "/" or path.startswith("/static/"):
        response.headers.setdefault("Cache-Control", "no-cache, no-store, must-revalidate")
        response.headers.setdefault("Pragma", "no-cache")
        response.headers.setdefault("Expires", "0")
    return response


def main() -> int:
    # Windows 下 aiohttp 默认使用 ProactorEventLoop（IOCP）；实测在部分启动方式
    # （如 start /min 新控制台 / pythonw）下，事件循环运行期间 spawn 的 report
    # 子进程会静默失败（rc=1、stdout/stderr 全空），看板数据接口稳定 500。
    # 强制使用 SelectorEventLoop 可避免该问题。
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 关键修复：stdin 失效会导致 spawn 子进程静默失败（rc=1、零输出）。
    # 后台启动（bash & / start /b）的进程，其 stdin 句柄随父会话关闭而失效，
    # 之后任何 subprocess 调用都会失败。必须用 SetStdHandle 修改
    # GetStdHandle(STD_INPUT_HANDLE) 的返回值（Python 的 dup2 改不了它）。
    if sys.platform == "win32":
        try:
            import ctypes
            import msvcrt as _msvcrt
            devnull = _os.open(_os.devnull, _os.O_RDWR)
            _os.dup2(devnull, 0)
            handle = _msvcrt.get_osfhandle(devnull)
            ctypes.windll.kernel32.SetStdHandle(-10, handle)  # STD_INPUT_HANDLE = -10
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="企业排查控制台看板")
    parser.add_argument("--port", type=int, default=18080, help="服务端口（默认 18080）")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址（默认 127.0.0.1）")
    args = parser.parse_args()
    _START_ARGS.extend(["--host", args.host, "--port", str(args.port)])

    app = web.Application(middlewares=[_no_cache_static])
    app.router.add_get("/", index)
    app.router.add_static("/static", DASHBOARD_DIR / "static", name="static")
    app.router.add_get("/api/runs", api_runs)
    app.router.add_get("/api/runs/latest", api_runs_latest)
    app.router.add_get("/api/runs/{run_id}", api_run_detail)
    app.router.add_get("/api/runs/{run_id}/progress", api_run_progress)
    app.router.add_get("/api/company", api_company)
    app.router.add_get("/api/phase", api_phase)
    app.router.add_get("/api/task_io", api_task_io)
    app.router.add_get("/api/agent/status", api_agent_status)
    app.router.add_post("/api/agent/start", api_agent_start)
    app.router.add_post("/api/agent/replay", api_agent_replay)
    app.router.add_post("/api/agent/stop", api_agent_stop)
    app.router.add_get("/api/agent/log", api_agent_log)
    app.router.add_get("/api/file", api_file)
    app.router.add_get("/api/diag", api_diag)

    _log(f"看板服务启动：http://{args.host}:{args.port}")
    # 端口绑定重试：自动重启的新实例可能在旧实例尚未完全释放端口时启动
    import socket as _socket
    for attempt in range(10):
        probe = _socket.socket()
        try:
            probe.bind((args.host, args.port))
            probe.close()
            break
        except OSError:
            probe.close()
            _log(f"端口 {args.port} 仍被占用，1 秒后重试（{attempt + 1}/10）")
            time.sleep(1)
    web.run_app(app, host=args.host, port=args.port, print=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
