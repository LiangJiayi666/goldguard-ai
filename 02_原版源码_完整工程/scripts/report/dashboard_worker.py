# -*- coding: utf-8 -*-
"""看板数据 worker：定时执行 report 脚本，把结果写入 JSON 文件。

背景：Windows 上 aiohttp 服务进程 spawn 子进程存在"静默失败"（子进程 rc=1 且
stdout/stderr 全空，/api/runs/latest 稳定 500）。而普通 python 进程反复 spawn
report 子进程稳定。因此由本 worker（普通 python 进程）代跑 extract_dashboard.py
与 extract_runs.py，server.py 只读生成的文件，彻底规避该问题。

输出：
- pipeline_output/dashboard_latest.json  （/api/runs/latest 数据包）
- pipeline_output/runs_list.json         （/api/runs 批次列表）

用法：python scripts/report/dashboard_worker.py
可选环境变量：DASH_WORKER_INTERVAL=秒（默认 8）
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
REPORT = ROOT / "scripts" / "report"
OUT_DIR = ROOT / "pipeline_output"
INTERVAL = float(os.environ.get("DASH_WORKER_INTERVAL", "8"))
LAST_RUN = OUT_DIR / "dashboard_latest.json"
RUNS_LIST = OUT_DIR / "runs_list.json"


def _clean_env() -> dict:
    env = os.environ.copy()
    for k in ("PYTHONPATH", "PYTHONHOME", "CONDA_PREFIX", "CONDA_DEFAULT_ENV",
              "CONDA_PYTHON_EXE", "VIRTUAL_ENV", "PYTHONSTARTUP"):
        env.pop(k, None)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


def _run(script: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(PYTHON), str(REPORT / script), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(ROOT),
        env=_clean_env(),
        stdin=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        timeout=90,
    )


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def main() -> int:
    # 关键修复：stdin 失效会导致 spawn 子进程静默失败（rc=1、零输出）。
    # 后台启动的进程 stdin 随父会话关闭而失效；必须用 SetStdHandle 修改
    # GetStdHandle(STD_INPUT_HANDLE) 的返回值（Python 的 dup2 改不了它）。
    if sys.platform == "win32":
        try:
            import ctypes
            import msvcrt
            devnull = os.open(os.devnull, os.O_RDWR)
            os.dup2(devnull, 0)
            handle = msvcrt.get_osfhandle(devnull)
            ctypes.windll.kernel32.SetStdHandle(-10, handle)  # STD_INPUT_HANDLE = -10
        except Exception:
            pass
    fails = 0
    print(f"[worker] 启动，输出到 {LAST_RUN.name} / {RUNS_LIST.name}，间隔 {INTERVAL}s", flush=True)
    while True:
        t0 = time.time()
        ok = True
        try:
            c = _run("extract_dashboard.py", "--run", "latest")
            if c.returncode == 0 and c.stdout.strip():
                _atomic_write(LAST_RUN, c.stdout)
            else:
                ok = False
                print(f"[worker] extract_dashboard rc={c.returncode} stdout={len(c.stdout)} stderr={c.stderr[:120]!r}", flush=True)
            c2 = _run("extract_runs.py")
            if c2.returncode == 0 and c2.stdout.strip():
                _atomic_write(RUNS_LIST, c2.stdout)
            else:
                ok = False
                print(f"[worker] extract_runs rc={c2.returncode}", flush=True)
        except Exception as exc:
            ok = False
            print(f"[worker] 异常: {exc}", flush=True)
        if ok:
            fails = 0
        else:
            fails += 1
            if fails >= 5:
                print("[worker] 连续失败 5 次，退出；请重启 dashboard.bat", flush=True)
                return 1
        time.sleep(max(1.0, INTERVAL - (time.time() - t0)))


if __name__ == "__main__":
    raise SystemExit(main())
