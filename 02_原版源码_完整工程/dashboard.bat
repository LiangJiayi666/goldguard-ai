@echo off
REM Dashboard launcher for icbc_gold_final
REM NOTE: keep this file pure ASCII (no Chinese comments)!
REM cmd.exe parses .bat files with the ANSI/GBK codepage; UTF-8 Chinese
REM comments get garbled and break the script (commands after them fail).

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] .venv\Scripts\python.exe not found. Please run install.bat first.
    pause
    exit /b 1
)

if not exist "logs" mkdir logs

set DASH_PORT=18080
echo Starting dashboard service on port %DASH_PORT%...

REM Kill stale instance listening on the port (filter by LISTENING only)
netstat -ano | findstr ":%DASH_PORT%" >nul 2>&1
if %errorlevel% equ 0 (
    echo Port %DASH_PORT% already in use. Attempting to free it...
    for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":%DASH_PORT%" ^| findstr "LISTENING"') do (
        taskkill /F /PID %%p >nul 2>&1
    )
    ping -n 2 127.0.0.1 >nul
)

REM MUST start with python.exe via start /b (inherits this cmd console).
REM Do NOT use pythonw.exe or start /min: on this Windows setup, the aiohttp
REM process then fails to spawn report subprocesses (silent rc=1, empty output),
REM and /api/runs/latest returns 500 (dashboard shows no data).
REM The data worker (plain python) generates dashboard JSON files; server.py
REM only reads them, so it never spawns subprocesses itself.
start /b "" .venv\Scripts\python.exe scripts\report\dashboard_worker.py > logs\dashboard_worker.log 2>&1
start /b "" .venv\Scripts\python.exe dashboard\server.py --host 127.0.0.1 --port %DASH_PORT% > logs\dashboard.log 2>&1

echo Waiting for service to be ready...
set /a tries=0
:wait_loop
ping -n 2 127.0.0.1 >nul
.venv\Scripts\python.exe -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:%DASH_PORT%/api/agent/status', timeout=2)" >nul 2>&1
if %errorlevel% equ 0 goto service_ready
set /a tries+=1
if %tries% lss 15 goto wait_loop

echo [ERROR] Dashboard service failed to start or timed out.
echo Please check logs\dashboard.log for details.
echo You can also run this command manually:
echo   .venv\Scripts\python.exe dashboard\server.py --host 127.0.0.1 --port %DASH_PORT%
pause
exit /b 1

:service_ready
echo Service ready, opening browser...
start "" "http://127.0.0.1:%DASH_PORT%"
echo Browser opened. If the page does not appear, please visit http://127.0.0.1:%DASH_PORT% manually.
pause
