@echo off
REM 复跑企业：读取 config/replay_companies.txt，从历史结果复用并补跑。
cd /d "%~dp0"
.venv\Scripts\python.exe src\control\main_agent.py --replay
pause
