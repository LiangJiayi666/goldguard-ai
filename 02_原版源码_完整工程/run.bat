@echo off
REM 运行主流程：读取 config/companies.txt，执行完整排查。
cd /d "%~dp0"
.venv\Scripts\python.exe src\control\main_agent.py
pause
