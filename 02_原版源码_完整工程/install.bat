@echo off
REM Double-click installer entry. Delegates to scripts\install.ps1.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\install.ps1" %*
echo.
echo (Press any key to close this window)
pause >nul
