@echo off
chcp 65001 >nul
REM 快手登录刷新：弹出浏览器扫码
cd /d "%~dp0"
.venv\Scripts\python.exe "src\social\kuaishou_login_refresh.py"
if errorlevel 1 (
  echo.
  echo 快手登录刷新失败，请检查网络后重试。
) else (
  echo.
  echo 快手登录态已刷新。
)
echo.
pause
