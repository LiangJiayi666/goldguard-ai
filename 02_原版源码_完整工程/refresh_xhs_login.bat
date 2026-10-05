@echo off
chcp 65001 >nul
REM 小红书登录刷新：弹出浏览器扫码
cd /d "%~dp0"
.venv\Scripts\python.exe "src\social\xhs_login_refresh.py"
if errorlevel 1 (
  echo.
  echo 小红书登录刷新失败，请检查网络后重试。
) else (
  echo.
  echo 小红书登录态已刷新。
)
echo.
pause
