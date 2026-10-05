@echo off
chcp 65001 >nul
REM 抖音登录刷新：弹出浏览器扫码
cd /d "%~dp0"
.venv\Scripts\python.exe "src\social\douyin_login_refresh.py"
if errorlevel 1 (
  echo.
  echo 抖音登录刷新失败，请检查网络后重试。
) else (
  echo.
  echo 抖音登录态已刷新。
)
echo.
pause
