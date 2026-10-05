@echo off
chcp 65001 >nul
title GoldGuard AI 离线演示（可运行Web产品）
cd /d "%~dp001_可运行Web产品_离线演示"
echo ============================================================
echo   GoldGuard AI · 黄金企业风险雷达（离线演示）
echo   即将打开 http://127.0.0.1:8765
echo   使用构造/脱敏数据，无需联网、无需 API Key。
echo   关闭本窗口即停止服务。
echo ============================================================
start "" http://127.0.0.1:8765
python run.py
pause
