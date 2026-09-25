@echo off
chcp 65001 >nul
title Gravia 市場掃描／自動新增策略
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
py polymarket_autopilot.py --no-telegram %*
echo.
echo 完成。（加 --dry-run 只掃描不新增）
pause
