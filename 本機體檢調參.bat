@echo off
chcp 65001 >nul
title Gravia 模擬盤體檢／調參
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
py polymarket_sim_doctor.py %*
echo.
echo 完成。（加 --dry-run 只分析不修改）
pause
