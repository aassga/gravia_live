@echo off
chcp 65001 >nul
title Gravia sim + live1 (DRY-RUN)
cd /d "%~dp0"

rem ---------------------------------------------------------------------------
rem 2026-09-30: sim + embedded live #1 (--with-live).
rem Real orders still require BOTH LIVE_TRADING=true and POLY_STRATEGY_ARMED=true
rem in .env; both are false, so this runs DRY-RUN only.
rem Body kept ASCII on purpose: Chinese text inside a .bat gets mangled by cp950
rem and breaks command parsing (hit this before). Chinese stays in the filename.
rem ---------------------------------------------------------------------------

set POLY_SIM_SELF_RESTART=true
set PYTHONUNBUFFERED=1
set PYTHONIOENCODING=utf-8

echo.
echo  === Gravia sim + live1 (local) ===
echo  Window open = running; close the window = stop.
echo  Sim dashboard  : web\polymarket.html        (port 8766)
echo  Live1 dashboard: web\polymarket_live.html   (port 8767)
echo  Config changes (variants / disabled list) trigger an auto-restart.
echo.

rem Read-only live #1 status server in its own window (never places orders)
start "Gravia live1 status 8767" cmd /c "cd /d "%~dp0" && py polymarket_live_status_server.py"

:loop
echo [%date% %time%] starting sim + live1 ...
py polymarket_server.py --with-live
echo [%date% %time%] process exited, restarting in 5s ...
timeout /t 5 /nobreak >nul
goto loop
