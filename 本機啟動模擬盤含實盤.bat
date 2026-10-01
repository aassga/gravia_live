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

rem 2026-10-01: refuse to start if another sim already owns port 8766, so the two
rem launchers can never silently override each other (on 09-30 the sim-only
rem launcher's loop kept replacing this one and live #1 was down for 13.5h
rem without any visible sign on the sim dashboard).
netstat -ano | findstr ":8766" | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
  echo.
  echo  [!] Port 8766 is already in use - a sim is already running.
  echo      Close that window first if you want to run this launcher instead.
  echo.
  pause
  exit /b 1
)

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

rem Read-only live #1 status server in its own window (never places orders).
rem Two start-related traps already hit here, hence the plain form below:
rem   1) nesting quotes in the command ("cd /d "%~dp0" && ...") breaks cmd's
rem      parsing and the child gets only a fragment (2026-09-30).
rem   2) /d "%~dp0" fails because %~dp0 ends with a backslash, so \" swallows
rem      the closing quote and the window never opens (2026-10-01).
rem The script already did cd /d "%~dp0" above, so start inherits the right cwd.
start "Gravia live1 status 8767" py polymarket_live_status_server.py

:loop
echo [%date% %time%] starting sim + live1 ...
py polymarket_server.py --with-live
echo [%date% %time%] process exited, restarting in 5s ...
timeout /t 5 /nobreak >nul
goto loop
