@echo off
chcp 65001 >nul
title Gravia tunnel - sim 8766 / live1 8767
cd /d "%~dp0"

rem ---------------------------------------------------------------------------
rem 2026-10-13: IP is now 34.255.2.122 (was 54.216.142.36, before that
rem 34.245.86.121). The Elastic IP was released, so the instance gets a NEW
rem auto-assigned public IP every stop/start - update HOST below after each
rem restart, or attach an Elastic IP again to keep it fixed.
rem Key filename: michaelky.pem
rem
rem Body is ASCII on purpose. This file is read by cmd using the system code
rem page (cp950 here), so UTF-8 Chinese in the body turns into garbage and every
rem line becomes an invalid command -> endless error loop. Hit this twice now.
rem Chinese stays in the filename only.
rem ---------------------------------------------------------------------------

set KEY=C:\Users\micha\Downloads\michaelky.pem
set HOST=ubuntu@34.255.2.122

echo.
echo  === Gravia tunnel ===
echo  Window open = tunnel up; close the window = disconnect.
echo  Then open in a browser:
echo    sim    web\polymarket.html         (port 8766)
echo    live1  web\polymarket_live.html    (port 8767)
echo.
echo  If it cannot connect, check the VPS security group allows your current
echo  public IP on port 22.
echo.

:loop
echo [%date% %time%] connecting ...
ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=accept-new ^
 -i "%KEY%" ^
 -L 8766:127.0.0.1:8766 ^
 -L 8767:127.0.0.1:8767 ^
 %HOST%

echo [%date% %time%] disconnected, retrying in 10s; close this window to stop.
timeout /t 10 /nobreak >nul
goto loop
