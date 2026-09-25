@echo off
chcp 65001 >nul
title Gravia 後台連線（模擬盤 8766 / 實盤 8767 8768 8770）
cd /d "%~dp0"

set KEY=C:\Users\micha\Downloads\michaelKey.pem
set HOST=ubuntu@34.242.205.251

echo.
echo  === Gravia 後台連線 ===
echo  這個視窗開著 = 連線中；關掉視窗 = 斷線。
echo  連上後用瀏覽器開：
echo    模擬盤  web\polymarket.html
echo    實盤    web\polymarket_live.html?port=8767 （8768 / 8770 是第 2、3 盤）
echo.

:loop
echo [%date% %time%] 連線中...
ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=accept-new ^
 -i "%KEY%" ^
 -L 8766:127.0.0.1:8766 ^
 -L 8767:127.0.0.1:8767 ^
 -L 8768:127.0.0.1:8768 ^
 -L 8769:127.0.0.1:8769 ^
 -L 8770:127.0.0.1:8770 ^
 -L 8771:127.0.0.1:8771 ^
 %HOST%

echo [%date% %time%] 連線中斷，10 秒後自動重連（要結束請直接關掉這個視窗）
timeout /t 10 /nobreak >nul
goto loop
