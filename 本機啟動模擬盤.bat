@echo off
chcp 65001 >nul
title Gravia 模擬盤（本機）
cd /d "%~dp0"

rem ── 只跑模擬盤，不載入任何實盤下單邏輯（沒有 --with-live）────────────────

rem ── 2026-10-01：防止兩個啟動器互相覆蓋 ─────────────────────────────────────
rem 9/30 這支的 :loop 一直重啟「純模擬盤」版本，把「含實盤」啟動器開的那個蓋掉，
rem 實盤①停了 13.5 小時都沒人發現（模擬盤畫面完全正常，看不出差別）。
netstat -ano | findstr ":8766" | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
  echo.
  echo  [!] 8766 已經有人在聽，表示已經有一個模擬盤在跑了。
  echo      如果那個是「本機啟動模擬盤含實盤.bat」，這支會把實盤①蓋掉，所以不啟動。
  echo      要改跑純模擬盤，請先關掉另一個視窗再執行這支。
  echo.
  pause
  exit /b 1
)

set POLY_SIM_SELF_RESTART=true
set PYTHONUNBUFFERED=1
set PYTHONIOENCODING=utf-8

echo.
echo  === Gravia 模擬盤（本機）===
echo  視窗開著 = 運行中；關掉視窗 = 停止。
echo  儀表板：用瀏覽器開 web\polymarket.html
echo  設定檔（變體／停用清單）變更時會自動重啟套用。
echo.

:loop
echo [%date% %time%] 啟動模擬盤...
py polymarket_server.py
echo [%date% %time%] 進程結束（可能是套用新設定），5 秒後重啟；要停止請關掉這個視窗。
timeout /t 5 /nobreak >nul
goto loop
