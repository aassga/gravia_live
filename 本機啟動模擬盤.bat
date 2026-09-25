@echo off
chcp 65001 >nul
title Gravia 模擬盤（本機）
cd /d "%~dp0"

rem ── 只跑模擬盤，不載入任何實盤下單邏輯（沒有 --with-live）────────────────
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
