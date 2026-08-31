# Gravia · 即時數據版 Dashboard

用真實市場數據跑「紙上交易」模擬（不動用真實資金、不接錢包、不需要 API Key），
目前有兩套獨立系統：

| | Gravia 資金費率套利 | Polymarket BTC Up/Down |
|---|---|---|
| 後端 | `server.py` | `polymarket_server.py` |
| 前端 | `web/dashboard.html` | `web/polymarket.html` |
| WebSocket | `ws://localhost:8765` | `ws://localhost:8766` |
| 數據來源 | Binance + Bybit 公開 API | Polymarket Gamma + CLOB API |
| 策略 | 兩交易所資金費率價差，一多一空對沖 | 5 分鐘 Up/Down 市場，分批建倉、配對鎖利 |

兩套可以同時啟動，互不干擾。

## 資料夾結構

```
gravia_live/
├── server.py               ← Gravia 後端（資金費率套利）
├── polymarket_server.py    ← Polymarket 後端（Up/Down 動態避險）
├── requirements.txt
├── README.md
└── web/
    ├── dashboard.html      ← Gravia 前端
    └── polymarket.html     ← Polymarket 前端
```

## 開啟步驟（照順序）

### 步驟 1：安裝套件
```powershell
cd C:\Users\micha\OneDrive\文件\gravia_live
pip install -r requirements.txt
```

### 步驟 2：啟動 server（依需要擇一或都跑）
```powershell
py server.py               # Gravia 資金費率套利
py polymarket_server.py    # Polymarket Up/Down
```
看到這個就代表成功：
```
[INFO] Gravia · Real-Time Data Bridge
[INFO] WebSocket: ws://localhost:8765
[INFO] 開啟 web/dashboard.html 查看即時數據
```
Windows 上如果 `python` 指令沒反應（跳出 Microsoft Store），請改用 `py`。

### 步驟 3：開啟 Dashboard
直接雙擊對應的 html 檔案，
左上角顯示 **LIVE · REAL DATA** 即代表成功接收真實數據。

## 數據來源

**Gravia（資金費率套利）**

| 數據 | 來源 | 更新頻率 |
|------|------|---------|
| BTC 價格 | Binance aggTrade stream | 即時（毫秒級） |
| ETH 價格 | Binance Futures API | 每 10 秒 |
| 資金費率（全市場） | Binance premiumIndex + Bybit tickers | 每 10 秒 |
| K 線 (5m) | Binance Futures klines | 每 10 秒 |

**Polymarket（Up/Down）**

| 數據 | 來源 | 更新頻率 |
|------|------|---------|
| Up/Down 報價、訂單簿 | Polymarket CLOB API | 每 3 秒 |
| 市場窗口、結算結果 | Polymarket Gamma API | 每 3 秒 |
| BTC 現貨價格、K 線 | Binance Futures API | 每 3 秒 |

**全部免費，不需要 API Key，不需要連錢包。**

## 常見問題

**Q: 左上角顯示 DISCONNECTED**
A: 對應的 server 沒有在跑。先執行 `py server.py` 或 `py polymarket_server.py`。

**Q: 執行 server 報錯 ModuleNotFoundError**
A: 執行 `pip install -r requirements.txt`

**Q: 數據顯示 —（破折號）**
A: 等 10–20 秒讓 server 抓到第一筆數據。
