# Gravia · 即時數據版 Dashboard

## 資料夾結構

```
gravia_live/
├── server.py          ← Python 後端（抓真實數據）
├── requirements.txt
├── README.md
└── web/
    └── dashboard.html ← 瀏覽器前端
```

## 開啟步驟（照順序）

### 步驟 1：安裝套件
```powershell
cd C:\Users\micha\OneDrive\文件\gravia_live
pip install -r requirements.txt
```

### 步驟 2：啟動 server
```powershell
python server.py
```
看到這個就代表成功：
```
[INFO] Gravia · Real-Time Data Bridge
[INFO] WebSocket: ws://localhost:8765
[INFO] 開啟 web/dashboard.html 查看即時數據
```

### 步驟 3：開啟 Dashboard
直接雙擊 `web/dashboard.html`，
左上角顯示 **LIVE · REAL DATA** 即代表成功接收真實數據。

## 數據來源

| 數據 | 來源 | 更新頻率 |
|------|------|---------|
| BTC 價格 | Binance aggTrade stream | 即時（毫秒級） |
| ETH 價格 | Binance Futures API | 每 10 秒 |
| Binance 資金費率 | Binance premiumIndex | 每 10 秒 |
| Bybit 資金費率 | Bybit tickers API | 每 10 秒 |
| K 線 (5m) | Binance Futures klines | 每 10 秒 |

**全部免費，不需要 API Key。**

## 常見問題

**Q: 左上角顯示 DISCONNECTED**
A: server.py 沒有在跑。先執行 `python server.py`。

**Q: python server.py 報錯 ModuleNotFoundError**
A: 執行 `pip install -r requirements.txt`

**Q: 數據顯示 —（破折號）**
A: 等 10–20 秒讓 server 抓到第一筆數據。
