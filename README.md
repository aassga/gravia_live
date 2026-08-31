# Polymarket BTC/ETH Up/Down Dashboard

使用 Polymarket 真實市場資料執行 BTC/ETH 5 分鐘 Up/Down 紙上交易模擬，
並提供獨立、預設停用的真實下單工具。

## 資料夾結構

```
gravia_live/
├── polymarket_server.py    ← Polymarket 後端（Up/Down 成交與動態避險模擬）
├── polymarket_live_strategy.py
├── polymarket_live_trader.py
├── polymarket_live_status_server.py
├── requirements.txt
├── README.md
└── web/
    ├── polymarket.html      ← 紙上交易 Dashboard
    └── polymarket_live.html ← 真實帳戶唯讀狀態頁
```

## 開啟步驟（照順序）

### 步驟 1：安裝套件
```powershell
cd C:\Users\micha\OneDrive\文件\gravia_live
pip install -r requirements.txt
```

### 步驟 2：啟動紙上模擬服務
```powershell
py polymarket_server.py
```
看到這個就代表成功：
```
[INFO] Polymarket BTC Up/Down · Real-Time Data Bridge
[INFO] WebSocket: ws://localhost:8766
[INFO] 開啟 web/polymarket.html 查看即時數據
```
Windows 上如果 `python` 指令沒反應（跳出 Microsoft Store），請改用 `py`。

### 步驟 3：開啟 Dashboard
直接雙擊對應的 html 檔案，
左上角顯示 **LIVE · REAL DATA** 即代表成功接收真實數據。

## 數據來源

**Polymarket（Up/Down）**

| 數據 | 來源 | 更新頻率 |
|------|------|---------|
| Up/Down 報價、訂單簿 | Polymarket CLOB API | 每 3 秒 |
| 市場窗口、結算結果 | Polymarket Gamma API | 每 3 秒 |
| BTC/ETH 參考價、K 線 | Binance Futures API | 每 3 秒 |

**全部免費，不需要 API Key，不需要連錢包。**

## 紙上模擬如何計算

- 買進只會吃 Ask，賣出只會吃 Bid，並按訂單簿深度計算 VWAP；深度不足就不假設成交。
- 所有進場、配對與退出條件統一使用「含滑點的最差深度價格，再向不利方向對齊市場 tick」作為判斷價；VWAP 只用來記錄模擬成交與損益，避免紙上結果比實盤樂觀。
- 預設模擬 taker，扣除 Crypto taker fee，另加 3 bps 不利滑點。
- 兩腿成本不只要低於設定門檻，還必須在費用後至少鎖定 1¢/股淨利。
- 第二腿成交前清楚標為方向性曝險；策略會依模型公平價決定是否提早退出。
- 公平價使用 Binance Futures 短期波動作為 Chainlink TWAP 的代理，再與 Polymarket 市場隱含機率混合校準；它不是真實 Chainlink feed。
- 每次下注百分比是「完整兩腿配對」的資金上限，會預留第二腿與費用。
- 紙上模擬與實盤預設每組都使用資產組合的 15%；實盤仍會套用單組金額上限與現金保留額。
- 持倉、已結算交易、費用與報價會寫入 `polymarket_sim.sqlite3`，服務重啟後可續跑。

Dashboard 會分開顯示鎖利交易、方向性交易、提早退出、累計費用與最大回撤。紙上結果仍不是實盤收益保證。

## 真實自動下單

`polymarket_live_strategy.py` 已同步紙上模擬的深度 VWAP、最差限價判斷、taker fee、滑點、公平價進場、資金預留、淨鎖利與動態退出規則。成交後會優先從成交紀錄回填真實均價；暫時查不到時則以送出的保守限價記帳。真實執行仍有一個無法消除的差異：兩腿無法保證原子成交，因此第二腿失敗時會嘗試緊急賣回第一腿。

安全預設：

- `LIVE_TRADING=false`：只跑 dry-run，不簽名、不送單。
- `POLY_STRATEGY_ARMED=false`：新增的第二道武裝開關。只有它與 `LIVE_TRADING` 同時為 `true` 才會送出真實策略訂單。
- `POLY_MAX_PAIR_BUDGET_USD=25`：每組兩腿最多 25 USDC。
- `POLY_MIN_CASH_RESERVE_USD=5`：至少保留 5 USDC 現金。
- `POLY_STAKE_PCT=15`：每組兩腿預算為可用現金的 15%。
- `POLY_ACTION_COOLDOWN_SECONDS=10`：下單嘗試間隔至少 10 秒。
- 只有 FOK 訂單回覆 `matched` 才當作成交；`delayed` 長時間無法確認時會自動停止下單。
- 真實策略狀態儲存在 `polymarket_live_strategy_state.json`，重啟後不會忘記持倉與停止原因。

先保持 `LIVE_TRADING=false` 執行：

```powershell
py polymarket_live_strategy.py
```

待完整跑過多個市場窗口、確認狀態頁與成交條件後，再由你自行決定是否開啟真實下單。

## 常見問題

**Q: 左上角顯示 DISCONNECTED**
A: `polymarket_server.py` 沒有在跑。先執行 `py polymarket_server.py`。

**Q: 執行 server 報錯 ModuleNotFoundError**
A: 執行 `pip install -r requirements.txt`

**Q: 數據顯示 —（破折號）**
A: 等 10–20 秒讓 server 抓到第一筆數據。
