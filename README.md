# Polymarket BTC/ETH Up/Down Dashboard

## 實盤報價來源

`polymarket_live_strategy.py` 以 Polymarket Market WebSocket
（`wss://ws-subscriptions-clob.polymarket.com/ws/market`）接收 Up/Down 即時完整訂單簿與增量更新。
每次簿價變動都會立即重新檢查進場、第二腿與提早退出條件；原本的 3 秒循環只保留市場換期、
Binance 模型資料更新與結算工作。WebSocket 尚未取得當前連線的完整快照、連線不健康或斷線時，
才會暫時改用 CLOB REST 訂單簿，連線恢復後自動切回 WebSocket。

啟動紀錄中的 `[QUOTE] source=websocket` 代表兩邊都來自目前連線的 WebSocket 快照；
`source=rest_fallback` 代表正在使用安全備援。這項變更不會略過
`LIVE_TRADING` 與 `POLY_STRATEGY_ARMED` 兩道真實送單開關。

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

`polymarket_live_strategy.py` 已同步紙上模擬的深度 VWAP、最差限價判斷、taker fee、滑點、公平價進場、資金預留、淨鎖利與動態退出規則。直接配對進場會先預熱新市場 token 的建單 metadata，再把 Up／Down 兩筆 FOK 放進同一次 `POST /orders` batch，減少兩個獨立 request 的到達時間差。成交後會優先從成交紀錄回填真實均價；暫時查不到時則以送出的保守限價記帳。Batch 仍不保證兩筆原子成交，因此其中一腿失敗時仍會先補該腿，最後才嘗試緊急賣回已成交腿。

安全預設：

- `LIVE_TRADING=false`：只跑 dry-run，不簽名、不送單。
- `POLY_STRATEGY_ARMED=false`：新增的第二道武裝開關。只有它與 `LIVE_TRADING` 同時為 `true` 才會送出真實策略訂單。
- `POLY_VALIDATE_ORDER_PATH=true`：安全驗證模式。每個新市場預熱並簽署兩筆 FOK，但硬性禁止 `POST /orders`；即使另外兩個開關誤設為 `true` 也不會真實執行。
- `POLY_MAX_PAIR_BUDGET_USD=25`：每組兩腿最多 25 USDC。
- `POLY_MIN_CASH_RESERVE_USD=5`：至少保留 5 USDC 現金。
- `POLY_STAKE_PCT=15`：每組兩腿預算為可用現金的 15%。
- `POLY_ACTION_COOLDOWN_SECONDS=10`：下單嘗試間隔至少 10 秒。
- 只有 FOK 訂單回覆 `matched` 才當作成交；`delayed` 長時間無法確認時會自動停止下單。
- 真實策略狀態儲存在 `polymarket_live_strategy_state.json`，重啟後不會忘記持倉與停止原因。

### 建議做法：嵌入模擬盤進程（`--with-live`），共用同一條 WS 連線

實盤跟模擬盤各自開一條獨立 WebSocket 連線的話，兩邊收到同一筆報價更新的時間點不保證相同——鎖利機會常常只存在幾百毫秒，一邊先動手鎖住深度時，另一邊可能因為晚收到同一筆更新而撲空。要讓兩邊判斷完全同步，改用 `--with-live` 旗標啟動模擬盤，讓實盤邏輯（`run_embedded()`）直接掛在模擬盤進程裡、共用同一條 WS 連線：

```powershell
py polymarket_server.py --with-live
```

啟動紀錄看到 `⚠ --with-live 已啟用` 代表成功掛載。**這個模式下不要再另外執行 `py polymarket_live_strategy.py`**——兩個進程各自對同一個帳戶下單會互相搶單、重複下單，或同時誤判「目前沒有未管理的持倉」。是否真的送出真實訂單，仍然完全由 `.env` 的 `LIVE_TRADING` 與 `POLY_STRATEGY_ARMED` 兩道開關決定，跟是否加 `--with-live` 無關。

先保持 `LIVE_TRADING=false` 用這個指令跑：

```powershell
py polymarket_server.py --with-live
```

待完整跑過多個市場窗口、確認狀態頁與成交條件後，再由你自行決定是否把 `.env` 的 `LIVE_TRADING` 與 `POLY_STRATEGY_ARMED` 改成 `true`。

### 舊做法：獨立進程（`polymarket_live_strategy.py`）

```powershell
py polymarket_live_strategy.py
```

會自己開一條獨立 WS 連線（`strategy_loop()`），邏輯完全相同，但跟模擬盤之間會有上述的時間差，不建議再使用；保留只是為了不強制中斷既有的執行方式。

## 常見問題

**Q: 左上角顯示 DISCONNECTED**
A: `polymarket_server.py` 沒有在跑。先執行 `py polymarket_server.py`。

**Q: 執行 server 報錯 ModuleNotFoundError**
A: 執行 `pip install -r requirements.txt`

**Q: 數據顯示 —（破折號）**
A: 等 10–20 秒讓 server 抓到第一筆數據。
