"""
Polymarket BTC Up/Down · Real-Time Data Bridge
────────────────────────────────────────────────
從 Polymarket 公開 API（Gamma + CLOB）拉取真實的 BTC 5 分鐘 Up/Down 市場報價，
模擬「先進場價格便宜的一邊、等另一邊也夠便宜時配對鎖利，鎖不到就抱到期結算」
的動態避險策略（紙上交易，不動用真實資金、不接錢包）。

全部使用公開唯讀端點，不需要 API key，不需要連錢包。

啟動方式：
    py polymarket_server.py

然後用瀏覽器開啟 web/polymarket.html
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from email.utils import parsedate_to_datetime
from statistics import NormalDist, pstdev

import aiohttp
import polymarket_diagnostics as decision_diag
import websockets
from websockets.server import serve

# 這支檔案本身以前沒有呼叫 load_dotenv()——只有 --with-live 模式下，main() 稍後才會
# import polymarket_live_trader，那支檔案內部才會載入 .env。純模擬模式（不加
# --with-live）完全不需要 .env，一直以來沒事；但像 SERVER_REGION 這種「模擬盤自己
# 也想讀的設定」在檔案最上面就讀了，那時候 .env 根本還沒載入，永遠只會拿到預設值。
# 這裡直接載入一次，兩種模式下都能正確讀到 .env——重複呼叫 load_dotenv() 是安全的。
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

if sys.platform == "win32":
    # Windows 終端機預設編碼常是 cp950/cp936，中文 log 會變亂碼，強制改 UTF-8
    # （polymarket_live_trader.py / polymarket_live_strategy.py 已經有這段，這支主程式
    # 之前漏掉了——單獨執行看起來還好是因為 Windows Terminal 常常自己就是 UTF-8，
    # 但 PowerShell 管線/重新導向會用系統預設編碼，這時候就會亂碼）。
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

if __name__ == "__main__":
    # `py polymarket_server.py` 直接執行時，這支檔案是以 __main__ 身份載入的；但
    # --with-live 模式下 main() 稍後會 `import polymarket_live_strategy`，那支檔案
    # 內部又是用 `import polymarket_server as sim` 取得這份模組——Python 不會把
    # 「正在跑的 __main__」跟「用檔名匯入的同一支檔案」視為同一個模組物件，沒有這行的話
    # 會重新執行一份全新、獨立的 polymarket_server.py，導致實盤那邊的 sim.state／
    # sim.markets_state 永遠是空的初始狀態，市場資料永遠抓不到、evaluate_and_act
    # 每次都在最前面就跳過，看起來像「沒出錯但也沒有在跑」。這裡先把自己註冊進
    # sys.modules，讓稍後的 import 直接拿到這個正在跑的 __main__ 物件。
    sys.modules.setdefault("polymarket_server", sys.modules[__name__])

# ── 設定 ──────────────────────────────────────────────────────────────────
HOST = "localhost"
PORT = int(os.environ.get("POLY_SIM_PORT", "8766"))  # 可讓隔離的 ETH MM 服務使用 8768
POLL_INTERVAL = 3       # 報價輪詢間隔（秒）－ Polymarket 沒有強制要求 WebSocket，輪詢就綽綽有餘

# 這個進程實際運行的地區標籤，純粹顯示用（例如 "TW-Home" / "AWS eu-west-1 Dublin"）。
# 每台機器的 .env 各自設定自己的值，同一份程式碼不用改就能在前端分辨現在是本機還是
# VPS 在跑——這是延遲比較（見 SERVER_PING_MS）的重要對照組。
SERVER_REGION = os.environ.get("SERVER_REGION", "未設定地區")

# 明確的啟動旗標，預設 False：不加這個參數，這支程式永遠只是純模擬、不可能送出任何
# 真實訂單，不管 .env 的 LIVE_TRADING/POLY_STRATEGY_ARMED 是什麼狀態。只有同時「執行
# 時明確加這個參數」+「.env 武裝」兩個條件都成立，才會真的去下真實單——這是刻意的
# 兩層防呆，不想讓「跑模擬盤」這個平常無害的動作，光靠一個設定檔就能變成真實下單。
WITH_LIVE = "--with-live" in sys.argv

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket")

# ── 模擬策略設定（紙上交易）────────────────────────────────────────────────
SIM_ENTRY_MAX_PRICE   = 0.40   # 主要策略：只有價格 <= 這個門檻才考慮先進場一邊
# 2026-09-12 依使用者要求關回純兩腿直接鎖利。單邊進場 2026-09-11 重開 8.5 小時的結果：
# btc-main 98 筆 -$4（74 筆補到腿 +$190、24 筆補不到 0/24 -$195），btc-loose 100 筆 -$47；
# 而重開前 195 小時的 +$322／+$331 全部來自兩腿直接鎖利，跟 entryMaxPrice 無關。
# 想再驗證單邊進場時把這個開關打開即可（變體的 entryMaxPrice 仍保留給 Dashboard 顯示）。
SIM_SINGLE_LEG_ENTRY_ENABLED = False
SIM_LOCK_MAX_SUM      = 0.95   # 主要策略：兩邊最差可成交限價 <= 門檻，且扣費用後達最低淨利才配對
                                # （2026-09 從 0.90 放寬到 0.95，增加鎖利機會頻率——真正擋住虧損單的
                                # 是 SIM_MIN_NET_LOCK_PER_SHARE 這個獨立的淨利門檻，不是這裡，所以
                                # 放寬這個只會多考慮更多候選機會，不會放行扣完費用還虧錢的單。
                                # 這個常數也是 polymarket_live_strategy.py 實盤 LOCK_MAX_SUM 的來源，
                                # 放寬會同時影響實盤的鎖利門檻。）
SIM_DEFAULT_BALANCE   = 100.0  # 起始虛擬總資產預設值（美元），可從前端輸入自訂（會重置模擬）
SIM_MIN_BALANCE       = 1.0
SIM_DEFAULT_STAKE_PCT = 15.0   # 每組完整兩腿預設佔目前資產組合 15%（僅紙上模擬）
SIM_MIN_STAKE_PCT     = 0.5
SIM_MAX_STAKE_PCT     = 25.0

# 成交與風控模型。紙上交易一律假設為 taker：用賣盤/買盤深度模擬成交，
# 並扣除 Crypto 市場費率。若日後要測 maker，必須另外建立排隊順位模型，不能假設掛單必成交。
SIM_TAKER_FEE_RATE          = 0.07
SIM_SLIPPAGE_BPS            = 3.0   # 模擬收到報價到成交之間的額外價格惡化
SIM_MIN_ENTRY_EDGE          = 0.025 # 模型公平機率扣除成本後，至少保留 2.5¢/股
SIM_MIN_NET_LOCK_PER_SHARE  = 0.01  # 完成配對後至少淨賺 1¢/股
SIM_MIN_ORDER_NOTIONAL_USD  = 1.0   # 跟實盤一致：單腿成交金額低於這個門檻就不下單（Polymarket 最小下注是 $1，不是 $5）
SIM_EXIT_EDGE               = 0.02  # 市場可賣價高於模型持有價值 2¢/股時提早退出
SIM_FAIR_MODEL_WEIGHT       = 0.65  # Binance 波動模型權重；其餘使用市場隱含機率校準
SIM_MIN_SIGMA_PER_SECOND    = {"btc": 0.000025, "btc-15m": 0.000025, "btc-4h": 0.000025, "eth": 0.000035}

# BTC inventory-rotation experiment. This is deliberately SIM-only: it buys
# one small slice at a time, accepts temporary directional inventory, and buys
# the opposite outcome later only when the newly paired shares lock a net edge.
ROTATION_SLICE_SHARES          = 5.0
ROTATION_MIN_EDGE              = 0.04
ROTATION_PAIR_MAX_SUM          = 0.98
ROTATION_MAX_GROSS_USD         = 25.0
ROTATION_MAX_RESIDUAL_SHARES   = 5.0
ROTATION_HEDGE_ONLY_SECONDS    = 30.0
ROTATION_ACTION_COOLDOWN       = 1.0
ROTATION_RESIDUAL_RISK_PREMIUM = 0.01
# A future hedge is also a taker fill. Reserve the maximum crypto taker fee per
# share up front instead of treating an apparently cheap first leg as free to
# hedge later. At p=0.5 the fee curve reaches rate * p * (1-p) = rate * 0.25.
ROTATION_FUTURE_HEDGE_FEE_RESERVE = SIM_TAKER_FEE_RATE * 0.25

# 實盤鏡像模擬組：直接讀取與 polymarket_live_strategy.py 相同的環境變數，讓模擬盤
# 有一張獨立卡片只累積「目前實盤有效策略」的結果，不和 main／晚進場方向性混在一起。
LIVE_MIRROR_ASSET_ID            = os.environ.get("POLY_LIVE_ASSET_ID", "btc")
LIVE_MIRROR_STAKE_PCT           = max(0.5, min(30.0, float(os.environ.get("POLY_STAKE_PCT", "15.0"))))
LIVE_MIRROR_MAX_PAIR_BUDGET_USD = max(1.0, float(os.environ.get("POLY_MAX_PAIR_BUDGET_USD", "25.0")))
LIVE_MIRROR_MIN_CASH_RESERVE_USD = max(0.0, float(os.environ.get("POLY_MIN_CASH_RESERVE_USD", "5.0")))
LIVE_MIRROR_LOCK_MAX_SUM        = max(0.01, min(0.99, float(os.environ.get("POLY_LIVE_LOCK_MAX_SUM", "0.95"))))
LIVE_MIRROR_DEPTH_MULTIPLIER    = max(1.0, float(os.environ.get("POLY_PAIR_MIN_DEPTH_MULTIPLIER", "1.0")))
LIVE_MIRROR_STABILITY_SECONDS   = max(0.0, float(os.environ.get("POLY_PAIR_STABILITY_SECONDS", "0.15")))
# 股數封頂在「當下看得到的深度」的這個比例。2026-09：實盤好幾次撞到「模擬盤跟實盤在
# 同一秒看到同一個機會，模擬盤保證吃得到、實盤卻因為深度不夠被拒」——這不是 bug，是
# 紙上模擬（吃剛看到的快照，保證成交）跟真實下單（要跟其他真人搶同一份流動性，中間
# 還有網路延遲）本質上的差異，沒辦法完全消除。曾經從 0.5 調低到 0.3 想降低這個風險，
# 但代價是每次能買的股數變少、賺得也變少，使用者要求先調回 0.5 試試看效果如何——
# 這個常數也是 polymarket_live_strategy.py 實盤股數封頂的來源，調整會同時影響兩邊。
SIM_DEPTH_CAP_FRACTION      = 0.5
# 送單價格在「已經對齊到最差 tick」之上，再多讓一格 tick（買方加價/賣方降價），
# 換取更高的一次成交機率。2026-09：實盤好幾次因為只差一個 tick 就搶輸真人、變成
# 單邊曝險再緊急平倉倒賠——多讓一格 tick 的代價是鎖利空間變小，但換來的是更少
# 需要緊急平倉的情況。這個常數也是模擬盤 decision_fill 的判斷價來源，調整會同時
# 影響模擬跟實盤，讓兩邊的「進場門檻」保持一致。
SIM_PRICE_BUFFER_TICKS     = 1

# 紙上成交只能使用同一輪、時間接近的兩腿 WebSocket 訂單簿。REST fallback 仍可拿來
# 顯示行情，但不能建立模擬交易；否則 WS 重連時可能把一腿的新 snapshot 跟另一腿的
# 舊 snapshot 拼在一起，製造實際上不存在的低價配對。
SIM_BOOK_MAX_AGE_SECONDS   = 2.0
SIM_BOOK_MAX_SKEW_SECONDS  = 0.5
SIM_DATA_GUARD_LOG_SECONDS = 30.0

# ── ETH maker 紙上策略 ─────────────────────────────────────────────────────
# 只模擬在 best bid（或價差夠寬時改善一格）掛被動 BUY，不會呼叫任何下單 API。
# 成交採保守 queue-ahead 模型：同價位的真實成交量必須先吃完掛單時看到的前方深度，
# 再累積到我們的完整股數才算成交。未完整成交的零碎量不列入資產，避免把 maker 回測
# 做得過度樂觀。兩腿成本上限同時保留至少 2 cents/share 的結算空間。
MM_MAX_PAIR_COST          = 0.98
MM_MIN_NET_PAIR_EDGE      = 0.02
MM_FIRST_LEG_MAX_PRICE    = 0.60
MM_INVENTORY_RESCUE_SECONDS = 15.0
MM_REQUOTE_SECONDS        = 2.0
MM_STOP_QUOTING_SECONDS   = 20.0

# ── 晚進場方向性策略（"late-direction" 變體專用）──────────────────────────
# BTC 5m 暫時在窗口最後 10 秒使用 Binance window delta 做隔離測試：不是在窗口一開始就靠模型優勢
# 賭單邊（那條退路驗證下來是 0% 勝率，2026-09 起已對其他變體關閉），而是等到窗口
# 快結束、現價已經明顯偏離「這個窗口開盤時的價格」——這時已經沒什麼時間反轉，
# 訊號的確定性遠比窗口剛開盤時高很多。
LATE_DIRECTION_WINDOW_SECONDS      = 10.0  # 還原 12:24:56 那筆所用的最後 10 秒窗口
LATE_DIRECTION_MIN_ENTRY_REMAINING = 3.0   # 還原原策略；只替換方向價格來源為 Chainlink
LATE_DIRECTION_MIN_DELTA_PCT       = 0.02  # Chainlink 60 秒 TWAP 相對窗口開盤 TWAP 的最低偏移

# BTC 15 分鐘專用方向策略。15 分鐘市場雖然同樣由 Chainlink BTC/USD 決勝，價格在窗口
# 內有更長時間累積偏移，不能沿用 5 分鐘的固定 0.02%／最後 3–10 秒門檻。這裡用最近
# 5 分鐘的 60 秒 TWAP 實現波動估計，再按剩餘秒數換算反轉風險；所有門檻先只跑紙上盤。
BTC_15M_DIRECTION_MAX_REMAINING       = 60.0
BTC_15M_DIRECTION_MIN_REMAINING       = 20.0
BTC_15M_DIRECTION_MIN_DELTA_PCT       = 0.04
BTC_15M_DIRECTION_VOL_LOOKBACK_SECONDS = 300.0
BTC_15M_DIRECTION_VOL_BUCKET_SECONDS  = 10.0
BTC_15M_DIRECTION_MIN_SIGMA_PCT       = 0.02
BTC_15M_DIRECTION_VOL_SAFETY_MULTIPLIER = 1.35
BTC_15M_DIRECTION_MIN_PROBABILITY     = 0.75
BTC_15M_DIRECTION_MIN_EDGE_PER_SHARE  = 0.03
BTC_15M_DIRECTION_MIN_MARKET_PROBABILITY = 0.55
BTC_15M_DIRECTION_MAX_SPREAD          = 0.05
BTC_15M_DIRECTION_MAX_PRICE           = 0.88
BTC_15M_DIRECTION_DEPTH_MULTIPLIER     = 1.25
BTC_15M_DIRECTION_STABILITY_SECONDS    = 0.75
BTC_15M_DIRECTION_STAKE_PCT            = 7.0
BTC_15M_DIRECTION_MAX_BUDGET_USD       = 10.0
BTC_15M_DIRECTION_MIN_CASH_RESERVE_USD = 10.0

# BTC 15 分鐘「暴跌後補腿」紙上策略。這組刻意使用新的 variant id，避免把舊的
# 15m 直接鎖利／Chainlink 尾盤方向性績效混進來。第一腿只在窗口開始後 120 秒內，
# 以 WebSocket best ask 與約 3 秒前的同腿 ask 比較；第二腿則不受進場窗口限制，
# 但必須用相同股數通過完整深度、滑價、動態費用與最低淨利檢查。
BTC_15M_DUMP_LOOKBACK_SECONDS       = 3.0
BTC_15M_DUMP_MIN_MOVE_PCT           = 15.0
BTC_15M_DUMP_ENTRY_WINDOW_SECONDS   = 120.0
BTC_15M_DUMP_TARGET_SHARES          = 5.0
BTC_15M_DUMP_LOCK_MAX_SUM           = 0.95
BTC_15M_DUMP_MIN_NET_PER_SHARE      = 0.01
BTC_15M_DUMP_MIN_CASH_RESERVE_USD   = 5.0
# Chainlink RTDS 仍供 15m／4h 方向性策略與結算觀察使用。BTC 5m 這次刻意改回 Binance
# 只是比較訊號來源是否影響下單率；Binance 並非結算來源，結果可能與市場最終判定不同。
CHAINLINK_RTDS_URL = "wss://ws-live-data.polymarket.com"
CHAINLINK_TWAP_WINDOW_SECONDS = 60
CHAINLINK_TWAP_MAX_AGE_SECONDS = 2.5
CHAINLINK_BOUNDARY_TOLERANCE_MS = 250
_chainlink_twap_history: deque[tuple[int, float]] = deque(maxlen=1200)
_chainlink_twap_latest: dict = {}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIM_DB_PATH = os.environ.get("POLY_SIM_DB_PATH", os.path.join(BASE_DIR, "polymarket_sim.sqlite3"))

# ── 追蹤的資產：Polymarket 目前總共有 7 個「5 分鐘漲跌」市場（直接對 Gamma API
#    逐一探測 slug 驗證過，其餘主流幣如 ADA/AVAX/LINK/DOT 都沒有對應市場，不是列表不全），
#    但 2026-09 起模擬盤只保留 BTC——BTC 是唯一累積夠樣本數的（37 筆、97.3% 勝率），
#    其餘幾個樣本太少（1~6 筆）沒有參考價值，先專注在 BTC，其餘之後有需要再加回來。
#    同一個資產也可以同時追蹤不同窗口長度——探測過 BTC 除了 5m 之外還有 15m／4h
#    的漲跌市場（1m/1h/1d 沒有），且訂單簿深度明顯比 5m 深很多（15m ~55k 股、
#    4h ~23k 股，5m 通常只有幾十到幾百股），值得先加進模擬盤觀察是否值得接進實盤。
#    id 要唯一（拿來當 markets_state／AB_VARIANTS 的 key），windowSeconds 沒填的話
#    預設是 WINDOW_SECONDS（5 分鐘）。
ASSET_CATALOG = [
    {"id": "btc",     "label": "BTC",      "slugPrefix": "btc-updown-5m-",  "binanceSymbol": "BTCUSDT", "windowSeconds": 300},
    {"id": "btc-15m", "label": "BTC 15m",  "slugPrefix": "btc-updown-15m-", "binanceSymbol": "BTCUSDT", "windowSeconds": 900},
    {"id": "btc-4h",  "label": "BTC 4h",   "slugPrefix": "btc-updown-4h-",  "binanceSymbol": "BTCUSDT", "windowSeconds": 14400},
    {"id": "eth",     "label": "ETH MM",   "slugPrefix": "eth-updown-5m-",  "binanceSymbol": "ETHUSDT", "windowSeconds": 300, "marketMakerOnly": True},
    {"id": "eth-alt", "label": "ETH",      "slugPrefix": "eth-updown-5m-",  "binanceSymbol": "ETHUSDT", "windowSeconds": 300},
    {"id": "sol",     "label": "SOL",      "slugPrefix": "sol-updown-5m-",  "binanceSymbol": "SOLUSDT", "windowSeconds": 300},
    {"id": "xrp",     "label": "XRP",      "slugPrefix": "xrp-updown-5m-",  "binanceSymbol": "XRPUSDT", "windowSeconds": 300},
    {"id": "bnb",     "label": "BNB",      "slugPrefix": "bnb-updown-5m-",  "binanceSymbol": "BNBUSDT", "windowSeconds": 300},
    {"id": "doge",    "label": "DOGE",     "slugPrefix": "doge-updown-5m-", "binanceSymbol": "DOGEUSDT", "windowSeconds": 300},
    {"id": "hype",    "label": "HYPE",     "slugPrefix": "hype-updown-5m-", "binanceSymbol": "HYPEUSDT", "windowSeconds": 300},
    {"id": "zec",     "label": "ZEC",      "slugPrefix": "zec-updown-5m-",  "binanceSymbol": "ZECUSDT", "windowSeconds": 300},
]
_default_asset_ids = "btc,btc-15m" if WITH_LIVE else "btc,btc-15m,eth"
_enabled_asset_ids = {
    value.strip() for value in os.environ.get("POLY_SIM_ASSETS", _default_asset_ids).split(",") if value.strip()
}
ASSETS = [asset for asset in ASSET_CATALOG if asset["id"] in _enabled_asset_ids]
if not ASSETS:
    raise RuntimeError("POLY_SIM_ASSETS 沒有選到任何已知資產")

# ── A/B 門檻測試：每個資產各自跑同一套四組門檻設定，彼此獨立記帳，方便直接比較
#    「同一套策略邏輯放到不同資產上，表現差多少」。variant id 格式是
#    "<資產id>-<組別>"（例如 "btc-main"），"<資產>-main" 這組固定對應
#    SIM_ENTRY_MAX_PRICE / SIM_LOCK_MAX_SUM。BTC 5m 的 "btc-binance-late-direction" 是實驗組：
#    進場邏輯跟其他三組不同，不用 entryMaxPrice/fair 模型賭單邊，而是只在窗口剩不到
#    10 秒、且現價已經明顯偏離開盤價時才進場賭方向，對齊公開資料裡「window delta」
#    那套做法，且進場後不補鎖利、不提早出場。
#    （附註：conservative/main/loose 找不到能立即鎖住兩邊的機會就空手，不賭單邊——
#    這條退路歷史勝率是 0%（42 戰 0 勝、-$364.75，BTC 上驗證的），關閉／重開過幾次，
#    2026-09 確認維持關閉。核心策略就是「兩邊都買才進場」，找不到就不進場。）
_VARIANT_CONFIGS = [
    {"key": "conservative", "labelSuffix": "保守 0.30/0.90", "entryMaxPrice": 0.30, "lockMaxSum": 0.90},
    {"key": "main",         "labelSuffix": "目前 0.40/0.95", "entryMaxPrice": SIM_ENTRY_MAX_PRICE, "lockMaxSum": SIM_LOCK_MAX_SUM},
    {"key": "loose",        "labelSuffix": "寬鬆 0.45/0.98", "entryMaxPrice": 0.45, "lockMaxSum": 0.98},
]
AB_VARIANTS = []
for _asset in ASSETS:
    if _asset.get("marketMakerOnly"):
        AB_VARIANTS.append({
            "id":              f"{_asset['id']}-mm",
            "assetId":         _asset["id"],
            "label":           f"{_asset['label']} 被動雙邊做市",
            "entryMaxPrice":   None,
            "lockMaxSum":      MM_MAX_PAIR_COST,
            "marketMakerOnly": True,
        })
        continue
    if _asset["id"] == "btc-15m":
        AB_VARIANTS.append({
            "id":                  "btc-15m-dump-then-hedge",
            "assetId":             "btc-15m",
            "label":               "BTC 15m 暴跌後補腿（3s/-15%）",
            "entryMaxPrice":       None,
            "lockMaxSum":          BTC_15M_DUMP_LOCK_MAX_SUM,
            "dumpThenHedge":       True,
            "lookbackSeconds":     BTC_15M_DUMP_LOOKBACK_SECONDS,
            "minMovePct":          BTC_15M_DUMP_MIN_MOVE_PCT,
            "entryWindowSeconds":  BTC_15M_DUMP_ENTRY_WINDOW_SECONDS,
            "targetShares":        BTC_15M_DUMP_TARGET_SHARES,
            "minNetPerShare":      BTC_15M_DUMP_MIN_NET_PER_SHARE,
            "minCashReserveUsd":   BTC_15M_DUMP_MIN_CASH_RESERVE_USD,
        })
    else:
        for _cfg in _VARIANT_CONFIGS:
            # BTC 5m 的保守 0.30/0.90 組已停止觀察；其他資產仍保留同組作橫向比較。
            if _asset["id"] == "btc" and _cfg["key"] == "conservative":
                continue
            AB_VARIANTS.append({
                "id":            f"{_asset['id']}-{_cfg['key']}",
                "assetId":       _asset["id"],
                "label":         f"{_asset['label']} {_cfg['labelSuffix']}",
                "entryMaxPrice": _cfg["entryMaxPrice"],
                "lockMaxSum":    _cfg["lockMaxSum"],
            })
    if _asset["id"] == LIVE_MIRROR_ASSET_ID:
        AB_VARIANTS.append({
            "id":                      f"{_asset['id']}-live-lock",
            "assetId":                 _asset["id"],
            "label":                   f"{_asset['label']} 實盤鏡像・兩腿鎖利",
            "entryMaxPrice":           None,
            "lockMaxSum":              LIVE_MIRROR_LOCK_MAX_SUM,
            "liveMirrorOnly":           True,
            "stakePct":                LIVE_MIRROR_STAKE_PCT,
            "maxPairBudgetUsd":         LIVE_MIRROR_MAX_PAIR_BUDGET_USD,
            "minCashReserveUsd":        LIVE_MIRROR_MIN_CASH_RESERVE_USD,
            "minDepthMultiplier":       LIVE_MIRROR_DEPTH_MULTIPLIER,
            "stabilitySeconds":         LIVE_MIRROR_STABILITY_SECONDS,
        })
    # BTC 15m／4h 保留結算同源 Chainlink；BTC 5m 暫時另開新的 Binance variant id，
    # 讓本次比較不會混入原本 Chainlink 方向組的歷史績效。
    if _asset["id"] == "btc":
        AB_VARIANTS.append({
            "id":                    "btc-binance-late-direction",
            "assetId":               "btc",
            "label":                 "BTC Binance 晚進場方向性（T-10s）",
            "entryMaxPrice":         None,
            "lockMaxSum":            SIM_LOCK_MAX_SUM,
            "lateDirectionOnly":     True,
            "directionSignalSource": "binance_window",
            "lateDirectionMaxPrice": 0.92,
        })
        AB_VARIANTS.append({
            "id":                    "btc-historical-hybrid",
            "assetId":               "btc",
            "label":                 "BTC 歷史混合（鎖利→Chainlink T-10s）",
            "entryMaxPrice":         None,
            "lockMaxSum":            LIVE_MIRROR_LOCK_MAX_SUM,
            "lateDirectionOnly":     True,
            "historicalHybrid":      True,
            "directionSignalSource": "chainlink_twap",
            "lateDirectionMaxPrice": 0.92,
            "stakePct":              LIVE_MIRROR_STAKE_PCT,
            "maxPairBudgetUsd":      LIVE_MIRROR_MAX_PAIR_BUDGET_USD,
            "minCashReserveUsd":     LIVE_MIRROR_MIN_CASH_RESERVE_USD,
            "minDepthMultiplier":    LIVE_MIRROR_DEPTH_MULTIPLIER,
            "stabilitySeconds":      LIVE_MIRROR_STABILITY_SECONDS,
        })
        AB_VARIANTS.append({
            "id":                    "btc-inventory-rotation",
            "assetId":               "btc",
            "label":                 "BTC \u52d5\u614b\u5eab\u5b58\u65cb\u8f49\uff08\u5206\u6279\u2192\u88dc\u817f\uff09",
            "entryMaxPrice":         None,
            "lockMaxSum":            ROTATION_PAIR_MAX_SUM,
            "inventoryRotation":     True,
            "simOnly":               True,
            "sliceShares":           ROTATION_SLICE_SHARES,
            "minEntryEdge":          ROTATION_MIN_EDGE,
            "maxGrossBudgetUsd":     ROTATION_MAX_GROSS_USD,
            "maxResidualShares":     ROTATION_MAX_RESIDUAL_SHARES,
            "hedgeOnlySeconds":      ROTATION_HEDGE_ONLY_SECONDS,
            "actionCooldownSeconds": ROTATION_ACTION_COOLDOWN,
            "residualRiskPremium":   ROTATION_RESIDUAL_RISK_PREMIUM,
            "futureHedgeFeeReserve": ROTATION_FUTURE_HEDGE_FEE_RESERVE,
            "requireChainlinkConfirm": True,
        })
    elif _asset["id"] != "btc-15m" and _asset["binanceSymbol"] == "BTCUSDT":
        AB_VARIANTS.append({
            "id":                    f"{_asset['id']}-chainlink-late-direction",
            "assetId":               _asset["id"],
            "label":                 f"{_asset['label']} Chainlink 晚進場方向性（T-10s）",
            "entryMaxPrice":         None,
            "lockMaxSum":            SIM_LOCK_MAX_SUM,
            "lateDirectionOnly":     True,
            "lateDirectionMaxPrice": 0.92,
        })
del _asset
AB_VARIANT_BY_ID = {v["id"]: v for v in AB_VARIANTS}
MARKET_MAKER_VARIANTS = [v for v in AB_VARIANTS if v.get("marketMakerOnly")]

def _new_market_state() -> dict:
    return {
        "market":        None,   # 目前追蹤的市場（Gamma market 物件）
        "windowEndsAt":  None,   # 這個窗口結束時間（unix ms）
        "upPrice":       None,
        "downPrice":     None,
        "upBook":        {"bids": [], "asks": []},
        "downBook":      {"bids": [], "asks": []},
        "spotPrice":     None,   # Binance Futures 參考價；不是市場結算用的 Chainlink TWAP
        "windowOpenSpotPrice": None,  # 這一輪窗口第一次觀察到的現價，晚進場方向性策略用來算偏移幅度
        "chainlinkTwapPrice": None,
        "chainlinkTwapObservedAt": None,
        "windowOpenChainlinkTwapPrice": None,
        "windowOpenChainlinkTwapObservedAt": None,
        "windowOpenChainlinkTwapSlug": None,
        "spotChangePct": None,   # 24h 漲跌幅
        "klines":        [],     # 真實 1 分鐘 K 線（Binance），畫蠟燭圖用
        "connected":     False,
        "upTokenId":     None,   # 目前這輪視窗的 token id，WS 收到報價時要靠這個反查是哪個資產
        "downTokenId":   None,
        "fair":          None,   # 最近一次算出來的公平價模型結果，WS 觸發的即時評估沿用這份，
                                  # 不用每個 tick 都重算（那要另外打 Binance API，划不來）。
    }

# ── 全域狀態：每個資產各自一份，互不干擾 ─────────────────────────────────────
markets_state = {a["id"]: _new_market_state() for a in ASSETS}
state = markets_state.get("btc") or next(iter(markets_state.values()))

def _new_variant_state() -> dict:
    return {
        "position":           None,  # {windowSlug, side, entryPrice, shares, entryTime, hedged, hedgeSide, hedgePrice, hedgeShares}
        "pendingSettlements": [],    # 已換窗口、結果還沒查到的舊倉位，每輪重試直到查到結果
        "trades":             [],    # 已結算紀錄，最新在前，最多保留 50 筆
        "totalPnl":           0.0,
        "totalTrades":        0,
        "wins":                0,
        "totalFees":           0.0,
        "lockedTrades":        0,
        "directionalTrades":   0,
        "earlyExits":          0,
        "peakPortfolio":       SIM_DEFAULT_BALANCE,
        "maxDrawdown":         0.0,
        "makerQuotes":         None,
        "windowDiagnostics":   [],
        "makerStats": {
            "quotesPlaced": 0,
            "fills": 0,
            "pairedFills": 0,
            "singleLegSettlements": 0,
            "queueVolumeConsumed": 0.0,
            "rescueAttempts": 0,
            "rescueHedges": 0,
            "rescueUnwinds": 0,
            "rescueFailures": 0,
        },
        "dumpHedgeStats": {
            "signalsDetected": 0,
            "leg1Entries": 0,
            "leg1Rejected": 0,
            "hedgeChecks": 0,
            "completedCycles": 0,
            "unhedgedSettlements": 0,
        },
        "rotationStats": {
            "windowsEntered": 0,
            "fills": 0,
            "pairEvents": 0,
            "pairedShares": 0.0,
            "unpairedSettlements": 0,
            "maxResidualShares": 0.0,
            "lastActionAt": 0.0,
        },
    }

ab_states = {v["id"]: _new_variant_state() for v in AB_VARIANTS}
DEFAULT_VARIANT_ID = "btc-main" if "btc-main" in ab_states else AB_VARIANTS[0]["id"]
_window_diag_dirty: set[tuple[str, str]] = set()

# 下注比例／起始資產是所有 A/B 組共用的設定，刻意保持一致，
# 這樣比較結果的差異只來自「進場/鎖利門檻」本身，不會被其他變因干擾。
shared_config = {
    "stakePct":     SIM_DEFAULT_STAKE_PCT,
    "startBalance": SIM_DEFAULT_BALANCE,
    "runId":        int(time.time() * 1000),
}

sim_state = ab_states[DEFAULT_VARIANT_ID]


def _window_diagnostic(variant_id: str, slug: str) -> dict:
    """Return the bounded per-window strategy record, creating it on first sight."""
    items = ab_states[variant_id].setdefault("windowDiagnostics", [])
    decision_diag.trim_old_window_evidence(items)
    for item in items:
        if item.get("windowSlug") == slug:
            return item
    item = {
        "windowSlug": slug,
        "firstSeenAt": time.time(),
        "lastSeenAt": time.time(),
        "status": "observing",
        "evaluations": 0,
        "reasonCounts": {},
        "lastReason": None,
    }
    items.insert(0, item)
    del items[50:]
    _window_diag_dirty.add((variant_id, slug))
    return item


def record_window_diagnostic(
    variant_id: str,
    slug: str,
    reason: str | None = None,
    **details,
) -> dict:
    """Aggregate useful entry evidence without writing one database row per WS tick."""
    item = _window_diagnostic(variant_id, slug)
    item["lastSeenAt"] = time.time()
    if reason is None:
        item["evaluations"] = int(item.get("evaluations", 0)) + 1
    else:
        item["diagnosticEvents"] = int(item.get("diagnosticEvents", 0)) + 1
    if reason:
        counts = item.setdefault("reasonCounts", {})
        counts[reason] = int(counts.get(reason, 0)) + 1
        item["lastReason"] = reason
    for key, value in details.items():
        if value is not None:
            item[key] = value
    delta = details.get("signalDeltaPct")
    if isinstance(delta, (int, float)):
        prior = item.get("maxAbsSignalDeltaPct")
        if prior is None or abs(float(delta)) > float(prior):
            item["maxAbsSignalDeltaPct"] = abs(float(delta))
    pair_sum = details.get("pairDecisionSum")
    if isinstance(pair_sum, (int, float)):
        prior = item.get("bestPairDecisionSum")
        if prior is None or float(pair_sum) < float(prior):
            item["bestPairDecisionSum"] = float(pair_sum)
    raw_pair_sum = details.get("rawPairAskSum")
    if isinstance(raw_pair_sum, (int, float)):
        prior = item.get("bestRawPairAskSum")
        if prior is None or float(raw_pair_sum) < float(prior):
            item["bestRawPairAskSum"] = float(raw_pair_sum)
    remaining = details.get("remainingSeconds")
    if isinstance(remaining, (int, float)):
        item["minRemainingSeconds"] = min(float(remaining), float(item.get("minRemainingSeconds", remaining)))
        item["maxRemainingSeconds"] = max(float(remaining), float(item.get("maxRemainingSeconds", remaining)))
    decision_diag.record(item, reason, details)
    _window_diag_dirty.add((variant_id, slug))
    return item


def start_window_diagnostics(asset_id: str, slug: str, ends_at_ms: float | None) -> None:
    for variant in AB_VARIANTS:
        if variant["assetId"] != asset_id:
            continue
        item = _window_diagnostic(variant["id"], slug)
        item["windowEndsAt"] = ends_at_ms
        _window_diag_dirty.add((variant["id"], slug))


def finalize_window_diagnostics(slug: str) -> None:
    for variant_id, st in ab_states.items():
        for item in st.get("windowDiagnostics", []):
            if item.get("windowSlug") != slug:
                continue
            if item.get("status") == "observing":
                item["status"] = "no_entry"
            item["finalizedAt"] = time.time()
            _window_diag_dirty.add((variant_id, slug))

def set_stake_pct(pct: float) -> None:
    shared_config["stakePct"] = max(SIM_MIN_STAKE_PCT, min(SIM_MAX_STAKE_PCT, float(pct)))
    log.info(f"[SIM] 下注比例調整為 {shared_config['stakePct']:.1f}%（複利，隨資產組合變動，套用到全部 A/B 組）")
    save_sim_state()

def reset_with_balance(start_balance: float) -> None:
    """自訂起始資產 = 重新開始：清空所有 A/B 組的持倉與歷史紀錄，下注比例維持原本設定不變。"""
    shared_config["startBalance"] = max(SIM_MIN_BALANCE, float(start_balance))
    shared_config["runId"] = int(time.time() * 1000)
    for vid in ab_states:
        ab_states[vid] = _new_variant_state()
        ab_states[vid]["peakPortfolio"] = shared_config["startBalance"]
    _btc_15m_ask_history.clear()
    _window_diag_dirty.clear()
    global sim_state
    sim_state = ab_states[DEFAULT_VARIANT_ID]
    save_sim_state()
    log.info(f"[SIM] 重置：起始資產=${shared_config['startBalance']:,.2f}（全部 A/B 組一起重置）")

CLIENTS: set = set()

# ── 工具 ──────────────────────────────────────────────────────────────────

clock_offset = 0.0  # 真實世界時間 - 本機時鐘；本機時鐘可能不準，用 HTTP 回應的 Date header 校正

def _update_clock_offset(headers) -> None:
    """從任何一次 API 回應的 Date header 校正本機時鐘跟真實世界的落差，
    這樣就算本機系統時間不準，倒數計時跟窗口判斷也不會受影響。"""
    global clock_offset
    date_hdr = headers.get("Date")
    if not date_hdr:
        return
    try:
        remote_dt = parsedate_to_datetime(date_hdr)
        if remote_dt.tzinfo is None:
            remote_dt = remote_dt.replace(tzinfo=timezone.utc)
        clock_offset = remote_dt.timestamp() - time.time()
    except Exception:
        pass

def real_now() -> float:
    """校正過的「現在」（unix 秒），不管本機系統時鐘準不準都可信。"""
    return time.time() + clock_offset

def _iso_to_ms(iso_str: str) -> float:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.timestamp() * 1000

def _market_tokens(market: dict):
    """從 Gamma market 物件解析出 (up_token_id, down_token_id)"""
    outcomes = json.loads(market.get("outcomes", "[]"))
    token_ids = json.loads(market.get("clobTokenIds", "[]"))
    mapping = dict(zip(outcomes, token_ids))
    return mapping.get("Up"), mapping.get("Down")


def taker_fee(shares: float, price: float) -> float:
    """Polymarket Crypto taker fee；price 是每股成交價。"""
    if shares <= 0 or price <= 0 or price >= 1:
        return 0.0
    # 官方以 5 位小數精度計費，低於 0.00001 USDC 不收。
    fee = round(shares * SIM_TAKER_FEE_RATE * price * (1 - price), 5)
    return fee if fee >= 0.00001 else 0.0


def marketable_limit_price(book: dict, fill: dict, side: str) -> float:
    """把含滑點的最差成交價向不利方向對齊 tick，作為模擬與實盤共用判斷價。"""
    tick_value = float(book.get("tickSize", 0.01) or 0.01)
    if not 0 < tick_value < 1:
        tick_value = 0.01
    tick = Decimal(str(tick_value))
    raw = Decimal(str(fill["worstPrice"]))
    is_buy = side.upper() == "BUY"
    rounding = ROUND_UP if is_buy else ROUND_DOWN
    units = (raw / tick).to_integral_value(rounding=rounding)
    units += SIM_PRICE_BUFFER_TICKS if is_buy else -SIM_PRICE_BUFFER_TICKS
    price = float(units * tick)
    return max(tick_value, min(1.0 - tick_value, price))


def decision_fill(book: dict, fill: dict, side: str) -> dict:
    """依最差限價建立保守成交假設；只供決策，實際/模擬損益仍用成交均價。"""
    shares = float(fill["shares"])
    price = marketable_limit_price(book, fill, side)
    notional = shares * price
    return {
        "observedVwap": float(fill["vwap"]),
        "decisionPrice": price,
        "decisionNotional": notional,
        "decisionFee": taker_fee(shares, price),
    }


def with_decision_fill(book: dict, fill: dict | None, side: str) -> dict | None:
    if not fill:
        return None
    enriched = dict(fill)
    enriched.update(decision_fill(book, fill, side))
    return enriched


def simulate_book_fill(levels: list, shares: float, side: str) -> dict | None:
    """用目前可見深度模擬 taker 成交。

    BUY 由最低 ask 往上吃，SELL 由最高 bid 往下賣；深度不足時整筆視為未成交，
    等同 FOK。額外套用少量延遲滑點，避免把收到報價的瞬間價格當成必然可得。
    """
    if shares <= 0 or not levels:
        return None
    is_buy = side.upper() == "BUY"
    ordered = sorted(levels, key=lambda x: float(x["price"]), reverse=not is_buy)
    remaining = shares
    notional = 0.0
    worst_price = None
    slip = SIM_SLIPPAGE_BPS / 10_000

    for level in ordered:
        available = max(0.0, float(level.get("size", 0)))
        if available <= 0:
            continue
        raw_price = float(level["price"])
        price = raw_price * (1 + slip if is_buy else 1 - slip)
        price = max(0.001, min(0.999, price))
        take = min(remaining, available)
        notional += take * price
        remaining -= take
        worst_price = price
        if remaining <= 1e-9:
            break

    if remaining > 1e-9:
        return None
    vwap = notional / shares
    fee = taker_fee(shares, vwap)
    return {
        "shares": shares,
        "vwap": vwap,
        "notional": notional,
        "fee": fee,
        "worstPrice": worst_price,
        "side": side.upper(),
    }


def simulate_buy_fill(book: dict, shares: float) -> dict | None:
    return with_decision_fill(book, simulate_book_fill(book.get("asks") or [], shares, "BUY"), "BUY")


def simulate_sell_fill(book: dict, shares: float) -> dict | None:
    return with_decision_fill(book, simulate_book_fill(book.get("bids") or [], shares, "SELL"), "SELL")


def _position_paid_cost(pos: dict) -> float:
    if pos.get("strategyMode") == "inventory_rotation":
        return (
            float(pos.get("upNotional", 0)) + float(pos.get("upFee", 0))
            + float(pos.get("downNotional", 0)) + float(pos.get("downFee", 0))
        )

    cost = float(pos.get("entryNotional", pos["shares"] * pos["entryPrice"])) + float(pos.get("entryFee", 0))
    if pos.get("hedged"):
        cost += float(pos.get("hedgeNotional", pos.get("hedgeShares", 0) * (pos.get("hedgePrice") or 0)))
        cost += float(pos.get("hedgeFee", 0))
    return cost


def _position_decision_cost(pos: dict) -> float:
    """回傳建立部位時的保守最差成本，用來決定後續是否真的能鎖利。"""
    if pos.get("strategyMode") == "inventory_rotation":
        return (
            float(pos.get("upDecisionNotional", 0)) + float(pos.get("upDecisionFee", 0))
            + float(pos.get("downDecisionNotional", 0)) + float(pos.get("downDecisionFee", 0))
        )

    cost = float(pos.get("entryDecisionNotional", pos.get("entryNotional", pos["shares"] * pos["entryPrice"])))
    cost += float(pos.get("entryDecisionFee", pos.get("entryFee", 0)))
    if pos.get("hedged"):
        cost += float(pos.get("hedgeDecisionNotional", pos.get("hedgeNotional", 0)))
        cost += float(pos.get("hedgeDecisionFee", pos.get("hedgeFee", 0)))
    return cost


def estimate_fair_up(asset_id: str) -> dict | None:
    """以窗口開盤附近的 Binance 價格、短期波動與剩餘時間估算 Up 機率。

    這是 Chainlink TWAP 的代理模型，不冒充真正的 Chainlink feed；最後再與市場隱含機率
    混合校準，降低單一交易所瞬間價格造成的過度自信。
    """
    ms = markets_state[asset_id]
    market = ms.get("market") or {}
    slug = market.get("slug") or ""
    klines = ms.get("klines") or []
    spot = ms.get("spotPrice")
    if not slug or not klines or not spot or spot <= 0 or ms.get("windowEndsAt") is None:
        return None
    try:
        window_start = int(slug.rsplit("-", 1)[-1])
    except (TypeError, ValueError):
        return None

    reference = None
    prior = None
    for k in klines:
        kt = int(k["t"] // 1000)
        if kt <= window_start:
            prior = float(k["o"])
        if kt <= window_start < kt + 60:
            reference = float(k["o"])
            break
    reference = reference or prior
    if not reference or reference <= 0:
        return None

    closes = [float(k["c"]) for k in klines[-30:] if float(k.get("c", 0)) > 0]
    returns = [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]
    sigma_60s = pstdev(returns) if len(returns) >= 5 else 0.0
    sigma_per_second = max(SIM_MIN_SIGMA_PER_SECOND.get(asset_id, 0.00003), sigma_60s / math.sqrt(60))
    remaining = max(1.0, ms["windowEndsAt"] / 1000 - real_now())
    # TWAP 會平滑最後一段價格，加入半個資料窗作為保守的有效預測期。
    effective_horizon = remaining + 30.0
    z = math.log(float(spot) / reference) / (sigma_per_second * math.sqrt(effective_horizon))
    model_up = max(0.02, min(0.98, NormalDist().cdf(z)))

    up_mid = ms.get("upPrice")
    down_mid = ms.get("downPrice")
    if up_mid and down_mid and up_mid + down_mid > 0:
        market_up = up_mid / (up_mid + down_mid)
        fair_up = SIM_FAIR_MODEL_WEIGHT * model_up + (1 - SIM_FAIR_MODEL_WEIGHT) * market_up
    else:
        market_up = None
        fair_up = model_up
    fair_up = max(0.02, min(0.98, fair_up))
    return {
        "fairUp": fair_up,
        "fairDown": 1 - fair_up,
        "modelUp": model_up,
        "marketUp": market_up,
        "referencePrice": reference,
        "sigmaPerSecond": sigma_per_second,
        "source": "binance-volatility-proxy+market-calibration",
    }


_sim_db: sqlite3.Connection | None = None


def _get_sim_db() -> sqlite3.Connection:
    global _sim_db
    if _sim_db is None:
        _sim_db = sqlite3.connect(SIM_DB_PATH)
        _sim_db.execute("PRAGMA journal_mode=WAL")
        _sim_db.executescript(
            """
            CREATE TABLE IF NOT EXISTS sim_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sim_state (
                variant_id TEXT PRIMARY KEY,
                run_id INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sim_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                variant_id TEXT NOT NULL,
                window_slug TEXT NOT NULL,
                exit_time REAL NOT NULL,
                trade_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sim_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                ts REAL NOT NULL,
                asset_id TEXT NOT NULL,
                window_slug TEXT,
                remaining_seconds REAL,
                up_bid REAL,
                up_ask REAL,
                down_bid REAL,
                down_ask REAL,
                fair_up REAL,
                spot_price REAL
            );
            CREATE TABLE IF NOT EXISTS sim_window_diagnostics (
                run_id INTEGER NOT NULL,
                variant_id TEXT NOT NULL,
                window_slug TEXT NOT NULL,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                diagnostic_json TEXT NOT NULL,
                PRIMARY KEY(run_id, variant_id, window_slug)
            );
            CREATE INDEX IF NOT EXISTS idx_sim_quotes_asset_ts ON sim_quotes(asset_id, ts);
            CREATE INDEX IF NOT EXISTS idx_sim_trades_variant_time ON sim_trades(variant_id, exit_time);
            CREATE INDEX IF NOT EXISTS idx_sim_window_diag_variant_time
                ON sim_window_diagnostics(variant_id, last_seen);
            """
        )
        _sim_db.commit()
    return _sim_db


def flush_window_diagnostics(db: sqlite3.Connection | None = None, asset_id: str | None = None) -> None:
    target = _get_sim_db() if db is None else db
    run_id = int(shared_config["runId"])
    for variant_id, slug in list(_window_diag_dirty):
        variant = AB_VARIANT_BY_ID.get(variant_id)
        if not variant or (asset_id is not None and variant["assetId"] != asset_id):
            continue
        item = next(
            (row for row in ab_states[variant_id].get("windowDiagnostics", []) if row.get("windowSlug") == slug),
            None,
        )
        if item is None:
            _window_diag_dirty.discard((variant_id, slug))
            continue
        target.execute(
            """INSERT OR REPLACE INTO sim_window_diagnostics(
                   run_id, variant_id, window_slug, first_seen, last_seen, diagnostic_json
               ) VALUES(?,?,?,?,?,?)""",
            (
                run_id,
                variant_id,
                slug,
                float(item.get("firstSeenAt", time.time())),
                float(item.get("lastSeenAt", time.time())),
                json.dumps(item),
            ),
        )
        _window_diag_dirty.discard((variant_id, slug))


def save_sim_state() -> None:
    db = _get_sim_db()
    run_id = int(shared_config["runId"])
    now = time.time()
    db.execute(
        "INSERT OR REPLACE INTO sim_meta(key, value) VALUES('shared_config', ?)",
        (json.dumps(shared_config),),
    )
    for variant_id, st in ab_states.items():
        db.execute(
            "INSERT OR REPLACE INTO sim_state(variant_id, run_id, state_json, updated_at) VALUES(?,?,?,?)",
            (variant_id, run_id, json.dumps(st), now),
        )
    flush_window_diagnostics(db)
    db.commit()


def load_sim_state() -> None:
    global sim_state
    db = _get_sim_db()
    meta = db.execute("SELECT value FROM sim_meta WHERE key='shared_config'").fetchone()
    if meta:
        try:
            loaded_config = json.loads(meta[0])
            shared_config.update(loaded_config)
        except Exception as exc:
            log.warning(f"[SIM] 無法載入共用設定，改用預設值：{exc}")
    rows = db.execute("SELECT variant_id, state_json FROM sim_state").fetchall()
    for variant_id, state_json in rows:
        if variant_id not in ab_states:
            continue
        try:
            loaded = json.loads(state_json)
            defaults = _new_variant_state()
            defaults.update(loaded)
            ab_states[variant_id] = defaults
        except Exception as exc:
            log.warning(f"[SIM:{variant_id}] 無法載入狀態，改用空白狀態：{exc}")
    run_id = int(shared_config["runId"])
    for variant_id in ab_states:
        diag_rows = db.execute(
            """SELECT diagnostic_json FROM sim_window_diagnostics
               WHERE run_id=? AND variant_id=? ORDER BY last_seen DESC LIMIT 50""",
            (run_id, variant_id),
        ).fetchall()
        if diag_rows:
            loaded_diagnostics = []
            for row in diag_rows:
                try:
                    loaded_diagnostics.append(json.loads(row[0]))
                except (TypeError, json.JSONDecodeError) as exc:
                    log.warning(f"[SIM:{variant_id}] skipping corrupt window diagnostic: {exc}")
            ab_states[variant_id]["windowDiagnostics"] = loaded_diagnostics
    sim_state = ab_states[DEFAULT_VARIANT_ID]


def persist_trade(variant_id: str, trade: dict) -> None:
    db = _get_sim_db()
    db.execute(
        "INSERT INTO sim_trades(run_id, variant_id, window_slug, exit_time, trade_json) VALUES(?,?,?,?,?)",
        (int(shared_config["runId"]), variant_id, trade["windowSlug"], trade["exitTime"], json.dumps(trade)),
    )
    db.commit()


def persist_quote(asset_id: str, fair: dict | None) -> None:
    ms = markets_state[asset_id]
    market = ms.get("market") or {}
    up_bids, up_asks = ms["upBook"].get("bids") or [], ms["upBook"].get("asks") or []
    down_bids, down_asks = ms["downBook"].get("bids") or [], ms["downBook"].get("asks") or []
    remaining = None if ms.get("windowEndsAt") is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    db = _get_sim_db()
    db.execute(
        """INSERT INTO sim_quotes(
               run_id, ts, asset_id, window_slug, remaining_seconds,
               up_bid, up_ask, down_bid, down_ask, fair_up, spot_price
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (
            int(shared_config["runId"]), time.time(), asset_id, market.get("slug"), remaining,
            max((x["price"] for x in up_bids), default=None), min((x["price"] for x in up_asks), default=None),
            max((x["price"] for x in down_bids), default=None), min((x["price"] for x in down_asks), default=None),
            fair.get("fairUp") if fair else None, ms.get("spotPrice"),
        ),
    )
    flush_window_diagnostics(db, asset_id)
    db.commit()

# ── Polymarket API ─────────────────────────────────────────────────────────

WINDOW_SECONDS = 300  # 預設窗口長度（5 分鐘），沒在 asset 設定裡指定 windowSeconds 時使用

async def fetch_active_market(
    session: aiohttp.ClientSession, slug_prefix: str, window_seconds: int = WINDOW_SECONDS
) -> dict | None:
    """直接用真實時間算出目前這個窗口的 slug 去查，不掃描 Gamma 的市場列表。

    原本用 active=true&closed=false 篩選、依 startDate 排序去找，結果發現不可靠：
    Polymarket 會把未來一整天的窗口都預先建好（全部也是 active=true），
    也有很多從很久以前就從沒被正確標記 closed 的舊窗口卡在列表裡，
    不管排序方向，抓到的都不是「現在正在進行」的那一個。
    直接用時間算 slug（格式：<slug_prefix><窗口開始時間的 unix 秒>）最準，
    這套邏輯跟資產無關，只是帶入的 slug_prefix／window_seconds 不同
    （2026-09 起同一個資產也可能同時追蹤好幾種窗口長度，例如 BTC 的 5m/15m/4h）。
    """
    window_start = int(real_now() // window_seconds) * window_seconds
    for start in (window_start, window_start - window_seconds):  # 抓不到當前窗口就退回上一個（剛好在交界處時的備援）
        slug = f"{slug_prefix}{start}"
        m = await fetch_market_by_slug(session, slug)
        if m and not m.get("closed", True):
            return m
    return None

async def fetch_market_by_slug(session: aiohttp.ClientSession, slug: str) -> dict | None:
    url = f"{GAMMA_BASE}/markets/slug/{slug}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        _update_clock_offset(r.headers)
        if r.status != 200:
            return None
        return await r.json()

async def fetch_midpoint(session: aiohttp.ClientSession, token_id: str) -> float:
    url = f"{CLOB_BASE}/midpoint?token_id={token_id}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        return float(d.get("mid", 0) or 0)

async def fetch_book(session: aiohttp.ClientSession, token_id: str, limit: int = 6) -> dict:
    url = f"{CLOB_BASE}/book?token_id={token_id}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        bids = sorted(d.get("bids", []), key=lambda x: -float(x["price"]))[:limit]
        asks = sorted(d.get("asks", []), key=lambda x: float(x["price"]))[:limit]
        return {
            "bids": [{"price": float(b["price"]), "size": float(b["size"])} for b in bids],
            "asks": [{"price": float(a["price"]), "size": float(a["size"])} for a in asks],
            "tickSize": float(d.get("tick_size", 0.01) or 0.01),
            "minOrderSize": float(d.get("min_order_size", 1) or 1),
        }

async def fetch_spot_price(session: aiohttp.ClientSession, symbol: str) -> dict:
    """Binance Futures 參考價與 24h 漲跌；函式名保留是為了相容既有呼叫。"""
    url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        return {"price": float(d["lastPrice"]), "changePct": float(d["priceChangePercent"])}

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, limit: int = 60) -> list:
    """真實 1 分鐘 K 線（Binance），畫成蠟燭圖讓畫面更有感、看得出價格走勢"""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        raw = await r.json()
        return [
            {"t": k[0], "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
            for k in raw
        ]

# ── Binance 期貨即時報價（WS，2026-09）─────────────────────────────────────
# 原本 spotPrice 只靠 REST 每 3 秒 poll 一次 Binance，理論價（theo）最多可能落後
# 真實行情快 3 秒——這段時間 Polymarket 訂單簿可能已經先反應了，我們卻還在用
# 舊的現貨價算公平機率，容易被抓到「報價沒跟上」的逆選擇機會（做市原型觀察到
# 的虧損就是這個模式）。額外開一條 Binance WS 連線，用 bookTicker（每次最佳
# 買賣價變動就推播，通常是秒等級以下）即時更新，theo 計算時永遠讀最新報價，
# 不用等下一輪 3 秒 poll 才看得到。斷線時退回 REST poll 到的價格（見
# get_binance_ws_price 的 max_age 判斷），不會整段沒有報價可用。
_binance_ws_price: dict = {}  # symbol -> {"price": float, "at": monotonic 時間}


async def binance_ws_loop() -> None:
    symbols = sorted({a["binanceSymbol"] for a in ASSETS})
    if not symbols:
        return
    stream = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    url = f"wss://fstream.binance.com/stream?streams={stream}"
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                log.info(f"[Binance-WS] 已連線，訂閱 {len(symbols)} 個商品即時報價")
                backoff_idx = 0
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data") or msg
                        symbol, bid, ask = data.get("s"), data.get("b"), data.get("a")
                        if symbol and bid and ask:
                            _binance_ws_price[symbol] = {
                                "price": (float(bid) + float(ask)) / 2,
                                "at": time.monotonic(),
                            }
                            _on_binance_price_tick(symbol)
                    except Exception:
                        continue
        except Exception as exc:
            log.warning(f"[Binance-WS] 連線失敗，稍後重試（期間退回 REST 報價）：{exc}")
        backoff_idx = min(backoff_idx + 1, len(WS_RECONNECT_BACKOFF) - 1)
        await asyncio.sleep(WS_RECONNECT_BACKOFF[backoff_idx])


def get_binance_ws_price(symbol: str, max_age_seconds: float = 5.0) -> float | None:
    """回傳 WS 即時報價；太舊（連線斷過還沒重連上）就不採用，讓呼叫端退回 REST 報價。"""
    entry = _binance_ws_price.get(symbol)
    if not entry or time.monotonic() - entry["at"] > max_age_seconds:
        return None
    return entry["price"]


def _market_window_start_ms(market: dict | None) -> int | None:
    slug = (market or {}).get("slug") or ""
    try:
        return int(slug.rsplit("-", 1)[-1]) * 1000
    except (TypeError, ValueError):
        return None


def _capture_window_open_chainlink_twap(ms: dict) -> bool:
    """Bind a market to the exact RTDS TWAP observation at its start boundary.

    RTDS publishes one observation per second.  We deliberately refuse a nearby
    Binance value or a late "first value seen" because neither is the market's
    Chainlink price-to-beat.
    """
    market = ms.get("market") or {}
    slug = market.get("slug")
    if slug and ms.get("windowOpenChainlinkTwapSlug") == slug:
        return True
    target_ms = _market_window_start_ms(market)
    if not slug or target_ms is None:
        return False
    candidates = [
        (abs(observed_ms - target_ms), observed_ms, value)
        for observed_ms, value in _chainlink_twap_history
        if abs(observed_ms - target_ms) <= CHAINLINK_BOUNDARY_TOLERANCE_MS
    ]
    if not candidates:
        return False
    _, observed_ms, value = min(candidates)
    ms["windowOpenChainlinkTwapPrice"] = value
    ms["windowOpenChainlinkTwapObservedAt"] = observed_ms
    ms["windowOpenChainlinkTwapSlug"] = slug
    return True


def get_chainlink_twap_signal(asset_id: str, now_ms: float | None = None) -> dict | None:
    """Return fresh settlement-aligned current/open TWAP values, else ``None``."""
    ms = markets_state.get(asset_id)
    if not ms or not _capture_window_open_chainlink_twap(ms):
        return None
    observed_ms = ms.get("chainlinkTwapObservedAt")
    current = ms.get("chainlinkTwapPrice")
    opening = ms.get("windowOpenChainlinkTwapPrice")
    if not isinstance(observed_ms, (int, float)) or not current or not opening:
        return None
    wall_ms = time.time() * 1000 if now_ms is None else float(now_ms)
    age_ms = wall_ms - float(observed_ms)
    if age_ms < -1000 or age_ms > CHAINLINK_TWAP_MAX_AGE_SECONDS * 1000:
        return None
    return {
        "current": float(current),
        "opening": float(opening),
        "observedAt": int(observed_ms),
        "ageSeconds": max(0.0, age_ms / 1000),
        "windowSeconds": CHAINLINK_TWAP_WINDOW_SECONDS,
    }


def chainlink_twap_status() -> dict:
    observed_ms = _chainlink_twap_latest.get("observedAt")
    age = None if observed_ms is None else max(0.0, time.time() - float(observed_ms) / 1000)
    return {
        "connected": bool(_chainlink_twap_latest.get("connected")),
        "healthy": bool(age is not None and age <= CHAINLINK_TWAP_MAX_AGE_SECONDS),
        "lastObservationAgeSeconds": age,
        "windowSeconds": CHAINLINK_TWAP_WINDOW_SECONDS,
    }


def _run_chainlink_simulation_tick(aid: str) -> None:
    ms = markets_state[aid]
    market = ms.get("market") or {}
    up_book, down_book = ms.get("upBook"), ms.get("downBook")
    if not market or not up_book or not down_book:
        return
    remaining = None if ms.get("windowEndsAt") is None else max(
        0.0, ms["windowEndsAt"] / 1000 - real_now()
    )
    for variant_id, variant in AB_VARIANT_BY_ID.items():
        if (
            variant["assetId"] == aid
            and variant.get("lateDirectionOnly")
            and variant.get("directionSignalSource") != "binance_window"
            and _variant_books_are_coherent(aid, variant, up_book, down_book)
        ):
            simulate_trading(
                variant_id, market["slug"], up_book, down_book,
                remaining, ms.get("fair"), False, evaluation_source="chainlink",
            )


def _on_chainlink_twap_tick() -> None:
    """Prioritize live Chainlink evaluation before equivalent simulations."""
    for asset in ASSETS:
        if asset.get("binanceSymbol") != "BTCUSDT":
            continue
        aid = asset["id"]
        ms = markets_state[aid]
        _capture_window_open_chainlink_twap(ms)
        market = ms.get("market") or {}
        if not market or not ms.get("upBook") or not ms.get("downBook"):
            continue
        live_action = bool(
            aid == "btc"
            and ms.get("upTokenId")
            and _notify_ws_price_listeners(ms["upTokenId"], "chainlink")
        )
        if live_action:
            _defer_simulation_tick(
                "chainlink", aid, lambda aid=aid: _run_chainlink_simulation_tick(aid)
            )
        else:
            _run_chainlink_simulation_tick(aid)


def _apply_chainlink_twap_message(message: dict) -> int:
    """Apply either an RTDS history subscription response or one live update."""
    if not isinstance(message, dict) or message.get("topic") != "crypto_prices_twap_sixty":
        return 0
    payload = message.get("payload") or {}
    if payload.get("symbol") not in (None, "btc/usd"):
        return 0
    rows = payload.get("data") if isinstance(payload.get("data"), list) else [payload]
    applied = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        # RTDS 的即時 update 把 window_s 放在單筆 payload；初始 subscribe history
        # 則放在外層 payload，data 內的每筆 observation 不會重複帶這個欄位。
        window_seconds = row.get("window_s", payload.get("window_s", 0))
        if int(window_seconds or 0) != CHAINLINK_TWAP_WINDOW_SECONDS:
            continue
        try:
            observed_ms = int(row["timestamp"])
            if row.get("full_accuracy_value") not in (None, ""):
                value = float(Decimal(str(row["full_accuracy_value"])) / Decimal(10**18))
            else:
                value = float(row["value"])
        except (KeyError, TypeError, ValueError, ArithmeticError):
            continue
        if value <= 0:
            continue
        _chainlink_twap_history.append((observed_ms, value))
        latest_ms = int(_chainlink_twap_latest.get("observedAt", 0) or 0)
        if observed_ms >= latest_ms:
            _chainlink_twap_latest.update({"price": value, "observedAt": observed_ms, "connected": True})
            for asset in ASSETS:
                if asset.get("binanceSymbol") == "BTCUSDT":
                    ms = markets_state[asset["id"]]
                    ms["chainlinkTwapPrice"] = value
                    ms["chainlinkTwapObservedAt"] = observed_ms
        applied += 1
    if applied:
        _on_chainlink_twap_tick()
    return applied


async def chainlink_twap_loop() -> None:
    subscription = {
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_twap_sixty",
            "type": "update",
            "filters": '{"symbol":"btc/usd"}',
        }],
    }
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(CHAINLINK_RTDS_URL, ping_interval=None) as ws:
                await ws.send(json.dumps(subscription))
                _chainlink_twap_latest["connected"] = True
                log.info("[Chainlink-RTDS] 已連線，訂閱 BTC/USD 60 秒 TWAP")
                backoff_idx = 0
                last_ping = time.monotonic()
                while True:
                    timeout = max(0.1, 5.0 - (time.monotonic() - last_ping))
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        raw = None
                    if time.monotonic() - last_ping >= 5.0:
                        await ws.send("PING")
                        last_ping = time.monotonic()
                    if not raw or raw == "PONG":
                        continue
                    try:
                        _apply_chainlink_twap_message(json.loads(raw))
                    except Exception as exc:
                        log.warning(f"[Chainlink-RTDS] 忽略無法解析的更新：{exc}")
        except Exception as exc:
            log.warning(f"[Chainlink-RTDS] 連線失敗，方向性策略暫停：{exc}")
        _chainlink_twap_latest["connected"] = False
        delay = WS_RECONNECT_BACKOFF[min(backoff_idx, len(WS_RECONNECT_BACKOFF) - 1)]
        backoff_idx += 1
        await asyncio.sleep(delay)


def _run_binance_simulation_tick(aid: str) -> None:
    ms = markets_state[aid]
    fair = estimate_fair_up(aid)
    if not fair:
        return
    ms["fair"] = fair
    slug = ms["market"]["slug"]
    remaining_seconds = (
        None if ms["windowEndsAt"] is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    )
    up_book, down_book = ms.get("upBook"), ms.get("downBook")
    if up_book and down_book:
        for variant_id, variant in AB_VARIANT_BY_ID.items():
            if variant["assetId"] == aid and _variant_books_are_coherent(
                aid, variant, up_book, down_book
            ):
                simulate_trading(
                    variant_id, slug, up_book, down_book, remaining_seconds, fair,
                    allow_early_exit=False,
                    evaluation_source="binance",
                )


def _on_binance_price_tick(symbol: str) -> None:
    """Binance 現貨價一有變動就立刻重算 theo、重跑鎖利判斷（純記憶體運算，沒有 I/O，
    很便宜，可以跑得比 3 秒輪詢頻繁很多）。跟 _on_ws_price_tick（Polymarket 訂單簿
    一有變動就重跑）是同一個精神，差別是這裡的觸發源是 Binance 報價本身有變化——
    舊版即使訂閱了 Binance WS，也只在 3 秒 poll 週期重算 theo，等於報價早就是新的、
    theo 卻還是舊的；這裡改成報價一變就重算，theo 不再是整段窗口內最多落後 3 秒的
    舊資料。"""
    price = get_binance_ws_price(symbol)
    if price is None:
        return
    for asset in ASSETS:
        if asset["binanceSymbol"] != symbol:
            continue
        aid = asset["id"]
        ms = markets_state[aid]
        if not ms.get("market"):
            continue
        ms["spotPrice"] = price
        # BTC 5m 實盤嵌入模式也要由 Binance tick 立即觸發判斷，否則雖然改用 Binance
        # 當訊號，真正送單仍只會等 Polymarket book／Chainlink 更新才被動重算。
        live_action = bool(
            aid == "btc"
            and ms.get("upTokenId")
            and _notify_ws_price_listeners(ms["upTokenId"], "binance")
        )
        if live_action:
            _defer_simulation_tick(
                "binance", aid, lambda aid=aid: _run_binance_simulation_tick(aid)
            )
        else:
            _run_binance_simulation_tick(aid)


async def fetch_outcome(session: aiohttp.ClientSession, slug: str) -> str | None:
    """查這個窗口的結算結果，還沒結算完成回傳 None"""
    m = await fetch_market_by_slug(session, slug)
    if not m or not m.get("closed"):
        return None
    outcomes = json.loads(m.get("outcomes", "[]"))
    prices = json.loads(m.get("outcomePrices", "[]"))
    for o, p in zip(outcomes, prices):
        if float(p) >= 0.99:
            return o
    return None

# ── 模擬自動交易（紙上交易，不動用真實資金）───────────────────────────────────

# 單組兩腿的資金上限與現金保留額。模擬版與真實下單版共用同一套計算公式
# （target_pair_order），只有這兩個數字、stake_pct 各自可能設定不同的值——
# 真實版可以用更保守的上限，但「怎麼從現金換算成股數」這件事本身完全一致。
SIM_MAX_PAIR_BUDGET_USD  = 25.0
SIM_MIN_CASH_RESERVE_USD = 5.0


def target_pair_order(
    cash: float,
    stake_pct: float,
    lock_max_sum: float,
    max_pair_budget_usd: float = SIM_MAX_PAIR_BUDGET_USD,
    min_cash_reserve_usd: float = SIM_MIN_CASH_RESERVE_USD,
) -> tuple[float, float]:
    """回傳（目標股數, 本輪兩腿總資金預算）。模擬版與真實下單版共用這個函式，
    保證「下注比例怎麼換算成實際股數」的邏輯完全一致，不是各寫一份、之後容易長歪。
    股數無條件捨去到整數，對應真實下單實際能送出的精度。"""
    available = max(0.0, cash - min_cash_reserve_usd)
    budget = min(max_pair_budget_usd, available * (stake_pct / 100))
    # 每腿 taker fee 的理論上限為 rate × 0.25；兩腿一起預留，避免補腿時才發現現金不足。
    max_two_leg_fee_per_share = 2 * SIM_TAKER_FEE_RATE * 0.25
    budget_per_share = lock_max_sum + max_two_leg_fee_per_share
    if budget <= 0 or budget_per_share <= 0:
        return 0.0, budget
    shares = float((Decimal(str(budget)) / Decimal(str(budget_per_share))).to_integral_value(rounding=ROUND_DOWN))
    return shares, budget


def _target_order_size(variant_id: str) -> tuple[float, float]:
    """模擬版包一層：帶入這組 A/B 變體自己的現金與鎖利門檻。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    cash, _ = compute_cash_and_portfolio(variant_id)
    return target_pair_order(
        cash,
        float(variant.get("stakePct", shared_config["stakePct"])),
        variant["lockMaxSum"],
        float(variant.get("maxPairBudgetUsd", SIM_MAX_PAIR_BUDGET_USD)),
        float(variant.get("minCashReserveUsd", SIM_MIN_CASH_RESERVE_USD)),
    )


_pair_stability_candidates: dict[str, dict] = {}
_direction_stability_candidates: dict[str, dict] = {}
_btc_15m_ask_history: dict[str, dict[str, deque[tuple[float, float]]]] = {}


def _dump_hedge_stats(st: dict) -> dict:
    defaults = {
        "signalsDetected": 0,
        "leg1Entries": 0,
        "leg1Rejected": 0,
        "hedgeChecks": 0,
        "completedCycles": 0,
        "unhedgedSettlements": 0,
    }
    stats = st.setdefault("dumpHedgeStats", {})
    for key, value in defaults.items():
        stats.setdefault(key, value)
    return stats


def _record_btc_15m_asks(
    slug: str,
    up_book: dict,
    down_book: dict,
    lookback_seconds: float,
) -> dict[str, dict]:
    """記錄兩腿 WebSocket best ask，回傳相對約 lookback 秒前的跌幅訊號。

    使用各腿實際收到 WS 更新的 monotonic timestamp，而不是策略被重算的時間，避免
    Binance／輪詢等額外觸發把同一份舊報價重複寫入，錯誤製造出「持續三秒」的歷史。
    """
    histories = _btc_15m_ask_history.setdefault(
        slug, {"Up": deque(maxlen=4096), "Down": deque(maxlen=4096)}
    )
    signals: dict[str, dict] = {}
    keep_seconds = max(10.0, float(lookback_seconds) * 4)
    for side, book in (("Up", up_book), ("Down", down_book)):
        asks = book.get("asks") or []
        observed_at = book.get("receivedAtMonotonic")
        if (
            not asks
            or book.get("quoteSource") != "websocket"
            or not isinstance(observed_at, (int, float))
        ):
            continue
        price = float(asks[0]["price"])
        history = histories[side]
        if history and float(observed_at) <= history[-1][0] + 1e-9:
            continue
        history.append((float(observed_at), price))
        cutoff = float(observed_at) - float(lookback_seconds)
        while len(history) > 1 and history[1][0] < float(observed_at) - keep_seconds:
            history.popleft()
        reference = None
        for sample_at, sample_price in reversed(history):
            if sample_at <= cutoff:
                reference = (sample_at, sample_price)
                break
        if reference and reference[1] > 0:
            drop_pct = (reference[1] - price) / reference[1] * 100
            signals[side] = {
                "side": side,
                "dropPct": drop_pct,
                "referencePrice": reference[1],
                "referenceAt": reference[0],
                "currentPrice": price,
                "currentAt": float(observed_at),
            }
    return signals


def executable_ask_depth(book: dict, limit_price: float) -> float:
    """回傳 BUY 限價以內真正可吃到的賣盤深度，不把更貴、訂單根本碰不到的檔位算進來。"""
    return sum(
        max(0.0, float(level.get("size", 0)))
        for level in (book.get("asks") or [])
        if float(level.get("price", 0)) <= float(limit_price) + 1e-9
    )


def pair_depth_is_safe(
    up_book: dict,
    down_book: dict,
    up_limit: float,
    down_limit: float,
    shares: float,
    multiplier: float,
) -> bool:
    """兩腿各自在限價內都要有數倍於下單量的深度，降低送達前被別人搶光的機率。"""
    required = float(shares) * max(1.0, float(multiplier))
    return (
        executable_ask_depth(up_book, up_limit) + 1e-9 >= required
        and executable_ask_depth(down_book, down_limit) + 1e-9 >= required
    )


def pair_candidate_is_stable(
    key: str,
    slug: str,
    shares: float,
    up_limit: float,
    down_limit: float,
    seconds: float,
    now: float | None = None,
) -> bool:
    """同一市場的鎖利條件必須持續成立一段時間，過濾只閃現一個 WS tick 的假機會。

    股數與兩腿限價可以隨訂單簿更新；呼叫端每個 tick 都會重新驗證最新價格、深度與費用。
    只有市場改變或任一 tick 不再符合鎖利條件時，穩定計時才會重新開始。
    """
    current = time.monotonic() if now is None else float(now)
    signature = (slug,)
    previous = _pair_stability_candidates.get(key)
    if not previous or previous.get("signature") != signature:
        _pair_stability_candidates[key] = {"signature": signature, "since": current}
        return float(seconds) <= 0
    return current - float(previous["since"]) >= float(seconds)


def clear_pair_candidate(key: str) -> None:
    _pair_stability_candidates.pop(key, None)


def direction_candidate_is_stable(
    key: str,
    slug: str,
    side: str,
    seconds: float,
    now: float | None = None,
) -> bool:
    """方向、窗口與全部風控條件必須連續成立，避免把單一跳價當成 15 分鐘訊號。"""
    current = time.monotonic() if now is None else float(now)
    signature = (slug, side)
    previous = _direction_stability_candidates.get(key)
    if not previous or previous.get("signature") != signature:
        _direction_stability_candidates[key] = {"signature": signature, "since": current}
        return float(seconds) <= 0
    return current - float(previous["since"]) >= float(seconds)


def clear_direction_candidate(key: str) -> None:
    _direction_stability_candidates.pop(key, None)


def enter_position(
    variant_id: str,
    slug: str,
    side: str,
    fill: dict,
    stake_budget: float,
    fair_probability: float | None,
    entry_edge: float | None,
) -> None:
    st = ab_states[variant_id]
    st["position"] = {
        "windowSlug":      slug,
        "side":            side,
        "entryPrice":      fill["vwap"],
        "entryNotional":   fill["notional"],
        "entryFee":        fill["fee"],
        "entryWorstPrice": fill["worstPrice"],
        "entryDecisionPrice": fill["decisionPrice"],
        "entryDecisionNotional": fill["decisionNotional"],
        "entryDecisionFee": fill["decisionFee"],
        "shares":          fill["shares"],
        "stakeBudgetUsd":  stake_budget,
        "stakeUsd":        fill["notional"] + fill["fee"],
        "entryStakeUsd":   fill["notional"] + fill["fee"],
        "fairProbability": fair_probability,
        "entryEdge":       entry_edge,
        "entryTime":       time.time(),
        "hedged":          False,
        "hedgeSide":       None,
        "hedgePrice":      None,
        "hedgeShares":     0.0,
        "hedgeNotional":   0.0,
        "hedgeFee":        0.0,
        "lockedPnl":       None,
    }
    item = record_window_diagnostic(
        variant_id,
        slug,
        "entered",
        status="entered",
        entrySide=side,
        entryDecisionPrice=fill["decisionPrice"],
        entryVwap=fill["vwap"],
        entryShares=fill["shares"],
        entryTime=st["position"]["entryTime"],
    )
    item["entryCount"] = int(item.get("entryCount", 0)) + 1
    save_sim_state()
    log.info(
        f"[SIM:{variant_id}] 進場 {side} VWAP=${fill['vwap']:.4f} decision=${fill['decisionPrice']:.4f} "
        f"fee=${fill['fee']:.4f} "
        f"股數={fill['shares']:.2f} edge={entry_edge if entry_edge is not None else float('nan'):+.4f}"
    )


def hedge_position(variant_id: str, side: str, fill: dict) -> None:
    pos = ab_states[variant_id]["position"]
    pos["hedged"] = True
    pos["hedgeSide"] = side
    pos["hedgePrice"] = fill["vwap"]
    pos["hedgeShares"] = fill["shares"]
    pos["hedgeNotional"] = fill["notional"]
    pos["hedgeFee"] = fill["fee"]
    pos["hedgeWorstPrice"] = fill["worstPrice"]
    pos["hedgeDecisionPrice"] = fill["decisionPrice"]
    pos["hedgeDecisionNotional"] = fill["decisionNotional"]
    pos["hedgeDecisionFee"] = fill["decisionFee"]
    pos["stakeUsd"] = _position_paid_cost(pos)
    pos["lockedPnl"] = pos["shares"] - _position_paid_cost(pos)
    record_window_diagnostic(
        variant_id,
        pos["windowSlug"],
        "hedged",
        status="entered_locked",
        hedgeSide=side,
        hedgeDecisionPrice=fill["decisionPrice"],
        lockedPnl=pos["lockedPnl"],
    )
    save_sim_state()
    log.info(
        f"[SIM:{variant_id}] 配對鎖利 {side} VWAP=${fill['vwap']:.4f} decision=${fill['decisionPrice']:.4f} "
        f"fee=${fill['fee']:.4f} "
        f"淨鎖利=${pos['lockedPnl']:+.2f}"
    )


# ── 診斷用：比對模擬盤／實盤兩條獨立 WS 連線在同一個瞬間看到的報價是否有落差 ──
# 只在兩邊最佳賣價加總「接近」鎖利門檻時才記錄（不是每個 tick 都記，會洗版），
# 且每個 tag 節流最多每 0.5 秒記一次。查完之後這段可以刪掉，不影響正式邏輯。
_diag_log_at: dict = {}
DIAG_NEAR_MISS_MARGIN = 0.05


def log_price_sum_diagnostic(tag: str, up_book: dict, down_book: dict, lock_max_sum: float) -> None:
    up_asks = up_book.get("asks") or []
    down_asks = down_book.get("asks") or []
    if not up_asks or not down_asks:
        return
    up_ask = float(up_asks[0]["price"])
    down_ask = float(down_asks[0]["price"])
    price_sum = up_ask + down_ask
    if price_sum > lock_max_sum + DIAG_NEAR_MISS_MARGIN:
        return
    now = time.monotonic()
    if now - _diag_log_at.get(tag, 0.0) < 0.5:
        return
    _diag_log_at[tag] = now
    log.info(
        f"[DIAG:{tag}] t={time.time():.3f} price_sum={price_sum:.4f} "
        f"up_ask={up_ask:.4f} down_ask={down_ask:.4f} lockMaxSum={lock_max_sum:.2f}"
    )


def _entry_candidate(side: str, book: dict, shares: float, fair_probability: float, max_price: float) -> dict | None:
    """單邊進場候選：買價要 <= entryMaxPrice，且公平機率扣掉全部成本後至少留 SIM_MIN_ENTRY_EDGE。

    2026-09-11：依使用者要求重新啟用。這條「找不到鎖利就先買便宜那一腿賭單邊」的退路，
    2026-09 初曾因 42 戰 0 勝、-$364.75 而關閉；現在重開是要在目前「兩腿加總卡在 $1.00、
    鎖利門檻幾乎碰不到」的市場條件下重新驗證。只影響有設 entryMaxPrice 的模擬變體
    （conservative／main／loose），實盤用的 btc-historical-hybrid 是 None，不受影響。
    跟真實版一樣不先按深度縮小股數：深度不夠 simulate_buy_fill 會直接回傳 None（等同 FOK 未成交）。"""
    fill = simulate_buy_fill(book, shares)
    if not fill or fill["decisionNotional"] < SIM_MIN_ORDER_NOTIONAL_USD or fill["decisionPrice"] > max_price:
        return None
    all_in_per_share = (fill["decisionNotional"] + fill["decisionFee"]) / fill["shares"]
    edge = fair_probability - all_in_per_share
    if edge < SIM_MIN_ENTRY_EDGE:
        return None
    return {"side": side, "fill": fill, "fair": fair_probability, "edge": edge}


def _try_single_leg_entry(
    variant_id: str, slug: str, up_book: dict, down_book: dict, fair: dict | None
) -> bool:
    """找不到兩腿鎖利時，用公平價模型挑一邊先進場（之後由既有補鎖利邏輯嘗試補另一腿）。"""
    if not SIM_SINGLE_LEG_ENTRY_ENABLED:
        return False
    variant = AB_VARIANT_BY_ID[variant_id]
    max_price = variant.get("entryMaxPrice")
    if max_price is None:
        return False
    if not fair:
        record_window_diagnostic(variant_id, slug, "single_leg_no_fair_model")
        return False
    target_shares, budget = _target_order_size(variant_id)
    if target_shares <= 0 or budget < SIM_MIN_ORDER_NOTIONAL_USD:
        record_window_diagnostic(variant_id, slug, "single_leg_budget_too_small")
        return False
    candidates = [
        _entry_candidate("Up", up_book, target_shares, fair["fairUp"], max_price),
        _entry_candidate("Down", down_book, target_shares, fair["fairDown"], max_price),
    ]
    candidates = [c for c in candidates if c]
    if not candidates:
        record_window_diagnostic(
            variant_id, slug, "single_leg_no_candidate",
            entryMaxPrice=max_price, fairUp=fair.get("fairUp"), fairDown=fair.get("fairDown"),
        )
        return False
    best = max(candidates, key=lambda c: c["edge"])
    enter_position(variant_id, slug, best["side"], best["fill"], budget, best["fair"], best["edge"])
    record_window_diagnostic(variant_id, slug, "single_leg_entered", side=best["side"], edge=best["edge"])
    return True


def _try_direct_pair(variant_id: str, slug: str, up_book: dict, down_book: dict) -> bool:
    """先檢查兩腿此刻是否可直接成交並鎖住淨利，這才是進場即無方向曝險的套利。

    股數會先按可見深度的 SIM_DEPTH_CAP_FRACTION（見該常數註解）封頂——超過這個比例
    會開始明顯吃掉自己的成交價，模擬出來的利潤會比實際能拿到的樂觀，也更容易在
    真實下單時因為深度已經被搶走而被拒。封頂之後如果連目標股數都吃不滿，才照原本
    邏輯整筆視為不可行（simulate_buy_fill 深度不足回傳 None）。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    stability_key = f"sim:{variant_id}"

    execution_safe = bool(variant.get("liveMirrorOnly") or variant.get("executionSafePair"))

    def reject(reason: str, **details) -> bool:
        record_window_diagnostic(variant_id, slug, reason, **details)
        if execution_safe:
            clear_pair_candidate(stability_key)
        return False

    shares, budget = _target_order_size(variant_id)
    if shares <= 0:
        return reject("pair_insufficient_budget", targetShares=shares, budgetUsd=budget)
    up_depth = sum(float(a.get("size", 0)) for a in (up_book.get("asks") or []))
    down_depth = sum(float(a.get("size", 0)) for a in (down_book.get("asks") or []))
    depth_fraction = SIM_DEPTH_CAP_FRACTION
    if execution_safe:
        depth_fraction = min(depth_fraction, 1.0 / float(variant["minDepthMultiplier"]))
    depth_cap = min(up_depth, down_depth) * depth_fraction
    shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
    if shares <= 0:
        return reject("pair_no_common_depth", upAskDepth=up_depth, downAskDepth=down_depth)
    up_fill = simulate_buy_fill(up_book, shares)
    down_fill = simulate_buy_fill(down_book, shares)
    if (
        not up_fill or not down_fill
        or up_fill["notional"] < SIM_MIN_ORDER_NOTIONAL_USD
        or down_fill["notional"] < SIM_MIN_ORDER_NOTIONAL_USD
        # Polymarket 真正的下限是「股數」不是金額——查證過真實 API 回傳的
        # minOrderSize 是 5 股，不是 $5，跟實盤 polymarket_live_strategy.py 用同一個欄位對齊。
        or up_fill["shares"] < float(up_book.get("minOrderSize", 1) or 1)
        or down_fill["shares"] < float(down_book.get("minOrderSize", 1) or 1)
    ):
        return reject(
            "pair_incomplete_fill_or_minimum",
            targetShares=shares,
            upAskDepth=up_depth,
            downAskDepth=down_depth,
        )
    price_sum = up_fill["decisionPrice"] + down_fill["decisionPrice"]
    total_decision_cost = (
        up_fill["decisionNotional"] + up_fill["decisionFee"]
        + down_fill["decisionNotional"] + down_fill["decisionFee"]
    )
    net_per_share = (shares - total_decision_cost) / shares
    cash, _ = compute_cash_and_portfolio(variant_id)
    pair_details = {
        "pairDecisionSum": price_sum,
        "pairNetPerShare": net_per_share,
        "pairDecisionCost": total_decision_cost,
        "targetShares": shares,
    }
    if price_sum > variant["lockMaxSum"]:
        return reject("pair_price_sum_above_maximum", **pair_details)
    if net_per_share < SIM_MIN_NET_LOCK_PER_SHARE:
        return reject("pair_net_edge_below_minimum", **pair_details)
    if total_decision_cost > cash:
        return reject("pair_insufficient_cash", cashUsd=cash, **pair_details)
    if execution_safe:
        if not pair_depth_is_safe(
            up_book,
            down_book,
            up_fill["decisionPrice"],
            down_fill["decisionPrice"],
            shares,
            float(variant["minDepthMultiplier"]),
        ):
            return reject("pair_depth_multiplier_not_met", **pair_details)
        if not pair_candidate_is_stable(
            stability_key,
            slug,
            shares,
            up_fill["decisionPrice"],
            down_fill["decisionPrice"],
            float(variant["stabilitySeconds"]),
        ):
            record_window_diagnostic(variant_id, slug, "pair_stability_wait", **pair_details)
            return False
        clear_pair_candidate(stability_key)
    # 深度封頂後實際成交金額可能比原本算的目標預算小，記錄實際花費的金額，
    # 不要留著封頂前那個沒用到的數字。
    budget = up_fill["notional"] + up_fill["fee"] + down_fill["notional"] + down_fill["fee"]
    enter_position(variant_id, slug, "Up", up_fill, budget, None, None)
    hedge_position(variant_id, "Down", down_fill)
    return True


def _try_btc_15m_dump_then_hedge(
    variant_id: str,
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float | None,
) -> None:
    """15m 紙上策略：前三分鐘內追蹤 ask，前兩分鐘暴跌先進一腿，之後費後補腿。

    這不是原子套利：Leg 1 到 Leg 2 之間刻意保留方向曝險。沒有等到合格的 Leg 2
    就抱到市場結算，讓模擬如實反映策略最主要的尾部風險。
    """
    variant = AB_VARIANT_BY_ID[variant_id]
    st = ab_states[variant_id]
    stats = _dump_hedge_stats(st)
    lookback = float(variant["lookbackSeconds"])
    signals = _record_btc_15m_asks(slug, up_book, down_book, lookback)
    pos = st.get("position")

    if pos is None:
        window_seconds = 900.0
        entry_window = float(variant["entryWindowSeconds"])
        if remaining_seconds is None:
            record_window_diagnostic(variant_id, slug, "missing_remaining_seconds")
            return
        elapsed = window_seconds - float(remaining_seconds)
        if elapsed < 0 or elapsed > entry_window:
            record_window_diagnostic(
                variant_id, slug, "outside_dump_entry_window",
                remainingSeconds=remaining_seconds,
                elapsedSeconds=max(0.0, elapsed),
            )
            return

        candidates = [
            signal for signal in signals.values()
            if float(signal["dropPct"]) >= float(variant["minMovePct"])
        ]
        if not candidates:
            best_drop = max((float(x["dropPct"]) for x in signals.values()), default=None)
            record_window_diagnostic(
                variant_id, slug, "dump_below_threshold" if signals else "dump_history_warming",
                remainingSeconds=remaining_seconds,
                bestDumpPct=best_drop,
                dumpThresholdPct=float(variant["minMovePct"]),
            )
            return

        signal = max(candidates, key=lambda row: float(row["dropPct"]))
        side = str(signal["side"])
        book = up_book if side == "Up" else down_book
        signal_key = (
            f"{slug}|{side}|{float(signal['referenceAt']):.6f}|"
            f"{float(signal['currentAt']):.6f}|{float(signal['currentPrice']):.6f}"
        )
        if st.get("lastDumpSignalKey") != signal_key:
            stats["signalsDetected"] += 1
            st["lastDumpSignalKey"] = signal_key
        common = {
            "remainingSeconds": remaining_seconds,
            "selectedSide": side,
            "dumpPct": float(signal["dropPct"]),
            "dumpReferencePrice": float(signal["referencePrice"]),
            "dumpCurrentAsk": float(signal["currentPrice"]),
            "dumpLookbackSeconds": lookback,
        }
        if not _simulation_direction_book_is_fresh(variant["assetId"], side, book):
            stats["leg1Rejected"] += 1
            record_window_diagnostic(
                variant_id, slug, "dump_leg_book_not_fresh",
                dataGuardReason=_simulation_single_book_guard_reason(book), **common,
            )
            return

        shares = float(variant["targetShares"])
        min_order_size = float(book.get("minOrderSize", 1) or 1)
        fill = simulate_buy_fill(book, shares)
        if (
            not fill
            or shares < min_order_size
            or fill["decisionNotional"] < SIM_MIN_ORDER_NOTIONAL_USD
        ):
            stats["leg1Rejected"] += 1
            record_window_diagnostic(
                variant_id, slug, "dump_leg1_incomplete_fill_or_minimum",
                targetShares=shares, minOrderSize=min_order_size, **common,
            )
            return

        cash, _ = compute_cash_and_portfolio(variant_id)
        reserve = float(variant.get("minCashReserveUsd", 0.0))
        # 進 Leg 1 前先替未來 Leg 2 保留完整資金，不讓已有鎖利部位把可用現金吃光後，
        # 新週期只買得起第一腿卻永遠買不起對沖腿。
        max_pair_fee_per_share = 2 * SIM_TAKER_FEE_RATE * 0.25
        required_pair_cash = shares * (float(variant["lockMaxSum"]) + max_pair_fee_per_share)
        if required_pair_cash > max(0.0, cash - reserve) + 1e-9:
            stats["leg1Rejected"] += 1
            record_window_diagnostic(
                variant_id, slug, "dump_insufficient_reserved_pair_cash",
                cashUsd=cash, requiredPairCash=required_pair_cash, reserveUsd=reserve, **common,
            )
            return

        enter_position(
            variant_id, slug, side, fill, required_pair_cash, None, None
        )
        pos = st["position"]
        pos.update({
            "strategyMode": "dump_then_hedge",
            "signalSource": "polymarket_ws_best_ask",
            "signalDropPct": float(signal["dropPct"]),
            "signalReferencePrice": float(signal["referencePrice"]),
            "signalCurrentAsk": float(signal["currentPrice"]),
            "signalLookbackSeconds": lookback,
            "signalRemainingSeconds": float(remaining_seconds),
            "hedgeWaitStartedAt": time.time(),
        })
        stats["leg1Entries"] += 1
        record_window_diagnostic(
            variant_id, slug, "dump_leg1_entered",
            status="waiting_for_hedge", **common,
        )
        save_sim_state()
        log.info(
            f"[SIM:{variant_id}] 暴跌 Leg1 {side} drop={signal['dropPct']:.2f}% "
            f"${signal['referencePrice']:.3f}->${signal['currentPrice']:.3f} "
            f"VWAP=${fill['vwap']:.4f} shares={shares:.0f}"
        )
        return

    if pos.get("windowSlug") != slug or pos.get("hedged"):
        return
    if remaining_seconds is None or float(remaining_seconds) <= 0:
        record_window_diagnostic(variant_id, slug, "dump_window_closed_before_hedge")
        return

    other_side = "Down" if pos["side"] == "Up" else "Up"
    other_book = down_book if other_side == "Down" else up_book
    stats["hedgeChecks"] += 1
    if not _simulation_direction_book_is_fresh(variant["assetId"], other_side, other_book):
        record_window_diagnostic(
            variant_id, slug, "dump_hedge_book_not_fresh",
            selectedSide=other_side,
            dataGuardReason=_simulation_single_book_guard_reason(other_book),
        )
        return
    hedge_fill = simulate_buy_fill(other_book, float(pos["shares"]))
    if not hedge_fill:
        record_window_diagnostic(
            variant_id, slug, "dump_hedge_insufficient_depth",
            selectedSide=other_side, targetShares=pos["shares"],
        )
        return

    projected_cost = (
        _position_decision_cost(pos)
        + hedge_fill["decisionNotional"]
        + hedge_fill["decisionFee"]
    )
    price_sum = float(pos["entryDecisionPrice"]) + float(hedge_fill["decisionPrice"])
    net_per_share = (float(pos["shares"]) - projected_cost) / float(pos["shares"])
    cash, _ = compute_cash_and_portfolio(variant_id)
    reserve = float(variant.get("minCashReserveUsd", 0.0))
    hedge_cost = hedge_fill["decisionNotional"] + hedge_fill["decisionFee"]
    details = {
        "selectedSide": other_side,
        "pairDecisionSum": price_sum,
        "pairNetPerShare": net_per_share,
        "hedgeDecisionPrice": hedge_fill["decisionPrice"],
        "targetShares": pos["shares"],
    }
    if price_sum > float(variant["lockMaxSum"]):
        record_window_diagnostic(variant_id, slug, "dump_hedge_sum_above_maximum", **details)
        return
    if net_per_share < float(variant["minNetPerShare"]):
        record_window_diagnostic(variant_id, slug, "dump_hedge_net_below_minimum", **details)
        return
    if hedge_cost > max(0.0, cash - reserve) + 1e-9:
        record_window_diagnostic(
            variant_id, slug, "dump_hedge_insufficient_cash", cashUsd=cash, **details
        )
        return

    hedge_position(variant_id, other_side, hedge_fill)
    completed = st["position"]
    completed["hedgeWaitSeconds"] = max(
        0.0, time.time() - float(completed.get("hedgeWaitStartedAt", time.time()))
    )
    completed["cycleCompletedAt"] = time.time()
    st["pendingSettlements"].append(completed)
    st["position"] = None
    stats["completedCycles"] += 1
    # 同一個 3 秒暴跌訊號只能建立一組；完成後重新累積三秒歷史，才允許下一個週期。
    _btc_15m_ask_history.pop(slug, None)
    record_window_diagnostic(
        variant_id, slug, "dump_cycle_completed",
        status="entered_locked", hedgeWaitSeconds=completed["hedgeWaitSeconds"], **details,
    )
    save_sim_state()
    log.info(
        f"[SIM:{variant_id}] 暴跌策略完成補腿 {other_side} "
        f"wait={completed['hedgeWaitSeconds']:.2f}s 淨鎖利=${completed['lockedPnl']:+.2f}"
    )


def estimate_btc_15m_direction_signal(
    signal: dict,
    remaining_seconds: float,
) -> dict:
    """用 Chainlink 60 秒 TWAP 自身的近期波動，估算目前方向維持到 15m 結束的機率。

    使用不重疊的 10 秒 bucket，避免把每秒高度自相關的 TWAP 更新誤當成大量獨立樣本。
    歷史不足時採固定波動下限，且再乘安全係數，刻意避免過度自信。
    """
    current = float(signal["current"])
    opening = float(signal["opening"])
    observed_ms = int(signal["observedAt"])
    cutoff_ms = observed_ms - int(BTC_15M_DIRECTION_VOL_LOOKBACK_SECONDS * 1000)
    bucket_ms = int(BTC_15M_DIRECTION_VOL_BUCKET_SECONDS * 1000)
    buckets: dict[int, tuple[int, float]] = {}
    for ts, value in _chainlink_twap_history:
        if ts < cutoff_ms or ts > observed_ms or value <= 0:
            continue
        bucket = int(ts) // bucket_ms
        previous = buckets.get(bucket)
        if previous is None or ts > previous[0]:
            buckets[bucket] = (int(ts), float(value))
    points = [row for _, row in sorted(buckets.items())]
    returns_pct = [
        math.log(cur[1] / prev[1]) * 100
        for prev, cur in zip(points, points[1:])
        if prev[1] > 0 and cur[1] > 0
    ]
    observed_sigma_pct = pstdev(returns_pct) if len(returns_pct) >= 5 else 0.0
    bucket_sigma_pct = max(BTC_15M_DIRECTION_MIN_SIGMA_PCT, observed_sigma_pct)
    horizon_buckets = max(1.0, float(remaining_seconds) / BTC_15M_DIRECTION_VOL_BUCKET_SECONDS)
    projected_sigma_pct = (
        bucket_sigma_pct
        * math.sqrt(horizon_buckets)
        * BTC_15M_DIRECTION_VOL_SAFETY_MULTIPLIER
    )
    delta_pct = (current - opening) / opening * 100
    z_score = abs(delta_pct) / projected_sigma_pct if projected_sigma_pct > 0 else 0.0
    probability = max(0.50, min(0.995, NormalDist().cdf(z_score)))
    return {
        "deltaPct": delta_pct,
        "observedSigmaPct": observed_sigma_pct,
        "projectedSigmaPct": projected_sigma_pct,
        "zScore": z_score,
        "probability": probability,
        "sampleCount": len(returns_pct),
    }


def _selected_leg_market_probability(book: dict) -> tuple[float, float] | None:
    """Return the selected token midpoint and spread without requiring the unused opposite leg."""
    bids, asks = book.get("bids") or [], book.get("asks") or []
    if not bids or not asks:
        return None
    bid, ask = float(bids[0]["price"]), float(asks[0]["price"])
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    return (bid + ask) / 2, ask - bid


def _direction_fill_with_budget(variant_id: str, book: dict) -> tuple[dict, float] | None:
    """以真正的單腿美元風險預算反推最大整數股數，不沿用兩腿成本換算公式。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    cash, _ = compute_cash_and_portfolio(variant_id)
    reserve = float(variant.get("minCashReserveUsd", SIM_MIN_CASH_RESERVE_USD))
    available = max(0.0, cash - reserve)
    budget = min(
        float(variant.get("maxDirectionalBudgetUsd", SIM_MAX_PAIR_BUDGET_USD)),
        available * float(variant.get("stakePct", shared_config["stakePct"])) / 100,
    )
    asks = book.get("asks") or []
    if budget < SIM_MIN_ORDER_NOTIONAL_USD or not asks:
        return None
    best_ask = float(asks[0].get("price", 0))
    if best_ask <= 0:
        return None
    depth_multiplier = max(1.0, float(variant.get("minDepthMultiplier", 1.0)))
    visible_depth = sum(max(0.0, float(level.get("size", 0))) for level in asks)
    high = int(min(visible_depth / depth_multiplier, budget / best_ask))
    low, best_fill = 1, None
    while low <= high:
        mid = (low + high) // 2
        fill = simulate_buy_fill(book, float(mid))
        cost = float("inf") if not fill else fill["decisionNotional"] + fill["decisionFee"]
        if fill and cost <= budget + 1e-9:
            best_fill = fill
            low = mid + 1
        else:
            high = mid - 1
    return (best_fill, budget) if best_fill else None


def _try_btc_15m_adaptive_direction_entry(
    variant_id: str,
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float,
) -> None:
    """BTC 15m 紙上方向策略：結算同源、波動自適應、EV、深度與穩定性一起驗證。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    stability_key = f"sim-direction:{variant_id}"

    def reject(reason: str, **details) -> None:
        record_window_diagnostic(variant_id, slug, reason, remainingSeconds=remaining_seconds, **details)
        clear_direction_candidate(stability_key)

    if not (
        BTC_15M_DIRECTION_MIN_REMAINING
        <= remaining_seconds
        <= BTC_15M_DIRECTION_MAX_REMAINING
    ):
        reject("outside_entry_window")
        return
    signal = get_chainlink_twap_signal(variant["assetId"])
    if not signal:
        reject("missing_chainlink_signal")
        return
    metrics = estimate_btc_15m_direction_signal(signal, remaining_seconds)
    if (
        abs(metrics["deltaPct"]) < BTC_15M_DIRECTION_MIN_DELTA_PCT
        or metrics["probability"] < BTC_15M_DIRECTION_MIN_PROBABILITY
    ):
        reason = (
            "delta_below_minimum"
            if abs(metrics["deltaPct"]) < BTC_15M_DIRECTION_MIN_DELTA_PCT
            else "model_probability_below_minimum"
        )
        reject(reason, signalDeltaPct=metrics["deltaPct"], modelProbability=metrics["probability"])
        return
    side, book = ("Up", up_book) if metrics["deltaPct"] > 0 else ("Down", down_book)
    if not _simulation_direction_book_is_fresh(variant["assetId"], side, book):
        reject(
            "selected_book_not_fresh",
            signalDeltaPct=metrics["deltaPct"],
            modelProbability=metrics["probability"],
            selectedSide=side,
            dataGuardReason=_simulation_single_book_guard_reason(book),
        )
        return
    market = _selected_leg_market_probability(book)
    if not market:
        reject(
            "missing_selected_market",
            signalDeltaPct=metrics["deltaPct"],
            modelProbability=metrics["probability"],
            selectedSide=side,
        )
        return
    market_probability, spread = market
    if (
        market_probability < BTC_15M_DIRECTION_MIN_MARKET_PROBABILITY
        or spread > BTC_15M_DIRECTION_MAX_SPREAD
    ):
        reason = (
            "market_probability_below_minimum"
            if market_probability < BTC_15M_DIRECTION_MIN_MARKET_PROBABILITY
            else "spread_above_maximum"
        )
        reject(
            reason,
            signalDeltaPct=metrics["deltaPct"],
            modelProbability=metrics["probability"],
            marketProbability=market_probability,
            spread=spread,
            selectedSide=side,
        )
        return
    planned = _direction_fill_with_budget(variant_id, book)
    if not planned:
        reject("insufficient_budget_or_ask_depth", selectedSide=side)
        return
    fill, budget = planned
    min_order_size = float(book.get("minOrderSize", 1) or 1)
    if fill["shares"] < min_order_size or fill["decisionNotional"] < SIM_MIN_ORDER_NOTIONAL_USD:
        reject("below_minimum_order", selectedSide=side, filledShares=fill["shares"], minOrderSize=min_order_size)
        return
    if fill["decisionPrice"] > float(variant["lateDirectionMaxPrice"]):
        reject("price_above_maximum", selectedSide=side, decisionPrice=fill["decisionPrice"])
        return
    required_depth = fill["shares"] * float(variant.get("minDepthMultiplier", 1.0))
    if executable_ask_depth(book, fill["decisionPrice"]) + 1e-9 < required_depth:
        reject("depth_multiplier_not_met", selectedSide=side, decisionPrice=fill["decisionPrice"])
        return
    decision_cost_per_share = (fill["decisionNotional"] + fill["decisionFee"]) / fill["shares"]
    entry_edge = metrics["probability"] - decision_cost_per_share
    if entry_edge < BTC_15M_DIRECTION_MIN_EDGE_PER_SHARE:
        reject(
            "edge_below_minimum",
            selectedSide=side,
            signalDeltaPct=metrics["deltaPct"],
            modelProbability=metrics["probability"],
            marketProbability=market_probability,
            decisionPrice=fill["decisionPrice"],
            entryEdge=entry_edge,
        )
        return
    if not direction_candidate_is_stable(
        stability_key,
        slug,
        side,
        float(variant.get("stabilitySeconds", 0.0)),
    ):
        record_window_diagnostic(
            variant_id,
            slug,
            "direction_stability_wait",
            remainingSeconds=remaining_seconds,
            selectedSide=side,
            signalDeltaPct=metrics["deltaPct"],
            modelProbability=metrics["probability"],
            marketProbability=market_probability,
            decisionPrice=fill["decisionPrice"],
            entryEdge=entry_edge,
        )
        return
    clear_direction_candidate(stability_key)
    enter_position(
        variant_id,
        slug,
        side,
        fill,
        min(budget, fill["notional"] + fill["fee"]),
        metrics["probability"],
        entry_edge,
    )
    pos = ab_states[variant_id]["position"]
    pos.update({
        "signalDeltaPct": metrics["deltaPct"],
        "signalZScore": metrics["zScore"],
        "signalProjectedSigmaPct": metrics["projectedSigmaPct"],
        "marketProbability": market_probability,
        "signalRemainingSeconds": remaining_seconds,
    })
    save_sim_state()
    log.info(
        f"[SIM:{variant_id}] 15m 自適應方向 {side} Δ={metrics['deltaPct']:+.3f}% "
        f"z={metrics['zScore']:.2f} p={metrics['probability']:.1%} market={market_probability:.1%} "
        f"edge={entry_edge:+.3f} 剩餘={remaining_seconds:.1f}s"
    )


def _try_late_direction_entry(
    variant_id: str, slug: str, up_book: dict, down_book: dict, remaining_seconds: float
) -> None:
    """晚進場方向性；BTC 5m 實驗組用 Binance，較長窗口仍用 Chainlink。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    if variant.get("directionProfile") == "btc-15m-adaptive":
        _try_btc_15m_adaptive_direction_entry(
            variant_id, slug, up_book, down_book, remaining_seconds
        )
        return
    if remaining_seconds > LATE_DIRECTION_WINDOW_SECONDS or remaining_seconds < LATE_DIRECTION_MIN_ENTRY_REMAINING:
        record_window_diagnostic(variant_id, slug, "outside_entry_window", remainingSeconds=remaining_seconds)
        return
    if variant.get("directionSignalSource") == "binance_window":
        ms = markets_state[variant["assetId"]]
        opening, current = ms.get("windowOpenSpotPrice"), ms.get("spotPrice")
        if not opening or not current or opening <= 0:
            record_window_diagnostic(variant_id, slug, "missing_binance_signal", remainingSeconds=remaining_seconds)
            return
        delta_pct = (current - opening) / opening * 100
        signal_source = "binance_futures_window"
    else:
        signal = get_chainlink_twap_signal(variant["assetId"])
        if not signal:
            record_window_diagnostic(variant_id, slug, "missing_chainlink_signal", remainingSeconds=remaining_seconds)
            return
        delta_pct = (signal["current"] - signal["opening"]) / signal["opening"] * 100
        signal_source = "chainlink_twap_60s"
    if abs(delta_pct) < LATE_DIRECTION_MIN_DELTA_PCT:
        record_window_diagnostic(
            variant_id, slug, "delta_below_minimum",
            remainingSeconds=remaining_seconds, signalSource=signal_source, signalDeltaPct=delta_pct,
        )
        return
    side, book = ("Up", up_book) if delta_pct > 0 else ("Down", down_book)
    asks = book.get("asks") or []
    selected_ask = float(asks[0]["price"]) if asks else None
    selected_depth = sum(float(level.get("size", 0)) for level in asks)
    common = {
        "remainingSeconds": remaining_seconds,
        "signalSource": signal_source,
        "signalDeltaPct": delta_pct,
        "selectedSide": side,
        "selectedAsk": selected_ask,
        "selectedAskDepth": selected_depth,
    }
    if not _simulation_direction_book_is_fresh(variant["assetId"], side, book):
        record_window_diagnostic(
            variant_id,
            slug,
            "selected_book_not_fresh",
            dataGuardReason=_simulation_single_book_guard_reason(book),
            **common,
        )
        return
    shares, budget = _target_order_size(variant_id)
    if shares <= 0 or budget < SIM_MIN_ORDER_NOTIONAL_USD:
        record_window_diagnostic(variant_id, slug, "insufficient_budget", targetShares=shares, budgetUsd=budget, **common)
        return
    fill = simulate_buy_fill(book, shares)
    if not fill or fill["decisionNotional"] < SIM_MIN_ORDER_NOTIONAL_USD:
        record_window_diagnostic(variant_id, slug, "insufficient_ask_depth", targetShares=shares, budgetUsd=budget, **common)
        return
    # 真正的下限是「股數」不是金額：查證過真實 API 回傳的 minOrderSize 是 5 股，不是 $5。
    if fill["shares"] < float(book.get("minOrderSize", 1) or 1):
        record_window_diagnostic(
            variant_id, slug, "below_minimum_shares",
            targetShares=shares, filledShares=fill["shares"], minOrderSize=book.get("minOrderSize"), **common,
        )
        return
    if fill["decisionPrice"] > variant["lateDirectionMaxPrice"]:
        record_window_diagnostic(
            variant_id, slug, "price_above_maximum",
            decisionPrice=fill["decisionPrice"], maxPrice=variant["lateDirectionMaxPrice"], **common,
        )
        return
    enter_position(variant_id, slug, side, fill, budget, None, None)
    ab_states[variant_id]["position"]["signalSource"] = signal_source
    ab_states[variant_id]["position"]["signalDeltaPct"] = delta_pct
    save_sim_state()
    log.info(
        f"[SIM:{variant_id}] 晚進場方向性 {side} source={signal_source} Δ={delta_pct:+.3f}% "
        f"剩餘={remaining_seconds:.1f}s VWAP=${fill['vwap']:.4f}"
    )


def _close_directional_position(variant_id: str, fill: dict, reason: str) -> None:
    st = ab_states[variant_id]
    pos = st["position"]
    net_proceeds = fill["notional"] - fill["fee"]
    pnl = net_proceeds - _position_paid_cost(pos)
    pos["exitPrice"] = fill["vwap"]
    pos["exitFee"] = fill["fee"]
    pos["exitDecisionPrice"] = fill["decisionPrice"]
    pos["exitDecisionFee"] = fill["decisionFee"]
    pos["exitReason"] = reason
    st["position"] = None
    record_trade(variant_id, pos, pnl, "EarlyExit")
    log.info(f"[SIM:{variant_id}] 提早退出 {pos['side']} VWAP=${fill['vwap']:.4f} PnL=${pnl:+.2f} reason={reason}")


def _maker_stats(st: dict) -> dict:
    defaults = {
        "quotesPlaced": 0,
        "fills": 0,
        "pairedFills": 0,
        "singleLegSettlements": 0,
        "queueVolumeConsumed": 0.0,
        "rescueAttempts": 0,
        "rescueHedges": 0,
        "rescueUnwinds": 0,
        "rescueFailures": 0,
    }
    stats = st.setdefault("makerStats", {})
    for key, value in defaults.items():
        stats.setdefault(key, value)
    return stats


def _maker_quote_candidate(book: dict, price_cap: float | None = None) -> tuple[float, float] | None:
    """回傳保證不 crossing 的 maker bid 與掛單時前方隊列量。"""
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    if not bids or not asks:
        return None
    tick = max(0.001, float(book.get("tickSize", 0.01) or 0.01))
    best_bid = max(float(level["price"]) for level in bids)
    best_ask = min(float(level["price"]) for level in asks)
    if best_ask - best_bid >= 2 * tick - 1e-9:
        candidate = best_bid + tick
    else:
        candidate = best_bid
    candidate = min(candidate, best_ask - tick)
    if price_cap is not None:
        candidate = min(candidate, float(price_cap))
    tick_d = Decimal(str(tick))
    candidate = float((Decimal(str(candidate)) / tick_d).to_integral_value(rounding=ROUND_DOWN) * tick_d)
    if candidate < tick or candidate >= best_ask - 1e-9:
        return None
    queue_ahead = sum(
        float(level.get("size", 0))
        for level in bids
        if abs(float(level["price"]) - candidate) < tick / 10
    )
    return candidate, queue_ahead


def _maker_set_quote(st: dict, slug: str, side: str, candidate: tuple[float, float], shares: float) -> bool:
    quotes = st.setdefault("makerQuotes", {"windowSlug": slug, "Up": None, "Down": None})
    now = time.time()
    price, queue_ahead = candidate
    old = quotes.get(side)
    if old and old.get("windowSlug") == slug:
        same_order = abs(float(old.get("price", 0)) - price) < 1e-9 and abs(float(old.get("shares", 0)) - shares) < 1e-9
        if same_order or now - float(old.get("placedAt", 0)) < MM_REQUOTE_SECONDS:
            return False
    quotes[side] = {
        "windowSlug": slug,
        "side": side,
        "price": price,
        "shares": shares,
        "queueAhead": queue_ahead,
        "fillProgress": 0.0,
        "placedAt": now,
    }
    _maker_stats(st)["quotesPlaced"] += 1
    log.info(
        f"[SIM:eth-mm] maker 掛價 {side} BUY ${price:.3f} x {shares:.2f} "
        f"queueAhead={queue_ahead:.2f}"
    )
    return True


def _rescue_maker_inventory(variant_id: str, up_book: dict, down_book: dict) -> None:
    """首腿久未配對時，先以 taker 鎖正收益；否則立即賣回市場，限制方向曝險。"""
    st = ab_states[variant_id]
    pos = st.get("position")
    if not pos or pos.get("hedged"):
        return

    now = time.time()
    last_attempt = float(pos.get("makerRescueAttemptAt", 0) or 0)
    if now - last_attempt < MM_REQUOTE_SECONDS:
        return
    pos["makerRescueAttemptAt"] = now
    stats = _maker_stats(st)
    stats["rescueAttempts"] += 1

    quotes = st.get("makerQuotes")
    if isinstance(quotes, dict):
        quotes["Up"] = quotes["Down"] = None

    other_side = "Down" if pos["side"] == "Up" else "Up"
    other_book = down_book if other_side == "Down" else up_book
    hedge_fill = simulate_buy_fill(other_book, float(pos["shares"]))
    if hedge_fill:
        projected_cost = (
            _position_decision_cost(pos)
            + hedge_fill["decisionNotional"]
            + hedge_fill["decisionFee"]
        )
        projected_net_per_share = (float(pos["shares"]) - projected_cost) / float(pos["shares"])
        cash, _ = compute_cash_and_portfolio(variant_id)
        hedge_cash = hedge_fill["decisionNotional"] + hedge_fill["decisionFee"]
        if projected_net_per_share >= SIM_MIN_NET_LOCK_PER_SHARE and hedge_cash <= cash + 1e-9:
            hedge_position(variant_id, other_side, hedge_fill)
            pos["makerRescueAction"] = "taker_hedge"
            stats["pairedFills"] += 1
            stats["rescueHedges"] += 1
            log.info(
                f"[SIM:{variant_id}] maker 15秒救援：taker 配對 {other_side} "
                f"VWAP=${hedge_fill['vwap']:.4f} 淨鎖利=${pos['lockedPnl']:+.2f}"
            )
            save_sim_state()
            return

    held_book = up_book if pos["side"] == "Up" else down_book
    exit_fill = simulate_sell_fill(held_book, float(pos["shares"]))
    if exit_fill:
        stats["rescueUnwinds"] += 1
        _close_directional_position(variant_id, exit_fill, "maker_inventory_timeout")
        log.info(f"[SIM:{variant_id}] maker 15秒救援：無正收益配對，已 taker 平倉")
        save_sim_state()
        return

    stats["rescueFailures"] += 1
    log.warning(f"[SIM:{variant_id}] maker 15秒救援失敗：持有腿沒有足夠 bid 深度，稍後重試")
    save_sim_state()


def update_market_maker_quotes(
    variant_id: str,
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float | None,
) -> None:
    """依即時 book 維護 ETH maker 紙上掛價；不會建立、簽署或送出真實訂單。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    if not variant.get("marketMakerOnly"):
        return
    st = ab_states[variant_id]
    changed = False
    quotes = st.get("makerQuotes")
    if not isinstance(quotes, dict) or quotes.get("windowSlug") != slug:
        st["makerQuotes"] = {"windowSlug": slug, "Up": None, "Down": None}
        quotes = st["makerQuotes"]
        changed = True

    pos = st.get("position")
    if pos and pos.get("windowSlug") == slug and not pos.get("hedged"):
        inventory_age = time.time() - float(pos.get("entryTime", time.time()))
        if inventory_age >= MM_INVENTORY_RESCUE_SECONDS:
            _rescue_maker_inventory(variant_id, up_book, down_book)
            return

    if remaining_seconds is None or remaining_seconds <= MM_STOP_QUOTING_SECONDS:
        if quotes.get("Up") is not None or quotes.get("Down") is not None:
            quotes["Up"] = quotes["Down"] = None
            changed = True
        if changed:
            save_sim_state()
        return

    if pos and pos.get("windowSlug") != slug:
        return
    shares, _ = _target_order_size(variant_id)
    if pos:
        shares = float(pos["shares"])
    shares = float(Decimal(str(shares)).to_integral_value(rounding=ROUND_DOWN))
    if shares <= 0:
        return

    books = {"Up": up_book, "Down": down_book}
    wanted = ["Up", "Down"] if pos is None else ["Down" if pos["side"] == "Up" else "Up"]
    candidates: dict[str, tuple[float, float]] = {}
    for side in wanted:
        price_cap = MM_FIRST_LEG_MAX_PRICE
        if pos:
            price_cap = 1.0 - float(pos.get("entryPrice", 0)) - MM_MIN_NET_PAIR_EDGE
        candidate = _maker_quote_candidate(books[side], price_cap)
        min_size = float(books[side].get("minOrderSize", 1) or 1)
        if candidate is None or shares < min_size or candidate[0] * shares < SIM_MIN_ORDER_NOTIONAL_USD:
            if quotes.get(side) is not None:
                quotes[side] = None
                changed = True
            continue
        candidates[side] = candidate

    if pos is None:
        if set(candidates) != {"Up", "Down"} or sum(x[0] for x in candidates.values()) > MM_MAX_PAIR_COST + 1e-9:
            if quotes.get("Up") is not None or quotes.get("Down") is not None:
                quotes["Up"] = quotes["Down"] = None
                changed = True
            if changed:
                save_sim_state()
            return

    cash, _ = compute_cash_and_portfolio(variant_id)
    required_cash = shares * sum(candidate[0] for candidate in candidates.values())
    if required_cash > cash + 1e-9:
        return
    for side, candidate in candidates.items():
        changed = _maker_set_quote(st, slug, side, candidate, shares) or changed
    for side in ("Up", "Down"):
        if side not in wanted and quotes.get(side) is not None:
            quotes[side] = None
            changed = True
    if changed:
        save_sim_state()


def process_market_maker_trade(token_id: str, trade: dict) -> None:
    """用真實 last_trade_price 消耗 maker queue；完整排到才記一筆紙上成交。"""
    for variant in MARKET_MAKER_VARIANTS:
        ms = markets_state[variant["assetId"]]
        side = "Up" if token_id == ms.get("upTokenId") else ("Down" if token_id == ms.get("downTokenId") else None)
        if side is None:
            continue
        st = ab_states[variant["id"]]
        quotes = st.get("makerQuotes") or {}
        quote = quotes.get(side)
        if not quote:
            continue
        trade_ts = float(trade.get("ts") or time.time())
        if trade_ts + 0.5 < float(quote.get("placedAt", 0)):
            continue
        if str(trade.get("side", "")).upper() != "SELL":
            # Maker BUY 只會被主動 SELL 吃到；BUY 成交發生在 ask，不能拿來消耗 bid queue。
            continue
        trade_price = float(trade["price"])
        quote_price = float(quote["price"])
        if trade_price > quote_price + 1e-9:
            continue
        trade_size = max(0.0, float(trade.get("size", 0)))
        queue_before = max(0.0, float(quote.get("queueAhead", 0)))
        if trade_price < quote_price - 1e-9:
            eligible = queue_before + float(quote["shares"])
        else:
            eligible = trade_size
        queue_used = min(queue_before, eligible)
        quote["queueAhead"] = queue_before - queue_used
        _maker_stats(st)["queueVolumeConsumed"] += queue_used
        quote["fillProgress"] = float(quote.get("fillProgress", 0)) + max(0.0, eligible - queue_used)
        if quote["fillProgress"] + 1e-9 < float(quote["shares"]):
            continue

        shares = float(quote["shares"])
        fill = {
            "vwap": quote_price,
            "notional": quote_price * shares,
            "fee": 0.0,
            "worstPrice": quote_price,
            "decisionPrice": quote_price,
            "decisionNotional": quote_price * shares,
            "decisionFee": 0.0,
            "shares": shares,
        }
        quotes[side] = None
        stats = _maker_stats(st)
        stats["fills"] += 1
        pos = st.get("position")
        if pos is None:
            enter_position(variant["id"], quote["windowSlug"], side, fill, fill["notional"], None, None)
            st["position"]["maker"] = True
            log.info(f"[SIM:{variant['id']}] maker 首腿成交 {side} ${quote_price:.3f} x {shares:.2f}")
        elif pos.get("windowSlug") == quote.get("windowSlug") and pos.get("side") != side:
            hedge_position(variant["id"], side, fill)
            st["position"]["maker"] = True
            stats["pairedFills"] += 1
            quotes["Up"] = quotes["Down"] = None
            log.info(f"[SIM:{variant['id']}] maker 兩腿完成，鎖定 PnL=${st['position']['lockedPnl']:+.2f}")
        save_sim_state()
        return


def simulate_trading(
    variant_id: str,
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float | None,
    fair: dict | None,
    allow_early_exit: bool = True,
    evaluation_source: str = "direct",
) -> None:
    if variant_id not in decision_diag.TARGET_VARIANTS:
        _simulate_trading_impl(
            variant_id, slug, up_book, down_book, remaining_seconds, fair, allow_early_exit
        )
        return
    variant = AB_VARIANT_BY_ID[variant_id]
    settings = {k: variant.get(k) for k in (
        "lockMaxSum", "lateDirectionMaxPrice", "directionSignalSource",
        "minDepthMultiplier", "stabilitySeconds", "lookbackSeconds",
        "minMovePct", "entryWindowSeconds", "targetShares", "minNetPerShare")}
    settings["stakePct"] = variant.get("stakePct", shared_config["stakePct"])
    with decision_evaluation("SIM", variant_id, slug, evaluation_source,
                             up_book, down_book, remaining_seconds, settings):
        _simulate_trading_impl(variant_id, slug, up_book, down_book,
                               remaining_seconds, fair, allow_early_exit)


def decision_evaluation(stream, variant_id, slug, source, up, down, remaining, settings):
    ms = markets_state[AB_VARIANT_BY_ID[variant_id]["assetId"]]
    signal = {k: ms.get(k) for k in ("chainlinkTwapPrice", "chainlinkTwapObservedAt",
        "windowOpenChainlinkTwapPrice", "windowOpenChainlinkTwapObservedAt",
        "windowOpenChainlinkTwapSlug", "spotPrice", "windowOpenSpotPrice")}
    token_ids = (ms.get("upTokenId"), ms.get("downTokenId"))
    return decision_diag.evaluation(stream, variant_id, slug, source, up, down,
        remaining, settings, signal,
        lambda: tuple(_ws_get_book(tid) if tid else None for tid in token_ids))



def _rotation_stats(st: dict) -> dict:
    defaults = {
        "windowsEntered": 0,
        "fills": 0,
        "pairEvents": 0,
        "pairedShares": 0.0,
        "unpairedSettlements": 0,
        "maxResidualShares": 0.0,
        "lastActionAt": 0.0,
    }
    stats = st.setdefault("rotationStats", {})
    for key, value in defaults.items():
        stats.setdefault(key, value)
    return stats


def _rotation_lot_slice(lots: list, start_shares: float, shares: float) -> dict | None:
    """Return paid and decision cost for a FIFO range of inventory lots."""
    skip = max(0.0, float(start_shares))
    remaining = max(0.0, float(shares))
    totals = {"notional": 0.0, "fee": 0.0, "decisionNotional": 0.0, "decisionFee": 0.0}
    for lot in lots:
        lot_shares = float(lot.get("shares", 0))
        if lot_shares <= 0:
            continue
        if skip >= lot_shares - 1e-9:
            skip -= lot_shares
            continue
        available = lot_shares - skip
        take = min(remaining, available)
        ratio = take / lot_shares
        for key in totals:
            totals[key] += float(lot.get(key, 0)) * ratio
        remaining -= take
        skip = 0.0
        if remaining <= 1e-9:
            return totals
    return None


def _rotation_metrics(pos: dict) -> dict:
    up_shares = sum(float(x.get("shares", 0)) for x in pos.get("upLots", []))
    down_shares = sum(float(x.get("shares", 0)) for x in pos.get("downLots", []))
    paired = min(up_shares, down_shares)
    up_pair = _rotation_lot_slice(pos.get("upLots", []), 0.0, paired) if paired else None
    down_pair = _rotation_lot_slice(pos.get("downLots", []), 0.0, paired) if paired else None
    paid_pair_cost = sum((x or {}).get("notional", 0) + (x or {}).get("fee", 0) for x in (up_pair, down_pair))
    decision_pair_cost = sum(
        (x or {}).get("decisionNotional", 0) + (x or {}).get("decisionFee", 0)
        for x in (up_pair, down_pair)
    )
    residual = abs(up_shares - down_shares)
    residual_side = "Up" if up_shares > down_shares else ("Down" if down_shares > up_shares else None)
    return {
        "upShares": up_shares,
        "downShares": down_shares,
        "pairedShares": paired,
        "residualShares": residual,
        "residualSide": residual_side,
        "lockedPnl": paired - paid_pair_cost,
        "decisionLockedPnl": paired - decision_pair_cost,
        "paidPairCost": paid_pair_cost,
        "decisionPairCost": decision_pair_cost,
    }


def _refresh_rotation_position(pos: dict) -> dict:
    metrics = _rotation_metrics(pos)
    pos.update(metrics)
    pos["upNotional"] = sum(float(x.get("notional", 0)) for x in pos.get("upLots", []))
    pos["downNotional"] = sum(float(x.get("notional", 0)) for x in pos.get("downLots", []))
    pos["upFee"] = sum(float(x.get("fee", 0)) for x in pos.get("upLots", []))
    pos["downFee"] = sum(float(x.get("fee", 0)) for x in pos.get("downLots", []))
    pos["upDecisionNotional"] = sum(float(x.get("decisionNotional", 0)) for x in pos.get("upLots", []))
    pos["downDecisionNotional"] = sum(float(x.get("decisionNotional", 0)) for x in pos.get("downLots", []))
    pos["upDecisionFee"] = sum(float(x.get("decisionFee", 0)) for x in pos.get("upLots", []))
    pos["downDecisionFee"] = sum(float(x.get("decisionFee", 0)) for x in pos.get("downLots", []))
    pos["upAvgPrice"] = pos["upNotional"] / metrics["upShares"] if metrics["upShares"] else None
    pos["downAvgPrice"] = pos["downNotional"] / metrics["downShares"] if metrics["downShares"] else None
    pos["shares"] = max(metrics["upShares"], metrics["downShares"])
    pos["stakeUsd"] = _position_paid_cost(pos)
    pos["hedged"] = metrics["pairedShares"] > 0 and metrics["residualShares"] <= 1e-9
    return metrics


def _rotation_add_fill(
    variant_id: str,
    slug: str,
    side: str,
    fill: dict,
    fair_probability: float,
    edge: float,
) -> None:
    st = ab_states[variant_id]
    stats = _rotation_stats(st)
    pos = st.get("position")
    first_fill = pos is None
    if first_fill:
        pos = {
            "windowSlug": slug,
            "strategyMode": "inventory_rotation",
            "side": side,
            "entryPrice": float(fill["vwap"]),
            "entryDecisionPrice": float(fill["decisionPrice"]),
            "entryNotional": float(fill["notional"]),
            "entryFee": float(fill["fee"]),
            "entryTime": time.time(),
            "entryEdge": edge,
            "fairProbability": fair_probability,
            "upLots": [],
            "downLots": [],
            "fillCount": 0,
            "hedged": False,
            "hedgeSide": None,
            "hedgePrice": None,
            "hedgeFee": 0.0,
            "exitFee": 0.0,
        }
        st["position"] = pos
        stats["windowsEntered"] += 1
    paired_before = float(pos.get("pairedShares", 0))
    lot = {
        "shares": float(fill["shares"]),
        "vwap": float(fill["vwap"]),
        "notional": float(fill["notional"]),
        "fee": float(fill["fee"]),
        "decisionPrice": float(fill["decisionPrice"]),
        "decisionNotional": float(fill["decisionNotional"]),
        "decisionFee": float(fill["decisionFee"]),
        "filledAt": time.time(),
        "fairProbability": fair_probability,
        "edge": edge,
    }
    pos[("upLots" if side == "Up" else "downLots")].append(lot)
    pos["fillCount"] = int(pos.get("fillCount", 0)) + 1
    pos["lastActionAt"] = time.time()
    metrics = _refresh_rotation_position(pos)
    stats["fills"] += 1
    stats["lastActionAt"] = pos["lastActionAt"]
    stats["maxResidualShares"] = max(float(stats.get("maxResidualShares", 0)), metrics["residualShares"])
    paired_delta = metrics["pairedShares"] - paired_before
    if paired_delta > 1e-9:
        stats["pairEvents"] += 1
        stats["pairedShares"] += paired_delta
    record_window_diagnostic(
        variant_id,
        slug,
        "rotation_pair_completed" if paired_delta > 0 else "rotation_accumulated",
        status="entered",
        side=side,
        fillShares=fill["shares"],
        fillDecisionPrice=fill["decisionPrice"],
        edge=edge,
        **metrics,
    )
    save_sim_state()
    log.info(
        f"[SIM:{variant_id}] rotation BUY {side} {fill['shares']:.0f} @ {fill['vwap']:.4f}; "
        f"paired={metrics['pairedShares']:.0f} residual={metrics['residualSide']} "
        f"{metrics['residualShares']:.0f} locked=${metrics['lockedPnl']:+.2f}"
    )


def _try_inventory_rotation(
    variant_id: str,
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float | None,
    fair: dict | None,
) -> None:
    variant = AB_VARIANT_BY_ID[variant_id]
    st = ab_states[variant_id]
    stats = _rotation_stats(st)
    if remaining_seconds is None or remaining_seconds <= 0:
        return
    if not _simulation_books_are_coherent(variant["assetId"], up_book, down_book):
        record_window_diagnostic(variant_id, slug, "rotation_books_not_coherent")
        return
    if not fair:
        record_window_diagnostic(variant_id, slug, "rotation_missing_fair_value")
        return
    cooldown = float(variant.get("actionCooldownSeconds", ROTATION_ACTION_COOLDOWN))
    if time.time() - float(stats.get("lastActionAt", 0)) < cooldown:
        record_window_diagnostic(variant_id, slug, "rotation_action_cooldown")
        return

    shares = float(variant.get("sliceShares", ROTATION_SLICE_SHARES))
    books = {"Up": up_book, "Down": down_book}
    fills = {side: simulate_buy_fill(book, shares) for side, book in books.items()}
    for side, fill in list(fills.items()):
        min_size = float(books[side].get("minOrderSize", 1) or 1)
        if (
            fill is None
            or fill["shares"] + 1e-9 < min_size
            or fill["decisionNotional"] < SIM_MIN_ORDER_NOTIONAL_USD
        ):
            fills[side] = None

    pos = st.get("position")
    if pos and (pos.get("windowSlug") != slug or pos.get("strategyMode") != "inventory_rotation"):
        return
    metrics = _rotation_metrics(pos) if pos else {
        "upShares": 0.0, "downShares": 0.0, "pairedShares": 0.0,
        "residualShares": 0.0, "residualSide": None,
    }

    selected_side = None
    selected_fill = None
    selected_edge = None
    # First priority is reducing an existing residual, but only when the new
    # matched slice remains profitable after conservative price and fee costs.
    if metrics["residualSide"]:
        selected_side = "Down" if metrics["residualSide"] == "Up" else "Up"
        selected_fill = fills.get(selected_side)
        pair_shares = min(shares, metrics["residualShares"])
        if selected_fill and pair_shares + 1e-9 >= shares:
            held_lots = pos["upLots"] if metrics["residualSide"] == "Up" else pos["downLots"]
            held = _rotation_lot_slice(held_lots, metrics["pairedShares"], pair_shares)
            pair_sum = (
                float(held["decisionNotional"]) + float(selected_fill["decisionNotional"])
            ) / pair_shares
            net_per_share = (
                pair_shares - float(held["decisionNotional"]) - float(held["decisionFee"])
                - float(selected_fill["decisionNotional"]) - float(selected_fill["decisionFee"])
            ) / pair_shares
            if pair_sum > float(variant["lockMaxSum"]) or net_per_share < SIM_MIN_NET_LOCK_PER_SHARE:
                selected_fill = None
                record_window_diagnostic(
                    variant_id, slug, "rotation_waiting_for_profitable_hedge",
                    pairDecisionSum=pair_sum, pairNetPerShare=net_per_share,
                    residualSide=metrics["residualSide"], residualShares=metrics["residualShares"],
                )
            else:
                fair_side = float(fair["fairUp"] if selected_side == "Up" else fair["fairDown"])
                selected_edge = (
                    fair_side
                    - float(selected_fill["decisionPrice"])
                    - float(selected_fill["decisionFee"]) / shares
                )
        elif selected_fill is None:
            record_window_diagnostic(
                variant_id, slug, "rotation_hedge_depth_insufficient",
                residualSide=metrics["residualSide"],
                residualShares=metrics["residualShares"],
            )

    # Once one side is open, the only permitted next fill is its opposite-side
    # profitable hedge. The old behaviour could add a second same-side slice
    # while waiting, which turned a 5-share experiment into a 10-share average
    # down and dominated recent losses.
    if selected_fill is None and metrics["residualSide"]:
        record_window_diagnostic(
            variant_id, slug, "rotation_same_side_averaging_blocked",
            residualSide=metrics["residualSide"],
            residualShares=metrics["residualShares"],
        )
        return

    # A new residual slice is allowed only before the hedge-only phase, with a
    # fresh settlement-aligned Chainlink direction confirmation. The model edge
    # must additionally pay a residual-risk premium and reserve the future hedge
    # taker fee; the entry fee is already included in ``edge`` below.
    if selected_fill is None:
        if remaining_seconds <= float(variant.get("hedgeOnlySeconds", ROTATION_HEDGE_ONLY_SECONDS)):
            record_window_diagnostic(
                variant_id, slug, "rotation_hedge_only_wait",
                residualSide=metrics["residualSide"],
                residualShares=metrics["residualShares"],
            )
            return
        chainlink_signal = get_chainlink_twap_signal(variant["assetId"])
        if variant.get("requireChainlinkConfirm") and chainlink_signal is None:
            record_window_diagnostic(variant_id, slug, "rotation_missing_chainlink_confirmation")
            return
        chainlink_side = None
        if chainlink_signal:
            if chainlink_signal["current"] > chainlink_signal["opening"]:
                chainlink_side = "Up"
            elif chainlink_signal["current"] < chainlink_signal["opening"]:
                chainlink_side = "Down"
        if variant.get("requireChainlinkConfirm") and chainlink_side is None:
            record_window_diagnostic(
                variant_id, slug, "rotation_chainlink_neutral",
                chainlinkCurrent=chainlink_signal["current"],
                chainlinkOpening=chainlink_signal["opening"],
            )
            return
        required_edge = (
            float(variant.get("minEntryEdge", ROTATION_MIN_EDGE))
            + float(variant.get("residualRiskPremium", ROTATION_RESIDUAL_RISK_PREMIUM))
            + float(variant.get("futureHedgeFeeReserve", ROTATION_FUTURE_HEDGE_FEE_RESERVE))
        )
        candidates = []
        for side, fill in fills.items():
            if fill is None:
                continue
            if chainlink_side and side != chainlink_side:
                continue
            fair_side = float(fair["fairUp"] if side == "Up" else fair["fairDown"])
            edge = fair_side - float(fill["decisionPrice"]) - float(fill["decisionFee"]) / shares
            next_up = metrics["upShares"] + (shares if side == "Up" else 0.0)
            next_down = metrics["downShares"] + (shares if side == "Down" else 0.0)
            next_residual = abs(next_up - next_down)
            if (
                edge >= required_edge
                and next_residual <= float(variant.get("maxResidualShares", ROTATION_MAX_RESIDUAL_SHARES))
            ):
                candidates.append((edge, side, fill))
        if not candidates:
            record_window_diagnostic(
                variant_id, slug, "rotation_edge_or_residual_limit",
                upEdge=(
                    float(fair["fairUp"]) - float(fills["Up"]["decisionPrice"])
                    - float(fills["Up"]["decisionFee"]) / shares
                    if fills.get("Up") else None
                ),
                downEdge=(
                    float(fair["fairDown"]) - float(fills["Down"]["decisionPrice"])
                    - float(fills["Down"]["decisionFee"]) / shares
                    if fills.get("Down") else None
                ),
                residualSide=metrics["residualSide"],
                residualShares=metrics["residualShares"],
                requiredEdge=required_edge,
                chainlinkSide=chainlink_side,
                chainlinkCurrent=chainlink_signal["current"] if chainlink_signal else None,
                chainlinkOpening=chainlink_signal["opening"] if chainlink_signal else None,
            )
            return
        selected_edge, selected_side, selected_fill = max(candidates, key=lambda item: item[0])

    gross_cost = _position_paid_cost(pos) if pos else 0.0
    new_cost = float(selected_fill["notional"]) + float(selected_fill["fee"])
    cash, _ = compute_cash_and_portfolio(variant_id)
    if gross_cost + new_cost > float(variant.get("maxGrossBudgetUsd", ROTATION_MAX_GROSS_USD)) or new_cost > cash:
        record_window_diagnostic(
            variant_id, slug, "rotation_budget_limit",
            grossCost=gross_cost, newCost=new_cost, cash=cash,
        )
        return
    fair_side = float(fair["fairUp"] if selected_side == "Up" else fair["fairDown"])
    _rotation_add_fill(
        variant_id, slug, selected_side, selected_fill, fair_side, float(selected_edge or 0.0)
    )


def _simulate_trading_impl(
    variant_id: str,
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float | None,
    fair: dict | None,
    allow_early_exit: bool = True,
) -> None:
    variant = AB_VARIANT_BY_ID[variant_id]
    st = ab_states[variant_id]
    pos = st["position"]
    up_asks = up_book.get("asks") or []
    down_asks = down_book.get("asks") or []
    up_ask = float(up_asks[0]["price"]) if up_asks else None
    down_ask = float(down_asks[0]["price"]) if down_asks else None
    record_window_diagnostic(
        variant_id,
        slug,
        remainingSeconds=remaining_seconds,
        upAsk=up_ask,
        downAsk=down_ask,
        rawPairAskSum=(up_ask + down_ask) if up_ask is not None and down_ask is not None else None,
        upQuoteSource=up_book.get("quoteSource"),
        downQuoteSource=down_book.get("quoteSource"),
    )

    if variant.get("marketMakerOnly"):
        update_market_maker_quotes(variant_id, slug, up_book, down_book, remaining_seconds)
        return

    if variant.get("dumpThenHedge"):
        _try_btc_15m_dump_then_hedge(
            variant_id, slug, up_book, down_book, remaining_seconds
        )
        return

    if variant.get("inventoryRotation"):
        _try_inventory_rotation(variant_id, slug, up_book, down_book, remaining_seconds, fair)
        return

    if pos is None:
        if remaining_seconds is None or remaining_seconds <= 0:
            return
        # 獨立重現 2026-09-03 的舊混合流程：整個窗口先找兩腿直接鎖利，只有沒有
        # 合格配對時，才在 T-3～10 秒使用指定的方向訊號嘗試單腿方向進場。
        # 報價一致性、完整深度、滑價、費用與最低淨利仍由現行模擬防護負責。
        if variant.get("historicalHybrid"):
            pair_books_ok = _simulation_books_are_coherent(variant["assetId"], up_book, down_book)
            if pair_books_ok:
                if _try_direct_pair(variant_id, slug, up_book, down_book):
                    return
            else:
                record_window_diagnostic(
                    variant_id,
                    slug,
                    "pair_books_not_coherent",
                    dataGuardReason=_simulation_book_guard_reason(up_book, down_book),
                )
            _try_late_direction_entry(variant_id, slug, up_book, down_book, remaining_seconds)
            return
        # 這一組是純方向性驗證，不能先被兩腿鎖利部位占用；否則 Dashboard 顯示的
        # 勝率其實都是 locked trades，完全沒有驗證即將上實盤的方向訊號。
        if variant.get("lateDirectionOnly"):
            _try_late_direction_entry(variant_id, slug, up_book, down_book, remaining_seconds)
            return
        if _try_direct_pair(variant_id, slug, up_book, down_book):
            return
        # 其餘變體：找不到能立即鎖住兩邊時，有設 entryMaxPrice 的組（conservative／main／
        # loose）改用公平價模型先買便宜那一腿，之後由下方補鎖利邏輯嘗試補另一腿。
        # 2026-09-11 依使用者要求重新啟用——這條退路 2026-09 初曾因 0% 勝率（42 戰 0 勝、
        # -$364.75）關閉，現在重開是要在「兩腿加總卡在 $1.00、鎖利門檻幾乎碰不到」的市場
        # 條件下重新驗證。entryMaxPrice 為 None 的變體（含實盤用的 historical-hybrid）仍維持空手。
        _try_single_leg_entry(variant_id, slug, up_book, down_book, fair)
        return

    if pos["hedged"] or pos["windowSlug"] != slug:
        return

    if variant.get("lateDirectionOnly"):
        # 晚進場方向性策略的核心就是抱著這個部位到結算，不補鎖利、不提早出場——
        # 進場當下對邊通常正好夠便宜可以「鎖利」，但那樣等於把方向性優勢換成
        # 極小的鎖利價差，違背了這組存在的目的。
        return

    other_side = "Down" if pos["side"] == "Up" else "Up"
    other_book = down_book if other_side == "Down" else up_book
    hedge_fill = simulate_buy_fill(other_book, pos["shares"])
    if hedge_fill:
        projected_cost = (
            _position_decision_cost(pos)
            + hedge_fill["decisionNotional"]
            + hedge_fill["decisionFee"]
        )
        projected_net = pos["shares"] - projected_cost
        projected_net_per_share = projected_net / pos["shares"]
        price_sum = float(pos.get("entryDecisionPrice", pos["entryPrice"])) + hedge_fill["decisionPrice"]
        cash, _ = compute_cash_and_portfolio(variant_id)
        if (
            price_sum <= variant["lockMaxSum"]
            and projected_net_per_share >= SIM_MIN_NET_LOCK_PER_SHARE
            and hedge_fill["decisionNotional"] + hedge_fill["decisionFee"] <= cash
        ):
            hedge_position(variant_id, other_side, hedge_fill)
            return

    # 若市場願意用顯著高於模型公平價的價格接手，提早賣出比繼續承擔方向風險更有利。
    # 刻意只在 3 秒輪詢節奏下檢查（allow_early_exit=False 時整段跳過）——WS 觸發的
    # 即時評估拿到的是薄訂單簿當下那一瞬間算出來的可賣價，波動本來就大，同一個瞬間
    # 閾值判斷用高頻率去採樣很容易把雜訊當成訊號。進場/補鎖利留在即時路徑是因為那邊
    # 抓的是「機會」，錯過了就沒有；停損不一樣，真的行情反轉的話，3 秒後再確認一次
    # 幾乎不會有差別，但可以濾掉大部分薄book瞬間跳動造成的誤判。
    if allow_early_exit and fair:
        held_book = up_book if pos["side"] == "Up" else down_book
        exit_fill = simulate_sell_fill(held_book, pos["shares"])
        if exit_fill:
            fair_side = fair["fairUp"] if pos["side"] == "Up" else fair["fairDown"]
            liquidation_value = exit_fill["decisionNotional"] - exit_fill["decisionFee"]
            expected_hold_value = pos["shares"] * fair_side
            if liquidation_value >= expected_hold_value + pos["shares"] * SIM_EXIT_EDGE:
                _close_directional_position(variant_id, exit_fill, "market_bid_above_model_value")


def compute_cash_and_portfolio(variant_id: str) -> tuple[float, float]:
    """現金扣除所有未結算成本；資產以可立即變現的 bid 或完整配對的固定 $1 payout 估值。"""
    st = ab_states[variant_id]
    ms = markets_state[AB_VARIANT_BY_ID[variant_id]["assetId"]]
    positions = ([st["position"]] if st["position"] else []) + list(st["pendingSettlements"])
    staked = sum(_position_paid_cost(pos) for pos in positions)
    market_value = 0.0

    current = st["position"]
    if current:
        if current.get("strategyMode") == "inventory_rotation":
            metrics = _rotation_metrics(current)
            market_value += metrics["pairedShares"]
            if metrics["residualShares"] > 1e-9:
                held_book = (
                    ms["upBook"] if metrics["residualSide"] == "Up" else ms["downBook"]
                )
                liquidation = simulate_sell_fill(held_book, metrics["residualShares"])
                if liquidation:
                    market_value += liquidation["notional"] - liquidation["fee"]
        elif current.get("hedged"):
            market_value += current["shares"]
        else:
            held_book = ms["upBook"] if current["side"] == "Up" else ms["downBook"]
            liquidation = simulate_sell_fill(held_book, current["shares"])
            if liquidation:
                market_value += liquidation["notional"] - liquidation["fee"]

    for pending in st["pendingSettlements"]:
        # 完整配對一定可收回每股 $1；單邊倉在未知結果期間保守估值為 0，絕不提前釋放本金。
        if pending.get("strategyMode") == "inventory_rotation":
            market_value += _rotation_metrics(pending)["pairedShares"]
        elif pending.get("hedged"):
            market_value += pending["shares"]

    cash = shared_config["startBalance"] + st["totalPnl"] - staked
    portfolio = cash + market_value
    peak = max(float(st.get("peakPortfolio", shared_config["startBalance"])), shared_config["startBalance"], portfolio)
    st["peakPortfolio"] = peak
    drawdown = ((peak - portfolio) / peak * 100) if peak > 0 else 0.0
    st["maxDrawdown"] = max(float(st.get("maxDrawdown", 0)), drawdown)
    return cash, portfolio


def record_trade(variant_id: str, pos: dict, pnl: float, outcome: str) -> None:
    st = ab_states[variant_id]
    is_rotation = pos.get("strategyMode") == "inventory_rotation"
    fees = (
        float(pos.get("upFee", 0)) + float(pos.get("downFee", 0)) + float(pos.get("exitFee", 0))
        if is_rotation
        else float(pos.get("entryFee", 0)) + float(pos.get("hedgeFee", 0)) + float(pos.get("exitFee", 0))
    )
    trade_type = "inventory_rotation" if is_rotation else (
        "locked" if pos.get("hedged") else ("early_exit" if outcome == "EarlyExit" else "directional")
    )
    trade = {
        "windowSlug":   pos["windowSlug"],
        "side":         pos["side"],
        "entryPrice":   pos["entryPrice"],
        "entryDecisionPrice": pos.get("entryDecisionPrice"),
        "entryNotional": pos.get("entryNotional"),
        "entryFee":     pos.get("entryFee", 0),
        "shares":       pos["shares"],
        "stakeUsd":     pos.get("stakeUsd"),
        "hedged":       pos.get("hedged", False),
        "hedgeSide":    pos.get("hedgeSide"),
        "hedgePrice":   pos.get("hedgePrice"),
        "hedgeDecisionPrice": pos.get("hedgeDecisionPrice"),
        "hedgeFee":     pos.get("hedgeFee", 0),
        "exitPrice":    pos.get("exitPrice"),
        "exitDecisionPrice": pos.get("exitDecisionPrice"),
        "exitFee":      pos.get("exitFee", 0),
        "exitReason":   pos.get("exitReason"),
        "fairProbability": pos.get("fairProbability"),
        "entryEdge":    pos.get("entryEdge"),
        "strategyMode": pos.get("strategyMode"),
        "signalSource": pos.get("signalSource"),
        "signalDropPct": pos.get("signalDropPct"),
        "signalReferencePrice": pos.get("signalReferencePrice"),
        "signalCurrentAsk": pos.get("signalCurrentAsk"),
        "signalLookbackSeconds": pos.get("signalLookbackSeconds"),
        "signalRemainingSeconds": pos.get("signalRemainingSeconds"),
        "hedgeWaitSeconds": pos.get("hedgeWaitSeconds"),
        "maker":         bool(pos.get("maker")),
        "tradeType":    trade_type,
        "outcome":      outcome,
        "fees":         fees,
        "grossPnl":     pnl + fees,
        "pnl":          pnl,
        "entryTime":    pos["entryTime"],
        "exitTime":     time.time(),
    }
    if is_rotation:
        metrics = _rotation_metrics(pos)
        trade.update({
            "upShares": metrics["upShares"],
            "downShares": metrics["downShares"],
            "pairedShares": metrics["pairedShares"],
            "residualSide": metrics["residualSide"],
            "residualShares": metrics["residualShares"],
            "upAvgPrice": pos.get("upAvgPrice"),
            "downAvgPrice": pos.get("downAvgPrice"),
            "lockedPnl": metrics["lockedPnl"],
            "fillCount": int(pos.get("fillCount", 0)),
            "totalMarketCost": _position_paid_cost(pos),
        })
    st["trades"].insert(0, trade)
    st["trades"] = st["trades"][:50]
    st["totalPnl"] += pnl
    st["totalFees"] += fees
    st["totalTrades"] += 1
    if trade_type == "inventory_rotation":
        if float(trade.get("residualShares", 0)) > 1e-9:
            _rotation_stats(st)["unpairedSettlements"] += 1
    elif trade_type == "locked":
        st["lockedTrades"] += 1
    elif trade_type == "early_exit":
        st["earlyExits"] += 1
    else:
        st["directionalTrades"] += 1
    if pnl > 0:
        st["wins"] += 1
    if AB_VARIANT_BY_ID[variant_id].get("marketMakerOnly") and not pos.get("hedged"):
        _maker_stats(st)["singleLegSettlements"] += 1
    if AB_VARIANT_BY_ID[variant_id].get("dumpThenHedge") and not pos.get("hedged"):
        _dump_hedge_stats(st)["unhedgedSettlements"] += 1
    record_window_diagnostic(
        variant_id,
        pos["windowSlug"],
        "settled",
        status="settled",
        outcome=outcome,
        tradeType=trade_type,
        pnl=pnl,
        fees=fees,
        settledAt=trade["exitTime"],
    )
    persist_trade(variant_id, trade)
    save_sim_state()


def _settle_pnl(pos: dict, outcome: str) -> float:
    if pos.get("strategyMode") == "inventory_rotation":
        payout = float(pos.get("upShares", 0)) if outcome == "Up" else float(pos.get("downShares", 0))
        return payout - _position_paid_cost(pos)
    payout = pos["shares"] if pos.get("hedged") or outcome == pos["side"] else 0.0
    return payout - _position_paid_cost(pos)


async def retry_pending_settlements(session: aiohttp.ClientSession) -> None:
    """窗口結束當下，Polymarket 的結算（Chainlink 資料源）通常還沒跑完，查不到結果。
    查不到不代表沒發生，是還沒好——把它放進待結算佇列，之後每一輪都重試，
    直到真的查到結果為止，不會因為第一次查不到就把這筆損益憑空丟掉。
    對每一組 A/B 都各自重試，互不影響。
    """
    state_changed = False
    for variant_id, st in ab_states.items():
        if not st["pendingSettlements"]:
            continue
        pending_before = len(st["pendingSettlements"])
        still_pending = []
        for pos in st["pendingSettlements"]:
            outcome = await fetch_outcome(session, pos["windowSlug"])
            if outcome is None:
                still_pending.append(pos)
                continue
            pnl = _settle_pnl(pos, outcome)
            record_trade(variant_id, pos, pnl, outcome)
            log.info(f"[SIM:{variant_id}] 結算 {pos['windowSlug']} 結果={outcome} "
                      f"{'(已鎖利)' if pos['hedged'] else '(方向性)'} PnL=${pnl:+.2f}")
        st["pendingSettlements"] = still_pending
        if len(still_pending) != pending_before:
            state_changed = True
    # This function runs every three seconds.  Rewriting every strategy row
    # when there was nothing to settle bloats SQLite's WAL and can pause the
    # same event loop that must drain the CLOB WebSocket.  Window rollover and
    # actual fills already persist immediately, so a no-op retry needs no write.
    if state_changed:
        save_sim_state()

def queue_settlement(slug: str) -> None:
    """窗口換了：每一組 A/B 如果上一個窗口還有沒結算的倉位，各自丟進自己的待結算佇列，
    換一個乾淨的位置開始追蹤新窗口。"""
    finalize_window_diagnostics(slug)
    for st in ab_states.values():
        pos = st["position"]
        if pos is not None and pos["windowSlug"] == slug:
            st["pendingSettlements"].append(pos)
            st["position"] = None
        quotes = st.get("makerQuotes")
        if isinstance(quotes, dict) and quotes.get("windowSlug") == slug:
            st["makerQuotes"] = None
    _btc_15m_ask_history.pop(slug, None)
    save_sim_state()

# ── Polymarket 市場資料 WebSocket（只用在模擬版）───────────────────────────
# 只是把「兩邊訂單簿報價」這件事從 3 秒輪詢一次的 REST，換成即時推播，
# 讓 _try_direct_pair 那種「當下兩邊剛好都夠便宜」的真無風險套利機會更容易被抓到
# ——這種瞬間通常很短暫，輪詢常常來不及看到就消失了。
# 刻意完全不動 fetch_book/fetch_midpoint 這兩個函式本身：polymarket_live_strategy.py
# 直接呼叫的是這兩個函式，維持原本的 REST 行為不變，這個 WS 只影響模擬版自己内部
# 怎麼填 ms["upBook"]/ms["downBook"]，不會連帶影響真實下單那邊。
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_PING_INTERVAL = 10.0
WS_RECONNECT_BACKOFF = [1, 2, 5, 10, 20]
# The CLOB stream can publish thousands of book deltas per second.  Running all
# live and paper strategies inline in ``ws.recv()`` makes this process a slow
# consumer and eventually causes a 1013 disconnect.  Keep the reader hot and
# evaluate the latest coalesced book on a separate, rate-limited event-loop task.
WS_TICK_DISPATCH_INTERVAL_SECONDS = max(
    0.005,
    min(0.250, float(os.environ.get("POLY_WS_TICK_DISPATCH_INTERVAL_MS", "20")) / 1000.0),
)
WS_PERF_LOG_INTERVAL_SECONDS = 60.0

_ws_books: dict = {}              # token_id -> {"bids": {price_str: size}, "asks": {price_str: size}}
_ws_meta: dict = {}               # token_id -> {"tickSize":, "minOrderSize":}，第一次見到時查一次就沿用
_ws_wanted_by_asset: dict = {}    # asset_id -> 這一輪視窗該資產需要的 token，7 個資產各自更新，
                                   # 合併起來才是 _ws_wanted_tokens——不能讓某個資產的更新覆蓋掉其他資產。
_ws_wanted_tokens: set = set()    # 目前所有資產合併起來真正需要的 token（隨各資產視窗替換而更新）
_ws_subscribed_tokens: set = set()  # WS 連線目前實際訂閱中的 token
_ws_conn = None                   # 目前存活的 WS 連線物件，斷線時是 None
_ws_connected = False
_ws_snapshot_tokens: set = set()
_ws_book_updated_at: dict = {}
_ws_last_message_at = 0.0
_ws_price_listeners: set = set()
_ws_simulation_ticks_enabled = True
_pending_simulation_ticks: set[tuple[str, str]] = set()
_pending_ws_price_ticks: set[str] = set()
_pending_ws_tick_queued_at: dict[str, float] = {}
_ws_tick_event: asyncio.Event | None = None
_ws_tick_dispatch_task: asyncio.Task | None = None
_ws_perf_last_log_at = 0.0
_ws_perf = {
    "rawMessages": 0,
    "eventsApplied": 0,
    "bookUpdates": 0,
    "coalescedUpdates": 0,
    "dispatchedTicks": 0,
    "dispatchBatches": 0,
    "maxPendingTokens": 0,
    "lastDispatchLagMs": None,
    "maxDispatchLagMs": 0.0,
    "disconnects": 0,
    "slowConsumerDisconnects": 0,
}
_sim_data_guard_log_at: dict = {}


def register_ws_price_listener(callback) -> None:
    """Register a sync or async callback called after a WS book change."""
    _ws_price_listeners.add(callback)


def unregister_ws_price_listener(callback) -> None:
    _ws_price_listeners.discard(callback)


def set_ws_simulation_ticks_enabled(enabled: bool) -> None:
    """Control simulation fills triggered by WS ticks in this process."""
    global _ws_simulation_ticks_enabled
    _ws_simulation_ticks_enabled = bool(enabled)


def ws_feed_status() -> dict:
    """Return a small, credential-free WS health snapshot."""
    age = None if not _ws_last_message_at else max(0.0, time.monotonic() - _ws_last_message_at)
    return {
        "connected": bool(_ws_connected),
        "healthy": bool(_ws_connected and age is not None and age <= WS_PING_INTERVAL * 2.5),
        "lastMessageAgeSeconds": age,
        "subscribedTokens": len(_ws_subscribed_tokens),
        "tickDispatchIntervalMs": WS_TICK_DISPATCH_INTERVAL_SECONDS * 1000.0,
        "pendingPriceTicks": len(_pending_ws_price_ticks),
        "performance": dict(_ws_perf),
    }


def _simulation_book_guard_reason(
    up_book: dict,
    down_book: dict,
    now: float | None = None,
    max_skew_seconds: float = SIM_BOOK_MAX_SKEW_SECONDS,
) -> str | None:
    """Return why a two-leg paper fill is unsafe, or ``None`` when coherent."""
    if up_book.get("quoteSource") != "websocket" or down_book.get("quoteSource") != "websocket":
        return "兩腿並非都來自 WebSocket 完整快照"
    up_at = up_book.get("receivedAtMonotonic")
    down_at = down_book.get("receivedAtMonotonic")
    if not isinstance(up_at, (int, float)) or not isinstance(down_at, (int, float)):
        return "兩腿缺少接收時間"
    current = time.monotonic() if now is None else float(now)
    up_age = current - float(up_at)
    down_age = current - float(down_at)
    if up_age < -0.1 or down_age < -0.1:
        return "兩腿接收時間異常"
    if max(up_age, down_age) > SIM_BOOK_MAX_AGE_SECONDS:
        return f"兩腿報價過舊 age={max(up_age, down_age):.3f}s"
    skew = abs(float(up_at) - float(down_at))
    if skew > max_skew_seconds:
        return f"兩腿更新時間差過大 skew={skew:.3f}s"
    return None


def _simulation_single_book_guard_reason(book: dict, now: float | None = None) -> str | None:
    """Return why one directional BUY book is unsafe, without requiring the opposite leg."""
    if book.get("quoteSource") != "websocket":
        return "方向腿不是來自 WebSocket 完整快照"
    received_at = book.get("receivedAtMonotonic")
    if not isinstance(received_at, (int, float)):
        return "方向腿缺少接收時間"
    current = time.monotonic() if now is None else float(now)
    age = current - float(received_at)
    if age < -0.05:
        return f"方向腿接收時間異常 age={age:.3f}s"
    if age > SIM_BOOK_MAX_AGE_SECONDS:
        return f"方向腿報價過舊 age={age:.3f}s"
    return None


def _simulation_direction_book_is_fresh(
    asset_id: str,
    side: str,
    book: dict,
    now: float | None = None,
) -> bool:
    reason = _simulation_single_book_guard_reason(book, now)
    if reason is None:
        return True
    current = time.monotonic() if now is None else float(now)
    log_key = f"{asset_id}:direction:{side.lower()}"
    if current - _sim_data_guard_log_at.get(log_key, 0.0) >= SIM_DATA_GUARD_LOG_SECONDS:
        _sim_data_guard_log_at[log_key] = current
        log.info(f"[SIM-DATA-GUARD:{log_key}] 跳過模擬成交：{reason}")
    return False


def _simulation_books_are_coherent(
    asset_id: str,
    up_book: dict,
    down_book: dict,
    now: float | None = None,
    max_skew_seconds: float = SIM_BOOK_MAX_SKEW_SECONDS,
) -> bool:
    reason = _simulation_book_guard_reason(up_book, down_book, now, max_skew_seconds)
    if reason is None:
        return True
    current = time.monotonic() if now is None else float(now)
    if current - _sim_data_guard_log_at.get(asset_id, 0.0) >= SIM_DATA_GUARD_LOG_SECONDS:
        _sim_data_guard_log_at[asset_id] = current
        log.info(f"[SIM-DATA-GUARD:{asset_id}] 跳過模擬成交：{reason}")
    return False


def _variant_books_are_coherent(asset_id: str, variant: dict, up_book: dict, down_book: dict) -> bool:
    """Dispatch each strategy only after the data it actually uses is safe."""
    # Every direction-only variant validates its selected BUY leg after the
    # signal determines the side. Pair strategies still require both legs.
    if variant.get("lateDirectionOnly") or variant.get("dumpThenHedge"):
        return True
    max_skew = SIM_BOOK_MAX_SKEW_SECONDS
    log_key = asset_id
    coherent = _simulation_books_are_coherent(
        log_key, up_book, down_book, max_skew_seconds=max_skew
    )
    if not coherent:
        market = markets_state.get(asset_id, {}).get("market") or {}
        slug = market.get("slug")
        if slug:
            record_window_diagnostic(
                variant["id"],
                slug,
                "pair_books_not_coherent",
                dataGuardReason=_simulation_book_guard_reason(
                    up_book, down_book, max_skew_seconds=max_skew
                ),
            )
    return coherent


def _notify_ws_price_listeners(token_id: str, source: str = "market_ws") -> bool:
    token = decision_diag.trigger.set(source)
    try:
        return _dispatch_ws_price_listeners(token_id)
    finally:
        decision_diag.trigger.reset(token)


def _dispatch_ws_price_listeners(token_id: str) -> bool:
    action_scheduled = False
    for callback in tuple(_ws_price_listeners):
        try:
            result = callback(token_id)
            if asyncio.iscoroutine(result):
                asyncio.get_running_loop().create_task(result)
            elif result:
                action_scheduled = True
        except Exception as exc:
            log.exception(f"[WS] price listener failed for token={token_id}: {exc}")
    return action_scheduled


def _defer_simulation_tick(source: str, key: str, callback) -> None:
    """Let a just-scheduled live action run before coalesced simulation work."""
    tick_key = (source, key)
    if tick_key in _pending_simulation_ticks:
        return
    _pending_simulation_ticks.add(tick_key)

    def run() -> None:
        _pending_simulation_ticks.discard(tick_key)
        callback()

    asyncio.get_running_loop().call_soon(run)


async def _ws_ensure_meta(session: aiohttp.ClientSession, token_id: str) -> None:
    """tick size / 最低下單股數不會在 WS 推播裡出現（那是靜態市場屬性，不是報價），
    第一次遇到這個 token 時用 REST 查一次、順便拿它的初始快照墊檔，
    避免視窗剛換、WS 資料還沒推過來之前的空窗期完全沒有報價可用。"""
    if token_id in _ws_meta:
        return
    try:
        book = await fetch_book(session, token_id)
        _ws_meta[token_id] = {"tickSize": book["tickSize"], "minOrderSize": book["minOrderSize"]}
        _ws_books[token_id] = {
            "bids": {str(b["price"]): b["size"] for b in book["bids"]},
            "asks": {str(a["price"]): a["size"] for a in book["asks"]},
        }
    except Exception as e:
        log.warning(f"[WS] 查 tick size / 最低股數失敗 token={token_id}：{e}")


async def _ws_set_wanted_tokens(asset_id: str, token_ids: set) -> None:
    """某個資產這一輪視窗換了、要追蹤的 token 也跟著換——只更新這個資產自己的那份，
    再跟其他資產目前的合併成整體想要的清單，不能整批覆蓋掉（不然 7 個資產輪流呼叫
    這個函式時，後面呼叫的資產會把前面資產的訂閱蓋掉）。
    如果 WS 目前是連線狀態就直接送訂閱/取消訂閱，不然只更新「想要的清單」，
    等連線建立/重連時會整批依這份清單訂閱。"""
    global _ws_wanted_tokens
    _ws_wanted_by_asset[asset_id] = set(token_ids)
    _ws_wanted_tokens = set().union(*_ws_wanted_by_asset.values()) if _ws_wanted_by_asset else set()
    if _ws_conn is None:
        return
    to_add = _ws_wanted_tokens - _ws_subscribed_tokens
    to_remove = _ws_subscribed_tokens - _ws_wanted_tokens
    try:
        if to_remove:
            await _ws_conn.send(json.dumps({"assets_ids": list(to_remove), "operation": "unsubscribe"}))
            _ws_subscribed_tokens.difference_update(to_remove)
        if to_add:
            await _ws_conn.send(json.dumps({"assets_ids": list(to_add), "operation": "subscribe"}))
            _ws_subscribed_tokens.update(to_add)
    except Exception as e:
        log.warning(f"[WS] 訂閱/取消訂閱失敗，等重連後會整批重新訂閱：{e}")


# ── 做市可行性觀察實驗（純唯讀，完全不下單）───────────────────────────────
# 只是記錄真實成交（last_trade_price）發生的頻率、量能、跟當下 best bid/ask 價差的
# 關係，估算「如果真的去掛做市單，大概多常會被吃到」。這不是精確回測——不知道
# 真的掛單會不會排在隊伍最前面，只是先用真實數據看這個市場擠不擠、有沒有量，
# 值不值得投入蓋一整套掛單/改價/庫存管理的基礎設施。
MM_TRADE_HISTORY_LIMIT = 300
MM_SUMMARY_INTERVAL = 60.0

_mm_trades: dict = {}          # token_id -> deque[{"ts","price","size","side"}]（最近成交）
_mm_last_summary_at = 0.0
_mm_seen_trade_keys = deque(maxlen=2_000)
_mm_seen_trade_key_set: set = set()


def _mm_record_trade(payload: dict) -> None:
    tid = payload.get("tokenId") or payload.get("asset_id")
    if not tid:
        return
    try:
        raw_ts = float(payload.get("timestamp", 0) or 0)
        trade = {
            "ts": (raw_ts / 1000.0 if raw_ts > 10_000_000_000 else raw_ts) if raw_ts > 0 else time.time(),
            "price": float(payload["price"]),
            "size": float(payload["size"]),
            "side": str(payload.get("side", "")).upper(),
        }
    except (KeyError, ValueError, TypeError):
        return
    trade_key = (tid, trade["ts"], trade["price"], trade["size"], trade["side"])
    if trade_key in _mm_seen_trade_key_set:
        return
    if len(_mm_seen_trade_keys) == _mm_seen_trade_keys.maxlen:
        _mm_seen_trade_key_set.discard(_mm_seen_trade_keys[0])
    _mm_seen_trade_keys.append(trade_key)
    _mm_seen_trade_key_set.add(trade_key)
    _mm_trades.setdefault(tid, deque(maxlen=MM_TRADE_HISTORY_LIMIT)).append(trade)
    if MARKET_MAKER_VARIANTS:
        process_market_maker_trade(tid, trade)


def _mm_maybe_log_summary() -> None:
    """每隔一段時間印一次觀察摘要，不用真的去看程式碼或另外開頁面就能追蹤。"""
    global _mm_last_summary_at
    now = time.time()
    if now - _mm_last_summary_at < MM_SUMMARY_INTERVAL:
        return
    _mm_last_summary_at = now
    for asset_id, ms in markets_state.items():
        for side_label, tid in (("Up", ms.get("upTokenId")), ("Down", ms.get("downTokenId"))):
            if not tid:
                continue
            dq = _mm_trades.get(tid)
            recent = [t for t in dq if now - t["ts"] <= MM_SUMMARY_INTERVAL] if dq else []
            book = _ws_get_book(tid)
            spread = None
            if book and book["bids"] and book["asks"]:
                spread = book["asks"][0]["price"] - book["bids"][0]["price"]
            spread_txt = f"${spread:.3f}" if spread is not None else "無雙邊報價"
            if not recent:
                log.info(f"[MM觀察:{asset_id}:{side_label}] 過去{MM_SUMMARY_INTERVAL:.0f}秒無成交　目前價差={spread_txt}")
                continue
            total_size = sum(t["size"] for t in recent)
            avg_size = total_size / len(recent)
            log.info(
                f"[MM觀察:{asset_id}:{side_label}] 過去{MM_SUMMARY_INTERVAL:.0f}秒 成交{len(recent)}筆　"
                f"總量={total_size:.1f}股　平均單筆={avg_size:.1f}股　目前價差={spread_txt}"
            )


def _ws_apply_message(msg: dict) -> None:
    """套用一則 WS 訊息、更新本地訂單簿；有真的變動到的 token 會立刻觸發一次評估
    （_on_ws_price_tick），不等下一個 3 秒輪詢——這就是補齊「文章那種即時反應速度」
    的關鍵：真正無風險套利的瞬間往往很短暫，等輪詢常常已經來不及。
    event_type 可能在最外層（book/price_change 實測過是這樣），也可能包在
    type + payload 裡（文件上 last_trade_price 是這樣）——兩種都接。"""
    _ws_perf["eventsApplied"] += 1
    event_type = msg.get("event_type") or msg.get("type")
    payload = msg.get("payload", msg)
    if event_type == "book":
        tid = payload.get("asset_id")
        if not tid:
            return
        _ws_books[tid] = {
            "bids": {str(b["price"]): float(b["size"]) for b in payload.get("bids", [])},
            "asks": {str(a["price"]): float(a["size"]) for a in payload.get("asks", [])},
        }
        _ws_snapshot_tokens.add(tid)
        _ws_book_updated_at[tid] = time.monotonic()
        _queue_ws_price_tick(tid)
    elif event_type == "price_change":
        touched = set()
        for change in payload.get("price_changes", []):
            tid = change.get("asset_id")
            if not tid:
                continue
            book = _ws_books.setdefault(tid, {"bids": {}, "asks": {}})
            side_key = "bids" if str(change.get("side", "")).upper() == "BUY" else "asks"
            price = str(change.get("price"))
            size = float(change.get("size", 0) or 0)
            if size <= 0:
                book[side_key].pop(price, None)
            else:
                book[side_key][price] = size
            touched.add(tid)
        for tid in touched:
            # Do not treat a delta received before the full book snapshot as a
            # tradable book.  The server normally sends ``book`` first.
            if tid in _ws_snapshot_tokens:
                _ws_book_updated_at[tid] = time.monotonic()
                _queue_ws_price_tick(tid)
    elif event_type == "last_trade_price":
        _mm_record_trade(payload)
    _mm_maybe_log_summary()


def _run_ws_simulation_tick(token_id: str) -> None:
    """跟目前這輪視窗有關的 token 報價一有變動就立刻重跑一次評估（純記憶體運算，
    沒有任何 I/O，很便宜，可以放心讓它跑得比 3 秒輪詢頻繁很多）。
    刻意不呼叫 persist_quote——那個會寫 SQLite，頻率這麼高的話划不來，
    報價歷史記錄還是交給原本的 3 秒輪詢週期就好。"""
    if _ws_simulation_ticks_enabled:
        for aid, ms in markets_state.items():
            if token_id not in (ms.get("upTokenId"), ms.get("downTokenId")):
                continue
            market = ms.get("market")
            if not market:
                break
            up_book = _ws_get_book(ms["upTokenId"])
            down_book = _ws_get_book(ms["downTokenId"])
            if up_book is None or down_book is None:
                break
            ms["upBook"], ms["downBook"] = up_book, down_book
            slug = market["slug"]
            remaining_seconds = (
                None if ms["windowEndsAt"] is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
            )
            fair = ms.get("fair")
            if aid == "btc":
                log_price_sum_diagnostic(
                    f"sim-ws-{aid}", up_book, down_book,
                    AB_VARIANT_BY_ID["btc-historical-hybrid"]["lockMaxSum"],
                )
            for variant_id, variant in AB_VARIANT_BY_ID.items():
                if variant["assetId"] == aid and _variant_books_are_coherent(
                    aid, variant, up_book, down_book
                ):
                    simulate_trading(
                        variant_id, slug, up_book, down_book, remaining_seconds, fair,
                        allow_early_exit=False,
                        evaluation_source="market_ws",
                    )
            break


def _on_ws_price_tick(token_id: str) -> None:
    """Prioritize live evaluation and defer simulation when it schedules work."""
    if _notify_ws_price_listeners(token_id):
        _defer_simulation_tick(
            "market_ws", token_id, lambda: _run_ws_simulation_tick(token_id)
        )
        return
    _run_ws_simulation_tick(token_id)


def _ensure_ws_tick_dispatcher() -> bool:
    """Start the coalescing worker when called from a running asyncio loop.

    Synchronous unit tests and offline helpers have no loop; they retain the old
    immediate-dispatch behaviour so this module remains easy to exercise.
    """
    global _ws_tick_event, _ws_tick_dispatch_task
    if _ws_tick_dispatch_task is not None and not _ws_tick_dispatch_task.done():
        return True
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return False
    _ws_tick_event = asyncio.Event()
    _ws_tick_dispatch_task = loop.create_task(
        _ws_price_tick_dispatch_loop(), name="polymarket-ws-tick-dispatch"
    )
    return True


def _queue_ws_price_tick(token_id: str) -> None:
    """Queue only the newest update for a token without blocking ``ws.recv()``."""
    if not _ensure_ws_tick_dispatcher():
        _on_ws_price_tick(token_id)
        return
    now = time.monotonic()
    _ws_perf["bookUpdates"] += 1
    if token_id in _pending_ws_price_ticks:
        _ws_perf["coalescedUpdates"] += 1
    _pending_ws_price_ticks.add(token_id)
    # Overwrite with the newest timestamp: the worker evaluates the newest book,
    # so this measures latency of the data that is actually used for a decision.
    _pending_ws_tick_queued_at[token_id] = now
    _ws_perf["maxPendingTokens"] = max(
        _ws_perf["maxPendingTokens"], len(_pending_ws_price_ticks)
    )
    if _ws_tick_event is not None:
        _ws_tick_event.set()


def _drain_ws_price_ticks() -> int:
    """Dispatch one evaluation per pending token using its latest in-memory book."""
    tokens = tuple(_pending_ws_price_ticks)
    if not tokens:
        return 0
    queued_at = {token_id: _pending_ws_tick_queued_at.get(token_id) for token_id in tokens}
    _pending_ws_price_ticks.difference_update(tokens)
    for token_id in tokens:
        _pending_ws_tick_queued_at.pop(token_id, None)

    now = time.monotonic()
    lags = [max(0.0, now - value) * 1000.0 for value in queued_at.values()
            if isinstance(value, (int, float))]
    if lags:
        lag_ms = max(lags)
        _ws_perf["lastDispatchLagMs"] = lag_ms
        _ws_perf["maxDispatchLagMs"] = max(_ws_perf["maxDispatchLagMs"], lag_ms)
    _ws_perf["dispatchBatches"] += 1
    _ws_perf["dispatchedTicks"] += len(tokens)
    for token_id in tokens:
        _on_ws_price_tick(token_id)
    return len(tokens)


async def _ws_price_tick_dispatch_loop() -> None:
    """Rate-limit strategy work while preserving the newest order book state."""
    global _ws_perf_last_log_at
    last_dispatch_at = 0.0
    while True:
        if not _pending_ws_price_ticks:
            assert _ws_tick_event is not None
            await _ws_tick_event.wait()
        wait_seconds = WS_TICK_DISPATCH_INTERVAL_SECONDS - (time.monotonic() - last_dispatch_at)
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        if _ws_tick_event is not None:
            _ws_tick_event.clear()
        dispatched = _drain_ws_price_ticks()
        if dispatched:
            last_dispatch_at = time.monotonic()
        if last_dispatch_at - _ws_perf_last_log_at >= WS_PERF_LOG_INTERVAL_SECONDS:
            _ws_perf_last_log_at = last_dispatch_at
            log.info(
                "[WS-PERF] raw=%d events=%d book_updates=%d dispatched=%d "
                "coalesced=%d pending=%d last_lag=%.1fms max_lag=%.1fms",
                _ws_perf["rawMessages"],
                _ws_perf["eventsApplied"],
                _ws_perf["bookUpdates"],
                _ws_perf["dispatchedTicks"],
                _ws_perf["coalescedUpdates"],
                len(_pending_ws_price_ticks),
                float(_ws_perf["lastDispatchLagMs"] or 0.0),
                float(_ws_perf["maxDispatchLagMs"] or 0.0),
            )


async def market_ws_loop() -> None:
    """背景常駐：連線 Polymarket 市場資料 WS，斷線自動重連（指數退避），
    重連後依 _ws_wanted_tokens 整批重新訂閱目前這輪視窗的 token。"""
    global _ws_conn, _ws_connected, _ws_subscribed_tokens
    global _ws_last_message_at, _ws_snapshot_tokens
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(MARKET_WS_URL, ping_interval=None) as ws:
                _ws_conn = ws
                _ws_subscribed_tokens = set()
                _ws_snapshot_tokens = set()
                if _ws_wanted_tokens:
                    await ws.send(json.dumps({"assets_ids": list(_ws_wanted_tokens), "type": "market"}))
                    _ws_subscribed_tokens = set(_ws_wanted_tokens)
                _ws_connected = True
                _ws_last_message_at = time.monotonic()
                backoff_idx = 0
                log.info(f"[WS] 市場資料流已連線，訂閱 {len(_ws_subscribed_tokens)} 個 token")

                last_ping = time.monotonic()
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_PING_INTERVAL)
                    except asyncio.TimeoutError:
                        raw = None
                    now = time.monotonic()
                    if now - last_ping >= WS_PING_INTERVAL:
                        await ws.send("PING")
                        last_ping = now
                    if raw is not None:
                        _ws_last_message_at = time.monotonic()
                        _ws_perf["rawMessages"] += 1
                    if raw is None or raw == "PONG":
                        continue
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        continue
                    for msg in (parsed if isinstance(parsed, list) else [parsed]):
                        if isinstance(msg, dict):
                            _ws_apply_message(msg)
        except Exception as e:
            _ws_perf["disconnects"] += 1
            if "slow consumer" in str(e).lower():
                _ws_perf["slowConsumerDisconnects"] += 1
            log.warning(f"[WS] 市場資料流斷線，準備重連：{e}")
        _ws_connected = False
        _ws_conn = None
        _ws_subscribed_tokens = set()
        _ws_snapshot_tokens = set()
        _pending_ws_price_ticks.clear()
        _pending_ws_tick_queued_at.clear()
        delay = WS_RECONNECT_BACKOFF[min(backoff_idx, len(WS_RECONNECT_BACKOFF) - 1)]
        backoff_idx += 1
        await asyncio.sleep(delay)


def _ws_get_book(token_id: str, limit: int = 6) -> dict | None:
    """回傳跟 fetch_book() 一模一樣格式的 dict，沒有資料就回傳 None 讓呼叫端退回 REST。"""
    raw = _ws_books.get(token_id)
    if not raw:
        return None
    bids = sorted(
        ({"price": float(p), "size": s} for p, s in raw["bids"].items() if s > 0),
        key=lambda x: -x["price"],
    )[:limit]
    asks = sorted(
        ({"price": float(p), "size": s} for p, s in raw["asks"].items() if s > 0),
        key=lambda x: x["price"],
    )[:limit]
    if not bids and not asks:
        return None
    meta = _ws_meta.get(token_id, {})
    return {
        "bids": bids,
        "asks": asks,
        "tickSize": meta.get("tickSize", 0.01),
        "minOrderSize": meta.get("minOrderSize", 1.0),
        "quoteSource": "websocket" if token_id in _ws_snapshot_tokens else "initial_rest_snapshot",
        "receivedAtMonotonic": _ws_book_updated_at.get(token_id),
    }


async def _get_book_ws_or_rest(session: aiohttp.ClientSession, token_id: str) -> dict:
    if ws_feed_status()["healthy"] and token_id in _ws_snapshot_tokens:
        book = _ws_get_book(token_id)
        if book is not None:
            return book
    book = await fetch_book(session, token_id)
    book["quoteSource"] = "rest_fallback"
    return book


async def _get_midpoint_ws_or_rest(session: aiohttp.ClientSession, token_id: str, book: dict) -> float:
    if book["bids"] and book["asks"]:
        return (book["bids"][0]["price"] + book["asks"][0]["price"]) / 2
    return await fetch_midpoint(session, token_id)


def _latest_ws_book_or_fallback(token_id: str, fallback: dict) -> dict:
    """Prefer the newest complete WS snapshot after unrelated polling I/O."""
    latest = _ws_get_book(token_id)
    if latest is not None and latest.get("quoteSource") == "websocket":
        return latest
    return fallback


# ── 背景抓取任務 ───────────────────────────────────────────────────────────

async def _fetch_one_asset(session: aiohttp.ClientSession, asset: dict) -> None:
    aid = asset["id"]
    ms = markets_state[aid]

    # 每輪都重新問 Polymarket「現在正在進行的是哪個窗口」，
    # 不要用本機時鐘去推算「是不是該換下一輪」——本機時鐘不見得準，
    # 但 Polymarket 自己回傳的 active/closed 狀態一定是對的，直接拿來當真相來源。
    cur = ms["market"]
    new_market = await fetch_active_market(
        session, asset["slugPrefix"], asset.get("windowSeconds", WINDOW_SECONDS)
    )

    if new_market and (cur is None or new_market["slug"] != cur["slug"]):
        if cur is not None:
            queue_settlement(cur["slug"])
        ms["market"] = new_market
        ms["windowEndsAt"] = _iso_to_ms(new_market["endDate"])
        ms["windowOpenSpotPrice"] = None  # 換窗口了，開盤價重新觀察
        ms["windowOpenChainlinkTwapPrice"] = None
        ms["windowOpenChainlinkTwapObservedAt"] = None
        ms["windowOpenChainlinkTwapSlug"] = None
        _capture_window_open_chainlink_twap(ms)
        start_window_diagnostics(aid, new_market["slug"], ms["windowEndsAt"])
        log.info(f"[MARKET:{aid}] 切換到新窗口 {new_market['slug']}　結束於 {new_market['endDate']}")

    if not ms["market"]:
        return
    up_id, down_id = _market_tokens(ms["market"])
    if not (up_id and down_id):
        return
    ms["upTokenId"], ms["downTokenId"] = up_id, down_id

    # 每輪都同步一次「想要的 token」（函式內部自己 diff，沒變就不會真的送訂閱訊息）；
    # 順便確保 tick size / 最低股數已經查過，WS 才有資料可以馬上用。
    await _ws_set_wanted_tokens(aid, {up_id, down_id})
    await asyncio.gather(_ws_ensure_meta(session, up_id), _ws_ensure_meta(session, down_id))

    up_book, down_book = await asyncio.gather(
        _get_book_ws_or_rest(session, up_id),
        _get_book_ws_or_rest(session, down_id),
        return_exceptions=True,
    )
    if isinstance(up_book, Exception):
        up_book = ms.get("upBook") or {"bids": [], "asks": []}
    if isinstance(down_book, Exception):
        down_book = ms.get("downBook") or {"bids": [], "asks": []}

    up_price, down_price, spot, klines = await asyncio.gather(
        _get_midpoint_ws_or_rest(session, up_id, up_book),
        _get_midpoint_ws_or_rest(session, down_id, down_book),
        fetch_spot_price(session, asset["binanceSymbol"]),
        fetch_klines(session, asset["binanceSymbol"], 60),
        return_exceptions=True,
    )
    # Spot/klines/midpoint requests can take hundreds of milliseconds. A newer
    # WS snapshot may have arrived meanwhile; do not overwrite it with the
    # books captured before those awaits.
    up_book = _latest_ws_book_or_fallback(up_id, up_book)
    down_book = _latest_ws_book_or_fallback(down_id, down_book)
    if up_book.get("bids") and up_book.get("asks"):
        up_price = (up_book["bids"][0]["price"] + up_book["asks"][0]["price"]) / 2
    if down_book.get("bids") and down_book.get("asks"):
        down_price = (down_book["bids"][0]["price"] + down_book["asks"][0]["price"]) / 2
    if not isinstance(up_price, Exception):   ms["upPrice"] = up_price
    if not isinstance(down_price, Exception): ms["downPrice"] = down_price
    ms["upBook"] = up_book
    ms["downBook"] = down_book
    if not isinstance(spot, Exception):
        # 現貨價優先採用 WS 即時報價（比 REST poll 新鮮很多），24h 漲跌%沒有 WS 來源，
        # 一律用 REST 這份——WS 斷線或還沒收到報價時，get_binance_ws_price 回傳 None，
        # 自動退回這輪 REST poll 到的價格。
        ws_price = get_binance_ws_price(asset["binanceSymbol"])
        ms["spotPrice"] = ws_price if ws_price is not None else spot["price"]
        ms["spotChangePct"] = spot["changePct"]
        if ms["windowOpenSpotPrice"] is None:
            ms["windowOpenSpotPrice"] = spot["price"]
    if not isinstance(klines, Exception) and klines:
        ms["klines"] = klines

    slug = ms["market"]["slug"]
    remaining_seconds = None if ms["windowEndsAt"] is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    fair = estimate_fair_up(aid)
    ms["fair"] = fair  # WS 觸發的即時評估（_on_ws_price_tick）沿用這份，不用每個 tick 都重算
    if aid == "btc" and _simulation_books_are_coherent(aid, ms["upBook"], ms["downBook"]):
        log_price_sum_diagnostic(
            f"sim-poll-{aid}", ms["upBook"], ms["downBook"],
            AB_VARIANT_BY_ID["btc-historical-hybrid"]["lockMaxSum"],
        )
    for variant_id, variant in AB_VARIANT_BY_ID.items():
        if variant["assetId"] == aid and _variant_books_are_coherent(
            aid, variant, ms["upBook"], ms["downBook"]
        ):
            simulate_trading(variant_id, slug, ms["upBook"], ms["downBook"], remaining_seconds, fair,
                             evaluation_source="poll")

    ms["connected"] = True
    persist_quote(aid, fair)

async def _fetch_one_asset_safe(session: aiohttp.ClientSession, asset: dict) -> None:
    try:
        await _fetch_one_asset(session, asset)
    except Exception as e:
        log.error(f"Fetch error [{asset['id']}]: {e}")
        markets_state[asset["id"]]["connected"] = False

# 量測到 CLOB API 的真實來回時間，給前端顯示用——單純 ping 量到的是最近的 Cloudflare
# 節點，量不出真正決定下單快慢的「轉送到 Polymarket 後端 + 處理 + 回傳」這段（實測過
# 兩者差到 10 倍以上，見對話紀錄），所以這裡量測的是一次真實 HTTP 請求。
#
# 2026-09：一開始讓這個量測共用 data_fetcher() 抓 7 個資產報價那個 session，結果量到
# 900ms 起跳，比同一台機器單獨測快了 30 倍以上——原因是那個 session 剛好在同一瞬間
# 有一大批（7 資產 × 好幾個端點）並發請求擠在同一個連線池排隊，量到的是「排在我們
# 自己那批請求後面的等待時間」，不是真實網路延遲，會誤導使用者以為連線變慢了。改成
# 用完全獨立的 session／連線池，只用來量這一件事，才不會被自己的其他流量污染。
_ping_state: dict = {"ms": None, "at": 0.0}


async def _measure_clob_ping(ping_session: aiohttp.ClientSession) -> None:
    t0 = time.monotonic()
    try:
        async with ping_session.get(CLOB_BASE + "/", timeout=aiohttp.ClientTimeout(total=5)) as r:
            await r.read()
        _ping_state["ms"] = (time.monotonic() - t0) * 1000
    except Exception:
        _ping_state["ms"] = None
    _ping_state["at"] = time.time()


async def data_fetcher():
    async with aiohttp.ClientSession() as session, aiohttp.ClientSession() as ping_session:
        while True:
            # 7 個資產平行抓，不要一個一個 await——依序抓的話單輪耗時會是 7 個資產的
            # 總和，很容易吃掉大半個 POLL_INTERVAL，導致晚抓到的資產報價明顯落後。
            await asyncio.gather(*(_fetch_one_asset_safe(session, asset) for asset in ASSETS))
            await _measure_clob_ping(ping_session)

            try:
                await retry_pending_settlements(session)  # 每輪都重試還沒結算成功的舊窗口（跨資產一起處理）
            except Exception as e:
                log.error(f"Settlement retry error: {e}")

            await asyncio.sleep(POLL_INTERVAL)

# ── WebSocket 廣播 ─────────────────────────────────────────────────────────

def build_ab_leaderboard() -> list:
    """每組 A/B 門檻的即時戰績，前端拿來畫比較表，一眼看出目前哪組門檻表現最好。"""
    rows = []
    for v in AB_VARIANTS:
        st = ab_states[v["id"]]
        cash, portfolio = compute_cash_and_portfolio(v["id"])
        win_rate = (st["wins"] / st["totalTrades"] * 100) if st["totalTrades"] else None
        rows.append({
            "id":            v["id"],
            "assetId":       v["assetId"],
            "label":         v["label"],
            "strategyType":  "rotation" if v.get("inventoryRotation") else (
                "maker" if v.get("marketMakerOnly") else "taker"
            ),
            "inventoryRotation": bool(v.get("inventoryRotation")),
            "liveMirrorOnly": bool(v.get("liveMirrorOnly")),
            "dumpThenHedge": bool(v.get("dumpThenHedge")),
            "entryMaxPrice": v["entryMaxPrice"],
            "lockMaxSum":    v["lockMaxSum"],
            "stakePct":      float(v.get("stakePct", shared_config["stakePct"])),
            "maxPairBudgetUsd": float(v.get("maxPairBudgetUsd", SIM_MAX_PAIR_BUDGET_USD)),
            "minCashReserveUsd": float(v.get("minCashReserveUsd", SIM_MIN_CASH_RESERVE_USD)),
            "minDepthMultiplier": float(v.get("minDepthMultiplier", 1.0)),
            "stabilitySeconds": float(v.get("stabilitySeconds", 0.0)),
            "totalPnl":      st["totalPnl"],
            "totalTrades":   st["totalTrades"],
            "wins":          st["wins"],
            "winRate":       win_rate,
            "cash":          cash,
            "portfolio":     portfolio,
            "hasPosition":   st["position"] is not None,
            "position":      st["position"],
            "pendingSettlements": len(st["pendingSettlements"]),
            "totalFees":     st.get("totalFees", 0.0),
            "lockedTrades":  st.get("lockedTrades", 0),
            "directionalTrades": st.get("directionalTrades", 0),
            "earlyExits":    st.get("earlyExits", 0),
            "maxDrawdown":   st.get("maxDrawdown", 0.0),
            "makerQuotes":   st.get("makerQuotes"),
            "makerStats":    _maker_stats(st) if v.get("marketMakerOnly") else None,
            "rotationStats": _rotation_stats(st) if v.get("inventoryRotation") else None,
            "sliceShares": v.get("sliceShares"),
            "minEntryEdge": v.get("minEntryEdge"),
            "maxGrossBudgetUsd": v.get("maxGrossBudgetUsd"),
            "maxResidualShares": v.get("maxResidualShares"),
            "hedgeOnlySeconds": v.get("hedgeOnlySeconds"),
            "actionCooldownSeconds": v.get("actionCooldownSeconds"),
            "residualRiskPremium": v.get("residualRiskPremium"),
            "futureHedgeFeeReserve": v.get("futureHedgeFeeReserve"),
            "requireChainlinkConfirm": bool(v.get("requireChainlinkConfirm")),
            "dumpHedgeStats": _dump_hedge_stats(st) if v.get("dumpThenHedge") else None,
            "lookbackSeconds": v.get("lookbackSeconds"),
            "minMovePct": v.get("minMovePct"),
            "entryWindowSeconds": v.get("entryWindowSeconds"),
            "targetShares": v.get("targetShares"),
            "minNetPerShare": v.get("minNetPerShare"),
            "windowDiagnostics": [decision_diag.public_summary(x)
                                  for x in st.get("windowDiagnostics", [])[:20]],
            "trades":        st["trades"],  # 這組自己的成交紀錄，前端獨立顯示，方便看個別下注金額
        })
    return rows

def build_asset_payload(asset_id: str) -> dict:
    """單一資產行情；策略戰績由 build_ab_leaderboard() 依 assetId 分流。"""
    ms = markets_state[asset_id]
    m = ms["market"] or {}
    remaining_seconds = None
    if ms["windowEndsAt"] is not None:
        remaining_seconds = max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    return {
        "market": {
            "slug":     m.get("slug"),
            "question": m.get("question"),
            "endDate":  m.get("endDate"),
        },
        "windowEndsAt":     ms["windowEndsAt"],
        "remainingSeconds": remaining_seconds,
        "upPrice":      ms["upPrice"],
        "downPrice":    ms["downPrice"],
        "upBook":       ms["upBook"],
        "downBook":     ms["downBook"],
        "spotPrice":     ms["spotPrice"],
        "chainlinkTwapPrice": ms.get("chainlinkTwapPrice"),
        "chainlinkTwapObservedAt": ms.get("chainlinkTwapObservedAt"),
        "windowOpenChainlinkTwapPrice": ms.get("windowOpenChainlinkTwapPrice"),
        "chainlinkTwapStatus": chainlink_twap_status(),
        "spotChangePct": ms["spotChangePct"],
        "klines":       ms.get("klines", []),
        "connected":    ms["connected"],
        "fair":         ms.get("fair"),
    }

def build_full_payload() -> str:
    return json.dumps({
        "type": "full",
        "serverTimeMs": real_now() * 1000,  # 校正過的真實時間，前端拿來顯示時鐘、不用本機系統時間
        "serverRegion": SERVER_REGION,
        "clobPingMs":   _ping_state["ms"],
        "assets":       {a["id"]: build_asset_payload(a["id"]) for a in ASSETS},
        "assetList":    [{"id": a["id"], "label": a["label"]} for a in ASSETS],
        "sharedConfig": {
            "startBalance": shared_config["startBalance"],
            "minBalance":   SIM_MIN_BALANCE,
            "stakePct":      shared_config["stakePct"],
            "minStakePct":   SIM_MIN_STAKE_PCT,
            "maxStakePct":   SIM_MAX_STAKE_PCT,
            "takerFeeRate":  SIM_TAKER_FEE_RATE,
            "slippageBps":   SIM_SLIPPAGE_BPS,
            "minEntryEdge":  SIM_MIN_ENTRY_EDGE,
            "minNetLockPerShare": SIM_MIN_NET_LOCK_PER_SHARE,
        },
        "abVariants":   build_ab_leaderboard(),
    })

async def broadcast(payload: str) -> None:
    dead = set()
    for client in list(CLIENTS):
        try:
            await client.send(payload)
        except Exception:
            dead.add(client)
    CLIENTS.difference_update(dead)

async def broadcast_loop():
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        if CLIENTS:
            await broadcast(build_full_payload())

async def ws_handler(websocket):
    CLIENTS.add(websocket)
    log.info(f"Dashboard 已連接（共 {len(CLIENTS)} 個客戶端）")
    try:
        await websocket.send(build_full_payload())
    except Exception:
        pass
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("type") == "configure":
                if "stakePct" in msg:
                    set_stake_pct(msg["stakePct"])
                if "startBalance" in msg:
                    reset_with_balance(msg["startBalance"])
                await broadcast(build_full_payload())  # 立刻推播最新設定，不用等下一輪
    finally:
        CLIENTS.discard(websocket)
        log.info(f"Dashboard 已斷線（剩 {len(CLIENTS)} 個客戶端）")

# ── 主程式 ────────────────────────────────────────────────────────────────

async def main():
    _get_sim_db()
    load_sim_state()
    log.info("=" * 50)
    log.info("  Polymarket BTC Up/Down · Real-Time Data Bridge")
    log.info(f"  WebSocket: ws://{HOST}:{PORT}")
    log.info("  數據來源: Polymarket 公開 API（Gamma + CLOB）")
    log.info(f"  模擬記錄: {SIM_DB_PATH}")
    log.info("  成交假設: Ask/Bid 深度 VWAP + Taker fee + 不利滑點")
    log.info("  開啟 web/polymarket.html 查看即時數據")
    if WITH_LIVE:
        log.info("  ⚠ --with-live 已啟用：會在這個進程裡跑真實下單邏輯（仍受 .env 雙開關控制）")
    log.info("=" * 50)

    tasks = [data_fetcher(), broadcast_loop(), market_ws_loop(), binance_ws_loop()]
    if any(asset.get("binanceSymbol") == "BTCUSDT" for asset in ASSETS):
        tasks.append(chainlink_twap_loop())
    if WITH_LIVE:
        import polymarket_live_strategy as live_strategy
        tasks.append(live_strategy.run_embedded())

    async with serve(ws_handler, HOST, PORT):
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
