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
PORT = 8766             # Polymarket 紙上模擬 Dashboard
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
SIM_MIN_SIGMA_PER_SECOND    = {"btc": 0.000025, "btc-15m": 0.000025, "btc-4h": 0.000025}
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

# ── 晚進場方向性策略（"late-direction" 變體專用）──────────────────────────
# 對齊公開資料裡「T-10 秒、window delta」那套做法：不是在窗口一開始就靠模型優勢
# 賭單邊（那條退路驗證下來是 0% 勝率，2026-09 起已對其他變體關閉），而是等到窗口
# 快結束、現價已經明顯偏離「這個窗口開盤時的價格」——這時已經沒什麼時間反轉，
# 訊號的確定性遠比窗口剛開盤時高很多。
LATE_DIRECTION_WINDOW_SECONDS      = 10.0  # 只在剩不到這個秒數才考慮晚進場
LATE_DIRECTION_MIN_ENTRY_REMAINING = 3.0   # 剩不到這個秒數就別進了，怕來不及成交
LATE_DIRECTION_MIN_DELTA_PCT       = 0.02  # 現價相對開盤價至少要偏移這個百分比（公開資料裡的「強訊號」門檻）

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIM_DB_PATH = os.path.join(BASE_DIR, "polymarket_sim.sqlite3")

# ── 追蹤的資產：Polymarket 目前總共有 7 個「5 分鐘漲跌」市場（直接對 Gamma API
#    逐一探測 slug 驗證過，其餘主流幣如 ADA/AVAX/LINK/DOT 都沒有對應市場，不是列表不全），
#    但 2026-09 起模擬盤只保留 BTC——BTC 是唯一累積夠樣本數的（37 筆、97.3% 勝率），
#    其餘幾個樣本太少（1~6 筆）沒有參考價值，先專注在 BTC，其餘之後有需要再加回來。
#    同一個資產也可以同時追蹤不同窗口長度——探測過 BTC 除了 5m 之外還有 15m／4h
#    的漲跌市場（1m/1h/1d 沒有），且訂單簿深度明顯比 5m 深很多（15m ~55k 股、
#    4h ~23k 股，5m 通常只有幾十到幾百股），值得先加進模擬盤觀察是否值得接進實盤。
#    id 要唯一（拿來當 markets_state／AB_VARIANTS 的 key），windowSeconds 沒填的話
#    預設是 WINDOW_SECONDS（5 分鐘）。
ASSETS = [
    {"id": "btc",     "label": "BTC",      "slugPrefix": "btc-updown-5m-",  "binanceSymbol": "BTCUSDT", "windowSeconds": 300},
    {"id": "btc-15m", "label": "BTC 15m",  "slugPrefix": "btc-updown-15m-", "binanceSymbol": "BTCUSDT", "windowSeconds": 900},
    {"id": "btc-4h",  "label": "BTC 4h",   "slugPrefix": "btc-updown-4h-",  "binanceSymbol": "BTCUSDT", "windowSeconds": 14400},
]

# ── A/B 門檻測試：每個資產各自跑同一套四組門檻設定，彼此獨立記帳，方便直接比較
#    「同一套策略邏輯放到不同資產上，表現差多少」。variant id 格式是
#    "<資產id>-<組別>"（例如 "btc-main"），"<資產>-main" 這組固定對應
#    SIM_ENTRY_MAX_PRICE / SIM_LOCK_MAX_SUM。"<資產>-late-direction" 是實驗組：
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
    for _cfg in _VARIANT_CONFIGS:
        AB_VARIANTS.append({
            "id":            f"{_asset['id']}-{_cfg['key']}",
            "assetId":       _asset["id"],
            "label":         f"{_asset['label']} {_cfg['labelSuffix']}",
            "entryMaxPrice": _cfg["entryMaxPrice"],
            "lockMaxSum":    _cfg["lockMaxSum"],
        })
    AB_VARIANTS.append({
        "id":                    f"{_asset['id']}-late-direction",
        "assetId":               _asset["id"],
        "label":                 f"{_asset['label']} 晚進場方向性（T-10s）",
        "entryMaxPrice":         None,
        "lockMaxSum":            SIM_LOCK_MAX_SUM,
        "lateDirectionOnly":     True,
        "lateDirectionMaxPrice": 0.92,
    })
del _asset, _cfg
AB_VARIANT_BY_ID = {v["id"]: v for v in AB_VARIANTS}

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
state = markets_state["btc"]  # 向後相容：既有程式碼引用 state 的地方，等同於 BTC 這份

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
    }

ab_states = {v["id"]: _new_variant_state() for v in AB_VARIANTS}

# 下注比例／起始資產是所有 A/B 組共用的設定，刻意保持一致，
# 這樣比較結果的差異只來自「進場/鎖利門檻」本身，不會被其他變因干擾。
shared_config = {
    "stakePct":     SIM_DEFAULT_STAKE_PCT,
    "startBalance": SIM_DEFAULT_BALANCE,
    "runId":        int(time.time() * 1000),
}

sim_state = ab_states["btc-main"]  # 向後相容：既有程式碼引用 sim_state 的地方，等同於 "main" 這組

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
    global sim_state
    sim_state = ab_states["btc-main"]
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
    cost = float(pos.get("entryNotional", pos["shares"] * pos["entryPrice"])) + float(pos.get("entryFee", 0))
    if pos.get("hedged"):
        cost += float(pos.get("hedgeNotional", pos.get("hedgeShares", 0) * (pos.get("hedgePrice") or 0)))
        cost += float(pos.get("hedgeFee", 0))
    return cost


def _position_decision_cost(pos: dict) -> float:
    """回傳建立部位時的保守最差成本，用來決定後續是否真的能鎖利。"""
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
            CREATE INDEX IF NOT EXISTS idx_sim_quotes_asset_ts ON sim_quotes(asset_id, ts);
            CREATE INDEX IF NOT EXISTS idx_sim_trades_variant_time ON sim_trades(variant_id, exit_time);
            """
        )
        _sim_db.commit()
    return _sim_db


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
    sim_state = ab_states["btc-main"]


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
    _get_sim_db().execute(
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
    _get_sim_db().commit()

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
        fair = estimate_fair_up(aid)
        if not fair:
            continue
        ms["fair"] = fair
        slug = ms["market"]["slug"]
        remaining_seconds = (
            None if ms["windowEndsAt"] is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
        )
        up_book, down_book = ms.get("upBook"), ms.get("downBook")
        if up_book and down_book:
            for variant_id, variant in AB_VARIANT_BY_ID.items():
                if variant["assetId"] == aid:
                    simulate_trading(
                        variant_id, slug, up_book, down_book, remaining_seconds, fair,
                        allow_early_exit=False,
                    )


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
    return target_pair_order(cash, shared_config["stakePct"], variant["lockMaxSum"])


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


# ── 波動速度防護（volatility guard，2026-09）──────────────────────────────
# 真實案例：兩腿平行送出，一腿意外用好價格成交（15.6 股 @ $0.10），另一腿沒接到；
# 重試時發現對邊價格在不到 1 秒內從 $0.68 衝到 $0.97，代表行情正在劇烈翻轉；緊急
# 平倉連兩次都因為完全沒有 Bid 可賣而失敗（不是價格不夠好，是那個瞬間根本沒有對手
# 盤），部位被迫抱到結算，虧光整筆本金。重試/緊急平倉的讓價、重試機制都無法解決
# 「瞬間流動性真空」這種情況——與其冒險進場、賭運氣不會撞上這種瞬間，不如在偵測到
# 最近報價正在劇烈波動時，直接跳過這次鎖利進場機會。
PRICE_VELOCITY_WINDOW_SECONDS = 2.0  # 抓最近這麼多秒的報價變化來判斷是不是在劇烈波動
PRICE_VELOCITY_MAX_MOVE       = 0.05  # 這段時間內任一邊最佳賣價變動超過這個值，視為劇烈波動
_ask_price_history: dict = {}  # asset_id -> {"up": deque[(monotonic_ts, price)], "down": deque[...]}


def velocity_guard_tripped(asset_id: str, up_book: dict, down_book: dict) -> bool:
    """回傳 True 代表最近 PRICE_VELOCITY_WINDOW_SECONDS 秒內，Up 或 Down 任一邊的
    最佳賣價變動超過 PRICE_VELOCITY_MAX_MOVE——這種時候先跳過鎖利進場，不要冒險。
    這裡只記錄／檢查，不會影響已經持有的部位（緊急平倉／補鎖利邏輯不受這個防護
    限制，那些是已經在場上要盡快處理，不是要不要新進場的問題）。"""
    now = time.monotonic()
    hist = _ask_price_history.setdefault(asset_id, {"up": deque(), "down": deque()})
    tripped = False
    for label, book in (("up", up_book), ("down", down_book)):
        asks = book.get("asks") or []
        ask = float(asks[0]["price"]) if asks else None
        dq = hist[label]
        if ask is not None:
            dq.append((now, ask))
        cutoff = now - PRICE_VELOCITY_WINDOW_SECONDS
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        if len(dq) >= 2:
            prices = [p for _, p in dq]
            if max(prices) - min(prices) > PRICE_VELOCITY_MAX_MOVE:
                tripped = True
    return tripped


def _try_direct_pair(variant_id: str, slug: str, up_book: dict, down_book: dict) -> bool:
    """先檢查兩腿此刻是否可直接成交並鎖住淨利，這才是進場即無方向曝險的套利。

    股數會先按可見深度的 SIM_DEPTH_CAP_FRACTION（見該常數註解）封頂——超過這個比例
    會開始明顯吃掉自己的成交價，模擬出來的利潤會比實際能拿到的樂觀，也更容易在
    真實下單時因為深度已經被搶走而被拒。封頂之後如果連目標股數都吃不滿，才照原本
    邏輯整筆視為不可行（simulate_buy_fill 深度不足回傳 None）。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    if velocity_guard_tripped(variant["assetId"], up_book, down_book):
        return False
    shares, budget = _target_order_size(variant_id)
    if shares <= 0:
        return False
    up_depth = sum(float(a.get("size", 0)) for a in (up_book.get("asks") or []))
    down_depth = sum(float(a.get("size", 0)) for a in (down_book.get("asks") or []))
    depth_cap = min(up_depth, down_depth) * SIM_DEPTH_CAP_FRACTION
    shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
    if shares <= 0:
        return False
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
        return False
    price_sum = up_fill["decisionPrice"] + down_fill["decisionPrice"]
    total_decision_cost = (
        up_fill["decisionNotional"] + up_fill["decisionFee"]
        + down_fill["decisionNotional"] + down_fill["decisionFee"]
    )
    net_per_share = (shares - total_decision_cost) / shares
    cash, _ = compute_cash_and_portfolio(variant_id)
    if (
        price_sum > variant["lockMaxSum"]
        or net_per_share < SIM_MIN_NET_LOCK_PER_SHARE
        or total_decision_cost > cash
    ):
        return False
    # 深度封頂後實際成交金額可能比原本算的目標預算小，記錄實際花費的金額，
    # 不要留著封頂前那個沒用到的數字。
    budget = up_fill["notional"] + up_fill["fee"] + down_fill["notional"] + down_fill["fee"]
    enter_position(variant_id, slug, "Up", up_fill, budget, None, None)
    hedge_position(variant_id, "Down", down_fill)
    return True


def _try_late_direction_entry(
    variant_id: str, slug: str, up_book: dict, down_book: dict, remaining_seconds: float
) -> None:
    """晚進場方向性策略：只在窗口快結束、現價已經明顯偏離開盤價時才賭方向。

    跟其他變體「一開窗口就靠模型優勢賭單邊」完全相反——那條路統計下來是 0% 勝率
    （鎖不到對沖，往往正代表市場對另一邊已有強烈共識，而共識通常是對的）。
    這裡反過來利用同一個道理：等到只剩最後幾秒，現價相對開盤價的偏移還沒完全
    反映在賠率上，且已經沒什麼時間反轉，這時候的方向性判斷確定性才夠高。
    """
    if remaining_seconds > LATE_DIRECTION_WINDOW_SECONDS or remaining_seconds < LATE_DIRECTION_MIN_ENTRY_REMAINING:
        return
    variant = AB_VARIANT_BY_ID[variant_id]
    ms = markets_state[variant["assetId"]]
    open_price = ms.get("windowOpenSpotPrice")
    spot = ms.get("spotPrice")
    if not open_price or not spot or open_price <= 0:
        return
    delta_pct = (spot - open_price) / open_price * 100
    if abs(delta_pct) < LATE_DIRECTION_MIN_DELTA_PCT:
        return
    side, book = ("Up", up_book) if delta_pct > 0 else ("Down", down_book)
    shares, budget = _target_order_size(variant_id)
    if shares <= 0 or budget < SIM_MIN_ORDER_NOTIONAL_USD:
        return
    fill = simulate_buy_fill(book, shares)
    if not fill or fill["decisionNotional"] < SIM_MIN_ORDER_NOTIONAL_USD:
        return
    # 真正的下限是「股數」不是金額：查證過真實 API 回傳的 minOrderSize 是 5 股，不是 $5。
    if fill["shares"] < float(book.get("minOrderSize", 1) or 1):
        return
    if fill["decisionPrice"] > variant["lateDirectionMaxPrice"]:
        return
    enter_position(variant_id, slug, side, fill, budget, None, None)
    log.info(
        f"[SIM:{variant_id}] 晚進場方向性 {side} Δ={delta_pct:+.3f}% "
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


def simulate_trading(
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

    if pos is None:
        if remaining_seconds is None or remaining_seconds <= 0:
            return
        if _try_direct_pair(variant_id, slug, up_book, down_book):
            return
        if variant.get("lateDirectionOnly"):
            _try_late_direction_entry(variant_id, slug, up_book, down_book, remaining_seconds)
        # 其餘變體：找不到能立即鎖住兩邊的機會就空手，不退而求其次先賭單邊留下方向性
        # 曝險——這條退路統計下來歷史勝率是 0%（42 戰 0 勝、-$364.75），關閉／重開過
        # 幾次，2026-09 確認維持關閉。核心策略就是「兩邊都買才進場」，找不到就不進場。
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
        if current.get("hedged"):
            market_value += current["shares"]
        else:
            held_book = ms["upBook"] if current["side"] == "Up" else ms["downBook"]
            liquidation = simulate_sell_fill(held_book, current["shares"])
            if liquidation:
                market_value += liquidation["notional"] - liquidation["fee"]

    for pending in st["pendingSettlements"]:
        # 完整配對一定可收回每股 $1；單邊倉在未知結果期間保守估值為 0，絕不提前釋放本金。
        if pending.get("hedged"):
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
    fees = float(pos.get("entryFee", 0)) + float(pos.get("hedgeFee", 0)) + float(pos.get("exitFee", 0))
    trade_type = "locked" if pos.get("hedged") else ("early_exit" if outcome == "EarlyExit" else "directional")
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
        "tradeType":    trade_type,
        "outcome":      outcome,
        "fees":         fees,
        "grossPnl":     pnl + fees,
        "pnl":          pnl,
        "entryTime":    pos["entryTime"],
        "exitTime":     time.time(),
    }
    st["trades"].insert(0, trade)
    st["trades"] = st["trades"][:50]
    st["totalPnl"] += pnl
    st["totalFees"] += fees
    st["totalTrades"] += 1
    if trade_type == "locked":
        st["lockedTrades"] += 1
    elif trade_type == "early_exit":
        st["earlyExits"] += 1
    else:
        st["directionalTrades"] += 1
    if pnl > 0:
        st["wins"] += 1
    persist_trade(variant_id, trade)
    save_sim_state()


def _settle_pnl(pos: dict, outcome: str) -> float:
    payout = pos["shares"] if pos.get("hedged") or outcome == pos["side"] else 0.0
    return payout - _position_paid_cost(pos)


async def retry_pending_settlements(session: aiohttp.ClientSession) -> None:
    """窗口結束當下，Polymarket 的結算（Chainlink 資料源）通常還沒跑完，查不到結果。
    查不到不代表沒發生，是還沒好——把它放進待結算佇列，之後每一輪都重試，
    直到真的查到結果為止，不會因為第一次查不到就把這筆損益憑空丟掉。
    對每一組 A/B 都各自重試，互不影響。
    """
    for variant_id, st in ab_states.items():
        if not st["pendingSettlements"]:
            continue
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
    save_sim_state()

def queue_settlement(slug: str) -> None:
    """窗口換了：每一組 A/B 如果上一個窗口還有沒結算的倉位，各自丟進自己的待結算佇列，
    換一個乾淨的位置開始追蹤新窗口。"""
    for st in ab_states.values():
        pos = st["position"]
        if pos is not None and pos["windowSlug"] == slug:
            st["pendingSettlements"].append(pos)
            st["position"] = None
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
    }


def _notify_ws_price_listeners(token_id: str) -> None:
    for callback in tuple(_ws_price_listeners):
        try:
            result = callback(token_id)
            if asyncio.iscoroutine(result):
                asyncio.get_running_loop().create_task(result)
        except Exception as exc:
            log.exception(f"[WS] price listener failed for token={token_id}: {exc}")


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


def _mm_record_trade(payload: dict) -> None:
    tid = payload.get("tokenId") or payload.get("asset_id")
    if not tid:
        return
    try:
        trade = {
            "ts": float(payload.get("timestamp", 0)) / 1000.0,
            "price": float(payload["price"]),
            "size": float(payload["size"]),
            "side": str(payload.get("side", "")).upper(),
        }
    except (KeyError, ValueError, TypeError):
        return
    _mm_trades.setdefault(tid, deque(maxlen=MM_TRADE_HISTORY_LIMIT)).append(trade)


def _mm_maybe_log_summary() -> None:
    """每隔一段時間印一次觀察摘要，不用真的去看程式碼或另外開頁面就能追蹤。"""
    global _mm_last_summary_at
    now = time.time()
    if now - _mm_last_summary_at < MM_SUMMARY_INTERVAL:
        return
    _mm_last_summary_at = now
    for ms in markets_state.values():
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
                log.info(f"[MM觀察:{side_label}] 過去{MM_SUMMARY_INTERVAL:.0f}秒無成交　目前價差={spread_txt}")
                continue
            total_size = sum(t["size"] for t in recent)
            avg_size = total_size / len(recent)
            log.info(
                f"[MM觀察:{side_label}] 過去{MM_SUMMARY_INTERVAL:.0f}秒 成交{len(recent)}筆　"
                f"總量={total_size:.1f}股　平均單筆={avg_size:.1f}股　目前價差={spread_txt}"
            )


def _ws_apply_message(msg: dict) -> None:
    """套用一則 WS 訊息、更新本地訂單簿；有真的變動到的 token 會立刻觸發一次評估
    （_on_ws_price_tick），不等下一個 3 秒輪詢——這就是補齊「文章那種即時反應速度」
    的關鍵：真正無風險套利的瞬間往往很短暫，等輪詢常常已經來不及。
    event_type 可能在最外層（book/price_change 實測過是這樣），也可能包在
    type + payload 裡（文件上 last_trade_price 是這樣）——兩種都接。"""
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
        _on_ws_price_tick(tid)
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
                _on_ws_price_tick(tid)
    elif event_type == "last_trade_price":
        _mm_record_trade(payload)
    _mm_maybe_log_summary()


def _on_ws_price_tick(token_id: str) -> None:
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
                log_price_sum_diagnostic(f"sim-ws-{aid}", up_book, down_book, SIM_LOCK_MAX_SUM)
            for variant_id, variant in AB_VARIANT_BY_ID.items():
                if variant["assetId"] == aid:
                    simulate_trading(
                        variant_id, slug, up_book, down_book, remaining_seconds, fair,
                        allow_early_exit=False,
                    )
            break
    _notify_ws_price_listeners(token_id)


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
            log.warning(f"[WS] 市場資料流斷線，準備重連：{e}")
        _ws_connected = False
        _ws_conn = None
        _ws_subscribed_tokens = set()
        _ws_snapshot_tokens = set()
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
    if aid == "btc":
        log_price_sum_diagnostic(f"sim-poll-{aid}", ms["upBook"], ms["downBook"], SIM_LOCK_MAX_SUM)
    for variant_id, variant in AB_VARIANT_BY_ID.items():
        if variant["assetId"] == aid:
            simulate_trading(variant_id, slug, ms["upBook"], ms["downBook"], remaining_seconds, fair)

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
            "entryMaxPrice": v["entryMaxPrice"],
            "lockMaxSum":    v["lockMaxSum"],
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
            "trades":        st["trades"],  # 這組自己的成交紀錄，前端獨立顯示，方便看個別下注金額
        })
    return rows

def build_asset_payload(asset_id: str) -> dict:
    """單一資產的市場行情。策略戰績不在這裡——每個資產各自 4 組（conservative/main/
    loose/late-direction），都在 build_ab_leaderboard() 裡用 "<資產id>-<組別>" 的 id
    區分，前端依網址參數 ?asset= 篩選要看哪個資產的那 4 組。"""
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
    if WITH_LIVE:
        import polymarket_live_strategy as live_strategy
        tasks.append(live_strategy.run_embedded())

    async with serve(ws_handler, HOST, PORT):
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
