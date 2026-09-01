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
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from email.utils import parsedate_to_datetime
from statistics import NormalDist, pstdev

import aiohttp
import websockets
from websockets.server import serve

# ── 設定 ──────────────────────────────────────────────────────────────────
HOST = "localhost"
PORT = 8766             # Polymarket 紙上模擬 Dashboard
POLL_INTERVAL = 3       # 報價輪詢間隔（秒）－ Polymarket 沒有強制要求 WebSocket，輪詢就綽綽有餘

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket")

# ── 模擬策略設定（紙上交易）────────────────────────────────────────────────
SIM_ENTRY_MAX_PRICE   = 0.40   # 主要策略：只有價格 <= 這個門檻才考慮先進場一邊
SIM_LOCK_MAX_SUM      = 0.90   # 主要策略：兩邊最差可成交限價 <= 門檻，且扣費用後達最低淨利才配對
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
SIM_EXIT_EDGE               = 0.02  # 市場可賣價高於模型持有價值 2¢/股時提早退出
SIM_MIN_ENTRY_REMAINING     = 90.0  # 距離結算不足 90 秒，不再開新的單邊部位
SIM_FAIR_MODEL_WEIGHT       = 0.65  # Binance 波動模型權重；其餘使用市場隱含機率校準
SIM_MIN_SIGMA_PER_SECOND    = {"btc": 0.000025}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIM_DB_PATH = os.path.join(BASE_DIR, "polymarket_sim.sqlite3")

# ── 追蹤的資產：只留 BTC。ETH 已經移除——5 分鐘短線盤本來就要專注在單一資產上，
#    多資產只是分散注意力，沒有實際帶來額外的套利機會。
ASSETS = [
    {"id": "btc", "label": "BTC", "slugPrefix": "btc-updown-5m-", "binanceSymbol": "BTCUSDT"},
]

# ── A/B 門檻測試：同時跑好幾組不同的進場/鎖利門檻，吃同一份真實報價，
#    彼此獨立記帳，方便直接比較哪組門檻的實際表現比較好（而不是憑感覺猜）。
#    "main" 這組固定對應 SIM_ENTRY_MAX_PRICE / SIM_LOCK_MAX_SUM，
#    也是既有前端面板（部位卡片、成交紀錄列表）顯示的那一組，維持向後相容。
#    "pure-arb" 這組刻意不設單邊進場門檻（entryMaxPrice=None，邏輯上也用不到）——
#    只做 _try_direct_pair 那條「當下兩邊能同時買、直接鎖住利潤」的真無風險套利，
#    找不到機會就空手，絕對不會退而求其次先賭單邊、留下方向性曝險。lockMaxSum
#    對齊 cnyes 那篇報導講的公開研究參數：合計價格門檻 <= $0.95（也就是至少
#    $0.05 原始價差，報導裡說這是「用於覆蓋執行滑點」的最低利潤門檻），
#    比原本自己抓的 0.99 更嚴格，且 _try_direct_pair 現在也對齊了報導提到的
#    另一個風控原則——單筆倉位封頂在可見深度的 50%，不會假設能吃光整本。
AB_VARIANTS = [
    {"id": "conservative", "assetId": "btc", "label": "BTC 保守 0.30/0.85", "entryMaxPrice": 0.30, "lockMaxSum": 0.85},
    {"id": "main",         "assetId": "btc", "label": "BTC 目前 0.40/0.90", "entryMaxPrice": SIM_ENTRY_MAX_PRICE, "lockMaxSum": SIM_LOCK_MAX_SUM},
    {"id": "loose",        "assetId": "btc", "label": "BTC 寬鬆 0.45/0.95", "entryMaxPrice": 0.45, "lockMaxSum": 0.95},
    {"id": "pure-arb",     "assetId": "btc", "label": "BTC 純套利（只做同時雙邊）", "entryMaxPrice": None, "lockMaxSum": 0.95, "pureArbOnly": True},
]
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

sim_state = ab_states["main"]  # 向後相容：既有程式碼引用 sim_state 的地方，等同於 "main" 這組

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
    sim_state = ab_states["main"]
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
    rounding = ROUND_UP if side.upper() == "BUY" else ROUND_DOWN
    units = (raw / tick).to_integral_value(rounding=rounding)
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
    sim_state = ab_states["main"]


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

WINDOW_SECONDS = 300  # 5 分鐘一個窗口

async def fetch_active_market(session: aiohttp.ClientSession, slug_prefix: str) -> dict | None:
    """直接用真實時間算出目前這個 5 分鐘窗口的 slug 去查，不掃描 Gamma 的市場列表。

    原本用 active=true&closed=false 篩選、依 startDate 排序去找，結果發現不可靠：
    Polymarket 會把未來一整天的窗口都預先建好（全部也是 active=true），
    也有很多從很久以前就從沒被正確標記 closed 的舊窗口卡在列表裡，
    不管排序方向，抓到的都不是「現在正在進行」的那一個。
    直接用時間算 slug（格式：<slug_prefix><窗口開始時間的 unix 秒>）最準，
    這套邏輯跟資產無關，只是帶入的 slug_prefix 不同。
    """
    window_start = int(real_now() // WINDOW_SECONDS) * WINDOW_SECONDS
    for start in (window_start, window_start - WINDOW_SECONDS):  # 抓不到當前窗口就退回上一個（剛好在交界處時的備援）
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


def _entry_candidate(side: str, book: dict, shares: float, fair_probability: float, max_price: float) -> dict | None:
    # 跟真實版一樣不先按深度縮小股數：深度不夠 simulate_buy_fill 會直接回傳 None（等同 FOK 未成交）。
    fill = simulate_buy_fill(book, shares)
    if not fill or fill["decisionNotional"] < 1.0 or fill["decisionPrice"] > max_price:
        return None
    all_in_per_share = (fill["decisionNotional"] + fill["decisionFee"]) / fill["shares"]
    edge = fair_probability - all_in_per_share
    if edge < SIM_MIN_ENTRY_EDGE:
        return None
    return {"side": side, "fill": fill, "fair": fair_probability, "edge": edge}


def _try_direct_pair(variant_id: str, slug: str, up_book: dict, down_book: dict) -> bool:
    """先檢查兩腿此刻是否可直接成交並鎖住淨利，這才是進場即無方向曝險的套利。

    股數會先按可見深度的 50% 封頂（跟公開研究裡「倉位上限＝訂單簿深度的 50%」
    這個風控原則對齊——超過這個比例會開始明顯吃掉自己的成交價，模擬出來的
    利潤會比實際能拿到的樂觀）。封頂之後如果連目標股數都吃不滿，
    才照原本邏輯整筆視為不可行（simulate_buy_fill 深度不足回傳 None）。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    shares, budget = _target_order_size(variant_id)
    if shares <= 0:
        return False
    up_depth = sum(float(a.get("size", 0)) for a in (up_book.get("asks") or []))
    down_depth = sum(float(a.get("size", 0)) for a in (down_book.get("asks") or []))
    depth_cap = min(up_depth, down_depth) * 0.5
    shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
    if shares <= 0:
        return False
    up_fill = simulate_buy_fill(up_book, shares)
    down_fill = simulate_buy_fill(down_book, shares)
    if not up_fill or not down_fill or up_fill["notional"] < 1 or down_fill["notional"] < 1:
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
        if remaining_seconds is None or remaining_seconds < SIM_MIN_ENTRY_REMAINING:
            return
        if _try_direct_pair(variant_id, slug, up_book, down_book):
            return
        if variant.get("pureArbOnly"):
            # 純套利模式：找不到當下能同時鎖住的機會就不進場，寧可空手，
            # 不退而求其次先賭單邊——這組存在的目的就是完全不承擔方向性風險。
            return
        if not fair:
            return
        target_shares, budget = _target_order_size(variant_id)
        if target_shares <= 0 or budget < 1:
            return
        candidates = [
            _entry_candidate("Up", up_book, target_shares, fair["fairUp"], variant["entryMaxPrice"]),
            _entry_candidate("Down", down_book, target_shares, fair["fairDown"], variant["entryMaxPrice"]),
        ]
        candidates = [c for c in candidates if c]
        if candidates:
            best = max(candidates, key=lambda c: c["edge"])
            enter_position(variant_id, slug, best["side"], best["fill"], budget, best["fair"], best["edge"])
        return

    if pos["hedged"] or pos["windowSlug"] != slug:
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
_ws_wanted_tokens: set = set()    # 這一輪視窗真正需要的 token（隨視窗替換而更新）
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


async def _ws_set_wanted_tokens(token_ids: set) -> None:
    """視窗換了、要追蹤的 token 也跟著換——如果 WS 目前是連線狀態就直接送訂閱/取消訂閱，
    不然只更新「想要的清單」，等連線建立/重連時會整批依這份清單訂閱。"""
    global _ws_wanted_tokens
    _ws_wanted_tokens = set(token_ids)
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
    new_market = await fetch_active_market(session, asset["slugPrefix"])

    if new_market and (cur is None or new_market["slug"] != cur["slug"]):
        if cur is not None:
            queue_settlement(cur["slug"])
        ms["market"] = new_market
        ms["windowEndsAt"] = _iso_to_ms(new_market["endDate"])
        log.info(f"[MARKET:{aid}] 切換到新窗口 {new_market['slug']}　結束於 {new_market['endDate']}")

    if not ms["market"]:
        return
    up_id, down_id = _market_tokens(ms["market"])
    if not (up_id and down_id):
        return
    ms["upTokenId"], ms["downTokenId"] = up_id, down_id

    # 每輪都同步一次「想要的 token」（函式內部自己 diff，沒變就不會真的送訂閱訊息）；
    # 順便確保 tick size / 最低股數已經查過，WS 才有資料可以馬上用。
    await _ws_set_wanted_tokens({up_id, down_id})
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
        ms["spotPrice"] = spot["price"]
        ms["spotChangePct"] = spot["changePct"]
    if not isinstance(klines, Exception) and klines:
        ms["klines"] = klines

    slug = ms["market"]["slug"]
    remaining_seconds = None if ms["windowEndsAt"] is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    fair = estimate_fair_up(aid)
    ms["fair"] = fair  # WS 觸發的即時評估（_on_ws_price_tick）沿用這份，不用每個 tick 都重算
    for variant_id, variant in AB_VARIANT_BY_ID.items():
        if variant["assetId"] == aid:
            simulate_trading(variant_id, slug, ms["upBook"], ms["downBook"], remaining_seconds, fair)

    ms["connected"] = True
    persist_quote(aid, fair)

async def data_fetcher():
    async with aiohttp.ClientSession() as session:
        while True:
            for asset in ASSETS:
                try:
                    await _fetch_one_asset(session, asset)
                except Exception as e:
                    log.error(f"Fetch error [{asset['id']}]: {e}")
                    markets_state[asset["id"]]["connected"] = False

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
            "totalFees":     st.get("totalFees", 0.0),
            "lockedTrades":  st.get("lockedTrades", 0),
            "directionalTrades": st.get("directionalTrades", 0),
            "earlyExits":    st.get("earlyExits", 0),
            "maxDrawdown":   st.get("maxDrawdown", 0.0),
            "trades":        st["trades"],  # 這組自己的成交紀錄，前端獨立顯示，方便看個別下注金額
        })
    return rows

def build_asset_summary(asset_id: str, variant_id: str) -> dict:
    """單一資產的市場行情 + 對應策略戰績。"""
    ms = markets_state[asset_id]
    st = ab_states[variant_id]
    m = ms["market"] or {}
    remaining_seconds = None
    if ms["windowEndsAt"] is not None:
        remaining_seconds = max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    cash, portfolio = compute_cash_and_portfolio(variant_id)
    variant = AB_VARIANT_BY_ID[variant_id]
    fair = estimate_fair_up(asset_id)
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
        "fair":         fair,
        "sim": {
            "position":    st["position"],
            "pendingSettlements": len(st["pendingSettlements"]),
            "trades":      st["trades"],
            "totalPnl":    st["totalPnl"],
            "totalTrades": st["totalTrades"],
            "wins":        st["wins"],
            "totalFees":   st.get("totalFees", 0.0),
            "lockedTrades": st.get("lockedTrades", 0),
            "directionalTrades": st.get("directionalTrades", 0),
            "earlyExits":  st.get("earlyExits", 0),
            "maxDrawdown": st.get("maxDrawdown", 0.0),
            "startBalance": shared_config["startBalance"],
            "minBalance":   SIM_MIN_BALANCE,
            "cash":         cash,
            "portfolio":    portfolio,
            "entryMaxPrice": variant["entryMaxPrice"],
            "lockMaxSum":    variant["lockMaxSum"],
            "stakePct":      shared_config["stakePct"],
            "stakeUsd":      min(cash, portfolio * (shared_config["stakePct"] / 100)),
            "minStakePct":   SIM_MIN_STAKE_PCT,
            "maxStakePct":   SIM_MAX_STAKE_PCT,
            "takerFeeRate":  SIM_TAKER_FEE_RATE,
            "slippageBps":   SIM_SLIPPAGE_BPS,
            "minEntryEdge":  SIM_MIN_ENTRY_EDGE,
            "minNetLockPerShare": SIM_MIN_NET_LOCK_PER_SHARE,
            "minEntryRemaining": SIM_MIN_ENTRY_REMAINING,
        },
    }

def build_full_payload() -> str:
    btc = build_asset_summary("btc", "main")
    return json.dumps({
        "type": "full",
        "serverTimeMs": real_now() * 1000,  # 校正過的真實時間，前端拿來顯示時鐘、不用本機系統時間
        # 以下這幾個頂層欄位維持跟原本一樣（等於 BTC），是既有前端面板在讀的，向後相容不動它們
        "market":           btc["market"],
        "windowEndsAt":     btc["windowEndsAt"],
        "remainingSeconds": btc["remainingSeconds"],
        "upPrice":      btc["upPrice"],
        "downPrice":    btc["downPrice"],
        "upBook":       btc["upBook"],
        "downBook":     btc["downBook"],
        "btcPrice":     btc["spotPrice"],
        "btcChangePct": btc["spotChangePct"],
        "klines":       btc["klines"],
        "connected":    btc["connected"],
        "fair":         btc["fair"],
        "sim":          btc["sim"],
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
    log.info("=" * 50)

    async with serve(ws_handler, HOST, PORT):
        await asyncio.gather(
            data_fetcher(),
            broadcast_loop(),
            market_ws_loop(),
        )

if __name__ == "__main__":
    asyncio.run(main())
