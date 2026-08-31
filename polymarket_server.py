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
SIM_MIN_SIGMA_PER_SECOND    = {"btc": 0.000025, "eth": 0.000040}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIM_DB_PATH = os.path.join(BASE_DIR, "polymarket_sim.sqlite3")

# ── 追蹤的資產：BTC 是原本的主力，ETH 是新加的第二個市場，同一套抓取/策略邏輯共用，
#    只是換一個 slug 前綴跟 Binance 報價代碼。
ASSETS = [
    {"id": "btc", "label": "BTC", "slugPrefix": "btc-updown-5m-", "binanceSymbol": "BTCUSDT"},
    {"id": "eth", "label": "ETH", "slugPrefix": "eth-updown-5m-", "binanceSymbol": "ETHUSDT"},
]

# ── A/B 門檻測試：同時跑好幾組不同的進場/鎖利門檻，吃同一份真實報價，
#    彼此獨立記帳，方便直接比較哪組門檻的實際表現比較好（而不是憑感覺猜）。
#    這幾組全部都是 BTC 的。"main" 這組固定對應 SIM_ENTRY_MAX_PRICE / SIM_LOCK_MAX_SUM，
#    也是既有前端面板（部位卡片、成交紀錄列表）顯示的那一組，維持向後相容。
#    ETH 目前只用單一策略（不做 A/B 比較），單獨用 "eth-main" 這個 id。
AB_VARIANTS = [
    {"id": "conservative", "assetId": "btc", "label": "BTC 保守 0.30/0.85", "entryMaxPrice": 0.30, "lockMaxSum": 0.85},
    {"id": "main",         "assetId": "btc", "label": "BTC 目前 0.40/0.90", "entryMaxPrice": SIM_ENTRY_MAX_PRICE, "lockMaxSum": SIM_LOCK_MAX_SUM},
    {"id": "loose",        "assetId": "btc", "label": "BTC 寬鬆 0.45/0.95", "entryMaxPrice": 0.45, "lockMaxSum": 0.95},
    {"id": "eth-main",     "assetId": "eth", "label": "ETH 0.40/0.90",      "entryMaxPrice": SIM_ENTRY_MAX_PRICE, "lockMaxSum": SIM_LOCK_MAX_SUM},
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
    這套邏輯跟資產無關，BTC/ETH 共用同一份，只是 slug_prefix 不同。
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

    刻意不先按可見深度縮小股數——跟真實版的 FOK 語意一致：要嘛完整目標股數
    兩腿都吃得到，要嘛整筆視為不可行（simulate_buy_fill 深度不足會回傳 None），
    不會「深度不夠就自動改買比較少」，這樣模擬結果才不會比真實下單能做到的樂觀。"""
    variant = AB_VARIANT_BY_ID[variant_id]
    shares, budget = _target_order_size(variant_id)
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
) -> None:
    variant = AB_VARIANT_BY_ID[variant_id]
    st = ab_states[variant_id]
    pos = st["position"]

    if pos is None:
        if remaining_seconds is None or remaining_seconds < SIM_MIN_ENTRY_REMAINING:
            return
        if _try_direct_pair(variant_id, slug, up_book, down_book):
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
    if fair:
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

    up_price, down_price, up_book, down_book, spot, klines = await asyncio.gather(
        fetch_midpoint(session, up_id),
        fetch_midpoint(session, down_id),
        fetch_book(session, up_id),
        fetch_book(session, down_id),
        fetch_spot_price(session, asset["binanceSymbol"]),
        fetch_klines(session, asset["binanceSymbol"], 60),
        return_exceptions=True,
    )
    if not isinstance(up_price, Exception):   ms["upPrice"] = up_price
    if not isinstance(down_price, Exception): ms["downPrice"] = down_price
    if not isinstance(up_book, Exception):    ms["upBook"] = up_book
    if not isinstance(down_book, Exception):  ms["downBook"] = down_book
    if not isinstance(spot, Exception):
        ms["spotPrice"] = spot["price"]
        ms["spotChangePct"] = spot["changePct"]
    if not isinstance(klines, Exception) and klines:
        ms["klines"] = klines

    slug = ms["market"]["slug"]
    remaining_seconds = None if ms["windowEndsAt"] is None else max(0.0, ms["windowEndsAt"] / 1000 - real_now())
    fair = estimate_fair_up(aid)
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
    """每組 A/B 門檻的即時戰績，前端拿來畫比較表，一眼看出目前哪組門檻表現最好。
    只列 BTC 的門檻比較（ETH 目前只有單一策略，不參與比較，避免混在一起看起來像跨資產比較）。"""
    rows = []
    for v in AB_VARIANTS:
        if v["assetId"] != "btc":
            continue
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
    """單一資產的市場行情 + 對應策略戰績，BTC 跟 ETH 共用同一份組裝邏輯。"""
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
    eth = build_asset_summary("eth", "eth-main")
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
        # 新增：ETH 獨立一份，前端有新的 ETH 區塊會讀這裡
        "eth": eth,
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
        )

if __name__ == "__main__":
    asyncio.run(main())
