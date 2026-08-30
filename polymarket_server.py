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
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
import websockets
from websockets.server import serve

# ── 設定 ──────────────────────────────────────────────────────────────────
HOST = "localhost"
PORT = 8766             # 跟 Gravia 的 8765 分開，兩套系統可以同時跑
POLL_INTERVAL = 3       # 報價輪詢間隔（秒）－ Polymarket 沒有強制要求 WebSocket，輪詢就綽綽有餘

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket")

# ── 模擬策略設定（紙上交易）────────────────────────────────────────────────
SIM_ENTRY_MAX_PRICE   = 0.40   # 主要策略：只有價格 <= 這個門檻才考慮先進場一邊
SIM_LOCK_MAX_SUM      = 0.90   # 主要策略：兩邊合計成本 <= 這個門檻，就配對鎖利（保證賺 (1-合計) 每股）
SIM_DEFAULT_BALANCE   = 100.0  # 起始虛擬總資產預設值（美元），可從前端輸入自訂（會重置模擬）
SIM_MIN_BALANCE       = 1.0
SIM_DEFAULT_STAKE_PCT = 3.0    # 每注預設佔目前資產組合的百分比（複利：資產越大，單注跟著變大）
SIM_MIN_STAKE_PCT     = 0.5
SIM_MAX_STAKE_PCT     = 25.0

# ── A/B 門檻測試：同時跑好幾組不同的進場/鎖利門檻，吃同一份真實報價，
#    彼此獨立記帳，方便直接比較哪組門檻的實際表現比較好（而不是憑感覺猜）。
#    "main" 這組固定對應 SIM_ENTRY_MAX_PRICE / SIM_LOCK_MAX_SUM，
#    也是既有前端面板（部位卡片、成交紀錄列表）顯示的那一組，維持向後相容。
AB_VARIANTS = [
    {"id": "conservative", "label": "保守 0.30 / 0.85", "entryMaxPrice": 0.30, "lockMaxSum": 0.85},
    {"id": "main",         "label": "目前 0.40 / 0.90", "entryMaxPrice": SIM_ENTRY_MAX_PRICE, "lockMaxSum": SIM_LOCK_MAX_SUM},
    {"id": "loose",        "label": "寬鬆 0.45 / 0.95", "entryMaxPrice": 0.45, "lockMaxSum": 0.95},
]
AB_VARIANT_BY_ID = {v["id"]: v for v in AB_VARIANTS}

# ── 全域狀態 ───────────────────────────────────────────────────────────────
state = {
    "market":        None,   # 目前追蹤的市場（Gamma market 物件）
    "windowEndsAt":  None,   # 這個窗口結束時間（unix ms）
    "upPrice":       None,
    "downPrice":     None,
    "upBook":        {"bids": [], "asks": []},
    "downBook":      {"bids": [], "asks": []},
    "btcPrice":      None,   # 真實 BTC 現貨價格（Binance），讓你看得出 Up/Down 報價背後在動什麼
    "btcChangePct":  None,   # 24h 漲跌幅
    "klines":        [],     # 真實 BTC 1 分鐘 K 線（Binance），畫蠟燭圖用
    "connected":     False,
}

def _new_variant_state() -> dict:
    return {
        "position":           None,  # {windowSlug, side, entryPrice, shares, entryTime, hedged, hedgeSide, hedgePrice, hedgeShares}
        "pendingSettlements": [],    # 已換窗口、結果還沒查到的舊倉位，每輪重試直到查到結果
        "trades":             [],    # 已結算紀錄，最新在前，最多保留 50 筆
        "totalPnl":           0.0,
        "totalTrades":        0,
        "wins":                0,
    }

ab_states = {v["id"]: _new_variant_state() for v in AB_VARIANTS}

# 下注比例／起始資產是所有 A/B 組共用的設定，刻意保持一致，
# 這樣比較結果的差異只來自「進場/鎖利門檻」本身，不會被其他變因干擾。
shared_config = {
    "stakePct":     SIM_DEFAULT_STAKE_PCT,
    "startBalance": SIM_DEFAULT_BALANCE,
}

sim_state = ab_states["main"]  # 向後相容：既有程式碼引用 sim_state 的地方，等同於 "main" 這組

def set_stake_pct(pct: float) -> None:
    shared_config["stakePct"] = max(SIM_MIN_STAKE_PCT, min(SIM_MAX_STAKE_PCT, float(pct)))
    log.info(f"[SIM] 下注比例調整為 {shared_config['stakePct']:.1f}%（複利，隨資產組合變動，套用到全部 A/B 組）")

def reset_with_balance(start_balance: float) -> None:
    """自訂起始資產 = 重新開始：清空所有 A/B 組的持倉與歷史紀錄，下注比例維持原本設定不變。"""
    shared_config["startBalance"] = max(SIM_MIN_BALANCE, float(start_balance))
    for vid in ab_states:
        ab_states[vid] = _new_variant_state()
    global sim_state
    sim_state = ab_states["main"]
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

# ── Polymarket API ─────────────────────────────────────────────────────────

WINDOW_SECONDS = 300  # 5 分鐘一個窗口

async def fetch_active_btc_5m_market(session: aiohttp.ClientSession) -> dict | None:
    """直接用真實時間算出目前這個 5 分鐘窗口的 slug 去查，不掃描 Gamma 的市場列表。

    原本用 active=true&closed=false 篩選、依 startDate 排序去找，結果發現不可靠：
    Polymarket 會把未來一整天的窗口都預先建好（全部也是 active=true），
    也有很多從很久以前就從沒被正確標記 closed 的舊窗口卡在列表裡，
    不管排序方向，抓到的都不是「現在正在進行」的那一個。
    直接用時間算 slug（格式：btc-updown-5m-<窗口開始時間的 unix 秒>）最準。
    """
    window_start = int(real_now() // WINDOW_SECONDS) * WINDOW_SECONDS
    for start in (window_start, window_start - WINDOW_SECONDS):  # 抓不到當前窗口就退回上一個（剛好在交界處時的備援）
        slug = f"btc-updown-5m-{start}"
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
        }

async def fetch_btc_price(session: aiohttp.ClientSession) -> dict:
    """真實 BTC/USDT 現貨價格（Binance），讓你看得出 Up/Down 報價背後實際在動的東西"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        return {"price": float(d["lastPrice"]), "changePct": float(d["priceChangePercent"])}

async def fetch_btc_klines(session: aiohttp.ClientSession, limit: int = 60) -> list:
    """真實 BTC 1 分鐘 K 線（Binance），畫成蠟燭圖讓畫面更有感、看得出價格走勢"""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit={limit}"
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

def enter_position(variant_id: str, slug: str, side: str, price: float) -> None:
    st = ab_states[variant_id]
    # 複利：下注金額 = 當下資產組合 × 下注比例，資產越大下一注也跟著變大（全部 A/B 組共用同一個比例）
    _, portfolio = compute_cash_and_portfolio(variant_id)
    stake_usd = portfolio * (shared_config["stakePct"] / 100)
    shares = stake_usd / price
    st["position"] = {
        "windowSlug": slug,
        "side":         side,
        "entryPrice":   price,
        "shares":       shares,
        "stakeUsd":     stake_usd,
        "entryTime":    time.time(),
        "hedged":       False,
        "hedgeSide":    None,
        "hedgePrice":   None,
        "hedgeShares":  0.0,
    }
    log.info(f"[SIM:{variant_id}] 進場 {side} @ ${price:.3f}　下注=${stake_usd:.2f}（{shared_config['stakePct']:.1f}%）股數={shares:.2f}")

def hedge_position(variant_id: str, side: str, price: float) -> None:
    pos = ab_states[variant_id]["position"]
    shares = pos["shares"]  # 配對相同股數，鎖住保證利潤（不管結果是哪邊贏都拿一樣多）
    pos["hedged"] = True
    pos["hedgeSide"] = side
    pos["hedgePrice"] = price
    pos["hedgeShares"] = shares
    locked = shares * (1 - pos["entryPrice"] - price)
    log.info(f"[SIM:{variant_id}] 配對鎖利 {side} @ ${price:.3f}　鎖定利潤=${locked:+.2f}")

def simulate_trading(variant_id: str, slug: str, up_price: float, down_price: float) -> None:
    if up_price is None or down_price is None or up_price <= 0 or down_price <= 0:
        return
    variant = AB_VARIANT_BY_ID[variant_id]
    pos = ab_states[variant_id]["position"]

    if pos is None:
        if up_price <= variant["entryMaxPrice"]:
            enter_position(variant_id, slug, "Up", up_price)
        elif down_price <= variant["entryMaxPrice"]:
            enter_position(variant_id, slug, "Down", down_price)
        return

    if pos["hedged"] or pos["windowSlug"] != slug:
        return  # 已經鎖利，或倉位是上一個窗口留下的殘影，等結算就好

    other_side  = "Down" if pos["side"] == "Up" else "Up"
    other_price = down_price if other_side == "Down" else up_price
    if pos["entryPrice"] + other_price <= variant["lockMaxSum"]:
        hedge_position(variant_id, other_side, other_price)

def compute_cash_and_portfolio(variant_id: str) -> tuple[float, float]:
    """比照 Polymarket 官方的『現金／資產組合』算法：
    現金 = 起始資產 + 已實現損益 - 目前壓在倉位裡的成本（買股票的錢從現金裡扣掉）
    資產組合 = 現金 + 目前持有部位的即時市值（用當下真實報價算，還沒結算前會隨報價波動）
    """
    st = ab_states[variant_id]
    staked = 0.0
    market_value = 0.0
    pos = st["position"]
    if pos:
        staked = pos["shares"] * pos["entryPrice"]
        up_price = state["upPrice"] or 0.0
        down_price = state["downPrice"] or 0.0
        market_value = pos["shares"] * (up_price if pos["side"] == "Up" else down_price)
        if pos["hedged"]:
            staked += pos["hedgeShares"] * pos["hedgePrice"]
            market_value += pos["hedgeShares"] * (up_price if pos["hedgeSide"] == "Up" else down_price)

    cash = shared_config["startBalance"] + st["totalPnl"] - staked
    portfolio = cash + market_value
    return cash, portfolio

def record_trade(variant_id: str, pos: dict, pnl: float, outcome: str) -> None:
    st = ab_states[variant_id]
    trade = {
        "windowSlug":  pos["windowSlug"],
        "side":        pos["side"],
        "entryPrice":  pos["entryPrice"],
        "shares":      pos["shares"],
        "stakeUsd":    pos.get("stakeUsd"),
        "hedged":      pos["hedged"],
        "hedgeSide":   pos.get("hedgeSide"),
        "hedgePrice":  pos.get("hedgePrice"),
        "outcome":     outcome,
        "pnl":         pnl,
        "entryTime":   pos["entryTime"],
        "exitTime":    time.time(),
    }
    st["trades"].insert(0, trade)
    st["trades"] = st["trades"][:50]
    st["totalPnl"] += pnl
    st["totalTrades"] += 1
    if pnl > 0:
        st["wins"] += 1

def _settle_pnl(pos: dict, outcome: str) -> float:
    if pos["hedged"]:
        # 兩邊都買了相同股數，結果是哪邊贏都拿一樣的錢回來，損益固定
        return pos["shares"] * (1 - pos["entryPrice"] - pos["hedgePrice"])
    won = (outcome == pos["side"])
    return pos["shares"] * (1 - pos["entryPrice"]) if won else -(pos["shares"] * pos["entryPrice"])

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

def queue_settlement(slug: str) -> None:
    """窗口換了：每一組 A/B 如果上一個窗口還有沒結算的倉位，各自丟進自己的待結算佇列，
    換一個乾淨的位置開始追蹤新窗口。"""
    for st in ab_states.values():
        pos = st["position"]
        if pos is not None and pos["windowSlug"] == slug:
            st["pendingSettlements"].append(pos)
        st["position"] = None

# ── 背景抓取任務 ───────────────────────────────────────────────────────────

async def data_fetcher():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # 每輪都重新問 Polymarket「現在正在進行的是哪個窗口」，
                # 不要用本機時鐘去推算「是不是該換下一輪」——本機時鐘不見得準，
                # 但 Polymarket 自己回傳的 active/closed 狀態一定是對的，直接拿來當真相來源。
                cur = state["market"]
                new_market = await fetch_active_btc_5m_market(session)

                if new_market and (cur is None or new_market["slug"] != cur["slug"]):
                    if cur is not None:
                        queue_settlement(cur["slug"])
                    state["market"] = new_market
                    state["windowEndsAt"] = _iso_to_ms(new_market["endDate"])
                    log.info(f"[MARKET] 切換到新窗口 {new_market['slug']}　結束於 {new_market['endDate']}")

                if state["market"]:
                    up_id, down_id = _market_tokens(state["market"])
                    if up_id and down_id:
                        up_price, down_price, up_book, down_book, btc, klines = await asyncio.gather(
                            fetch_midpoint(session, up_id),
                            fetch_midpoint(session, down_id),
                            fetch_book(session, up_id),
                            fetch_book(session, down_id),
                            fetch_btc_price(session),
                            fetch_btc_klines(session, 60),
                            return_exceptions=True,
                        )
                        if not isinstance(up_price, Exception):   state["upPrice"] = up_price
                        if not isinstance(down_price, Exception): state["downPrice"] = down_price
                        if not isinstance(up_book, Exception):    state["upBook"] = up_book
                        if not isinstance(down_book, Exception):  state["downBook"] = down_book
                        if not isinstance(btc, Exception):
                            state["btcPrice"] = btc["price"]
                            state["btcChangePct"] = btc["changePct"]
                        if not isinstance(klines, Exception) and klines:
                            state["klines"] = klines

                        slug = state["market"]["slug"]
                        for variant_id in ab_states:
                            simulate_trading(variant_id, slug, state["upPrice"], state["downPrice"])

                await retry_pending_settlements(session)  # 每輪都重試還沒結算成功的舊窗口

                state["connected"] = True

            except Exception as e:
                log.error(f"Fetch error: {e}")
                state["connected"] = False

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
        })
    return rows

def build_full_payload() -> str:
    m = state["market"] or {}
    remaining_seconds = None
    if state["windowEndsAt"] is not None:
        remaining_seconds = max(0.0, state["windowEndsAt"] / 1000 - real_now())
    cash, portfolio = compute_cash_and_portfolio("main")
    return json.dumps({
        "type": "full",
        "market": {
            "slug":     m.get("slug"),
            "question": m.get("question"),
            "endDate":  m.get("endDate"),
        },
        "windowEndsAt":     state["windowEndsAt"],
        "remainingSeconds": remaining_seconds,  # 已用真實世界時間校正過，前端直接倒數就好，不用自己比對本機時鐘
        "serverTimeMs":     real_now() * 1000,  # 校正過的真實時間，前端拿來顯示時鐘、不用本機系統時間
        "upPrice":      state["upPrice"],
        "downPrice":    state["downPrice"],
        "upBook":       state["upBook"],
        "downBook":     state["downBook"],
        "btcPrice":     state["btcPrice"],
        "btcChangePct": state["btcChangePct"],
        "klines":       state.get("klines", []),
        "connected":    state["connected"],
        "sim": {
            "position":    sim_state["position"],
            "pendingSettlements": len(sim_state["pendingSettlements"]),
            "trades":      sim_state["trades"],
            "totalPnl":    sim_state["totalPnl"],
            "totalTrades": sim_state["totalTrades"],
            "wins":        sim_state["wins"],
            "startBalance": shared_config["startBalance"],
            "minBalance":   SIM_MIN_BALANCE,
            "cash":         cash,
            "portfolio":    portfolio,
            "entryMaxPrice": SIM_ENTRY_MAX_PRICE,
            "lockMaxSum":    SIM_LOCK_MAX_SUM,
            "stakePct":      shared_config["stakePct"],
            "stakeUsd":      portfolio * (shared_config["stakePct"] / 100),  # 下一注預估金額（複利，隨資產組合變動）
            "minStakePct":   SIM_MIN_STAKE_PCT,
            "maxStakePct":   SIM_MAX_STAKE_PCT,
        },
        "abVariants": build_ab_leaderboard(),
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
    log.info("=" * 50)
    log.info("  Polymarket BTC Up/Down · Real-Time Data Bridge")
    log.info(f"  WebSocket: ws://{HOST}:{PORT}")
    log.info("  數據來源: Polymarket 公開 API（Gamma + CLOB）")
    log.info("  開啟 web/polymarket.html 查看即時數據")
    log.info("=" * 50)

    async with serve(ws_handler, HOST, PORT):
        await asyncio.gather(
            data_fetcher(),
            broadcast_loop(),
        )

if __name__ == "__main__":
    asyncio.run(main())
