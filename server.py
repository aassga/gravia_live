"""
Gravia · Real-Time Data Bridge
────────────────────────────────
從 Binance / Bybit 公開 API 拉取真實數據，
透過 WebSocket 推送給 dashboard.html。

不需要 API key — 全部使用公開市場端點。

啟動方式：
    python server.py

然後用瀏覽器開啟 web/dashboard.html
"""

import asyncio
import json
import random
import time
import logging
from datetime import datetime, timezone

import aiohttp
import websockets
from websockets.server import serve

# ── 設定 ──────────────────────────────────────────────────────────────────
HOST = "localhost"
PORT = 8765
FETCH_INTERVAL = 10   # 每幾秒抓一次（資金費率每 8h 才更新，10s 綽綽有餘）
PRICE_INTERVAL = 2    # 價格更新頻率（秒）
EXCLUDE_SYMBOLS = {"BTCUSDT"}          # 明確排除的幣種（BTC 兩邊費率太貼近，不適合這個策略）
MIN_24H_VOLUME_USD = 2_000_000         # 兩邊 24h 成交額都要超過這個門檻才視為「適合」（避免滑價吃掉價差）
MARKETS_DISPLAY_TOP = 500               # 送給前端顯示的監控清單筆數上限（目前總共約 170 多個共同上市幣種，設高一點等於全部顯示）

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("gravia")

# ── 全域狀態 ───────────────────────────────────────────────────────────────
state = {
    "btcPrice":    0.0,
    "ethPrice":    0.0,
    "bnRate":      0.0,
    "bbRate":      0.0,
    "bnNextFund":  "",
    "bbNextFund":  "",
    "spread":      0.0,
    "spreadApr":   0.0,
    "longOn":      "—",
    "shortOn":     "—",
    "bnVolume":    0.0,
    "bbVolume":    0.0,
    "lastUpdate":  "",
    "priceHistory": [],    # [{t, v}] BTC 價格歷史，最多 120 筆
    "orderBook":   {"bids": [], "asks": []},  # 真實 Binance 深度報價
    "pnlHistory":  [],      # [{t, v}] 模擬帳戶累計損益歷史，最多 90 筆
    "markets":     [],      # 監控清單（依淨 APR 由高到低），送給前端顯示
    "focusSymbol": None,    # Funding Rates／力度圖頭部目前聚焦的幣種（持倉中的，或當下最佳機會）
    "connected":   False,
}

CLIENTS: set = set()

# ── 模擬自動交易（紙上交易，不動用真實資金）───────────────────────────────────
SIM_ENTRY_APR           = 30.0     # 淨 APR 超過這個門檻才自動開倉（要夠高才可能撐過滑價損益兩平所需的時間）
SIM_DEFAULT_LEVERAGE    = 10.0     # 槓桿倍數預設值：每筆名目 = (當下餘額 / 持倉筆數上限) × 這個倍數
SIM_DEFAULT_BALANCE     = 100.0    # 起始虛擬本金預設值（美元）
SIM_DEFAULT_MAX_POSITIONS = 10     # 最多同時持有幾筆倉位預設值（分散到不同幣種，均分餘額）
SIM_MIN_LEVERAGE        = 1.0
SIM_MAX_LEVERAGE        = 100.0
SIM_MIN_BALANCE         = 1.0
SIM_MIN_POSITIONS       = 1
SIM_MAX_POSITIONS_LIMIT = 50
SECONDS_PER_YEAR        = 365 * 24 * 3600

# 真實下單風險模擬（依然是紙上交易，只是用真實價格算「如果真的下單會發生什麼」）
SIM_MAINTENANCE_RATIO = 0.8      # 維持保證金比例：單腳不利波動達到 (1/槓桿)×此比例 就模擬強平
SIM_SLIPPAGE_PCT      = 0.0005   # 每次進場/出場的模擬滑價成本（占名目比例，反映買賣價差）
SIM_REJECT_PROB       = 0.03     # 模擬下單被拒的機率（跳過這個機會，換下一個候選）
SIM_PARTIAL_FILL_PROB = 0.08     # 模擬部分成交的機率
SIM_PARTIAL_FILL_RANGE = (0.5, 0.9)  # 部分成交時，實際成交比例的隨機範圍
SIM_NAKED_LEG_PROB    = 0.05     # 模擬「只有一腳先成交」的機率：另一腳延遲到下一輪才補上，期間單腳裸露曝險
SIM_PERSISTENCE_TICKS = 3        # 淨 APR 要連續維持在門檻之上幾輪（每輪 FETCH_INTERVAL 秒），才視為穩定機會而非雜訊尖峰

sim_state = {
    "startBalance": SIM_DEFAULT_BALANCE,          # 使用者可從前端面板調整，調整後會重置模擬
    "leverage":     SIM_DEFAULT_LEVERAGE,         # 同上
    "maxPositions": SIM_DEFAULT_MAX_POSITIONS,    # 同上
    "positions":    [],    # [{symbol, entryTime, lastTick, longOn, shortOn, entrySpreadApr, notional, accumPnl}]，最多 maxPositions 筆
    "trades":       [],    # 已平倉紀錄，最新在前，最多保留 50 筆
    "totalPnl":     0.0,
    "totalTrades":  0,
    "wins":         0,
}

apr_history: dict = {}  # symbol -> 最近 SIM_PERSISTENCE_TICKS 輪的淨 APR，判斷價差是不是穩定持續而非瞬間尖峰

def update_apr_history(markets: list) -> None:
    """每輪把當下淨 APR 記進每個幣種的歷史裡，只留最近 SIM_PERSISTENCE_TICKS 筆。"""
    seen = set()
    for m in markets:
        sym = m["symbol"]
        seen.add(sym)
        hist = apr_history.setdefault(sym, [])
        hist.append(m["netApr"])
        if len(hist) > SIM_PERSISTENCE_TICKS:
            hist.pop(0)
    for sym in set(apr_history) - seen:  # 這輪沒抓到資料的幣種，歷史直接清掉避免無限累積
        del apr_history[sym]

def is_persistent(symbol: str) -> bool:
    """這個幣種是不是連續 SIM_PERSISTENCE_TICKS 輪淨 APR 都超過進場門檻（不是單輪雜訊尖峰）。"""
    hist = apr_history.get(symbol, [])
    return len(hist) >= SIM_PERSISTENCE_TICKS and min(hist) > SIM_ENTRY_APR

def reset_sim(start_balance: float = None, leverage: float = None, max_positions: int = None) -> None:
    """重置模擬帳戶：清空所有持倉與歷史紀錄，可同時調整起始本金／槓桿倍數／最大持倉筆數。"""
    if start_balance is not None:
        sim_state["startBalance"] = max(SIM_MIN_BALANCE, float(start_balance))
    if leverage is not None:
        sim_state["leverage"] = max(SIM_MIN_LEVERAGE, min(SIM_MAX_LEVERAGE, float(leverage)))
    if max_positions is not None:
        sim_state["maxPositions"] = max(SIM_MIN_POSITIONS, min(SIM_MAX_POSITIONS_LIMIT, int(max_positions)))
    sim_state["positions"] = []
    sim_state["trades"] = []
    sim_state["totalPnl"] = 0.0
    sim_state["totalTrades"] = 0
    sim_state["wins"] = 0
    log.info(f"[SIM] 重置：起始本金=${sim_state['startBalance']:,.2f} 槓桿={sim_state['leverage']:.0f}x "
              f"最大持倉={sim_state['maxPositions']}筆")

def leg_price_pnl_pct(pos: dict, cur: dict, leg_exchange: str) -> float:
    """算某一腳（Binance 或 Bybit）從進場價到現在的損益百分比（正值=賺錢），
    用該交易所自己的真實價格，不管另一腳有沒有對沖住——這正是保證金為什麼要分開算的原因。
    """
    if leg_exchange == "Binance":
        entry_p, cur_p = pos["entryBnPrice"], cur.get("bnPrice", 0.0)
    else:
        entry_p, cur_p = pos["entryBbPrice"], cur.get("bbPrice", 0.0)
    if entry_p <= 0 or cur_p <= 0:
        return 0.0
    raw_move = (cur_p - entry_p) / entry_p
    return raw_move if pos["longOn"] == leg_exchange else -raw_move

def position_net_apr(pos: dict, cur: dict) -> float:
    """算這筆倉位「照它實際持有的方向」現在真正在賺（正）還是在付（負）多少淨 APR。
    跟 cur['netApr']（永遠是正值，代表當下最佳方向的價差大小）不同——
    如果倉位方向跟目前最佳方向不一致（翻轉了），這裡會正確回傳負值。
    """
    if pos["longOn"] == "Binance":
        signed_spread = cur["bbRate"] - cur["bnRate"]
    else:
        signed_spread = cur["bnRate"] - cur["bbRate"]
    return signed_spread * 3 * 365 * 100 - 0.2

def close_position(pos: dict, now: float, exit_pnl: float, reason: str) -> None:
    """結算一筆倉位：扣出場滑價、寫入歷史紀錄、更新累計統計。"""
    exit_pnl -= pos["notional"] * SIM_SLIPPAGE_PCT
    held = now - pos["entryTime"]
    trade = {
        "symbol":      pos["symbol"],
        "entryTime":   pos["entryTime"],
        "exitTime":    now,
        "longOn":      pos["longOn"],
        "shortOn":     pos["shortOn"],
        "entryApr":    pos["entrySpreadApr"],
        "heldSeconds": held,
        "notional":    pos["notional"],
        "pnl":         exit_pnl,
        "exitReason":  reason,
    }
    sim_state["trades"].insert(0, trade)
    sim_state["trades"] = sim_state["trades"][:50]
    sim_state["totalPnl"] += exit_pnl
    sim_state["totalTrades"] += 1
    if exit_pnl > 0:
        sim_state["wins"] += 1
    log.info(f"[SIM] 平倉 {pos['symbol']} PnL=${exit_pnl:+.2f} 持有{held:.0f}s (原因: {reason})")

def simulate_trading(markets: list) -> None:
    """用當下真實抓到的資金費率價差與真實價格，逐次累加模擬部位的浮動損益，
    並模擬三種真實下單會遇到的風險（詳見各段落）。

    同時最多持有 sim_state["maxPositions"] 筆不同幣種的倉位，分散布局而不是全押一組。
    進場：該幣種淨 APR > SIM_ENTRY_APR 且連續 SIM_PERSISTENCE_TICKS 輪都維持在門檻之上（過濾雜訊尖峰），
          且還有空位；名目採複利 = (當下餘額 / 持倉筆數上限) × 槓桿。
    出場：該持倉幣種的多空方向翻轉、淨 APR 轉為 <= 0，或觸發模擬強平（只看自己持倉的幣種）。
    損益採 mark-to-market：每個 tick 用「當時真實觀測到的淨 APR」對經過的真實秒數計息。
    """
    now = time.time()
    market_by_symbol = {m["symbol"]: m for m in markets}
    update_apr_history(markets)

    still_open = []
    for pos in sim_state["positions"]:
        cur = market_by_symbol.get(pos["symbol"])
        if cur is None:
            still_open.append(pos)  # 這個 tick 抓不到該幣種資料，跳過，下次再試
            continue

        # ① 裸露腳：上一輪只有一腳先成交，這一輪補上另一腳，
        #    期間的損益改用「已成交那一腳」的真實價格漲跌計算（沒有對沖，全曝險）。
        if pos.get("naked"):
            filled_leg = pos["longOn"] if pos["nakedLeg"] == "long" else pos["shortOn"]
            pct = leg_price_pnl_pct(pos, cur, filled_leg)
            naked_pnl = pos["notional"] * pct
            pos["accumPnl"] += naked_pnl
            pos["naked"] = False
            pos["nakedLeg"] = None
            pos["lastTick"] = now
            log.info(f"[SIM] {pos['symbol']} 補上另一腳，裸露期間損益=${naked_pnl:+.2f}")
            still_open.append(pos)
            continue

        # ② 保證金／強平檢查：兩腳分開算，任一腳不利波動超過維持保證金就模擬強平。
        #    這是真實槓桿交易的風險來源——交易所不知道你另一邊有對沖倉位。
        threshold = -(1.0 / pos["leverage"]) * SIM_MAINTENANCE_RATIO
        bn_pct = leg_price_pnl_pct(pos, cur, "Binance")
        bb_pct = leg_price_pnl_pct(pos, cur, "Bybit")
        liquidated_leg = None
        if bn_pct <= threshold:
            liquidated_leg = "Binance"
        elif bb_pct <= threshold:
            liquidated_leg = "Bybit"

        if liquidated_leg:
            surviving_leg = "Bybit" if liquidated_leg == "Binance" else "Binance"
            lost_margin = pos["notional"] / pos["leverage"]
            surviving_pnl = pos["notional"] * leg_price_pnl_pct(pos, cur, surviving_leg)
            total_pnl = pos["accumPnl"] - lost_margin + surviving_pnl
            close_position(pos, now, total_pnl, f"強制平倉（{liquidated_leg} 腳保證金不足）")
            continue

        # ③ 正常計息：用「這筆倉位實際持有方向」的真實淨 APR（有正負號）對經過的秒數計息。
        #    翻轉時這裡會正確算成負的（代表現在在倒貼），而不是誤當成還在賺錢。
        dt = now - pos["lastTick"]
        signed_apr = position_net_apr(pos, cur)
        pos["accumPnl"] += pos["notional"] * (signed_apr / 100) * (dt / SECONDS_PER_YEAR)
        pos["lastTick"] = now

        # ④ 出場判斷：翻轉加緩衝帶——新方向的價差也要超過進場門檻，才當作真正反轉；
        #    只是在零附近雜訊擺動的假翻轉不出場，避免頻繁進出白繳滑價。
        flipped = cur["longOn"] != pos["longOn"]
        should_exit = (cur["netApr"] > SIM_ENTRY_APR) if flipped else (cur["netApr"] <= 0)
        if should_exit:
            close_position(pos, now, pos["accumPnl"], "費率翻轉" if flipped else "淨APR轉負")
        else:
            still_open.append(pos)

    sim_state["positions"] = still_open

    max_positions = sim_state["maxPositions"]
    slots = max_positions - len(sim_state["positions"])
    if slots > 0:
        open_symbols = {p["symbol"] for p in sim_state["positions"]}
        candidates = [
            m for m in markets
            if m["symbol"] not in open_symbols
            and m["netApr"] > SIM_ENTRY_APR
            and is_persistent(m["symbol"])  # 要連續穩定在門檻之上，不是單輪雜訊尖峰
        ]
        candidates.sort(key=lambda m: m["netApr"], reverse=True)

        filled = 0
        for m in candidates:
            if filled >= slots:
                break

            # ④ 模擬下單被拒：這個機會這次跳過，換下一個候選，不占用名額。
            if random.random() < SIM_REJECT_PROB:
                log.info(f"[SIM] {m['symbol']} 開倉被拒（模擬下單被拒），跳過")
                continue

            unrealized = sum(p["accumPnl"] for p in sim_state["positions"])
            balance = sim_state["startBalance"] + sim_state["totalPnl"] + unrealized  # 複利：用目前餘額算這一筆的名目
            leverage = sim_state["leverage"]
            notional = (balance / max_positions) * leverage

            # ⑤ 模擬部分成交：只成交到一部分名目。
            partial_note = ""
            if random.random() < SIM_PARTIAL_FILL_PROB:
                fill_ratio = random.uniform(*SIM_PARTIAL_FILL_RANGE)
                notional *= fill_ratio
                partial_note = f" 部分成交{fill_ratio*100:.0f}%"

            # ⑥ 模擬單腳先成交（裸露曝險），另一腳留到下一輪才補上。
            naked = random.random() < SIM_NAKED_LEG_PROB
            naked_leg = random.choice(["long", "short"]) if naked else None

            entry_slippage = notional * SIM_SLIPPAGE_PCT  # 進場滑價成本，先扣掉

            sim_state["positions"].append({
                "symbol":         m["symbol"],
                "entryTime":      now,
                "lastTick":       now,
                "longOn":         m["longOn"],
                "shortOn":        m["shortOn"],
                "entrySpreadApr": m["netApr"],
                "notional":       notional,
                "leverage":       leverage,
                "accumPnl":       -entry_slippage,
                "entryBnPrice":   m.get("bnPrice", 0.0),
                "entryBbPrice":   m.get("bbPrice", 0.0),
                "naked":          naked,
                "nakedLeg":       naked_leg,
            })
            filled += 1
            log.info(f"[SIM] 開倉 {m['symbol']} Long={m['longOn']} Short={m['shortOn']} "
                      f"進場淨APR={m['netApr']:.2f}% 名目=${notional:,.2f}{partial_note}"
                      f"{' 單腳先成交(裸露中)' if naked else ''} "
                      f"({len(sim_state['positions'])}/{max_positions})")

def sim_payload() -> dict:
    positions = sim_state["positions"]
    unrealized_total = sum(p["accumPnl"] for p in positions)

    # 連勝：從最新一筆往回數，遇到虧損就停
    win_streak = 0
    for t in sim_state["trades"]:
        if t["pnl"] > 0:
            win_streak += 1
        else:
            break

    return {
        "positions": [
            {
                "symbol":         p["symbol"],
                "entryTime":      p["entryTime"],
                "longOn":         p["longOn"],
                "shortOn":        p["shortOn"],
                "entrySpreadApr": p["entrySpreadApr"],
                "notional":       p["notional"],
                "leverage":       p["leverage"],
                "unrealizedPnl":  p["accumPnl"],
                "naked":          p.get("naked", False),
            }
            for p in positions
        ],
        "maxPositions":   sim_state["maxPositions"],
        "minPositions":   SIM_MIN_POSITIONS,
        "maxPositionsLimit": SIM_MAX_POSITIONS_LIMIT,
        "trades":         sim_state["trades"],
        "totalPnl":       sim_state["totalPnl"],
        "totalTrades":    sim_state["totalTrades"],
        "wins":           sim_state["wins"],
        "balance":        sim_state["startBalance"] + sim_state["totalPnl"] + unrealized_total,
        "startBalance":   sim_state["startBalance"],
        "leverage":       sim_state["leverage"],
        "minLeverage":    SIM_MIN_LEVERAGE,
        "maxLeverage":    SIM_MAX_LEVERAGE,
        "winStreak":      win_streak,
    }

# ── API 抓取函數 ───────────────────────────────────────────────────────────

async def fetch_binance_ticker(session: aiohttp.ClientSession) -> dict:
    """BTC 現貨價格 + 24h 成交量"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        return {
            "price":  float(d["lastPrice"]),
            "volume": float(d["volume"]),
        }

async def fetch_binance_all_funding(session: aiohttp.ClientSession) -> dict:
    """一次拉 Binance 全部 USDT 永續合約的資金費率 + 真實標記價格（單一請求，不用逐幣種問）"""
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        data = await r.json()
        return {
            d["symbol"]: {
                "rate":     float(d["lastFundingRate"]),
                "nextTime": d.get("nextFundingTime", 0),
                "price":    float(d.get("markPrice") or 0),
            }
            for d in data if d["symbol"].endswith("USDT")
        }

async def fetch_binance_all_volume(session: aiohttp.ClientSession) -> dict:
    """一次拉 Binance 全部 USDT 永續合約的 24h 成交額（報價幣計價，已經是美元）"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        data = await r.json()
        return {d["symbol"]: float(d["quoteVolume"]) for d in data if d["symbol"].endswith("USDT")}

async def fetch_bybit_all_funding(session: aiohttp.ClientSession) -> dict:
    """一次拉 Bybit 全部 linear USDT 永續合約的資金費率 + 24h 成交額"""
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        data = await r.json()
        out = {}
        for item in data["result"]["list"]:
            sym = item["symbol"]
            if not sym.endswith("USDT"):
                continue
            rate = item.get("fundingRate")
            out[sym] = {
                "rate":     float(rate) if rate not in (None, "") else 0.0,
                "nextTime": int(item.get("nextFundingTime") or 0),
                "volume":   float(item.get("turnover24h") or 0),
                "price":    float(item.get("lastPrice") or 0),
            }
        return out

async def scan_market_universe(session: aiohttp.ClientSession) -> list:
    """掃描 Binance ∩ Bybit 全部共同上市的 USDT 永續合約，算出每一組的真實費率價差，
    過濾掉 BTC 與成交量太低的幣種，依淨 APR 由高到低排序回傳。
    """
    bn_funding, bn_volume, bb_funding = await asyncio.gather(
        fetch_binance_all_funding(session),
        fetch_binance_all_volume(session),
        fetch_bybit_all_funding(session),
    )
    common = (set(bn_funding) & set(bb_funding)) - EXCLUDE_SYMBOLS

    markets = []
    for sym in common:
        bn_vol = bn_volume.get(sym, 0.0)
        bb_vol = bb_funding[sym]["volume"]
        if bn_vol < MIN_24H_VOLUME_USD or bb_vol < MIN_24H_VOLUME_USD:
            continue
        bn_rate = bn_funding[sym]["rate"]
        bb_rate = bb_funding[sym]["rate"]
        sp = calc_spread(bn_rate, bb_rate)
        markets.append({
            "symbol":      sym,
            "bnRate":      bn_rate,
            "bbRate":      bb_rate,
            "bnNextFund":  ms_to_hms(bn_funding[sym]["nextTime"]),
            "bbNextFund":  ms_to_hms(bb_funding[sym]["nextTime"]),
            "bnVolume":    bn_vol,
            "bbVolume":    bb_vol,
            "bnPrice":     bn_funding[sym]["price"],
            "bbPrice":     bb_funding[sym]["price"],
            **sp,
        })

    markets.sort(key=lambda m: m["netApr"], reverse=True)
    return markets

async def fetch_eth_price(session: aiohttp.ClientSession) -> float:
    """ETH 現貨價格"""
    url = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=ETHUSDT"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        return float(d["price"])

async def fetch_binance_orderbook(session: aiohttp.ClientSession, limit=5) -> dict:
    """BTC 永續合約訂單簿深度（真實買賣盤）"""
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit={limit}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json()
        return {
            "bids": [{"price": float(p), "qty": float(q)} for p, q in d["bids"][:limit]],
            "asks": [{"price": float(p), "qty": float(q)} for p, q in d["asks"][:limit]],
        }

async def fetch_binance_klines(session: aiohttp.ClientSession, limit=60) -> list:
    """BTC 5 分鐘 K 線"""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit={limit}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        raw = await r.json()
        return [
            {"t": k[0], "o": float(k[1]), "h": float(k[2]),
             "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
            for k in raw
        ]

def ms_to_hms(ms: int) -> str:
    if not ms:
        return "—"
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M UTC")

def calc_spread(bn_rate: float, bb_rate: float) -> dict:
    spread = abs(bn_rate - bb_rate)
    # 估算年化：每 8h 結算 3 次/天 × 365 天，扣除約 0.2% 年化手續費
    gross_apr = spread * 3 * 365 * 100
    net_apr   = gross_apr - 0.2
    long_on   = "Binance" if bn_rate < bb_rate else "Bybit"
    short_on  = "Bybit"   if bn_rate < bb_rate else "Binance"
    return {
        "spread":    spread,
        "spreadPct": spread * 100,
        "grossApr":  gross_apr,
        "netApr":    net_apr,
        "longOn":    long_on,
        "shortOn":   short_on,
        "isProfitable": net_apr > 0,
    }

# ── 背景抓取任務 ───────────────────────────────────────────────────────────

async def data_fetcher():
    """每隔 FETCH_INTERVAL 秒抓一次完整數據並更新 state"""
    async with aiohttp.ClientSession() as session:
        # 先抓一次 K 線初始化
        try:
            klines = await fetch_binance_klines(session, 60)
            state["klines"] = klines
            log.info(f"K線初始化完成，{len(klines)} 根")
        except Exception as e:
            log.warning(f"K線初始化失敗: {e}")
            state["klines"] = []

        while True:
            try:
                # 並行抓取所有數據；資金費率改成一次掃描整個 Binance∩Bybit 共同上市清單
                bn_ticker, eth_price, klines, order_book, markets = await asyncio.gather(
                    fetch_binance_ticker(session),
                    fetch_eth_price(session),
                    fetch_binance_klines(session, 60),
                    fetch_binance_orderbook(session, 5),
                    scan_market_universe(session),
                    return_exceptions=True
                )

                # 處理每個結果（有例外就跳過，保留舊值）
                if not isinstance(bn_ticker, Exception):
                    state["btcPrice"] = bn_ticker["price"]
                    state["bnVolume"] = bn_ticker["volume"]

                if not isinstance(eth_price, Exception):
                    state["ethPrice"] = eth_price

                if not isinstance(klines, Exception) and klines:
                    state["klines"] = klines

                if not isinstance(order_book, Exception):
                    state["orderBook"] = order_book

                if isinstance(markets, Exception):
                    markets = state.get("markets", [])

                # 顯示清單：淨 APR 前 N 名，若目前持倉的幣種掉出榜外也強制帶上，讓面板隨時看得到它們
                open_symbols = {p["symbol"] for p in sim_state["positions"]}
                display_markets = markets[:MARKETS_DISPLAY_TOP]
                missing = open_symbols - {m["symbol"] for m in display_markets}
                if missing:
                    display_markets = display_markets + [m for m in markets if m["symbol"] in missing]
                state["markets"] = display_markets

                # Funding Rates／力度圖頭部顯示「持倉中淨 APR 最高的那筆」，沒有持倉就顯示當下最佳機會
                focus = None
                if open_symbols:
                    open_markets = [m for m in markets if m["symbol"] in open_symbols]
                    if open_markets:
                        focus = max(open_markets, key=lambda m: m["netApr"])
                if focus is None and markets:
                    focus = markets[0]
                if focus:
                    state["focusSymbol"] = focus["symbol"]
                    state["bnRate"]     = focus["bnRate"]
                    state["bbRate"]     = focus["bbRate"]
                    state["bnNextFund"] = focus["bnNextFund"]
                    state["bbNextFund"] = focus["bbNextFund"]
                    state["spread"]     = focus["spread"]
                    state["spreadPct"]  = focus["spreadPct"]
                    state["spreadApr"]  = focus["netApr"]
                    state["longOn"]     = focus["longOn"]
                    state["shortOn"]    = focus["shortOn"]
                    state["isProfitable"] = focus["isProfitable"]

                state["lastUpdate"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                state["connected"] = True

                simulate_trading(display_markets)

                unrealized = sum(p["accumPnl"] for p in sim_state["positions"])
                state["pnlHistory"].append({
                    "t": time.time(),
                    "v": sim_state["totalPnl"] + unrealized,
                })
                if len(state["pnlHistory"]) > 90:
                    state["pnlHistory"].pop(0)

                log.info(
                    f"BTC=${state['btcPrice']:,.2f} | "
                    f"共同上市幣種={len(markets)} | "
                    f"焦點={state.get('focusSymbol') or '—'} "
                    f"BN={state['bnRate']*100:+.4f}% "
                    f"BB={state['bbRate']*100:+.4f}% "
                    f"Spread={state['spread']*100:.4f}% "
                    f"NetAPR={state['spreadApr']:.1f}%"
                )

            except Exception as e:
                log.error(f"Fetch error: {e}")
                state["connected"] = False

            await asyncio.sleep(FETCH_INTERVAL)

async def price_updater():
    """用 Binance WebSocket stream 接收即時 BTC 價格"""
    url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
    while True:
        try:
            async with websockets.connect(url) as ws:
                log.info("Binance 即時價格 stream 已連接")
                async for msg in ws:
                    data = json.loads(msg)
                    price = float(data["p"])
                    state["btcPrice"] = price
                    # 價格歷史（最多 300 筆，每筆約 2 秒 → 10 分鐘）
                    state["priceHistory"].append({
                        "t": data["T"],
                        "v": price
                    })
                    if len(state["priceHistory"]) > 300:
                        state["priceHistory"].pop(0)
                    # 廣播給所有 dashboard 客戶端
                    if CLIENTS:
                        msg_out = json.dumps({
                            "type": "price",
                            "btcPrice": price,
                            "lastUpdate": state["lastUpdate"],
                        })
                        await asyncio.gather(
                            *[c.send(msg_out) for c in list(CLIENTS)],
                            return_exceptions=True
                        )
        except Exception as e:
            log.warning(f"Price stream 斷線，5 秒後重連: {e}")
            await asyncio.sleep(5)

# ── WebSocket 廣播 ─────────────────────────────────────────────────────────

def build_full_payload() -> str:
    return json.dumps({
        "type":        "full",
        "btcPrice":    state["btcPrice"],
        "ethPrice":    state["ethPrice"],
        "bnRate":      state["bnRate"],
        "bbRate":      state["bbRate"],
        "bnNextFund":  state["bnNextFund"],
        "bbNextFund":  state["bbNextFund"],
        "spread":      state["spread"],
        "spreadPct":   state.get("spreadPct", 0),
        "spreadApr":   state["spreadApr"],
        "longOn":      state["longOn"],
        "shortOn":     state["shortOn"],
        "bnVolume":    state["bnVolume"],
        "bbVolume":    state["bbVolume"],
        "isProfitable": state.get("isProfitable", False),
        "lastUpdate":  state["lastUpdate"],
        "klines":      state.get("klines", []),
        "orderBook":   state.get("orderBook", {"bids": [], "asks": []}),
        "pnlHistory":  state.get("pnlHistory", []),
        "markets":     state.get("markets", []),
        "focusSymbol": state.get("focusSymbol"),
        "connected":   state["connected"],
        "sim":         sim_payload(),
    })

async def broadcast(payload: str) -> None:
    """把一份 payload 送給所有連線中的客戶端，送失敗的自動移除"""
    dead = set()
    for client in list(CLIENTS):
        try:
            await client.send(payload)
        except Exception:
            dead.add(client)
    CLIENTS.difference_update(dead)

async def broadcast_state():
    """每 FETCH_INTERVAL 秒廣播完整 state 給所有客戶端"""
    while True:
        await asyncio.sleep(FETCH_INTERVAL)
        if CLIENTS and state["btcPrice"] > 0:
            await broadcast(build_full_payload())

async def ws_handler(websocket):
    """處理新的 dashboard 連線；同時接收前端送來的指令（目前只有 configure 重置模擬設定）"""
    CLIENTS.add(websocket)
    ip = websocket.remote_address
    log.info(f"Dashboard 已連接: {ip}  (共 {len(CLIENTS)} 個客戶端)")

    # 立刻送一次完整 state
    try:
        if state["btcPrice"] > 0:
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
                reset_sim(
                    start_balance=msg.get("startBalance"),
                    leverage=msg.get("leverage"),
                    max_positions=msg.get("maxPositions"),
                )
                # 重置後立刻廣播最新狀態，前端不用等下一輪 10 秒
                await broadcast(build_full_payload())
    finally:
        CLIENTS.discard(websocket)
        log.info(f"Dashboard 已斷線: {ip}  (剩 {len(CLIENTS)} 個客戶端)")

# ── 主程式 ────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 50)
    log.info("  Gravia · Real-Time Data Bridge")
    log.info(f"  WebSocket: ws://{HOST}:{PORT}")
    log.info("  數據來源: Binance + Bybit 公開 API")
    log.info("  開啟 web/dashboard.html 查看即時數據")
    log.info("=" * 50)

    async with serve(ws_handler, HOST, PORT):
        await asyncio.gather(
            data_fetcher(),
            price_updater(),
            broadcast_state(),
        )

if __name__ == "__main__":
    asyncio.run(main())
