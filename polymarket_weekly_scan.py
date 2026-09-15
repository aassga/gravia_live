"""
每週市場掃描：用 Polymarket 公開成交資料找出 BTC 5 分鐘 Up/Down 市場上最多人用的策略，
記錄下來，跟我們目前的實盤設定比對差異，列出可能的調整與優缺點，交給使用者決定。

    - 只讀公開 API（Gamma + data-api），不需要私鑰、不下單、不改任何設定。
    - 結果存 reports/weekly/<日期>.json 與 .md；摘要透過 Telegram bot 推給白名單使用者。
    - 「比對」是規則式：把掃到的參數（買價區間、進場秒數、是否停損、兩邊掛單…）跟目前
      實盤設定逐項對照，用數據給優缺點；不會自動改任何東西。

用法：
    python polymarket_weekly_scan.py            # 掃最近 24 小時並推播
    python polymarket_weekly_scan.py --hours 12 --no-telegram
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import logging
import os
import statistics
import time
from datetime import datetime, timedelta, timezone

import httpx

log = logging.getLogger("weekly-scan")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # httpx 的 INFO 會把含 token 的 URL 印進日誌

TAIPEI = timezone(timedelta(hours=8))
REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports", "weekly")
GAMMA = "https://gamma-api.polymarket.com/markets"
DATA_API = "https://data-api.polymarket.com/trades"
LIVE_WS = os.environ.get("TG_LIVE_STATUS_WS", "ws://127.0.0.1:8767")
WINDOW_SECONDS = 300
LATE_SECONDS = 60          # 「最後 60 秒」型態的判定
# 2026-09-14：可掃描的 Up/Down 市場（TG /scan 可選）；"other" 走全站探索（discover_top_markets）。
MARKETS = {
    "btc":     {"prefix": "btc-updown-5m-",  "window": 300, "label": "BTC 5 分鐘"},
    "btc-15m": {"prefix": "btc-updown-15m-", "window": 900, "label": "BTC 15 分鐘"},
    "eth":     {"prefix": "eth-updown-5m-",  "window": 300, "label": "ETH 5 分鐘"},
    "sol":     {"prefix": "sol-updown-5m-",  "window": 300, "label": "SOL 5 分鐘"},
    "xrp":     {"prefix": "xrp-updown-5m-",  "window": 300, "label": "XRP 5 分鐘"},
}
FAVORITE_MIN_ASK = 0.88    # 買領先方型態的最低買價（比我們的 0.95 寬，才看得到整條曲線）
# 2026-09-15 自動駕駛：不只 5 個市場，探測 Polymarket 上所有「<幣>-updown-<週期>-<ts>」系列。
SERIES_SYMBOLS = ["btc", "eth", "sol", "xrp", "bnb", "doge", "hype", "zec", "ada", "avax", "link", "ltc", "sui", "ton",
                  "trx", "dot", "shib", "pepe", "bch", "near", "apt", "arb", "op", "pol", "uni", "aave", "fil", "atom",
                  "ena", "wld", "tao", "sei", "tia", "inj", "kas", "xlm", "hbar", "etc", "algo", "render", "ondo"]
SERIES_TIMEFRAMES = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}


def discover_updown_series(client: httpx.Client) -> dict:
    """探測目前有開盤的 Up/Down 系列（用「上一個已結束窗口」的 slug 問 Gamma），回傳 {market_key: spec}。
    已知的 5 個沿用原 key（btc / btc-15m / eth / sol / xrp），其餘用 <幣>-<週期>。"""
    found = {}
    now = int(time.time())
    known = {(v["prefix"]): k for k, v in MARKETS.items()}
    for sym in SERIES_SYMBOLS:
        for tf, wsec in SERIES_TIMEFRAMES.items():
            prefix = f"{sym}-updown-{tf}-"
            ws = now // wsec * wsec - 2 * wsec
            try:
                m = client.get(GAMMA, params={"slug": f"{prefix}{ws}"}).json() or \
                    client.get(GAMMA, params={"slug": f"{prefix}{ws}", "closed": "true"}).json()
            except Exception as exc:
                log.warning(f"discover {prefix}: {exc}")
                continue
            if not m:
                continue
            key = known.get(prefix) or (sym if tf == "5m" else f"{sym}-{tf}")
            found[key] = {"prefix": prefix, "window": wsec, "label": f"{sym.upper()} {tf if tf != '5m' else '5 分鐘'}" if tf != "5m" else f"{sym.upper()} 5 分鐘",
                          "symbol": sym, "timeframe": tf}
            time.sleep(0.03)
    for k, v in found.items():
        MARKETS.setdefault(k, v)
    log.info(f"discovered {len(found)} up/down series: {', '.join(sorted(found))}")
    return found


def sim_asset_for_market(market: str) -> dict:
    """自動變體要掛在哪個模擬資產上：已知的對應現有 id，其餘用 market key 當 id（模擬盤會自動建資產）。"""
    spec = MARKETS.get(market) or {}
    aid = MARKET_TO_SIM_ASSET.get(market, market)
    sym = spec.get("symbol") or market.split("-")[0]
    return {"id": aid, "label": spec.get("label", market), "slugPrefix": spec.get("prefix"),
            "windowSeconds": int(spec.get("window", 300)), "binanceSymbol": f"{sym.upper()}USDT"}
MIN_WALLET_WINDOWS = 10    # 至少做過這麼多窗口才算「機器人／常用者」


# ── 抓資料 ─────────────────────────────────────────────────────────────────

def fetch_windows(hours: float, client: httpx.Client, market: str = "btc") -> list[dict]:
    global WINDOW_SECONDS, LATE_SECONDS
    spec = MARKETS.get(market) or (market if isinstance(market, dict) else MARKETS["btc"])
    WINDOW_SECONDS = int(spec["window"])
    LATE_SECONDS = 60 if WINDOW_SECONDS <= 300 else 120
    now = int(time.time())
    last_closed = now // WINDOW_SECONDS * WINDOW_SECONDS - 2 * WINDOW_SECONDS
    n = int(hours * 3600 // WINDOW_SECONDS)
    windows = []
    for i in range(n):
        ws = last_closed - WINDOW_SECONDS * i
        slug = f"{spec['prefix']}{ws}"
        try:
            m = client.get(GAMMA, params={"slug": slug}).json() or client.get(GAMMA, params={"slug": slug, "closed": "true"}).json()
        except Exception as exc:
            log.warning(f"gamma {slug}: {exc}")
            continue
        if not m:
            continue
        mk = m[0]
        try:
            prices = json.loads(mk.get("outcomePrices") or "[]")
            outcome = ("Up" if float(prices[0]) > 0.5 else "Down") if prices else None
        except Exception:
            outcome = None
        if outcome is None:
            continue
        trades, offset = [], 0
        while True:
            try:
                batch = client.get(DATA_API, params={"market": mk["conditionId"], "limit": 1000, "offset": offset}).json()
            except Exception as exc:
                log.warning(f"data-api {slug} offset={offset}: {exc}")
                break
            if not batch:
                break
            trades += batch
            if len(batch) < 1000 or offset >= 4000:
                break
            offset += 1000
        windows.append({"slug": slug, "start": ws, "outcome": outcome, "trades": trades})
        time.sleep(0.1)
    log.info(f"windows={len(windows)} trades={sum(len(w['trades']) for w in windows)}")
    return windows


# ── 分類 ───────────────────────────────────────────────────────────────────

def classify_wallet_window(ws: int, outcome: str, ts: list[dict]) -> dict:
    buys = [t for t in ts if t["side"] == "BUY"]
    sells = [t for t in ts if t["side"] == "SELL"]
    notional = sum(float(t["price"]) * float(t["size"]) for t in ts)
    info = {"notional": notional, "kind": None, "won": None, "pnl": 0.0}
    if not buys:
        info["kind"] = "sell_only"
        return info
    sides = {t["outcome"] for t in buys}
    rel = [int(t["timestamp"]) - ws for t in buys]
    if len(sides) == 2:
        def vwap(side):
            l = [(float(t["price"]), float(t["size"])) for t in buys if t["outcome"] == side]
            return sum(p * s for p, s in l) / sum(s for _, s in l)
        s = vwap("Up") + vwap("Down")
        info["kind"] = "both_sides_lock" if s < 1.0 else "both_sides_over1"
        info["pairSum"] = s
        info["pnl"] = min(sum(float(t["size"]) for t in buys if t["outcome"] == "Up"),
                          sum(float(t["size"]) for t in buys if t["outcome"] == "Down")) - sum(float(t["price"]) * float(t["size"]) for t in buys)
        return info
    side = next(iter(sides))
    shares = sum(float(t["size"]) for t in buys)
    cost = sum(float(t["price"]) * float(t["size"]) for t in buys)
    vw = cost / shares if shares else 0
    sold = [t for t in sells if t["outcome"] == side]
    sold_sh = sum(float(t["size"]) for t in sold)
    sold_val = sum(float(t["price"]) * float(t["size"]) for t in sold)
    held = max(0.0, shares - sold_sh)
    won = outcome == side
    info.update({"side": side, "vwap": vw, "shares": shares, "won": won, "sold": bool(sold),
                 "pnl": (held if won else 0.0) + sold_val - cost, "tBeforeClose": WINDOW_SECONDS - min(rel)})
    # 2026-09-14 修正：買領先方型態只看買單決定，之後有沒有賣出（停損／獲利了結）記在 sold 屬性；
    # 原本一有賣單就歸到「買了再賣」，導致買領先方的「曾賣出比例」永遠是 0%。
    if max(rel) >= WINDOW_SECONDS - LATE_SECONDS and min(rel) >= 0 and vw >= FAVORITE_MIN_ASK:
        info["kind"] = "late_favorite"
    elif sells and not sold:
        info["kind"] = "buy_then_sell_other"
    elif sold:
        info["kind"] = "buy_then_sell"
    elif min(rel) < 0:
        info["kind"] = "pre_open"
    elif max(rel) >= WINDOW_SECONDS - 10:
        info["kind"] = "last_10s_directional"
    elif max(rel) >= WINDOW_SECONDS - LATE_SECONDS:
        info["kind"] = "last_60s_directional"
    elif min(rel) <= 60:
        info["kind"] = "early_directional"
    else:
        info["kind"] = "mid_directional"
    return info


KIND_LABELS = {
    "late_favorite": "最後 {late} 秒買領先方（>=0.88）",
    "last_60s_directional": "最後 {late} 秒單邊方向性（<0.88）",
    "last_10s_directional": "最後 10 秒單邊方向性",
    "mid_directional": "窗口中段單邊方向性",
    "early_directional": "開盤 60 秒內單邊方向性",
    "pre_open": "開盤前先買",
    "buy_then_sell": "買了再賣（短線／停損）",
    "buy_then_sell_other": "買一邊、賣另一邊",
    "both_sides_lock": "兩邊都買、加總 < $1（鎖利）",
    "both_sides_over1": "兩邊都買、加總 >= $1",
    "sell_only": "只賣（出庫存）",
}


def kind_label(kind: str) -> str:
    # 2026-09-15：15 分鐘市場的「最後 N 秒」要跟 LATE_SECONDS 一致，不能寫死 60。
    return KIND_LABELS.get(kind, kind).format(late=LATE_SECONDS)


def _risk_fields(items: list[dict]) -> dict:
    """一次虧損要幾次獲利才補回（賠率）、打平所需勝率、實際勝率是否夠。
    2026-09-15 依使用者要求：勝率高但一次虧損就歸零的區間要標出來。"""
    per = [i["pnl"] / i["shares"] for i in items if i["shares"] > 0]
    wins = [p for p in per if p > 0]; losses = [-p for p in per if p < 0]
    avg_win = statistics.mean(wins) if wins else 0.0
    avg_loss = statistics.mean(losses) if losses else 0.0
    win_rate = sum(1 for i in items if i["won"]) / len(items) if items else 0.0
    losses_per_win = (avg_loss / avg_win) if avg_win > 0 and avg_loss > 0 else None   # 1 次輸要幾次贏
    # 沒有虧損樣本就不算打平勝率（否則會顯示 0%），留 None 表示本期看不出賠率。
    break_even = (avg_loss / (avg_win + avg_loss)) if (avg_win > 0 and avg_loss > 0) else None
    return {
        "avgWinPerShare": avg_win, "avgLossPerShare": avg_loss, "worstPerShare": min(per) if per else 0.0,
        "lossesPerWin": losses_per_win, "breakEvenWinRate": break_even,
        "negativeEV": bool(break_even is not None and win_rate < break_even),
    }


def risk_text(b: dict) -> str:
    """TG 用的一句話：1 輸＝N 贏，勝率不夠補虧損就標 ❌。"""
    lpw = b.get("lossesPerWin")
    if lpw is None:
        return "本期零虧損" if b.get("avgLossPerShare", 0) == 0 else "無獲利樣本"
    return f"1 輸＝{lpw:.0f} 贏{'，❌ 勝率不夠補虧損' if b.get('negativeEV') else ''}"


def analyze(windows: list[dict]) -> dict:
    per_wallet: dict[str, list[dict]] = collections.defaultdict(list)
    kind_count = collections.Counter(); kind_notional = collections.Counter(); kind_wallets = collections.defaultdict(set)
    kind_pnl = collections.Counter(); kind_won = collections.Counter(); kind_decided = collections.Counter()
    for w in windows:
        by = collections.defaultdict(list)
        for t in w["trades"]:
            by[t["proxyWallet"]].append(t)
        for wallet, ts in by.items():
            info = classify_wallet_window(w["start"], w["outcome"], ts)
            k = info["kind"]
            kind_count[k] += 1; kind_notional[k] += info["notional"]; kind_wallets[k].add(wallet); kind_pnl[k] += info["pnl"]
            if info["won"] is not None:
                kind_decided[k] += 1; kind_won[k] += int(info["won"])
            info["slug"] = w["slug"]
            per_wallet[wallet].append(info)
    total = sum(kind_count.values()) or 1
    kinds = []
    # 2026-09-15 依使用者要求：型態改依粗估總損益排序（原本依使用人數）。
    for k, n in sorted(kind_count.items(), key=lambda kv: kind_pnl[kv[0]], reverse=True):
        kinds.append({
            "kind": k, "label": kind_label(k), "count": n, "share": n / total,
            "notional": kind_notional[k], "wallets": len(kind_wallets[k]),
            "winRate": (kind_won[k] / kind_decided[k]) if kind_decided[k] else None,
            "pnl": kind_pnl[k],
        })
    # late-favorite 細分：買價區間、進場秒數、有無停損
    fav = [i for L in per_wallet.values() for i in L if i["kind"] == "late_favorite"]
    def bucket(items, key, edges):
        out = []
        for lo, hi in zip(edges, edges[1:]):
            g = [i for i in items if lo <= i[key] < hi]
            if g:
                # 2026-09-14 修正：每股淨利改為「每個錢包窗口各算一次、等權平均」並附中位數；
                # 原本用總損益 ÷ 總股數，會被一兩個巨鯨的大額虧損拉偏（出現 99% 勝率卻每股為負）。
                per = [i["pnl"] / i["shares"] for i in g if i["shares"] > 0]
                out.append({"lo": lo, "hi": hi, "n": len(g), "winRate": sum(i["won"] for i in g) / len(g),
                            "pnlPerShare": statistics.mean(per) if per else 0.0,
                            "pnlPerShareMedian": statistics.median(per) if per else 0.0,
                            "pnlPerShareSizeWeighted": sum(i["pnl"] for i in g) / sum(i["shares"] for i in g),
                            **_risk_fields(g)})
        return out
    fav_price = bucket(fav, "vwap", [0.88, 0.92, 0.95, 0.98, 1.01])
    # 2026-09-15 修正：進場秒數分桶依 LATE_SECONDS 等比放大（15 分鐘市場是 120 秒，原本只到 61s 會漏掉 61～120s）。
    kt = LATE_SECONDS / 60.0
    fav_time = bucket(fav, "tBeforeClose", [int(round(e)) for e in (0, 10 * kt, 20 * kt, 30 * kt, 45 * kt, 60 * kt + 1)])
    fav_stop = {"withSell": sum(1 for i in fav if i["sold"]), "total": len(fav)}
    # 全勤機器人
    bots = []
    for wallet, L in per_wallet.items():
        if len(L) < MIN_WALLET_WINDOWS:
            continue
        kinds_c = collections.Counter(i["kind"] for i in L)
        decided = [i for i in L if i["won"] is not None]
        favs = [i for i in L if i["kind"] == "late_favorite"]
        bots.append({
            "wallet": wallet, "windows": len(L), "notional": sum(i["notional"] for i in L),
            "pnl": sum(i["pnl"] for i in L), "mainKind": kinds_c.most_common(1)[0][0],
            "winRate": (sum(i["won"] for i in decided) / len(decided)) if decided else None,
            "favAvgPrice": statistics.mean(i["vwap"] for i in favs) if favs else None,
            "favMedianT": statistics.median(i["tBeforeClose"] for i in favs) if favs else None,
            "favSellRate": (sum(i["sold"] for i in favs) / len(favs)) if favs else None,
        })
    bots.sort(key=lambda b: b["pnl"], reverse=True)
    return {
        "windows": len(windows), "trades": sum(len(w["trades"]) for w in windows),
        "outcomes": dict(collections.Counter(w["outcome"] for w in windows)),
        "kinds": kinds, "lateFavorite": {"priceBuckets": fav_price, "timeBuckets": fav_time, "stop": fav_stop, "n": len(fav)},
        "topBots": bots[:15], "botCount": len(bots),
    }


# ── 自動駕駛用：從原始窗口找「最後 N 秒買領先方」家族裡收益最高的規則 ───────
MARKET_TO_SIM_ASSET = {"btc": "btc", "btc-15m": "btc-15m", "eth": "eth-alt", "sol": "sol", "xrp": "xrp"}
CANDIDATE_MIN_SAMPLES = 100
CANDIDATE_PRICE_EDGES = [0.88, 0.92, 0.95, 0.98, 1.0]


def late_favorite_candidates(windows: list[dict], market: str) -> list[dict]:
    """2026-09-15：買價區間 × 進場秒數 的二維格。依使用者要求「主要看總收益」：每格算該格所有錢包窗口的
    粗估總損益（totalPnl），只留 totalPnl > 0 且 n >= CANDIDATE_MIN_SAMPLES 的格子，依 totalPnl 排序；
    每股平均／中位、打平勝率只當附註。回傳可直接變成模擬變體的規格。"""
    spec = MARKETS.get(market, MARKETS["btc"])
    aid = MARKET_TO_SIM_ASSET.get(market, market)
    k = LATE_SECONDS / 60.0
    time_edges = [int(round(e)) for e in (5 * k, 15 * k, 30 * k, 45 * k, 60 * k)]
    fav = []
    for w in windows:
        by = collections.defaultdict(list)
        for t in w["trades"]:
            by[t["proxyWallet"]].append(t)
        for ts in by.values():
            info = classify_wallet_window(w["start"], w["outcome"], ts)
            if info["kind"] == "late_favorite" and not info.get("sold"):
                fav.append(info)
    out = []
    for plo, phi in zip(CANDIDATE_PRICE_EDGES, CANDIDATE_PRICE_EDGES[1:]):
        for tlo, thi in zip(time_edges, time_edges[1:]):
            g = [i for i in fav if plo <= i["vwap"] < phi and tlo <= i["tBeforeClose"] < thi]
            if len(g) < CANDIDATE_MIN_SAMPLES:
                continue
            per = [i["pnl"] / i["shares"] for i in g if i["shares"] > 0]
            mean, med = statistics.mean(per), statistics.median(per)
            risk = _risk_fields(g)
            total_pnl = sum(i["pnl"] for i in g)
            if total_pnl <= 0:
                continue
            win_rate = sum(1 for i in g if i["won"]) / len(g)
            vid = f"{aid}-auto-{tlo}-{thi}s-{int(round(plo * 100)):03d}-{int(round(min(phi, 0.99) * 100)):03d}"
            out.append({
                "id": vid, "assetId": aid, "market": market, "asset": sim_asset_for_market(market),
                "label": f"⚙ 自動 {spec['label']} 最後 {tlo}～{thi} 秒買領先方（{plo:.2f}～{min(phi, 0.99):.2f}、不停損）",
                "favoriteWindowSeconds": float(thi), "favoriteMinRemaining": float(tlo),
                "favoriteMinPrice": plo, "favoriteMaxPrice": min(phi, 0.99),
                "stats": {"n": len(g), "winRate": win_rate, "totalPnl": total_pnl, "pnlPerShare": mean, "pnlPerShareMedian": med,
                          "breakEvenWinRate": risk["breakEvenWinRate"], "lossesPerWin": risk["lossesPerWin"],
                          "negativeEV": risk["negativeEV"], "score": total_pnl},
            })
    out.sort(key=lambda c: c["stats"]["score"], reverse=True)
    return out


# ── 全站探索：目前最多人玩的盤 ────────────────────────────────────────────

def classify_market_kind(slug: str, end_date: str | None) -> str:
    s = (slug or "").lower()
    if "updown-5m" in s or "updown-15m" in s:
        return "crypto_window"
    if "updown" in s or "up-or-down" in s:
        return "crypto_daily"
    if end_date:
        try:
            days = (datetime.fromisoformat(end_date.replace("Z", "+00:00")) - datetime.now(timezone.utc)).days
            if days <= 2:
                return "event_soon"
        except Exception:
            pass
    return "long_dated"


MARKET_KIND_LABELS = {
    "crypto_window": "加密幣 5／15 分鐘 Up/Down",
    "crypto_daily":  "加密幣日／週 Up/Down",
    "event_soon":    "兩天內結算的事件（球賽／選舉／利率）",
    "long_dated":    "長天期事件",
}

MARKET_KIND_NOTES = {
    "crypto_window": ("每 5／15 分鐘結算一次、規則明確、可重複驗證；我們現有的買領先方／鎖利引擎直接適用。",
                      "最後幾十秒常來回翻面；手續費在 0.5 附近最貴；深度薄（BTC 以外每窗只有幾百到幾千美元）。"),
    "crypto_daily":  ("結算頻率較低、走勢確立後翻面機率小；買領先方邏輯可套用（時間參數放大）。",
                      "資金鎖住數小時到一天；機會少、每筆利潤薄；我們的引擎尚未支援非固定窗口的市場。"),
    "event_soon":    ("成交額最大（球賽／利率決議常達百萬美元）、深度厚；主流玩法是賽前／會前依賠率買領先方、或做市收價差。",
                      "沒有價格時間衰減可用，勝負取決於資訊優勢；賽中價格跟著比分劇烈跳動；我們沒有這類市場的訊號來源與程式支援。"),
    "long_dated":    ("流動性穩定、可做市；價格慢慢收斂到結果。",
                      "資金鎖住數週到數月、年化報酬低；需要對事件本身有判斷；與我們的短窗口引擎完全不同，等於另開一套系統。"),
}


def _current_prices(mk: dict) -> dict:
    """Gamma 的 outcomes / outcomePrices（JSON 字串）→ {結果名: 現價}。"""
    try:
        names = mk.get("outcomes"); prices = mk.get("outcomePrices")
        names = json.loads(names) if isinstance(names, str) else (names or [])
        prices = json.loads(prices) if isinstance(prices, str) else (prices or [])
        return {str(n): float(p) for n, p in zip(names, prices)}
    except Exception:
        return {}


def discover_top_markets(client: httpx.Client, limit: int = 12) -> dict:
    """撈全站 24h 成交額最高的市場，看每個市場最近成交的玩法（買賣比、單量、價位、獨立錢包數）。"""
    try:
        rows = client.get(GAMMA, params={"closed": "false", "active": "true", "order": "volume24hr", "ascending": "false", "limit": limit}).json()
    except Exception as exc:
        log.warning(f"gamma discover: {exc}")
        rows = []
    out = []
    for mk in rows:
        ev = (mk.get("events") or [{}])[0]
        kind = classify_market_kind(mk.get("slug") or "", mk.get("endDate"))
        try:
            trades = client.get(DATA_API, params={"market": mk["conditionId"], "limit": 1000}).json() or []
        except Exception:
            trades = []
        wallets = {t.get("proxyWallet") for t in trades}
        buys = [t for t in trades if t.get("side") == "BUY"]
        # 2026-09-15：跟領先方（買 >= 0.85）按市場現價估算每股淨利與最大單筆虧損，用來判斷該市場值不值得跟。
        cur = _current_prices(mk)
        fav_buys = [t for t in buys if float(t.get("price") or 0) >= 0.85 and t.get("outcome") in cur]
        fav_per = [cur[t["outcome"]] - float(t["price"]) for t in fav_buys]
        fav_usd = [(cur[t["outcome"]] - float(t["price"])) * float(t.get("size") or 0) for t in fav_buys]
        sizes = [float(t.get("size") or 0) * float(t.get("price") or 0) for t in trades]
        prices = [float(t.get("price") or 0) for t in buys]
        # 玩法推估：買在 >= 0.85 的比例（跟領先方）、兩邊都買的錢包比例（做市／鎖利）、賣單比例（短線）
        per_wallet = collections.defaultdict(set)
        for t in trades:
            per_wallet[t.get("proxyWallet")].add(t.get("outcome"))
        both = sum(1 for w, sides in per_wallet.items() if len(sides) >= 2)
        out.append({
            "slug": mk.get("slug"), "question": (mk.get("question") or "")[:60], "event": (ev.get("title") or "")[:40],
            "kind": kind, "volume24h": float(mk.get("volume24hr") or 0), "liquidity": float(mk.get("liquidity") or 0),
            "endDate": (mk.get("endDate") or "")[:10], "trades": len(trades), "wallets": len(wallets),
            "medianTradeUsd": statistics.median(sizes) if sizes else 0.0,
            "buyShare": (len(buys) / len(trades)) if trades else 0.0,
            "favoriteShare": (sum(1 for p in prices if p >= 0.85) / len(prices)) if prices else 0.0,
            "bothSidesShare": (both / len(per_wallet)) if per_wallet else 0.0,
            "priceMedian": statistics.median(prices) if prices else 0.0,
            "favN": len(fav_buys),
            "favMtmPerShare": statistics.mean(fav_per) if fav_per else None,
            "favWorstUsd": min(fav_usd) if fav_usd else 0.0,
            "favTotalUsd": sum(fav_usd),
        })
        time.sleep(0.05)
    kinds = collections.Counter(m["kind"] for m in out)
    return {"markets": out, "kinds": dict(kinds)}


def render_discovery_telegram(disc: dict) -> list[str]:
    ms = disc["markets"]
    if not ms:
        return ["🌐 全站探索：Gamma API 沒有回資料。"]
    head = ["🌐 全站探索：目前 24h 成交額最高的市場（依「跟領先方按現價估的總損益」排序）", "分類：" + "；".join(f"{MARKET_KIND_LABELS.get(k, k)} {v} 個" for k, v in disc["kinds"].items())]
    msgs = ["\n".join(head)]
    body = []
    # 2026-09-15 依使用者要求：以收益為主——跟領先方的人在這個市場到底有沒有賺，而不是有多少人在跟。
    ms = sorted(ms, key=lambda m: m.get("favTotalUsd") or 0.0, reverse=True)
    for i, m in enumerate(ms[:10], 1):
        play = []
        if m["favoriteShare"] >= 0.5: play.append(f"跟領先方（{m['favoriteShare']*100:.0f}% 買在 ≥0.85）")
        if m["bothSidesShare"] >= 0.15: play.append(f"做市／兩邊都買（{m['bothSidesShare']*100:.0f}% 錢包）")
        if m["buyShare"] < 0.7: play.append(f"短線進出（賣單 {100 - m['buyShare']*100:.0f}%）")
        if not play: play.append("單邊持有到結算")
        mtm = m.get("favMtmPerShare")
        if mtm is None or (m.get("favN") or 0) < 5:
            verdict = "— 跟領先方樣本不足"
        elif mtm > 0:
            verdict = f"👍 跟領先方按現價每股 {mtm:+.3f}、共 {m.get('favTotalUsd', 0):+,.0f}、最大單筆 {m.get('favWorstUsd', 0):,.0f}"
        else:
            verdict = f"👎 跟領先方按現價每股 {mtm:+.3f}、共 {m.get('favTotalUsd', 0):+,.0f}、最大單筆 {m.get('favWorstUsd', 0):,.0f}"
        body.append(f"{i}. {m['event'] or m['question']}｜{m['question']}\n   24h ${m['volume24h']:,.0f}·深度 ${m['liquidity']:,.0f}·{m['endDate']}·{MARKET_KIND_LABELS.get(m['kind'], m['kind'])}\n   近 {m['trades']} 筆／{m['wallets']} 錢包·中位單 ${m['medianTradeUsd']:,.0f}·買價中位 {m['priceMedian']:.2f}·玩法：{'、'.join(play)}\n   {verdict}")
    msgs.append("\n".join(body))
    for k in disc["kinds"]:
        pros, cons = MARKET_KIND_NOTES.get(k, ("—", "—"))
        msgs.append(f"🔎 {MARKET_KIND_LABELS.get(k, k)}\n👍 {pros}\n👎 {cons}")
    msgs.append("以上為公開成交的行為推估（未結算市場以現價估損益），不是明確策略；任何更改請回覆指示後再由人工執行。")
    return msgs


# ── 跟目前實盤設定比對 ─────────────────────────────────────────────────────

async def fetch_live_config() -> dict:
    try:
        import websockets
        async with websockets.connect(LIVE_WS, open_timeout=8, max_size=None) as ws:
            d = json.loads(await asyncio.wait_for(ws.recv(), 8))
            return d.get("strategyConfig") or {}
    except Exception as exc:
        log.warning(f"live config unavailable: {exc}")
        return {}


def compare(report: dict, cfg: dict) -> list[dict]:
    """規則式比對：回傳 [{title, finding, pros, cons}]，不做任何變更。"""
    out = []
    fav = report["lateFavorite"]
    ours_min = cfg.get("lateFavoriteMinPrice"); ours_max = cfg.get("lateFavoriteMaxPrice")
    ours_stop = cfg.get("lateFavoriteStopLossPrice")
    if cfg.get("lateFavoriteEnabled") and fav["priceBuckets"]:
        # 2026-09-15 依使用者要求：「本週最佳」改以每股淨利中位數挑（不看勝率），並標出勝率補不回虧損的區間。
        key = lambda b: (b.get("pnlPerShareMedian", 0), b["pnlPerShare"])
        best = max(fav["priceBuckets"], key=key)
        ours = [b for b in fav["priceBuckets"] if ours_min is not None and b["lo"] <= ours_min < b["hi"]]
        finding = "市場買價區間每股淨利（平均／中位；1 輸＝要幾次贏才補回）：" + "；".join(
            f"{b['lo']:.2f}～{b['hi']:.2f} {b['pnlPerShare']:+.4f}／{b.get('pnlPerShareMedian', 0):+.4f}（n={b['n']}，{risk_text(b)}）" for b in fav["priceBuckets"])
        bad = [b for b in fav["priceBuckets"] if b.get("negativeEV")]
        if ours and best is not ours[0]:
            out.append({
                "title": "買價區間",
                "finding": finding + f"。我們目前 {ours_min:.2f}～{ours_max:.2f}（中位 {ours[0].get('pnlPerShareMedian', 0):+.4f}{'，❌ 長期負期望' if ours[0].get('negativeEV') else ''}）；本週每股中位最高是 {best['lo']:.2f}～{best['hi']:.2f}。",
                "pros": f"改到 {best['lo']:.2f}～{best['hi']:.2f}：每股中位 {best.get('pnlPerShareMedian', 0):+.4f} vs 我們區間 {ours[0].get('pnlPerShareMedian', 0):+.4f}。",
                "cons": "區間越貴每股毛利越薄、一次翻面要更多次獲利才補回；樣本只有一週，需連續兩週一致再改。",
            })
        else:
            out.append({"title": "買價區間", "finding": finding + f"。我們目前 {ours_min}～{ours_max}，已在本週每股中位最高的區間。",
                        "pros": "維持。", "cons": ("❌ 但此區間勝率補不回虧損（長期負期望）。" if ours and ours[0].get("negativeEV") else "—")})
        if bad:
            out.append({
                "title": "勝率高卻長期賠錢的區間",
                "finding": "；".join(f"{b['lo']:.2f}～{b['hi']:.2f}：打平需勝率 {b['breakEvenWinRate']*100:.1f}%，實際 {b['winRate']*100:.1f}%，一次虧損要 {b['lossesPerWin']:.0f} 次獲利才補回" for b in bad),
                "pros": "避開這些區間（或加停損把單次虧損壓到 1 輸＝10 贏以內）可直接改善總損益。",
                "cons": "一週樣本內一兩次大翻面就會讓區間變負；請連續兩週一致再定案。",
            })
        if fav["timeBuckets"]:
            bt = max(fav["timeBuckets"], key=key)
            out.append({
                "title": "進場時點",
                "finding": "進場前秒數每股淨利（平均／中位）：" + "；".join(f"{b['lo']}～{b['hi']}s {b['pnlPerShare']:+.4f}／{b.get('pnlPerShareMedian', 0):+.4f}（{risk_text(b)}）" for b in fav["timeBuckets"]) + f"。我們目前 {cfg.get('lateFavoriteMinRemaining', 5):.0f}～{cfg.get('lateFavoriteWindowSeconds', 60):.0f}s 都可進。",
                "pros": f"若只在 {bt['lo']}～{bt['hi']}s 進，每股中位可到 {bt.get('pnlPerShareMedian', 0):+.4f}。",
                "cons": "縮窄時間會少掉機會；最後 10 秒內深度薄、交易所偶爾關單。",
            })
        stop = fav["stop"]
        if stop["total"]:
            rate = stop["withSell"] / stop["total"]
            need = [b["breakEvenWinRate"] for b in fav["priceBuckets"] if b.get("breakEvenWinRate") is not None]
            need_txt = f"{max(need)*100:.0f}%" if need else "96%"
            out.append({
                "title": "停損",
                "finding": f"市場上做這型態的錢包只有 {rate*100:.0f}% 曾在窗口內賣出（停損／獲利了結）；我們目前停損 {ours_stop}。",
                "pros": "不停損：省掉假停損成本，勝率夠高時期望值最高。" if rate < 0.2 else "多數人有停損，跟我們一致。",
                "cons": f"不停損時一次翻面整注歸零，依本週賠率需勝率 >= {need_txt} 才划算；低於此停損仍是必要保護。",
            })
    both = next((k for k in report["kinds"] if k["kind"] == "both_sides_lock"), None)
    if both:
        out.append({
            "title": "兩邊掛單鎖利（他們有、我們目前沒開）",
            "finding": f"本週 {both['wallets']} 個錢包、{both['count']} 個窗口做兩邊都買且加總 < $1，粗估 PnL {both['pnl']:+.0f}。",
            "pros": "無方向風險、被動收價差；掃描裡成交額最大的機器人多屬此類。",
            "cons": "需要掛單（maker）能力與較大資金；我們的紙上模擬版此前 200 筆為負，還沒找到可行參數。",
        })
    top = report["topBots"][:3]
    if top:
        out.append({
            "title": "本週最賺錢的三個錢包",
            "finding": "；".join(
                f"{b['wallet'][:10]}… {kind_label(b['mainKind'])}，{b['windows']} 窗、PnL {b['pnl']:+.0f}"
                + (f"、買價 {b['favAvgPrice']:.3f}、進場前 {b['favMedianT']:.0f}s、賣出率 {b['favSellRate']*100:.0f}%" if b['favAvgPrice'] else "")
                for b in top),
            "pros": "可對照他們的買價／時點調整我們的參數。", "cons": "PnL 為公開成交粗估（不含手續費、賣單另計），僅供方向參考。",
        })
    return out


# ── 輸出 ───────────────────────────────────────────────────────────────────

def render_markdown(report: dict, suggestions: list[dict], cfg: dict, hours: float) -> str:
    now = datetime.now(TAIPEI).strftime("%Y-%m-%d %H:%M")
    lines = [f"# 每週市場掃描 {now}（台北）", "",
             f"範圍：最近 {hours:.0f} 小時、{report['windows']} 個 BTC 5 分鐘窗口、{report['trades']:,} 筆成交；結算 {report['outcomes']}", "",
             "## 型態（依粗估 PnL 排序；錢包×窗口）", "", "| 型態 | 次數 | 佔比 | 錢包數 | 勝率 | 粗估 PnL |", "|---|---|---|---|---|---|"]
    for k in report["kinds"]:
        wr = f"{k['winRate']*100:.1f}%" if k["winRate"] is not None else "—"
        lines.append(f"| {k['label']} | {k['count']} | {k['share']*100:.1f}% | {k['wallets']} | {wr} | {k['pnl']:+.0f} |")
    fav = report["lateFavorite"]
    def _be(b):
        return f"{b['breakEvenWinRate']*100:.1f}%" if b.get("breakEvenWinRate") is not None else "—"
    def _lpw(b):
        return f"{b['lossesPerWin']:.1f}" if b.get("lossesPerWin") is not None else "—"
    lines += ["", f"## 最後 {LATE_SECONDS} 秒買領先方細分（n={fav['n']}）", "",
              "| 買價 | n | 勝率 | 打平需勝率 | 1 輸＝N 贏 | 每股淨利（等權平均） | 中位數 | 依股數加權 | 判定 |", "|---|---|---|---|---|---|---|---|---|"]
    for b in fav["priceBuckets"]:
        lines.append(f"| {b['lo']:.2f}～{b['hi']:.2f} | {b['n']} | {b['winRate']*100:.1f}% | {_be(b)} | {_lpw(b)} | {b['pnlPerShare']:+.4f} | {b.get('pnlPerShareMedian', 0):+.4f} | {b.get('pnlPerShareSizeWeighted', 0):+.4f} | {'❌ 負期望' if b.get('negativeEV') else '✅'} |")
    lines += ["", "| 進場前秒數 | n | 勝率 | 每股淨利（等權平均） | 中位數 |", "|---|---|---|---|---|"]
    for b in fav["timeBuckets"]:
        lines.append(f"| {b['lo']}～{b['hi']}s | {b['n']} | {b['winRate']*100:.1f}% | {b['pnlPerShare']:+.4f} | {b.get('pnlPerShareMedian', 0):+.4f} |")
    lines += ["", f"## 我們目前的實盤設定", "", f"`{json.dumps({k: cfg.get(k) for k in ('label','lateFavoriteMinPrice','lateFavoriteMaxPrice','lateFavoriteStopLossPrice','lateFavoriteWindowSeconds','stakePct')}, ensure_ascii=False)}`", "",
              "## 比對與建議（由使用者決定，不會自動更改）", ""]
    for s in suggestions:
        lines += [f"### {s['title']}", "", s["finding"], "", f"- 優點：{s['pros']}", f"- 缺點：{s['cons']}", ""]
    return "\n".join(lines)


def render_telegram(report: dict, suggestions: list[dict], hours: float) -> list[str]:
    msgs = []
    head = [f"📈 市場掃描 {report.get('marketLabel', 'BTC 5 分鐘')}（最近 {hours:.0f}h、{report['windows']} 窗、{report['trades']:,} 筆）", "本週最賺型態（依粗估 PnL）："]
    # 2026-09-15 依使用者要求：TG 摘要以收益為主，不列勝率（完整數字仍在 .md 報告）。
    for k in report["kinds"][:5]:
        head.append(f"• {k['label']}：粗估 {k['pnl']:+.0f}、{k['wallets']} 錢包、{k['share']*100:.0f}%")
    fav = report["lateFavorite"]
    head.append("買領先方各買價區間每股淨利（平均／中位；1 輸＝N 贏）：" + "；".join(
        f"{b['lo']:.2f}～{b['hi']:.2f} {b['pnlPerShare']:+.3f}／{b.get('pnlPerShareMedian', 0):+.3f}（{risk_text(b)}）" for b in fav["priceBuckets"]))
    msgs.append("\n".join(head))
    for s in suggestions:
        msgs.append(f"🔎 {s['title']}\n{s['finding']}\n👍 {s['pros']}\n👎 {s['cons']}")
    msgs.append("以上僅供參考，任何更改請回覆指示後再由人工執行。完整報告：reports/weekly/")
    return msgs


def send_telegram(messages: list[str]) -> None:
    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    ids = [int(v) for v in os.environ.get("TG_ALLOWED_USER_IDS", "").replace(" ", "").split(",") if v.isdigit()]
    if not token or not ids:
        log.warning("TG 未設定，略過推播")
        return
    with httpx.Client(timeout=20) as c:
        for uid in ids:
            for text in messages:
                try:
                    c.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": uid, "text": text[:4000]})
                except Exception as exc:
                    log.warning(f"sendMessage failed: {exc}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--market", default="btc", help="btc / btc-15m / eth / sol / xrp / other（全站探索）")
    ap.add_argument("--no-telegram", action="store_true")
    args = ap.parse_args()
    os.makedirs(REPORT_DIR, exist_ok=True)
    stamp = datetime.now(TAIPEI).strftime("%Y-%m-%d")
    if args.market == "other":
        with httpx.Client(timeout=25) as client:
            disc = discover_top_markets(client)
        with open(os.path.join(REPORT_DIR, f"{stamp}-discover.json"), "w", encoding="utf-8") as f:
            json.dump({"generatedAt": time.time(), "discover": disc}, f, ensure_ascii=False, indent=2)
        log.info(f"discovery written: {REPORT_DIR}/{stamp}-discover.json")
        if not args.no_telegram:
            send_telegram(render_discovery_telegram(disc))
        return
    if args.market not in MARKETS:
        raise SystemExit(f"未知市場 {args.market}，可用：{', '.join(MARKETS)} 或 other")
    with httpx.Client(timeout=25) as client:
        windows = fetch_windows(args.hours, client, args.market)
    if not windows:
        log.error("沒有抓到任何窗口")
        return
    report = analyze(windows)
    report["market"] = args.market
    report["marketLabel"] = MARKETS[args.market]["label"]
    cfg = asyncio.run(fetch_live_config())
    suggestions = compare(report, cfg)
    suffix = "" if args.market == "btc" else f"-{args.market}"
    with open(os.path.join(REPORT_DIR, f"{stamp}{suffix}.json"), "w", encoding="utf-8") as f:
        json.dump({"generatedAt": time.time(), "hours": args.hours, "market": args.market, "liveConfig": cfg, "report": report, "suggestions": suggestions}, f, ensure_ascii=False, indent=2)
    md = render_markdown(report, suggestions, cfg, args.hours)
    with open(os.path.join(REPORT_DIR, f"{stamp}{suffix}.md"), "w", encoding="utf-8") as f:
        f.write(md)
    log.info(f"report written: {REPORT_DIR}/{stamp}{suffix}.md")
    if not args.no_telegram:
        send_telegram(render_telegram(report, suggestions, args.hours))


if __name__ == "__main__":
    main()
