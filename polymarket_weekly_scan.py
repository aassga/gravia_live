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

TAIPEI = timezone(timedelta(hours=8))
REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports", "weekly")
GAMMA = "https://gamma-api.polymarket.com/markets"
DATA_API = "https://data-api.polymarket.com/trades"
LIVE_WS = os.environ.get("TG_LIVE_STATUS_WS", "ws://127.0.0.1:8767")
WINDOW_SECONDS = 300
LATE_SECONDS = 60          # 「最後 60 秒」型態的判定
FAVORITE_MIN_ASK = 0.88    # 買領先方型態的最低買價（比我們的 0.95 寬，才看得到整條曲線）
MIN_WALLET_WINDOWS = 10    # 至少做過這麼多窗口才算「機器人／常用者」


# ── 抓資料 ─────────────────────────────────────────────────────────────────

def fetch_windows(hours: float, client: httpx.Client) -> list[dict]:
    now = int(time.time())
    last_closed = now // WINDOW_SECONDS * WINDOW_SECONDS - 2 * WINDOW_SECONDS
    n = int(hours * 3600 // WINDOW_SECONDS)
    windows = []
    for i in range(n):
        ws = last_closed - WINDOW_SECONDS * i
        slug = f"btc-updown-5m-{ws}"
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
    if sells and not sold:
        info["kind"] = "buy_then_sell_other"
    elif sold:
        info["kind"] = "buy_then_sell"
    elif min(rel) < 0:
        info["kind"] = "pre_open"
    elif max(rel) >= WINDOW_SECONDS - LATE_SECONDS and vw >= FAVORITE_MIN_ASK:
        info["kind"] = "late_favorite"
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
    "late_favorite": "最後 60 秒買領先方（>=0.88）",
    "last_60s_directional": "最後 60 秒單邊方向性（<0.88）",
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
    for k, n in kind_count.most_common():
        kinds.append({
            "kind": k, "label": KIND_LABELS.get(k, k), "count": n, "share": n / total,
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
                sh = sum(i["shares"] for i in g)
                out.append({"lo": lo, "hi": hi, "n": len(g), "winRate": sum(i["won"] for i in g) / len(g),
                            "pnlPerShare": sum(i["pnl"] for i in g) / sh if sh else 0.0})
        return out
    fav_price = bucket(fav, "vwap", [0.88, 0.92, 0.95, 0.98, 1.01])
    fav_time = bucket(fav, "tBeforeClose", [0, 10, 20, 30, 45, 61])
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
        best = max(fav["priceBuckets"], key=lambda b: b["pnlPerShare"])
        ours = [b for b in fav["priceBuckets"] if ours_min is not None and b["lo"] <= ours_min < b["hi"]]
        finding = "市場買價區間表現：" + "；".join(
            f"{b['lo']:.2f}～{b['hi']:.2f} 勝率 {b['winRate']*100:.1f}%、每股 {b['pnlPerShare']:+.4f}（n={b['n']}）" for b in fav["priceBuckets"])
        if ours and best is not ours[0]:
            out.append({
                "title": "買價區間",
                "finding": finding + f"。我們目前 {ours_min:.2f}～{ours_max:.2f}，落在勝率 {ours[0]['winRate']*100:.1f}% 的區間；本週最佳是 {best['lo']:.2f}～{best['hi']:.2f}。",
                "pros": f"改到 {best['lo']:.2f}～{best['hi']:.2f}：每股淨利 {best['pnlPerShare']:+.4f} vs 我們區間 {ours[0]['pnlPerShare']:+.4f}。",
                "cons": "區間越貴每股毛利越薄、對停損跳空更敏感；樣本只有一週，需連續兩週一致再改。",
            })
        else:
            out.append({"title": "買價區間", "finding": finding + f"。我們目前 {ours_min}～{ours_max}，已在本週最佳區間。", "pros": "維持。", "cons": "—"})
        if fav["timeBuckets"]:
            bt = max(fav["timeBuckets"], key=lambda b: b["pnlPerShare"])
            out.append({
                "title": "進場時點",
                "finding": "進場前秒數：" + "；".join(f"{b['lo']}～{b['hi']}s 勝率 {b['winRate']*100:.1f}%、每股 {b['pnlPerShare']:+.4f}" for b in fav["timeBuckets"]) + f"。我們目前 {cfg.get('lateFavoriteMinRemaining', 5):.0f}～{cfg.get('lateFavoriteWindowSeconds', 60):.0f}s 都可進。",
                "pros": f"若只在 {bt['lo']}～{bt['hi']}s 進，每股可到 {bt['pnlPerShare']:+.4f}。",
                "cons": "縮窄時間會少掉機會；最後 10 秒內深度薄、交易所偶爾關單。",
            })
        stop = fav["stop"]
        if stop["total"]:
            rate = stop["withSell"] / stop["total"]
            out.append({
                "title": "停損",
                "finding": f"市場上做這型態的錢包只有 {rate*100:.0f}% 曾在窗口內賣出（停損／獲利了結）；我們目前停損 {ours_stop}。",
                "pros": "不停損：省掉假停損成本，在 99% 勝率下期望值最高。" if rate < 0.2 else "多數人有停損，跟我們一致。",
                "cons": "不停損時一次翻面整注歸零，需勝率 >= 96% 才划算；我們實測勝率若低於此，停損仍是必要保護。",
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
                f"{b['wallet'][:10]}… {KIND_LABELS.get(b['mainKind'], b['mainKind'])}，{b['windows']} 窗、PnL {b['pnl']:+.0f}"
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
             "## 最多人使用的型態（錢包×窗口）", "", "| 型態 | 次數 | 佔比 | 錢包數 | 勝率 | 粗估 PnL |", "|---|---|---|---|---|---|"]
    for k in report["kinds"]:
        wr = f"{k['winRate']*100:.1f}%" if k["winRate"] is not None else "—"
        lines.append(f"| {k['label']} | {k['count']} | {k['share']*100:.1f}% | {k['wallets']} | {wr} | {k['pnl']:+.0f} |")
    fav = report["lateFavorite"]
    lines += ["", f"## 最後 60 秒買領先方細分（n={fav['n']}）", "", "| 買價 | n | 勝率 | 每股淨利 |", "|---|---|---|---|"]
    for b in fav["priceBuckets"]:
        lines.append(f"| {b['lo']:.2f}～{b['hi']:.2f} | {b['n']} | {b['winRate']*100:.1f}% | {b['pnlPerShare']:+.4f} |")
    lines += ["", "| 進場前秒數 | n | 勝率 | 每股淨利 |", "|---|---|---|---|"]
    for b in fav["timeBuckets"]:
        lines.append(f"| {b['lo']}～{b['hi']}s | {b['n']} | {b['winRate']*100:.1f}% | {b['pnlPerShare']:+.4f} |")
    lines += ["", f"## 我們目前的實盤設定", "", f"`{json.dumps({k: cfg.get(k) for k in ('label','lateFavoriteMinPrice','lateFavoriteMaxPrice','lateFavoriteStopLossPrice','lateFavoriteWindowSeconds','stakePct')}, ensure_ascii=False)}`", "",
              "## 比對與建議（由使用者決定，不會自動更改）", ""]
    for s in suggestions:
        lines += [f"### {s['title']}", "", s["finding"], "", f"- 優點：{s['pros']}", f"- 缺點：{s['cons']}", ""]
    return "\n".join(lines)


def render_telegram(report: dict, suggestions: list[dict], hours: float) -> list[str]:
    msgs = []
    head = [f"📈 每週市場掃描（最近 {hours:.0f}h、{report['windows']} 窗、{report['trades']:,} 筆）", "最多人使用："]
    for k in report["kinds"][:5]:
        wr = f" 勝率 {k['winRate']*100:.0f}%" if k["winRate"] is not None else ""
        head.append(f"• {k['label']}：{k['share']*100:.0f}%、{k['wallets']} 錢包{wr}")
    fav = report["lateFavorite"]
    head.append("買領先方各買價區間：" + "；".join(f"{b['lo']:.2f}～{b['hi']:.2f} {b['winRate']*100:.0f}%/{b['pnlPerShare']:+.3f}" for b in fav["priceBuckets"]))
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
    ap.add_argument("--no-telegram", action="store_true")
    args = ap.parse_args()
    with httpx.Client(timeout=25) as client:
        windows = fetch_windows(args.hours, client)
    if not windows:
        log.error("沒有抓到任何窗口")
        return
    report = analyze(windows)
    cfg = asyncio.run(fetch_live_config())
    suggestions = compare(report, cfg)
    os.makedirs(REPORT_DIR, exist_ok=True)
    stamp = datetime.now(TAIPEI).strftime("%Y-%m-%d")
    with open(os.path.join(REPORT_DIR, f"{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump({"generatedAt": time.time(), "hours": args.hours, "liveConfig": cfg, "report": report, "suggestions": suggestions}, f, ensure_ascii=False, indent=2)
    md = render_markdown(report, suggestions, cfg, args.hours)
    with open(os.path.join(REPORT_DIR, f"{stamp}.md"), "w", encoding="utf-8") as f:
        f.write(md)
    log.info(f"report written: {REPORT_DIR}/{stamp}.md")
    if not args.no_telegram:
        send_telegram(render_telegram(report, suggestions, args.hours))


if __name__ == "__main__":
    main()
