"""
實盤虧損原因分析（純讀取；給 TG /loss 用，也可直接 python polymarket_loss_analysis.py <狀態檔> <資產id> <變體id>）。

2026-09-20 依使用者要求：把之前人工做的「為什麼這筆會輸」分析變成指令。每筆真實虧損單：
    - 進場時：現貨相對開盤的領先幅度、對邊 ask（訂單簿一致性）、模型 fair
    - 進場後：何時跌破 0.5（翻面）、持有腿最低 bid、是否觸發停損
    - 主模擬盤同變體同窗口有沒有進（鏡像一致性）
    - 判定：薄單假領先／領先幅度薄／停損（真／假）／真實反轉／模擬盤沒進（執行差異）
資料來源：實盤狀態檔（成交）＋主模擬盤 sqlite 的 sim_quotes（每 ~3 秒取樣）與 sim_trades。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

TP = timezone(timedelta(hours=8))
THIN_BOOK_OTHER_ASK = 0.12     # 對邊 ask 超過這個值（兩邊 ask 合計 > ~1.03）視為薄單假領先
THIN_LEAD_PCT = 0.02           # 進場時現貨領先幅度低於 0.02% 視為「領先薄」


# 窗口 slug 前綴 → 模擬盤資產 id（實盤可能在虧損後已換策略，所以資產一律從成交的 slug 推回，不用目前 env）
_SLUG_ASSET = {"btc-updown-5m-": "btc", "btc-updown-15m-": "btc-15m", "eth-updown-5m-": "eth-alt", "eth-updown-15m-": "eth-15m",
               "xrp-updown-5m-": "xrp", "xrp-updown-15m-": "xrp-15m", "sol-updown-5m-": "sol", "sol-updown-15m-": "sol-15m",
               "doge-updown-5m-": "doge", "bnb-updown-5m-": "bnb"}


def asset_from_slug(slug: str, default: str) -> str:
    for prefix, aid in _SLUG_ASSET.items():
        if str(slug).startswith(prefix):
            return aid
    return default


def _ts(t: float | None) -> str:
    return datetime.fromtimestamp(float(t), TP).strftime("%m-%d %H:%M") if t else "—"


def _window_quotes(db: sqlite3.Connection, asset_id: str, slug: str) -> list[dict]:
    rows = db.execute(
        "SELECT ts, remaining_seconds, up_ask, down_ask, up_bid, down_bid, fair_up, spot_price FROM sim_quotes "
        "WHERE asset_id=? AND window_slug=? ORDER BY ts", (asset_id, slug)).fetchall()
    keys = ("ts", "rem", "up_ask", "down_ask", "up_bid", "down_bid", "fair_up", "spot")
    return [dict(zip(keys, r)) for r in rows]


def _sim_same_window(db: sqlite3.Connection, variant_id: str, slug: str, asset_id: str | None = None, side: str | None = None) -> dict | None:
    """主模擬盤同變體同窗口的成交；舊成交沒記 variantId 時（variant_id 為空）退而找同資產、同方向的任一買領先方變體。"""
    try:
        if variant_id:
            row = db.execute(
                "SELECT trade_json FROM sim_trades WHERE variant_id=? AND json_extract(trade_json,'$.windowSlug')=? ORDER BY exit_time DESC LIMIT 1",
                (variant_id, slug)).fetchone()
            return json.loads(row[0]) if row else None
        if asset_id:
            for (tj,) in db.execute(
                    "SELECT trade_json FROM sim_trades WHERE variant_id LIKE ? AND json_extract(trade_json,'$.windowSlug')=? ORDER BY exit_time DESC",
                    (f"{asset_id}-%", slug)):
                t = json.loads(tj)
                if side is None or t.get("side") == side:
                    return dict(t, _anyVariant=True)
    except sqlite3.Error:
        return None
    return None


def analyze_trade(trade: dict, quotes: list[dict], sim_trade: dict | None) -> dict:
    """單筆虧損單的分析（純函式，方便測試）。"""
    side = trade.get("side")
    entry_t = float(trade.get("entryTime") or 0)
    held_bid = "up_bid" if side == "Up" else "down_bid"
    other_ask_key = "down_ask" if side == "Up" else "up_ask"
    out: dict = {"trade": trade, "openSpot": None, "leadPct": None, "leadUsd": None, "otherAsk": None, "fair": None,
                 "flipAfterSec": None, "flipRemaining": None, "minBid": None, "endSpot": None, "simEntered": sim_trade is not None,
                 "simPnl": (sim_trade or {}).get("pnl"), "simAnyVariant": bool((sim_trade or {}).get("_anyVariant"))}
    with_spot = [q for q in quotes if q.get("spot") is not None]
    if with_spot:
        out["openSpot"] = with_spot[0]["spot"]
        out["endSpot"] = with_spot[-1]["spot"]
    at = min((q for q in quotes if abs(float(q["ts"]) - entry_t) <= 6), key=lambda q: abs(float(q["ts"]) - entry_t), default=None)
    if at:
        if at.get("spot") is not None and out["openSpot"]:
            sign = 1 if side == "Up" else -1
            out["leadUsd"] = sign * (at["spot"] - out["openSpot"])
            out["leadPct"] = out["leadUsd"] / out["openSpot"] * 100
        out["otherAsk"] = at.get(other_ask_key)
        fu = at.get("fair_up")
        out["fair"] = (fu if side == "Up" else (1 - fu)) if fu is not None else None
    after = [q for q in quotes if float(q["ts"]) > entry_t and q.get(held_bid) is not None]
    if after:
        out["minBid"] = min(float(q[held_bid]) for q in after)
        flip = next((q for q in after if float(q[held_bid]) < 0.5), None)
        if flip:
            out["flipAfterSec"] = float(flip["ts"]) - entry_t
            out["flipRemaining"] = flip.get("rem")
    trade_type = str(trade.get("tradeType") or "")
    won_anyway = (trade.get("outcome") == side)
    if "stop" in trade_type:
        out["kind"] = "假停損（結算其實會贏）" if won_anyway else "停損（真翻面）"
    elif out["otherAsk"] is not None and float(out["otherAsk"]) > THIN_BOOK_OTHER_ASK:
        out["kind"] = "薄單假領先（對邊 ask 沒跟著掉）"
    elif out["leadPct"] is not None and out["leadPct"] < THIN_LEAD_PCT:
        out["kind"] = "真實反轉（領先幅度薄，市場過度自信）"
    else:
        out["kind"] = "真實反轉（進場後行情反向）"
    if not out["simEntered"]:
        out["kind"] += "；模擬盤同窗沒進（執行差異）"
    return out


def format_analysis(name: str, items: list[dict], wins: list[dict]) -> str:
    if not items:
        return f"{name}：最近沒有真實虧損單。"
    lines = [f"🔎 {name} 最近 {len(items)} 筆真實虧損："]
    for i, a in enumerate(items, 1):
        t = a["trade"]; sh = float(t.get("shares") or 0); ep = float(t.get("entryPrice") or 0)
        lines.append(f"{i}. {_ts(t.get('entryTime'))} {t.get('side')} {ep:.2f}×{sh:.1f}=${ep * sh:.1f} → {t.get('outcome')}  {float(t.get('pnlEstimate') or 0):+.2f}")
        entry_bits = []
        if a["leadPct"] is not None:
            entry_bits.append(f"現貨 {a['leadUsd']:+.1f}（{a['leadPct']:+.3f}%）{'領先極薄' if a['leadPct'] < THIN_LEAD_PCT else '領先'}")
        if a["otherAsk"] is not None:
            entry_bits.append(f"對邊 ask {float(a['otherAsk']):.2f}（{'訂單簿不一致' if float(a['otherAsk']) > THIN_BOOK_OTHER_ASK else '一致'}）")
        if a["fair"] is not None:
            entry_bits.append(f"fair {a['fair']:.2f}")
        if entry_bits:
            lines.append("   進場時：" + " · ".join(entry_bits))
        after_bits = []
        if a["flipAfterSec"] is not None:
            after_bits.append(f"進場後 {a['flipAfterSec']:.0f} 秒翻面（剩 {float(a['flipRemaining'] or 0):.0f}s）")
        elif a["minBid"] is not None:
            after_bits.append("取樣內未見翻面")
        if a["minBid"] is not None:
            after_bits.append(f"最低 bid {a['minBid']:.2f}")
        if a["openSpot"] and a["endSpot"]:
            after_bits.append(f"收盤現貨 {a['endSpot'] - a['openSpot']:+.1f}")
        if after_bits:
            lines.append("   進場後：" + " · ".join(after_bits))
        sim_txt = f"有進（{float(a['simPnl']):+.1f}）" if a["simEntered"] and a["simPnl"] is not None else ("有進" if a["simEntered"] else "沒進")
        if a.get("simAnyVariant"):
            sim_txt += "（同資產變體）"
        lines.append(f"   模擬盤同窗：{sim_txt} · 判定：{a['kind']}")
    kinds: dict[str, int] = {}
    for a in items:
        k = a["kind"].split("（")[0].split("；")[0]
        kinds[k] = kinds.get(k, 0) + 1
    total = sum(float(a["trade"].get("pnlEstimate") or 0) for a in items)
    summary = "、".join(f"{k} {n} 筆" for k, n in kinds.items())
    lines.append(f"合計 {total:+.2f}：{summary}")
    if wins:
        avg_win = sum(float(t.get("pnlEstimate") or 0) for t in wins) / len(wins)
        avg_loss = -total / len(items)
        if avg_win > 0:
            lines.append(f"同期贏單 {len(wins)} 筆平均 {avg_win:+.2f}/筆 → 一次輸 ≈ {avg_loss / avg_win:.0f} 次贏")
    return "\n".join(lines)


def analyze_losses(state_path: str, asset_id: str, variant_id: str, db_path: str, limit: int = 5, name: str = "實盤") -> str:
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)
    real = [t for t in state.get("trades", []) if not t.get("dryRun", True)]
    losses = sorted((t for t in real if float(t.get("pnlEstimate") or 0) <= 0), key=lambda t: float(t.get("exitTime") or 0), reverse=True)[:limit]
    if not losses:
        return format_analysis(name, [], [])
    since = min(float(t.get("exitTime") or 0) for t in losses)
    wins = [t for t in real if float(t.get("pnlEstimate") or 0) > 0 and float(t.get("exitTime") or 0) >= since]
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        items = []
        for t in losses:
            aid = asset_from_slug(t["windowSlug"], asset_id)
            # 成交有記 variantId 就精準對；沒有（舊紀錄）就退而找同資產、同方向的任一買領先方變體
            sim_t = _sim_same_window(db, str(t.get("variantId") or ""), t["windowSlug"], aid, t.get("side"))
            items.append(analyze_trade(t, _window_quotes(db, aid, t["windowSlug"]), sim_t))
    finally:
        db.close()
    return format_analysis(name, items, wins)


if __name__ == "__main__":
    import os
    state, asset, vid = sys.argv[1], sys.argv[2], sys.argv[3]
    db = sys.argv[4] if len(sys.argv) > 4 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_sim.sqlite3")
    print(analyze_losses(state, asset, vid, db, int(sys.argv[5]) if len(sys.argv) > 5 else 5))
