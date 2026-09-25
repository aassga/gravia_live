"""
模擬盤自動體檢／調參（2026-09-23 依使用者要求，每 3 天跑一次，不經過 TG、不詢問、直接改）。

每次執行：
  1. 有成交的變體：逐筆分析虧損原因，並在真實報價路徑上回放所有出場設定（價格停損 0.40～0.95 每 0.01、
     金額停損＝成本的 2%～60%、以及不停損），用「每筆平均損益 + 1 標準誤」規則挑最佳設定——只有在
     統計上確實優於目前設定時才改（樣本不足或差距在誤差內就不動），把誤判降到最低。
  2. 沒有成交的變體：看窗口診斷裡卡住的主因（沒有領先方／超出時間窗／領先幅度不足／訂單簿不一致…），
     自動放寬那一項；連續放寬 MAX_LOOSEN 次仍無成交就停用並移除。
  3. 樣本足夠但最佳設定仍是負期望的變體：停用並移除（繼續測試沒有意義）。
  4. 每個被調整的變體都會清空原本紀錄（備份後重來），避免新舊設定的成績混在一起。

只碰模擬盤：sim_auto_variants.json（自動變體規格）、sim_variant_overrides.json（停損覆寫，主進程熱更新）、
sim_disabled_variants.json（停用清單）與 sqlite 內該變體的紀錄；不碰 .env、不碰實盤。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("sim-doctor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

TAIPEI = timezone(timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("POLY_SIM_DB_PATH", os.path.join(HERE, "polymarket_sim.sqlite3"))
AUTO_FILE = os.environ.get("POLY_SIM_AUTO_VARIANTS_FILE", os.path.join(HERE, "sim_auto_variants.json"))
DISABLED_FILE = os.environ.get("POLY_SIM_DISABLED_VARIANTS_FILE", os.path.join(HERE, "sim_disabled_variants.json"))
OVERRIDES_FILE = os.environ.get("POLY_SIM_VARIANT_OVERRIDES_FILE", os.path.join(HERE, "sim_variant_overrides.json"))
REPORT_DIR = os.path.join(HERE, "reports", "doctor")
BACKUP_DIR = os.path.join(HERE, "backup")
SIM_WS = os.environ.get("TG_SIM_STATUS_WS", "ws://127.0.0.1:8766")

MIN_TRADES_FOR_EXIT_TUNE = 12     # 少於這麼多筆不動出場設定（樣本太少改了也是雜訊）
MIN_TRADES_FOR_KILL = 40          # 少於這麼多筆不判死刑
MIN_LOSSES_FOR_STOP = 2           # 至少要有這麼多筆虧損才考慮加停損
SE_MARGIN = 1.0                   # 新設定的每筆平均要贏過舊設定 1 個標準誤才改
MAX_LOOSEN = 3                    # 沒成交的變體最多自動放寬幾次
NO_TRADE_MIN_WINDOWS = 30         # 至少觀察過這麼多個窗口才判定「條件太嚴」
TICK = 0.01
STOP_PRICE_GRID = [round(0.40 + i * TICK, 2) for i in range(int((0.95 - 0.40) / TICK) + 1)]
USD_STOP_FRACTIONS = [round(0.02 + i * 0.02, 2) for i in range(30)]   # 成本的 2%～60%

SINGLE_LEG_FLAGS = ("lateFavorite", "openMomentum", "openReversal", "followWallets", "lateUnderdog")


# ── 檔案 ──────────────────────────────────────────────────────────────────

def _load(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save(path, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def taker_fee(shares: float, price: float, rate: float = 0.02, exponent: float = 1.0) -> float:
    fee = round(shares * rate * (price * (1 - price)) ** exponent, 5)
    return fee if fee >= 0.00001 else 0.0


# ── 讀資料 ────────────────────────────────────────────────────────────────

def _ro(db_path: str = DB_PATH) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def run_id(db: sqlite3.Connection) -> int:
    row = db.execute("SELECT value FROM sim_meta WHERE key='shared_config'").fetchone()
    return int(json.loads(row[0])["runId"]) if row else 0


def variant_trades(db: sqlite3.Connection, rid: int, vid: str) -> list[dict]:
    return [json.loads(r[0]) for r in db.execute(
        "SELECT trade_json FROM sim_trades WHERE run_id=? AND variant_id=? ORDER BY exit_time", (rid, vid))]


def variant_diagnostics(db: sqlite3.Connection, rid: int, vid: str) -> list[dict]:
    return [json.loads(r[0]) for r in db.execute(
        "SELECT diagnostic_json FROM sim_window_diagnostics WHERE run_id=? AND variant_id=?", (rid, vid))]


def bid_path(db: sqlite3.Connection, asset_id: str, slug: str, side: str, after_ts: float) -> list[tuple[float, float]]:
    """進場後持有腿的最佳 bid 路徑 [(ts, bid)]。"""
    col = "up_bid" if side == "Up" else "down_bid"
    return [(float(ts), float(b)) for ts, b in db.execute(
        f"SELECT ts, {col} FROM sim_quotes WHERE asset_id=? AND window_slug=? AND ts>? AND {col} IS NOT NULL ORDER BY ts",
        (asset_id, slug, after_ts))]


# ── 出場設定回放（精算） ──────────────────────────────────────────────────

def trade_cost(trade: dict) -> float:
    shares = float(trade["shares"])
    return float(trade.get("stakeUsd") or 0) or (float(trade["entryPrice"]) * shares + float(trade.get("entryFee") or 0))


def hold_pnl(trade: dict, path: list[tuple[float, float]]) -> float | None:
    """抱到結算的損益。實際就是抱到結算的單直接用紀錄；被停損掃出的單要從報價路徑末端還原
    （最後 bid >= 0.9 = 那邊贏、<= 0.1 = 輸），還原不了就回 None（這筆不列入回放）。"""
    if "stop" not in str(trade.get("exitReason") or "") and str(trade.get("outcome")) not in ("EarlyExit", "", "None", "None"):
        return float(trade["pnl"])
    if not path:
        return None
    last = path[-1][1]
    shares = float(trade["shares"])
    if last >= 0.9:
        return shares - trade_cost(trade)
    if last <= 0.1:
        return -trade_cost(trade)
    return None


def replay_trade(trade: dict, path: list[tuple[float, float]], stop_price: float | None, stop_usd: float | None,
                 baseline: float | None = None) -> float | None:
    """在真實 bid 路徑上套用出場設定後的單筆損益；沒觸發就用「抱到結算」的損益（baseline）。
    觸發判斷看最佳 bid（跟線上邏輯一致），成交價保守估在 bid 下一檔。"""
    base = hold_pnl(trade, path) if baseline is None else baseline
    if stop_price is None and stop_usd is None:
        return base
    if base is None:
        return None
    shares = float(trade["shares"])
    cost = trade_cost(trade)
    for _ts, bid in path:
        mark = bid * shares - taker_fee(shares, bid) - cost
        hit = (stop_usd is not None and mark <= -stop_usd + 1e-9) or (stop_price is not None and bid <= stop_price + 1e-9)
        if hit:
            px = max(0.01, round(bid - TICK, 4))
            return px * shares - taker_fee(shares, px) - cost
    return base


def _stats(values: list[float]) -> tuple[float, float]:
    """回傳 (每筆平均, 平均的標準誤)。"""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    if n < 2:
        return mean, abs(mean)
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(var / n)


def best_exit_config(items: list[dict], current_price, current_usd) -> dict:
    """items: [{trade, path}]。回放所有設定，回傳最佳與目前設定的比較。
    只有新設定的每筆平均贏過目前設定 SE_MARGIN 個標準誤才建議更換。"""
    usable = [dict(i, base=hold_pnl(i["trade"], i["path"])) for i in items]
    usable = [i for i in usable if i["base"] is not None and i["path"]]
    if not usable:
        return {"current": None, "best": None, "margin": 0.0, "threshold": 0.0, "change": False, "usable": 0}
    med_cost = sorted(trade_cost(i["trade"]) for i in usable)[len(usable) // 2] or 1.0
    candidates: list[tuple[float | None, float | None]] = [(None, None)]
    candidates += [(p, None) for p in STOP_PRICE_GRID]
    candidates += [(None, round(f * med_cost, 2)) for f in USD_STOP_FRACTIONS]
    if (current_price, current_usd) not in candidates:
        candidates.append((current_price, current_usd))
    scored = []
    for sp, su in candidates:
        pnls = [replay_trade(i["trade"], i["path"], sp, su, i["base"]) for i in usable]
        mean, se = _stats(pnls)
        scored.append({"stopPrice": sp, "stopUsd": su, "mean": mean, "se": se, "net": sum(pnls),
                       "wins": sum(1 for v in pnls if v > 0), "losses": sum(1 for v in pnls if v <= 0)})
    cur = next(s for s in scored if s["stopPrice"] == current_price and s["stopUsd"] == current_usd)
    best = max(scored, key=lambda s: s["mean"])
    margin = best["mean"] - cur["mean"]
    threshold = SE_MARGIN * math.sqrt(best["se"] ** 2 + cur["se"] ** 2)
    return {"current": cur, "best": best, "margin": margin, "threshold": threshold, "usable": len(usable),
            "change": margin > threshold and (best["stopPrice"] != cur["stopPrice"] or best["stopUsd"] != cur["stopUsd"])}


# ── 虧損原因 ──────────────────────────────────────────────────────────────

def loss_reasons(items: list[dict]) -> dict[str, int]:
    """逐筆虧損歸因：停損出場／進場後跳空翻面／慢慢走弱／假停損。"""
    out: dict[str, int] = {}
    for i in items:
        t = i["trade"]
        if float(t["pnl"]) > 0:
            continue
        reason = str(t.get("exitReason") or "")
        if "stop" in reason:
            key = "假停損（結算其實會贏）" if t.get("outcome") == t.get("side") else "停損出場（真翻面）"
        else:
            path = i["path"]
            first = path[0][1] if path else None
            low = min((b for _, b in path), default=None)
            if first is not None and low is not None and first - low >= 0.30:
                key = "進場後跳空翻面"
            elif low is not None and low >= 0.5:
                key = "結算前未翻面但結算輸（貼身翻盤）"
            else:
                key = "行情緩步反向"
        out[key] = out.get(key, 0) + 1
    return out


# ── 沒成交：找卡住的條件並放寬 ────────────────────────────────────────────

LOOSEN_RULES = [
    # (診斷原因, 參數, 放寬函式, 說明)
    ("favorite_no_leader", "favoriteMinPrice", lambda v: max(0.80, round(v - 0.02, 2)), "買價下限下調 0.02"),
    ("favorite_price_above_maximum", "favoriteMaxPrice", lambda v: min(0.99, round(v + 0.01, 2)), "買價上限上調 0.01"),
    ("favorite_lead_below_minimum", "favoriteMinLeadPct", lambda v: (None if v <= 0.005 else round(v / 2, 4)), "領先幅度門檻減半"),
    ("favorite_book_inconsistent", "favoriteMaxPairAskSum", lambda v: min(1.10, round(v + 0.02, 2)), "訂單簿一致性放寬 0.02"),
    ("favorite_not_stable_yet", "favoriteStableSeconds", lambda v: (None if v <= 2 else round(v / 2, 1)), "穩定秒數減半"),
    ("underdog_no_cheap_side", "underdogMaxPrice", lambda v: min(0.45, round(v + 0.05, 2)), "便宜邊上限上調 0.05"),
    ("momentum_below_minimum", "openMinMovePct", lambda v: (None if v <= 0.005 else round(v / 2, 4)), "動能門檻減半"),
    ("momentum_price_above_maximum", "openMaxPrice", lambda v: min(0.80, round(v + 0.05, 2)), "買價上限上調 0.05"),
]
CAPITAL_REASONS = ("insufficient_budget", "insufficient_ask_depth", "below_minimum_shares")


def plan_loosening(variant: dict, diags: list[dict]) -> dict | None:
    """從診斷找出擋最多次的條件並放寬一級；回傳 {param, from, to, note, reason} 或 None。"""
    counts: dict[str, int] = {}
    for w in diags:
        for reason, n in (w.get("reasonCounts") or {}).items():
            if reason == "outside_entry_window":
                continue   # 時間窗是設計的一部分，單獨處理
            counts[reason] = counts.get(reason, 0) + int(n)
    if not counts:
        # 全部都卡在 outside_entry_window：時間窗太窄 → 放寬
        if variant.get("lateFavorite") or variant.get("lateUnderdog"):
            key_win = "favoriteWindowSeconds" if variant.get("lateFavorite") else "underdogWindowSeconds"
            key_min = "favoriteMinRemaining" if variant.get("lateFavorite") else "underdogMinRemaining"
            win = float(variant.get(key_win) or 60.0)
            return {"param": key_win, "from": win, "to": round(win * 1.5, 1), "note": "進場時間窗放大 1.5 倍",
                    "reason": "outside_entry_window", "extra": {key_min: max(5.0, float(variant.get(key_min) or 5.0) / 1.5)}}
        return None
    top = max(counts.items(), key=lambda kv: kv[1])[0]
    if top in CAPITAL_REASONS:
        return {"param": None, "note": "卡在資金／深度不足，不是條件問題", "reason": top}
    for reason, param, fn, note in LOOSEN_RULES:
        if reason != top:
            continue
        cur = variant.get(param)
        if cur is None:
            return {"param": None, "note": f"{reason} 但沒有對應參數可放寬", "reason": reason}
        nxt = fn(float(cur))
        if nxt == cur:
            return {"param": None, "note": f"{param} 已到放寬上限", "reason": reason}
        return {"param": param, "from": cur, "to": nxt, "note": note, "reason": reason}
    return {"param": None, "note": f"主因 {top} 沒有自動放寬規則", "reason": top}


# ── 套用 ──────────────────────────────────────────────────────────────────

def reset_variant_records(vid: str, db_path: str = DB_PATH, backup_dir: str = BACKUP_DIR) -> str:
    """清空該變體的成交與診斷（備份後），狀態重設、啟用時間改成現在。主進程需重啟後才會重新載入。"""
    os.makedirs(backup_dir, exist_ok=True)
    db = sqlite3.connect(db_path)
    try:
        cfg = json.loads(db.execute("SELECT value FROM sim_meta WHERE key='shared_config'").fetchone()[0])
        trades = [json.loads(r[0]) for r in db.execute("SELECT trade_json FROM sim_trades WHERE variant_id=?", (vid,))]
        state = db.execute("SELECT state_json FROM sim_state WHERE variant_id=?", (vid,)).fetchone()
        path = os.path.join(backup_dir, f"doctor_{vid}_{int(time.time())}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"trades": trades, "state": json.loads(state[0]) if state else None}, f, ensure_ascii=False)
        n1 = db.execute("DELETE FROM sim_trades WHERE variant_id=?", (vid,)).rowcount
        n2 = db.execute("DELETE FROM sim_window_diagnostics WHERE variant_id=?", (vid,)).rowcount
        fresh = {"enabledAt": time.time(), "totalStaked": 0.0}
        if cfg.get("startBalance"):
            fresh["peakPortfolio"] = float(cfg["startBalance"])
        db.execute("INSERT OR REPLACE INTO sim_state (variant_id, run_id, state_json, updated_at) VALUES (?,?,?,?)",
                   (vid, int(cfg["runId"]), json.dumps(fresh), time.time()))
        db.commit()
        return f"清空 {n1} 筆成交、{n2} 筆診斷（備份 {os.path.basename(path)}）"
    finally:
        db.close()


def restart_sim() -> bool:
    # 2026-09-25：Windows／沒有 systemd 的環境不呼叫 systemctl——模擬盤會自己偵測設定檔變更並重啟。
    if os.name == "nt" or not shutil.which("systemctl"):
        log.info("非 systemd 環境：設定已寫入，模擬盤會在數秒內自行重新載入")
        return True
    try:
        subprocess.run(["sudo", "-n", "systemctl", "restart", "gravia.service"], check=True, timeout=120)
        return True
    except Exception as exc:
        log.error(f"restart gravia.service failed: {exc}")
        return False


async def sim_snapshot() -> dict:
    try:
        import websockets
        async with websockets.connect(SIM_WS, open_timeout=8, max_size=None) as ws:
            return json.loads(await asyncio.wait_for(ws.recv(), 15))
    except Exception as exc:
        log.warning(f"sim snapshot unavailable: {exc}")
        return {}


# ── 主流程 ────────────────────────────────────────────────────────────────

def diagnose(variants: list[dict], db: sqlite3.Connection, rid: int) -> list[dict]:
    """對每個變體產生診斷與建議動作（不寫檔）。"""
    out = []
    for v in variants:
        vid = v.get("id")
        if not vid or not any(v.get(f) for f in SINGLE_LEG_FLAGS):
            continue   # 只處理單腿方向性策略；兩腿鎖利／做市不在自動調參範圍
        trades = variant_trades(db, rid, vid)
        diags = variant_diagnostics(db, rid, vid)
        rec: dict = {"id": vid, "label": v.get("label"), "assetId": v.get("assetId"), "trades": len(trades),
                     "windows": len(diags), "pnl": round(sum(t["pnl"] for t in trades), 2), "action": "keep", "detail": ""}
        if not trades:
            if len(diags) < NO_TRADE_MIN_WINDOWS:
                rec["action"] = "wait"; rec["detail"] = f"只觀察了 {len(diags)} 個窗口，樣本不足"
                out.append(rec); continue
            loosen = plan_loosening(v, diags)
            tries = int(v.get("autoTuneLoosenCount") or 0)
            if not loosen or not loosen.get("param"):
                rec["action"] = "kill" if tries >= MAX_LOOSEN else "wait"
                rec["detail"] = (loosen or {}).get("note", "沒有可放寬的條件")
            elif tries >= MAX_LOOSEN:
                rec["action"] = "kill"; rec["detail"] = f"已自動放寬 {tries} 次仍無成交"
            else:
                rec["action"] = "loosen"; rec["loosen"] = loosen
                rec["detail"] = f"{loosen['note']}（{loosen['param']} {loosen.get('from')} → {loosen.get('to')}，主因 {loosen['reason']}）"
            out.append(rec); continue
        items = []
        for t in trades:
            items.append({"trade": t, "path": bid_path(db, str(v.get("assetId")), t["windowSlug"], t["side"], float(t["entryTime"]))})
        rec["lossReasons"] = loss_reasons(items)
        rec["winRate"] = round(100 * sum(1 for t in trades if t["pnl"] > 0) / len(trades), 1)
        if len(trades) < MIN_TRADES_FOR_EXIT_TUNE:
            rec["action"] = "wait"; rec["detail"] = f"只有 {len(trades)} 筆，未達調參門檻 {MIN_TRADES_FOR_EXIT_TUNE}"
            out.append(rec); continue
        cmp = best_exit_config(items, v.get("favoriteStopLossPrice"), v.get("favoriteStopLossUsd"))
        if cmp["best"] is None or cmp["usable"] < MIN_TRADES_FOR_EXIT_TUNE:
            rec["action"] = "wait"; rec["detail"] = f"可回放的成交只有 {cmp['usable']} 筆（缺報價路徑）"
            out.append(rec); continue
        rec["exit"] = {k: cmp[k] for k in ("margin", "threshold", "change", "usable")}
        rec["exit"]["current"] = cmp["current"]; rec["exit"]["best"] = cmp["best"]
        losses = len(trades) - int(rec["winRate"] * len(trades) / 100 + 0.5)
        if cmp["change"] and (cmp["best"]["stopPrice"] is not None or cmp["best"]["stopUsd"] is not None) and losses < MIN_LOSSES_FOR_STOP:
            rec["action"] = "wait"; rec["detail"] = f"虧損樣本只有 {losses} 筆，先不動停損"
        elif cmp["change"]:
            rec["action"] = "retune"
            b = cmp["best"]
            rec["detail"] = (f"停損 {cmp['current']['stopPrice']}／${cmp['current']['stopUsd']} → {b['stopPrice']}／${b['stopUsd']}："
                             f"每筆 {cmp['current']['mean']:+.3f} → {b['mean']:+.3f}（差 {cmp['margin']:+.3f} > 誤差 {cmp['threshold']:.3f}）")
        elif len(trades) >= MIN_TRADES_FOR_KILL and cmp["best"]["mean"] + SE_MARGIN * cmp["best"]["se"] <= 0:
            rec["action"] = "kill"
            rec["detail"] = (f"{len(trades)} 筆（可回放 {cmp['usable']}），最佳設定每筆仍 "
                             f"{cmp['best']['mean']:+.3f}±{cmp['best']['se']:.3f}（負期望）")
        else:
            rec["detail"] = f"維持現狀（最佳設定僅多 {cmp['margin']:+.3f}，誤差 {cmp['threshold']:.3f}）"
        out.append(rec)
    return out


def apply_actions(records: list[dict], auto: list[dict], disabled: list[str], overrides: dict) -> dict:
    """把 retune／loosen／kill 寫進三個設定檔（回傳摘要；呼叫端負責存檔、清紀錄、重啟）。"""
    by_id = {s.get("id"): s for s in auto}
    changed_ids, killed_ids = [], []
    for rec in records:
        vid = rec["id"]
        if rec["action"] == "retune":
            b = rec["exit"]["best"]
            overrides.setdefault(vid, {})["favoriteStopLossPrice"] = b["stopPrice"]
            overrides[vid]["favoriteStopLossUsd"] = b["stopUsd"]
            spec = by_id.get(vid)
            if spec is not None:
                spec["favoriteStopLossPrice"] = b["stopPrice"]
                spec["favoriteStopLossUsd"] = b["stopUsd"]
                spec["noStop"] = b["stopPrice"] is None and b["stopUsd"] is None
            changed_ids.append(vid)
        elif rec["action"] == "loosen":
            spec = by_id.get(vid)
            lo = rec["loosen"]
            if spec is None:
                rec["action"] = "skip"; rec["detail"] += "（內建變體不能改參數，只能停用）"
                continue
            spec[lo["param"]] = lo["to"]
            for k, val in (lo.get("extra") or {}).items():
                spec[k] = val
            spec["autoTuneLoosenCount"] = int(spec.get("autoTuneLoosenCount") or 0) + 1
            changed_ids.append(vid)
        elif rec["action"] == "kill":
            if vid not in disabled:
                disabled.append(vid)
            auto[:] = [s for s in auto if s.get("id") != vid]
            overrides.pop(vid, None)
            killed_ids.append(vid)
    return {"changed": changed_ids, "killed": killed_ids}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只分析，不寫檔、不清紀錄、不重啟")
    args = ap.parse_args()
    os.makedirs(REPORT_DIR, exist_ok=True)

    snapshot = asyncio.run(sim_snapshot())
    variants = snapshot.get("abVariants") or []
    if not variants:
        log.error("讀不到模擬盤快照，這次不動作"); return
    db = _ro()
    try:
        rid = run_id(db)
        records = diagnose(variants, db, rid)
    finally:
        db.close()

    auto = [s for s in (_load(AUTO_FILE, []) or []) if isinstance(s, dict)]
    disabled = [str(x) for x in (_load(DISABLED_FILE, []) or []) if x]
    overrides = _load(OVERRIDES_FILE, {}) or {}
    summary = apply_actions(records, auto, disabled, overrides)
    result = {"generatedAt": time.time(), "records": records, **summary}

    for rec in records:
        if rec["action"] != "keep":
            log.info(f"[{rec['action']}] {rec['label'] or rec['id']}：{rec['detail']}")
    if args.dry_run:
        log.info(f"dry-run：會調整 {len(summary['changed'])} 組、移除 {len(summary['killed'])} 組")
        print(json.dumps(result, ensure_ascii=False, indent=2)[:4000]); return

    touched = summary["changed"] + summary["killed"]
    if touched:
        for path, data in ((AUTO_FILE, auto), (DISABLED_FILE, disabled), (OVERRIDES_FILE, overrides)):
            shutil.copy(path, os.path.join(BACKUP_DIR, f"{os.path.basename(path)}.doctor.{int(time.time())}")) if os.path.exists(path) else None
            _save(path, data)
        for vid in touched:
            log.info(f"  {vid}：{reset_variant_records(vid)}")
        result["restart"] = "restarted" if restart_sim() else "failed"
    else:
        result["restart"] = "none"
    _save(os.path.join(REPORT_DIR, f"{datetime.now(TAIPEI).strftime('%Y-%m-%d-%H%M')}.json"), result)
    log.info(f"完成：調整 {len(summary['changed'])} 組、移除 {len(summary['killed'])} 組、重啟 {result['restart']}")


if __name__ == "__main__":
    main()
