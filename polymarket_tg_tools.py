"""
TG 機器人的分析／維護工具（2026-09-20 依使用者要求）：純函式，讀主模擬盤 sqlite 與狀態檔，不下單。

    roi_table            /roi     各資產 ROI／每筆 ROI／1 輸＝幾贏 排行
    stop_replay_table    /tune    某變體的停損回放（各檔位的假停損／真停損／淨損益）
    mirror_report        /mirror  模擬盤 vs 實盤同窗口一致性
    reset_sim_variant    /reset   清空某模擬盤變體（服務停止時執行）
    reset_live_states    /reset   實盤頁面全部重製（服務停止時執行）
    set_variant_disabled /disable /enable  改停用清單
    stake_shares_check   餘額警報：目前每注 % 在 0.99 買得到幾股
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone

TP = timezone(timedelta(hours=8))
MIN_ORDER_SHARES = 5.0
STOP_LEVELS = (None, 0.92, 0.90, 0.88, 0.85, 0.80, 0.70, 0.60, 0.40)


def _ro(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _run_id(db: sqlite3.Connection) -> int:
    row = db.execute("SELECT value FROM sim_meta WHERE key='shared_config'").fetchone()
    return int(json.loads(row[0])["runId"]) if row else 0


def _trades(db: sqlite3.Connection, run_id: int, vid: str) -> list[dict]:
    return [json.loads(r[0]) for r in db.execute("SELECT trade_json FROM sim_trades WHERE run_id=? AND variant_id=? ORDER BY exit_time", (run_id, vid))]


# ── /roi ─────────────────────────────────────────────────────────────────

def roi_rows(db_path: str, min_trades: int = 10) -> list[dict]:
    db = _ro(db_path)
    try:
        run_id = _run_id(db)
        out = []
        for (vid,) in db.execute("SELECT DISTINCT variant_id FROM sim_trades WHERE run_id=?", (run_id,)):
            ts = _trades(db, run_id, vid)
            if len(ts) < min_trades:
                continue
            wins = [t for t in ts if t["pnl"] > 0]; losses = [t for t in ts if t["pnl"] <= 0]
            stake = sum(float(t.get("stakeUsd") or 0) for t in ts); pnl = sum(t["pnl"] for t in ts)
            aw = sum(t["pnl"] for t in wins) / len(wins) if wins else 0.0
            al = -sum(t["pnl"] for t in losses) / len(losses) if losses else 0.0
            out.append({"id": vid, "n": len(ts), "winRate": 100 * len(wins) / len(ts), "pnl": pnl, "stake": stake,
                        "roi": (100 * pnl / stake) if stake else None, "roiPerTrade": (100 * pnl / stake / len(ts)) if stake else None,
                        "avgWin": aw, "avgLoss": al, "lossesPerWin": (al / aw) if aw else None,
                        "breakEven": (100 * al / (al + aw)) if (al + aw) else None})
        return out
    finally:
        db.close()


def roi_table(db_path: str, labels: dict[str, str] | None = None, per_asset: int = 5, min_trades: int = 10) -> str:
    rows = roi_rows(db_path, min_trades)
    if not rows:
        return "模擬盤還沒有 ≥10 筆的變體。"
    labels = labels or {}
    groups: dict[str, list[dict]] = {}
    for r in rows:
        aid = r["id"].split("-auto-")[0].split("-last")[0].split("-mid")[0].split("-follow")[0].split("-early")[0].split("-price")[0]
        groups.setdefault(aid, []).append(r)
    lines = ["📊 模擬盤 ROI 排行（≥10 筆；ROI＝損益÷投入本金）"]
    for aid, rs in groups.items():
        rs.sort(key=lambda r: -(r["roi"] or -999))
        lines.append(f"— {aid} —")
        for r in rs[:per_asset]:
            lpw = f"{r['lossesPerWin']:.1f}" if r["lossesPerWin"] is not None else "—"
            be = f"{r['breakEven']:.1f}%" if r["breakEven"] is not None else "—"
            lines.append(f"{labels.get(r['id'], r['id'])[:34]}：ROI {r['roi']:+.2f}% · {r['n']} 筆 勝率 {r['winRate']:.1f}%（打平 {be}）· 1輸={lpw}贏 · 淨 {r['pnl']:+.1f}")
    return "\n".join(lines)


# ── /tune ────────────────────────────────────────────────────────────────

def stop_replay_rows(db_path: str, vid: str, asset_id: str, stops=STOP_LEVELS) -> tuple[int, list[dict]]:
    """回放各停損檔（以 stop-0.01 成交估）。回傳 (可回放筆數, rows)。"""
    db = _ro(db_path)
    try:
        run_id = _run_id(db)
        items = []
        for t in _trades(db, run_id, vid):
            side = t["side"]; col = "up_bid" if side == "Up" else "down_bid"
            after = [b for (b,) in db.execute(f"SELECT {col} FROM sim_quotes WHERE asset_id=? AND window_slug=? AND ts>? AND {col} IS NOT NULL",
                                              (asset_id, t["windowSlug"], t["entryTime"]))]
            if not after:
                continue
            items.append({"entry": t["entryPrice"], "shares": t["shares"], "pnl": t["pnl"], "won": t["pnl"] > 0, "minbid": min(after)})
    finally:
        db.close()
    rows = []
    for stop in stops:
        wins = losses = 0; pw = pl = 0.0; false_stops = 0
        for r in items:
            if stop is not None and r["minbid"] <= stop and r["entry"] > stop:
                loss = (r["entry"] - (stop - 0.01)) * r["shares"]
                losses += 1; pl -= loss
                if r["won"]:
                    false_stops += 1
            elif r["won"]:
                wins += 1; pw += r["pnl"]
            else:
                losses += 1; pl += r["pnl"]
        aw = pw / wins if wins else 0.0; al = -pl / losses if losses else 0.0
        rows.append({"stop": stop, "wins": wins, "losses": losses, "falseStops": false_stops, "net": pw + pl, "avgWin": aw, "avgLoss": al,
                     "lossesPerWin": (al / aw) if aw else None, "winRate": 100 * wins / max(1, wins + losses),
                     "breakEven": (100 * al / (al + aw)) if (al + aw) else None})
    return len(items), rows


def stop_replay_table(db_path: str, vid: str, asset_id: str, label: str | None = None) -> str:
    n, rows = stop_replay_rows(db_path, vid, asset_id)
    if not n:
        return f"{label or vid}：沒有可回放的成交（需要有進場後的報價取樣）。"
    lines = [f"🧪 {label or vid} 停損回放（{n} 筆；停損以 stop−0.01 成交估，真實跳空會更差）"]
    for r in rows:
        stop = "不停損" if r["stop"] is None else f"{r['stop']:.2f}"
        lpw = f"{r['lossesPerWin']:.1f}" if r["lossesPerWin"] is not None else "—"
        lines.append(f"{stop:<5} 淨 {r['net']:+7.1f} · 贏 {r['wins']}/輸 {r['losses']}（假停損 {r['falseStops']}）· 勝率 {r['winRate']:.1f}% · 1輸={lpw}贏")
    best = max(rows, key=lambda r: r["net"])
    lines.append(f"→ 淨損益最高：{'不停損' if best['stop'] is None else f'停損 {best['stop']:.2f}'}（{best['net']:+.1f}）")
    return "\n".join(lines)


# ── /mirror ──────────────────────────────────────────────────────────────

def mirror_report(db_path: str, state_path: str, vid: str, hours: float = 12.0, name: str = "實盤") -> str:
    """模擬盤（主進程）同變體 vs 該實盤：每個窗口誰有進；只有一邊進的列出原因。"""
    since = time.time() - hours * 3600
    with open(state_path, "r", encoding="utf-8") as f:
        live = json.load(f)
    live_tr = {t["windowSlug"]: t for t in live.get("trades", []) if not t.get("dryRun", True) and (t.get("entryTime") or 0) >= since}
    live_dry = {t["windowSlug"] for t in live.get("trades", []) if t.get("dryRun", True) and (t.get("entryTime") or 0) >= since}
    pos = live.get("position")
    if pos and (pos.get("entryTime") or 0) >= since:
        (live_tr if not pos.get("dryRun", True) else live_dry).__setitem__(pos["windowSlug"], pos) if not pos.get("dryRun", True) else live_dry.add(pos["windowSlug"])
    live_diag = {w.get("windowSlug"): w for w in live.get("windowDiagnostics", []) if (w.get("firstSeenAt") or 0) >= since}
    db = _ro(db_path)
    try:
        run_id = _run_id(db)
        sim_tr = {}
        for t in _trades(db, run_id, vid):
            if (t.get("exitTime") or 0) >= since:
                sim_tr[t["windowSlug"]] = t
        sim_diag = {}
        for (dj,) in db.execute("SELECT diagnostic_json FROM sim_window_diagnostics WHERE run_id=? AND variant_id=?", (run_id, vid)):
            w = json.loads(dj)
            if (w.get("firstSeenAt") or 0) >= since:
                sim_diag[w["windowSlug"]] = w
    finally:
        db.close()
    slugs = sorted(set(sim_tr) | set(live_tr) | set(sim_diag) | set(live_diag))
    both = only_live = only_sim = dry_match = 0; details = []
    for s in slugs:
        a, b = s in sim_tr, s in live_tr
        if a and s in live_dry:
            dry_match += 1   # 實盤當時是 DRY-RUN、也有跟到 → 不算不一致
            continue
        if a and b:
            both += 1
        elif b:
            only_live += 1; w = sim_diag.get(s) or {}
            details.append(f"只實盤 {datetime.fromtimestamp(int(s.rsplit('-', 1)[-1]), TP).strftime('%m-%d %H:%M')} 模擬原因 {w.get('lastReason') or '無診斷'}")
        elif a:
            only_sim += 1; w = live_diag.get(s) or {}
            details.append(f"只模擬 {datetime.fromtimestamp(int(s.rsplit('-', 1)[-1]), TP).strftime('%m-%d %H:%M')} 實盤原因 {w.get('lastReason') or '無診斷'}{('/' + str(w.get('orderError'))) if w.get('orderError') else ''}")
    lines = [f"🪞 {name} vs 模擬盤 {vid}（最近 {hours:.0f}h，{len(slugs)} 窗）：兩邊都進 {both} · 只實盤 {only_live} · 只模擬 {only_sim}"
             + (f" · DRY-RUN 跟到 {dry_match}（不計）" if dry_match else "")]
    if not live_tr and not sim_tr:
        lines.append("（這段期間沒有真實成交，DRY-RUN 不列入）")
    lines += details[:8]
    if len(details) > 8:
        lines.append(f"…另 {len(details) - 8} 個")
    return "\n".join(lines)


# ── /reset ───────────────────────────────────────────────────────────────

def reset_sim_variant(db_path: str, vid: str, backup_dir: str) -> str:
    """清空單一模擬變體（呼叫端先停主進程）：備份成交後刪 sim_trades／診斷，狀態改空白（enabledAt=現在）。"""
    db = sqlite3.connect(db_path)
    try:
        cfg = json.loads(db.execute("SELECT value FROM sim_meta WHERE key='shared_config'").fetchone()[0])
        trades = [json.loads(r[0]) for r in db.execute("SELECT trade_json FROM sim_trades WHERE variant_id=?", (vid,))]
        state = db.execute("SELECT state_json FROM sim_state WHERE variant_id=?", (vid,)).fetchone()
        os.makedirs(backup_dir, exist_ok=True)
        path = os.path.join(backup_dir, f"sim_{vid}_{int(time.time())}.json")
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
        return f"已清空 {vid}：成交 {n1} 筆、診斷 {n2} 筆（備份 {os.path.basename(path)}），啟用時間重設為現在"
    finally:
        db.close()


def reset_live_states(state_files: list[str], baseline_files: list[str], backup_dir: str) -> str:
    """實盤全部重製（呼叫端先停服務、確認無真實持倉）。"""
    stamp = time.strftime("%Y%m%d-%H%M%S")
    os.makedirs(backup_dir, exist_ok=True)
    lines = []
    for f in state_files:
        with open(f, "r", encoding="utf-8") as fh:
            d = json.load(fh)
        pos = d.get("position")
        if (pos and not pos.get("dryRun", True)) or [p for p in (d.get("pendingSettlements") or []) if not p.get("dryRun", True)]:
            raise RuntimeError(f"{os.path.basename(f)} 仍有真實持倉／待結算")
        shutil.copy(f, os.path.join(backup_dir, f"{os.path.basename(f)}.{stamp}"))
        fresh = {"position": None, "pendingSettlements": [], "trades": [], "windowDiagnostics": [],
                 "totalPnlEstimate": 0.0, "totalFeesEstimate": 0.0, "totalTrades": 0, "lockedTrades": 0, "directionalTrades": 0,
                 "earlyExits": 0, "winningTrades": 0, "losingTrades": 0, "lastActionAt": 0.0,
                 "halted": False, "haltReason": None, "unconfirmedOrder": None, "runtimeDryRun": False,
                 "firstTradeGuard": None, "preflightSlug": None, "validationSlug": None, "validationResult": None,
                 "quoteSource": "not_started", "wsConnected": False, "updatedAt": time.time(), "resetAt": time.time()}
        tmp = f + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(fresh, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, f)
        lines.append(f"{os.path.basename(f)}：備份 {len(d.get('trades', []))} 筆 → 重設")
    for b in baseline_files:
        if os.path.exists(b):
            shutil.copy(b, os.path.join(backup_dir, f"{os.path.basename(b)}.{stamp}")); os.remove(b)
            lines.append(f"{os.path.basename(b)}：已刪除，重啟後以當下餘額重設基準")
    return "\n".join(lines)


# ── /disable /enable ─────────────────────────────────────────────────────

def set_variant_disabled(path: str, vid: str, disabled: bool) -> list[str]:
    data = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = [str(x) for x in (json.load(f) or []) if x]
    if disabled and vid not in data:
        data.append(vid)
    if not disabled and vid in data:
        data.remove(vid)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)
    return data


def disabled_variants(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [str(x) for x in (json.load(f) or []) if x]


# ── 餘額警報 ──────────────────────────────────────────────────────────────

def stake_shares_check(balance: float, positions_cost: float, stake_pct: float, ask: float = 0.99) -> dict:
    """算法 B：注金 = (現金＋在場部位成本) × %，上限為現金；股數 = ⌊注金 ÷ ask⌋。"""
    equity = float(balance) + float(positions_cost)
    budget = min(equity * float(stake_pct) / 100.0, float(balance))
    shares = int(budget // ask) if ask > 0 else 0
    need_pct = (MIN_ORDER_SHARES * ask / equity * 100.0) if equity > 0 else None
    return {"equity": equity, "budget": budget, "shares": shares, "ok": shares >= MIN_ORDER_SHARES, "minPctNeeded": need_pct}
