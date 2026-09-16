#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gravia 模擬盤自動駕駛（2026-09-15 依使用者要求；systemd timer 每 6 小時跑一次）。

每輪：
  1. 探測 Polymarket 上所有 <幣>-updown-<週期> 系列，各掃最近 24 小時的公開成交。
  2. 在「最後 N 秒買領先方」家族內找粗估總收益為正、樣本夠的規則（買價 × 進場秒數），
     依總收益排序（使用者要求：主要看總收益），每輪最多新增 MAX_ADD_PER_RUN 個、每市場最多 MAX_ADD_PER_MARKET 個到模擬盤
     （寫 sim_auto_variants.json，polymarket_server.py 啟動時讀入）。
  3. 模擬盤累計虧損 <= -DISABLE_LOSS_USD 的變體寫進 sim_disabled_variants.json（只停用，歷史保留）。
  4. 有任何變更且實盤無持倉 → 重啟 gravia.service 讓變更生效；有持倉就留到下一輪。
  5. 摘要推 Telegram，並存 reports/autopilot/<時間>.json。

不碰實盤設定（.env 的 POLY_LIVE_* / POLY_STRATEGY_ARMED）——實盤策略自動替換依使用者要求先移除。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone

import httpx

import polymarket_weekly_scan as scan

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket_autopilot")
logging.getLogger("httpx").setLevel(logging.WARNING)

TAIPEI = timezone(timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
AUTO_FILE = os.environ.get("POLY_SIM_AUTO_VARIANTS_FILE", os.path.join(HERE, "sim_auto_variants.json"))
DISABLED_FILE = os.environ.get("POLY_SIM_DISABLED_VARIANTS_FILE", os.path.join(HERE, "sim_disabled_variants.json"))
LIVE_STATE_FILE = os.path.join(HERE, "polymarket_live_strategy_state.json")
REPORT_DIR = os.path.join(HERE, "reports", "autopilot")
SIM_WS = os.environ.get("TG_SIM_STATUS_WS", "ws://127.0.0.1:8766")

# 2026-09-16 依使用者要求：每市場不再限 1 組（改 3），每輪總數放寬到 6。
MAX_ADD_PER_RUN = 6
MAX_ADD_PER_MARKET = 3
DISABLE_LOSS_USD = 350.0
SCAN_HOURS = 24.0


def _load(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def _save(path, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ── 純邏輯（可測試） ────────────────────────────────────────────────────────

def pick_new_variants(candidates: list[dict], existing_ids: set[str], disabled_ids: set[str],
                      max_total: int = MAX_ADD_PER_RUN, max_per_market: int = MAX_ADD_PER_MARKET) -> list[dict]:
    """從各市場候選（已依 score 排序、合併）挑要新增的：跳過已存在／已停用，每市場最多 max_per_market。"""
    chosen, per_market = [], {}
    for c in sorted(candidates, key=lambda c: c["stats"]["score"], reverse=True):
        if c["id"] in existing_ids or c["id"] in disabled_ids:
            continue
        if per_market.get(c["market"], 0) >= max_per_market:
            continue
        chosen.append(c)
        per_market[c["market"]] = per_market.get(c["market"], 0) + 1
        if len(chosen) >= max_total:
            break
    return chosen


def pick_disable(sim_variants: list[dict], disabled_ids: set[str], threshold: float = DISABLE_LOSS_USD) -> list[dict]:
    """模擬盤累計虧損 <= -threshold 且尚未停用的變體。"""
    out = []
    for v in sim_variants:
        vid = v.get("id")
        if not vid or vid in disabled_ids:
            continue
        if float(v.get("totalPnl") or 0) <= -threshold:
            out.append({"id": vid, "label": v.get("label"), "totalPnl": float(v.get("totalPnl") or 0),
                        "totalTrades": int(v.get("totalTrades") or 0)})
    return out


# ── I/O ────────────────────────────────────────────────────────────────────

async def _sim_snapshot() -> dict:
    try:
        import websockets
        async with websockets.connect(SIM_WS, open_timeout=8, max_size=None) as ws:
            return json.loads(await asyncio.wait_for(ws.recv(), 10))
    except Exception as exc:
        log.warning(f"sim snapshot unavailable: {exc}")
        return {}


def live_has_position() -> bool:
    st = _load(LIVE_STATE_FILE, {}) or {}
    return bool(st.get("position")) or bool(st.get("pendingSettlements"))


def restart_sim() -> bool:
    try:
        subprocess.run(["sudo", "-n", "systemctl", "restart", "gravia.service"], check=True, timeout=60)
        return True
    except Exception as exc:
        log.error(f"restart gravia.service failed: {exc}")
        return False


def scan_all_markets(client: httpx.Client, hours: float) -> tuple[dict, list[dict]]:
    """回傳 ({market: 摘要}, 合併後候選清單)。"""
    series = scan.discover_updown_series(client)
    summaries, candidates = {}, []
    for key in sorted(series):
        try:
            windows = scan.fetch_windows(hours, client, key)
        except Exception as exc:
            log.warning(f"scan {key}: {exc}")
            continue
        if not windows:
            summaries[key] = {"windows": 0, "trades": 0, "candidates": 0}
            continue
        cands = scan.late_favorite_candidates(windows, key)
        report = scan.analyze(windows)
        fav_kind = next((k for k in report["kinds"] if k["kind"] == "late_favorite"), None)
        summaries[key] = {"windows": len(windows), "trades": report["trades"], "candidates": len(cands),
                          "lateFavoritePnl": fav_kind["pnl"] if fav_kind else None,
                          "best": cands[0] if cands else None}
        candidates += cands
    return summaries, candidates


def render_telegram(result: dict) -> list[str]:
    now = datetime.now(TAIPEI).strftime("%m-%d %H:%M")
    head = [f"🤖 自動駕駛 {now}（掃 {len(result['markets'])} 個 Up/Down 系列、最近 {result['hours']:.0f}h）"]
    pos = [(k, s) for k, s in result["markets"].items() if s.get("best")]
    if pos:
        head.append("各市場最佳規則（總收益；每股平均／中位、n）：")
        for k, s in sorted(pos, key=lambda kv: kv[1]["best"]["stats"]["score"], reverse=True)[:8]:
            b = s["best"]["stats"]
            head.append(f"• {s['best']['label'].replace('⚙ 自動 ', '')}：總收益 {b['totalPnl']:+,.0f}；{b['pnlPerShare']:+.3f}／{b['pnlPerShareMedian']:+.3f}、n={b['n']}"
                        f"{'（⚠ 勝率不夠補虧損）' if b.get('negativeEV') else ''}")
    else:
        head.append("這輪沒有任何市場出現總收益為正、樣本夠的規則。")
    msgs = ["\n".join(head)]
    body = []
    if result["added"]:
        body.append("➕ 新增到模擬盤：\n" + "\n".join(f"• {c['label']}（總收益 {c['stats']['totalPnl']:+,.0f}、每股 {c['stats']['pnlPerShare']:+.3f}、n={c['stats']['n']}）" for c in result["added"]))
    else:
        body.append("➕ 這輪沒有新增（候選已存在、已停用或不夠格）。")
    if result["disabled"]:
        body.append("➖ 停用（累計虧損 ≥ 350）：\n" + "\n".join(f"• {d['label']}：{d['totalPnl']:+.0f}（{d['totalTrades']} 筆）" for d in result["disabled"]))
    else:
        body.append("➖ 沒有變體達到停用門檻。")
    body.append({"restarted": "🔄 已重啟模擬盤套用變更。", "deferred": "⏸ 實盤有持倉，變更留到下一輪重啟時套用。",
                 "failed": "⚠️ 重啟模擬盤失敗，請看日誌。", "none": "（無變更，不重啟）"}[result["restart"]])
    body.append("實盤設定未動；自動替換實盤策略依要求先移除。")
    msgs.append("\n".join(body))
    return msgs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=SCAN_HOURS)
    ap.add_argument("--dry-run", action="store_true", help="只掃描與計算，不寫檔、不重啟、不推播")
    ap.add_argument("--no-telegram", action="store_true")
    args = ap.parse_args()
    os.makedirs(REPORT_DIR, exist_ok=True)

    with httpx.Client(timeout=25) as client:
        summaries, candidates = scan_all_markets(client, args.hours)

    auto = [s for s in (_load(AUTO_FILE, []) or []) if isinstance(s, dict)]
    disabled = [str(x) for x in (_load(DISABLED_FILE, []) or []) if x]
    snapshot = asyncio.run(_sim_snapshot())
    sim_variants = snapshot.get("abVariants") or []
    existing_ids = {v.get("id") for v in sim_variants} | {s.get("id") for s in auto}

    added = pick_new_variants(candidates, existing_ids, set(disabled))
    to_disable = pick_disable(sim_variants, set(disabled))
    now = time.time()
    for c in added:
        c["addedAt"] = now
    result = {"generatedAt": now, "hours": args.hours, "markets": summaries, "candidates": candidates[:30],
              "added": added, "disabled": to_disable, "restart": "none"}

    if not args.dry_run:
        if added:
            _save(AUTO_FILE, auto + added)
        if to_disable:
            _save(DISABLED_FILE, disabled + [d["id"] for d in to_disable])
        if added or to_disable:
            if live_has_position():
                result["restart"] = "deferred"
            else:
                result["restart"] = "restarted" if restart_sim() else "failed"
        stamp = datetime.now(TAIPEI).strftime("%Y-%m-%d-%H%M")
        _save(os.path.join(REPORT_DIR, f"{stamp}.json"), result)
    msgs = render_telegram(result)
    for m in msgs:
        log.info(m)
    if not args.dry_run and not args.no_telegram:
        scan.send_telegram(msgs)


if __name__ == "__main__":
    main()
