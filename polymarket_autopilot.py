#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Gravia 模擬盤自動駕駛（2026-09-15 依使用者要求；systemd timer 每 6 小時跑一次）。

每輪：
  1. 探測 Polymarket 上所有 <幣>-updown-<週期> 系列，各掃最近 24 小時的公開成交。
  2. 在「最後 N 秒買領先方」家族內找粗估總收益為正、樣本夠的規則（買價 × 進場秒數），
     依總收益排序（使用者要求：主要看總收益）列進報告；自動加進模擬盤的功能 2026-09-17 起預設關閉
     （POLY_AUTOPILOT_AUTO_ADD=true 才會寫 sim_auto_variants.json，polymarket_server.py 啟動時讀入）。
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
import shutil
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

# 2026-09-16 依使用者要求：不設上限——每輪把所有符合條件（總收益 > 0、n >= 100）的規則一次全加進模擬盤。
MAX_ADD_PER_RUN = None        # None = 不限
MAX_ADD_PER_MARKET = None     # None = 不限
DISABLE_LOSS_USD = 350.0
# 2026-09-23 依使用者要求：執行頻率改為每 3 天（systemd timer）。取樣仍用 24 小時——
# 24h 掃 24 個系列已經要 15 分鐘、記憶體峰值 1GB，72h 會到 ~3GB 有拖垮模擬盤的風險，
# 而 24h 樣本（3,000+ 窗口、30+ 候選）已足夠挑出值得測的規則。
SCAN_HOURS = float(os.environ.get("POLY_AUTOPILOT_SCAN_HOURS", "24"))
# 2026-09-17 依使用者要求：只在白名單市場找候選加進模擬盤（其餘系列照掃、只進報告），
# 避免自動駕駛把模擬盤資產越加越多拖慢實盤（曾一夜長到 11 個資產、98 組）。POLY_AUTOPILOT_MARKETS 可覆寫。
# 2026-09-23 依使用者要求：自動添加不再限制市場（空字串 = 全部掃到的市場都可以加）
CANDIDATE_MARKETS = {m.strip() for m in os.environ.get("POLY_AUTOPILOT_MARKETS", "").split(",") if m.strip()}
# 2026-09-17 依使用者要求：不再自動把新變體加進模擬盤（掃描、報告、停用虧損 >= 350 照跑）。
# 要重新開啟：.env 設 POLY_AUTOPILOT_AUTO_ADD=true。
# 2026-09-23 依使用者要求：掃描後直接把適合的規則加進模擬盤，不再詢問。
AUTO_ADD_ENABLED = os.environ.get("POLY_AUTOPILOT_AUTO_ADD", "true").strip().lower() == "true"
# 2026-09-23 依使用者要求：不限幣種、不限數量——只要掃到、模擬盤還沒有的都加進去（設環境變數才限制）。
_max_add_raw = os.environ.get("POLY_AUTOPILOT_MAX_ADD_PER_MARKET", "").strip()
MAX_ADD_PER_MARKET_DEFAULT = int(_max_add_raw) if _max_add_raw.isdigit() else None


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
                      max_total: int | None = MAX_ADD_PER_RUN, max_per_market: int | None = MAX_ADD_PER_MARKET_DEFAULT,
                      markets: set[str] | None = None) -> list[dict]:
    """從各市場候選（已依 score 排序、合併）挑要新增的：只收白名單市場、跳過已存在／已停用；上限為 None 表示不限。"""
    allowed = CANDIDATE_MARKETS if markets is None else markets
    chosen, per_market = [], {}
    for c in sorted(candidates, key=lambda c: c["stats"]["score"], reverse=True):
        if c["id"] in existing_ids or c["id"] in disabled_ids:
            continue
        if allowed and c["market"] not in allowed:
            continue
        if max_per_market is not None and per_market.get(c["market"], 0) >= max_per_market:
            continue
        chosen.append(c)
        per_market[c["market"]] = per_market.get(c["market"], 0) + 1
        if max_total is not None and len(chosen) >= max_total:
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
    """任一實盤進程（polymarket_live*_state.json）有持倉或待結算就算有。"""
    import glob
    for path in sorted(set(glob.glob(os.path.join(HERE, "polymarket_live*_state.json")) | {LIVE_STATE_FILE})):
        st = _load(path, {}) or {}
        if st.get("position") or st.get("pendingSettlements"):
            return True
    return False


def restart_sim() -> bool:
    # 2026-09-25：Windows／沒有 systemd 的環境不呼叫 systemctl——模擬盤會自己偵測設定檔變更並重啟。
    if os.name == "nt" or not shutil.which("systemctl"):
        log.info("非 systemd 環境：設定已寫入，模擬盤會在數秒內自行重新載入")
        return True
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
    elif not AUTO_ADD_ENABLED:
        body.append("➕ 自動新增已關閉（只報告，不加進模擬盤）。")
    else:
        body.append("➕ 這輪沒有新增（候選已存在、已停用、不夠格或不在白名單市場）。")
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

    added = pick_new_variants(candidates, existing_ids, set(disabled)) if AUTO_ADD_ENABLED else []
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
