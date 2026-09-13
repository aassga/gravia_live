"""
Telegram 查詢機器人：回報實盤／模擬盤目前狀態與損益。純唯讀。

    - 只讀本機兩個狀態伺服器的 WebSocket 快照（實盤 8767、模擬 8766），跟網頁同一份資料。
    - 不碰 .env 私鑰、不下單、不改任何設定；沒有任何指令能改變策略行為。
    - 只回應 TG_ALLOWED_USER_IDS 白名單內的 Telegram user id，其他人一律不理。
    - 主動推播（每 15 秒比對一次快照）：「策略停機／恢復」、「REAL↔DRY-RUN 切換」、「新部位」（含 DRY-RUN）；
      結算不推（2026-09-14 依使用者要求），要看用 /trades。
    - /scan 手動觸發每週市場掃描（polymarket_weekly_scan.py），/report 看最近一次報告摘要。

環境變數（.env）：
    TG_BOT_TOKEN          @BotFather 給的 token
    TG_ALLOWED_USER_IDS   逗號分隔的 Telegram user id
    TG_LIVE_STATUS_WS     預設 ws://127.0.0.1:8767
    TG_SIM_STATUS_WS      預設 ws://127.0.0.1:8766
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import httpx
import websockets

log = logging.getLogger("tg-bot")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # httpx 的 INFO 會把含 token 的 URL 印進日誌

BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
ALLOWED_USER_IDS = {
    int(value) for value in os.environ.get("TG_ALLOWED_USER_IDS", "").replace(" ", "").split(",") if value.isdigit()
}
LIVE_WS = os.environ.get("TG_LIVE_STATUS_WS", "ws://127.0.0.1:8767")
SIM_WS = os.environ.get("TG_SIM_STATUS_WS", "ws://127.0.0.1:8766")
API = f"https://api.telegram.org/bot{BOT_TOKEN}"
TAIPEI = timezone(timedelta(hours=8))
ALERT_POLL_SECONDS = 15.0
SNAPSHOT_TIMEOUT = 8.0


# ── 資料來源 ───────────────────────────────────────────────────────────────

async def fetch_snapshot(url: str) -> dict:
    """連上狀態伺服器拿第一份完整快照就斷開；伺服器每輪都會廣播完整資料。"""
    async with websockets.connect(url, open_timeout=SNAPSHOT_TIMEOUT, max_size=None) as ws:
        raw = await asyncio.wait_for(ws.recv(), SNAPSHOT_TIMEOUT)
        return json.loads(raw)


# ── 格式化（純函式，方便測試）─────────────────────────────────────────────

def _money(value, signed=True) -> str:
    value = float(value or 0)
    return f"{value:+.2f}" if signed else f"{value:.2f}"


def _ts(seconds) -> str:
    if not seconds:
        return "—"
    return datetime.fromtimestamp(float(seconds), TAIPEI).strftime("%m-%d %H:%M:%S")


def format_status(live: dict) -> str:
    cfg = live.get("strategyConfig") or {}
    st = live.get("strategyState") or {}
    pos = st.get("position")
    mode = "REAL 真實下單" if live.get("strategyExecutionEnabled") else "DRY-RUN（不送真單）"
    lines = [
        f"🤖 實盤狀態 · {mode}",
        f"策略：{cfg.get('label') or cfg.get('variantId') or '—'}",
        f"每注 {float(cfg.get('stakePct') or 0):.0f}% · 停損 {cfg.get('lateFavoriteStopLossPrice') if cfg.get('lateFavoriteStopLossPrice') is not None else '—'}",
        f"USDC 餘額：${_money(live.get('balanceUsdc'), signed=False)}",
    ]
    if st.get("halted"):
        lines.append(f"⛔ 已自動停機：{st.get('haltReason')}")
    if pos:
        lines.append(
            f"部位：{pos.get('side')} {float(pos.get('shares') or 0):.2f} 股 @ {float(pos.get('entryPrice') or 0):.3f}"
            f"（{pos.get('strategy') or '—'}，{'DRY' if pos.get('dryRun', True) else 'REAL'}）"
        )
    else:
        lines.append("部位：空手")
    lines.append(f"更新：{_ts(st.get('updatedAt'))}")
    return "\n".join(lines)


def format_pnl(live: dict) -> str:
    st = live.get("strategyState") or {}
    wins = int(st.get("winningTrades") or 0)
    losses = int(st.get("losingTrades") or 0)
    decided = wins + losses
    rate = f"{100 * wins / decided:.1f}%" if decided else "—"
    real_trades = [t for t in (st.get("trades") or []) if not t.get("dryRun", True)]
    today_start = datetime.now(TAIPEI).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today = [t for t in real_trades if float(t.get("exitTime") or 0) >= today_start]
    today_pnl = sum(float(t.get("pnlEstimate") or 0) for t in today)
    today_wins = sum(1 for t in today if float(t.get("pnlEstimate") or 0) > 0)
    lines = [
        "💰 實盤損益",
        f"餘額 ${_money(live.get('balanceUsdc'), signed=False)} · 基準 ${_money(live.get('baselineBalance'), signed=False)}"
        f"（{_ts(live.get('baselineSetAt'))} 起）",
        f"餘額變動：{_money(live.get('totalPnl'))}",
        f"策略估算：{_money(st.get('totalPnlEstimate'))} · 費用 {_money(st.get('totalFeesEstimate'), signed=False)}",
        f"交易 {int(st.get('totalTrades') or 0)} 筆 · 勝率 {rate}（{wins} 勝 {losses} 敗）",
        f"今日（台北）：{len(today)} 筆 · {today_wins} 勝 · {_money(today_pnl)}",
    ]
    return "\n".join(lines)


def format_trades(live: dict, limit: int = 10) -> str:
    st = live.get("strategyState") or {}
    trades = [t for t in (st.get("trades") or []) if not t.get("dryRun", True)][:limit]
    if not trades:
        return "📒 尚無真實成交紀錄"
    lines = [f"📒 最近 {len(trades)} 筆真單（台北時間）"]
    for t in trades:
        kind = t.get("tradeType") or "?"
        if kind == "early_exit":
            kind = f"提早退出 {t.get('exitReason') or ''}".strip()
        elif kind == "locked":
            kind = "鎖利"
        else:
            kind = "抱到結算"
        lines.append(
            f"{_ts(t.get('exitTime'))} {t.get('side')} @{float(t.get('entryPrice') or 0):.3f}"
            f" ×{float(t.get('shares') or 0):.1f} → {t.get('outcome')} {_money(t.get('pnlEstimate'))}（{kind}）"
        )
    return "\n".join(lines)


def format_sim(sim: dict, asset_id: str = "btc") -> str:
    rows = [v for v in (sim.get("abVariants") or []) if v.get("assetId") == asset_id]
    if not rows:
        return "📊 模擬盤沒有資料"
    rows.sort(key=lambda v: float(v.get("totalPnl") or 0), reverse=True)
    lines = [f"📊 模擬盤 {asset_id.upper()}（各組獨立記帳）"]
    for v in rows:
        rate = v.get("winRate")
        lines.append(
            f"{'★' if v is rows[0] else '·'} {v.get('label')}：{_money(v.get('totalPnl'))}"
            f" · {int(v.get('totalTrades') or 0)} 筆 · 勝率 {f'{rate:.0f}%' if rate is not None else '—'}"
            f"{' · 持倉中' if v.get('hasPosition') else ''}"
        )
    return "\n".join(lines)


HELP_TEXT = (
    "可用指令：\n"
    "/status — 實盤開關、策略、部位、餘額\n"
    "/pnl — 實盤損益、勝率、今日統計\n"
    "/trades [n] — 最近 n 筆真單（預設 10）\n"
    "/sim — 模擬盤各組損益\n"
    "/scan [小時] — 立刻跑一次市場掃描（預設 24h，約 10～15 分鐘，完成後推播）\n"
    "/report — 最近一次市場掃描的建議摘要\n"
    "/help — 這份說明\n"
    "（純查詢，沒有任何會改設定或下單的指令；主動推播：停機／恢復、REAL↔DRY-RUN 切換、新部位）"
)

REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports", "weekly")
_scan_running = {"v": False}


def latest_report_summary() -> str:
    try:
        files = sorted(f for f in os.listdir(REPORT_DIR) if f.endswith(".json"))
    except FileNotFoundError:
        files = []
    if not files:
        return "📈 還沒有掃描報告；用 /scan 產生一份，或等每週一 12:00（台北）的排程。"
    with open(os.path.join(REPORT_DIR, files[-1]), encoding="utf-8") as f:
        data = json.load(f)
    lines = [f"📈 最近一次掃描：{files[-1][:-5]}（最近 {float(data.get('hours') or 0):.0f}h、{data['report']['windows']} 窗）"]
    for k in data["report"]["kinds"][:4]:
        wr = f" 勝率 {k['winRate']*100:.0f}%" if k.get("winRate") is not None else ""
        lines.append(f"• {k['label']}：{k['share']*100:.0f}%{wr}")
    for sug in data.get("suggestions", []):
        lines.append(f"\n🔎 {sug['title']}\n{sug['finding']}\n👍 {sug['pros']}\n👎 {sug['cons']}")
    return "\n".join(lines)


async def run_scan_in_background(client: httpx.AsyncClient, chat_id: int, hours: float) -> None:
    import sys
    if _scan_running["v"]:
        await tg_send(client, chat_id, "⏳ 已有一次掃描在跑，請稍候。")
        return
    _scan_running["v"] = True
    try:
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_weekly_scan.py")
        proc = await asyncio.create_subprocess_exec(sys.executable, script, "--hours", str(hours))
        code = await proc.wait()
        if code != 0:
            await tg_send(client, chat_id, f"⚠️ 掃描結束但回傳碼 {code}，請看 gravia-tg.service 日誌。")
    finally:
        _scan_running["v"] = False


async def handle_command(text: str) -> str:
    parts = (text or "").strip().split()
    if not parts:
        return HELP_TEXT
    cmd = parts[0].split("@")[0].lower()
    try:
        if cmd in ("/status", "/start"):
            return format_status(await fetch_snapshot(LIVE_WS))
        if cmd == "/pnl":
            return format_pnl(await fetch_snapshot(LIVE_WS))
        if cmd == "/trades":
            limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10
            return format_trades(await fetch_snapshot(LIVE_WS), max(1, min(limit, 30)))
        if cmd == "/sim":
            return format_sim(await fetch_snapshot(SIM_WS))
        if cmd == "/report":
            return latest_report_summary()
    except Exception as exc:  # 狀態伺服器沒開、逾時等
        log.warning(f"snapshot failed for {cmd}: {exc}")
        return f"⚠️ 讀不到狀態伺服器（{exc.__class__.__name__}），請確認 gravia-status.service / gravia.service 是否在跑。"
    return HELP_TEXT


# ── Telegram I/O ───────────────────────────────────────────────────────────

async def tg_send(client: httpx.AsyncClient, chat_id: int, text: str) -> None:
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": text[:4000]})
    except Exception as exc:
        log.warning(f"sendMessage failed: {exc}")


def is_allowed(update: dict) -> bool:
    user = ((update.get("message") or {}).get("from") or {})
    return int(user.get("id") or 0) in ALLOWED_USER_IDS


async def poll_updates(client: httpx.AsyncClient) -> None:
    offset = None
    while True:
        try:
            params = {"timeout": 25, "allowed_updates": json.dumps(["message"])}
            if offset is not None:
                params["offset"] = offset
            r = await client.get(f"{API}/getUpdates", params=params, timeout=35)
            data = r.json()
            for upd in data.get("result", []):
                offset = int(upd["update_id"]) + 1
                msg = upd.get("message") or {}
                chat_id = (msg.get("chat") or {}).get("id")
                if not chat_id:
                    continue
                if not is_allowed(upd):
                    log.info(f"ignored message from user {((msg.get('from') or {}).get('id'))}")
                    continue
                text = msg.get("text") or ""
                parts = text.strip().split()
                if parts and parts[0].split("@")[0].lower() == "/scan":
                    hours = float(parts[1]) if len(parts) > 1 and parts[1].replace(".", "", 1).isdigit() else 24.0
                    hours = max(1.0, min(hours, 72.0))
                    await tg_send(client, chat_id, f"🔍 開始掃描最近 {hours:.0f} 小時，完成後會推播結果（約 10～15 分鐘）。")
                    asyncio.get_running_loop().create_task(run_scan_in_background(client, chat_id, hours))
                    continue
                reply = await handle_command(text)
                await tg_send(client, chat_id, reply)
        except Exception as exc:
            log.warning(f"getUpdates failed: {exc}")
            await asyncio.sleep(5)


# ── 主動推播 ───────────────────────────────────────────────────────────────

def diff_alerts(prev: dict | None, cur: dict) -> list[str]:
    """比對兩份實盤快照，回傳要推播的訊息；第一份快照只記住、不推。"""
    if prev is None:
        return []
    alerts: list[str] = []
    ps, cs = prev.get("strategyState") or {}, cur.get("strategyState") or {}
    if bool(cs.get("halted")) != bool(ps.get("halted")):
        alerts.append(f"⛔ 策略自動停機：{cs.get('haltReason')}" if cs.get("halted") else "✅ 策略已恢復下單")
    if bool(cur.get("strategyExecutionEnabled")) != bool(prev.get("strategyExecutionEnabled")):
        alerts.append("🔴 實盤切換為 REAL 真實下單" if cur.get("strategyExecutionEnabled") else "🟡 實盤切換為 DRY-RUN")
    # 2026-09-14 依使用者要求：新部位推播（含 DRY-RUN，標示模式）；結算不推，要看請用 /trades。
    ppos, cpos = ps.get("position"), cs.get("position")
    if cpos and (not ppos or (ppos.get("windowSlug"), ppos.get("entryTime")) != (cpos.get("windowSlug"), cpos.get("entryTime"))):
        mode = "REAL" if cpos.get("dryRun") is False else "DRY-RUN"
        alerts.append(
            f"{'🟢' if mode == 'REAL' else '🟡'} 新部位（{mode}）{cpos.get('side')} "
            f"{float(cpos.get('shares') or 0):.2f} 股 @ {float(cpos.get('entryPrice') or 0):.3f}"
            f" · ${float(cpos.get('stakeUsd') or 0):.2f} · {cpos.get('strategy') or '—'} · {cpos.get('windowSlug')}"
        )
    return alerts


async def alert_loop(client: httpx.AsyncClient) -> None:
    prev: dict | None = None
    while True:
        try:
            cur = await fetch_snapshot(LIVE_WS)
            for text in diff_alerts(prev, cur):
                for uid in ALLOWED_USER_IDS:
                    await tg_send(client, uid, text)
            prev = cur
        except Exception as exc:
            log.warning(f"alert snapshot failed: {exc}")
        await asyncio.sleep(ALERT_POLL_SECONDS)


async def main() -> None:
    if not BOT_TOKEN or not ALLOWED_USER_IDS:
        raise SystemExit("需要 TG_BOT_TOKEN 與 TG_ALLOWED_USER_IDS")
    log.info(f"TG bot 啟動：白名單 {sorted(ALLOWED_USER_IDS)} · live={LIVE_WS} · sim={SIM_WS}")
    async with httpx.AsyncClient() as client:
        await asyncio.gather(poll_updates(client), alert_loop(client))


if __name__ == "__main__":
    asyncio.run(main())
