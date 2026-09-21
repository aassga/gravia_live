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
# 2026-09-17 多實盤：TG_LIVE_INSTANCES="名稱|狀態WS|env檔|要重啟的服務(空白分隔)|狀態檔;名稱|..."
# 沒設就只有一個實盤（原本的 LIVE_WS／.env／gravia.service）。
_HERE = os.path.dirname(os.path.abspath(__file__))


def _parse_live_instances() -> list[dict]:
    raw = os.environ.get("TG_LIVE_INSTANCES", "").strip()
    if not raw:
        return [{"name": "實盤", "ws": LIVE_WS, "env": os.path.join(_HERE, ".env"),
                 "services": ["gravia.service", "gravia-status.service"],
                 "state": os.path.join(_HERE, "polymarket_live_strategy_state.json")}]
    out = []
    for chunk in raw.split(";"):
        parts = [p.strip() for p in chunk.split("|")]
        if len(parts) < 5:
            continue
        out.append({"name": parts[0], "ws": parts[1], "env": parts[2], "services": parts[3].split(), "state": parts[4]})
    return out


LIVE_INSTANCES = _parse_live_instances()
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
    # 2026-09-15 依使用者要求：以收益為主，不看勝率。
    st = live.get("strategyState") or {}
    real_trades = [t for t in (st.get("trades") or []) if not t.get("dryRun", True)]
    pnls = [float(t.get("pnlEstimate") or 0) for t in real_trades]
    avg = (sum(pnls) / len(pnls)) if pnls else 0.0
    worst = min(pnls) if pnls else 0.0
    best = max(pnls) if pnls else 0.0
    today_start = datetime.now(TAIPEI).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today = [t for t in real_trades if float(t.get("exitTime") or 0) >= today_start]
    today_pnl = sum(float(t.get("pnlEstimate") or 0) for t in today)
    lines = [
        "💰 實盤損益",
        f"餘額 ${_money(live.get('balanceUsdc'), signed=False)} · 基準 ${_money(live.get('baselineBalance'), signed=False)}"
        f"（{_ts(live.get('baselineSetAt'))} 起）",
        f"餘額變動：{_money(live.get('totalPnl'))}",
        f"策略估算：{_money(st.get('totalPnlEstimate'))} · 費用 {_money(st.get('totalFeesEstimate'), signed=False)}",
        f"交易 {int(st.get('totalTrades') or 0)} 筆 · 平均 {_money(avg)}/筆 · 最好 {_money(best)} · 最差 {_money(worst)}",
        f"今日（台北）：{len(today)} 筆 · {_money(today_pnl)}",
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


def format_sim(sim: dict, asset_id: str | None = None) -> str:
    """2026-09-15 依使用者要求：預設列出模擬盤所有資產（每個資產一段、各自依損益排序）；
    /sim btc 這種帶資產代號的只列該資產。"""
    assets = [a for a in (sim.get("assetList") or [])]
    if asset_id:
        assets = [a for a in assets if a.get("id") == asset_id] or [{"id": asset_id, "label": asset_id.upper()}]
    if not assets:
        return "📊 模擬盤沒有資料"
    out = []
    for a in assets:
        rows = [v for v in (sim.get("abVariants") or []) if v.get("assetId") == a.get("id")]
        if not rows:
            continue
        rows.sort(key=lambda v: float(v.get("totalPnl") or 0), reverse=True)
        total = sum(float(v.get("totalPnl") or 0) for v in rows)
        lines = [f"📊 {a.get('label') or a.get('id')}（{len(rows)} 組，合計 {_money(total)}）"]
        for v in rows:
            n = int(v.get("totalTrades") or 0)
            avg = (float(v.get("totalPnl") or 0) / n) if n else 0.0
            since = f" · 自 {_ts(v['enabledAt'])[:11]}" if v.get("enabledAt") else ""
            roi = v.get("roi")
            roi_txt = f" · ROI {roi:+.2f}%" if isinstance(roi, (int, float)) else ""
            lines.append(
                f"{'★' if v is rows[0] else '·'} {v.get('label')}：{_money(v.get('totalPnl'))}{roi_txt}"
                f" · {n} 筆 · 平均 {_money(avg)}/筆 · 回撤 ${float(v.get('maxDrawdown') or 0):.2f}"
                f"{' · 持倉中' if v.get('hasPosition') else ''}{since}"
            )
        out.append("\n".join(lines))
    return "\n\n".join(out) if out else "📊 模擬盤沒有資料"


HELP_TEXT = (
    "可用指令：\n"
    "/status — 實盤開關、策略、部位、餘額\n"
    "/live — 實盤真實下單開關（REAL ↔ DRY-RUN，按鈕確認後切換並重啟）\n"
    "/strategy — 更換實盤策略（選實盤 → 選模擬盤的買領先方變體 → 確認；不動每注%與 REAL/DRY-RUN）\n"
    "/stake — 改實盤每注 %（選實盤 → 選 5～100% → 確認；或 /stake <實盤編號> <數字>，0.5～100）\n"
    "/stop — 改模擬盤變體的停損（買領先方／跟單／便宜邊等單腿策略；選資產 → 選變體 → 選值 → 確認）；有實盤在用同一變體會一起改並重啟\n"
    "/loss — 分析某實盤最近的真實虧損原因（選實盤；或 /loss <實盤編號> [筆數]，預設 5 筆）\n"
    "/roi — 模擬盤 ROI 排行（各資產前 5、≥10 筆；含打平勝率、1 輸＝幾贏）\n"
    "/tune — 某模擬盤變體的停損回放（各檔位假停損／淨損益）\n"
    "/mirror — 模擬盤 vs 實盤同窗口一致性（最近 12h 誰有進誰沒進）\n"
    "/disable /enable — 停用／啟用模擬盤變體（改停用清單；實盤①空手時重啟主進程）\n"
    "/reset — 清空某模擬盤變體重記，或實盤頁面全部重製（備份後重設基準）\n"
    "/pnl — 實盤損益、平均每筆、最好／最差、今日統計\n"
    "/trades [n] — 最近 n 筆真單（預設 10）\n"
    "/sim — 模擬盤各組損益\n"
    "/scan — 選擇要掃描的市場（BTC 5m／15m、ETH、SOL、XRP、其他＝全站探索最多人玩的盤）\n"
    "/report — 最近一次市場掃描的建議摘要\n"
    "/help — 這份說明\n"
    "（除了 /live 開關與 /scan 之外都是純查詢；主動推播：停機／恢復、REAL↔DRY-RUN 切換、新部位）"
)


# ── 2026-09-15 依使用者要求：TG 上的實盤真實下單開關 ────────────────────────
# 只改 .env 的 POLY_STRATEGY_ARMED 並重啟 gravia.service + gravia-status.service；
# 開啟 REAL 要再按一次確認；實盤有持倉／待結算時拒絕切換（等結算完再切）。
ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
LIVE_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_live_strategy_state.json")


def write_env_flag(key: str, value: str, path: str = ENV_FILE) -> None:
    lines = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    prefix, replacement, done = f"{key}=", f"{key}={value}\n", False
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            lines[i] = replacement; done = True
            break
    if not done:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(replacement)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.writelines(lines)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def live_toggle_keyboard(execution_enabled: bool, idx: int = 0) -> list[list[dict]]:
    if execution_enabled:
        rows = [[{"text": "🔴 切換為 DRY-RUN（停止真實下單）", "callback_data": f"live:{idx}:dry"}]]
    else:
        rows = [[{"text": "🟢 開啟真實下單（REAL）", "callback_data": f"live:{idx}:real"}]]
    rows.append([{"text": "🔁 更換策略", "callback_data": f"strat:{idx}"},      # 2026-09-20
                 {"text": "💰 每注 %", "callback_data": f"stake:{idx}"}])
    return rows


def live_confirm_keyboard(target: str, idx: int = 0) -> list[list[dict]]:
    label = "✅ 確認開啟真實下單" if target == "real" else "✅ 確認切換為 DRY-RUN"
    return [[{"text": label, "callback_data": f"live:{idx}:{target}:confirm"}], [{"text": "取消", "callback_data": "live:cancel"}]]


def _live_position_open(state_path: str = LIVE_STATE_FILE) -> bool:
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            st = json.load(f)
    except Exception:
        return False
    return bool(st.get("position")) or bool(st.get("pendingSettlements"))


async def apply_live_mode(target: str, idx: int = 0) -> str:
    """target: 'real' | 'dry'；idx = 第幾個實盤。回傳給使用者看的結果文字。"""
    inst = LIVE_INSTANCES[idx] if 0 <= idx < len(LIVE_INSTANCES) else LIVE_INSTANCES[0]
    if _live_position_open(inst["state"]):
        return f"⏸ {inst['name']}目前有持倉或待結算，先不切換；等結算完再按一次。"
    write_env_flag("POLY_STRATEGY_ARMED", "true" if target == "real" else "false", inst["env"])
    proc = await asyncio.create_subprocess_exec(
        "sudo", "-n", "systemctl", "restart", *inst["services"],
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), 90)
    mode = "REAL" if target == "real" else "DRY-RUN"
    if proc.returncode != 0:
        return f"⚠️ {inst['name']} 的 env 已改為 {mode}，但重啟失敗（{proc.returncode}）：{(out or b'').decode(errors='replace')[:300]}"
    return (f"🟢 {inst['name']}已開啟真實下單（REAL），服務已重啟。" if target == "real" else f"🔴 {inst['name']}已切換為 DRY-RUN，服務已重啟。")


async def send_live_menu(client: httpx.AsyncClient, chat_id: int) -> None:
    for idx, inst in enumerate(LIVE_INSTANCES):
        try:
            live = await fetch_snapshot(inst["ws"])
        except Exception as exc:
            await tg_send(client, chat_id, f"⚠️ 讀不到{inst['name']}狀態（{exc.__class__.__name__}）。")
            continue
        enabled = bool(live.get("strategyExecutionEnabled"))
        cfg = live.get("strategyConfig") or {}
        pos = (live.get("strategyState") or {}).get("position")
        text = (f"{inst['name']}目前：{'🟢 REAL 真實下單' if enabled else '🔴 DRY-RUN'}\n"
                f"策略：{cfg.get('label') or '—'}\n"
                f"部位：{'持倉中（切換要等結算）' if pos else '空手'}")
        try:
            await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": text,
                                                            "reply_markup": {"inline_keyboard": live_toggle_keyboard(enabled, idx)}})
        except Exception as exc:
            log.warning(f"sendMessage(live menu) failed: {exc}")

# ── 2026-09-20 依使用者要求：TG 上更換實盤策略 ──────────────────────────────
# /strategy → 選實盤 → 列出主模擬盤「可供實盤選用」（lateFavorite 且非 simOnly）的變體 → 確認 → 寫該實盤 env
# （資產／變體／買價區間／停損；②③ 另寫進程內輕量模擬盤的資產與變體）、自動改 TG 選單名稱、重啟該實盤服務。
# 不動每注 % 與 POLY_STRATEGY_ARMED；有持倉／待結算時拒絕。
_STRATEGY_CANDIDATES: dict[int, list[dict]] = {}
_ASSET_SHORT = {"btc": "BTC5m", "btc-15m": "BTC15m", "eth-alt": "ETH", "eth": "ETH", "eth-15m": "ETH15m",
                "xrp": "XRP", "xrp-15m": "XRP15m", "sol": "SOL", "sol-15m": "SOL15m", "doge": "DOGE", "bnb": "BNB"}
_CIRCLED = "①②③④⑤⑥⑦⑧⑨"


def strategy_candidates(sim: dict) -> list[dict]:
    """主模擬盤快照裡可供實盤選用的變體（買領先方家族、非 simOnly），依資產、損益排序。"""
    rows = [v for v in (sim.get("abVariants") or []) if v.get("lateFavorite") and not v.get("simOnly")]
    order = {a.get("id"): i for i, a in enumerate(sim.get("assetList") or [])}
    rows.sort(key=lambda v: (order.get(v.get("assetId"), 99), -float(v.get("totalPnl") or 0)))
    return rows


def instance_short_name(idx: int, v: dict) -> str:
    """TG 選單名稱，例：實盤③BTC15m-60~90s-098。"""
    asset = _ASSET_SHORT.get(str(v.get("assetId")), str(v.get("assetId") or "").upper())
    lo = int(float(v.get("favoriteMinRemaining") or 0)); hi = int(float(v.get("favoriteWindowSeconds") or 0))
    price = f"{int(round(float(v.get('favoriteMinPrice') or 0) * 100)):03d}"
    mark = _CIRCLED[idx] if idx < len(_CIRCLED) else str(idx + 1)
    return f"實盤{mark}{asset}-{lo}~{hi}s-{price}"


def _read_env_value(path: str, key: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith(key + "="):
                    return line[len(key) + 1:].strip()
    except FileNotFoundError:
        pass
    return ""


def strategy_env_updates(inst: dict, v: dict, main_env: str = ENV_FILE) -> dict[str, str]:
    """要寫進該實盤 env 的鍵值。① 用主 .env：只確認資產在 POLY_SIM_ASSETS 裡；②③ 的輕量模擬盤只跑這一組。"""
    stop = v.get("favoriteStopLossPrice")
    upd = {
        "POLY_LIVE_ASSET_ID": str(v["assetId"]),
        "POLY_LIVE_VARIANT_ID": str(v["id"]),
        "POLY_LIVE_FAVORITE_MIN_PRICE": f"{float(v.get('favoriteMinPrice') or 0):.2f}",
        "POLY_LIVE_FAVORITE_MAX_PRICE": f"{float(v.get('favoriteMaxPrice') or 0):.2f}",
        "POLY_LIVE_FAVORITE_STOP_LOSS_PRICE": f"{float(stop):.2f}" if stop else "0",
    }
    if os.path.abspath(inst["env"]) == os.path.abspath(main_env):
        assets = [a.strip() for a in _read_env_value(inst["env"], "POLY_SIM_ASSETS").split(",") if a.strip()]
        if v["assetId"] not in assets:
            upd["POLY_SIM_ASSETS"] = ",".join(assets + [str(v["assetId"])])
    else:
        upd["POLY_SIM_ASSETS"] = str(v["assetId"])
        upd["POLY_SIM_ONLY_VARIANTS"] = str(v["id"])
    return upd


def rename_live_instance(idx: int, new_name: str, main_env: str = ENV_FILE) -> str | None:
    """改主 .env 的 TG_LIVE_INSTANCES 第 idx 段的名稱；回傳新值（沒設 TG_LIVE_INSTANCES 就回 None）。"""
    raw = _read_env_value(main_env, "TG_LIVE_INSTANCES")
    if not raw:
        return None
    chunks = raw.split(";")
    if idx >= len(chunks):
        return None
    parts = chunks[idx].split("|")
    parts[0] = new_name
    chunks[idx] = "|".join(parts)
    value = ";".join(chunks)
    write_env_flag("TG_LIVE_INSTANCES", value, main_env)
    return value


def strategy_instance_keyboard() -> list[list[dict]]:
    return [[{"text": f"🔁 {inst['name']}", "callback_data": f"strat:{idx}"}] for idx, inst in enumerate(LIVE_INSTANCES)]


def strategy_list_keyboard(idx: int, rows: list[dict], current_id: str | None) -> list[list[dict]]:
    kb = []
    for n, v in enumerate(rows):
        star = "★ " if v.get("id") == current_id else ""
        roi = v.get("roi")
        roi_txt = f" ROI{roi:+.1f}%" if isinstance(roi, (int, float)) else ""
        text = f"{star}{v.get('label')} {_money(v.get('totalPnl'))}/{int(v.get('totalTrades') or 0)}筆{roi_txt}"
        kb.append([{"text": text[:60], "callback_data": f"strat:{idx}:{n}"}])
    kb.append([{"text": "取消", "callback_data": "strat:cancel"}])
    return kb


def strategy_confirm_keyboard(idx: int, n: int) -> list[list[dict]]:
    return [[{"text": "✅ 確認更換（會重啟該實盤服務）", "callback_data": f"strat:{idx}:{n}:confirm"}],
            [{"text": "取消", "callback_data": "strat:cancel"}]]


async def send_strategy_menu(client: httpx.AsyncClient, chat_id: int) -> None:
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "要更換哪個實盤的策略？",
                                                        "reply_markup": {"inline_keyboard": strategy_instance_keyboard()}})
    except Exception as exc:
        log.warning(f"sendMessage(strategy menu) failed: {exc}")


async def send_strategy_list(client: httpx.AsyncClient, chat_id: int, idx: int) -> None:
    inst = LIVE_INSTANCES[idx]
    try:
        sim = await fetch_snapshot(SIM_WS)
    except Exception as exc:
        await tg_send(client, chat_id, f"⚠️ 讀不到模擬盤快照（{exc.__class__.__name__}），稍後再試。"); return
    rows = strategy_candidates(sim)
    if not rows:
        await tg_send(client, chat_id, "模擬盤目前沒有可供實盤選用的變體。"); return
    _STRATEGY_CANDIDATES[idx] = rows
    current = _read_env_value(inst["env"], "POLY_LIVE_VARIANT_ID") or None
    try:
        await client.post(f"{API}/sendMessage", json={
            "chat_id": chat_id,
            "text": f"{inst['name']} 目前：{current or '—'}\n選擇新策略（★＝目前使用中；數字＝模擬盤損益/筆數/ROI）：",
            "reply_markup": {"inline_keyboard": strategy_list_keyboard(idx, rows, current)}})
    except Exception as exc:
        log.warning(f"sendMessage(strategy list) failed: {exc}")


async def apply_strategy(idx: int, v: dict) -> tuple[str, bool]:
    """套用策略：回傳 (給使用者的文字, 是否需要重啟 TG bot 本身以載入新名稱)。"""
    inst = LIVE_INSTANCES[idx]
    if _live_position_open(inst["state"]):
        return f"⏸ {inst['name']}目前有持倉或待結算，先不更換；等結算完再按一次。", False
    for key, value in strategy_env_updates(inst, v).items():
        write_env_flag(key, value, inst["env"])
    new_name = instance_short_name(idx, v)
    renamed = rename_live_instance(idx, new_name) is not None
    proc = await asyncio.create_subprocess_exec(
        "sudo", "-n", "systemctl", "restart", *inst["services"],
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), 90)
    armed = _read_env_value(inst["env"], "POLY_STRATEGY_ARMED").lower() == "true"
    stake = _read_env_value(inst["env"], "POLY_STAKE_PCT") or "?"
    if proc.returncode != 0:
        return (f"⚠️ {inst['name']} 的 env 已改為 {v.get('label')}，但重啟失敗（{proc.returncode}）："
                f"{(out or b'').decode(errors='replace')[:300]}"), renamed
    stop = v.get("favoriteStopLossPrice")
    return (f"🔁 {inst['name']} 已改為：{v.get('label')}\n"
            f"買價 {float(v.get('favoriteMinPrice') or 0):.2f}～{float(v.get('favoriteMaxPrice') or 0):.2f}"
            f" · {('停損 ' + format(float(stop), '.2f')) if stop else '不停損'}"
            f" · 每注 {stake}% · 模式 {'REAL' if armed else 'DRY-RUN'}（未變）\n服務已重啟"
            + (f"；TG 選單名稱改為「{new_name}」，bot 重啟中" if renamed else "")), renamed


# ── 2026-09-20 依使用者要求：TG 上改實盤每注 % ──────────────────────────────
STAKE_PRESETS = (5, 10, 15, 20, 25, 30, 50, 75, 100)
STAKE_MIN, STAKE_MAX = 0.5, 100.0   # 與 polymarket_live_strategy.STAKE_PCT 的夾限一致（2026-09-20 上限 30 → 100）


def parse_stake_pct(text: str) -> float | None:
    try:
        v = float(text)
    except (TypeError, ValueError):
        return None
    return v if STAKE_MIN <= v <= STAKE_MAX else None


def _fmt_pct(v: float) -> str:
    return f"{v:g}"


def stake_instance_keyboard() -> list[list[dict]]:
    rows = []
    for idx, inst in enumerate(LIVE_INSTANCES):
        cur = _read_env_value(inst["env"], "POLY_STAKE_PCT") or "?"
        rows.append([{"text": f"💰 {inst['name']}（目前 {cur}%）", "callback_data": f"stake:{idx}"}])
    return rows


def stake_pct_keyboard(idx: int, current: str) -> list[list[dict]]:
    row = [{"text": ("★ " if _fmt_pct(float(p)) == current else "") + f"{p}%", "callback_data": f"stake:{idx}:{p}"} for p in STAKE_PRESETS]
    return [row[:3], row[3:6], row[6:], [{"text": "取消", "callback_data": "stake:cancel"}]]


def stake_confirm_keyboard(idx: int, pct: float) -> list[list[dict]]:
    return [[{"text": f"✅ 確認改為 {_fmt_pct(pct)}%（會重啟該實盤服務）", "callback_data": f"stake:{idx}:{_fmt_pct(pct)}:confirm"}],
            [{"text": "取消", "callback_data": "stake:cancel"}]]


async def send_stake_menu(client: httpx.AsyncClient, chat_id: int) -> None:
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "要改哪個實盤的每注 %？（也可直接輸入 /stake <實盤編號> <數字>）",
                                                        "reply_markup": {"inline_keyboard": stake_instance_keyboard()}})
    except Exception as exc:
        log.warning(f"sendMessage(stake menu) failed: {exc}")


async def send_stake_pct_menu(client: httpx.AsyncClient, chat_id: int, idx: int) -> None:
    inst = LIVE_INSTANCES[idx]
    cur = _read_env_value(inst["env"], "POLY_STAKE_PCT") or "?"
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": f"{inst['name']} 目前每注 {cur}%，改為：",
                                                        "reply_markup": {"inline_keyboard": stake_pct_keyboard(idx, cur)}})
    except Exception as exc:
        log.warning(f"sendMessage(stake pct menu) failed: {exc}")


async def send_stake_confirm(client: httpx.AsyncClient, chat_id: int, idx: int, pct: float) -> None:
    inst = LIVE_INSTANCES[idx]
    cur = _read_env_value(inst["env"], "POLY_STAKE_PCT") or "?"
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id,
                                                        "text": f"確定把 {inst['name']} 每注 {cur}% → {_fmt_pct(pct)}%？（不動策略與 REAL/DRY-RUN；有持倉會被拒絕）"
                                                                + (f"\n⚠️ 超過 30%：一次翻面就是總資產的 {_fmt_pct(pct)}%；多盤同時持倉時後面的盤會因現金不足縮注或跳過。" if pct > 30 else ""),
                                                        "reply_markup": {"inline_keyboard": stake_confirm_keyboard(idx, pct)}})
    except Exception as exc:
        log.warning(f"sendMessage(stake confirm) failed: {exc}")


async def apply_stake(idx: int, pct: float) -> str:
    inst = LIVE_INSTANCES[idx]
    if _live_position_open(inst["state"]):
        return f"⏸ {inst['name']}目前有持倉或待結算，先不改；等結算完再按一次。"
    old = _read_env_value(inst["env"], "POLY_STAKE_PCT") or "?"
    write_env_flag("POLY_STAKE_PCT", _fmt_pct(pct), inst["env"])
    proc = await asyncio.create_subprocess_exec(
        "sudo", "-n", "systemctl", "restart", *inst["services"],
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), 90)
    armed = _read_env_value(inst["env"], "POLY_STRATEGY_ARMED").lower() == "true"
    if proc.returncode != 0:
        return f"⚠️ {inst['name']} 的 env 已改為每注 {_fmt_pct(pct)}%，但重啟失敗（{proc.returncode}）：{(out or b'').decode(errors='replace')[:300]}"
    return f"💰 {inst['name']} 每注 {old}% → {_fmt_pct(pct)}%，服務已重啟，模式 {'REAL' if armed else 'DRY-RUN'}（未變）"


# ── 2026-09-20 依使用者要求：TG 上改模擬盤變體的停損；有實盤在用同一變體就一起改 ──────────
# 模擬盤：寫 sim_variant_overrides.json（主進程每 5 秒熱更新，不用重啟）；實盤：改該實盤 env 的
# POLY_LIVE_FAVORITE_STOP_LOSS_PRICE 並重啟其服務（有持倉先不重啟，等結算後再按一次）。
SIM_VARIANT_OVERRIDES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim_variant_overrides.json")
STOP_PRESETS = ("0", "0.40", "0.50", "0.60", "0.70", "0.80", "0.90")
_STOP_CANDIDATES: dict[str, list[dict]] = {}   # asset_id -> variants


def parse_stop_price(text: str) -> float | None:
    """回傳停損價；'0'／'none' = 不停損（回 0.0）；無效回 None。"""
    t = str(text).strip().lower()
    if t in ("0", "none", "off", "不停損"):
        return 0.0
    try:
        v = float(t)
    except ValueError:
        return None
    return v if 0.05 <= v <= 0.95 else None


def _stop_txt(stop) -> str:
    return f"停損 {float(stop):.2f}" if stop else "不停損"


def stop_variants(sim: dict) -> dict[str, list[dict]]:
    """主模擬盤裡的買領先方變體，依資產分組（含 simOnly；停損欄位就是它的現值）。"""
    out: dict[str, list[dict]] = {}
    for v in sim.get("abVariants") or []:
        # 2026-09-21 依使用者要求：所有單腿方向性策略（買領先方／開盤動能／開盤反向／跟單／買便宜邊）都可設停損
        if any(v.get(f) for f in ("lateFavorite", "openMomentum", "openReversal", "followWallets", "lateUnderdog")):
            out.setdefault(str(v.get("assetId")), []).append(v)
    for rows in out.values():
        rows.sort(key=lambda v: -float(v.get("totalPnl") or 0))
    return out


def write_variant_override(vid: str, stop: float, path: str = SIM_VARIANT_OVERRIDES_FILE) -> dict:
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            data = {}
    data.setdefault(vid, {})["favoriteStopLossPrice"] = (float(stop) if stop else None)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    return data


def live_instances_using(vid: str) -> list[int]:
    return [idx for idx, inst in enumerate(LIVE_INSTANCES) if _read_env_value(inst["env"], "POLY_LIVE_VARIANT_ID") == vid]


def stop_asset_keyboard(groups: dict[str, list[dict]], asset_labels: dict[str, str]) -> list[list[dict]]:
    kb = [[{"text": f"{asset_labels.get(aid, aid)}（{len(rows)} 組）", "callback_data": f"stop:a:{aid}"}] for aid, rows in groups.items()]
    kb.append([{"text": "取消", "callback_data": "stop:cancel"}])
    return kb


def stop_variant_keyboard(aid: str, rows: list[dict]) -> list[list[dict]]:
    kb = []
    for n, v in enumerate(rows):
        live_mark = "🟢" if live_instances_using(str(v.get("id"))) else ""
        text = f"{live_mark}{v.get('label')} {_money(v.get('totalPnl'))}/{int(v.get('totalTrades') or 0)}筆"
        kb.append([{"text": text[:60], "callback_data": f"stop:v:{aid}:{n}"}])
    kb.append([{"text": "取消", "callback_data": "stop:cancel"}])
    return kb


def stop_value_keyboard(aid: str, n: int, current) -> list[list[dict]]:
    cur = f"{float(current):.2f}" if current else "0"
    btns = [{"text": ("★ " if p == cur else "") + ("不停損" if p == "0" else p), "callback_data": f"stop:s:{aid}:{n}:{p}"} for p in STOP_PRESETS]
    return [btns[:4], btns[4:], [{"text": "取消", "callback_data": "stop:cancel"}]]


def stop_confirm_keyboard(aid: str, n: int, p: str) -> list[list[dict]]:
    return [[{"text": f"✅ 確認改為 {_stop_txt(float(p))}", "callback_data": f"stop:c:{aid}:{n}:{p}"}],
            [{"text": "取消", "callback_data": "stop:cancel"}]]


async def send_stop_asset_menu(client: httpx.AsyncClient, chat_id: int) -> None:
    try:
        sim = await fetch_snapshot(SIM_WS)
    except Exception as exc:
        await tg_send(client, chat_id, f"⚠️ 讀不到模擬盤快照（{exc.__class__.__name__}），稍後再試。"); return
    groups = stop_variants(sim)
    if not groups:
        await tg_send(client, chat_id, "模擬盤目前沒有買領先方變體。"); return
    _STOP_CANDIDATES.clear(); _STOP_CANDIDATES.update(groups)
    labels = {a.get("id"): a.get("label") for a in (sim.get("assetList") or [])}
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "要改哪個資產的變體停損？",
                                                        "reply_markup": {"inline_keyboard": stop_asset_keyboard(groups, labels)}})
    except Exception as exc:
        log.warning(f"sendMessage(stop asset menu) failed: {exc}")


async def send_stop_variant_menu(client: httpx.AsyncClient, chat_id: int, aid: str) -> None:
    rows = _STOP_CANDIDATES.get(aid) or []
    if not rows:
        await tg_send(client, chat_id, "清單已過期，請重新 /stop。"); return
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "選變體（🟢＝有實盤正在用，會一起改）：",
                                                        "reply_markup": {"inline_keyboard": stop_variant_keyboard(aid, rows)}})
    except Exception as exc:
        log.warning(f"sendMessage(stop variant menu) failed: {exc}")


async def send_stop_value_menu(client: httpx.AsyncClient, chat_id: int, aid: str, n: int) -> None:
    v = _STOP_CANDIDATES[aid][n]
    using = [LIVE_INSTANCES[i]["name"] for i in live_instances_using(str(v.get("id")))]
    text = f"{v.get('label')}\n目前：{_stop_txt(v.get('favoriteStopLossPrice'))}" + (f"\n使用中的實盤：{'、'.join(using)}（會一起改並重啟）" if using else "") + "\n改為："
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": text,
                                                        "reply_markup": {"inline_keyboard": stop_value_keyboard(aid, n, v.get("favoriteStopLossPrice"))}})
    except Exception as exc:
        log.warning(f"sendMessage(stop value menu) failed: {exc}")


async def apply_stop(v: dict, stop: float) -> str:
    """模擬盤：寫覆寫檔（主進程熱更新）。實盤：使用同一變體的每一盤改 env 並重啟（有持倉的先不重啟）。"""
    vid = str(v.get("id"))
    write_variant_override(vid, stop)
    lines = [f"🛑 {v.get('label')}：{_stop_txt(v.get('favoriteStopLossPrice'))} → {_stop_txt(stop)}", "模擬盤：已寫入覆寫檔，主進程 5 秒內套用"]
    for idx in live_instances_using(vid):
        inst = LIVE_INSTANCES[idx]
        write_env_flag("POLY_LIVE_FAVORITE_STOP_LOSS_PRICE", f"{float(stop):.2f}" if stop else "0", inst["env"])
        if _live_position_open(inst["state"]):
            lines.append(f"⏸ {inst['name']}：env 已改，但目前有持倉／待結算，未重啟；結算後再按一次或用 /live 重啟")
            continue
        proc = await asyncio.create_subprocess_exec(
            "sudo", "-n", "systemctl", "restart", *inst["services"],
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        out, _ = await asyncio.wait_for(proc.communicate(), 90)
        if proc.returncode != 0:
            lines.append(f"⚠️ {inst['name']}：env 已改，但重啟失敗（{proc.returncode}）：{(out or b'').decode(errors='replace')[:200]}")
        else:
            lines.append(f"🔁 {inst['name']}：已改為 {_stop_txt(stop)}，服務已重啟")
    return "\n".join(lines)


# ── 2026-09-20 依使用者要求：TG 上分析某實盤的虧損原因（純讀取）────────────────────
SIM_DB_PATH = os.environ.get("POLY_SIM_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_sim.sqlite3"))


def loss_instance_keyboard() -> list[list[dict]]:
    return [[{"text": f"🔎 {inst['name']}", "callback_data": f"loss:{idx}"}] for idx, inst in enumerate(LIVE_INSTANCES)]


async def send_loss_menu(client: httpx.AsyncClient, chat_id: int) -> None:
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "要分析哪個實盤的虧損？（也可 /loss <實盤編號> [筆數]）",
                                                        "reply_markup": {"inline_keyboard": loss_instance_keyboard()}})
    except Exception as exc:
        log.warning(f"sendMessage(loss menu) failed: {exc}")


async def run_loss_analysis(idx: int, limit: int = 5) -> str:
    import polymarket_loss_analysis as loss
    inst = LIVE_INSTANCES[idx]
    asset_id = _read_env_value(inst["env"], "POLY_LIVE_ASSET_ID") or "btc"
    variant_id = _read_env_value(inst["env"], "POLY_LIVE_VARIANT_ID") or ""
    return await asyncio.to_thread(loss.analyze_losses, inst["state"], asset_id, variant_id, SIM_DB_PATH, limit, inst["name"])


# ── 2026-09-20 依使用者要求：/roi /tune /mirror /disable /enable /reset ─────────────────────
import polymarket_tg_tools as tools

SIM_DISABLED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim_disabled_variants.json")
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup")
MAIN_SERVICES = ["gravia.service", "gravia-status.service"]
_PICK: dict[str, list[dict]] = {}   # 各指令的候選清單（callback 用索引）


async def _restart_services(services: list[str]) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec("sudo", "-n", "systemctl", "restart", *services,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(), 120)
    return proc.returncode, (out or b"").decode(errors="replace")[:300]


async def _stop_services(services: list[str]) -> None:
    proc = await asyncio.create_subprocess_exec("sudo", "-n", "systemctl", "stop", *services)
    await asyncio.wait_for(proc.wait(), 120)


async def _start_services(services: list[str]) -> None:
    proc = await asyncio.create_subprocess_exec("sudo", "-n", "systemctl", "start", *services)
    await asyncio.wait_for(proc.wait(), 120)


def _live1_real_position_open() -> bool:
    """主進程內嵌實盤①：有真實持倉／待結算就不能重啟主進程。"""
    inst = LIVE_INSTANCES[0]
    try:
        with open(inst["state"], "r", encoding="utf-8") as f:
            st = json.load(f)
    except Exception:
        return False
    pos = st.get("position")
    if pos and not pos.get("dryRun", True):
        return True
    return any(not p.get("dryRun", True) for p in (st.get("pendingSettlements") or []))


def _variants_in_use() -> dict[str, str]:
    return {_read_env_value(inst["env"], "POLY_LIVE_VARIANT_ID"): inst["name"] for inst in LIVE_INSTANCES}


async def _sim_variants_by_asset(client, chat_id, only_late_favorite: bool = False):
    try:
        sim = await fetch_snapshot(SIM_WS)
    except Exception as exc:
        await tg_send(client, chat_id, f"⚠️ 讀不到模擬盤快照（{exc.__class__.__name__}），稍後再試。"); return None, None
    groups: dict[str, list[dict]] = {}
    for v in sim.get("abVariants") or []:
        if only_late_favorite and not v.get("lateFavorite"):
            continue
        groups.setdefault(str(v.get("assetId")), []).append(v)
    for rows in groups.values():
        rows.sort(key=lambda v: -float(v.get("totalPnl") or 0))
    labels = {a.get("id"): a.get("label") for a in (sim.get("assetList") or [])}
    return groups, labels


def _asset_keyboard(prefix: str, groups: dict, labels: dict) -> list[list[dict]]:
    kb = [[{"text": f"{labels.get(aid, aid)}（{len(rows)} 組）", "callback_data": f"{prefix}:a:{aid}"}] for aid, rows in groups.items()]
    kb.append([{"text": "取消", "callback_data": f"{prefix}:cancel"}])
    return kb


def _variant_keyboard(prefix: str, aid: str, rows: list[dict], mark_ids: dict | None = None) -> list[list[dict]]:
    kb = []
    for n, v in enumerate(rows):
        mark = "🟢" if mark_ids and v.get("id") in mark_ids else ""
        kb.append([{"text": f"{mark}{v.get('label')} {_money(v.get('totalPnl'))}/{int(v.get('totalTrades') or 0)}筆"[:60], "callback_data": f"{prefix}:v:{aid}:{n}"}])
    kb.append([{"text": "取消", "callback_data": f"{prefix}:cancel"}])
    return kb


async def send_pick_asset(client, chat_id, prefix: str, title: str, only_late_favorite: bool = False) -> None:
    groups, labels = await _sim_variants_by_asset(client, chat_id, only_late_favorite)
    if not groups:
        return
    _PICK.clear(); _PICK.update(groups)
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": title, "reply_markup": {"inline_keyboard": _asset_keyboard(prefix, groups, labels)}})
    except Exception as exc:
        log.warning(f"sendMessage({prefix} asset) failed: {exc}")


async def send_pick_variant(client, chat_id, prefix: str, aid: str, title: str) -> None:
    rows = _PICK.get(aid) or []
    if not rows:
        await tg_send(client, chat_id, f"清單已過期，請重新 /{prefix}。"); return
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": title,
                                                        "reply_markup": {"inline_keyboard": _variant_keyboard(prefix, aid, rows, _variants_in_use())}})
    except Exception as exc:
        log.warning(f"sendMessage({prefix} variant) failed: {exc}")


def _confirm_kb(prefix: str, aid: str, n: int, text: str) -> list[list[dict]]:
    return [[{"text": text, "callback_data": f"{prefix}:c:{aid}:{n}"}], [{"text": "取消", "callback_data": f"{prefix}:cancel"}]]


async def apply_disable(v: dict, disabled: bool) -> str:
    vid = str(v.get("id"))
    in_use = _variants_in_use()
    if disabled and vid in in_use:
        return f"⛔ {v.get('label')} 正被 {in_use[vid]} 使用，停用會讓實盤崩潰；請先用 /strategy 換掉它。"
    tools.set_variant_disabled(SIM_DISABLED_FILE, vid, disabled)
    action = "停用" if disabled else "啟用"
    if _live1_real_position_open():
        return f"✅ {v.get('label')} 已寫入{action}清單，但實盤①目前有真實持倉，主進程未重啟；結算後再按一次或等下次重啟生效。"
    rc, out = await _restart_services(MAIN_SERVICES)
    return (f"✅ {v.get('label')} 已{action}，主進程已重啟" if rc == 0 else f"⚠️ 已寫入{action}清單，但重啟失敗（{rc}）：{out}")


async def send_enable_menu(client, chat_id) -> None:
    ids = tools.disabled_variants(SIM_DISABLED_FILE)
    if not ids:
        await tg_send(client, chat_id, "停用清單是空的。"); return
    rows = [{"id": vid, "label": vid, "totalPnl": 0, "totalTrades": 0} for vid in sorted(ids)]
    _PICK.clear(); _PICK["_disabled"] = rows
    kb = [[{"text": vid, "callback_data": f"enable:v:_disabled:{n}"}] for n, vid in enumerate(sorted(ids))]
    kb.append([{"text": "取消", "callback_data": "enable:cancel"}])
    try:
        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": f"停用清單（{len(ids)} 組），選一組啟用：", "reply_markup": {"inline_keyboard": kb}})
    except Exception as exc:
        log.warning(f"sendMessage(enable) failed: {exc}")


async def apply_reset_variant(v: dict) -> str:
    if _live1_real_position_open():
        return "⏸ 實盤①目前有真實持倉，主進程不能停；結算後再按一次。"
    await _stop_services(MAIN_SERVICES)
    try:
        msg = await asyncio.to_thread(tools.reset_sim_variant, SIM_DB_PATH, str(v.get("id")), BACKUP_DIR)
    finally:
        await _start_services(MAIN_SERVICES)
    return f"🧹 {v.get('label')}：{msg}；主進程已重啟"


async def apply_reset_live() -> str:
    for inst in LIVE_INSTANCES:
        if _live_position_open(inst["state"]):
            return f"⏸ {inst['name']}目前有持倉或待結算，先不重製；等結算完再按一次。"
    services = sorted({s for inst in LIVE_INSTANCES for s in inst["services"]})
    await _stop_services(services)
    try:
        states = [inst["state"] for inst in LIVE_INSTANCES]
        baselines = [st.replace("_strategy_state.json", "_baseline.json").replace("_state.json", "_baseline.json") for st in states]
        msg = await asyncio.to_thread(tools.reset_live_states, states, baselines, BACKUP_DIR)
    finally:
        await _start_services(services)
    return "🧹 實盤全部重製：\n" + msg + "\n服務已重啟，基準以當下餘額重設"


def reset_menu_keyboard() -> list[list[dict]]:
    return [[{"text": "🧹 清空某模擬盤變體重記", "callback_data": "reset:sim"}],
            [{"text": "🧹 實盤頁面全部重製（三盤）", "callback_data": "reset:live"}],
            [{"text": "取消", "callback_data": "reset:cancel"}]]


async def run_roi() -> str:
    labels = {}
    try:
        sim = await fetch_snapshot(SIM_WS)
        labels = {v.get("id"): v.get("label") for v in (sim.get("abVariants") or [])}
    except Exception:
        pass
    return await asyncio.to_thread(tools.roi_table, SIM_DB_PATH, labels)


async def run_tune(v: dict) -> str:
    return await asyncio.to_thread(tools.stop_replay_table, SIM_DB_PATH, str(v.get("id")), str(v.get("assetId")), v.get("label"))


async def run_mirror(idx: int) -> str:
    inst = LIVE_INSTANCES[idx]
    vid = _read_env_value(inst["env"], "POLY_LIVE_VARIANT_ID") or ""
    return await asyncio.to_thread(tools.mirror_report, SIM_DB_PATH, inst["state"], vid, 12.0, inst["name"])


def mirror_instance_keyboard() -> list[list[dict]]:
    return [[{"text": f"🪞 {inst['name']}", "callback_data": f"mirror:{idx}"}] for idx, inst in enumerate(LIVE_INSTANCES)]


# ── 推播：真實虧損即時分析、餘額不足 5 股警報 ──────────────────────────────
_last_loss_alert: dict[int, float] = {}
_stake_alert_state: dict[int, bool] = {}


def new_real_loss(prev: dict | None, cur: dict) -> dict | None:
    """cur 的最新一筆真實成交若是虧損且 prev 沒有它 → 回傳該成交。"""
    if prev is None:
        return None
    ct = ((cur.get("strategyState") or {}).get("trades") or [])
    pt = ((prev.get("strategyState") or {}).get("trades") or [])
    if not ct:
        return None
    t = ct[0]
    if t.get("dryRun", True) or float(t.get("pnlEstimate") or 0) > 0:
        return None
    key = (t.get("windowSlug"), t.get("exitTime"))
    if pt and (pt[0].get("windowSlug"), pt[0].get("exitTime")) == key:
        return None
    return t


def stake_alert_text(cur: dict, stake_pct: float, name: str) -> str | None:
    bal = cur.get("balanceUsdc")
    if bal is None or not cur.get("strategyExecutionEnabled"):
        return None
    pos = (cur.get("strategyState") or {}).get("position") or {}
    cost = float(pos.get("stakeUsd") or 0) if pos and not pos.get("dryRun", True) else 0.0
    chk = tools.stake_shares_check(float(bal), cost, stake_pct)
    if chk["ok"]:
        return None
    need = f"{chk['minPctNeeded']:.0f}%" if chk["minPctNeeded"] else "—"
    return (f"⚠️ {name} 餘額 ${float(bal):.2f}、每注 {stake_pct:g}% → 注金 ${chk['budget']:.2f} 只買得到 {chk['shares']} 股（最低 5 股），"
            f"下單會被拒；至少要 {need}（/stake 調整）")


async def _restart_self_later() -> None:
    await asyncio.sleep(1.0)
    proc = await asyncio.create_subprocess_exec("sudo", "-n", "systemctl", "restart", "gravia-tg.service")
    await proc.wait()


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
    lines = [f"📈 最近一次掃描：{files[-1][:-5]}（最近 {float(data.get('hours') or 0):.0f}h、{data['report']['windows']} 窗）", "最賺型態（依粗估 PnL）："]
    for k in sorted(data["report"]["kinds"], key=lambda k: k.get("pnl", 0), reverse=True)[:4]:
        lines.append(f"• {k['label']}：粗估 {k.get('pnl', 0):+.0f} · {k['share']*100:.0f}%")
    for sug in data.get("suggestions", []):
        lines.append(f"\n🔎 {sug['title']}\n{sug['finding']}\n👍 {sug['pros']}\n👎 {sug['cons']}")
    return "\n".join(lines)


SCAN_MARKETS = [("BTC 5 分鐘", "btc"), ("BTC 15 分鐘", "btc-15m"), ("ETH 5 分鐘", "eth"),
                ("SOL 5 分鐘", "sol"), ("XRP 5 分鐘", "xrp"), ("其他：全站最多人玩的盤", "other")]


async def send_scan_menu(client: httpx.AsyncClient, chat_id: int) -> None:
    keyboard = [[{"text": label, "callback_data": f"scan:{key}"}] for label, key in SCAN_MARKETS]
    try:
        await client.post(f"{API}/sendMessage", json={
            "chat_id": chat_id, "text": "要掃描哪個市場？（Up/Down 市場掃最近 24 小時；「其他」會撈全站 24h 成交額最高的盤並推估玩法）",
            "reply_markup": {"inline_keyboard": keyboard},
        })
    except Exception as exc:
        log.warning(f"sendMessage(menu) failed: {exc}")


async def run_scan_in_background(client: httpx.AsyncClient, chat_id: int, hours: float, market: str = "btc") -> None:
    import sys
    if _scan_running["v"]:
        await tg_send(client, chat_id, "⏳ 已有一次掃描在跑，請稍候。")
        return
    _scan_running["v"] = True
    try:
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_weekly_scan.py")
        proc = await asyncio.create_subprocess_exec(sys.executable, script, "--hours", str(hours), "--market", market)
        code = await proc.wait()
        if code != 0:
            await tg_send(client, chat_id, f"⚠️ 掃描結束但回傳碼 {code}，請看 gravia-tg.service 日誌。")
    finally:
        _scan_running["v"] = False


async def _for_each_live(fmt) -> str:
    """多實盤：每個實盤各跑一次格式化，前面加名稱；單實盤不加標題。"""
    if len(LIVE_INSTANCES) == 1:
        return fmt(await fetch_snapshot(LIVE_INSTANCES[0]["ws"]))
    out = []
    for inst in LIVE_INSTANCES:
        try:
            out.append(f"【{inst['name']}】\n" + fmt(await fetch_snapshot(inst["ws"])))
        except Exception as exc:
            out.append(f"【{inst['name']}】\n⚠️ 讀不到狀態（{exc.__class__.__name__}）")
    return "\n\n".join(out)


async def handle_command(text: str) -> str:
    parts = (text or "").strip().split()
    if not parts:
        return HELP_TEXT
    cmd = parts[0].split("@")[0].lower()
    try:
        if cmd in ("/status", "/start"):
            return await _for_each_live(format_status)
        if cmd == "/pnl":
            return await _for_each_live(format_pnl)
        if cmd == "/trades":
            limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10
            return await _for_each_live(lambda live: format_trades(live, max(1, min(limit, 30))))
        if cmd == "/sim":
            asset = parts[1].lower() if len(parts) > 1 else None
            alias = {"eth": "eth-alt", "15m": "btc-15m", "btc15m": "btc-15m"}
            return format_sim(await fetch_snapshot(SIM_WS), alias.get(asset, asset))
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
            params = {"timeout": 25, "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset is not None:
                params["offset"] = offset
            r = await client.get(f"{API}/getUpdates", params=params, timeout=35)
            data = r.json()
            for upd in data.get("result", []):
                offset = int(upd["update_id"]) + 1
                cq = upd.get("callback_query")
                if cq:
                    uid = int((cq.get("from") or {}).get("id") or 0)
                    chat_id = ((cq.get("message") or {}).get("chat") or {}).get("id")
                    try:
                        await client.post(f"{API}/answerCallbackQuery", json={"callback_query_id": cq.get("id")})
                    except Exception:
                        pass
                    if uid not in ALLOWED_USER_IDS or not chat_id:
                        continue
                    data_str = str(cq.get("data") or "")
                    if data_str.startswith("live:"):
                        parts_cb = data_str.split(":")          # live:<idx>:<real|dry>[:confirm]
                        if data_str == "live:cancel":
                            await tg_send(client, chat_id, "已取消。")
                        elif len(parts_cb) == 3 and parts_cb[1].isdigit() and parts_cb[2] in ("real", "dry"):
                            idx, target = int(parts_cb[1]), parts_cb[2]
                            name = LIVE_INSTANCES[idx]["name"] if idx < len(LIVE_INSTANCES) else "實盤"
                            warn = ("⚠️ 這會用真錢下單。" if target == "real" else "")
                            try:
                                await client.post(f"{API}/sendMessage", json={
                                    "chat_id": chat_id,
                                    "text": f"{warn}確定要把{name}{'開啟真實下單（REAL）' if target == 'real' else '切換為 DRY-RUN'}？會重啟其服務。",
                                    "reply_markup": {"inline_keyboard": live_confirm_keyboard(target, idx)}})
                            except Exception as exc:
                                log.warning(f"sendMessage(live confirm) failed: {exc}")
                        elif len(parts_cb) == 4 and parts_cb[1].isdigit() and parts_cb[3] == "confirm" and parts_cb[2] in ("real", "dry"):
                            log.info(f"[TG] user {uid} switching live#{parts_cb[1]} to {parts_cb[2]}")
                            try:
                                await tg_send(client, chat_id, await apply_live_mode(parts_cb[2], int(parts_cb[1])))
                            except Exception as exc:
                                await tg_send(client, chat_id, f"⚠️ 切換失敗：{exc}")
                        continue
                    if data_str.startswith("strat:"):
                        parts_cb = data_str.split(":")          # strat:<idx> | strat:<idx>:<n> | strat:<idx>:<n>:confirm | strat:cancel
                        if data_str == "strat:cancel":
                            await tg_send(client, chat_id, "已取消。")
                        elif len(parts_cb) == 2 and parts_cb[1].isdigit() and int(parts_cb[1]) < len(LIVE_INSTANCES):
                            await send_strategy_list(client, chat_id, int(parts_cb[1]))
                        elif len(parts_cb) >= 3 and parts_cb[1].isdigit() and parts_cb[2].isdigit():
                            idx, n = int(parts_cb[1]), int(parts_cb[2])
                            rows = _STRATEGY_CANDIDATES.get(idx) or []
                            if n >= len(rows):
                                await tg_send(client, chat_id, "清單已過期，請重新 /strategy。")
                            elif len(parts_cb) == 3:
                                v = rows[n]
                                try:
                                    await client.post(f"{API}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": f"確定把 {LIVE_INSTANCES[idx]['name']} 改為：\n{v.get('label')}\n（不動每注 % 與 REAL/DRY-RUN；有持倉會被拒絕）",
                                        "reply_markup": {"inline_keyboard": strategy_confirm_keyboard(idx, n)}})
                                except Exception as exc:
                                    log.warning(f"sendMessage(strategy confirm) failed: {exc}")
                            elif parts_cb[3] == "confirm":
                                log.info(f"[TG] user {uid} switching live#{idx} strategy to {rows[n].get('id')}")
                                try:
                                    text, restart_self = await apply_strategy(idx, rows[n])
                                    await tg_send(client, chat_id, text)
                                    if restart_self:
                                        asyncio.get_running_loop().create_task(_restart_self_later())
                                except Exception as exc:
                                    await tg_send(client, chat_id, f"⚠️ 更換失敗：{exc}")
                        continue
                    if data_str.startswith("stake:"):
                        parts_cb = data_str.split(":")          # stake:<idx> | stake:<idx>:<pct> | stake:<idx>:<pct>:confirm | stake:cancel
                        if data_str == "stake:cancel":
                            await tg_send(client, chat_id, "已取消。")
                        elif len(parts_cb) == 2 and parts_cb[1].isdigit() and int(parts_cb[1]) < len(LIVE_INSTANCES):
                            await send_stake_pct_menu(client, chat_id, int(parts_cb[1]))
                        elif len(parts_cb) >= 3 and parts_cb[1].isdigit() and int(parts_cb[1]) < len(LIVE_INSTANCES) and parse_stake_pct(parts_cb[2]) is not None:
                            idx, pct = int(parts_cb[1]), parse_stake_pct(parts_cb[2])
                            if len(parts_cb) == 3:
                                await send_stake_confirm(client, chat_id, idx, pct)
                            elif parts_cb[3] == "confirm":
                                log.info(f"[TG] user {uid} setting live#{idx} stake to {pct}%")
                                try:
                                    await tg_send(client, chat_id, await apply_stake(idx, pct))
                                except Exception as exc:
                                    await tg_send(client, chat_id, f"⚠️ 修改失敗：{exc}")
                        continue
                    if data_str.startswith("stop:"):
                        parts_cb = data_str.split(":")   # stop:a:<aid> | stop:v:<aid>:<n> | stop:s:<aid>:<n>:<p> | stop:c:<aid>:<n>:<p> | stop:cancel
                        try:
                            if data_str == "stop:cancel":
                                await tg_send(client, chat_id, "已取消。")
                            elif parts_cb[1] == "a" and len(parts_cb) == 3:
                                await send_stop_variant_menu(client, chat_id, parts_cb[2])
                            elif parts_cb[1] == "v" and len(parts_cb) == 4 and parts_cb[3].isdigit() and int(parts_cb[3]) < len(_STOP_CANDIDATES.get(parts_cb[2]) or []):
                                await send_stop_value_menu(client, chat_id, parts_cb[2], int(parts_cb[3]))
                            elif parts_cb[1] in ("s", "c") and len(parts_cb) == 5 and parts_cb[3].isdigit() and int(parts_cb[3]) < len(_STOP_CANDIDATES.get(parts_cb[2]) or []) and parse_stop_price(parts_cb[4]) is not None:
                                aid, n, p = parts_cb[2], int(parts_cb[3]), parts_cb[4]
                                v = _STOP_CANDIDATES[aid][n]
                                if parts_cb[1] == "s":
                                    using = [LIVE_INSTANCES[i]["name"] for i in live_instances_using(str(v.get("id")))]
                                    await client.post(f"{API}/sendMessage", json={
                                        "chat_id": chat_id,
                                        "text": f"確定把 {v.get('label')} 改為 {_stop_txt(parse_stop_price(p))}？" + (f"\n實盤 {'、'.join(using)} 會一起改並重啟（有持倉會先不重啟）" if using else ""),
                                        "reply_markup": {"inline_keyboard": stop_confirm_keyboard(aid, n, p)}})
                                else:
                                    log.info(f"[TG] user {uid} setting stop of {v.get('id')} to {p}")
                                    await tg_send(client, chat_id, await apply_stop(v, parse_stop_price(p)))
                            else:
                                await tg_send(client, chat_id, "清單已過期，請重新 /stop。")
                        except Exception as exc:
                            await tg_send(client, chat_id, f"⚠️ 停損修改失敗：{exc}")
                        continue
                    if data_str.startswith("loss:"):
                        parts_cb = data_str.split(":")
                        if len(parts_cb) == 2 and parts_cb[1].isdigit() and int(parts_cb[1]) < len(LIVE_INSTANCES):
                            try:
                                await tg_send(client, chat_id, await run_loss_analysis(int(parts_cb[1])))
                            except Exception as exc:
                                await tg_send(client, chat_id, f"⚠️ 分析失敗：{exc}")
                        continue
                    if data_str.split(":")[0] in ("disable", "enable", "tune", "reset", "mirror", "rsim"):
                        parts_cb = data_str.split(":")
                        prefix = parts_cb[0]
                        try:
                            if parts_cb[-1] == "cancel":
                                await tg_send(client, chat_id, "已取消。")
                            elif prefix == "mirror" and len(parts_cb) == 2 and parts_cb[1].isdigit() and int(parts_cb[1]) < len(LIVE_INSTANCES):
                                await tg_send(client, chat_id, await run_mirror(int(parts_cb[1])))
                            elif prefix == "reset" and len(parts_cb) == 2:
                                if parts_cb[1] == "sim":
                                    await send_pick_asset(client, chat_id, "rsim", "要清空哪個資產的變體？")
                                elif parts_cb[1] == "live":
                                    await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "確定重製三個實盤頁面？（備份後清空成交／計數，基準以當下餘額重設；有持倉會被拒絕）",
                                                                                    "reply_markup": {"inline_keyboard": [[{"text": "✅ 確認重製", "callback_data": "reset:live:confirm"}], [{"text": "取消", "callback_data": "reset:cancel"}]]}})
                            elif prefix == "reset" and data_str == "reset:live:confirm":
                                log.info(f"[TG] user {uid} resetting live states")
                                await tg_send(client, chat_id, await apply_reset_live())
                            elif len(parts_cb) >= 3 and parts_cb[1] == "a":
                                titles = {"disable": "選要停用的變體（🟢＝實盤使用中，不能停用）：", "tune": "選要回放停損的變體：", "rsim": "選要清空重記的變體（🟢＝實盤使用中）："}
                                await send_pick_variant(client, chat_id, prefix, parts_cb[2], titles.get(prefix, "選變體："))
                            elif len(parts_cb) == 4 and parts_cb[1] == "v" and parts_cb[3].isdigit() and int(parts_cb[3]) < len(_PICK.get(parts_cb[2]) or []):
                                aid, n = parts_cb[2], int(parts_cb[3]); v = _PICK[aid][n]
                                if prefix == "tune":
                                    await tg_send(client, chat_id, await run_tune(v))
                                else:
                                    verb = {"disable": "停用", "enable": "啟用", "rsim": "清空重記"}[prefix]
                                    await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": f"確定{verb}：{v.get('label')}？",
                                                                                    "reply_markup": {"inline_keyboard": _confirm_kb(prefix, aid, n, f"✅ 確認{verb}")}})
                            elif len(parts_cb) == 4 and parts_cb[1] == "c" and parts_cb[3].isdigit() and int(parts_cb[3]) < len(_PICK.get(parts_cb[2]) or []):
                                v = _PICK[parts_cb[2]][int(parts_cb[3])]
                                log.info(f"[TG] user {uid} {prefix} {v.get('id')}")
                                if prefix == "disable":
                                    await tg_send(client, chat_id, await apply_disable(v, True))
                                elif prefix == "enable":
                                    await tg_send(client, chat_id, await apply_disable(v, False))
                                elif prefix == "rsim":
                                    await tg_send(client, chat_id, await apply_reset_variant(v))
                            else:
                                await tg_send(client, chat_id, f"清單已過期，請重新 /{prefix}。")
                        except Exception as exc:
                            await tg_send(client, chat_id, f"⚠️ 操作失敗：{exc}")
                        continue
                    if data_str.startswith("scan:"):
                        market = data_str.split(":", 1)[1]
                        label = next((l for l, k in SCAN_MARKETS if k == market), market)
                        await tg_send(client, chat_id, f"🔍 開始掃描：{label}（完成後推播，Up/Down 市場約 10～15 分鐘、全站探索約 1～2 分鐘）。")
                        asyncio.get_running_loop().create_task(run_scan_in_background(client, chat_id, 24.0, market))
                    continue
                msg = upd.get("message") or {}
                chat_id = (msg.get("chat") or {}).get("id")
                if not chat_id:
                    continue
                if not is_allowed(upd):
                    log.info(f"ignored message from user {((msg.get('from') or {}).get('id'))}")
                    continue
                text = msg.get("text") or ""
                parts = text.strip().split()
                if parts and parts[0].split("@")[0].lower() == "/live":
                    await send_live_menu(client, chat_id)
                    continue
                if parts and parts[0].split("@")[0].lower() == "/strategy":
                    await send_strategy_menu(client, chat_id)
                    continue
                cmd0 = parts[0].split("@")[0].lower() if parts else ""
                if cmd0 == "/roi":
                    try:
                        await tg_send(client, chat_id, await run_roi())
                    except Exception as exc:
                        await tg_send(client, chat_id, f"⚠️ 計算失敗：{exc}")
                    continue
                if cmd0 == "/tune":
                    await send_pick_asset(client, chat_id, "tune", "要回放哪個資產的變體？", only_late_favorite=True); continue
                if cmd0 == "/disable":
                    await send_pick_asset(client, chat_id, "disable", "要停用哪個資產的變體？"); continue
                if cmd0 == "/enable":
                    await send_enable_menu(client, chat_id); continue
                if cmd0 == "/reset":
                    try:
                        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "要重製什麼？", "reply_markup": {"inline_keyboard": reset_menu_keyboard()}})
                    except Exception as exc:
                        log.warning(f"sendMessage(reset) failed: {exc}")
                    continue
                if cmd0 == "/mirror":
                    try:
                        await client.post(f"{API}/sendMessage", json={"chat_id": chat_id, "text": "要檢查哪個實盤的鏡像一致性？", "reply_markup": {"inline_keyboard": mirror_instance_keyboard()}})
                    except Exception as exc:
                        log.warning(f"sendMessage(mirror) failed: {exc}")
                    continue
                if parts and parts[0].split("@")[0].lower() == "/loss":
                    if len(parts) >= 2 and parts[1].isdigit() and 1 <= int(parts[1]) <= len(LIVE_INSTANCES):
                        limit = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 5
                        try:
                            await tg_send(client, chat_id, await run_loss_analysis(int(parts[1]) - 1, max(1, min(limit, 20))))
                        except Exception as exc:
                            await tg_send(client, chat_id, f"⚠️ 分析失敗：{exc}")
                    else:
                        await send_loss_menu(client, chat_id)
                    continue
                if parts and parts[0].split("@")[0].lower() == "/stop":
                    await send_stop_asset_menu(client, chat_id)
                    continue
                if parts and parts[0].split("@")[0].lower() == "/stake":
                    # /stake → 選單；/stake <實盤編號 1~N> <數字> → 直接到確認
                    if len(parts) == 3 and parts[1].isdigit() and 1 <= int(parts[1]) <= len(LIVE_INSTANCES) and parse_stake_pct(parts[2]) is not None:
                        await send_stake_confirm(client, chat_id, int(parts[1]) - 1, parse_stake_pct(parts[2]))
                    elif len(parts) == 3:
                        await tg_send(client, chat_id, f"格式：/stake <實盤編號 1～{len(LIVE_INSTANCES)}> <每注 %，{STAKE_MIN:g}～{STAKE_MAX:g}>")
                    else:
                        await send_stake_menu(client, chat_id)
                    continue
                if parts and parts[0].split("@")[0].lower() == "/scan":
                    # 2026-09-16 依使用者要求：移除 /scan [小時]，一律出市場選單
                    await send_scan_menu(client, chat_id)
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
    prev: dict[int, dict | None] = {}
    while True:
        for idx, inst in enumerate(LIVE_INSTANCES):
            try:
                cur = await fetch_snapshot(inst["ws"])
                prefix = f"【{inst['name']}】" if len(LIVE_INSTANCES) > 1 else ""
                for text in diff_alerts(prev.get(idx), cur):
                    for uid in ALLOWED_USER_IDS:
                        await tg_send(client, uid, prefix + text)
                # 2026-09-20：真實虧損即時分析（/loss 的單筆版）
                lost = new_real_loss(prev.get(idx), cur)
                if lost is not None:
                    try:
                        text = await run_loss_analysis(idx, 1)
                    except Exception as exc:
                        text = f"真實虧損 {float(lost.get('pnlEstimate') or 0):+.2f}（分析失敗：{exc}）"
                    for uid in ALLOWED_USER_IDS:
                        await tg_send(client, uid, prefix + "🔻 " + text)
                # 2026-09-20：餘額不足 5 股警報（狀態改變時推一次）
                try:
                    stake_pct = float(_read_env_value(inst["env"], "POLY_STAKE_PCT") or 15.0)
                    warn = stake_alert_text(cur, stake_pct, inst["name"])
                except Exception:
                    warn = None
                if bool(warn) != _stake_alert_state.get(idx, False):
                    _stake_alert_state[idx] = bool(warn)
                    if warn:
                        for uid in ALLOWED_USER_IDS:
                            await tg_send(client, uid, warn)
                prev[idx] = cur
            except Exception as exc:
                log.warning(f"alert snapshot failed ({inst['name']}): {exc}")
        await asyncio.sleep(ALERT_POLL_SECONDS)


async def main() -> None:
    if not BOT_TOKEN or not ALLOWED_USER_IDS:
        raise SystemExit("需要 TG_BOT_TOKEN 與 TG_ALLOWED_USER_IDS")
    log.info(f"TG bot 啟動：白名單 {sorted(ALLOWED_USER_IDS)} · live={[i['name'] + '@' + i['ws'] for i in LIVE_INSTANCES]} · sim={SIM_WS}")
    async with httpx.AsyncClient() as client:
        await asyncio.gather(poll_updates(client), alert_loop(client))


if __name__ == "__main__":
    asyncio.run(main())
