"""
Telegram 查詢機器人：回報實盤／模擬盤目前狀態與損益。純唯讀。

    - 只讀本機兩個狀態伺服器的 WebSocket 快照（實盤 8767、模擬 8766），跟網頁同一份資料。
    - 不碰 .env 私鑰、不下單、不改任何設定；沒有任何指令能改變策略行為。
    - 只回應 TG_ALLOWED_USER_IDS 白名單內的 Telegram user id，其他人一律不理。
    - 主動推播（每 15 秒比對一次快照）：「策略停機／恢復」、「REAL↔DRY-RUN 切換」、「新部位」（含 DRY-RUN）；
      結算不推（2026-09-14 依使用者要求），要看用 /trades。

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
    "/stake — 改實盤每注 %（選實盤 → 選 5～100% → 確認；或 /stake <實盤編號> <數字>，0.5～100）\n"
    "/loss — 分析某實盤最近的真實虧損原因（選實盤；或 /loss <實盤編號> [筆數]，預設 5 筆）\n"
    "/pnl — 實盤損益、平均每筆、最好／最差、今日統計\n"
    "/trades [n] — 最近 n 筆真單（預設 10）\n"
    "/sim — 模擬盤各組損益\n"
    "/help — 這份說明\n"
    "（只有 /live /stake 會改設定，其餘純查詢；主動推播：停機／恢復、REAL↔DRY-RUN 切換、新部位、真實虧損、餘額不足）"
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
    rows.append([{"text": "💰 每注 %", "callback_data": f"stake:{idx}"}])
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

def _read_env_value(path: str, key: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith(key + "="):
                    return line[len(key) + 1:].strip()
    except FileNotFoundError:
        pass
    return ""


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


# ── 推播：真實虧損即時分析、餘額不足 5 股警報 ──────────────────────────────
import polymarket_tg_tools as tools   # 餘額夠不夠買最低 5 股的檢查

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
                    if data_str.startswith("loss:"):
                        parts_cb = data_str.split(":")
                        if len(parts_cb) == 2 and parts_cb[1].isdigit() and int(parts_cb[1]) < len(LIVE_INSTANCES):
                            try:
                                await tg_send(client, chat_id, await run_loss_analysis(int(parts_cb[1])))
                            except Exception as exc:
                                await tg_send(client, chat_id, f"⚠️ 分析失敗：{exc}")
                        continue
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
                if parts and parts[0].split("@")[0].lower() == "/stake":
                    # /stake → 選單；/stake <實盤編號 1~N> <數字> → 直接到確認
                    if len(parts) == 3 and parts[1].isdigit() and 1 <= int(parts[1]) <= len(LIVE_INSTANCES) and parse_stake_pct(parts[2]) is not None:
                        await send_stake_confirm(client, chat_id, int(parts[1]) - 1, parse_stake_pct(parts[2]))
                    elif len(parts) == 3:
                        await tg_send(client, chat_id, f"格式：/stake <實盤編號 1～{len(LIVE_INSTANCES)}> <每注 %，{STAKE_MIN:g}～{STAKE_MAX:g}>")
                    else:
                        await send_stake_menu(client, chat_id)
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
