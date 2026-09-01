"""
Polymarket 真實損益 · 唯讀狀態伺服器
─────────────────────────────────
只查詢真實帳戶狀態（餘額、掛單、成交紀錄），完全不會下單、不會動用任何資金。
用來獨立顯示 polymarket_live_trader.py 那個真實錢包目前的實際狀況，
跟 polymarket_server.py（紙上模擬，port 8766）完全分開、互不影響。

啟動方式：
    py polymarket_live_status_server.py

然後開啟 web/polymarket_live.html
"""

import asyncio
import json
import logging
import os
import sys
import time

from websockets.server import serve

import polymarket_live_trader as live

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

HOST = "localhost"
PORT = 8767
POLL_INTERVAL = 10  # 真實帳戶查詢，別抓太快，10 秒一次就夠

BASELINE_FILE = os.path.join(os.path.dirname(__file__), "polymarket_live_baseline.json")
STRATEGY_STATE_FILE = os.path.join(os.path.dirname(__file__), "polymarket_live_strategy_state.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket_live_status")

CLIENTS: set = set()
last_payload: dict = {"connected": False, "error": None, "fetchedAt": time.time()}
_RESOLVED_TOKENS: set = set()  # 已確認市場結算、訂單簿撤掉的 token，之後不用再查，省 API 呼叫也省吵人的 404 log


def _load_or_init_baseline(current_balance: float) -> dict:
    """第一次抓到真實餘額時記錄成「起始基準」，之後拿目前餘額跟它比較算總收益。
    存成本機檔案，重啟伺服器也不會遺失基準點。"""
    if os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    baseline = {"baselineBalance": current_balance, "baselineSetAt": time.time()}
    with open(BASELINE_FILE, "w", encoding="utf-8") as f:
        json.dump(baseline, f)
    log.info(f"設定總收益起始基準：${current_balance:.2f}")
    return baseline


def _load_strategy_state() -> dict:
    """讀取自動策略持久化狀態。這個狀態檔不含私鑰，本伺服器也不會寫入它。"""
    if not os.path.exists(STRATEGY_STATE_FILE):
        return {}
    try:
        with open(STRATEGY_STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        position = state.get("position")
        return {
            "halted": bool(state.get("halted", False)),
            "haltReason": state.get("haltReason"),
            "position": position,
            "pendingSettlements": len(state.get("pendingSettlements", [])),
            "totalPnlEstimate": float(state.get("totalPnlEstimate", 0)),
            "totalFeesEstimate": float(state.get("totalFeesEstimate", 0)),
            "totalTrades": int(state.get("totalTrades", 0)),
            "lockedTrades": int(state.get("lockedTrades", 0)),
            "directionalTrades": int(state.get("directionalTrades", 0)),
            "earlyExits": int(state.get("earlyExits", 0)),
            "updatedAt": state.get("updatedAt"),
        }
    except Exception as exc:
        return {"halted": True, "haltReason": f"strategy_state_read_failed: {exc}"}


def _compute_open_positions(trades: list, max_tokens: int = 10) -> list:
    """從最近成交紀錄反推「目前實際還持有」的部位，用真實鏈上餘額驗證——
    已經被兌換（redeem）掉的部位查出來會是 0，不會出現在這裡，
    所以這份清單天生就會排除掉已結算完成的舊部位，只留下真的還在抱著的。
    只查最近幾個不同的 token，避免每輪都要打一大堆 API。"""
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    client = live.get_client()

    seen_tokens = []
    for t in trades:
        tid = t.get("asset_id")
        if tid and tid not in seen_tokens:
            seen_tokens.append(tid)
        if len(seen_tokens) >= max_tokens:
            break

    positions = []
    for tid in seen_tokens:
        if tid in _RESOLVED_TOKENS:
            continue
        try:
            bal = client.get_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=tid)
            )
            shares = int(bal.get("balance", 0)) / 1_000_000
        except Exception:
            continue
        if shares < 0.01:
            continue  # 已兌換或本來就沒買到，不算持有中

        entry_trade = next((t for t in trades if t.get("asset_id") == tid), None)
        entry_price = float(entry_trade.get("price", 0)) if entry_trade else 0.0
        cost = shares * entry_price

        current_price = None
        try:
            mid = client.get_midpoint(tid)
            current_price = float(mid.get("mid", 0) or 0)
        except Exception:
            # 查不到中價通常代表這個 token 的市場已經結算、訂單簿已被撤掉
            # （輸的那邊 token 餘額不會歸零，但市場已經沒有意義了）——
            # 這種情況視為已結算，不算持有中，不然結算過的舊部位會永遠留在清單裡。
            # 市場結算是永久狀態，記下來以後不用再查，省得每輪都打 API 又洗一堆 404 log。
            _RESOLVED_TOKENS.add(tid)
            continue

        value = shares * current_price if current_price is not None else None
        pnl = (value - cost) if value is not None else None
        pnl_pct = (pnl / cost * 100) if (pnl is not None and cost > 0) else None

        positions.append({
            "tokenId":      tid,
            "shares":       shares,
            "entryPrice":   entry_price,
            "currentPrice": current_price,
            "cost":         cost,
            "value":        value,
            "pnl":          pnl,
            "pnlPct":       pnl_pct,
        })
    return positions


def _fetch_state() -> dict:
    """同步呼叫 py_clob_client_v2（會阻塞），外面用 asyncio.to_thread 包起來跑，不卡住其他連線。"""
    balance_raw = live.get_usdc_balance()
    orders = live.get_open_orders()
    trades = live.get_trade_history(limit=30)
    positions = _compute_open_positions(trades)
    strategy_state = _load_strategy_state()

    balance_usdc = int(balance_raw.get("balance", 0)) / 1_000_000
    baseline = _load_or_init_baseline(balance_usdc)
    total_pnl = balance_usdc - baseline["baselineBalance"]

    return {
        "connected": True,
        "error": None,
        "fetchedAt": time.time(),
        "funderAddress": live.FUNDER_ADDRESS,
        "liveTradingEnabled": live.LIVE_TRADING,
        "strategyArmed": os.environ.get("POLY_STRATEGY_ARMED", "false").strip().lower() == "true",
        "strategyExecutionEnabled": live.LIVE_TRADING and os.environ.get("POLY_STRATEGY_ARMED", "false").strip().lower() == "true",
        "balanceUsdc": balance_usdc,
        "baselineBalance": baseline["baselineBalance"],
        "baselineSetAt": baseline["baselineSetAt"],
        "totalPnl": total_pnl,
        "openOrders": orders,
        "openPositions": positions,
        "trades": trades,
        "strategyState": strategy_state,
    }


async def broadcast(payload: str) -> None:
    dead = set()
    for client in list(CLIENTS):
        try:
            await client.send(payload)
        except Exception:
            dead.add(client)
    CLIENTS.difference_update(dead)


async def poll_loop():
    global last_payload
    while True:
        try:
            state = await asyncio.to_thread(_fetch_state)
            last_payload = state
            log.info(
                f"真實帳戶狀態：餘額 ${state['balanceUsdc']:.2f}，"
                f"持有部位 {len(state['openPositions'])} 個，成交 {len(state['trades'])} 筆"
            )
        except Exception as e:
            log.error(f"查詢真實帳戶狀態失敗：{e}")
            last_payload = {"connected": False, "error": str(e), "fetchedAt": time.time()}
        await broadcast(json.dumps(last_payload))
        await asyncio.sleep(POLL_INTERVAL)


async def ws_handler(websocket):
    CLIENTS.add(websocket)
    log.info(f"真實損益頁面已連接（共 {len(CLIENTS)} 個客戶端）")
    try:
        await websocket.send(json.dumps(last_payload))
    except Exception:
        pass
    try:
        async for _ in websocket:
            pass  # 純唯讀，不接受任何指令，不會下單
    finally:
        CLIENTS.discard(websocket)
        log.info(f"真實損益頁面已斷線（剩 {len(CLIENTS)} 個客戶端）")


async def main():
    log.info("=" * 50)
    log.info("  Polymarket 真實損益 · 唯讀狀態伺服器")
    log.info(f"  WebSocket: ws://{HOST}:{PORT}")
    log.info("  純唯讀查詢，不會下單、不會動用任何資金")
    log.info("  開啟 web/polymarket_live.html 查看")
    log.info("=" * 50)

    async with serve(ws_handler, HOST, PORT):
        await poll_loop()


if __name__ == "__main__":
    asyncio.run(main())
