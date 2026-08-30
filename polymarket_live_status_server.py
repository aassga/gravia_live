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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket_live_status")

CLIENTS: set = set()
last_payload: dict = {"connected": False, "error": None, "fetchedAt": time.time()}


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


def _fetch_state() -> dict:
    """同步呼叫 py_clob_client_v2（會阻塞），外面用 asyncio.to_thread 包起來跑，不卡住其他連線。"""
    balance_raw = live.get_usdc_balance()
    orders = live.get_open_orders()
    trades = live.get_trade_history(limit=30)

    balance_usdc = int(balance_raw.get("balance", 0)) / 1_000_000
    baseline = _load_or_init_baseline(balance_usdc)
    total_pnl = balance_usdc - baseline["baselineBalance"]

    return {
        "connected": True,
        "error": None,
        "fetchedAt": time.time(),
        "funderAddress": live.FUNDER_ADDRESS,
        "liveTradingEnabled": live.LIVE_TRADING,
        "balanceUsdc": balance_usdc,
        "baselineBalance": baseline["baselineBalance"],
        "baselineSetAt": baseline["baselineSetAt"],
        "totalPnl": total_pnl,
        "openOrders": orders,
        "trades": trades,
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
                f"掛單 {len(state['openOrders'])} 筆，成交 {len(state['trades'])} 筆"
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
