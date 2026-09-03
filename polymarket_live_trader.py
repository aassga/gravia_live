"""
Polymarket 真實下單骨架 —— 獨立於 polymarket_server.py（紙上交易模擬）。

這個檔案不會被 polymarket_server.py 匯入或呼叫，兩者互不影響。
polymarket_server.py 目前跑的模擬交易面板完全不受這個檔案影響。

安全機制：
    - 私鑰只從 .env / 環境變數讀取，絕不寫死在程式碼裡。
    - 預設 LIVE_TRADING=false（dry-run）：所有下單動作只會印出「將會送出的訂單」，
      不會真的呼叫 API。要真的送出真實訂單，必須在 .env 裡明確設 LIVE_TRADING=true。
    - 即使 LIVE_TRADING=true，place_limit_order() 仍可用 dry_run=True 覆蓋，強制預覽不送單。

使用前準備：
    1. pip install py_clob_client_v2 python-dotenv
    2. cp .env.example .env，填入你的 POLY_PRIVATE_KEY / POLY_FUNDER_ADDRESS
    3. 先跑：py polymarket_live_trader.py balance      （檢查連線、查真實餘額，不會下單）
    4. 再跑：py polymarket_live_trader.py preview ...   （預覽訂單簽名結果，仍不送單）
    5. 確認無誤、且 .env 設 LIVE_TRADING=true 後，才會真的送出訂單

注意（2026-08 驗證過）：
    - Polymarket 已於 2026-04-28 把 CLOB 換成 V2 架構，舊版 py-clob-client 套件對正式環境
      已經失效（查什麼都會是空的/0，不是帳號或程式碼的問題）。這裡改用官方新版
      py_clob_client_v2。
    - 新帳號（deposit wallet 架構）要用 signature_type=3（POLY_1271），不是舊版常見的 1/2。
    - 官方新版 SDK 目前有已知未修復的 bug（py-clob-client-v2 repo issue #70）：
      signature_type=3 + 有帶 funder 時，某些簽名流程仍可能用錯地址。balance 查詢已驗證正常，
      但實際下單（place_limit_order）如果遇到簽名/驗證錯誤，很可能就是撞到這個上游 bug，
      不是這支程式寫錯，先回報前確認一下官方 repo 有沒有更新。
"""

from __future__ import annotations

import logging
import os
import sys
import threading

if sys.platform == "win32":
    # Windows 終端機預設編碼常是 cp950/cp936，中文 log 會變亂碼，強制改 UTF-8
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

log = logging.getLogger("polymarket_live")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# httpx（py_clob_client_v2 底層用的 HTTP 函式庫）預設會把每一次請求都印成 INFO log，
# 背景餘額刷新任務固定每 8 秒打一次 API，這樣洗下去 log 會被灌爆。調高門檻只是
# 少印這些「已發出請求」的紀錄，不影響我們自己的 log、也不影響任何下單邏輯。
logging.getLogger("httpx").setLevel(logging.WARNING)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    log.warning("未安裝 python-dotenv，將只讀取系統環境變數（不會自動載入 .env 檔）")

# ── 設定（全部從環境變數讀取，不寫死任何機密資訊）───────────────────────────

PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "")
FUNDER_ADDRESS = os.environ.get("POLY_FUNDER_ADDRESS", "") or None
CLOB_HOST = os.environ.get("POLY_CLOB_HOST", "https://clob.polymarket.com")
CHAIN_ID = int(os.environ.get("POLY_CHAIN_ID", "137"))
LIVE_TRADING = os.environ.get("LIVE_TRADING", "false").strip().lower() == "true"

# 0=EOA 直接持有資金, 1=POLY_PROXY（舊版 Email/Magic 帳號）, 2=POLY_GNOSIS_SAFE（連接外部錢包帳號）,
# 3=POLY_1271（V2 新式 deposit wallet，2026-04 後新建的帳號多半是這個，已實測驗證正確）
SIGNATURE_TYPE = int(os.environ.get("POLY_SIGNATURE_TYPE", "3"))

_client = None  # lazy-initialized singleton
_client_lock = threading.Lock()  # get_client() 會透過 asyncio.to_thread 從不同背景執行緒呼叫
                                  # （策略主迴圈 vs 背景餘額刷新任務都可能同時是第一個呼叫者），
                                  # 用鎖避免兩邊都通過「還沒初始化」的檢查、各自重複建立一次 client。


def _require_private_key() -> None:
    if not PRIVATE_KEY or PRIVATE_KEY.startswith("0xyour_"):
        raise RuntimeError(
            "尚未設定 POLY_PRIVATE_KEY。請複製 .env.example 為 .env 並填入你的私鑰，"
            "或直接設定同名環境變數。絕對不要把私鑰寫進程式碼或提交到 git。"
        )


def get_client():
    """建立（並快取）已驗證的 ClobClient。只在真的需要打 API 時才會呼叫。"""
    global _client
    if _client is not None:
        return _client

    with _client_lock:
        if _client is not None:  # 等鎖的期間可能已經被另一個執行緒建立好了
            return _client

        _require_private_key()

        from py_clob_client_v2.client import ClobClient

        log.info(f"連線到 {CLOB_HOST}（chain_id={CHAIN_ID}, signature_type={SIGNATURE_TYPE}）...")

        if FUNDER_ADDRESS:
            # 資金在另一個錢包（proxy / deposit wallet），這把私鑰只負責簽名
            client = ClobClient(
                CLOB_HOST,
                key=PRIVATE_KEY,
                chain_id=CHAIN_ID,
                signature_type=SIGNATURE_TYPE,
                funder=FUNDER_ADDRESS,
            )
        else:
            # 私鑰本身就是持有資金的錢包
            client = ClobClient(CLOB_HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID)

        creds = client.create_or_derive_api_key()
        client.set_api_creds(creds)
        log.info("API 憑證取得成功")

        _client = client
        return _client


def get_usdc_balance() -> dict:
    """查詢真實 USDC 餘額與 allowance（唯讀，不影響任何部位，可安心先跑這個測連線）。"""
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    client = get_client()
    return client.get_balance_allowance(
        params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )


def get_conditional_balance(token_id: str) -> float:
    """查詢單一 outcome token 的真實持有股數（唯讀）。"""
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

    raw = get_client().get_balance_allowance(
        params=BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
    )
    return int(raw.get("balance", 0)) / 1_000_000


def get_open_orders() -> list:
    """查詢目前掛在真實帳號上的未成交訂單（唯讀）。"""
    client = get_client()
    return client.get_open_orders()


def get_order(order_id: str) -> dict:
    """查詢單筆訂單狀態（唯讀），用來追蹤 delayed 回覆是否真正成交。"""
    return get_client().get_order(order_id)


def get_trade_history(limit: int = 50) -> list:
    """查詢真實成交紀錄（唯讀），依時間新到舊排序，最多回傳 limit 筆。"""
    client = get_client()
    trades = client.get_trades(only_first_page=True)
    trades.sort(key=lambda t: t.get("match_time", t.get("timestamp", 0)), reverse=True)
    return trades[:limit]


def summarize_order_fills(order_response: dict, trades: list) -> dict | None:
    """從近期成交紀錄還原某筆訂單的實際成交均價；找不到時回傳 None。"""
    if not isinstance(order_response, dict):
        return None
    order_id = str(order_response.get("orderID") or order_response.get("orderId") or order_response.get("id") or "")
    trade_ids = {
        str(value)
        for value in (order_response.get("tradeIDs") or order_response.get("associate_trades") or [])
        if value
    }
    matched = []
    for trade in trades or []:
        if not isinstance(trade, dict):
            continue
        trade_id = str(trade.get("id") or trade.get("trade_id") or "")
        taker_order_id = str(trade.get("taker_order_id") or trade.get("takerOrderId") or "")
        maker_order_ids = {
            str(item.get("order_id") or item.get("orderId") or "")
            for item in (trade.get("maker_orders") or trade.get("makerOrders") or [])
            if isinstance(item, dict)
        }
        if (trade_ids and trade_id in trade_ids) or (order_id and (taker_order_id == order_id or order_id in maker_order_ids)):
            matched.append(trade)
    if not matched:
        return None

    shares = 0.0
    notional = 0.0
    reported_fee = 0.0
    has_reported_fee = False
    for trade in matched:
        try:
            size = float(trade.get("size", trade.get("matched_amount", 0)) or 0)
            price = float(trade.get("price", 0) or 0)
        except (TypeError, ValueError):
            continue
        if size <= 0 or not 0 < price < 1:
            continue
        shares += size
        notional += size * price
        for key in ("feeUsdc", "fee_usdc", "fee"):
            if trade.get(key) not in (None, ""):
                try:
                    reported_fee += float(trade[key])
                    has_reported_fee = True
                except (TypeError, ValueError):
                    pass
                break
    if shares <= 0:
        return None
    return {
        "shares": shares,
        "price": notional / shares,
        "notional": notional,
        "fee": reported_fee if has_reported_fee else None,
        "tradeIds": [str(t.get("id") or t.get("trade_id") or "") for t in matched],
    }


def get_order_fill_summary(order_response: dict, limit: int = 100) -> dict | None:
    """查詢近期真實成交並回傳指定 FOK 訂單的加權平均成交資料。"""
    return summarize_order_fills(order_response, get_trade_history(limit))


def build_order(token_id: str, side: str, price: float, size: float):
    """建立並簽名一筆訂單物件，但不送出。可用來預覽/檢查簽名是否成功。"""
    from py_clob_client_v2.clob_types import OrderArgsV2
    from py_clob_client_v2.order_builder.constants import BUY, SELL

    side_const = BUY if side.upper() == "BUY" else SELL
    client = get_client()
    order_args = OrderArgsV2(
        token_id=token_id,
        price=price,
        size=size,
        side=side_const,
    )
    return client.create_order(order_args)


def place_limit_order(
    token_id: str,
    side: str,
    price: float,
    size: float,
    dry_run: bool | None = None,
    order_type: str = "GTC",
    validate_signature: bool = True,
) -> dict:
    """
    下一筆限價單。

    order_type:
        "GTC" -> 一直掛著直到成交或取消（預設）
        "FOK" -> 全部成交或直接取消，不會有部分成交卡著的殘留掛單（自動策略建議用這個）

    dry_run:
        None  -> 依 .env 的 LIVE_TRADING 決定（預設 False，即預設 dry-run）
        True  -> 強制只預覽，不送單，即使 LIVE_TRADING=true
        False -> 強制送單（仍要求 LIVE_TRADING=true 才會真的執行，否則拋錯，避免誤觸）
    """
    effective_dry_run = (not LIVE_TRADING) if dry_run is None else dry_run

    if effective_dry_run:
        # CLI preview 預設仍會建立並簽署訂單；自動策略的 dry-run 可關閉此驗證，
        # 方便在沒有私鑰的環境完整測試策略，且絕不會呼叫下單 API。
        if validate_signature:
            build_order(token_id, side, price, size)
        log.info(
            f"[DRY-RUN] 不會送出 —— {side.upper()} token={token_id} "
            f"price={price} size={size} order_type={order_type}（LIVE_TRADING={LIVE_TRADING}）"
        )
        return {
            "dry_run": True,
            "success": True,
            "status": "matched",
            "would_submit": {"token_id": token_id, "side": side, "price": price, "size": size, "order_type": order_type},
        }

    if not LIVE_TRADING:
        raise RuntimeError(
            "dry_run=False 但 .env 的 LIVE_TRADING 不是 true。"
            "請先確認你真的要送出真實訂單，再把 .env 的 LIVE_TRADING 改成 true。"
        )

    from py_clob_client_v2.clob_types import OrderType

    client = get_client()
    signed_order = build_order(token_id, side, price, size)
    log.warning(f"[LIVE] 送出真實訂單 —— {side.upper()} token={token_id} price={price} size={size} order_type={order_type}")
    resp = client.post_order(signed_order, getattr(OrderType, order_type))
    log.warning(f"[LIVE] 訂單回應：{resp}")
    return resp


def order_response_filled(response: dict) -> bool:
    """只有明確 matched 才視為成交。

    success=true 只代表 CLOB 接受請求；delayed/live/unmatched 都不能當成已成交。
    FOK 在 matched 狀態下才能安全地建立完整持倉記錄。
    """
    if not isinstance(response, dict):
        return False
    if response.get("dry_run"):
        return True
    status = str(response.get("status", "")).lower()
    # GET /data/order/{id} 使用 ORDER_STATUS_MATCHED，且不包含 success 欄位。
    if status == "order_status_matched":
        return bool(response.get("id") or response.get("orderID") or response.get("orderId"))
    accepted = response.get("success")
    if accepted is None:
        accepted = response.get("ok", False)
    return bool(accepted) and status == "matched"


def cancel_order(order_id: str) -> dict:
    if not LIVE_TRADING:
        raise RuntimeError("LIVE_TRADING 未開啟，無法取消真實訂單（因為根本不會有真實訂單存在）。")
    client = get_client()
    return client.cancel_orders([order_id])


# ── CLI，方便手動測試每個步驟 ───────────────────────────────────────────────

def _cli():
    if len(sys.argv) < 2:
        print("用法：")
        print("  py polymarket_live_trader.py balance                       # 查真實餘額（唯讀，安全）")
        print("  py polymarket_live_trader.py orders                        # 查真實未成交訂單（唯讀，安全）")
        print("  py polymarket_live_trader.py preview <token_id> <side> <price> <size>  # 預覽建單+簽名，不送出")
        return

    cmd = sys.argv[1]

    if cmd == "balance":
        print(get_usdc_balance())
    elif cmd == "orders":
        print(get_open_orders())
    elif cmd == "preview":
        _, _, token_id, side, price, size = sys.argv
        result = place_limit_order(token_id, side, float(price), float(size), dry_run=True)
        print(result)
    else:
        print(f"未知指令：{cmd}")


if __name__ == "__main__":
    _cli()
