"""
Polymarket BTC 5 分鐘 Up/Down · 真實自動下單策略

真實版與紙上模擬共用同一套核心判斷：
    - 使用 Ask/Bid 深度計算 VWAP，再以含滑點、向不利 tick 取整的最差限價作決策。
    - 進場必須通過公平價優勢、價格門檻與剩餘時間條件。
    - 第二腿必須在 taker fee 後仍達到最低淨鎖利才下單。
    - 單邊曝險可在市場 Bid 顯著高於模型持有價值時提早退出。

真實執行額外保護：
    - 只有 LIVE_TRADING=true 與 POLY_STRATEGY_ARMED=true 同時成立才送真實訂單。
      其餘情況是 dry-run，不會簽名或送出訂單。
    - 下單使用 FOK；只有 API 明確回覆 matched 才記錄為已成交，並盡量回填真實成交均價。
    - delayed 訂單會短暫追蹤；若仍無法確認，策略自動停止後續下單。
    - 每組完整兩腿共用一份資金預算，另有單組絕對金額上限與現金保留額。
    - 狀態會寫入 polymarket_live_strategy_state.json，重啟不會忘記未結算曝險。

本程式會動用真實資金。啟用 LIVE_TRADING=true 前，請先以 dry-run 跑完整窗口。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time

import aiohttp

import polymarket_live_trader as live
import polymarket_server as sim

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket_live_strategy")

POLL_INTERVAL = 3
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_live_strategy_state.json")

STAKE_PCT = max(0.5, min(30.0, float(os.environ.get("POLY_STAKE_PCT", "15.0"))))
STRATEGY_ARMED = os.environ.get("POLY_STRATEGY_ARMED", "false").strip().lower() == "true"
REAL_EXECUTION_ENABLED = live.LIVE_TRADING and STRATEGY_ARMED
MAX_PAIR_BUDGET_USD = max(1.0, float(os.environ.get("POLY_MAX_PAIR_BUDGET_USD", "25.0")))
MIN_CASH_RESERVE_USD = max(0.0, float(os.environ.get("POLY_MIN_CASH_RESERVE_USD", "5.0")))
DRY_RUN_BALANCE_USD = max(1.0, float(os.environ.get("POLY_DRY_RUN_BALANCE_USD", "100.0")))
ACTION_COOLDOWN_SECONDS = max(1.0, float(os.environ.get("POLY_ACTION_COOLDOWN_SECONDS", "10.0")))
ORDER_CONFIRM_ATTEMPTS = max(1, int(os.environ.get("POLY_ORDER_CONFIRM_ATTEMPTS", "6")))
ORDER_CONFIRM_INTERVAL = max(0.5, float(os.environ.get("POLY_ORDER_CONFIRM_INTERVAL", "1.0")))

# 真實版套用模擬版 A/B 測試裡的「BTC 目前」這組門檻（sim.AB_VARIANT_BY_ID["main"]：0.40/0.90，
# 底層就是 sim.SIM_ENTRY_MAX_PRICE/SIM_LOCK_MAX_SUM）。這裡引用 AB_VARIANT_BY_ID 而不是
# 直接寫死數字，是為了跟模擬版共用同一個真實來源，模擬版調整這組門檻時真實版會自動跟著同步。
_LIVE_VARIANT = sim.AB_VARIANT_BY_ID["main"]
ENTRY_MAX_PRICE = _LIVE_VARIANT["entryMaxPrice"]
LOCK_MAX_SUM    = _LIVE_VARIANT["lockMaxSum"]


def _new_live_state() -> dict:
    return {
        "position": None,
        "pendingSettlements": [],
        "trades": [],
        "totalPnlEstimate": 0.0,
        "totalFeesEstimate": 0.0,
        "totalTrades": 0,
        "lockedTrades": 0,
        "directionalTrades": 0,
        "earlyExits": 0,
        "lastActionAt": 0.0,
        "halted": False,
        "haltReason": None,
        "unconfirmedOrder": None,
        "preflightSlug": None,
        "updatedAt": time.time(),
    }


def _load_live_state() -> dict:
    defaults = _new_live_state()
    if not os.path.exists(STATE_FILE):
        return defaults
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        defaults.update(loaded)
    except Exception as exc:
        log.error(f"[LIVE] 無法讀取策略狀態，為避免遺忘真實曝險將停止下單：{exc}")
        defaults["halted"] = True
        defaults["haltReason"] = f"state_load_failed: {exc}"
    return defaults


live_state = _load_live_state()


def save_live_state() -> None:
    live_state["updatedAt"] = time.time()
    tmp_path = STATE_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(live_state, f, ensure_ascii=False, indent=2)
    # 這個檔案在 OneDrive 同步的資料夾裡，OneDrive 偶爾會在同步當下短暫鎖住檔案，
    # 讓 os.replace 原子改名瞬間失敗（WinError 5）。重試幾次、每次等一下下就好，
    # 不是真的權限問題，鎖通常幾十毫秒內就會放開。
    for attempt in range(5):
        try:
            os.replace(tmp_path, STATE_FILE)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.1 * (attempt + 1))


def reset_live_state_for_tests() -> None:
    """只供單元測試在記憶體中清狀態；不會刪除實際狀態檔。"""
    live_state.clear()
    live_state.update(_new_live_state())


def _set_halt(reason: str, order: dict | None = None) -> None:
    live_state["halted"] = True
    live_state["haltReason"] = reason
    live_state["unconfirmedOrder"] = order
    save_live_state()
    log.critical(f"[LIVE] 策略已自動停止下單：{reason}")


def _position_paid_cost(pos: dict) -> float:
    cost = float(pos.get("entryNotional", pos.get("entryRiskNotional", 0))) + float(pos.get("entryFee", 0))
    if pos.get("hedged"):
        cost += float(pos.get("hedgeNotional", pos.get("hedgeRiskNotional", 0)))
        cost += float(pos.get("hedgeFee", 0))
    return cost


def _position_risk_cost(pos: dict) -> float:
    """最差限價成本只用於下單判斷；實際損益另由 _position_paid_cost 計算。"""
    cost = float(pos.get("entryRiskNotional", pos.get("entryNotional", 0)))
    cost += float(pos.get("entryRiskFee", pos.get("entryFee", 0)))
    if pos.get("hedged"):
        cost += float(pos.get("hedgeRiskNotional", pos.get("hedgeNotional", 0)))
        cost += float(pos.get("hedgeRiskFee", pos.get("hedgeFee", 0)))
    return cost


def _settle_pnl_estimate(pos: dict, outcome: str) -> float:
    payout = float(pos["shares"]) if pos.get("hedged") or outcome == pos["side"] else 0.0
    return payout - _position_paid_cost(pos)


def _record_trade(pos: dict, pnl: float, outcome: str, trade_type: str) -> None:
    fees = float(pos.get("entryFee", 0)) + float(pos.get("hedgeFee", 0)) + float(pos.get("exitFee", 0))
    trade = {
        "windowSlug": pos["windowSlug"],
        "side": pos["side"],
        "shares": pos["shares"],
        "entryPrice": pos["entryPrice"],
        "entryObservedVwap": pos.get("entryObservedVwap"),
        "entryLimitPrice": pos.get("entryLimitPrice"),
        "entryPriceSource": pos.get("entryPriceSource"),
        "hedged": pos.get("hedged", False),
        "hedgeSide": pos.get("hedgeSide"),
        "hedgePrice": pos.get("hedgePrice"),
        "hedgeObservedVwap": pos.get("hedgeObservedVwap"),
        "hedgeLimitPrice": pos.get("hedgeLimitPrice"),
        "hedgePriceSource": pos.get("hedgePriceSource"),
        "exitPrice": pos.get("exitPrice"),
        "exitObservedVwap": pos.get("exitObservedVwap"),
        "exitLimitPrice": pos.get("exitLimitPrice"),
        "exitPriceSource": pos.get("exitPriceSource"),
        "entryEdge": pos.get("entryEdge"),
        "feesEstimate": fees,
        "pnlEstimate": pnl,
        "outcome": outcome,
        "tradeType": trade_type,
        "dryRun": pos.get("dryRun", True),
        "entryTime": pos.get("entryTime"),
        "exitTime": time.time(),
    }
    live_state["trades"].insert(0, trade)
    live_state["trades"] = live_state["trades"][:100]
    live_state["totalPnlEstimate"] += pnl
    live_state["totalFeesEstimate"] += fees
    live_state["totalTrades"] += 1
    if trade_type == "locked":
        live_state["lockedTrades"] += 1
    elif trade_type == "early_exit":
        live_state["earlyExits"] += 1
    else:
        live_state["directionalTrades"] += 1
    save_live_state()


def marketable_limit_price(book: dict, fill: dict, side: str) -> float:
    """向後相容入口；模擬與實盤實際共用 polymarket_server 的同一函式。"""
    return sim.marketable_limit_price(book, fill, side)


def _risk_fill(book: dict, fill: dict, side: str) -> dict:
    decision = sim.decision_fill(book, fill, side)
    shares = float(fill["shares"])
    return {
        "shares": shares,
        "observedVwap": decision["observedVwap"],
        "limitPrice": decision["decisionPrice"],
        "riskNotional": decision["decisionNotional"],
        "fee": decision["decisionFee"],
    }


async def _resolved_execution(plan: dict, response: dict, dry_run: bool) -> dict:
    """取得成交後的記帳價格：dry-run 用模擬 VWAP，實盤優先用成交紀錄，否則用保守限價。"""
    source = "simulated_vwap" if dry_run else "conservative_limit"
    price = float(plan["observedVwap"] if dry_run else plan["limitPrice"])
    notional = float(plan["shares"]) * price
    fee = sim.taker_fee(float(plan["shares"]), price)

    if not dry_run:
        try:
            summary = await asyncio.to_thread(live.get_order_fill_summary, response)
        except Exception as exc:
            log.warning(f"[LIVE] 無法取得實際成交均價，暫以保守限價記帳：{exc}")
            summary = None
        if summary and abs(float(summary["shares"]) - float(plan["shares"])) <= 1e-6:
            price = float(summary["price"])
            notional = float(summary["notional"])
            fee = summary.get("fee")
            fee = sim.taker_fee(float(plan["shares"]), price) if fee is None else float(fee)
            source = "matched_trades"

    return {"price": price, "notional": notional, "fee": fee, "source": source}


def _ask_depth(book: dict) -> float:
    """訂單簿目前看得到的賣單總深度，用來把想要的股數縮到真的吃得到的量，
    避免算出來的股數超過深度、FOK/FAK 整筆判定未成交，白白錯過機會。"""
    return sum(float(a.get("size", 0)) for a in (book.get("asks") or []))


def _buy_plan(side: str, book: dict, shares: float, fair_probability: float | None = None) -> dict | None:
    fill = sim.simulate_buy_fill(book, shares)
    if not fill:
        return None
    risk = _risk_fill(book, fill, "BUY")
    if shares < float(book.get("minOrderSize", 1) or 1) or risk["riskNotional"] < 1.0:
        return None
    all_in_per_share = (risk["riskNotional"] + risk["fee"]) / shares
    edge = None if fair_probability is None else fair_probability - all_in_per_share
    return {"side": side, "book": book, "fair": fair_probability, "edge": edge, **risk}


def _sell_plan(side: str, book: dict, shares: float) -> dict | None:
    fill = sim.simulate_sell_fill(book, shares)
    if not fill:
        return None
    risk = _risk_fill(book, fill, "SELL")
    if shares < float(book.get("minOrderSize", 1) or 1) or risk["riskNotional"] < 1.0:
        return None
    return {"side": side, "book": book, **risk}


def _entry_candidate(side: str, book: dict, shares: float, fair_probability: float) -> dict | None:
    plan = _buy_plan(side, book, shares, fair_probability)
    if not plan or plan["limitPrice"] > ENTRY_MAX_PRICE:
        return None
    if plan["edge"] is None or plan["edge"] < sim.SIM_MIN_ENTRY_EDGE:
        return None
    return plan


def _target_pair_order(cash: float) -> tuple[float, float]:
    """跟模擬版共用同一個計算函式（sim.target_pair_order），只是帶入真實版自己的
    下注比例／資金上限／保留額——公式本身跟模擬版保證一致，不會各寫一份長歪。"""
    return sim.target_pair_order(cash, STAKE_PCT, LOCK_MAX_SUM, MAX_PAIR_BUDGET_USD, MIN_CASH_RESERVE_USD)


def _direct_pair_plans(up_book: dict, down_book: dict, shares: float, cash: float) -> tuple[dict, dict] | None:
    up = _buy_plan("Up", up_book, shares)
    down = _buy_plan("Down", down_book, shares)
    if not up or not down:
        return None
    total_cost = up["riskNotional"] + up["fee"] + down["riskNotional"] + down["fee"]
    net_per_share = (shares - total_cost) / shares
    if up["limitPrice"] + down["limitPrice"] > LOCK_MAX_SUM:
        return None
    if net_per_share < sim.SIM_MIN_NET_LOCK_PER_SHARE or total_cost > cash - MIN_CASH_RESERVE_USD:
        return None
    return up, down


def _get_real_cash() -> float:
    raw = live.get_usdc_balance()
    return int(raw.get("balance", 0)) / 1_000_000


# 每輪迴圈（約 3 秒一次）都查一次真實餘額太頻繁——閒置時沒有任何候選也照查不誤，
# 洗一堆重複 log。餘額只會因為我們自己下單、或部位結算才會變，兩者都受 10 秒的
# ACTION_COOLDOWN_SECONDS 限制，所以快取 8 秒（小於冷卻時間）不會影響下單決策的正確性。
CASH_CACHE_TTL_SECONDS = 8.0
_cash_cache: dict = {"value": None, "at": 0.0}


def _invalidate_cash_cache() -> None:
    _cash_cache["at"] = 0.0


async def _strategy_cash(dry_run: bool) -> float:
    if dry_run:
        return DRY_RUN_BALANCE_USD
    now = time.time()
    if _cash_cache["value"] is not None and now - _cash_cache["at"] < CASH_CACHE_TTL_SECONDS:
        return _cash_cache["value"]
    value = await asyncio.to_thread(_get_real_cash)
    _cash_cache["value"] = value
    _cash_cache["at"] = now
    return value


async def _ensure_no_unmanaged_current_position() -> bool:
    """真實模式的首筆下單前，確認當前兩個 token 都沒有策略狀態之外的持倉。"""
    up_id, down_id = sim._market_tokens(sim.state["market"])
    balances = await asyncio.gather(
        asyncio.to_thread(live.get_conditional_balance, up_id),
        asyncio.to_thread(live.get_conditional_balance, down_id),
        return_exceptions=True,
    )
    if any(isinstance(balance, Exception) for balance in balances):
        details = ", ".join(str(balance) for balance in balances if isinstance(balance, Exception))
        _set_halt(f"preflight_position_check_failed: {details}")
        return False
    if float(balances[0]) >= 0.01 or float(balances[1]) >= 0.01:
        _set_halt(
            f"unmanaged_current_market_position up={float(balances[0]):.6f} down={float(balances[1]):.6f}"
        )
        return False
    return True


async def _submit_fok(token_id: str, side: str, plan: dict, dry_run: bool) -> tuple[str, dict]:
    live_state["lastActionAt"] = time.time()
    save_live_state()
    if not dry_run:
        _invalidate_cash_cache()
    from py_clob_client_v2.exceptions import PolyApiException

    try:
        response = await asyncio.to_thread(
            live.place_limit_order,
            token_id,
            side,
            plan["limitPrice"],
            plan["shares"],
            dry_run,
            "FOK",
            not dry_run,
        )
    except PolyApiException as exc:
        # FOK 沒吃滿（訂單簿在下單瞬間跟決策當下的快照之間變薄了）是正常會發生的情況，
        # 不是程式錯誤——CLOB 直接回 400 而不是回一個帶 status 的訂單物件，用例外表達。
        # 當成跟 status=unmatched 一樣的「這次沒成交」處理，不要整包當未預期例外往外拋。
        log.info(f"[LIVE] FOK 未成交（下單瞬間深度不夠）：{exc}")
        return "not_filled", {"error": str(exc)}
    if live.order_response_filled(response):
        return "filled", response

    status = str(response.get("status", "")).lower() if isinstance(response, dict) else ""
    if status in {"unmatched", "cancelled", "canceled", "rejected", ""}:
        return "not_filled", response
    if status != "delayed":
        return "unconfirmed", response

    order_id = response.get("orderID") or response.get("orderId")
    if not order_id:
        return "unconfirmed", response
    latest = response
    for _ in range(ORDER_CONFIRM_ATTEMPTS):
        await asyncio.sleep(ORDER_CONFIRM_INTERVAL)
        try:
            latest = await asyncio.to_thread(live.get_order, order_id)
        except Exception as exc:
            log.warning(f"[LIVE] 追蹤 delayed 訂單 {order_id} 失敗：{exc}")
            continue
        if live.order_response_filled(latest):
            return "filled", latest
        latest_status = str(latest.get("status", "")).lower() if isinstance(latest, dict) else ""
        if latest_status in {"unmatched", "cancelled", "canceled", "rejected"}:
            return "not_filled", latest
    return "unconfirmed", latest


def _token_id(side: str) -> str:
    up_id, down_id = sim._market_tokens(sim.state["market"])
    return up_id if side == "Up" else down_id


async def _enter_position(slug: str, plan: dict, dry_run: bool) -> str:
    token_id = _token_id(plan["side"])
    result, response = await _submit_fok(token_id, "BUY", plan, dry_run)
    if result == "unconfirmed":
        _set_halt(
            f"entry_order_unconfirmed side={plan['side']}",
            {"orderID": response.get("orderID") or response.get("orderId"), "side": plan["side"], "tokenId": token_id},
        )
        return result
    if result != "filled":
        log.info(f"[LIVE] {plan['side']} FOK 未成交，不建立持倉")
        return result

    execution = await _resolved_execution(plan, response, dry_run)

    live_state["position"] = {
        "windowSlug": slug,
        "side": plan["side"],
        "tokenId": token_id,
        "shares": plan["shares"],
        "stakeUsd": execution["notional"] + execution["fee"],
        "entryPrice": execution["price"],
        "entryObservedVwap": plan["observedVwap"],
        "entryLimitPrice": plan["limitPrice"],
        "entryPriceSource": execution["source"],
        "entryNotional": execution["notional"],
        "entryRiskNotional": plan["riskNotional"],
        "entryFee": execution["fee"],
        "entryRiskFee": plan["fee"],
        "fairProbability": plan.get("fair"),
        "entryEdge": plan.get("edge"),
        "entryTime": time.time(),
        "entryOrderId": response.get("orderID") or response.get("orderId"),
        "hedged": False,
        "hedgeSide": None,
        "hedgePrice": None,
        "hedgeObservedVwap": None,
        "hedgeLimitPrice": None,
        "hedgePriceSource": None,
        "hedgeNotional": 0.0,
        "hedgeRiskNotional": 0.0,
        "hedgeFee": 0.0,
        "hedgeRiskFee": 0.0,
        "dryRun": dry_run,
    }
    save_live_state()
    tag = "DRY-RUN" if dry_run else "REAL"
    log.warning(
        f"[LIVE][{tag}] 進場 {plan['side']} limit=${plan['limitPrice']:.3f} "
        f"shares={plan['shares']:.2f} edge={plan.get('edge') if plan.get('edge') is not None else float('nan'):+.4f}"
    )
    return result


async def _hedge_position(plan: dict, dry_run: bool) -> str:
    pos = live_state["position"]
    token_id = _token_id(plan["side"])
    result, response = await _submit_fok(token_id, "BUY", plan, dry_run)
    if result == "unconfirmed":
        _set_halt(
            f"hedge_order_unconfirmed side={plan['side']}",
            {"orderID": response.get("orderID") or response.get("orderId"), "side": plan["side"], "tokenId": token_id},
        )
        return result
    if result != "filled":
        log.error(f"[LIVE] 第二腿 {plan['side']} FOK 未成交，依然是單邊曝險")
        return result

    execution = await _resolved_execution(plan, response, dry_run)

    pos["hedged"] = True
    pos["hedgeSide"] = plan["side"]
    pos["hedgeTokenId"] = token_id
    pos["hedgePrice"] = execution["price"]
    pos["hedgeObservedVwap"] = plan["observedVwap"]
    pos["hedgeLimitPrice"] = plan["limitPrice"]
    pos["hedgePriceSource"] = execution["source"]
    pos["hedgeNotional"] = execution["notional"]
    pos["hedgeRiskNotional"] = plan["riskNotional"]
    pos["hedgeFee"] = execution["fee"]
    pos["hedgeRiskFee"] = plan["fee"]
    pos["hedgeOrderId"] = response.get("orderID") or response.get("orderId")
    pos["stakeUsd"] = _position_paid_cost(pos)
    pos["lockedPnlEstimate"] = pos["shares"] - _position_paid_cost(pos)
    pos["lockedPnlWorstCase"] = pos["shares"] - _position_risk_cost(pos)
    save_live_state()
    tag = "DRY-RUN" if dry_run else "REAL"
    log.warning(
        f"[LIVE][{tag}] 第二腿 {plan['side']} limit=${plan['limitPrice']:.3f} "
        f"保守淨鎖利估計=${pos['lockedPnlEstimate']:+.2f}"
    )
    return result


async def _close_position(plan: dict, dry_run: bool, reason: str) -> str:
    pos = live_state["position"]
    result, response = await _submit_fok(pos["tokenId"], "SELL", plan, dry_run)
    if result == "unconfirmed":
        _set_halt(
            f"exit_order_unconfirmed reason={reason}",
            {"orderID": response.get("orderID") or response.get("orderId"), "side": "SELL", "tokenId": pos["tokenId"]},
        )
        return result
    if result != "filled":
        log.error(f"[LIVE] 退出 FOK 未成交，持倉保留：reason={reason}")
        return result

    execution = await _resolved_execution(plan, response, dry_run)

    pos["exitPrice"] = execution["price"]
    pos["exitObservedVwap"] = plan["observedVwap"]
    pos["exitLimitPrice"] = plan["limitPrice"]
    pos["exitPriceSource"] = execution["source"]
    pos["exitFee"] = execution["fee"]
    pos["exitReason"] = reason
    net_proceeds = execution["notional"] - execution["fee"]
    pnl = net_proceeds - _position_paid_cost(pos)
    live_state["position"] = None
    _record_trade(pos, pnl, "EarlyExit", "early_exit")
    tag = "DRY-RUN" if dry_run else "REAL"
    log.warning(f"[LIVE][{tag}] 提早退出 {pos['side']} 保守淨損益=${pnl:+.2f} reason={reason}")
    return result


async def _emergency_unwind(session: aiohttp.ClientSession, reason: str) -> None:
    pos = live_state.get("position")
    if not pos or pos.get("hedged"):
        return
    try:
        latest_book = await sim.fetch_book(session, pos["tokenId"])
    except Exception as exc:
        log.error(f"[LIVE] 緊急退出前無法取得訂單簿：{exc}")
        return
    plan = _sell_plan(pos["side"], latest_book, pos["shares"])
    if not plan:
        log.error("[LIVE] 第二腿失敗，且第一腿目前沒有足夠 Bid 可緊急退出")
        return
    await _close_position(plan, bool(pos.get("dryRun", True)), reason)


async def _execute_direct_pair(
    session: aiohttp.ClientSession,
    slug: str,
    up: dict,
    down: dict,
    fair: dict | None,
    dry_run: bool,
) -> None:
    # 先買即使第二腿失敗仍較有模型優勢的一邊；真實 CLOB 並沒有兩腿原子成交保證。
    plans = [up, down]
    if fair:
        for plan in plans:
            fair_side = fair["fairUp"] if plan["side"] == "Up" else fair["fairDown"]
            plan["fair"] = fair_side
            plan["edge"] = fair_side - (plan["riskNotional"] + plan["fee"]) / plan["shares"]
        plans.sort(key=lambda p: p.get("edge", float("-inf")), reverse=True)

    first, second = plans
    if await _enter_position(slug, first, dry_run) != "filled":
        return
    result = await _hedge_position(second, dry_run)
    if result == "not_filled":
        await _emergency_unwind(session, "direct_pair_second_leg_failed")


async def retry_pending_settlements(session: aiohttp.ClientSession) -> None:
    if not live_state["pendingSettlements"]:
        return
    still_pending = []
    for pos in live_state["pendingSettlements"]:
        outcome = await sim.fetch_outcome(session, pos["windowSlug"])
        if outcome is None:
            still_pending.append(pos)
            continue
        pnl = _settle_pnl_estimate(pos, outcome)
        trade_type = "locked" if pos.get("hedged") else "directional"
        _record_trade(pos, pnl, outcome, trade_type)
        tag = "DRY-RUN" if pos.get("dryRun", True) else "REAL"
        log.warning(
            f"[LIVE][{tag}] 結算 {pos['windowSlug']} outcome={outcome} "
            f"type={trade_type} 保守淨損益估計=${pnl:+.2f}"
        )
    live_state["pendingSettlements"] = still_pending
    save_live_state()


def queue_settlement(slug: str) -> None:
    pos = live_state.get("position")
    if pos is not None and pos.get("windowSlug") == slug:
        live_state["pendingSettlements"].append(pos)
        live_state["position"] = None
        save_live_state()


async def evaluate_and_act(
    slug: str,
    session: aiohttp.ClientSession,
    remaining_seconds: float | None,
    fair: dict | None,
) -> None:
    if live_state.get("halted"):
        return
    if time.time() - float(live_state.get("lastActionAt", 0)) < ACTION_COOLDOWN_SECONDS:
        return

    up_book, down_book = sim.state["upBook"], sim.state["downBook"]
    pos = live_state.get("position")

    if pos is None:
        if remaining_seconds is None or remaining_seconds < sim.SIM_MIN_ENTRY_REMAINING:
            return
        dry_run = not REAL_EXECUTION_ENABLED
        if not dry_run and live_state.get("preflightSlug") != slug:
            if not await _ensure_no_unmanaged_current_position():
                return
            live_state["preflightSlug"] = slug
            save_live_state()
        cash = await _strategy_cash(dry_run)
        shares, budget = _target_pair_order(cash)
        if budget < 1.0 or shares < 1.0:
            return

        # 配對鎖利兩腿股數要一致，用兩邊深度較小的那個縮限；單邊方向性進場則各自
        # 用自己那邊的深度縮限。縮到比想要的股數少，好過整筆因深度不夠而判定未成交。
        paired_shares = min(shares, _ask_depth(up_book), _ask_depth(down_book))
        direct = _direct_pair_plans(up_book, down_book, paired_shares, cash) if paired_shares >= 1.0 else None
        if direct:
            await _execute_direct_pair(session, slug, direct[0], direct[1], fair, dry_run)
            return
        if not fair:
            return
        up_shares = min(shares, _ask_depth(up_book))
        down_shares = min(shares, _ask_depth(down_book))
        candidates = [
            _entry_candidate("Up", up_book, up_shares, fair["fairUp"]) if up_shares >= 1.0 else None,
            _entry_candidate("Down", down_book, down_shares, fair["fairDown"]) if down_shares >= 1.0 else None,
        ]
        candidates = [candidate for candidate in candidates if candidate]
        if candidates:
            await _enter_position(slug, max(candidates, key=lambda x: x["edge"]), dry_run)
        return

    if pos.get("hedged") or pos.get("windowSlug") != slug:
        return
    if not pos.get("dryRun", True) and not REAL_EXECUTION_ENABLED:
        log.error("[LIVE] 存在真實持倉，但真實策略未完整武裝；本程式不會假裝已對沖")
        return

    dry_run = bool(pos.get("dryRun", True))
    other_side = "Down" if pos["side"] == "Up" else "Up"
    other_book = down_book if other_side == "Down" else up_book
    hedge = _buy_plan(other_side, other_book, pos["shares"])
    if hedge:
        projected_cost = _position_risk_cost(pos) + hedge["riskNotional"] + hedge["fee"]
        net_per_share = (pos["shares"] - projected_cost) / pos["shares"]
        cash = await _strategy_cash(dry_run)
        if (
            float(pos.get("entryLimitPrice", pos["entryPrice"])) + hedge["limitPrice"] <= LOCK_MAX_SUM
            and net_per_share >= sim.SIM_MIN_NET_LOCK_PER_SHARE
            and hedge["riskNotional"] + hedge["fee"] <= max(0.0, cash - MIN_CASH_RESERVE_USD)
        ):
            await _hedge_position(hedge, dry_run)
            return

    if fair:
        held_book = up_book if pos["side"] == "Up" else down_book
        exit_plan = _sell_plan(pos["side"], held_book, pos["shares"])
        if exit_plan:
            fair_side = fair["fairUp"] if pos["side"] == "Up" else fair["fairDown"]
            minimum_liquidation = exit_plan["riskNotional"] - exit_plan["fee"]
            expected_hold = pos["shares"] * fair_side
            if minimum_liquidation >= expected_hold + pos["shares"] * sim.SIM_EXIT_EDGE:
                await _close_position(exit_plan, dry_run, "market_bid_above_model_value")


async def strategy_loop() -> None:
    log.info("=" * 64)
    log.info("  Polymarket BTC Up/Down · 真實自動下單策略")
    log.info(
        f"  LIVE_TRADING={live.LIVE_TRADING} · POLY_STRATEGY_ARMED={STRATEGY_ARMED} "
        f"· REAL_EXECUTION={REAL_EXECUTION_ENABLED}"
    )
    log.info(f"  pair budget={STAKE_PCT:.1f}% · hard cap=${MAX_PAIR_BUDGET_USD:.2f}")
    log.info(f"  cash reserve=${MIN_CASH_RESERVE_USD:.2f} · action cooldown={ACTION_COOLDOWN_SECONDS:.0f}s")
    log.info(f"  entry <= ${ENTRY_MAX_PRICE}　lock sum <= ${LOCK_MAX_SUM}（{_LIVE_VARIANT['label']}）")
    log.info(
        f"  entry edge>={sim.SIM_MIN_ENTRY_EDGE:.3f} · net lock/share>={sim.SIM_MIN_NET_LOCK_PER_SHARE:.3f} "
        f"· no new entry under {sim.SIM_MIN_ENTRY_REMAINING:.0f}s"
    )
    if live_state.get("halted"):
        log.critical(f"  STRATEGY HALTED: {live_state.get('haltReason')}")
    log.info("=" * 64)

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                cur = sim.state["market"]
                new_market = await sim.fetch_active_market(session, "btc-updown-5m-")
                if new_market and (cur is None or new_market["slug"] != cur["slug"]):
                    if cur is not None:
                        queue_settlement(cur["slug"])
                    elif (
                        live_state.get("position")
                        and live_state["position"].get("windowSlug") != new_market["slug"]
                    ):
                        # 進程重啟後 sim.state 是空的，但真實策略狀態可能仍有上一窗口的持倉。
                        queue_settlement(live_state["position"]["windowSlug"])
                    sim.state["market"] = new_market
                    sim.state["windowEndsAt"] = sim._iso_to_ms(new_market["endDate"])
                    log.info(f"[MARKET] 切換到新窗口 {new_market['slug']}")

                if sim.state["market"]:
                    up_id, down_id = sim._market_tokens(sim.state["market"])
                    if up_id and down_id:
                        up_price, down_price, up_book, down_book, spot, klines = await asyncio.gather(
                            sim.fetch_midpoint(session, up_id),
                            sim.fetch_midpoint(session, down_id),
                            sim.fetch_book(session, up_id),
                            sim.fetch_book(session, down_id),
                            sim.fetch_spot_price(session, "BTCUSDT"),
                            sim.fetch_klines(session, "BTCUSDT", 60),
                            return_exceptions=True,
                        )
                        if not isinstance(up_price, Exception):
                            sim.state["upPrice"] = up_price
                        if not isinstance(down_price, Exception):
                            sim.state["downPrice"] = down_price
                        if not isinstance(up_book, Exception):
                            sim.state["upBook"] = up_book
                        if not isinstance(down_book, Exception):
                            sim.state["downBook"] = down_book
                        if not isinstance(spot, Exception):
                            sim.state["spotPrice"] = spot["price"]
                            sim.state["spotChangePct"] = spot["changePct"]
                        if not isinstance(klines, Exception) and klines:
                            sim.state["klines"] = klines

                        remaining = max(0.0, sim.state["windowEndsAt"] / 1000 - sim.real_now())
                        fair = sim.estimate_fair_up("btc")
                        await evaluate_and_act(sim.state["market"]["slug"], session, remaining, fair)

                await retry_pending_settlements(session)
            except Exception as exc:
                log.exception(f"策略迴圈錯誤：{exc}")
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(strategy_loop())
