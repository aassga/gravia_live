"""
Polymarket BTC 5 分鐘 Up/Down · 真實自動下單策略

真實版與紙上模擬共用同一套核心判斷（BTC 5m 使用 "btc-historical-hybrid"）：
    - 使用 Ask/Bid 深度計算 VWAP，再以含滑點、向不利 tick 取整的最差限價作決策。
    - 優先鎖利：當下兩邊同時買得到、扣費用後淨賺達門檻才配對進場，這是唯一的
      無方向曝險進場路徑。
    - 單腿晚進場方向性預設停用；只有 POLY_ENABLE_LATE_DIRECTION=true 才會在窗口
      剩不到 10 秒、且現價明顯偏離開盤價時啟用，這不是鎖利交易。
    - 只有「兩腿配對其中一腿失敗、留下未預期單邊曝險」這種例外情況，才會嘗試
      補鎖利或在市場 Bid 顯著高於模型持有價值時提早退出——不是常態進場路徑。

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
from decimal import Decimal, ROUND_DOWN

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
# Validation mode is a hard safety interlock: it can never submit real orders.
REAL_EXECUTION_ENABLED = live.LIVE_TRADING and STRATEGY_ARMED and not live.VALIDATE_ORDER_PATH
_LATE_DIRECTION_REQUESTED = os.environ.get("POLY_ENABLE_LATE_DIRECTION", "false").strip().lower() == "true"
MAX_PAIR_BUDGET_USD = max(1.0, float(os.environ.get("POLY_MAX_PAIR_BUDGET_USD", "25.0")))
MIN_CASH_RESERVE_USD = max(0.0, float(os.environ.get("POLY_MIN_CASH_RESERVE_USD", "5.0")))
DRY_RUN_BALANCE_USD = max(1.0, float(os.environ.get("POLY_DRY_RUN_BALANCE_USD", "100.0")))
ACTION_COOLDOWN_SECONDS = max(1.0, float(os.environ.get("POLY_ACTION_COOLDOWN_SECONDS", "10.0")))
ORDER_CONFIRM_ATTEMPTS = max(1, int(os.environ.get("POLY_ORDER_CONFIRM_ATTEMPTS", "6")))
ORDER_CONFIRM_INTERVAL = max(0.5, float(os.environ.get("POLY_ORDER_CONFIRM_INTERVAL", "1.0")))
# 2026-09：第一腿剛 BUY matched 後立刻送 SELL，CLOB 仍可能因鏈上結算／餘額快取尚未
# 完成而回覆 balance: 0。不能用固定 sleep 猜入帳時間：救援會主動刷新 Conditional Token
# balance/allowance，最多等待 15 秒，確認完整股數可賣後才依最新 bid 送出 SELL。
EMERGENCY_UNWIND_WAIT_SECONDS = max(1.0, float(os.environ.get("POLY_EMERGENCY_UNWIND_WAIT_SECONDS", "15.0")))
EMERGENCY_UNWIND_POLL_INTERVAL = max(0.25, float(os.environ.get("POLY_EMERGENCY_UNWIND_POLL_INTERVAL", "0.5")))
EMERGENCY_UNWIND_ORDER_INTERVAL = max(0.5, float(os.environ.get("POLY_EMERGENCY_UNWIND_ORDER_INTERVAL", "1.0")))
# 2026-09：緊急平倉連續兩次都失敗、部位被迫抱到自然結算、整筆本金虧光的真實案例
# （-$3.28 那筆）——正常補鎖利跟緊急平倉共用同一套「保守限價多讓一格 tick」的定價，
# 但緊急平倉的目標是「不計代價盡快出場」，不是「盡量拿到好價格」，值得比平常更激進：
# 在正常保守限價之上，再多讓這麼多格 tick，犧牲一點價格換取更高的立即成交機率。
EMERGENCY_UNWIND_EXTRA_TICKS = max(0, int(os.environ.get("POLY_EMERGENCY_UNWIND_EXTRA_TICKS", "3")))
_validated_order_path_slug: str | None = None
_live_data_guard_log_at = 0.0

# 真實版跟隨模擬版 ASSETS 清單裡的哪一個市場，預設是 BTC 5 分鐘窗口（"btc"）。
# 2026-09：模擬盤驗證出 15 分鐘／4 小時窗口的訂單簿深度比 5 分鐘深很多（少踩到「兩腿
# batch 送出、一腿沒接到」這個結構性風險），先讓真實版可以指到 sim.ASSETS 裡任何一個
# id（例如 "btc-15m"），評估其他窗口在真實下單時表不表現得更好，不用另外複製一份程式。
LIVE_ASSET_ID = os.environ.get("POLY_LIVE_ASSET_ID", "btc")
if LIVE_ASSET_ID != "btc":
    sim.state = sim.markets_state[LIVE_ASSET_ID]  # 重新指向對應資產的市場狀態（見 sim.state 的定義）

# 真實版可用環境變數選擇模擬盤的同資產策略，讓兩邊共用同一組策略定義。
# 未設定時保留原本的歷史混合策略；VPS 測試其他策略時不需要再修改程式碼。
_DEFAULT_LIVE_VARIANT_ID = (
    "btc-historical-hybrid"
    if LIVE_ASSET_ID == "btc"
    else f"{LIVE_ASSET_ID}-chainlink-late-direction"
)
LIVE_VARIANT_ID = os.environ.get("POLY_LIVE_VARIANT_ID", _DEFAULT_LIVE_VARIANT_ID).strip()
if LIVE_VARIANT_ID not in sim.AB_VARIANT_BY_ID:
    raise RuntimeError(f"未知的 POLY_LIVE_VARIANT_ID: {LIVE_VARIANT_ID}")
_LIVE_VARIANT = sim.AB_VARIANT_BY_ID[LIVE_VARIANT_ID]
if _LIVE_VARIANT["assetId"] != LIVE_ASSET_ID:
    raise RuntimeError(
        f"POLY_LIVE_VARIANT_ID={LIVE_VARIANT_ID} 不屬於 POLY_LIVE_ASSET_ID={LIVE_ASSET_ID}"
    )
# 保留舊名稱供既有工具／測試相容。只有明確屬於晚進場方向性的變體才能開啟單腿交易；
# btc-loose 等兩腿策略即使環境殘留 true，也不會意外啟用方向性下注。
_LIVE_DIRECTION_VARIANT_ID = LIVE_VARIANT_ID
ENABLE_LATE_DIRECTION = _LATE_DIRECTION_REQUESTED and bool(_LIVE_VARIANT.get("lateDirectionOnly"))
# 純晚進場方向性變體不能先被兩腿鎖利部位占用；歷史混合變體則保留「先鎖利、
# 找不到才方向性」的既有流程。
DIRECT_PAIR_ENABLED = not bool(_LIVE_VARIANT.get("lateDirectionOnly")) or bool(
    _LIVE_VARIANT.get("historicalHybrid")
)
# 2026-09-07 實盤再次出現「快照上兩腿合計 0.92，但 346ms 後只成交一腿」。公開 API
# 的 batch 不是原子交易，因此把門檻收緊、要求限價內有數倍深度，並只接受持續存在的機會。
# 這些參數也由 btc-live-lock 模擬組讀取，避免模擬與實盤再次使用不同條件。
LOCK_MAX_SUM = max(0.01, min(0.99, float(os.environ.get("POLY_LIVE_LOCK_MAX_SUM", "0.95"))))
PAIR_MIN_DEPTH_MULTIPLIER = max(1.0, float(os.environ.get("POLY_PAIR_MIN_DEPTH_MULTIPLIER", "1.0")))
PAIR_STABILITY_SECONDS = max(0.0, float(os.environ.get("POLY_PAIR_STABILITY_SECONDS", "0.15")))
RESCUE_LOCK_MAX_SUM = max(LOCK_MAX_SUM, min(0.99, float(os.environ.get("POLY_RESCUE_LOCK_MAX_SUM", "0.99"))))
LATE_DIRECTION_MAX_PRICE = float(_LIVE_VARIANT.get("lateDirectionMaxPrice", 0.92))


def _new_live_state() -> dict:
    return {
        "position": None,
        "pendingSettlements": [],
        "trades": [],
        "windowDiagnostics": [],
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
        "validationSlug": None,
        "validationResult": None,
        "quoteSource": "not_started",
        "wsConnected": False,
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
sim.decision_diag.trim_old_window_evidence(live_state.setdefault("windowDiagnostics", []))
_live_window_diag_dirty = False


def save_live_state() -> None:
    global _live_window_diag_dirty
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
            _live_window_diag_dirty = False
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.1 * (attempt + 1))


def reset_live_state_for_tests() -> None:
    """只供單元測試在記憶體中清狀態；不會刪除實際狀態檔。"""
    global _validated_order_path_slug, _live_window_diag_dirty
    _validated_order_path_slug = None
    live_state.clear()
    live_state.update(_new_live_state())
    _live_window_diag_dirty = False


def _live_window_diagnostic(slug: str) -> dict:
    """Return one bounded, persistent diagnostic summary for a live market window."""
    global _live_window_diag_dirty
    items = live_state.setdefault("windowDiagnostics", [])
    sim.decision_diag.trim_old_window_evidence(items)
    execution_mode = "REAL" if REAL_EXECUTION_ENABLED else "DRY-RUN"
    for item in items:
        if (
            item.get("windowSlug") == slug
            and item.get("strategyVariant") == LIVE_VARIANT_ID
            and item.get("executionMode") == execution_mode
        ):
            return item
    now = time.time()
    item = {
        "windowSlug": slug,
        "strategyVariant": LIVE_VARIANT_ID,
        "strategyLabel": _LIVE_VARIANT["label"],
        "executionMode": execution_mode,
        "firstSeenAt": now,
        "lastSeenAt": now,
        "status": "observing",
        "evaluations": 0,
        "diagnosticEvents": 0,
        "reasonCounts": {},
        "lastReason": None,
    }
    items.insert(0, item)
    del items[50:]
    _live_window_diag_dirty = True
    return item


def record_live_window_diagnostic(slug: str, reason: str | None = None, **details) -> dict:
    """Aggregate live-path evidence in memory; the 3-second loop persists it in one write."""
    global _live_window_diag_dirty
    item = _live_window_diagnostic(slug)
    item["lastSeenAt"] = time.time()
    if reason is None:
        item["evaluations"] = int(item.get("evaluations", 0)) + 1
        source = details.get("evaluationSource")
        if source:
            sources = item.setdefault("evaluationSources", {})
            sources[source] = int(sources.get(source, 0)) + 1
    else:
        item["diagnosticEvents"] = int(item.get("diagnosticEvents", 0)) + 1
        counts = item.setdefault("reasonCounts", {})
        counts[reason] = int(counts.get(reason, 0)) + 1
        item["lastReason"] = reason
    for key, value in details.items():
        if value is not None:
            item[key] = value
    for detail_key, best_key in (
        ("rawPairAskSum", "bestRawPairAskSum"),
        ("pairDecisionSum", "bestPairDecisionSum"),
    ):
        value = details.get(detail_key)
        if isinstance(value, (int, float)):
            prior = item.get(best_key)
            if prior is None or float(value) < float(prior):
                item[best_key] = float(value)
    delta = details.get("signalDeltaPct")
    if isinstance(delta, (int, float)):
        prior = item.get("maxAbsSignalDeltaPct")
        if prior is None or abs(float(delta)) > float(prior):
            item["maxAbsSignalDeltaPct"] = abs(float(delta))
    remaining = details.get("remainingSeconds")
    if isinstance(remaining, (int, float)):
        item["minRemainingSeconds"] = min(float(remaining), float(item.get("minRemainingSeconds", remaining)))
        item["maxRemainingSeconds"] = max(float(remaining), float(item.get("maxRemainingSeconds", remaining)))
    sim.decision_diag.record(item, reason, details)
    _live_window_diag_dirty = True
    return item


def record_live_window_observation(
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float | None,
    evaluation_source: str,
) -> None:
    up_asks = up_book.get("asks") or []
    down_asks = down_book.get("asks") or []
    up_ask = float(up_asks[0]["price"]) if up_asks else None
    down_ask = float(down_asks[0]["price"]) if down_asks else None
    record_live_window_diagnostic(
        slug,
        remainingSeconds=remaining_seconds,
        evaluationSource=evaluation_source,
        upAsk=up_ask,
        downAsk=down_ask,
        upAskDepth=sum(float(level.get("size", 0)) for level in up_asks),
        downAskDepth=sum(float(level.get("size", 0)) for level in down_asks),
        rawPairAskSum=(up_ask + down_ask) if up_ask is not None and down_ask is not None else None,
        upQuoteSource=up_book.get("quoteSource"),
        downQuoteSource=down_book.get("quoteSource"),
    )


def flush_live_window_diagnostics() -> None:
    if _live_window_diag_dirty:
        save_live_state()


def finalize_live_window_diagnostic(slug: str) -> None:
    global _live_window_diag_dirty
    matching = [
        item
        for item in live_state.setdefault("windowDiagnostics", [])
        if item.get("windowSlug") == slug
    ]
    if not matching:
        matching = [_live_window_diagnostic(slug)]
    finalized_at = time.time()
    for item in matching:
        if not item.get("entryCount"):
            item["status"] = "no_entry"
        item["finalizedAt"] = finalized_at
    _live_window_diag_dirty = True


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
    # 2026-09：真實成交量不保證等於下單當時規劃的股數（見 _resolved_execution），
    # 補鎖利那一腿的真實股數（hedgeShares）可能跟進場那一腿的真實股數（shares）不一樣。
    # 贏的那一邊每股payout $1，所以到底該用哪一腿的股數要看結算結果是哪一邊贏，
    # 不能無條件都用 pos["shares"]（進場那腿的股數）——兩腿股數只要有差，這樣算會系統性算錯。
    if pos.get("hedged"):
        hedge_shares = float(pos.get("hedgeShares", pos["shares"]))
        payout = hedge_shares if outcome == pos.get("hedgeSide") else float(pos["shares"])
    elif outcome == pos["side"]:
        payout = float(pos["shares"])
    else:
        payout = 0.0
    return payout - _position_paid_cost(pos)


def _record_trade(pos: dict, pnl: float, outcome: str, trade_type: str) -> None:
    fees = float(pos.get("entryFee", 0)) + float(pos.get("hedgeFee", 0)) + float(pos.get("exitFee", 0))
    trade = {
        "windowSlug": pos["windowSlug"],
        "side": pos["side"],
        "shares": pos["shares"],
        "stakeUsd": pos.get("stakeUsd", 0.0),
        "entryPrice": pos["entryPrice"],
        "entryObservedVwap": pos.get("entryObservedVwap"),
        "entryLimitPrice": pos.get("entryLimitPrice"),
        "entryPriceSource": pos.get("entryPriceSource"),
        "hedged": pos.get("hedged", False),
        "hedgeSide": pos.get("hedgeSide"),
        "hedgeShares": pos.get("hedgeShares"),
        "shareMismatch": pos.get("shareMismatch"),
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
    diagnostic = record_live_window_diagnostic(
        pos["windowSlug"],
        "settled",
        status="settled",
        outcome=outcome,
        tradeType=trade_type,
        pnlEstimate=pnl,
        feesEstimate=fees,
        settledAt=trade["exitTime"],
    )
    diagnostic["settlementCount"] = int(diagnostic.get("settlementCount", 0)) + 1
    save_live_state()


def marketable_limit_price(book: dict, fill: dict, side: str) -> float:
    """向後相容入口；模擬與實盤實際共用 polymarket_server 的同一函式。"""
    return sim.marketable_limit_price(book, fill, side)


def _market_condition_id() -> str | None:
    market = sim.state.get("market") or {}
    value = market.get("conditionId") or market.get("condition_id")
    return str(value) if value else None


def _fee_for_side(side: str, shares: float, price: float) -> tuple[float, float, float]:
    """回傳 (fee, rate, exponent)；實盤只接受預熱取得的 V2 動態市場費率。"""
    if not REAL_EXECUTION_ENABLED:
        return sim.taker_fee(shares, price), sim.SIM_TAKER_FEE_RATE, 1.0
    token_id = _token_id(side)
    config = live.get_cached_market_fee(token_id)
    if config is None:
        raise RuntimeError(f"missing_v2_market_fee token={token_id}")
    rate = float(config["rate"])
    exponent = float(config["exponent"])
    fee = round(shares * rate * (price * (1 - price)) ** exponent, 5)
    return (fee if fee >= 0.00001 else 0.0), rate, exponent


def _fee_from_plan(plan: dict, shares: float, price: float) -> float:
    rate = float(plan.get("feeRate", sim.SIM_TAKER_FEE_RATE))
    exponent = float(plan.get("feeExponent", 1.0))
    fee = round(shares * rate * (price * (1 - price)) ** exponent, 5)
    return fee if fee >= 0.00001 else 0.0


def _risk_fill(book: dict, fill: dict, order_side: str, market_side: str) -> dict:
    decision = sim.decision_fill(book, fill, order_side)
    shares = float(fill["shares"])
    fee, fee_rate, fee_exponent = _fee_for_side(market_side, shares, decision["decisionPrice"])
    return {
        "shares": shares,
        "observedVwap": decision["observedVwap"],
        "limitPrice": decision["decisionPrice"],
        "riskNotional": decision["decisionNotional"],
        "fee": fee,
        "feeRate": fee_rate,
        "feeExponent": fee_exponent,
    }


async def _resolved_execution(plan: dict, response: dict, dry_run: bool) -> dict:
    """取得成交後的記帳價格：dry-run 用模擬 VWAP，實盤優先用成交紀錄，否則用保守限價。

    2026-09：實測發現 Polymarket 的 FOK 成交量不保證等於下單當時規劃的股數（同一次
    真實下單：規劃 13 股，實際成交 13.565216 股）。舊版這裡要求「查到的真實成交股數
    幾乎完全等於規劃股數」才採用，一旦真實成交量跟規劃不同就整段丟棄真實資料、
    靜默退回保守限價記帳——結果是帳面數字系統性地跟 Polymarket 對不起來，而且完全
    沒有 log 可以看出發生過這件事。現在改成：只要查得到真實成交紀錄就一律採用
    （不管股數是否跟規劃吻合），股數不吻合時記一筆警告讓人知道，但不再拒用真實資料。
    回傳值新增 "shares" 欄位——呼叫端要用這個（真實成交股數）去更新部位追蹤，
    不能再假設「送出多少股就一定成交多少股」。
    """
    source = "simulated_vwap" if dry_run else "conservative_limit"
    price = float(plan["observedVwap"] if dry_run else plan["limitPrice"])
    shares = float(plan["shares"])
    notional = shares * price
    fee = _fee_from_plan(plan, shares, price)

    if not dry_run:
        try:
            summary = await asyncio.to_thread(live.get_order_fill_summary, response)
        except Exception as exc:
            log.warning(f"[LIVE] 無法取得實際成交均價，暫以保守限價記帳：{exc}")
            summary = None
        if not summary:
            log.warning(
                "[LIVE] 查不到這筆訂單的真實成交紀錄（可能是 API 索引還沒跟上剛成交的訂單），"
                f"暫以保守限價記帳，事後應人工核對：orderID="
                f"{response.get('orderID') or response.get('orderId')}"
            )
        else:
            real_shares = float(summary["shares"])
            if abs(real_shares - shares) > max(1e-6, shares * 0.001):
                log.warning(
                    f"[LIVE] 真實成交股數（{real_shares:.6f}）跟下單規劃股數（{shares:.6f}）"
                    "不一致——這個交易所的 FOK 成交量不保證等於送出的股數。改用真實股數/"
                    "均價記帳，部位追蹤會跟著校正。"
                )
            shares = real_shares
            price = float(summary["price"])
            notional = float(summary["notional"])
            fee = summary.get("fee")
            fee = _fee_from_plan(plan, shares, price) if fee is None else float(fee)
            source = "matched_trades"

    return {"price": price, "notional": notional, "fee": fee, "source": source, "shares": shares}


def _ask_depth(book: dict) -> float:
    """訂單簿目前看得到的賣單總深度，用來把想要的股數縮到真的吃得到的量，
    避免算出來的股數超過深度、FOK/FAK 整筆判定未成交，白白錯過機會。"""
    return sum(float(a.get("size", 0)) for a in (book.get("asks") or []))


def _buy_plan(side: str, book: dict, shares: float, fair_probability: float | None = None) -> dict | None:
    fill = sim.simulate_buy_fill(book, shares)
    if not fill:
        return None
    risk = _risk_fill(book, fill, "BUY", side)
    if shares < float(book.get("minOrderSize", 1) or 1) or risk["riskNotional"] < sim.SIM_MIN_ORDER_NOTIONAL_USD:
        return None
    all_in_per_share = (risk["riskNotional"] + risk["fee"]) / shares
    edge = None if fair_probability is None else fair_probability - all_in_per_share
    return {"side": side, "book": book, "fair": fair_probability, "edge": edge, **risk}


def _sell_plan(side: str, book: dict, shares: float) -> dict | None:
    fill = sim.simulate_sell_fill(book, shares)
    if not fill:
        return None
    risk = _risk_fill(book, fill, "SELL", side)
    if shares < float(book.get("minOrderSize", 1) or 1) or risk["riskNotional"] < sim.SIM_MIN_ORDER_NOTIONAL_USD:
        return None
    return {"side": side, "book": book, **risk}


def _aggressive_sell_plan(side: str, book: dict, shares: float) -> dict | None:
    """緊急平倉專用：比 _sell_plan 的保守限價再多讓 EMERGENCY_UNWIND_EXTRA_TICKS 格
    tick，目標是「不計代價盡快出場」，換取更高的立即成交機率。"""
    plan = _sell_plan(side, book, shares)
    if not plan or EMERGENCY_UNWIND_EXTRA_TICKS <= 0:
        return plan
    tick_value = float(book.get("tickSize", 0.01) or 0.01)
    tick = Decimal(str(tick_value))
    price = Decimal(str(plan["limitPrice"])) - tick * EMERGENCY_UNWIND_EXTRA_TICKS
    price = float(max(tick, min(Decimal("1") - tick, price)))
    plan = dict(plan)
    plan["limitPrice"] = price
    plan["riskNotional"] = plan["shares"] * price
    plan["fee"] = _fee_from_plan(plan, plan["shares"], price)
    return plan


def _late_direction_plan(
    up_book: dict,
    down_book: dict,
    remaining_seconds: float,
    shares: float,
    diagnostic_slug: str | None = None,
) -> dict | None:
    """依實盤選定變體的價格來源建立最後 3～10 秒方向單計畫。"""
    if (
        remaining_seconds > sim.LATE_DIRECTION_WINDOW_SECONDS
        or remaining_seconds < sim.LATE_DIRECTION_MIN_ENTRY_REMAINING
    ):
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug, "outside_direction_window", remainingSeconds=remaining_seconds
            )
        return None
    if _LIVE_VARIANT.get("directionSignalSource") == "binance_window":
        opening, current = sim.state.get("windowOpenSpotPrice"), sim.state.get("spotPrice")
        if not opening or not current or opening <= 0:
            if diagnostic_slug:
                record_live_window_diagnostic(
                    diagnostic_slug, "missing_binance_signal", remainingSeconds=remaining_seconds
                )
            return None
        delta_pct = (current - opening) / opening * 100
        signal_source = "binance_futures_window"
        signal_observed_at = int(time.time() * 1000)
        signal_age_seconds = 0.0
    else:
        signal = sim.get_chainlink_twap_signal(LIVE_ASSET_ID)
        if not signal:
            if diagnostic_slug:
                record_live_window_diagnostic(
                    diagnostic_slug, "missing_chainlink_signal", remainingSeconds=remaining_seconds
                )
            return None
        delta_pct = (signal["current"] - signal["opening"]) / signal["opening"] * 100
        signal_source = "chainlink_twap_60s"
        signal_observed_at = signal["observedAt"]
        signal_age_seconds = signal["ageSeconds"]
    if abs(delta_pct) < sim.LATE_DIRECTION_MIN_DELTA_PCT:
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug,
                "direction_delta_below_minimum",
                remainingSeconds=remaining_seconds,
                signalSource=signal_source,
                signalDeltaPct=delta_pct,
                signalAgeSeconds=signal_age_seconds,
                minimumSignalDeltaPct=sim.LATE_DIRECTION_MIN_DELTA_PCT,
            )
        return None
    side, book = ("Up", up_book) if delta_pct > 0 else ("Down", down_book)
    if not _live_direction_book_is_fresh(side, book):
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug,
                "direction_book_not_fresh",
                remainingSeconds=remaining_seconds,
                signalSource=signal_source,
                signalDeltaPct=delta_pct,
                selectedSide=side,
                dataGuardReason=sim._simulation_single_book_guard_reason(book),
            )
        return None
    # 跟模擬版對齊：不把股數縮到「當下看得到的深度」——真正的 FOK 語意是要嘛整筆用
    # 目標股數成交、要嘛深度不夠就整筆不成交，不會自動改成「有多少吃多少」。這裡故意
    # 不呼叫 _ask_depth 縮股，讓 _buy_plan 內部的 simulate_buy_fill 用同一套全有全無
    # 判斷，深度不足就直接放棄這次機會，跟模擬版的驗證結果一致。
    plan = _buy_plan(side, book, shares)
    if not plan:
        if diagnostic_slug:
            asks = book.get("asks") or []
            record_live_window_diagnostic(
                diagnostic_slug,
                "direction_insufficient_depth_or_minimum",
                remainingSeconds=remaining_seconds,
                signalSource=signal_source,
                signalDeltaPct=delta_pct,
                selectedSide=side,
                selectedAsk=float(asks[0]["price"]) if asks else None,
                selectedAskDepth=sum(float(level.get("size", 0)) for level in asks),
                targetShares=shares,
            )
        return None
    if plan["limitPrice"] > LATE_DIRECTION_MAX_PRICE:
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug,
                "direction_price_above_maximum",
                remainingSeconds=remaining_seconds,
                signalSource=signal_source,
                signalDeltaPct=delta_pct,
                selectedSide=side,
                selectedAsk=float((book.get("asks") or [{}])[0].get("price", 0)),
                pairDecisionSum=None,
                directionLimitPrice=plan["limitPrice"],
                directionMaxPrice=LATE_DIRECTION_MAX_PRICE,
                targetShares=shares,
            )
        return None
    plan["_deltaPct"] = delta_pct
    plan["_signalSource"] = signal_source
    plan["_signalObservedAt"] = signal_observed_at
    plan["_signalAgeSeconds"] = signal_age_seconds
    plan["_bookQuoteSource"] = book.get("quoteSource")
    plan["_bookReceivedAtMonotonic"] = book.get("receivedAtMonotonic")
    if diagnostic_slug:
        record_live_window_diagnostic(
            diagnostic_slug,
            "direction_candidate",
            status="candidate",
            remainingSeconds=remaining_seconds,
            signalSource=signal_source,
            signalDeltaPct=delta_pct,
            signalAgeSeconds=signal_age_seconds,
            selectedSide=side,
            directionLimitPrice=plan["limitPrice"],
            targetShares=shares,
        )
    return plan


async def _try_late_direction_entry(
    slug: str,
    up_book: dict,
    down_book: dict,
    remaining_seconds: float,
    shares: float,
    dry_run: bool,
) -> bool:
    """3 秒輪詢路徑用的原本介面：判斷＋送單一起做。跟 _on_ws_tick_sync 快速路徑共用
    同一個 _late_direction_plan 判斷邏輯，兩條路不會長歪成不同標準。"""
    plan = _late_direction_plan(
        up_book, down_book, remaining_seconds, shares, diagnostic_slug=slug
    )
    if not plan:
        return False
    log.info(
        f"[LIVE] 晚進場方向性 {plan['side']} source={plan['_signalSource']} Δ={plan['_deltaPct']:+.3f}% "
        f"age={plan['_signalAgeSeconds']:.3f}s "
        f"剩餘={remaining_seconds:.1f}s"
    )
    result = await _enter_position(slug, plan, dry_run)
    if result == "filled":
        live_state["position"]["strategy"] = "late_direction"
        save_live_state()
    return result == "filled"


def _target_pair_order(cash: float) -> tuple[float, float]:
    """跟模擬版共用同一個計算函式（sim.target_pair_order），只是帶入真實版自己的
    下注比例／資金上限／保留額——公式本身跟模擬版保證一致，不會各寫一份長歪。"""
    return sim.target_pair_order(cash, STAKE_PCT, LOCK_MAX_SUM, MAX_PAIR_BUDGET_USD, MIN_CASH_RESERVE_USD)


def _direct_pair_plans(
    up_book: dict,
    down_book: dict,
    shares: float,
    cash: float,
    diagnostic_slug: str | None = None,
) -> tuple[dict, dict] | None:
    up = _buy_plan("Up", up_book, shares)
    down = _buy_plan("Down", down_book, shares)
    if not up or not down:
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug,
                "pair_incomplete_fill_or_minimum",
                targetShares=shares,
                upAskDepth=sum(float(level.get("size", 0)) for level in (up_book.get("asks") or [])),
                downAskDepth=sum(float(level.get("size", 0)) for level in (down_book.get("asks") or [])),
            )
        return None
    total_cost = up["riskNotional"] + up["fee"] + down["riskNotional"] + down["fee"]
    net_per_share = (shares - total_cost) / shares
    decision_sum = up["limitPrice"] + down["limitPrice"]
    common = {
        "targetShares": shares,
        "pairDecisionSum": decision_sum,
        "pairNetPerShare": net_per_share,
        "upLimitPrice": up["limitPrice"],
        "downLimitPrice": down["limitPrice"],
    }
    if decision_sum > LOCK_MAX_SUM:
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug, "pair_price_sum_above_maximum", lockMaxSum=LOCK_MAX_SUM, **common
            )
        return None
    # 跟模擬版 sim._try_direct_pair 對齊：這裡只比對 cash 本身，不再扣一次
    # MIN_CASH_RESERVE_USD——保留額已經在 _target_pair_order／target_pair_order
    # 算股數預算時扣過了，這裡如果再扣一次會變成保留額重複計算，讓實盤比模擬更早
    # 放棄本可成立的鎖利機會。
    if net_per_share < sim.SIM_MIN_NET_LOCK_PER_SHARE:
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug,
                "pair_net_edge_below_minimum",
                minimumNetLockPerShare=sim.SIM_MIN_NET_LOCK_PER_SHARE,
                **common,
            )
        return None
    if total_cost > cash:
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug, "pair_insufficient_cash", totalRiskCost=total_cost, cashUsd=cash, **common
            )
        return None
    if not sim.pair_depth_is_safe(
        up_book,
        down_book,
        up["limitPrice"],
        down["limitPrice"],
        shares,
        PAIR_MIN_DEPTH_MULTIPLIER,
    ):
        if diagnostic_slug:
            record_live_window_diagnostic(
                diagnostic_slug,
                "pair_depth_multiplier_not_met",
                minimumDepthMultiplier=PAIR_MIN_DEPTH_MULTIPLIER,
                **common,
            )
        return None
    if diagnostic_slug:
        record_live_window_diagnostic(
            diagnostic_slug, "pair_candidate", status="candidate", totalRiskCost=total_cost, **common
        )
    return up, down


def _get_real_cash() -> float:
    raw = live.get_usdc_balance()
    return int(raw.get("balance", 0)) / 1_000_000


# 真實餘額改成背景任務定期刷新（見 _cash_refresh_loop），evaluate_and_act 的決策路徑
# 永遠只讀快取，不會自己等網路 I/O——這條路徑是在 decision_lock 保護下跑的，如果在這裡
# await 真實 API（即使只是 8 秒才發生一次），鎖住的那 100~300ms 之間新進來的 WS 報價
# 會被 _evaluate_ws_tick 直接丟棄（見那邊的 decision_lock.locked() 判斷）。稍縱即逝的
# 錯價窗口如果剛好撞上這段等待，就會被平白錯過——這是實盤曾經漏接模擬盤抓到的鎖利
# 機會的根本原因，不是運氣不好。
CASH_CACHE_TTL_SECONDS = 8.0
_cash_cache: dict = {"value": None, "at": 0.0}


async def _refresh_cash_cache() -> None:
    try:
        value = await asyncio.to_thread(_get_real_cash)
        _cash_cache["value"] = value
        _cash_cache["at"] = time.time()
    except Exception as exc:
        log.warning(f"[LIVE] 刷新真實餘額快取失敗：{exc}")


def _invalidate_cash_cache() -> None:
    _cash_cache["at"] = 0.0
    if REAL_EXECUTION_ENABLED:
        # 下單後立刻在背景重查一次，不用整整等到下一輪 8 秒週期——但這個 task 本身
        # 不會被 decision_lock 卡住，也不會讓呼叫端等待。
        asyncio.create_task(_refresh_cash_cache())


async def _cash_refresh_loop() -> None:
    """背景持續刷新真實餘額快取，讓 evaluate_and_act 決策路徑不必再自己 await 網路 I/O。"""
    while True:
        if REAL_EXECUTION_ENABLED:
            await _refresh_cash_cache()
        await asyncio.sleep(CASH_CACHE_TTL_SECONDS)


async def _strategy_cash(dry_run: bool) -> float:
    if dry_run:
        return DRY_RUN_BALANCE_USD
    if _cash_cache["value"] is not None:
        return _cash_cache["value"]
    # 背景刷新任務還沒跑過第一次（進程剛啟動的瞬間），退而求其次同步查一次墊底，
    # 這是唯一還會在決策路徑上等網路 I/O 的情況，只會發生一次。
    await _refresh_cash_cache()
    return _cash_cache["value"] if _cash_cache["value"] is not None else 0.0


def _strategy_cash_sync(dry_run: bool) -> float | None:
    """零延遲同步版本，只給 _on_ws_tick_sync 這條快速路徑用：dry-run 直接回傳常數；
    真實模式只讀已經快取好的餘額，快取還沒熱過就回傳 None，讓呼叫端乾脆跳過這個 tick、
    改靠 3 秒輪詢那條路（原本的 evaluate_and_act，會正確 await 刷新）兜底，不在這條
    必須零延遲的路徑上等網路 I/O。"""
    if dry_run:
        return DRY_RUN_BALANCE_USD
    return _cash_cache["value"]


async def _query_conditional_balance_with_retry(token_id: str) -> float:
    """查詢單一 token 餘額，失敗重試一次。2026-09：這裡原本用 asyncio.gather 讓兩個
    token 的查詢同時發出，結果兩個背景執行緒在程式剛啟動時同時打中 py_clob_client_v2
    共用的 httpx.Client(http2=True)、搶著建立同一條連線的第一個 request，穩定重現
    [Errno 11] Resource temporarily unavailable（連兩台不同 VPS 都一樣）。改成序列
    查詢完全避開這個 race，並加一次重試——這個檢查失敗會直接讓整個策略停止下單，
    不該讓單次暫時性錯誤就要人工介入重啟。"""
    try:
        return await asyncio.to_thread(live.get_conditional_balance, token_id)
    except Exception:
        await asyncio.sleep(1.0)
        return await asyncio.to_thread(live.get_conditional_balance, token_id)


async def _ensure_no_unmanaged_current_position() -> bool:
    """真實模式首筆下單前，確認當前 token 沒有策略狀態外的持倉或掛單。"""
    up_id, down_id = sim._market_tokens(sim.state["market"])
    try:
        up_balance = await _query_conditional_balance_with_retry(up_id)
        down_balance = await _query_conditional_balance_with_retry(down_id)
    except Exception as exc:
        _set_halt(f"preflight_position_check_failed: {exc}")
        return False
    if up_balance >= 0.01 or down_balance >= 0.01:
        _set_halt(
            f"unmanaged_current_market_position up={up_balance:.6f} down={down_balance:.6f}"
        )
        return False
    try:
        open_orders = await asyncio.to_thread(live.get_open_orders)
    except Exception as exc:
        _set_halt(f"preflight_open_order_check_failed: {exc}")
        return False
    current_tokens = {str(up_id), str(down_id)}
    current_open_orders = []
    for order in open_orders or []:
        if not isinstance(order, dict):
            continue
        token_id = str(
            order.get("asset_id")
            or order.get("assetId")
            or order.get("token_id")
            or order.get("tokenId")
            or ""
        )
        if token_id in current_tokens:
            current_open_orders.append(order)
    if current_open_orders:
        _set_halt(
            f"unmanaged_current_market_open_orders count={len(current_open_orders)}",
            {"orders": current_open_orders},
        )
        return False
    try:
        # 每個 5 分鐘窗口都換新 token。先在 preflight（非搶單臨界路徑）填好 SDK 的
        # tick-size／neg-risk／version 快取，避免真正要送單時才多等數個 GET 往返。
        condition_id = _market_condition_id()
        if not condition_id:
            raise RuntimeError("current market is missing conditionId")
        await asyncio.to_thread(live.prewarm_order_tokens, [up_id, down_id], condition_id)
    except Exception as exc:
        _set_halt(f"preflight_order_warmup_failed: {exc}")
        return False
    return True


def _live_book_guard_reason(up_book: dict, down_book: dict) -> str | None:
    """實盤進場只允許同一時間範圍內的兩份完整 WS 快照。"""
    return sim._simulation_book_guard_reason(up_book, down_book)


def _live_books_are_coherent(up_book: dict, down_book: dict) -> bool:
    global _live_data_guard_log_at
    reason = _live_book_guard_reason(up_book, down_book)
    if reason is None:
        return True
    now = time.monotonic()
    if now - _live_data_guard_log_at >= sim.SIM_DATA_GUARD_LOG_SECONDS:
        _live_data_guard_log_at = now
        log.warning(f"[LIVE-DATA-GUARD] 跳過真實下單：{reason}")
    return False


def _live_direction_book_is_fresh(side: str, book: dict) -> bool:
    """Direction orders only need a fresh WebSocket snapshot for the selected BUY leg."""
    global _live_data_guard_log_at
    reason = sim._simulation_single_book_guard_reason(book)
    if reason is None:
        return True
    now = time.monotonic()
    if now - _live_data_guard_log_at >= sim.SIM_DATA_GUARD_LOG_SECONDS:
        _live_data_guard_log_at = now
        log.warning(f"[LIVE-DATA-GUARD] 跳過方向性下單 {side}：{reason}")
    return False


async def _current_account_reconciliation() -> dict:
    """結果不明時留下唯讀帳戶快照，供人工判斷；絕不據此自動重送訂單。"""
    snapshot: dict = {"capturedAt": time.time(), "balances": {}, "openOrders": [], "recentTrades": []}
    try:
        up_id, down_id = sim._market_tokens(sim.state["market"])
        for label, token_id in (("Up", up_id), ("Down", down_id)):
            snapshot["balances"][label] = await _query_conditional_balance_with_retry(token_id)
    except Exception as exc:
        snapshot["balanceError"] = str(exc)
    try:
        orders = await asyncio.to_thread(live.get_open_orders)
        snapshot["openOrders"] = [
            {
                "id": order.get("id") or order.get("orderID") or order.get("orderId"),
                "status": order.get("status"),
                "tokenId": order.get("asset_id") or order.get("assetId") or order.get("token_id") or order.get("tokenId"),
                "side": order.get("side"),
                "price": order.get("price"),
                "size": order.get("size") or order.get("original_size"),
            }
            for order in (orders or [])
            if isinstance(order, dict)
        ]
    except Exception as exc:
        snapshot["openOrdersError"] = str(exc)
    try:
        trades = await asyncio.to_thread(live.get_trade_history, 20)
        snapshot["recentTrades"] = [
            {
                "id": trade.get("id") or trade.get("trade_id"),
                "takerOrderId": trade.get("taker_order_id") or trade.get("takerOrderId"),
                "tokenId": trade.get("asset_id") or trade.get("assetId"),
                "side": trade.get("side"),
                "price": trade.get("price"),
                "size": trade.get("size"),
                "status": trade.get("status"),
            }
            for trade in (trades or [])
            if isinstance(trade, dict)
        ]
    except Exception as exc:
        snapshot["tradesError"] = str(exc)
    return snapshot


async def _halt_for_unconfirmed(reason: str, order: dict, reconcile: bool = True) -> None:
    order = dict(order)
    # 先停機再查帳，確保唯讀查詢期間不會有其他路徑送出新單。
    _set_halt(reason, order)
    if reconcile:
        order["reconciliation"] = await _current_account_reconciliation()
    live_state["unconfirmedOrder"] = order
    save_live_state()


async def _validate_order_path_once(slug: str) -> bool:
    """Safely prewarm and sign the current market pair once, without POST."""
    global _validated_order_path_slug
    if not live.VALIDATE_ORDER_PATH or _validated_order_path_slug == slug:
        return True

    up_id, down_id = sim._market_tokens(sim.state["market"])
    try:
        result = await asyncio.to_thread(
            live.validate_batch_order_path,
            [up_id, down_id],
        )
    except Exception as exc:
        _set_halt(f"order_path_validation_failed: {exc}")
        return False

    _validated_order_path_slug = slug
    live_state["validationSlug"] = slug
    live_state["validationResult"] = result
    save_live_state()
    return True


def _record_order_action_started() -> None:
    live_state["lastActionAt"] = time.time()
    save_live_state()


async def _resolve_fok_response(response: dict, dry_run: bool) -> tuple[str, dict]:
    """把單筆 FOK 回應歸類；single /order 與 batch /orders 共用同一套確認規則。"""
    if not isinstance(response, dict):
        return "unconfirmed", {"error": f"unexpected_order_response: {response!r}"}
    if live.order_response_filled(response):
        return "filled", response

    status = str(response.get("status", "")).lower()
    terminal_not_filled = {
        "unmatched", "cancelled", "canceled", "rejected", "",
        "order_status_invalid", "order_status_canceled", "order_status_cancelled",
        "order_status_canceled_market_resolved", "order_status_cancelled_market_resolved",
    }
    if status in terminal_not_filled:
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
        if latest_status in terminal_not_filled:
            return "not_filled", latest
    return "unconfirmed", latest


async def _submit_fok(token_id: str, side: str, plan: dict, dry_run: bool) -> tuple[str, dict]:
    _record_order_action_started()
    if not dry_run:
        _invalidate_cash_cache()

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
    except Exception as exc:
        # 把 py_clob_client_v2 的 import 留到真的發生例外時；正常搶單路徑不應為了
        # exception type 做一次可能很慢的首次套件 import。
        from py_clob_client_v2.exceptions import PolyApiException

        if not isinstance(exc, PolyApiException):
            # 連線中斷／timeout 可能發生在伺服器已經收單之後，不能當成沒成交。
            return "unconfirmed", {"error": str(exc), "exceptionType": type(exc).__name__}
        # FOK 沒吃滿（訂單簿在下單瞬間跟決策當下的快照之間變薄了）是正常會發生的情況，
        # 不是程式錯誤——CLOB 直接回 400 而不是回一個帶 status 的訂單物件，用例外表達。
        # 當成跟 status=unmatched 一樣的「這次沒成交」處理，不要整包當未預期例外往外拋。
        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int) and 400 <= status_code < 500 and status_code != 408:
            log.info(f"[LIVE] FOK 被 CLOB 明確拒絕、未成交：{exc}")
            return "not_filled", {"error": str(exc), "statusCode": status_code}
        return "unconfirmed", {"error": str(exc), "statusCode": status_code}
    return await _resolve_fok_response(response, dry_run)


async def _submit_fok_pair(
    up_token: str,
    up: dict,
    down_token: str,
    down: dict,
    dry_run: bool,
) -> tuple[tuple[str, dict], tuple[str, dict]]:
    """兩腿先簽名，再以同一次 POST /orders 送達 CLOB。

    Batch 只縮小兩腿的客戶端／網路到達差，不提供跨訂單原子性，因此仍逐腿分類結果，
    讓既有的補鎖利與緊急平倉邏輯接手單腿成交情況。
    """
    _record_order_action_started()
    if not dry_run:
        _invalidate_cash_cache()

    orders = [
        {
            "token_id": up_token,
            "side": "BUY",
            "price": up["limitPrice"],
            "size": up["shares"],
        },
        {
            "token_id": down_token,
            "side": "BUY",
            "price": down["limitPrice"],
            "size": down["shares"],
        },
    ]
    try:
        responses = await asyncio.to_thread(
            live.place_limit_orders_batch,
            orders,
            dry_run,
            "FOK",
            not dry_run,
        )
    except Exception as exc:
        # 整個 batch 沒有逐腿回應時，不能假設兩腿都沒成交；網路斷線、5xx 或 SDK
        # 例外都可能發生在伺服器已收單之後。標成 unconfirmed 會讓上層 halt，避免重送。
        error = {"error": str(exc), "batch": True}
        log.error(f"[LIVE] FOK batch 結果不明：{exc}", exc_info=True)
        return ("unconfirmed", error), ("unconfirmed", error)

    if len(responses) != 2:
        error = {"error": f"unexpected_batch_response_count={len(responses)}", "batch": True}
        return ("unconfirmed", error), ("unconfirmed", error)

    up_result, down_result = await asyncio.gather(
        _resolve_fok_response(responses[0], dry_run),
        _resolve_fok_response(responses[1], dry_run),
    )
    return up_result, down_result


def _token_id(side: str) -> str:
    up_id, down_id = sim._market_tokens(sim.state["market"])
    return up_id if side == "Up" else down_id


def _build_position_dict(slug: str, plan: dict, response: dict, execution: dict, dry_run: bool) -> dict:
    """建立單腿部位字典（尚未對沖）。從 _enter_position 抽出來，讓兩腿 batch 送出時
    （見 _execute_direct_pair）不管哪一腿成交都能重用同一份邏輯建立部位，不用
    另外寫一份容易長歪。"""
    return {
        "windowSlug": slug,
        "side": plan["side"],
        "tokenId": _token_id(plan["side"]),
        "shares": execution["shares"],
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
        "signalSource": plan.get("_signalSource"),
        "signalDeltaPct": plan.get("_deltaPct"),
        "signalObservedAt": plan.get("_signalObservedAt"),
        "signalAgeSeconds": plan.get("_signalAgeSeconds"),
        "entryTime": time.time(),
        "entryOrderId": response.get("orderID") or response.get("orderId"),
        "entryTradeIds": list(response.get("tradeIDs") or response.get("associate_trades") or []),
        "entryTransactionHashes": list(response.get("transactionsHashes") or []),
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


def _warn_if_shares_corrected(label: str, plan: dict, execution: dict, dry_run: bool) -> None:
    if not dry_run and abs(execution["shares"] - plan["shares"]) > max(1e-6, plan["shares"] * 0.001):
        log.warning(
            f"[LIVE] {label}真實成交股數（{execution['shares']:.6f}）校正了規劃股數"
            f"（{plan['shares']:.6f}）——部位追蹤改用真實股數。"
        )


async def _enter_position(slug: str, plan: dict, dry_run: bool) -> str:
    if plan.get("_signalSource"):
        selected_snapshot = {
            "quoteSource": plan.get("_bookQuoteSource"),
            "receivedAtMonotonic": plan.get("_bookReceivedAtMonotonic"),
        }
        if not _live_direction_book_is_fresh(plan["side"], selected_snapshot):
            record_live_window_diagnostic(
                slug,
                "direction_book_stale_at_submit",
                selectedSide=plan["side"],
                dataGuardReason=sim._simulation_single_book_guard_reason(selected_snapshot),
            )
            return "not_filled"
    elif not dry_run:
        up_book, down_book = sim.state.get("upBook"), sim.state.get("downBook")
        if not up_book or not down_book or not _live_books_are_coherent(up_book, down_book):
            return "not_filled"
    token_id = _token_id(plan["side"])
    result, response = await _submit_fok(token_id, "BUY", plan, dry_run)
    record_live_window_diagnostic(
        slug,
        f"entry_order_{result}",
        status="entry_submitted" if result != "filled" else "entered",
        entryMode="DRY-RUN" if dry_run else "REAL",
        entrySide=plan["side"],
        entryLimitPrice=plan["limitPrice"],
        entryShares=plan["shares"],
        orderStatus=response.get("status"),
        orderError=response.get("error") or response.get("errorMsg"),
    )
    if result == "unconfirmed":
        await _halt_for_unconfirmed(
            f"entry_order_unconfirmed side={plan['side']}",
            {"orderID": response.get("orderID") or response.get("orderId"), "side": plan["side"], "tokenId": token_id},
            reconcile=not dry_run,
        )
        return result
    if result != "filled":
        log.info(f"[LIVE] {plan['side']} FOK 未成交，不建立持倉")
        return result

    execution = await _resolved_execution(plan, response, dry_run)
    _warn_if_shares_corrected("進場", plan, execution, dry_run)

    live_state["position"] = _build_position_dict(slug, plan, response, execution, dry_run)
    diagnostic = _live_window_diagnostic(slug)
    diagnostic["entryCount"] = int(diagnostic.get("entryCount", 0)) + 1
    save_live_state()
    tag = "DRY-RUN" if dry_run else "REAL"
    log.warning(
        f"[LIVE][{tag}] 進場 {plan['side']} limit=${plan['limitPrice']:.3f} "
        f"shares={execution['shares']:.2f} edge={plan.get('edge') if plan.get('edge') is not None else float('nan'):+.4f}"
    )
    return result


def _apply_hedge_fields(pos: dict, plan: dict, response: dict, execution: dict) -> None:
    """把補鎖利那一腿的成交結果套進既有部位、計算保守鎖利估計，兩腿真實股數對不上時
    記錄殘值——依序版 _hedge_position 跟 _execute_direct_pair batch 送出剛好兩腿都成交
    的情況共用這份邏輯，不用各寫一份容易長歪。"""
    pos["hedged"] = True
    pos["hedgeSide"] = plan["side"]
    pos["hedgeTokenId"] = _token_id(plan["side"])
    pos["hedgeShares"] = execution["shares"]
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

    # 2026-09：進場跟補鎖利兩腿的真實成交股數各自可能跟規劃不同（見 _resolved_execution），
    # 兩腿彼此也可能對不上。保證會贏、不管哪邊贏都拿得到的股數是兩腿的較小值——多出來的
    # 那一小段其實還是方向性曝險，沒有真的被鎖住。這裡不自動再下單去補平（見
    # EMERGENCY_UNWIND 的教訓：同一個交易所上再下一筆單，同樣的多／少成交問題可能再發生
    # 一次），只留清楚的紀錄跟警告，讓人決定要不要手動處理。
    min_shares = min(float(pos["shares"]), float(pos["hedgeShares"]))
    if abs(float(pos["shares"]) - float(pos["hedgeShares"])) > max(1e-6, min_shares * 0.001):
        pos["shareMismatch"] = {
            "entryShares": pos["shares"],
            "hedgeShares": pos["hedgeShares"],
            "unhedgedResidual": round(abs(float(pos["shares"]) - float(pos["hedgeShares"])), 6),
        }
        log.warning(
            f"[LIVE] 兩腿真實成交股數對不上：進場 {pos['shares']:.6f} 股 vs 補鎖利 "
            f"{pos['hedgeShares']:.6f} 股，殘值 {pos['shareMismatch']['unhedgedResidual']:.6f} 股"
            "仍是方向性曝險，沒有自動處理，需要人工核對。"
        )
    pos["lockedPnlEstimate"] = min_shares - _position_paid_cost(pos)
    pos["lockedPnlWorstCase"] = min_shares - _position_risk_cost(pos)


async def _hedge_position(plan: dict, dry_run: bool) -> str:
    pos = live_state["position"]
    token_id = _token_id(plan["side"])
    result, response = await _submit_fok(token_id, "BUY", plan, dry_run)
    record_live_window_diagnostic(
        pos["windowSlug"],
        f"hedge_order_{result}",
        hedgeMode="DRY-RUN" if dry_run else "REAL",
        hedgeSide=plan["side"],
        hedgeLimitPrice=plan["limitPrice"],
        hedgeShares=plan["shares"],
        orderStatus=response.get("status"),
        orderError=response.get("error") or response.get("errorMsg"),
    )
    if result == "unconfirmed":
        await _halt_for_unconfirmed(
            f"hedge_order_unconfirmed side={plan['side']}",
            {"orderID": response.get("orderID") or response.get("orderId"), "side": plan["side"], "tokenId": token_id},
            reconcile=not dry_run,
        )
        return result
    if result != "filled":
        log.error(f"[LIVE] 第二腿 {plan['side']} FOK 未成交，依然是單邊曝險")
        return result

    execution = await _resolved_execution(plan, response, dry_run)
    _apply_hedge_fields(pos, plan, response, execution)
    diagnostic = _live_window_diagnostic(pos["windowSlug"])
    diagnostic["status"] = "entered_locked"
    diagnostic["hedgeCount"] = int(diagnostic.get("hedgeCount", 0)) + 1
    diagnostic["lockedPnlEstimate"] = pos.get("lockedPnlEstimate")
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
        await _halt_for_unconfirmed(
            f"exit_order_unconfirmed reason={reason}",
            {"orderID": response.get("orderID") or response.get("orderId"), "side": "SELL", "tokenId": pos["tokenId"]},
            reconcile=not dry_run,
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
    sold_shares = execution["shares"]
    intended_shares = float(plan["shares"])
    if not dry_run and sold_shares < intended_shares - max(1e-6, intended_shares * 0.001):
        log.warning(
            f"[LIVE] 出場真實成交股數（{sold_shares:.6f}）少於原本持有股數"
            f"（{intended_shares:.6f}），帳上可能還留著 {intended_shares - sold_shares:.6f} "
            "股沒賣掉，需要人工核對錢包餘額。"
        )
    live_state["position"] = None
    _record_trade(pos, pnl, "EarlyExit", "early_exit")
    tag = "DRY-RUN" if dry_run else "REAL"
    log.warning(f"[LIVE][{tag}] 提早退出 {pos['side']} 保守淨損益=${pnl:+.2f} reason={reason}")
    return result


async def _emergency_unwind(session: aiohttp.ClientSession, reason: str) -> None:
    """等待首腿可交割後，持續依最新 bid 重算並嘗試強制平倉。"""
    pos = live_state.get("position")
    if not pos or pos.get("hedged"):
        return
    if pos.get("dryRun", True):
        latest_book = await sim._get_book_ws_or_rest(session, pos["tokenId"])
        plan = _aggressive_sell_plan(pos["side"], latest_book, pos["shares"])
        if plan:
            await _close_position(plan, True, reason)
        return

    deadline = time.monotonic() + EMERGENCY_UNWIND_WAIT_SECONDS
    next_order_at = 0.0
    polls = 0
    attempts = 0
    balance = 0.0
    pos["emergencyUnwindPending"] = True
    pos["emergencyUnwind"] = {
        "reason": reason,
        "startedAt": time.time(),
        "waitSeconds": EMERGENCY_UNWIND_WAIT_SECONDS,
        "polls": 0,
        "orderAttempts": 0,
        "lastBalance": 0.0,
        "tradeStatuses": [],
    }
    save_live_state()

    while True:
        pos = live_state.get("position")
        if not pos or pos.get("hedged"):
            return
        polls += 1
        diagnostic = pos.setdefault("emergencyUnwind", {})
        diagnostic["polls"] = polls

        try:
            balance = await asyncio.to_thread(live.refresh_conditional_balance, pos["tokenId"])
            diagnostic["lastBalance"] = balance
            diagnostic.pop("lastBalanceError", None)
        except Exception as exc:
            balance = 0.0
            diagnostic["lastBalanceError"] = str(exc)

        try:
            order_ref = {
                "orderID": pos.get("entryOrderId"),
                "tradeIDs": pos.get("entryTradeIds") or [],
            }
            diagnostic["tradeStatuses"] = await asyncio.to_thread(
                live.get_order_trade_statuses, order_ref
            )
            diagnostic.pop("tradeStatusError", None)
        except Exception as exc:
            diagnostic["tradeStatusError"] = str(exc)

        target_shares = float(pos["shares"])
        tolerance = max(1e-6, target_shares * 0.001)
        now = time.monotonic()
        if balance + tolerance >= target_shares and now >= next_order_at:
            try:
                latest_book = await sim._get_book_ws_or_rest(session, pos["tokenId"])
                diagnostic.pop("lastBookError", None)
            except Exception as exc:
                diagnostic["lastBookError"] = str(exc)
            else:
                plan = _aggressive_sell_plan(pos["side"], latest_book, target_shares)
                if plan:
                    attempts += 1
                    diagnostic["orderAttempts"] = attempts
                    diagnostic["lastLimitPrice"] = plan["limitPrice"]
                    result = await _close_position(plan, False, reason)
                    if result in ("filled", "unconfirmed"):
                        return
                    next_order_at = time.monotonic() + EMERGENCY_UNWIND_ORDER_INTERVAL

        save_live_state()
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(EMERGENCY_UNWIND_POLL_INTERVAL)

    pos = live_state.get("position")
    if pos and not pos.get("hedged"):
        diagnostic = pos.setdefault("emergencyUnwind", {})
        diagnostic["timedOutAt"] = time.time()
        diagnostic["lastBalance"] = balance
        save_live_state()
        log.error(
            f"[LIVE] 緊急平倉等待 {EMERGENCY_UNWIND_WAIT_SECONDS:.1f}s 仍未完成："
            f"token balance={balance:.6f}/{float(pos['shares']):.6f}，"
            "保留救援狀態供下一個 tick 繼續"
        )


async def _retry_failed_leg_once(session: aiohttp.ClientSession, failed_side: str, dry_run: bool) -> str:
    """Batch 送出兩腿、其中一腿沒成交時，先用最新訂單簿重試一次這一腿，成功就直接
    變成完整鎖利，不用退而求其次緊急平倉。2026-09：實測那一瞬間的失敗常常只是被
    搶走那一口深度，市場一兩秒內多半就恢復了，值得立刻補一次而不是馬上放棄。
    跟正常補鎖利路徑（evaluate_and_act／_on_ws_tick_sync）用同一套鎖利門檻判斷，
    避免為了搶救單邊曝險而硬鎖一個實際上不划算的價位。

    回傳 "filled"／"not_filled"／"unconfirmed"：unconfirmed 代表 _hedge_position
    內部已經觸發 halt（下單結果不明，可能已經成交也可能沒有），呼叫端不該接著再對
    第一腿送緊急平倉——那樣萬一重試那腿其實有成交，就會變成三邊曝險，比不動作更糟。"""
    pos = live_state["position"]
    try:
        latest_book = await sim._get_book_ws_or_rest(session, _token_id(failed_side))
    except Exception as exc:
        log.warning(f"[LIVE] 補鎖利重試前無法取得 {failed_side} 訂單簿：{exc}")
        return "not_filled"
    hedge = _buy_plan(failed_side, latest_book, pos["shares"])
    if not hedge:
        log.info(f"[LIVE] 補鎖利重試：{failed_side} 目前沒有足夠深度，放棄重試")
        return "not_filled"
    projected_cost = _position_risk_cost(pos) + hedge["riskNotional"] + hedge["fee"]
    net_per_share = (pos["shares"] - projected_cost) / pos["shares"]
    cash = await _strategy_cash(dry_run)
    if not (
        float(pos.get("entryLimitPrice", pos["entryPrice"])) + hedge["limitPrice"] <= RESCUE_LOCK_MAX_SUM
        and net_per_share >= sim.SIM_MIN_NET_LOCK_PER_SHARE
        and hedge["riskNotional"] + hedge["fee"] <= cash
    ):
        log.info(
            f"[LIVE] 補鎖利重試：{failed_side} 新報價 limit=${hedge['limitPrice']:.3f} "
            "已經不划算，放棄重試"
        )
        return "not_filled"
    return await _hedge_position(hedge, dry_run)


async def _execute_direct_pair(
    session: aiohttp.ClientSession,
    slug: str,
    up: dict,
    down: dict,
    fair: dict | None,
    dry_run: bool,
) -> None:
    if not dry_run and not _live_books_are_coherent(up["book"], down["book"]):
        record_live_window_diagnostic(
            slug,
            "pair_books_stale_at_submit",
            dataGuardReason=_live_book_guard_reason(up["book"], down["book"]),
        )
        return
    if fair:
        for plan in (up, down):
            fair_side = fair["fairUp"] if plan["side"] == "Up" else fair["fairDown"]
            plan["fair"] = fair_side
            plan["edge"] = fair_side - (plan["riskNotional"] + plan["fee"]) / plan["shares"]

    # 2026-09：兩腿使用官方 POST /orders 放在同一個 HTTP request；CLOB 會平行處理
    # batch 內容。這比兩個 thread 各送 POST /order 少掉執行緒排程、兩條 HTTP/2 stream
    # 與 request 到達時間差。Batch 仍不是原子交易，兩筆回應要分開判斷；單腿成交時
    # 繼續沿用下方的補鎖利／緊急平倉救援。
    up_token = _token_id("Up")
    down_token = _token_id("Down")
    record_live_window_diagnostic(
        slug,
        "pair_batch_submitted",
        status="pair_submitted",
        submissionMode="DRY-RUN" if dry_run else "REAL",
        pairedShares=min(float(up["shares"]), float(down["shares"])),
        pairDecisionSum=float(up["limitPrice"]) + float(down["limitPrice"]),
        upLimitPrice=up["limitPrice"],
        downLimitPrice=down["limitPrice"],
    )
    (up_result, up_response), (down_result, down_response) = await _submit_fok_pair(
        up_token,
        up,
        down_token,
        down,
        dry_run,
    )
    record_live_window_diagnostic(
        slug,
        "pair_batch_result",
        upResult=up_result,
        downResult=down_result,
        upOrderStatus=up_response.get("status"),
        downOrderStatus=down_response.get("status"),
        upOrderError=up_response.get("error") or up_response.get("errorMsg"),
        downOrderError=down_response.get("error") or down_response.get("errorMsg"),
    )

    unconfirmed_legs = [
        (side, response, token_id)
        for side, result, response, token_id in (
            ("Up", up_result, up_response, up_token),
            ("Down", down_result, down_response, down_token),
        )
        if result == "unconfirmed"
    ]
    if unconfirmed_legs:
        record_live_window_diagnostic(
            slug, "pair_batch_unconfirmed", status="halted_unconfirmed"
        )
        # 若另一腿已明確 matched，必須先留下已知持倉，不能讓狀態頁顯示空倉。
        known_filled = None
        if up_result == "filled":
            known_filled = (up, up_response)
        elif down_result == "filled":
            known_filled = (down, down_response)
        if known_filled is not None:
            filled_plan, filled_response = known_filled
            execution = await _resolved_execution(filled_plan, filled_response, dry_run)
            pos = _build_position_dict(slug, filled_plan, filled_response, execution, dry_run)
            pos["reconciliationRequired"] = True
            pos["unknownLegs"] = [side for side, _response, _token_id_value in unconfirmed_legs]
            live_state["position"] = pos
            save_live_state()

        details = {
            "batch": True,
            "legs": [
                {
                    "side": side,
                    "result": result,
                    "orderID": response.get("orderID") or response.get("orderId"),
                    "tokenId": token_id,
                    "error": response.get("error"),
                }
                for side, result, response, token_id in (
                    ("Up", up_result, up_response, up_token),
                    ("Down", down_result, down_response, down_token),
                )
            ],
        }
        await _halt_for_unconfirmed(
            "direct_pair_batch_unconfirmed",
            details,
            reconcile=not dry_run,
        )
        return

    up_filled = up_result == "filled"
    down_filled = down_result == "filled"

    if not up_filled and not down_filled:
        record_live_window_diagnostic(slug, "pair_batch_not_filled", status="observing")
        log.info("[LIVE] 兩腿 batch 皆未成交，不建立持倉")
        return

    if up_filled and down_filled:
        up_execution = await _resolved_execution(up, up_response, dry_run)
        down_execution = await _resolved_execution(down, down_response, dry_run)
        _warn_if_shares_corrected("進場", up, up_execution, dry_run)
        _warn_if_shares_corrected("補鎖利", down, down_execution, dry_run)

        pos = _build_position_dict(slug, up, up_response, up_execution, dry_run)
        live_state["position"] = pos
        _apply_hedge_fields(pos, down, down_response, down_execution)
        diagnostic = record_live_window_diagnostic(
            slug,
            "pair_batch_filled",
            status="entered_locked",
            lockedPnlEstimate=pos.get("lockedPnlEstimate"),
        )
        diagnostic["entryCount"] = int(diagnostic.get("entryCount", 0)) + 1
        diagnostic["hedgeCount"] = int(diagnostic.get("hedgeCount", 0)) + 1
        save_live_state()
        tag = "DRY-RUN" if dry_run else "REAL"
        log.warning(
            f"[LIVE][{tag}] batch 鎖利 {up['side']}+{down['side']} "
            f"保守淨鎖利估計=${pos['lockedPnlEstimate']:+.2f}"
        )
        return

    filled_side, filled_plan, filled_response = (
        ("Up", up, up_response) if up_filled else ("Down", down, down_response)
    )
    failed_side = "Down" if filled_side == "Up" else "Up"
    execution = await _resolved_execution(filled_plan, filled_response, dry_run)
    _warn_if_shares_corrected("進場", filled_plan, execution, dry_run)

    live_state["position"] = _build_position_dict(slug, filled_plan, filled_response, execution, dry_run)
    diagnostic = record_live_window_diagnostic(
        slug,
        "pair_batch_single_leg_filled",
        status="single_leg_exposure",
        filledSide=filled_side,
        failedSide=failed_side,
        filledLimitPrice=filled_plan["limitPrice"],
        filledShares=execution["shares"],
    )
    diagnostic["entryCount"] = int(diagnostic.get("entryCount", 0)) + 1
    save_live_state()
    tag = "DRY-RUN" if dry_run else "REAL"
    log.warning(
        f"[LIVE][{tag}] batch 只有 {filled_side} 成交，先重試 {failed_side} 一次 "
        f"limit=${filled_plan['limitPrice']:.3f} shares={execution['shares']:.2f}"
    )

    retry_result = await _retry_failed_leg_once(session, failed_side, dry_run)
    if retry_result == "filled":
        log.warning(f"[LIVE][{tag}] 補鎖利重試成功，{failed_side} 補上鎖利")
        return
    if retry_result == "unconfirmed":
        # _hedge_position 內部已經觸發 halt；重試結果不明，不能再對第一腿送緊急平倉。
        return

    await _emergency_unwind(session, "direct_pair_batch_single_leg_filled")


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
    finalize_live_window_diagnostic(slug)
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
    allow_early_exit: bool = True,
) -> None:
    with _decision_evaluation(slug, "poll", sim.state.get("upBook") or {},
                              sim.state.get("downBook") or {}, remaining_seconds):
        await _evaluate_and_act_impl(slug, session, remaining_seconds, fair, allow_early_exit)


def _decision_evaluation(slug, source, up, down, remaining):
    return sim.decision_evaluation("REAL" if REAL_EXECUTION_ENABLED else "DRY-RUN",
        LIVE_VARIANT_ID, slug, source, up, down, remaining,
        {"lockMaxSum": LOCK_MAX_SUM, "lateDirectionMaxPrice": LATE_DIRECTION_MAX_PRICE,
         "stakePct": STAKE_PCT, "minDepthMultiplier": PAIR_MIN_DEPTH_MULTIPLIER,
         "stabilitySeconds": PAIR_STABILITY_SECONDS, "halted": live_state.get("halted"),
         "positionPresent": live_state.get("position") is not None,
         "lastActionAt": live_state.get("lastActionAt"),
         "actionCooldownSeconds": ACTION_COOLDOWN_SECONDS})


async def _evaluate_and_act_impl(
    slug: str,
    session: aiohttp.ClientSession,
    remaining_seconds: float | None,
    fair: dict | None,
    allow_early_exit: bool = True,
) -> None:
    if live_state.get("halted"):
        record_live_window_diagnostic(
            slug, "strategy_halted", haltReason=live_state.get("haltReason")
        )
        return

    up_book, down_book = sim.state["upBook"], sim.state["downBook"]
    record_live_window_observation(slug, up_book, down_book, remaining_seconds, "poll")
    sim.log_price_sum_diagnostic("live-btc", up_book, down_book, LOCK_MAX_SUM)
    pos = live_state.get("position")

    if pos is None:
        if live.VALIDATE_ORDER_PATH and not await _validate_order_path_once(slug):
            return

        if time.time() - float(live_state.get("lastActionAt", 0)) < ACTION_COOLDOWN_SECONDS:
            return
        # 90 秒門檻已經拿掉：鎖利（下面的 direct pair）本身是即時原子成交，不需要
        # 留時間緩衝；晚進場方向性更是刻意只在剩不到 10 秒才動作，跟舊的 90 秒門檻
        # 完全衝突，所以只保留「還沒結算」這個最基本的條件。
        if remaining_seconds is None or remaining_seconds <= 0:
            return
        dry_run = not REAL_EXECUTION_ENABLED
        if not dry_run and live_state.get("preflightSlug") != slug:
            if not await _ensure_no_unmanaged_current_position():
                return
            live_state["preflightSlug"] = slug
            save_live_state()
        if not dry_run:
            up_id, down_id = sim._market_tokens(sim.state["market"])
            if not live.order_tokens_and_fees_are_warm([up_id, down_id]):
                try:
                    # preflightSlug 會寫入磁碟；若程式在同一窗口重啟，它可能已經是目前 slug，
                    # 但 SDK 的記憶體快取已清空，所以仍要獨立確認這個進程真的完成預熱。
                    condition_id = _market_condition_id()
                    if not condition_id:
                        raise RuntimeError("current market is missing conditionId")
                    await asyncio.to_thread(live.prewarm_order_tokens, [up_id, down_id], condition_id)
                except Exception as exc:
                    _set_halt(f"order_warmup_failed: {exc}")
                    return
        cash = await _strategy_cash(dry_run)
        shares, budget = _target_pair_order(cash)
        if budget < 1.0 or shares < 1.0:
            return

        if DIRECT_PAIR_ENABLED:
            # 股數先按可見深度封頂；純方向性模式完全略過這段，不會先建立鎖利部位。
            direct = None
            paired_shares = 0.0
            if _live_books_are_coherent(up_book, down_book):
                depth_fraction = min(sim.SIM_DEPTH_CAP_FRACTION, 1.0 / PAIR_MIN_DEPTH_MULTIPLIER)
                depth_cap = min(_ask_depth(up_book), _ask_depth(down_book)) * depth_fraction
                paired_shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
                direct = (
                    _direct_pair_plans(
                        up_book, down_book, paired_shares, cash, diagnostic_slug=slug
                    )
                    if paired_shares >= 1.0
                    else None
                )
                if paired_shares < 1.0:
                    record_live_window_diagnostic(
                        slug,
                        "pair_no_common_depth",
                        targetShares=shares,
                        pairedShares=paired_shares,
                        upAskDepth=_ask_depth(up_book),
                        downAskDepth=_ask_depth(down_book),
                    )
            else:
                record_live_window_diagnostic(
                    slug,
                    "pair_books_not_coherent",
                    dataGuardReason=_live_book_guard_reason(up_book, down_book),
                )
            if direct:
                if not sim.pair_candidate_is_stable(
                    "live:pair",
                    slug,
                    paired_shares,
                    direct[0]["limitPrice"],
                    direct[1]["limitPrice"],
                    PAIR_STABILITY_SECONDS,
                ):
                    record_live_window_diagnostic(
                        slug,
                        "pair_stability_wait",
                        pairedShares=paired_shares,
                        stabilitySeconds=PAIR_STABILITY_SECONDS,
                    )
                    return
                sim.clear_pair_candidate("live:pair")
                await _execute_direct_pair(session, slug, direct[0], direct[1], fair, dry_run)
                return
            sim.clear_pair_candidate("live:pair")
        if ENABLE_LATE_DIRECTION:
            await _try_late_direction_entry(slug, up_book, down_book, remaining_seconds, shares, dry_run)
        return

    if pos.get("hedged") or pos.get("windowSlug") != slug:
        return
    if not pos.get("dryRun", True) and not REAL_EXECUTION_ENABLED:
        log.error("[LIVE] 存在真實持倉，但真實策略未完整武裝；本程式不會假裝已對沖")
        return
    if pos.get("emergencyUnwindPending"):
        await _emergency_unwind(session, pos.get("emergencyUnwind", {}).get("reason", "resume_emergency_unwind"))
        return
    if pos.get("strategy") == "late_direction":
        # 晚進場方向性進場後就抱到結算，不補鎖利、不提早出場——道理跟 sim 那邊一樣：
        # 進場當下對邊常常正好夠便宜可以「鎖利」，但那樣等於把方向性優勢換成極小的
        # 鎖利價差，違背了這條路存在的目的。
        return

    dry_run = bool(pos.get("dryRun", True))
    other_side = "Down" if pos["side"] == "Up" else "Up"
    other_book = down_book if other_side == "Down" else up_book
    hedge = _buy_plan(other_side, other_book, pos["shares"])
    if hedge:
        projected_cost = _position_risk_cost(pos) + hedge["riskNotional"] + hedge["fee"]
        net_per_share = (pos["shares"] - projected_cost) / pos["shares"]
        cash = await _strategy_cash(dry_run)
        # 跟模擬版 sim.simulate_trading 的補鎖利判斷對齊：只比對 cash 本身，不再扣
        # MIN_CASH_RESERVE_USD——理由同 _direct_pair_plans，避免保留額重複扣兩次。
        if (
            float(pos.get("entryLimitPrice", pos["entryPrice"])) + hedge["limitPrice"] <= RESCUE_LOCK_MAX_SUM
            and net_per_share >= sim.SIM_MIN_NET_LOCK_PER_SHARE
            and hedge["riskNotional"] + hedge["fee"] <= cash
        ):
            await _hedge_position(hedge, dry_run)
            return

    # 跟模擬版一樣，停損（提早退出）判斷刻意只在 3 秒輪詢節奏下檢查（allow_early_exit=False
    # 時整段跳過）——WS 觸發的即時評估拿到的是薄訂單簿當下那一瞬間算出來的可賣價，波動本來
    # 就大，同一個瞬間閾值判斷用高頻率去採樣很容易把雜訊當成訊號。補鎖利留在即時路徑是因為
    # 那邊抓的是「機會」，錯過了就沒有；停損不一樣，真的行情反轉的話，3 秒後再確認一次
    # 幾乎不會有差別，但可以濾掉大部分薄 book 瞬間跳動造成的誤判。
    if allow_early_exit and fair:
        held_book = up_book if pos["side"] == "Up" else down_book
        exit_plan = _sell_plan(pos["side"], held_book, pos["shares"])
        if exit_plan:
            fair_side = fair["fairUp"] if pos["side"] == "Up" else fair["fairDown"]
            minimum_liquidation = exit_plan["riskNotional"] - exit_plan["fee"]
            expected_hold = pos["shares"] * fair_side
            if minimum_liquidation >= expected_hold + pos["shares"] * sim.SIM_EXIT_EDGE:
                await _close_position(exit_plan, dry_run, "market_bid_above_model_value")


def _set_quote_status(source: str) -> None:
    status = sim.ws_feed_status()
    previous = live_state.get("quoteSource")
    previous_connected = bool(live_state.get("wsConnected"))
    live_state["quoteSource"] = source
    live_state["wsConnected"] = bool(status["connected"])
    if source != previous or bool(status["connected"]) != previous_connected:
        log.info(
            f"[QUOTE] source={source} ws_connected={status['connected']} "
            f"subscribed_tokens={status['subscribedTokens']}"
        )
        save_live_state()


# 只給 _on_ws_tick_sync 用的「已經排了一筆動作、還沒真的開始執行」防抖旗標。
#
# 背景：舊版 _evaluate_ws_tick 整段包成 async 函式，靠 asyncio.create_task 排程執行——
# 但 create_task 只是「排進事件迴圈稍後跑」，不是「現在立刻跑」。鎖利機會常常只存在
# 一兩秒，如果那個瞬間事件迴圈剛好在忙（例如同時有好幾個資產的 WS tick 湧進來），這筆
# 排程可能要等到報價已經又變了才真的開始判斷——模擬盤是在收到 WS 訊息當下同步、立即
# 判斷，完全沒有這個排程空窗，所以會出現「模擬盤鎖到了，實盤這裡看到的還是舊報價」的
# 落差。實測：模擬盤鎖到的那個時間點附近，這裡的即時診斷 log 顯示的還是好幾秒前的舊
# price_sum，就是這個排程延遲造成的。
#
# 修法：判斷本身（讀報價、算 price_sum、決定要不要進場/補鎖利）改成純同步、零延遲執行，
# 跟模擬盤自己的 WS tick 處理站在同一個起跑點；只有真的決定要送單（會動用網路 I/O）
# 才用 create_task 切到 async。這個旗標存在的唯一理由：sync 判斷跟「送單 task 真的開始
# 執行、拿到 decision_lock」之間還是有一個排程空窗，這段空窗內 decision_lock.locked()
# 還是 False，旗標用來擋住這段空窗內重複判斷、重複排程同一筆動作。
_ws_action_in_flight = {"v": False}


def _on_ws_tick_sync(token_id: str, session: aiohttp.ClientSession, decision_lock: asyncio.Lock) -> bool:
    market = sim.state.get("market") or {}
    up_id, down_id = sim._market_tokens(market)
    if token_id not in (up_id, down_id):
        return False
    books = (sim._ws_get_book(up_id), sim._ws_get_book(down_id))
    remaining = max(0.0, float(sim.state.get("windowEndsAt") or 0) / 1000 - sim.real_now())
    with _decision_evaluation(market.get("slug"), sim.decision_diag.trigger.get(),
            books[0] or {}, books[1] or {}, remaining):
        _on_ws_tick_sync_impl(token_id, session, decision_lock, books)
    return bool(_ws_action_in_flight["v"] or decision_lock.locked())


def _on_ws_tick_sync_impl(token_id: str, session: aiohttp.ClientSession, decision_lock: asyncio.Lock, books) -> None:
    """同步、零延遲版本，取代舊版 _evaluate_ws_tick——見上面 _ws_action_in_flight 的說明。"""
    if live_state.get("halted"):
        return
    market = sim.state.get("market")
    if not market:
        return
    up_id, down_id = sim._market_tokens(market)
    if token_id not in (up_id, down_id):
        return
    up_book, down_book = books
    if up_book is None or down_book is None:
        record_live_window_diagnostic(market["slug"], "missing_ws_book",
                                      upBookPresent=up_book is not None,
                                      downBookPresent=down_book is not None)
        return
    # 跟模擬盤自己的即時判斷（_on_ws_price_tick）對齊：只要求兩邊都有書可用，不額外
    # 要求 quoteSource 一定要是 "websocket"。以前這裡多這條件，但 WS 每次斷線重連時
    # _ws_snapshot_tokens 會被整批清空（見 polymarket_server.py market_ws_loop），
    # 14 個 token 要一個一個等新的 book 快照回來才會變回 "websocket"——這段真空期正好
    # 是模擬盤照樣抓得到（用的還是重連前的舊書，is not None 就夠了），實盤這裡卻因為
    # 多這條件整段跳過，錯過機會的原因之一。真實案例：模擬盤鎖到 BTC 的前 3 秒，log
    # 裡剛好有一次「slow consumer」斷線重連紀錄。
    if up_book.get("quoteSource") != "websocket" or down_book.get("quoteSource") != "websocket":
        source_for_status = "rest_fallback"
    else:
        source_for_status = "websocket"

    sim.state["upBook"], sim.state["downBook"] = up_book, down_book
    if up_book["bids"] and up_book["asks"]:
        sim.state["upPrice"] = (up_book["bids"][0]["price"] + up_book["asks"][0]["price"]) / 2
    if down_book["bids"] and down_book["asks"]:
        sim.state["downPrice"] = (down_book["bids"][0]["price"] + down_book["asks"][0]["price"]) / 2
    slug = market["slug"]
    remaining = max(0.0, sim.state["windowEndsAt"] / 1000 - sim.real_now())
    fair = sim.estimate_fair_up(LIVE_ASSET_ID)
    _set_quote_status(source_for_status)
    record_live_window_observation(slug, up_book, down_book, remaining, "ws")

    if decision_lock.locked() or _ws_action_in_flight["v"]:
        record_live_window_diagnostic(slug, "decision_busy",
            decisionLocked=decision_lock.locked(), actionInFlight=_ws_action_in_flight["v"])
        return

    pos = live_state.get("position")

    if pos is None:
        if time.time() - float(live_state.get("lastActionAt", 0)) < ACTION_COOLDOWN_SECONDS:
            record_live_window_diagnostic(slug, "action_cooldown")
            return
        if remaining <= 0:
            return
        dry_run = not REAL_EXECUTION_ENABLED
        if not dry_run and live_state.get("preflightSlug") != slug:
            # 真實模式每個窗口第一次要做的 preflight 檢查需要真的等網路 I/O，不屬於這條
            # 零延遲路徑該做的事，留給 3 秒輪詢那條路（原本的 evaluate_and_act）處理。
            return
        if not dry_run and not live.order_tokens_and_fees_are_warm([up_id, down_id]):
            # 程式重啟會清空 SDK 的記憶體快取；預熱完成前不從 WS 快速路徑搶單，改由
            # 3 秒輪詢路徑在背景完成預熱後再開放。
            return
        cash = _strategy_cash_sync(dry_run)
        if cash is None:
            return
        shares, budget = _target_pair_order(cash)
        if budget < 1.0 or shares < 1.0:
            return

        if DIRECT_PAIR_ENABLED:
            direct = None
            paired_shares = 0.0
            if _live_books_are_coherent(up_book, down_book):
                depth_fraction = min(sim.SIM_DEPTH_CAP_FRACTION, 1.0 / PAIR_MIN_DEPTH_MULTIPLIER)
                depth_cap = min(_ask_depth(up_book), _ask_depth(down_book)) * depth_fraction
                paired_shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
                direct = (
                    _direct_pair_plans(
                        up_book, down_book, paired_shares, cash, diagnostic_slug=slug
                    )
                    if paired_shares >= 1.0
                    else None
                )
                if paired_shares < 1.0:
                    record_live_window_diagnostic(
                        slug,
                        "pair_no_common_depth",
                        targetShares=shares,
                        pairedShares=paired_shares,
                        upAskDepth=_ask_depth(up_book),
                        downAskDepth=_ask_depth(down_book),
                    )
            else:
                record_live_window_diagnostic(
                    slug,
                    "pair_books_not_coherent",
                    dataGuardReason=_live_book_guard_reason(up_book, down_book),
                )
            if direct:
                if not sim.pair_candidate_is_stable(
                    "live:pair",
                    slug,
                    paired_shares,
                    direct[0]["limitPrice"],
                    direct[1]["limitPrice"],
                    PAIR_STABILITY_SECONDS,
                ):
                    record_live_window_diagnostic(
                        slug,
                        "pair_stability_wait",
                        pairedShares=paired_shares,
                        stabilitySeconds=PAIR_STABILITY_SECONDS,
                    )
                    return
                sim.clear_pair_candidate("live:pair")
                _ws_action_in_flight["v"] = True
                asyncio.get_running_loop().create_task(
                    _run_ws_pair_entry(session, slug, direct[0], direct[1], fair, dry_run, decision_lock)
                )
                return
            sim.clear_pair_candidate("live:pair")

        plan = (
            _late_direction_plan(
                up_book, down_book, remaining, shares, diagnostic_slug=slug
            )
            if ENABLE_LATE_DIRECTION
            else None
        )
        if plan:
            _ws_action_in_flight["v"] = True
            asyncio.get_running_loop().create_task(
                _run_ws_late_direction_entry(slug, plan, dry_run, decision_lock)
            )
        return

    if pos.get("hedged") or pos.get("windowSlug") != slug:
        return
    if not pos.get("dryRun", True) and not REAL_EXECUTION_ENABLED:
        return
    if pos.get("emergencyUnwindPending"):
        # 救援含餘額刷新與網路 I/O，交由 3 秒輪詢路徑執行；WS 快速路徑不重複排程。
        return
    if pos.get("strategy") == "late_direction":
        return

    dry_run = bool(pos.get("dryRun", True))
    other_side = "Down" if pos["side"] == "Up" else "Up"
    other_book = down_book if other_side == "Down" else up_book
    hedge = _buy_plan(other_side, other_book, pos["shares"])
    if not hedge:
        return
    projected_cost = _position_risk_cost(pos) + hedge["riskNotional"] + hedge["fee"]
    net_per_share = (pos["shares"] - projected_cost) / pos["shares"]
    cash = _strategy_cash_sync(dry_run)
    if cash is None:
        return
    if (
        float(pos.get("entryLimitPrice", pos["entryPrice"])) + hedge["limitPrice"] <= RESCUE_LOCK_MAX_SUM
        and net_per_share >= sim.SIM_MIN_NET_LOCK_PER_SHARE
        and hedge["riskNotional"] + hedge["fee"] <= cash
    ):
        _ws_action_in_flight["v"] = True
        asyncio.get_running_loop().create_task(_run_ws_hedge(hedge, dry_run, slug, decision_lock))


async def _run_ws_pair_entry(
    session: aiohttp.ClientSession,
    slug: str,
    up: dict,
    down: dict,
    fair: dict | None,
    dry_run: bool,
    decision_lock: asyncio.Lock,
) -> None:
    _ws_action_in_flight["v"] = False
    async with decision_lock:
        if live_state.get("position") is not None:
            return
        latest_up = sim.state.get("upBook") or up["book"]
        latest_down = sim.state.get("downBook") or down["book"]
        if not dry_run and not _live_books_are_coherent(latest_up, latest_down):
            record_live_window_diagnostic(
                slug,
                "pair_books_stale_at_submit",
                dataGuardReason=_live_book_guard_reason(latest_up, latest_down),
            )
            return
        cash = _strategy_cash_sync(dry_run)
        if cash is None:
            record_live_window_diagnostic(slug, "cash_cache_unavailable_at_submit")
            return
        latest = _direct_pair_plans(
            latest_up,
            latest_down,
            float(up["shares"]),
            cash,
            diagnostic_slug=slug,
        )
        if not latest:
            record_live_window_diagnostic(slug, "pair_candidate_vanished_before_submit")
            return
        await _execute_direct_pair(session, slug, latest[0], latest[1], fair, dry_run)


async def _run_ws_late_direction_entry(
    slug: str, plan: dict, dry_run: bool, decision_lock: asyncio.Lock
) -> None:
    _ws_action_in_flight["v"] = False
    async with decision_lock:
        if live_state.get("position") is not None:
            return
        if not ENABLE_LATE_DIRECTION:
            return
        up_book, down_book = sim.state.get("upBook"), sim.state.get("downBook")
        if not dry_run and (not up_book or not down_book or not _live_books_are_coherent(up_book, down_book)):
            record_live_window_diagnostic(
                slug,
                "direction_books_stale_at_submit",
                dataGuardReason=(
                    _live_book_guard_reason(up_book, down_book)
                    if up_book and down_book
                    else "missing current book"
                ),
            )
            return
        log.info(
            f"[LIVE] 晚進場方向性 {plan['side']} source={plan['_signalSource']} "
            f"Δ={plan['_deltaPct']:+.3f}% age={plan['_signalAgeSeconds']:.3f}s（WS 即時觸發）"
        )
        result = await _enter_position(slug, plan, dry_run)
        if result == "filled":
            live_state["position"]["strategy"] = "late_direction"
            save_live_state()


async def _run_ws_hedge(hedge: dict, dry_run: bool, slug: str, decision_lock: asyncio.Lock) -> None:
    _ws_action_in_flight["v"] = False
    async with decision_lock:
        pos = live_state.get("position")
        if not pos or pos.get("hedged") or pos.get("windowSlug") != slug:
            return
        await _hedge_position(hedge, dry_run)


def _log_startup_banner(mode: str) -> None:
    log.info("=" * 64)
    log.info(f"  Polymarket BTC Up/Down · 真實自動下單策略（{mode}）")
    log.info(
        f"  LIVE_TRADING={live.LIVE_TRADING} · POLY_STRATEGY_ARMED={STRATEGY_ARMED} "
        f"· POLY_VALIDATE_ORDER_PATH={live.VALIDATE_ORDER_PATH} "
        f"· REAL_EXECUTION={REAL_EXECUTION_ENABLED}"
    )
    if live.VALIDATE_ORDER_PATH:
        log.warning("  ORDER PATH VALIDATION: signing enabled, POST /orders hard-disabled")
    log.info(f"  pair budget={STAKE_PCT:.1f}% · hard cap=${MAX_PAIR_BUDGET_USD:.2f}")
    log.info(f"  cash reserve=${MIN_CASH_RESERVE_USD:.2f} · action cooldown={ACTION_COOLDOWN_SECONDS:.0f}s")
    log.info(f"  lock sum <= ${LOCK_MAX_SUM}　net lock/share>={sim.SIM_MIN_NET_LOCK_PER_SHARE:.3f}")
    log.info(
        f"  pair safeguard: executable depth >= {PAIR_MIN_DEPTH_MULTIPLIER:.1f}x/leg "
        f"and unchanged opportunity >= {PAIR_STABILITY_SECONDS:.2f}s"
    )
    log.info(f"  one-leg rescue lock sum <= ${RESCUE_LOCK_MAX_SUM}")
    log.info(
        f"  active variant={LIVE_VARIANT_ID} · direct pair={'enabled' if DIRECT_PAIR_ENABLED else 'disabled'}"
    )
    backend = live.signing_backend_name()
    if backend == "CoinCurveECCBackend":
        log.info(f"  signing backend={backend} (libsecp256k1 accelerated)")
    else:
        log.warning(f"  signing backend={backend} (pure-Python signing may cause latency spikes)")
    log.info(
        f"  emergency unwind: wait={EMERGENCY_UNWIND_WAIT_SECONDS:.1f}s "
        f"balance poll={EMERGENCY_UNWIND_POLL_INTERVAL:.2f}s order retry={EMERGENCY_UNWIND_ORDER_INTERVAL:.1f}s"
    )
    if ENABLE_LATE_DIRECTION:
        direction_source = (
            "Binance Futures 窗口漲跌"
            if _LIVE_VARIANT.get("directionSignalSource") == "binance_window"
            else f"Chainlink {sim.CHAINLINK_TWAP_WINDOW_SECONDS}s TWAP"
        )
        log.warning(
            f"  單腿方向性下注已啟用：{direction_source}，剩餘 "
            f"{sim.LATE_DIRECTION_MIN_ENTRY_REMAINING:.0f}~"
            f"{sim.LATE_DIRECTION_WINDOW_SECONDS:.0f}s、偏移開盤價>={sim.LATE_DIRECTION_MIN_DELTA_PCT:.2f}%、"
            f"不要求市場同向、進場價<=${LATE_DIRECTION_MAX_PRICE}"
        )
    else:
        log.info("  單腿方向性下注已停用（POLY_ENABLE_LATE_DIRECTION=false）")
    if live_state.get("halted"):
        log.critical(f"  STRATEGY HALTED: {live_state.get('haltReason')}")
    log.info("=" * 64)


async def strategy_loop() -> None:
    """獨立進程執行：自己開一條 WS 連線。跟模擬盤各自獨立，會有各自連線收到報價的
    時間差（見對話紀錄裡的診斷）。如果要完全消除這個時間差，改用 run_embedded()，
    讓實盤判斷邏輯跑在 polymarket_server.py 那個進程裡、共用同一條連線。"""
    _log_startup_banner("獨立進程")

    async with aiohttp.ClientSession() as session:
        decision_lock = asyncio.Lock()

        def on_ws_tick(token_id: str) -> None:
            _on_ws_tick_sync(token_id, session, decision_lock)

        # The live process only consumes the shared WS book implementation; it
        # must not execute the paper-simulation tick handler in the same process.
        sim.set_ws_simulation_ticks_enabled(False)
        sim.register_ws_price_listener(on_ws_tick)
        ws_task = asyncio.create_task(sim.market_ws_loop(), name="polymarket-market-ws")
        cash_task = asyncio.create_task(_cash_refresh_loop(), name="polymarket-cash-refresh")
        while True:
            try:
                cur = sim.state["market"]
                _asset_cfg = next(a for a in sim.ASSETS if a["id"] == LIVE_ASSET_ID)
                new_market = await sim.fetch_active_market(
                    session, _asset_cfg["slugPrefix"], _asset_cfg.get("windowSeconds", sim.WINDOW_SECONDS)
                )
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
                    sim.state["windowOpenSpotPrice"] = None  # 換窗口了，開盤價重新觀察
                    log.info(f"[MARKET] 切換到新窗口 {new_market['slug']}")

                if sim.state["market"]:
                    up_id, down_id = sim._market_tokens(sim.state["market"])
                    if up_id and down_id:
                        sim.state["upTokenId"] = up_id
                        sim.state["downTokenId"] = down_id
                        await sim._ws_set_wanted_tokens(LIVE_ASSET_ID, {up_id, down_id})
                        await asyncio.gather(
                            sim._ws_ensure_meta(session, up_id),
                            sim._ws_ensure_meta(session, down_id),
                        )
                        up_book, down_book, spot, klines = await asyncio.gather(
                            sim._get_book_ws_or_rest(session, up_id),
                            sim._get_book_ws_or_rest(session, down_id),
                            sim.fetch_spot_price(session, "BTCUSDT"),
                            sim.fetch_klines(session, "BTCUSDT", 60),
                            return_exceptions=True,
                        )
                        if not isinstance(up_book, Exception):
                            sim.state["upBook"] = up_book
                            if up_book["bids"] and up_book["asks"]:
                                sim.state["upPrice"] = (
                                    up_book["bids"][0]["price"] + up_book["asks"][0]["price"]
                                ) / 2
                        if not isinstance(down_book, Exception):
                            sim.state["downBook"] = down_book
                            if down_book["bids"] and down_book["asks"]:
                                sim.state["downPrice"] = (
                                    down_book["bids"][0]["price"] + down_book["asks"][0]["price"]
                                ) / 2
                        if not isinstance(spot, Exception):
                            sim.state["spotPrice"] = spot["price"]
                            sim.state["spotChangePct"] = spot["changePct"]
                            if sim.state.get("windowOpenSpotPrice") is None:
                                sim.state["windowOpenSpotPrice"] = spot["price"]
                        if not isinstance(klines, Exception) and klines:
                            sim.state["klines"] = klines

                        remaining = max(0.0, sim.state["windowEndsAt"] / 1000 - sim.real_now())
                        fair = sim.estimate_fair_up(LIVE_ASSET_ID)
                        if not isinstance(up_book, Exception) and not isinstance(down_book, Exception):
                            source = (
                                "websocket"
                                if up_book.get("quoteSource") == "websocket"
                                and down_book.get("quoteSource") == "websocket"
                                else "rest_fallback"
                            )
                            _set_quote_status(source)
                            if not decision_lock.locked():
                                async with decision_lock:
                                    await evaluate_and_act(
                                        sim.state["market"]["slug"], session, remaining, fair
                                    )

                await retry_pending_settlements(session)
            except Exception as exc:
                log.exception(f"策略迴圈錯誤：{exc}")
            finally:
                flush_live_window_diagnostics()
            await asyncio.sleep(POLL_INTERVAL)


async def run_embedded() -> None:
    """在 polymarket_server.py 那個進程裡直接跑，共用同一條 WS 連線，徹底消除
    「兩條獨立連線收到報價時間點不同」的問題——只有明確加 --with-live 啟動旗標
    才會呼叫這個函式，預設純模擬模式完全不受影響、不會有任何真實下單風險。

    跟 strategy_loop()（獨立進程）的差別：
      - 不自己開 WS 連線（sim.market_ws_loop() 已經在跑，這裡只是掛一個監聽器）。
      - 不呼叫 set_ws_simulation_ticks_enabled(False)——要讓 sim 自己那 7 個資產、
        BTC 自己 4 組模擬變體的判斷邏輯繼續正常運作，不能關掉。
      - 不自己重複輪詢 BTC 市場資料（fetch_active_market／fetch_book／...）——
        sim.data_fetcher() 每 3 秒已經在幫 sim.state（=markets_state["btc"]）
        補上最新資料，這裡直接讀就是最新的，不用再打一次 API。
      - 換窗口／待結算偵測改成輪詢比對 sim.state["market"]["slug"]，因為 live_state
        的部位追蹤是完全獨立於 sim 自己的 ab_states 之外的另一份帳本。
    """
    _log_startup_banner("嵌入模擬盤進程，共用 WS 連線")

    async with aiohttp.ClientSession() as session:
        decision_lock = asyncio.Lock()

        def on_ws_tick(token_id: str) -> None:
            _on_ws_tick_sync(token_id, session, decision_lock)

        sim.register_ws_price_listener(on_ws_tick)
        cash_task = asyncio.create_task(_cash_refresh_loop(), name="polymarket-cash-refresh")
        try:
            last_seen_slug: str | None = None
            while True:
                try:
                    market = sim.state.get("market")
                    if market:
                        slug = market["slug"]
                        if last_seen_slug is None:
                            pos = live_state.get("position")
                            if pos and pos.get("windowSlug") != slug:
                                # 進程重啟後才走到這裡：真實策略狀態可能還留著上一窗口的持倉。
                                queue_settlement(pos["windowSlug"])
                        elif slug != last_seen_slug:
                            queue_settlement(last_seen_slug)
                        last_seen_slug = slug

                        remaining = (
                            None if sim.state.get("windowEndsAt") is None
                            else max(0.0, sim.state["windowEndsAt"] / 1000 - sim.real_now())
                        )
                        fair = sim.state.get("fair")
                        status = sim.ws_feed_status()
                        _set_quote_status("websocket" if status["connected"] else "rest_fallback")
                        if not decision_lock.locked():
                            async with decision_lock:
                                await evaluate_and_act(slug, session, remaining, fair)

                    await retry_pending_settlements(session)
                except Exception as exc:
                    log.exception(f"[LIVE-embedded] 策略迴圈錯誤：{exc}")
                finally:
                    flush_live_window_diagnostics()
                await asyncio.sleep(POLL_INTERVAL)
        finally:
            sim.unregister_ws_price_listener(on_ws_tick)
            cash_task.cancel()


if __name__ == "__main__":
    asyncio.run(strategy_loop())
