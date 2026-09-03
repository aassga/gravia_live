"""
Polymarket BTC 5 分鐘 Up/Down · 真實自動下單策略

真實版與紙上模擬共用同一套核心判斷（套用模擬版 "btc-late-direction" 這組）：
    - 使用 Ask/Bid 深度計算 VWAP，再以含滑點、向不利 tick 取整的最差限價作決策。
    - 優先鎖利：當下兩邊同時買得到、扣費用後淨賺達門檻才配對進場，這是唯一的
      無方向曝險進場路徑。
    - 鎖不到時的備案是晚進場方向性：只在窗口剩不到 10 秒、且現價已經明顯偏離
      這個窗口開盤價時才賭方向，進場後不補鎖利、不提早出場，抱到結算為止。
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
REAL_EXECUTION_ENABLED = live.LIVE_TRADING and STRATEGY_ARMED
MAX_PAIR_BUDGET_USD = max(1.0, float(os.environ.get("POLY_MAX_PAIR_BUDGET_USD", "25.0")))
MIN_CASH_RESERVE_USD = max(0.0, float(os.environ.get("POLY_MIN_CASH_RESERVE_USD", "5.0")))
DRY_RUN_BALANCE_USD = max(1.0, float(os.environ.get("POLY_DRY_RUN_BALANCE_USD", "100.0")))
ACTION_COOLDOWN_SECONDS = max(1.0, float(os.environ.get("POLY_ACTION_COOLDOWN_SECONDS", "10.0")))
ORDER_CONFIRM_ATTEMPTS = max(1, int(os.environ.get("POLY_ORDER_CONFIRM_ATTEMPTS", "6")))
ORDER_CONFIRM_INTERVAL = max(0.5, float(os.environ.get("POLY_ORDER_CONFIRM_INTERVAL", "1.0")))
# 2026-09：實測發現第一腿剛成交、緊接著想緊急平倉賣掉那一腿時，CLOB 有時會回
# 「balance: 0」拒單——剛成交的部位在鏈上還沒入帳/索引完成，不是真的沒有這個部位。
# 短暫等一下通常就會過。這裡刻意只給一次額外重試（不是無限重試）：_emergency_unwind
# 是在 decision_lock 保護下執行的，多等一次就多佔用一次鎖的時間，擋住同一時間本來
# 可以正常運作的補鎖利重試（實測那次真的靠這條路 17 秒後補鎖利成功）——重試次數
# 抓少一點，是刻意在「給結算延遲一次機會」跟「不要卡住正常補鎖利路徑太久」之間取平衡。
EMERGENCY_UNWIND_RETRY_ATTEMPTS = max(1, int(os.environ.get("POLY_EMERGENCY_UNWIND_RETRY_ATTEMPTS", "2")))
EMERGENCY_UNWIND_RETRY_INTERVAL = max(0.5, float(os.environ.get("POLY_EMERGENCY_UNWIND_RETRY_INTERVAL", "2.0")))

# 真實版套用模擬版 A/B 測試裡的「BTC 晚進場方向性」這組（sim.AB_VARIANT_BY_ID["btc-late-direction"]）。
# 這裡引用 AB_VARIANT_BY_ID 而不是直接寫死數字，是為了跟模擬版共用同一個真實來源，模擬版調整
# 這組門檻時真實版會自動跟著同步。
# 2026-09：從「btc-main」（鎖利優先，找不到就靠公平價模型賭單邊）改成「btc-late-direction」
# （鎖利優先＋找不到鎖利時改成只在窗口剩不到 10 秒、現價已明顯偏離開盤價時才賭方向）——
# 模擬盤驗證下來後者的方向性單邊勝率遠高於前者（91% vs 0%），詳見對話紀錄。
# 鎖利（_direct_pair_plans／_execute_direct_pair）邏輯完全沒變，只換掉找不到鎖利時的備案。
_LIVE_VARIANT = sim.AB_VARIANT_BY_ID["btc-late-direction"]
# 鎖利門檻改回跟 btc-late-direction 這組自己的 lockMaxSum 一致（0.95），不再借用
# btc-loose 的 0.98——這樣實盤才是單一模擬變體的精確複製，不會變成混用兩組門檻、
# 模擬盤沒有對應組合可驗證的組合。
LOCK_MAX_SUM = _LIVE_VARIANT["lockMaxSum"]
LATE_DIRECTION_MAX_PRICE = _LIVE_VARIANT["lateDirectionMaxPrice"]


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
    fee = sim.taker_fee(shares, price)

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
            fee = sim.taker_fee(shares, price) if fee is None else float(fee)
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
    risk = _risk_fill(book, fill, "BUY")
    if shares < float(book.get("minOrderSize", 1) or 1) or risk["riskNotional"] < sim.SIM_MIN_ORDER_NOTIONAL_USD:
        return None
    all_in_per_share = (risk["riskNotional"] + risk["fee"]) / shares
    edge = None if fair_probability is None else fair_probability - all_in_per_share
    return {"side": side, "book": book, "fair": fair_probability, "edge": edge, **risk}


def _sell_plan(side: str, book: dict, shares: float) -> dict | None:
    fill = sim.simulate_sell_fill(book, shares)
    if not fill:
        return None
    risk = _risk_fill(book, fill, "SELL")
    if shares < float(book.get("minOrderSize", 1) or 1) or risk["riskNotional"] < sim.SIM_MIN_ORDER_NOTIONAL_USD:
        return None
    return {"side": side, "book": book, **risk}


def _late_direction_plan(
    up_book: dict,
    down_book: dict,
    remaining_seconds: float,
    shares: float,
) -> dict | None:
    """純同步、零延遲的判斷：只在窗口剩不到 10 秒、且現價已經明顯偏離這個窗口開盤價時
    才考慮賭方向。跟模擬版 sim._try_late_direction_entry 同一套門檻。拆成獨立的同步
    函式是為了讓 _on_ws_tick_sync 這條快速路徑可以直接呼叫，不用等 create_task 排程——
    真正送單（會動用網路 I/O）仍然留在呼叫端用 async 處理。"""
    if (
        remaining_seconds > sim.LATE_DIRECTION_WINDOW_SECONDS
        or remaining_seconds < sim.LATE_DIRECTION_MIN_ENTRY_REMAINING
    ):
        return None
    open_price = sim.state.get("windowOpenSpotPrice")
    spot = sim.state.get("spotPrice")
    if not open_price or not spot or open_price <= 0:
        return None
    delta_pct = (spot - open_price) / open_price * 100
    if abs(delta_pct) < sim.LATE_DIRECTION_MIN_DELTA_PCT:
        return None
    side, book = ("Up", up_book) if delta_pct > 0 else ("Down", down_book)
    # 跟模擬版對齊：不把股數縮到「當下看得到的深度」——真正的 FOK 語意是要嘛整筆用
    # 目標股數成交、要嘛深度不夠就整筆不成交，不會自動改成「有多少吃多少」。這裡故意
    # 不呼叫 _ask_depth 縮股，讓 _buy_plan 內部的 simulate_buy_fill 用同一套全有全無
    # 判斷，深度不足就直接放棄這次機會，跟模擬版的驗證結果一致。
    plan = _buy_plan(side, book, shares)
    if not plan or plan["limitPrice"] > LATE_DIRECTION_MAX_PRICE:
        return None
    plan["_deltaPct"] = delta_pct
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
    plan = _late_direction_plan(up_book, down_book, remaining_seconds, shares)
    if not plan:
        return False
    log.info(f"[LIVE] 晚進場方向性 {plan['side']} Δ={plan['_deltaPct']:+.3f}% 剩餘={remaining_seconds:.1f}s")
    result = await _enter_position(slug, plan, dry_run)
    if result == "filled":
        live_state["position"]["strategy"] = "late_direction"
        save_live_state()
    return result == "filled"


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
    # 跟模擬版 sim._try_direct_pair 對齊：這裡只比對 cash 本身，不再扣一次
    # MIN_CASH_RESERVE_USD——保留額已經在 _target_pair_order／target_pair_order
    # 算股數預算時扣過了，這裡如果再扣一次會變成保留額重複計算，讓實盤比模擬更早
    # 放棄本可成立的鎖利機會。
    if net_per_share < sim.SIM_MIN_NET_LOCK_PER_SHARE or total_cost > cash:
        return None
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
    if not dry_run and abs(execution["shares"] - plan["shares"]) > max(1e-6, plan["shares"] * 0.001):
        log.warning(
            f"[LIVE] 進場真實成交股數（{execution['shares']:.6f}）校正了規劃股數"
            f"（{plan['shares']:.6f}）——部位追蹤改用真實股數。"
        )

    live_state["position"] = {
        "windowSlug": slug,
        "side": plan["side"],
        "tokenId": token_id,
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
        f"shares={execution['shares']:.2f} edge={plan.get('edge') if plan.get('edge') is not None else float('nan'):+.4f}"
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
    for attempt in range(EMERGENCY_UNWIND_RETRY_ATTEMPTS):
        if attempt > 0:
            await asyncio.sleep(EMERGENCY_UNWIND_RETRY_INTERVAL)
        pos = live_state.get("position")
        if not pos or pos.get("hedged"):
            # 等待重試的空檔裡，正常補鎖利路徑可能已經處理掉了，不用再搶著平倉。
            return
        try:
            latest_book = await sim._get_book_ws_or_rest(session, pos["tokenId"])
        except Exception as exc:
            log.error(f"[LIVE] 緊急退出前無法取得訂單簿：{exc}")
            continue
        plan = _sell_plan(pos["side"], latest_book, pos["shares"])
        if not plan:
            log.error("[LIVE] 第二腿失敗，且第一腿目前沒有足夠 Bid 可緊急退出")
            continue
        result = await _close_position(plan, bool(pos.get("dryRun", True)), reason)
        if result in ("filled", "unconfirmed"):
            # filled：平倉完成；unconfirmed：_close_position 內部已經觸發 halt，
            # 兩種情況都不該再重試。
            return
        log.warning(
            f"[LIVE] 緊急平倉第 {attempt + 1}/{EMERGENCY_UNWIND_RETRY_ATTEMPTS} 次嘗試未成交"
            f"（reason={reason}），可能是剛成交的部位鏈上還沒入帳，稍後重試"
        )
    log.error(
        f"[LIVE] 緊急平倉重試 {EMERGENCY_UNWIND_RETRY_ATTEMPTS} 次仍未成交，持倉保留為未對沖狀態，"
        "改靠正常補鎖利流程持續嘗試"
    )


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
    allow_early_exit: bool = True,
) -> None:
    if live_state.get("halted"):
        return

    up_book, down_book = sim.state["upBook"], sim.state["downBook"]
    sim.log_price_sum_diagnostic("live-btc", up_book, down_book, LOCK_MAX_SUM)
    pos = live_state.get("position")

    if pos is None:
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
        cash = await _strategy_cash(dry_run)
        shares, budget = _target_pair_order(cash)
        if budget < 1.0 or shares < 1.0:
            return

        # 先試鎖利（找不到就空手，不賭單邊——這條退路模擬盤驗證下來是 0% 勝率，
        # 詳見對話紀錄）；鎖不到才在窗口快結束時改用晚進場方向性當備案。
        # 股數先按可見深度的 sim.SIM_DEPTH_CAP_FRACTION 封頂，跟模擬版 sim._try_direct_pair
        # 對齊（見該常數註解）；封頂後再無條件捨去到整數，對應真實下單實際能送出的精度。
        depth_cap = min(_ask_depth(up_book), _ask_depth(down_book)) * sim.SIM_DEPTH_CAP_FRACTION
        paired_shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
        direct = _direct_pair_plans(up_book, down_book, paired_shares, cash) if paired_shares >= 1.0 else None
        if direct:
            await _execute_direct_pair(session, slug, direct[0], direct[1], fair, dry_run)
            return
        await _try_late_direction_entry(slug, up_book, down_book, remaining_seconds, shares, dry_run)
        return

    if pos.get("hedged") or pos.get("windowSlug") != slug:
        return
    if not pos.get("dryRun", True) and not REAL_EXECUTION_ENABLED:
        log.error("[LIVE] 存在真實持倉，但真實策略未完整武裝；本程式不會假裝已對沖")
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
            float(pos.get("entryLimitPrice", pos["entryPrice"])) + hedge["limitPrice"] <= LOCK_MAX_SUM
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


def _on_ws_tick_sync(token_id: str, session: aiohttp.ClientSession, decision_lock: asyncio.Lock) -> None:
    """同步、零延遲版本，取代舊版 _evaluate_ws_tick——見上面 _ws_action_in_flight 的說明。"""
    if live_state.get("halted"):
        return
    market = sim.state.get("market")
    if not market:
        return
    up_id, down_id = sim._market_tokens(market)
    if token_id not in (up_id, down_id):
        return
    up_book = sim._ws_get_book(up_id)
    down_book = sim._ws_get_book(down_id)
    if up_book is None or down_book is None:
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
    fair = sim.estimate_fair_up("btc")
    _set_quote_status(source_for_status)

    if decision_lock.locked() or _ws_action_in_flight["v"]:
        return

    pos = live_state.get("position")

    if pos is None:
        if time.time() - float(live_state.get("lastActionAt", 0)) < ACTION_COOLDOWN_SECONDS:
            return
        if remaining <= 0:
            return
        dry_run = not REAL_EXECUTION_ENABLED
        if not dry_run and live_state.get("preflightSlug") != slug:
            # 真實模式每個窗口第一次要做的 preflight 檢查需要真的等網路 I/O，不屬於這條
            # 零延遲路徑該做的事，留給 3 秒輪詢那條路（原本的 evaluate_and_act）處理。
            return
        cash = _strategy_cash_sync(dry_run)
        if cash is None:
            return
        shares, budget = _target_pair_order(cash)
        if budget < 1.0 or shares < 1.0:
            return

        depth_cap = min(_ask_depth(up_book), _ask_depth(down_book)) * sim.SIM_DEPTH_CAP_FRACTION
        paired_shares = float(Decimal(str(min(shares, depth_cap))).to_integral_value(rounding=ROUND_DOWN))
        direct = _direct_pair_plans(up_book, down_book, paired_shares, cash) if paired_shares >= 1.0 else None
        if direct:
            _ws_action_in_flight["v"] = True
            asyncio.get_running_loop().create_task(
                _run_ws_pair_entry(session, slug, direct[0], direct[1], fair, dry_run, decision_lock)
            )
            return

        plan = _late_direction_plan(up_book, down_book, remaining, shares)
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
        float(pos.get("entryLimitPrice", pos["entryPrice"])) + hedge["limitPrice"] <= LOCK_MAX_SUM
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
        await _execute_direct_pair(session, slug, up, down, fair, dry_run)


async def _run_ws_late_direction_entry(
    slug: str, plan: dict, dry_run: bool, decision_lock: asyncio.Lock
) -> None:
    _ws_action_in_flight["v"] = False
    async with decision_lock:
        if live_state.get("position") is not None:
            return
        log.info(
            f"[LIVE] 晚進場方向性 {plan['side']} Δ={plan['_deltaPct']:+.3f}%（WS 即時觸發）"
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
        f"· REAL_EXECUTION={REAL_EXECUTION_ENABLED}"
    )
    log.info(f"  pair budget={STAKE_PCT:.1f}% · hard cap=${MAX_PAIR_BUDGET_USD:.2f}")
    log.info(f"  cash reserve=${MIN_CASH_RESERVE_USD:.2f} · action cooldown={ACTION_COOLDOWN_SECONDS:.0f}s")
    log.info(f"  lock sum <= ${LOCK_MAX_SUM}（跟隨 btc-late-direction）　net lock/share>={sim.SIM_MIN_NET_LOCK_PER_SHARE:.3f}")
    log.info(
        f"  找不到鎖利時備案＝晚進場方向性：剩餘 {sim.LATE_DIRECTION_MIN_ENTRY_REMAINING:.0f}~"
        f"{sim.LATE_DIRECTION_WINDOW_SECONDS:.0f}s、偏移開盤價>={sim.LATE_DIRECTION_MIN_DELTA_PCT:.2f}%、"
        f"進場價<=${LATE_DIRECTION_MAX_PRICE} 才進場，進場後不補鎖利／不提早出場"
    )
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
                    sim.state["windowOpenSpotPrice"] = None  # 換窗口了，開盤價重新觀察
                    log.info(f"[MARKET] 切換到新窗口 {new_market['slug']}")

                if sim.state["market"]:
                    up_id, down_id = sim._market_tokens(sim.state["market"])
                    if up_id and down_id:
                        sim.state["upTokenId"] = up_id
                        sim.state["downTokenId"] = down_id
                        await sim._ws_set_wanted_tokens("btc", {up_id, down_id})
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
                        fair = sim.estimate_fair_up("btc")
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
                await asyncio.sleep(POLL_INTERVAL)
        finally:
            sim.unregister_ws_price_listener(on_ws_tick)
            cash_task.cancel()


if __name__ == "__main__":
    asyncio.run(strategy_loop())
