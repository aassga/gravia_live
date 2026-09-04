import asyncio
import os
import json
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import polymarket_live_strategy as strategy
import polymarket_live_trader as trader


class LiveStrategyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._old_state_file = strategy.STATE_FILE
        self._tmpdir = tempfile.TemporaryDirectory()
        strategy.STATE_FILE = os.path.join(self._tmpdir.name, "live-state.json")
        strategy.reset_live_state_for_tests()

        # 測試永遠強制跑 dry-run，不管本機真實的 .env 有沒有武裝真實下單——
        # 避免哪天忘記，測試意外打到真實 API。
        self._old_strategy_armed = strategy.STRATEGY_ARMED
        self._old_real_execution = strategy.REAL_EXECUTION_ENABLED
        strategy.STRATEGY_ARMED = False
        strategy.REAL_EXECUTION_ENABLED = False

        # 波動速度防護的歷史紀錄是模組層級的全域狀態，不清掉的話，不同測試案例的報價
        # 快照會被誤判成「同一段時間內的劇烈波動」，互相污染。
        strategy.sim._ask_price_history.clear()

    def tearDown(self):
        strategy.STATE_FILE = self._old_state_file
        self._tmpdir.cleanup()
        strategy.STRATEGY_ARMED = self._old_strategy_armed
        strategy.REAL_EXECUTION_ENABLED = self._old_real_execution

    def test_only_matched_order_is_treated_as_filled(self):
        self.assertTrue(trader.order_response_filled({"success": True, "status": "matched"}))
        self.assertFalse(trader.order_response_filled({"success": True, "status": "delayed"}))
        self.assertFalse(trader.order_response_filled({"success": True, "status": "unmatched"}))
        self.assertFalse(trader.order_response_filled({"status": "matched"}))
        self.assertTrue(trader.order_response_filled({"id": "order-1", "status": "ORDER_STATUS_MATCHED"}))

    def test_strategy_dry_run_does_not_require_signature(self):
        response = trader.place_limit_order(
            "token", "BUY", 0.40, 5.0, dry_run=True, order_type="FOK", validate_signature=False
        )
        self.assertTrue(response["dry_run"])
        self.assertEqual(response["status"], "matched")

    def test_limit_price_rounds_in_adverse_direction_plus_one_tick_buffer(self):
        book = {"tickSize": 0.01}
        buy_fill = {"worstPrice": 0.40012}
        sell_fill = {"worstPrice": 0.39988}
        # 對齊到最差 tick（0.41／0.39）之後，再多讓一格 tick 提高成交機率。
        self.assertEqual(strategy.marketable_limit_price(book, buy_fill, "BUY"), 0.42)
        self.assertEqual(strategy.marketable_limit_price(book, sell_fill, "SELL"), 0.38)

    def test_live_and_simulation_share_decision_price(self):
        book = {"tickSize": 0.01}
        fill = {"shares": 5, "vwap": 0.4001, "worstPrice": 0.40012}
        self.assertEqual(
            strategy.marketable_limit_price(book, fill, "BUY"),
            strategy.sim.marketable_limit_price(book, fill, "BUY"),
        )

    def test_trade_fills_produce_weighted_actual_price(self):
        response = {"orderID": "order-1", "tradeIDs": ["trade-1", "trade-2"]}
        trades = [
            {"id": "trade-1", "taker_order_id": "order-1", "size": "4", "price": "0.40"},
            {"id": "trade-2", "taker_order_id": "order-1", "size": "6", "price": "0.45"},
            {"id": "other", "taker_order_id": "order-2", "size": "100", "price": "0.99"},
        ]
        summary = trader.summarize_order_fills(response, trades)
        self.assertEqual(summary["shares"], 10)
        self.assertAlmostEqual(summary["price"], 0.43)
        self.assertAlmostEqual(summary["notional"], 4.3)

    async def test_late_direction_skips_outside_window(self):
        strategy.sim.state["windowOpenSpotPrice"] = 100.0
        strategy.sim.state["spotPrice"] = 100.5  # +0.5%，遠超門檻
        up_book = {"tickSize": 0.01, "minOrderSize": 1, "asks": [{"price": 0.60, "size": 100}], "bids": []}
        down_book = {"tickSize": 0.01, "minOrderSize": 1, "asks": [{"price": 0.40, "size": 100}], "bids": []}
        filled = await strategy._try_late_direction_entry(
            "btc-window", up_book, down_book, remaining_seconds=30.0, shares=10.0, dry_run=True
        )
        self.assertFalse(filled)
        self.assertIsNone(strategy.live_state["position"])

    async def test_late_direction_enters_favored_side_near_close(self):
        strategy.sim.state["windowOpenSpotPrice"] = 100.0
        strategy.sim.state["spotPrice"] = 100.5  # +0.5%，偏 Up
        up_book = {"tickSize": 0.01, "minOrderSize": 1, "asks": [{"price": 0.60, "size": 100}], "bids": []}
        down_book = {"tickSize": 0.01, "minOrderSize": 1, "asks": [{"price": 0.40, "size": 100}], "bids": []}
        filled = await strategy._try_late_direction_entry(
            "btc-window", up_book, down_book, remaining_seconds=5.0, shares=10.0, dry_run=True
        )
        self.assertTrue(filled)
        pos = strategy.live_state["position"]
        self.assertEqual(pos["side"], "Up")
        self.assertEqual(pos["strategy"], "late_direction")
        self.assertFalse(pos["hedged"])

    def test_direct_pair_checks_worst_case_limit_and_fees(self):
        up_book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.39, "size": 100}],
            "bids": [],
        }
        down_book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.39, "size": 100}],
            "bids": [],
        }
        plans = strategy._direct_pair_plans(up_book, down_book, 10, 100)
        self.assertIsNotNone(plans)
        total = sum(plan["riskNotional"] + plan["fee"] for plan in plans)
        self.assertGreater(10 - total, 0)

    def test_direct_pair_rejects_same_tick_boundary_as_simulation(self):
        # 數字挑在剛好卡在目前 LOCK_MAX_SUM（0.95，跟隨 sim.AB_VARIANT_BY_ID["btc-late-direction"]）
        # 兩側：tick-aligned 的保守限價加總超過門檻，應該被拒絕。
        up_book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.48, "size": 100}],
            "bids": [],
        }
        down_book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.49, "size": 100}],
            "bids": [],
        }
        self.assertIsNone(strategy._direct_pair_plans(up_book, down_book, 10, 100))

    def test_state_survives_restart(self):
        strategy.live_state["totalTrades"] = 7
        strategy.live_state["position"] = {"windowSlug": "btc-window", "side": "Up"}
        strategy.save_live_state()
        restored = strategy._load_live_state()
        self.assertEqual(restored["totalTrades"], 7)
        self.assertEqual(restored["position"]["windowSlug"], "btc-window")

    async def test_submit_fok_requires_matched_response(self):
        plan = {"limitPrice": 0.40, "shares": 5.0}
        with patch.object(trader, "place_limit_order", return_value={"success": True, "status": "unmatched"}):
            result, _ = await strategy._submit_fok("token", "BUY", plan, True)
        self.assertEqual(result, "not_filled")

    async def test_real_execution_prefers_matched_trade_price(self):
        plan = {"observedVwap": 0.40, "limitPrice": 0.42, "shares": 10.0}
        summary = {"shares": 10.0, "price": 0.405, "notional": 4.05, "fee": None}
        with patch.object(trader, "get_order_fill_summary", return_value=summary):
            execution = await strategy._resolved_execution(plan, {"orderID": "order-1"}, False)
        self.assertEqual(execution["source"], "matched_trades")
        self.assertEqual(execution["price"], 0.405)
        self.assertEqual(execution["shares"], 10.0)
        self.assertAlmostEqual(execution["fee"], strategy.sim.taker_fee(10.0, 0.405))

    async def test_real_execution_still_uses_real_fill_when_shares_differ_from_plan(self):
        # 2026-09 真實案例：規劃 13 股，FOK 實際成交 13.565216 股。舊版會因為股數對不上
        # 整段丟棄真實成交資料、退回保守限價記帳，跟 Polymarket 官方紀錄對不起來。
        # 現在應該一律採用真實資料，並把真實股數回報出去讓部位追蹤跟著校正。
        plan = {"observedVwap": 0.22, "limitPrice": 0.24, "shares": 13.0}
        summary = {"shares": 13.565216, "price": 0.23, "notional": 3.12, "fee": None}
        with patch.object(trader, "get_order_fill_summary", return_value=summary):
            execution = await strategy._resolved_execution(plan, {"orderID": "order-1"}, False)
        self.assertEqual(execution["source"], "matched_trades")
        self.assertEqual(execution["price"], 0.23)
        self.assertEqual(execution["shares"], 13.565216)

    async def test_hedge_records_share_mismatch_and_uses_min_for_locked_pnl(self):
        strategy.live_state["lastActionAt"] = 0
        strategy.live_state["position"] = {
            "windowSlug": "btc-window",
            "side": "Down",
            "tokenId": "down-token",
            "shares": 13.565216,
            "entryNotional": 3.12,
            "entryRiskNotional": 3.12,
            "entryFee": 0.05,
            "entryRiskFee": 0.05,
            "hedged": False,
            "dryRun": False,
        }
        hedge_plan = {
            "side": "Up",
            "observedVwap": 0.60,
            "limitPrice": 0.61,
            "shares": 13.0,
            "riskNotional": 8.0,
            "fee": 0.2,
        }
        summary = {"shares": 14.41818, "price": 0.55, "notional": 7.93, "fee": None}
        with (
            patch.object(strategy, "_token_id", return_value="up-token"),
            patch.object(
                strategy,
                "_submit_fok",
                AsyncMock(return_value=("filled", {"orderID": "order-hedge"})),
            ),
            patch.object(trader, "get_order_fill_summary", return_value=summary),
        ):
            await strategy._hedge_position(hedge_plan, False)
        pos = strategy.live_state["position"]
        self.assertEqual(pos["hedgeShares"], 14.41818)
        self.assertIsNotNone(pos["shareMismatch"])
        self.assertAlmostEqual(pos["shareMismatch"]["unhedgedResidual"], 14.41818 - 13.565216)
        # 較保守的鎖利估計應該用兩腿較小的股數，不是無條件用進場那腿的股數。
        min_shares = min(13.565216, 14.41818)
        self.assertAlmostEqual(pos["lockedPnlEstimate"], min_shares - strategy._position_paid_cost(pos))

    def test_settle_pnl_uses_winning_sides_own_share_count_when_mismatched(self):
        pos = {
            "side": "Down",
            "shares": 13.565216,
            "hedged": True,
            "hedgeSide": "Up",
            "hedgeShares": 14.41818,
            "entryNotional": 3.12,
            "entryFee": 0.05,
            "hedgeNotional": 7.93,
            "hedgeFee": 0.2,
        }
        # Up 贏：應該拿 hedgeShares（真的持有的 Up 股數）算 payout，不是無條件用 pos["shares"]。
        pnl_up_wins = strategy._settle_pnl_estimate(pos, "Up")
        self.assertAlmostEqual(pnl_up_wins, 14.41818 - (3.12 + 0.05 + 7.93 + 0.2))
        # Down 贏：應該拿 shares（真的持有的 Down 股數）算 payout。
        pnl_down_wins = strategy._settle_pnl_estimate(pos, "Down")
        self.assertAlmostEqual(pnl_down_wins, 13.565216 - (3.12 + 0.05 + 7.93 + 0.2))

    def test_aggressive_sell_plan_widens_price_beyond_conservative_limit(self):
        book = {"tickSize": 0.01, "minOrderSize": 1, "bids": [{"price": 0.50, "size": 10}], "asks": []}
        normal = strategy._sell_plan("Up", book, 5.0)
        aggressive = strategy._aggressive_sell_plan("Up", book, 5.0)
        self.assertIsNotNone(normal)
        self.assertIsNotNone(aggressive)
        expected = round(normal["limitPrice"] - strategy.EMERGENCY_UNWIND_EXTRA_TICKS * 0.01, 6)
        self.assertAlmostEqual(aggressive["limitPrice"], expected)
        self.assertLess(aggressive["limitPrice"], normal["limitPrice"])
        self.assertAlmostEqual(aggressive["riskNotional"], aggressive["shares"] * aggressive["limitPrice"])

    def test_aggressive_sell_plan_clips_at_tick_floor(self):
        book = {"tickSize": 0.01, "minOrderSize": 1, "bids": [{"price": 0.05, "size": 1000}], "asks": []}
        aggressive = strategy._aggressive_sell_plan("Up", book, 1000.0)
        self.assertIsNotNone(aggressive)
        self.assertGreaterEqual(aggressive["limitPrice"], 0.01)

    # 2026-09：兩腿改成平行送出（見 _execute_direct_pair），不再依序呼叫
    # _enter_position/_hedge_position，改成直接呼叫 _submit_fok，所以下面這幾個測試
    # 改成 mock _submit_fok 本身，涵蓋兩腿都成交／只有一腿成交／兩腿都沒成交三種情況。
    def _fair_and_legs(self):
        strategy.sim.state["market"] = {
            "outcomes": json.dumps(["Up", "Down"]),
            "clobTokenIds": json.dumps(["up-token", "down-token"]),
        }
        up = {"side": "Up", "riskNotional": 2.0, "fee": 0.1, "shares": 5.0,
              "observedVwap": 0.40, "limitPrice": 0.42}
        down = {"side": "Down", "riskNotional": 2.0, "fee": 0.1, "shares": 5.0,
                "observedVwap": 0.50, "limitPrice": 0.52}
        fair = {"fairUp": 0.6, "fairDown": 0.4}
        return up, down, fair

    async def test_parallel_legs_both_filled_creates_locked_position(self):
        up, down, fair = self._fair_and_legs()

        async def fake_submit_fok(token_id, side, plan, dry_run):
            return "filled", {"orderID": f"order-{plan['side']}"}

        with patch.object(strategy, "_submit_fok", side_effect=fake_submit_fok):
            await strategy._execute_direct_pair(None, "btc-window", up, down, fair, True)

        pos = strategy.live_state["position"]
        self.assertIsNotNone(pos)
        self.assertTrue(pos["hedged"])
        self.assertEqual(pos["side"], "Up")
        self.assertEqual(pos["hedgeSide"], "Down")

    async def test_parallel_legs_only_one_filled_retries_failed_leg_then_unwinds(self):
        up, down, fair = self._fair_and_legs()

        async def fake_submit_fok(token_id, side, plan, dry_run):
            if plan["side"] == "Up":
                return "filled", {"orderID": "order-up"}
            return "not_filled", {}

        with (
            patch.object(strategy, "_submit_fok", side_effect=fake_submit_fok),
            patch.object(strategy, "_retry_failed_leg_once", AsyncMock(return_value="not_filled")) as retry,
            patch.object(strategy, "_emergency_unwind", AsyncMock()) as unwind,
        ):
            await strategy._execute_direct_pair(None, "btc-window", up, down, fair, True)

        pos = strategy.live_state["position"]
        self.assertIsNotNone(pos)
        self.assertFalse(pos["hedged"])
        self.assertEqual(pos["side"], "Up")
        retry.assert_awaited_once_with(None, "Down", True)
        unwind.assert_awaited_once()

    async def test_parallel_legs_retry_success_skips_emergency_unwind(self):
        up, down, fair = self._fair_and_legs()

        async def fake_submit_fok(token_id, side, plan, dry_run):
            if plan["side"] == "Up":
                return "filled", {"orderID": "order-up"}
            return "not_filled", {}

        with (
            patch.object(strategy, "_submit_fok", side_effect=fake_submit_fok),
            patch.object(strategy, "_retry_failed_leg_once", AsyncMock(return_value="filled")) as retry,
            patch.object(strategy, "_emergency_unwind", AsyncMock()) as unwind,
        ):
            await strategy._execute_direct_pair(None, "btc-window", up, down, fair, True)

        retry.assert_awaited_once_with(None, "Down", True)
        unwind.assert_not_awaited()

    async def test_parallel_legs_retry_unconfirmed_skips_emergency_unwind(self):
        # _retry_failed_leg_once 內部（透過 _hedge_position）已經觸發 halt，結果不明——
        # 這種情況不該再對第一腿送緊急平倉，避免萬一重試那腿其實有成交、變成三邊曝險。
        up, down, fair = self._fair_and_legs()

        async def fake_submit_fok(token_id, side, plan, dry_run):
            if plan["side"] == "Up":
                return "filled", {"orderID": "order-up"}
            return "not_filled", {}

        with (
            patch.object(strategy, "_submit_fok", side_effect=fake_submit_fok),
            patch.object(strategy, "_retry_failed_leg_once", AsyncMock(return_value="unconfirmed")) as retry,
            patch.object(strategy, "_emergency_unwind", AsyncMock()) as unwind,
        ):
            await strategy._execute_direct_pair(None, "btc-window", up, down, fair, True)

        retry.assert_awaited_once_with(None, "Down", True)
        unwind.assert_not_awaited()

    async def test_retry_failed_leg_once_hedges_when_fresh_quote_still_locks_profit(self):
        strategy.live_state["position"] = {
            "windowSlug": "btc-window", "side": "Up", "tokenId": "up-token",
            "shares": 5.0, "entryPrice": 0.40, "entryLimitPrice": 0.42,
            "entryRiskNotional": 2.1, "entryRiskFee": 0.1, "entryNotional": 2.0, "entryFee": 0.1,
            "hedged": False, "dryRun": True,
        }
        fresh_book = {"tickSize": 0.01, "minOrderSize": 1,
                      "asks": [{"price": 0.50, "size": 10}], "bids": []}

        async def fake_submit_fok(token_id, side, plan, dry_run):
            return "filled", {"orderID": "order-down"}

        with (
            patch.object(strategy.sim, "_get_book_ws_or_rest", AsyncMock(return_value=fresh_book)),
            patch.object(strategy, "_submit_fok", side_effect=fake_submit_fok),
        ):
            result = await strategy._retry_failed_leg_once(None, "Down", True)

        self.assertEqual(result, "filled")
        pos = strategy.live_state["position"]
        self.assertTrue(pos["hedged"])
        self.assertEqual(pos["hedgeSide"], "Down")

    async def test_retry_failed_leg_once_gives_up_when_fresh_quote_no_longer_locks_profit(self):
        strategy.live_state["position"] = {
            "windowSlug": "btc-window", "side": "Up", "tokenId": "up-token",
            "shares": 5.0, "entryPrice": 0.40, "entryLimitPrice": 0.42,
            "entryRiskNotional": 2.1, "entryRiskFee": 0.1, "entryNotional": 2.0, "entryFee": 0.1,
            "hedged": False, "dryRun": True,
        }
        # 價格已經惡化到跟進場價加總會超過 LOCK_MAX_SUM，重試不該硬鎖這個不划算的價位。
        expensive_book = {"tickSize": 0.01, "minOrderSize": 1,
                           "asks": [{"price": 0.95, "size": 10}], "bids": []}

        with (
            patch.object(strategy.sim, "_get_book_ws_or_rest", AsyncMock(return_value=expensive_book)),
            patch.object(strategy, "_submit_fok", AsyncMock()) as submit,
        ):
            result = await strategy._retry_failed_leg_once(None, "Down", True)

        self.assertEqual(result, "not_filled")
        submit.assert_not_awaited()
        self.assertFalse(strategy.live_state["position"]["hedged"])

    async def test_parallel_legs_neither_filled_creates_no_position(self):
        up, down, _fair = self._fair_and_legs()

        async def fake_submit_fok(token_id, side, plan, dry_run):
            return "not_filled", {}

        with patch.object(strategy, "_submit_fok", side_effect=fake_submit_fok):
            await strategy._execute_direct_pair(None, "btc-window", up, down, None, True)

        self.assertIsNone(strategy.live_state["position"])

    async def test_emergency_unwind_retries_once_on_transient_settlement_lag(self):
        # 2026-09 真實交易案例：第一腿剛成交，緊急平倉的 SELL 第一次被 CLOB 拒絕
        # （剛成交的部位鏈上還沒入帳，回 balance: 0），短暫等一下重試就成交了。
        # _emergency_unwind 現在應該自己重試一次，不是撞一次就放棄讓部位繼續單邊曝險。
        strategy.live_state["position"] = {
            "windowSlug": "btc-window",
            "side": "Down",
            "tokenId": "tok",
            "shares": 13.0,
            "dryRun": False,
            "hedged": False,
        }
        with (
            patch.object(strategy, "EMERGENCY_UNWIND_RETRY_INTERVAL", 0.01),
            patch.object(strategy.sim, "_get_book_ws_or_rest", AsyncMock(return_value={"bids": [], "asks": []})),
            patch.object(strategy, "_sell_plan", return_value={"side": "Down", "shares": 13.0, "limitPrice": 0.30}),
            patch.object(strategy, "_close_position", AsyncMock(side_effect=["not_filled", "filled"])) as close,
        ):
            await strategy._emergency_unwind(None, "direct_pair_second_leg_failed")
        self.assertEqual(close.await_count, 2)

    async def test_emergency_unwind_stops_retrying_once_hedged_elsewhere(self):
        # 重試等待的空檔裡，如果正常補鎖利路徑已經把部位對沖掉了，不該再搶著平倉。
        strategy.live_state["position"] = {
            "windowSlug": "btc-window",
            "side": "Down",
            "tokenId": "tok",
            "shares": 13.0,
            "dryRun": False,
            "hedged": False,
        }

        async def hedge_it_during_wait(_seconds):
            strategy.live_state["position"]["hedged"] = True

        with (
            patch.object(strategy, "EMERGENCY_UNWIND_RETRY_INTERVAL", 0.01),
            patch.object(strategy.sim, "_get_book_ws_or_rest", AsyncMock(return_value={"bids": [], "asks": []})),
            patch.object(strategy, "_sell_plan", return_value={"side": "Down", "shares": 13.0, "limitPrice": 0.30}),
            patch.object(strategy, "_close_position", AsyncMock(return_value="not_filled")) as close,
            patch("asyncio.sleep", side_effect=hedge_it_during_wait),
        ):
            await strategy._emergency_unwind(None, "direct_pair_second_leg_failed")
        self.assertEqual(close.await_count, 1)

    async def test_no_new_entry_after_window_closed(self):
        # 90 秒門檻已經拿掉（鎖利不需要、晚進場方向性還得靠它才能在剩不到 10 秒時動作），
        # 現在唯一會擋新倉位的是「已經沒剩餘時間」。
        with patch.object(strategy, "_strategy_cash", AsyncMock()) as cash:
            await strategy.evaluate_and_act("btc-window", None, 0.0, {"fairUp": 0.5, "fairDown": 0.5})
        cash.assert_not_awaited()

    async def test_new_entry_allowed_with_little_time_left(self):
        # 剩 30 秒——舊的 90 秒門檻會擋掉這個情境，新設計應該放行到鎖利／晚進場方向性判斷。
        with patch.object(strategy, "_strategy_cash", AsyncMock(return_value=100.0)) as cash:
            await strategy.evaluate_and_act("btc-window", None, 30.0, {"fairUp": 0.5, "fairDown": 0.5})
        cash.assert_awaited_once()

    async def test_full_dry_run_pair_never_signs_or_sends(self):
        strategy.live_state["lastActionAt"] = 0
        strategy.sim.state["market"] = {
            "outcomes": json.dumps(["Up", "Down"]),
            "clobTokenIds": json.dumps(["up-token", "down-token"]),
        }
        strategy.sim.state["upBook"] = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.39, "size": 100}],
            "bids": [{"price": 0.38, "size": 100}],
        }
        strategy.sim.state["downBook"] = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.39, "size": 100}],
            "bids": [{"price": 0.38, "size": 100}],
        }
        with patch.object(trader, "build_order", side_effect=AssertionError("dry-run must not sign")):
            await strategy.evaluate_and_act(
                "btc-window", None, 180.0, {"fairUp": 0.5, "fairDown": 0.5}
            )
        self.assertTrue(strategy.live_state["position"]["hedged"])
        self.assertTrue(strategy.live_state["position"]["dryRun"])

    async def test_live_book_prefers_current_websocket_snapshot(self):
        token = "ws-token"
        ws_book = {
            "bids": {"0.39": 10.0},
            "asks": {"0.40": 10.0},
        }
        with (
            patch.object(strategy.sim, "_ws_connected", True),
            patch.object(strategy.sim, "_ws_last_message_at", time.monotonic()),
            patch.object(strategy.sim, "_ws_snapshot_tokens", {token}),
            patch.object(strategy.sim, "_ws_books", {token: ws_book}),
            patch.object(strategy.sim, "fetch_book", AsyncMock()) as rest_fetch,
        ):
            book = await strategy.sim._get_book_ws_or_rest(None, token)
        self.assertEqual(book["quoteSource"], "websocket")
        self.assertEqual(book["asks"][0]["price"], 0.40)
        rest_fetch.assert_not_awaited()

    async def test_live_book_falls_back_to_rest_without_current_snapshot(self):
        rest_book = {"bids": [], "asks": [], "tickSize": 0.01, "minOrderSize": 1.0}
        with (
            patch.object(strategy.sim, "_ws_connected", True),
            patch.object(strategy.sim, "_ws_last_message_at", time.monotonic()),
            patch.object(strategy.sim, "_ws_snapshot_tokens", set()),
            patch.object(strategy.sim, "fetch_book", AsyncMock(return_value=rest_book)) as rest_fetch,
        ):
            book = await strategy.sim._get_book_ws_or_rest(None, "missing-token")
        self.assertEqual(book["quoteSource"], "rest_fallback")
        rest_fetch.assert_awaited_once()

    def test_ws_tick_immediately_evaluates_complete_book_pair(self):
        # _on_ws_tick_sync 取代了舊版 _evaluate_ws_tick：判斷本身改成純同步、零延遲
        # 執行（不再靠 asyncio.create_task 排程整段判斷），只有真的要送單才切到 async。
        # 見 _ws_action_in_flight 旁的說明——排程延遲曾經造成實盤錯過模擬盤同步抓到的
        # 鎖利機會。這裡驗證的是「收到完整 WS book pair 後立刻同步套用到 sim.state
        # 並標記 quoteSource=websocket」這個可觀察行為，不再依賴內部呼叫 evaluate_and_act
        # 這個已經不存在的中介步驟。
        market = {
            "slug": "btc-window",
            "outcomes": json.dumps(["Up", "Down"]),
            "clobTokenIds": json.dumps(["up-token", "down-token"]),
        }
        books = {
            "up-token": {"bids": {"0.39": 10.0}, "asks": {"0.40": 10.0}},
            "down-token": {"bids": {"0.59": 10.0}, "asks": {"0.60": 10.0}},
        }
        strategy.sim.state["market"] = market
        strategy.sim.state["windowEndsAt"] = (strategy.sim.real_now() + 180) * 1000
        with (
            patch.object(strategy.sim, "_ws_connected", True),
            patch.object(strategy.sim, "_ws_last_message_at", time.monotonic()),
            patch.object(strategy.sim, "_ws_snapshot_tokens", {"up-token", "down-token"}),
            patch.object(strategy.sim, "_ws_books", books),
        ):
            strategy._on_ws_tick_sync("up-token", None, asyncio.Lock())
        self.assertEqual(strategy.live_state["quoteSource"], "websocket")
        self.assertEqual(strategy.sim.state["upBook"]["quoteSource"], "websocket")
        self.assertEqual(strategy.sim.state["downBook"]["quoteSource"], "websocket")

    async def test_ws_tick_sync_enters_lock_pair_without_waiting_for_a_scheduled_pass(self):
        # 這個測試驗證這次修的問題本身：一個真的可以鎖利的 WS book pair，_on_ws_tick_sync
        # 必須「當下同步判斷出機會」（不必等 asyncio.create_task 排程），只有真的送單那步
        # 才切到 async task。判斷完成後只需要事件迴圈跑一輪（asyncio.sleep(0)）讓那顆
        # task 執行完，不需要任何額外的輪詢週期。
        strategy.live_state["lastActionAt"] = 0
        market = {
            "slug": "btc-window",
            "outcomes": json.dumps(["Up", "Down"]),
            "clobTokenIds": json.dumps(["up-token", "down-token"]),
        }
        strategy.sim.state["market"] = market
        strategy.sim.state["windowEndsAt"] = (strategy.sim.real_now() + 180) * 1000
        books = {
            "up-token": {"bids": {"0.38": 100.0}, "asks": {"0.39": 100.0}},
            "down-token": {"bids": {"0.38": 100.0}, "asks": {"0.39": 100.0}},
        }
        decision_lock = asyncio.Lock()
        with (
            patch.object(strategy.sim, "_ws_connected", True),
            patch.object(strategy.sim, "_ws_last_message_at", time.monotonic()),
            patch.object(strategy.sim, "_ws_snapshot_tokens", {"up-token", "down-token"}),
            patch.object(strategy.sim, "_ws_books", books),
            patch.object(trader, "build_order", side_effect=AssertionError("dry-run must not sign")),
        ):
            strategy._on_ws_tick_sync("up-token", None, decision_lock)
            self.assertTrue(strategy._ws_action_in_flight["v"], "opportunity was not detected synchronously")
            # 讓事件迴圈跑一輪，把 create_task 排出去的送單 task 執行完。
            for _ in range(5):
                await asyncio.sleep(0)
        self.assertTrue(strategy.live_state["position"]["hedged"])
        self.assertTrue(strategy.live_state["position"]["dryRun"])
        self.assertFalse(strategy._ws_action_in_flight["v"])

    async def test_direct_pair_shares_capped_at_configured_fraction_of_depth(self):
        # 跟模擬版 sim._try_direct_pair 對齊：股數封頂在可見深度的
        # sim.SIM_DEPTH_CAP_FRACTION（見該常數註解），不是 100%。
        strategy.live_state["lastActionAt"] = 0
        strategy.sim.state["market"] = {
            "outcomes": json.dumps(["Up", "Down"]),
            "clobTokenIds": json.dumps(["up-token", "down-token"]),
        }
        depth = 40  # 封頂後 (depth * SIM_DEPTH_CAP_FRACTION) 要小於 MAX_PAIR_BUDGET_USD
                    # 換算出的股數上限，才能確定是深度、不是資金，在限制最終股數。
        strategy.sim.state["upBook"] = {
            "tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": 0.10, "size": depth}], "bids": [],
        }
        strategy.sim.state["downBook"] = {
            "tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": 0.10, "size": depth}], "bids": [],
        }
        # 現金給大一點，確保是深度（不是資金）在限制股數，跟本機 .env 的 STAKE_PCT 設多少無關。
        with patch.object(strategy, "_strategy_cash", AsyncMock(return_value=100_000.0)):
            await strategy.evaluate_and_act(
                "btc-window", None, 180.0, {"fairUp": 0.5, "fairDown": 0.5}
            )
        pos = strategy.live_state["position"]
        self.assertIsNotNone(pos)
        self.assertTrue(pos["hedged"])
        self.assertEqual(pos["shares"], depth * strategy.sim.SIM_DEPTH_CAP_FRACTION)

    def _set_market_for_preflight(self):
        strategy.sim.state["market"] = {
            "outcomes": json.dumps(["Up", "Down"]),
            "clobTokenIds": json.dumps(["up-token", "down-token"]),
        }

    # 2026-09：真實案例——兩個 token 的餘額查詢原本用 asyncio.gather 同時發出，兩個背景
    # 執行緒同時打中 py_clob_client_v2 共用的 httpx.Client(http2=True)、搶著建立第一條
    # 連線，穩定重現 [Errno 11] Resource temporarily unavailable，連兩台不同 VPS 都一樣。
    # 改成序列查詢＋重試一次後，下面驗證：(1) 兩次查詢確實不再同時發出、(2) 單次暫時性
    # 錯誤靠重試自己救回來、不會白白觸發 halt。
    async def test_preflight_check_queries_balances_sequentially_not_concurrently(self):
        self._set_market_for_preflight()
        in_flight = []
        max_concurrent = 0

        def fake_get_balance(token_id):
            in_flight.append(token_id)
            nonlocal max_concurrent
            max_concurrent = max(max_concurrent, len(in_flight))
            in_flight.remove(token_id)
            return 0.0

        with patch.object(strategy.live, "get_conditional_balance", side_effect=fake_get_balance):
            result = await strategy._ensure_no_unmanaged_current_position()

        self.assertTrue(result)
        self.assertEqual(max_concurrent, 1)

    async def test_preflight_check_retries_once_on_transient_error(self):
        self._set_market_for_preflight()
        calls = {"n": 0}

        def flaky_get_balance(token_id):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("[Errno 11] Resource temporarily unavailable")
            return 0.0

        with (
            patch.object(strategy.live, "get_conditional_balance", side_effect=flaky_get_balance),
            patch.object(strategy.asyncio, "sleep", AsyncMock()),
        ):
            result = await strategy._ensure_no_unmanaged_current_position()

        self.assertTrue(result)
        self.assertFalse(strategy.live_state.get("halted"))

    async def test_preflight_check_halts_when_retry_also_fails(self):
        self._set_market_for_preflight()

        def always_fails(token_id):
            raise OSError("[Errno 11] Resource temporarily unavailable")

        with (
            patch.object(strategy.live, "get_conditional_balance", side_effect=always_fails),
            patch.object(strategy.asyncio, "sleep", AsyncMock()),
        ):
            result = await strategy._ensure_no_unmanaged_current_position()

        self.assertFalse(result)
        self.assertTrue(strategy.live_state.get("halted"))

    async def test_preflight_check_halts_on_real_unmanaged_position(self):
        self._set_market_for_preflight()

        with patch.object(strategy.live, "get_conditional_balance", side_effect=[3.0, 0.0]):
            result = await strategy._ensure_no_unmanaged_current_position()

        self.assertFalse(result)
        self.assertTrue(strategy.live_state.get("halted"))


if __name__ == "__main__":
    unittest.main()
