import os
import json
import tempfile
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

    def tearDown(self):
        strategy.STATE_FILE = self._old_state_file
        self._tmpdir.cleanup()

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

    def test_limit_price_rounds_in_adverse_direction(self):
        book = {"tickSize": 0.01}
        buy_fill = {"worstPrice": 0.40012}
        sell_fill = {"worstPrice": 0.39988}
        self.assertEqual(strategy.marketable_limit_price(book, buy_fill, "BUY"), 0.41)
        self.assertEqual(strategy.marketable_limit_price(book, sell_fill, "SELL"), 0.39)

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

    def test_entry_candidate_requires_fee_adjusted_edge(self):
        book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.35, "size": 100}],
            "bids": [],
        }
        self.assertIsNotNone(strategy._entry_candidate("Up", book, 10, 0.60))
        self.assertIsNone(strategy._entry_candidate("Up", book, 10, 0.37))

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
        up_book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.44, "size": 100}],
            "bids": [],
        }
        down_book = {
            "tickSize": 0.01,
            "minOrderSize": 1,
            "asks": [{"price": 0.45, "size": 100}],
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
        self.assertAlmostEqual(execution["fee"], strategy.sim.taker_fee(10.0, 0.405))

    async def test_second_leg_failure_triggers_emergency_unwind(self):
        first = {"side": "Up", "edge": 0.1}
        second = {"side": "Down", "edge": 0.05}
        fair = {"fairUp": 0.6, "fairDown": 0.4}
        first.update({"riskNotional": 2.0, "fee": 0.1, "shares": 5})
        second.update({"riskNotional": 2.0, "fee": 0.1, "shares": 5})
        with (
            patch.object(strategy, "_enter_position", AsyncMock(return_value="filled")),
            patch.object(strategy, "_hedge_position", AsyncMock(return_value="not_filled")),
            patch.object(strategy, "_emergency_unwind", AsyncMock()) as unwind,
        ):
            await strategy._execute_direct_pair(None, "btc-window", first, second, fair, True)
        unwind.assert_awaited_once()

    async def test_no_new_entry_near_resolution(self):
        with patch.object(strategy, "_strategy_cash", AsyncMock()) as cash:
            await strategy.evaluate_and_act("btc-window", None, 30.0, {"fairUp": 0.5, "fairDown": 0.5})
        cash.assert_not_awaited()

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


if __name__ == "__main__":
    unittest.main()
