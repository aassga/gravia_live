import os
import tempfile
import unittest

import polymarket_server as sim


class PolymarketSimulationTests(unittest.TestCase):
    def setUp(self):
        self._old_db_path = sim.SIM_DB_PATH
        self._tmpdir = tempfile.TemporaryDirectory()
        sim.SIM_DB_PATH = os.path.join(self._tmpdir.name, "simulation.sqlite3")
        if sim._sim_db is not None:
            sim._sim_db.close()
        sim._sim_db = None
        sim.shared_config.update({"startBalance": 100.0, "stakePct": 15.0, "runId": 1})
        for variant in sim.AB_VARIANTS:
            sim.ab_states[variant["id"]] = sim._new_variant_state()
        sim.sim_state = sim.ab_states["btc-main"]
        for market in sim.markets_state.values():
            market["upBook"] = {"bids": [], "asks": []}
            market["downBook"] = {"bids": [], "asks": []}
            market["windowOpenSpotPrice"] = None
            market["spotPrice"] = None
        sim._mm_seen_trade_keys.clear()
        sim._mm_seen_trade_key_set.clear()

    def tearDown(self):
        if sim._sim_db is not None:
            sim._sim_db.close()
        sim._sim_db = None
        sim.SIM_DB_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def test_buy_fill_uses_depth_vwap_slippage_and_fee(self):
        book = {
            "tickSize": 0.01,
            "asks": [
                {"price": 0.40, "size": 5.0},
                {"price": 0.42, "size": 5.0},
            ]
        }
        fill = sim.simulate_buy_fill(book, 10.0)
        expected_vwap = 0.41 * (1 + sim.SIM_SLIPPAGE_BPS / 10_000)
        self.assertAlmostEqual(fill["vwap"], expected_vwap)
        self.assertAlmostEqual(fill["fee"], sim.taker_fee(10.0, expected_vwap))
        # 對齊最差 tick（0.42）之後，再多讓一格 tick 提高成交機率，變成 0.44。
        self.assertEqual(fill["decisionPrice"], 0.44)
        self.assertAlmostEqual(fill["decisionFee"], sim.taker_fee(10.0, 0.44))
        self.assertGreater(fill["fee"], 0)

    def test_fill_rejects_insufficient_depth(self):
        book = {"asks": [{"price": 0.40, "size": 2.0}]}
        self.assertIsNone(sim.simulate_buy_fill(book, 3.0))

    def test_98_cent_pair_is_negative_after_taker_fees(self):
        pos = {
            "shares": 100.0,
            "side": "Up",
            "entryPrice": 0.44,
            "entryNotional": 44.0,
            "entryFee": sim.taker_fee(100.0, 0.44),
            "hedged": True,
            "hedgeShares": 100.0,
            "hedgePrice": 0.54,
            "hedgeNotional": 54.0,
            "hedgeFee": sim.taker_fee(100.0, 0.54),
        }
        self.assertLess(sim._settle_pnl(pos, "Up"), 0)

    def test_direct_pair_requires_positive_net_lock(self):
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        self.assertTrue(sim._try_direct_pair("btc-main", "btc-window", up_book, down_book))
        position = sim.ab_states["btc-main"]["position"]
        self.assertTrue(position["hedged"])
        self.assertGreater(position["lockedPnl"], 0)
        cash, portfolio = sim.compute_cash_and_portfolio("btc-main")
        self.assertGreater(cash, 0)
        self.assertAlmostEqual(portfolio, 100.0 + position["lockedPnl"])

    def test_direct_pair_rejects_when_below_real_min_order_shares(self):
        # Polymarket 真正的下限是股數（查證過真實 API 是 5 股），不是金額——就算金額、
        # 深度都夠，股數不到 minOrderSize 一樣不能進場。
        up_book = {"tickSize": 0.01, "minOrderSize": 1000, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "minOrderSize": 1000, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        self.assertFalse(sim._try_direct_pair("btc-main", "btc-window", up_book, down_book))

    def test_direct_pair_uses_same_tick_aligned_decision_price_as_live(self):
        # 數字挑在剛好卡在 btc-main 目前的 lockMaxSum（0.95）兩側：樂觀的 vwap 加總
        # 看起來夠便宜會通過，但保守的 tick-aligned 決策價加總超過門檻，應該被拒絕。
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.46, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.48, "size": 1_000.0}], "bids": []}
        up_fill = sim.simulate_buy_fill(up_book, 10.0)
        down_fill = sim.simulate_buy_fill(down_book, 10.0)
        self.assertLess(up_fill["vwap"] + down_fill["vwap"], 0.95)
        # 每腿再多讓一格 tick 的緩衝，兩腿加總比只對齊到最差 tick 多 0.02。
        self.assertEqual(up_fill["decisionPrice"] + down_fill["decisionPrice"], 0.98)
        self.assertFalse(sim._try_direct_pair("btc-main", "btc-window", up_book, down_book))

    def test_window_roll_does_not_clear_other_variant_position(self):
        main_pos = {"windowSlug": "btc-window-a"}
        loose_pos = {"windowSlug": "btc-window-b"}
        sim.ab_states["btc-main"]["position"] = main_pos
        sim.ab_states["btc-loose"]["position"] = loose_pos
        sim.queue_settlement("btc-window-a")
        self.assertIsNone(sim.ab_states["btc-main"]["position"])
        self.assertEqual(sim.ab_states["btc-main"]["pendingSettlements"], [main_pos])
        self.assertIs(sim.ab_states["btc-loose"]["position"], loose_pos)

    def test_pending_directional_position_keeps_capital_reserved(self):
        pos = {
            "shares": 10.0,
            "side": "Up",
            "entryPrice": 0.30,
            "entryNotional": 3.0,
            "entryFee": sim.taker_fee(10.0, 0.30),
            "hedged": False,
        }
        sim.ab_states["btc-main"]["pendingSettlements"] = [pos]
        cash, portfolio = sim.compute_cash_and_portfolio("btc-main")
        expected = 100.0 - sim._position_paid_cost(pos)
        self.assertAlmostEqual(cash, expected)
        self.assertAlmostEqual(portfolio, expected)

    def test_state_survives_restart(self):
        sim.ab_states["btc-main"]["totalPnl"] = 12.34
        sim.save_sim_state()
        sim.ab_states["btc-main"] = sim._new_variant_state()
        sim.sim_state = sim.ab_states["btc-main"]
        sim.load_sim_state()
        self.assertEqual(sim.ab_states["btc-main"]["totalPnl"], 12.34)
        self.assertIs(sim.sim_state, sim.ab_states["btc-main"])

    def test_late_direction_skips_outside_window(self):
        ms = sim.markets_state["btc"]
        ms["windowOpenSpotPrice"] = 100.0
        ms["spotPrice"] = 100.5  # +0.5%，遠超門檻
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.60, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        sim._try_late_direction_entry("btc-late-direction", "btc-window", up_book, down_book, remaining_seconds=30.0)
        self.assertIsNone(sim.ab_states["btc-late-direction"]["position"])

    def test_late_direction_enters_favored_side_near_close(self):
        ms = sim.markets_state["btc"]
        ms["windowOpenSpotPrice"] = 100.0
        ms["spotPrice"] = 100.5  # +0.5%，偏 Up
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.60, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        sim._try_late_direction_entry("btc-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        pos = sim.ab_states["btc-late-direction"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertFalse(pos["hedged"])

    def test_late_direction_position_never_auto_hedges(self):
        ms = sim.markets_state["btc"]
        ms["windowOpenSpotPrice"] = 100.0
        ms["spotPrice"] = 100.5
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.60, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.30, "size": 1_000.0}], "bids": []}  # 便宜到能鎖利
        sim._try_late_direction_entry("btc-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        self.assertFalse(sim.ab_states["btc-late-direction"]["position"]["hedged"])
        sim.simulate_trading("btc-late-direction", "btc-window", up_book, down_book, remaining_seconds=4.0, fair=None)
        self.assertFalse(sim.ab_states["btc-late-direction"]["position"]["hedged"])

    def test_websocket_book_change_notifies_registered_listener(self):
        received = []
        callback = received.append
        old_enabled = sim._ws_simulation_ticks_enabled
        sim.set_ws_simulation_ticks_enabled(False)
        sim.register_ws_price_listener(callback)
        try:
            sim._ws_apply_message(
                {
                    "event_type": "book",
                    "asset_id": "token-a",
                    "bids": [{"price": "0.39", "size": "5"}],
                    "asks": [{"price": "0.40", "size": "5"}],
                }
            )
        finally:
            sim.unregister_ws_price_listener(callback)
            sim.set_ws_simulation_ticks_enabled(old_enabled)
        self.assertEqual(received, ["token-a"])
        self.assertEqual(sim._ws_get_book("token-a")["quoteSource"], "websocket")

    def _eth_mm_books(self, bid=0.45, ask=0.46, queue=10.0):
        book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "bids": [{"price": bid, "size": queue}],
            "asks": [{"price": ask, "size": 100.0}],
        }
        return dict(book), dict(book)

    def _prepare_eth_mm(self):
        ms = sim.markets_state["eth"]
        ms["upTokenId"] = "eth-up-token"
        ms["downTokenId"] = "eth-down-token"
        return ms

    def test_eth_asset_has_only_market_maker_variant(self):
        eth = next(asset for asset in sim.ASSETS if asset["id"] == "eth")
        variants = [v for v in sim.AB_VARIANTS if v["assetId"] == "eth"]
        self.assertTrue(eth["marketMakerOnly"])
        self.assertEqual([v["id"] for v in variants], ["eth-mm"])
        self.assertTrue(variants[0]["marketMakerOnly"])

    def test_eth_maker_waits_for_queue_ahead_before_fill(self):
        self._prepare_eth_mm()
        up_book, down_book = self._eth_mm_books()
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        quote = sim.ab_states["eth-mm"]["makerQuotes"]["Up"]
        self.assertEqual(quote["price"], 0.45)
        self.assertEqual(quote["queueAhead"], 10.0)

        sim.process_market_maker_trade("eth-up-token", {
            "ts": quote["placedAt"] + 0.05, "price": 0.45, "size": 1_000.0, "side": "BUY",
        })
        self.assertIsNone(sim.ab_states["eth-mm"]["position"])
        self.assertEqual(quote["queueAhead"], 10.0)

        sim.process_market_maker_trade("eth-up-token", {
            "ts": quote["placedAt"] + 0.1, "price": 0.45, "size": 10.0, "side": "SELL",
        })
        self.assertIsNone(sim.ab_states["eth-mm"]["position"])
        shares = quote["shares"]
        sim.process_market_maker_trade("eth-up-token", {
            "ts": quote["placedAt"] + 0.2, "price": 0.45, "size": shares, "side": "SELL",
        })
        pos = sim.ab_states["eth-mm"]["position"]
        self.assertEqual(pos["side"], "Up")
        self.assertTrue(pos["maker"])
        self.assertEqual(pos["entryFee"], 0.0)

    def test_eth_maker_two_queued_fills_create_locked_pair(self):
        self._prepare_eth_mm()
        up_book, down_book = self._eth_mm_books()
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        up_quote = sim.ab_states["eth-mm"]["makerQuotes"]["Up"]
        sim.process_market_maker_trade("eth-up-token", {
            "ts": up_quote["placedAt"] + 0.1,
            "price": up_quote["price"] - 0.01,
            "size": up_quote["shares"],
            "side": "SELL",
        })
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 110.0, None)
        down_quote = sim.ab_states["eth-mm"]["makerQuotes"]["Down"]
        sim.process_market_maker_trade("eth-down-token", {
            "ts": down_quote["placedAt"] + 0.1,
            "price": down_quote["price"] - 0.01,
            "size": down_quote["shares"],
            "side": "SELL",
        })
        state = sim.ab_states["eth-mm"]
        self.assertTrue(state["position"]["hedged"])
        self.assertTrue(state["position"]["maker"])
        self.assertGreater(state["position"]["lockedPnl"], 0)
        self.assertEqual(state["makerStats"]["fills"], 2)
        self.assertEqual(state["makerStats"]["pairedFills"], 1)

    def test_eth_maker_rejects_unprofitable_pair_and_stops_near_close(self):
        self._prepare_eth_mm()
        expensive_up, expensive_down = self._eth_mm_books(bid=0.50, ask=0.51)
        sim.simulate_trading("eth-mm", "eth-window", expensive_up, expensive_down, 120.0, None)
        quotes = sim.ab_states["eth-mm"]["makerQuotes"]
        self.assertIsNone(quotes["Up"])
        self.assertIsNone(quotes["Down"])

        up_book, down_book = self._eth_mm_books()
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        self.assertIsNotNone(sim.ab_states["eth-mm"]["makerQuotes"]["Up"])
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 10.0, None)
        self.assertIsNone(sim.ab_states["eth-mm"]["makerQuotes"]["Up"])
        self.assertIsNone(sim.ab_states["eth-mm"]["makerQuotes"]["Down"])

    def test_eth_maker_metrics_are_exposed_to_dashboard(self):
        self._prepare_eth_mm()
        up_book, down_book = self._eth_mm_books()
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        row = next(r for r in sim.build_ab_leaderboard() if r["id"] == "eth-mm")
        self.assertEqual(row["strategyType"], "maker")
        self.assertEqual(row["makerStats"]["quotesPlaced"], 2)
        self.assertIsNotNone(row["makerQuotes"]["Up"])


if __name__ == "__main__":
    unittest.main()
