import os
import tempfile
import time
import unittest
from unittest.mock import patch

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
            market["market"] = None
            market["chainlinkTwapPrice"] = None
            market["chainlinkTwapObservedAt"] = None
            market["windowOpenChainlinkTwapPrice"] = None
            market["windowOpenChainlinkTwapObservedAt"] = None
            market["windowOpenChainlinkTwapSlug"] = None
        sim._mm_seen_trade_keys.clear()
        sim._mm_seen_trade_key_set.clear()
        sim._ws_books.clear()
        sim._ws_snapshot_tokens.clear()
        sim._ws_book_updated_at.clear()
        sim._sim_data_guard_log_at.clear()
        sim._pair_stability_candidates.clear()
        sim._direction_stability_candidates.clear()
        sim._chainlink_twap_history.clear()
        sim._chainlink_twap_latest.clear()

    def _set_chainlink_signal(self, opening=100.0, current=100.5, asset_id="btc", slug="btc-window"):
        ms = sim.markets_state[asset_id]
        ms["market"] = {"slug": slug}
        ms["windowOpenChainlinkTwapSlug"] = slug
        ms["windowOpenChainlinkTwapPrice"] = opening
        ms["windowOpenChainlinkTwapObservedAt"] = int(time.time() * 1000) - 300_000
        ms["chainlinkTwapPrice"] = current
        ms["chainlinkTwapObservedAt"] = int(time.time() * 1000)
        return ms

    def _set_binance_signal(self, opening=100.0, current=100.5):
        ms = sim.markets_state["btc"]
        ms["market"] = {"slug": "btc-window"}
        ms["windowOpenSpotPrice"] = opening
        ms["spotPrice"] = current
        return ms

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

    def test_live_lock_variant_mirrors_live_sizing_and_disables_directional_entry(self):
        variant = sim.AB_VARIANT_BY_ID["btc-live-lock"]
        self.assertTrue(variant["liveMirrorOnly"])
        self.assertEqual(variant["lockMaxSum"], sim.LIVE_MIRROR_LOCK_MAX_SUM)
        self.assertEqual(variant["stakePct"], sim.LIVE_MIRROR_STAKE_PCT)
        self.assertEqual(variant["maxPairBudgetUsd"], sim.LIVE_MIRROR_MAX_PAIR_BUDGET_USD)
        self.assertEqual(variant["minCashReserveUsd"], sim.LIVE_MIRROR_MIN_CASH_RESERVE_USD)
        self.assertEqual(variant["minDepthMultiplier"], sim.LIVE_MIRROR_DEPTH_MULTIPLIER)
        self.assertEqual(variant["stabilitySeconds"], sim.LIVE_MIRROR_STABILITY_SECONDS)

        expected = sim.target_pair_order(
            sim.shared_config["startBalance"],
            sim.LIVE_MIRROR_STAKE_PCT,
            sim.LIVE_MIRROR_LOCK_MAX_SUM,
            sim.LIVE_MIRROR_MAX_PAIR_BUDGET_USD,
            sim.LIVE_MIRROR_MIN_CASH_RESERVE_USD,
        )
        self.assertEqual(sim._target_order_size("btc-live-lock"), expected)

        # 即使符合晚進場方向訊號，只要沒有兩腿鎖利機會，實盤鏡像組仍保持空手。
        sim.markets_state["btc"]["windowOpenSpotPrice"] = 100.0
        sim.markets_state["btc"]["spotPrice"] = 101.0
        up_book = {"tickSize": 0.01, "minOrderSize": 5.0, "asks": [{"price": 0.60, "size": 100.0}], "bids": []}
        down_book = {"tickSize": 0.01, "minOrderSize": 5.0, "asks": [{"price": 0.60, "size": 100.0}], "bids": []}
        sim.simulate_trading("btc-live-lock", "btc-window", up_book, down_book, 5.0, None)
        self.assertIsNone(sim.ab_states["btc-live-lock"]["position"])

    def test_live_lock_waits_for_stable_deep_pair_before_simulated_fill(self):
        up_book = {"tickSize": 0.01, "minOrderSize": 5.0, "asks": [{"price": 0.39, "size": 500.0}], "bids": []}
        down_book = {"tickSize": 0.01, "minOrderSize": 5.0, "asks": [{"price": 0.39, "size": 500.0}], "bids": []}

        with (
            patch.dict(sim.AB_VARIANT_BY_ID["btc-live-lock"], {"stabilitySeconds": 0.15}),
            patch.object(sim.time, "monotonic", side_effect=[100.0, 100.16]),
        ):
            self.assertFalse(sim._try_direct_pair("btc-live-lock", "btc-window", up_book, down_book))
            self.assertTrue(sim._try_direct_pair("btc-live-lock", "btc-window", up_book, down_book))

        self.assertTrue(sim.ab_states["btc-live-lock"]["position"]["hedged"])

    def test_pair_stability_allows_prices_and_size_to_move_while_opportunity_remains_valid(self):
        self.assertFalse(
            sim.pair_candidate_is_stable("live:pair", "btc-window", 5, 0.40, 0.50, 0.25, now=10.0)
        )
        self.assertTrue(
            sim.pair_candidate_is_stable("live:pair", "btc-window", 6, 0.42, 0.48, 0.25, now=10.26)
        )
        self.assertFalse(
            sim.pair_candidate_is_stable("live:pair", "btc-next-window", 6, 0.42, 0.48, 0.25, now=10.27)
        )

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
        self._set_binance_signal()
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": [{"price": 0.60, "size": 1_000.0}]}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": [{"price": 0.39, "size": 1_000.0}]}
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=30.0)
        self.assertIsNone(sim.ab_states["btc-binance-late-direction"]["position"])

        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=2.0)
        self.assertIsNone(sim.ab_states["btc-binance-late-direction"]["position"])

    def test_late_direction_enters_favored_side_near_close(self):
        self._set_binance_signal()
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": [{"price": 0.60, "size": 1_000.0}]}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": [{"price": 0.39, "size": 1_000.0}]}
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        pos = sim.ab_states["btc-binance-late-direction"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertFalse(pos["hedged"])
        self.assertEqual(pos["signalSource"], "binance_futures_window")

    def test_btc_5m_uses_separate_binance_variant_for_clean_comparison(self):
        ids = [v["id"] for v in sim.AB_VARIANTS if v["assetId"] == "btc"]
        self.assertIn("btc-binance-late-direction", ids)
        self.assertIn("btc-historical-hybrid", ids)
        self.assertNotIn("btc-chainlink-late-direction", ids)
        variant = sim.AB_VARIANT_BY_ID["btc-binance-late-direction"]
        self.assertEqual(variant["directionSignalSource"], "binance_window")
        hybrid = sim.AB_VARIANT_BY_ID["btc-historical-hybrid"]
        self.assertEqual(hybrid["directionSignalSource"], "chainlink_twap")

    def test_historical_hybrid_prioritizes_direct_pair(self):
        self._set_binance_signal()
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}

        sim.simulate_trading("btc-historical-hybrid", "btc-window", up_book, down_book, 5.0, None)

        pos = sim.ab_states["btc-historical-hybrid"]["position"]
        self.assertIsNotNone(pos)
        self.assertTrue(pos["hedged"])
        self.assertGreater(pos["lockedPnl"], 0)

    def test_historical_hybrid_falls_back_to_chainlink_late_direction(self):
        self._set_chainlink_signal(opening=100.0, current=100.5)
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}

        sim.simulate_trading("btc-historical-hybrid", "btc-window", up_book, down_book, 5.0, None)

        pos = sim.ab_states["btc-historical-hybrid"]["position"]
        self.assertIsNotNone(pos)
        self.assertFalse(pos["hedged"])
        self.assertEqual(pos["side"], "Up")
        self.assertEqual(pos["signalSource"], "chainlink_twap_60s")

    def test_chainlink_tick_drives_historical_hybrid_but_not_binance_variant(self):
        ms = self._set_chainlink_signal(opening=100.0, current=100.5)
        ms["windowEndsAt"] = (time.time() + 5.0) * 1000
        ms["upBook"] = {"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": []}
        ms["downBook"] = {"tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}

        with (
            patch.object(sim, "_simulation_books_are_coherent", return_value=True),
            patch.object(sim, "simulate_trading") as simulate,
        ):
            sim._on_chainlink_twap_tick()

        triggered = [call.args[0] for call in simulate.call_args_list]
        self.assertIn("btc-historical-hybrid", triggered)
        self.assertNotIn("btc-binance-late-direction", triggered)

    def test_late_direction_position_never_auto_hedges(self):
        self._set_binance_signal()
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": [{"price": 0.60, "size": 1_000.0}]}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.30, "size": 1_000.0}], "bids": [{"price": 0.29, "size": 1_000.0}]}  # 便宜到能鎖利
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        self.assertFalse(sim.ab_states["btc-binance-late-direction"]["position"]["hedged"])
        sim.simulate_trading("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=4.0, fair=None)
        self.assertFalse(sim.ab_states["btc-binance-late-direction"]["position"]["hedged"])

    def test_late_direction_allows_original_market_disagreement_behavior(self):
        self._set_binance_signal(opening=100.0, current=99.5)
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.98, "size": 1_000.0}], "bids": [{"price": 0.97, "size": 1_000.0}]}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.20, "size": 1_000.0}], "bids": []}
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        pos = sim.ab_states["btc-binance-late-direction"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Down")

    def test_btc_15m_uses_only_dedicated_lock_and_adaptive_direction_variants(self):
        variants = [v for v in sim.AB_VARIANTS if v["assetId"] == "btc-15m"]
        self.assertEqual(
            [v["id"] for v in variants],
            ["btc-15m-adaptive-lock", "btc-15m-chainlink-late-direction"],
        )
        self.assertTrue(variants[0]["executionSafePair"])
        self.assertEqual(variants[0]["minDepthMultiplier"], 2.0)
        self.assertEqual(variants[1]["directionProfile"], "btc-15m-adaptive")
        self.assertEqual(variants[1]["stakePct"], 5.0)

    def test_btc_15m_lock_requires_continuous_deep_pair(self):
        up_book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": 0.39, "size": 1_000.0}],
            "bids": [],
        }
        down_book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": 0.39, "size": 1_000.0}],
            "bids": [],
        }
        with patch.object(sim.time, "monotonic", side_effect=[100.0, 100.21]):
            self.assertFalse(
                sim._try_direct_pair("btc-15m-adaptive-lock", "btc-15m-window", up_book, down_book)
            )
            self.assertTrue(
                sim._try_direct_pair("btc-15m-adaptive-lock", "btc-15m-window", up_book, down_book)
            )
        self.assertTrue(sim.ab_states["btc-15m-adaptive-lock"]["position"]["hedged"])

    def test_btc_15m_adaptive_direction_requires_stable_chainlink_and_market_agreement(self):
        slug = "btc-updown-15m-test"
        self._set_chainlink_signal(
            opening=100.0, current=100.15, asset_id="btc-15m", slug=slug
        )
        up_book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": 0.80, "size": 1_000.0}],
            "bids": [{"price": 0.79, "size": 1_000.0}],
        }
        down_book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": 0.20, "size": 1_000.0}],
            "bids": [{"price": 0.19, "size": 1_000.0}],
        }
        with patch.object(sim.time, "monotonic", side_effect=[100.0, 101.6]):
            sim._try_late_direction_entry(
                "btc-15m-chainlink-late-direction", slug, up_book, down_book, 20.0
            )
            self.assertIsNone(sim.ab_states["btc-15m-chainlink-late-direction"]["position"])
            sim._try_late_direction_entry(
                "btc-15m-chainlink-late-direction", slug, up_book, down_book, 19.0
            )
        pos = sim.ab_states["btc-15m-chainlink-late-direction"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertGreaterEqual(pos["fairProbability"], sim.BTC_15M_DIRECTION_MIN_PROBABILITY)
        self.assertGreaterEqual(pos["entryEdge"], sim.BTC_15M_DIRECTION_MIN_EDGE_PER_SHARE)
        self.assertLessEqual(
            pos["entryDecisionNotional"] + pos["entryDecisionFee"],
            sim.BTC_15M_DIRECTION_MAX_BUDGET_USD,
        )

    def test_btc_15m_adaptive_direction_rejects_market_disagreement(self):
        slug = "btc-updown-15m-disagree"
        self._set_chainlink_signal(
            opening=100.0, current=100.20, asset_id="btc-15m", slug=slug
        )
        up_book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": 0.41, "size": 1_000.0}],
            "bids": [{"price": 0.40, "size": 1_000.0}],
        }
        down_book = {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": 0.61, "size": 1_000.0}],
            "bids": [{"price": 0.60, "size": 1_000.0}],
        }
        sim._try_late_direction_entry(
            "btc-15m-chainlink-late-direction", slug, up_book, down_book, 20.0
        )
        self.assertIsNone(sim.ab_states["btc-15m-chainlink-late-direction"]["position"])

    def test_btc_15m_signal_scales_confidence_with_remaining_volatility(self):
        now_ms = int(time.time() * 1000)
        sim._chainlink_twap_history.extend(
            (now_ms - (60 - i * 10) * 1000, 100.0 + (0.03 if i % 2 else -0.03))
            for i in range(7)
        )
        signal = {"opening": 100.0, "current": 100.1, "observedAt": now_ms}
        short = sim.estimate_btc_15m_direction_signal(signal, 12.0)
        long = sim.estimate_btc_15m_direction_signal(signal, 30.0)
        self.assertGreater(short["probability"], long["probability"])
        self.assertLess(short["projectedSigmaPct"], long["projectedSigmaPct"])

    def test_chainlink_signal_rejects_stale_observation(self):
        self._set_chainlink_signal()
        sim.markets_state["btc"]["chainlinkTwapObservedAt"] = int(
            (time.time() - sim.CHAINLINK_TWAP_MAX_AGE_SECONDS - 1) * 1000
        )
        self.assertIsNone(sim.get_chainlink_twap_signal("btc"))

    def test_chainlink_rtds_history_captures_exact_window_open(self):
        start = int(time.time())
        ms = sim.markets_state["btc"]
        ms["market"] = {"slug": f"btc-updown-5m-{start}"}
        applied = sim._apply_chainlink_twap_message({
            "topic": "crypto_prices_twap_sixty",
            "type": "subscribe",
            "payload": {
                "symbol": "btc/usd",
                "window_s": 60,
                "data": [{
                    "timestamp": start * 1000,
                    "full_accuracy_value": "100500000000000000000",
                }],
            },
        })
        self.assertEqual(applied, 1)
        self.assertTrue(sim._capture_window_open_chainlink_twap(ms))
        self.assertEqual(ms["windowOpenChainlinkTwapPrice"], 100.5)
        self.assertEqual(ms["windowOpenChainlinkTwapSlug"], ms["market"]["slug"])

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

    def test_binance_tick_notifies_live_listener_immediately_for_btc(self):
        received = []
        callback = received.append
        ms = sim.markets_state["btc"]
        ms["market"] = {"slug": "btc-window"}
        ms["upTokenId"] = "btc-up-token"
        sim.register_ws_price_listener(callback)
        try:
            with (
                patch.object(sim, "get_binance_ws_price", return_value=100.5),
                patch.object(sim, "estimate_fair_up", return_value=None),
            ):
                sim._on_binance_price_tick("BTCUSDT")
        finally:
            sim.unregister_ws_price_listener(callback)
        self.assertEqual(received, ["btc-up-token"])
        self.assertEqual(ms["spotPrice"], 100.5)

    def test_simulation_data_guard_rejects_mixed_reconnect_snapshots(self):
        now = 100.0
        fresh_up = {
            "quoteSource": "websocket",
            "receivedAtMonotonic": now,
            "asks": [{"price": 0.09, "size": 100.0}],
        }
        stale_down = {
            "quoteSource": "initial_rest_snapshot",
            "receivedAtMonotonic": now - 2.0,
            "asks": [{"price": 0.60, "size": 100.0}],
        }
        reason = sim._simulation_book_guard_reason(fresh_up, stale_down, now)
        self.assertIn("WebSocket", reason)
        self.assertFalse(sim._simulation_books_are_coherent("btc", fresh_up, stale_down, now))

    def test_simulation_data_guard_rejects_stale_or_skewed_books(self):
        now = 100.0
        up = {"quoteSource": "websocket", "receivedAtMonotonic": now}
        stale_down = {
            "quoteSource": "websocket",
            "receivedAtMonotonic": now - sim.SIM_BOOK_MAX_AGE_SECONDS - 0.1,
        }
        self.assertIn("過舊", sim._simulation_book_guard_reason(up, stale_down, now))

        skewed_down = {
            "quoteSource": "websocket",
            "receivedAtMonotonic": now - sim.SIM_BOOK_MAX_SKEW_SECONDS - 0.1,
        }
        self.assertIn("時間差", sim._simulation_book_guard_reason(up, skewed_down, now))

    def test_simulation_data_guard_accepts_fresh_two_leg_websocket_books(self):
        now = 100.0
        up = {"quoteSource": "websocket", "receivedAtMonotonic": now - 0.10}
        down = {"quoteSource": "websocket", "receivedAtMonotonic": now - 0.15}
        self.assertIsNone(sim._simulation_book_guard_reason(up, down, now))
        self.assertTrue(sim._simulation_books_are_coherent("btc", up, down, now))

    def test_ws_tick_does_not_trade_until_both_reconnect_snapshots_arrive(self):
        ms = sim.markets_state["btc"]
        ms["market"] = {"slug": "btc-reconnect-window"}
        ms["windowEndsAt"] = (sim.real_now() + 120.0) * 1000
        ms["upTokenId"] = "reconnect-up"
        ms["downTokenId"] = "reconnect-down"
        sim._ws_books.update({
            "reconnect-up": {
                "bids": {"0.08": 100.0},
                "asks": {"0.09": 100.0},
            },
            "reconnect-down": {
                "bids": {"0.59": 100.0},
                "asks": {"0.60": 100.0},
            },
        })
        now = time.monotonic()
        sim._ws_snapshot_tokens.add("reconnect-up")
        sim._ws_book_updated_at.update({"reconnect-up": now, "reconnect-down": now - 2.0})

        sim._on_ws_price_tick("reconnect-up")

        for variant in sim.AB_VARIANTS:
            if variant["assetId"] == "btc":
                self.assertIsNone(sim.ab_states[variant["id"]]["position"])

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

    def test_altcoin_catalog_has_current_five_minute_markets(self):
        expected = {
            "eth-alt": ("eth-updown-5m-", "ETHUSDT"),
            "sol": ("sol-updown-5m-", "SOLUSDT"),
            "xrp": ("xrp-updown-5m-", "XRPUSDT"),
            "bnb": ("bnb-updown-5m-", "BNBUSDT"),
            "doge": ("doge-updown-5m-", "DOGEUSDT"),
            "hype": ("hype-updown-5m-", "HYPEUSDT"),
            "zec": ("zec-updown-5m-", "ZECUSDT"),
        }
        catalog = {asset["id"]: asset for asset in sim.ASSET_CATALOG}
        for asset_id, (slug_prefix, symbol) in expected.items():
            self.assertEqual(catalog[asset_id]["slugPrefix"], slug_prefix)
            self.assertEqual(catalog[asset_id]["binanceSymbol"], symbol)
            self.assertFalse(catalog[asset_id].get("marketMakerOnly", False))

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

    def test_eth_maker_caps_first_leg_quotes_at_sixty_cents(self):
        self._prepare_eth_mm()
        up_book = {
            "tickSize": 0.01, "minOrderSize": 5.0,
            "bids": [{"price": 0.63, "size": 100.0}],
            "asks": [{"price": 0.65, "size": 100.0}],
        }
        down_book = {
            "tickSize": 0.01, "minOrderSize": 5.0,
            "bids": [{"price": 0.29, "size": 100.0}],
            "asks": [{"price": 0.31, "size": 100.0}],
        }
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        quotes = sim.ab_states["eth-mm"]["makerQuotes"]
        self.assertEqual(quotes["Up"]["price"], sim.MM_FIRST_LEG_MAX_PRICE)
        self.assertEqual(quotes["Down"]["price"], 0.30)

    def test_eth_maker_rescue_taker_hedges_when_net_profit_is_available(self):
        self._prepare_eth_mm()
        up_book, down_book = self._eth_mm_books(bid=0.45, ask=0.46, queue=100.0)
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        quote = sim.ab_states["eth-mm"]["makerQuotes"]["Up"]
        sim.process_market_maker_trade("eth-up-token", {
            "ts": quote["placedAt"] + 0.1, "price": quote["price"] - 0.01,
            "size": quote["shares"], "side": "SELL",
        })
        state = sim.ab_states["eth-mm"]
        state["position"]["entryTime"] = time.time() - sim.MM_INVENTORY_RESCUE_SECONDS - 1

        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 100.0, None)

        self.assertTrue(state["position"]["hedged"])
        self.assertEqual(state["position"]["makerRescueAction"], "taker_hedge")
        self.assertGreater(state["position"]["lockedPnl"], 0)
        self.assertEqual(state["makerStats"]["rescueHedges"], 1)
        self.assertEqual(state["makerStats"]["pairedFills"], 1)

    def test_eth_maker_rescue_unwinds_when_profitable_hedge_is_unavailable(self):
        self._prepare_eth_mm()
        up_book, down_book = self._eth_mm_books(bid=0.45, ask=0.46, queue=100.0)
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        quote = sim.ab_states["eth-mm"]["makerQuotes"]["Up"]
        sim.process_market_maker_trade("eth-up-token", {
            "ts": quote["placedAt"] + 0.1, "price": quote["price"] - 0.01,
            "size": quote["shares"], "side": "SELL",
        })
        state = sim.ab_states["eth-mm"]
        state["position"]["entryTime"] = time.time() - sim.MM_INVENTORY_RESCUE_SECONDS - 1
        expensive_down = {
            "tickSize": 0.01, "minOrderSize": 5.0,
            "bids": [{"price": 0.53, "size": 100.0}],
            "asks": [{"price": 0.54, "size": 100.0}],
        }

        sim.simulate_trading("eth-mm", "eth-window", up_book, expensive_down, 100.0, None)

        self.assertIsNone(state["position"])
        self.assertEqual(state["makerStats"]["rescueUnwinds"], 1)
        self.assertEqual(state["makerStats"]["singleLegSettlements"], 1)
        self.assertEqual(state["trades"][0]["exitReason"], "maker_inventory_timeout")

    def test_eth_maker_does_not_rescue_before_inventory_timeout(self):
        self._prepare_eth_mm()
        up_book, down_book = self._eth_mm_books(queue=0.0)
        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 120.0, None)
        quote = sim.ab_states["eth-mm"]["makerQuotes"]["Up"]
        sim.process_market_maker_trade("eth-up-token", {
            "ts": quote["placedAt"] + 0.1, "price": quote["price"] - 0.01,
            "size": quote["shares"], "side": "SELL",
        })
        state = sim.ab_states["eth-mm"]
        state["position"]["entryTime"] = time.time() - sim.MM_INVENTORY_RESCUE_SECONDS + 1

        sim.simulate_trading("eth-mm", "eth-window", up_book, down_book, 100.0, None)

        self.assertFalse(state["position"]["hedged"])
        self.assertIsNotNone(state["makerQuotes"]["Down"])
        self.assertEqual(state["makerStats"]["rescueAttempts"], 0)

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
