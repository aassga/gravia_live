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
        sim.shared_config.update({"startBalance": 100.0, "stakePct": 3.0, "runId": 1})
        for variant in sim.AB_VARIANTS:
            sim.ab_states[variant["id"]] = sim._new_variant_state()
        sim.sim_state = sim.ab_states["main"]
        for market in sim.markets_state.values():
            market["upBook"] = {"bids": [], "asks": []}
            market["downBook"] = {"bids": [], "asks": []}

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
        self.assertEqual(fill["decisionPrice"], 0.43)
        self.assertAlmostEqual(fill["decisionFee"], sim.taker_fee(10.0, 0.43))
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
        self.assertTrue(sim._try_direct_pair("main", "btc-window", up_book, down_book))
        position = sim.ab_states["main"]["position"]
        self.assertTrue(position["hedged"])
        self.assertGreater(position["lockedPnl"], 0)
        cash, portfolio = sim.compute_cash_and_portfolio("main")
        self.assertGreater(cash, 0)
        self.assertAlmostEqual(portfolio, 100.0 + position["lockedPnl"])

    def test_direct_pair_uses_same_tick_aligned_decision_price_as_live(self):
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.44, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.45, "size": 1_000.0}], "bids": []}
        up_fill = sim.simulate_buy_fill(up_book, 10.0)
        down_fill = sim.simulate_buy_fill(down_book, 10.0)
        self.assertLess(up_fill["vwap"] + down_fill["vwap"], 0.90)
        self.assertEqual(up_fill["decisionPrice"] + down_fill["decisionPrice"], 0.91)
        self.assertFalse(sim._try_direct_pair("main", "btc-window", up_book, down_book))

    def test_window_roll_does_not_clear_other_variant_position(self):
        main_pos = {"windowSlug": "btc-window-a"}
        loose_pos = {"windowSlug": "btc-window-b"}
        sim.ab_states["main"]["position"] = main_pos
        sim.ab_states["loose"]["position"] = loose_pos
        sim.queue_settlement("btc-window-a")
        self.assertIsNone(sim.ab_states["main"]["position"])
        self.assertEqual(sim.ab_states["main"]["pendingSettlements"], [main_pos])
        self.assertIs(sim.ab_states["loose"]["position"], loose_pos)

    def test_pending_directional_position_keeps_capital_reserved(self):
        pos = {
            "shares": 10.0,
            "side": "Up",
            "entryPrice": 0.30,
            "entryNotional": 3.0,
            "entryFee": sim.taker_fee(10.0, 0.30),
            "hedged": False,
        }
        sim.ab_states["main"]["pendingSettlements"] = [pos]
        cash, portfolio = sim.compute_cash_and_portfolio("main")
        expected = 100.0 - sim._position_paid_cost(pos)
        self.assertAlmostEqual(cash, expected)
        self.assertAlmostEqual(portfolio, expected)

    def test_state_survives_restart(self):
        sim.ab_states["main"]["totalPnl"] = 12.34
        sim.save_sim_state()
        sim.ab_states["main"] = sim._new_variant_state()
        sim.sim_state = sim.ab_states["main"]
        sim.load_sim_state()
        self.assertEqual(sim.ab_states["main"]["totalPnl"], 12.34)
        self.assertIs(sim.sim_state, sim.ab_states["main"])


if __name__ == "__main__":
    unittest.main()
