import asyncio
import json
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
        sim._btc_15m_ask_history.clear()
        sim._window_diag_dirty.clear()
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

    def _fresh_ws_book(self, book: dict) -> dict:
        return {
            **book,
            "quoteSource": "websocket",
            "receivedAtMonotonic": time.monotonic(),
        }

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

    # 2026-09-11 依使用者要求重新啟用 entryMaxPrice 單邊進場：兩腿加總卡在 $1.00 鎖不到時，
    # 有設 entryMaxPrice 的組（conservative／main／loose）改用公平價模型先買便宜那一腿。
    def test_single_leg_entry_fires_when_lock_impossible_and_cheap_side_has_edge(self):
        # 加總 1.00，鎖利門檻 0.95 碰不到；但 Up 只要 0.30、模型認為 Up 有 60% 機率 → 有 edge
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.30, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.70, "size": 1_000.0}], "bids": []}
        fair = {"fairUp": 0.60, "fairDown": 0.40}
        with patch.object(sim, "SIM_SINGLE_LEG_ENTRY_ENABLED", True):
            sim.simulate_trading("btc-main", "btc-window", up_book, down_book, 180.0, fair)
        pos = sim.ab_states["btc-main"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertFalse(pos["hedged"])
        self.assertGreater(pos["entryEdge"], sim.SIM_MIN_ENTRY_EDGE)

    def test_single_leg_entry_respects_entry_max_price(self):
        # main 的 entryMaxPrice=0.40：Up 賣 0.45 就算模型 edge 很大也不進
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.45, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.60, "size": 1_000.0}], "bids": []}
        fair = {"fairUp": 0.90, "fairDown": 0.10}
        with patch.object(sim, "SIM_SINGLE_LEG_ENTRY_ENABLED", True):
            sim.simulate_trading("btc-main", "btc-window", up_book, down_book, 180.0, fair)
            self.assertIsNone(sim.ab_states["btc-main"]["position"])
            # loose 的 entryMaxPrice=0.45（decision price 會多讓 tick 變 0.47，仍超過）→ 也不進
            sim.simulate_trading("btc-loose", "btc-window", up_book, down_book, 180.0, fair)
            self.assertIsNone(sim.ab_states["btc-loose"]["position"])

    def test_single_leg_entry_skipped_for_variants_without_entry_max_price(self):
        # historical-hybrid（實盤用的那組）entryMaxPrice=None，就算條件再好也不能走單邊路
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.30, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.70, "size": 1_000.0}], "bids": []}
        fair = {"fairUp": 0.60, "fairDown": 0.40}
        with patch.object(sim, "SIM_SINGLE_LEG_ENTRY_ENABLED", True):
            self.assertFalse(sim._try_single_leg_entry("btc-historical-hybrid", "btc-window", up_book, down_book, fair))
        self.assertIsNone(sim.ab_states["btc-historical-hybrid"]["position"])

    def _favorite_books(self, up_ask=0.92, down_ask=0.09):
        up = self._fresh_ws_book({"tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": up_ask, "size": 500.0}], "bids": [{"price": round(up_ask - 0.01, 2), "size": 500.0}]})
        down = self._fresh_ws_book({"tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": down_ask, "size": 500.0}], "bids": [{"price": max(0.01, round(down_ask - 0.01, 2)), "size": 500.0}]})
        return up, down

    def test_late_favorite_buys_leader_in_last_minute_and_holds(self):
        # 最後 60 秒、Up 賣 0.92（>= 0.90、<= 0.97）、Chainlink 同向 → 買 Up，抱到結算
        self._set_chainlink_signal(opening=100.0, current=100.3)
        up, down = self._favorite_books()
        sim.simulate_trading("btc-late-favorite", "btc-window", up, down, 40.0, None)
        pos = sim.ab_states["btc-late-favorite"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertFalse(pos["hedged"])
        self.assertEqual(pos.get("signalSource"), "late_favorite")
        # 同窗口不再進第二次；之後對邊變便宜也不補腿
        sim.ab_states["btc-late-favorite"]["position"] = None
        sim.simulate_trading("btc-late-favorite", "btc-window", up, down, 30.0, None)
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])

    def test_late_favorite_skips_outside_window_or_no_leader_or_too_expensive(self):
        self._set_chainlink_signal(opening=100.0, current=100.3)
        up, down = self._favorite_books()
        sim.simulate_trading("btc-late-favorite", "btc-window", up, down, 120.0, None)   # 還沒到最後 60 秒
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])
        sim.simulate_trading("btc-late-favorite", "btc-window", up, down, 3.0, None)     # 剩不到 5 秒
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])
        up2, down2 = self._favorite_books(up_ask=0.70, down_ask=0.31)                    # 沒有 >= 0.90 的領先方
        sim.simulate_trading("btc-late-favorite", "btc-window", up2, down2, 40.0, None)
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])
        up3, down3 = self._favorite_books(up_ask=0.99, down_ask=0.02)                    # 超過 0.97 沒利潤
        sim.simulate_trading("btc-late-favorite", "btc-window", up3, down3, 40.0, None)
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])

    def test_late_favorite_requires_chainlink_agreement_when_signal_present(self):
        # 市場領先 Up 但 Chainlink TWAP 低於開盤 → 不進
        self._set_chainlink_signal(opening=100.0, current=99.7)
        up, down = self._favorite_books()
        sim.simulate_trading("btc-late-favorite", "btc-window", up, down, 40.0, None)
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])

    def test_late_favorite_stop_loss_sells_when_leader_flips(self):
        # 12:33 那筆：買 Down 0.90 後翻面。停損 0.85：Down 買盤掉到 0.80 → 賣掉；掉到 0.88 → 不賣
        self._set_chainlink_signal(opening=100.0, current=99.7)
        up, down = self._favorite_books(up_ask=0.09, down_ask=0.92)
        sim.simulate_trading("btc-late-favorite", "btc-window", up, down, 40.0, None)
        self.assertEqual(sim.ab_states["btc-late-favorite"]["position"]["side"], "Down")
        up2, down2 = self._favorite_books(up_ask=0.10, down_ask=0.89)
        sim.simulate_trading("btc-late-favorite", "btc-window", up2, down2, 30.0, None)
        self.assertIsNotNone(sim.ab_states["btc-late-favorite"]["position"])
        up3, down3 = self._favorite_books(up_ask=0.21, down_ask=0.81)
        sim.simulate_trading("btc-late-favorite", "btc-window", up3, down3, 25.0, None)
        self.assertIsNone(sim.ab_states["btc-late-favorite"]["position"])
        last = sim.ab_states["btc-late-favorite"]["trades"][0]
        self.assertEqual(last["exitReason"], "favorite_stop_loss")
        self.assertLess(last["pnl"], 0)
        self.assertGreater(last["pnl"], -last["stakeUsd"])

    def test_btc_two_sided_maker_variant_uses_relaxed_parameters(self):
        v = sim.AB_VARIANT_BY_ID["btc-two-sided-maker"]
        self.assertTrue(v["marketMakerOnly"]); self.assertTrue(v["simOnly"])
        self.assertIn(v, sim.MARKET_MAKER_VARIANTS)
        self.assertEqual(v["mmMaxPairCost"], 0.99)
        self.assertEqual(v["mmFirstLegMaxPrice"], 0.70)
        self.assertEqual(v["mmRescueSeconds"], 45.0)
        # 兩邊 bid/ask 0.47/0.49 與 0.49/0.51：掛 0.48 + 0.50 = 0.98 <= 0.99 → 兩邊都掛
        up = self._fresh_ws_book({"tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": 0.49, "size": 300.0}], "bids": [{"price": 0.47, "size": 300.0}]})
        down = self._fresh_ws_book({"tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": 0.51, "size": 300.0}], "bids": [{"price": 0.49, "size": 300.0}]})
        sim.simulate_trading("btc-two-sided-maker", "btc-window", up, down, 200.0, None)
        quotes = sim.ab_states["btc-two-sided-maker"]["makerQuotes"]
        self.assertIsNotNone(quotes["Up"]); self.assertIsNotNone(quotes["Down"])
        self.assertLessEqual(quotes["Up"]["price"] + quotes["Down"]["price"], 0.99 + 1e-9)
        # ETH 版 0.98 上限下同樣的 book：0.48+0.50=0.98 也掛得出來；把 Down 提高一檔則只有 BTC 版還掛
        down_hi = self._fresh_ws_book({"tickSize": 0.01, "minOrderSize": 1,
            "asks": [{"price": 0.52, "size": 300.0}], "bids": [{"price": 0.50, "size": 300.0}]})
        sim.ab_states["btc-two-sided-maker"] = sim._new_variant_state()
        sim.simulate_trading("btc-two-sided-maker", "btc-window", up, down_hi, 200.0, None)
        quotes = sim.ab_states["btc-two-sided-maker"]["makerQuotes"]
        self.assertIsNotNone(quotes["Up"]); self.assertIsNotNone(quotes["Down"])
        self.assertAlmostEqual(quotes["Up"]["price"] + quotes["Down"]["price"], 0.99, places=6)

    def test_single_leg_entry_disabled_by_default_keeps_lock_only_behaviour(self):
        # 2026-09-12 預設關閉：鎖不到就空手，不走單邊
        self.assertFalse(sim.SIM_SINGLE_LEG_ENTRY_ENABLED)
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.30, "size": 1_000.0}], "bids": []}
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.70, "size": 1_000.0}], "bids": []}
        fair = {"fairUp": 0.60, "fairDown": 0.40}
        sim.simulate_trading("btc-main", "btc-window", up_book, down_book, 180.0, fair)
        self.assertIsNone(sim.ab_states["btc-main"]["position"])

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

    def test_empty_settlement_retry_does_not_rewrite_all_sim_state(self):
        async def scenario():
            with patch.object(sim, "save_sim_state") as save:
                await sim.retry_pending_settlements(None)
                save.assert_not_called()

        asyncio.run(scenario())

    def test_resolved_settlement_retry_persists_queue_removal(self):
        st = sim.ab_states["btc-main"]
        st["pendingSettlements"] = [{"windowSlug": "resolved-window", "hedged": False}]

        async def scenario():
            with (
                patch.object(sim, "fetch_outcome", return_value="Up"),
                patch.object(sim, "_settle_pnl", return_value=1.0),
                patch.object(sim, "record_trade"),
                patch.object(sim, "save_sim_state") as save,
            ):
                await sim.retry_pending_settlements(None)
                self.assertEqual(st["pendingSettlements"], [])
                save.assert_called_once_with()

        asyncio.run(scenario())

    def test_window_diagnostics_persist_each_window_and_rejection_reason(self):
        slug = "btc-updown-5m-diagnostic"
        sim.start_window_diagnostics("btc", slug, 123_000.0)
        self._set_binance_signal(opening=100.0, current=100.01)
        sim.markets_state["btc"]["market"] = {"slug": slug}
        up_book = self._fresh_ws_book({
            "tickSize": 0.01,
            "asks": [{"price": 0.61, "size": 1_000.0}],
            "bids": [],
        })
        down_book = {"quoteSource": "rest_fallback", "asks": [], "bids": []}

        sim._try_late_direction_entry(
            "btc-binance-late-direction", slug, up_book, down_book, remaining_seconds=5.0
        )
        sim.finalize_window_diagnostics(slug)
        db = sim._get_sim_db()
        sim.flush_window_diagnostics(db)
        db.commit()

        row = db.execute(
            """SELECT diagnostic_json FROM sim_window_diagnostics
               WHERE run_id=? AND variant_id=? AND window_slug=?""",
            (1, "btc-binance-late-direction", slug),
        ).fetchone()
        self.assertIsNotNone(row)
        diagnostic = json.loads(row[0])
        self.assertEqual(diagnostic["status"], "no_entry")
        self.assertGreater(diagnostic["reasonCounts"]["delta_below_minimum"], 0)
        self.assertAlmostEqual(diagnostic["maxAbsSignalDeltaPct"], 0.01)

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
        up_book = self._fresh_ws_book({"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": [{"price": 0.60, "size": 1_000.0}]})
        down_book = {"quoteSource": "rest_fallback", "tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": [{"price": 0.39, "size": 1_000.0}]}
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        pos = sim.ab_states["btc-binance-late-direction"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertFalse(pos["hedged"])
        self.assertEqual(pos["signalSource"], "binance_futures_window")

    def test_btc_5m_uses_separate_binance_variant_for_clean_comparison(self):
        ids = [v["id"] for v in sim.AB_VARIANTS if v["assetId"] == "btc"]
        self.assertNotIn("btc-conservative", ids)
        self.assertIn("btc-binance-late-direction", ids)
        self.assertIn("btc-historical-hybrid", ids)
        self.assertNotIn("btc-chainlink-late-direction", ids)
        variant = sim.AB_VARIANT_BY_ID["btc-binance-late-direction"]
        self.assertEqual(variant["directionSignalSource"], "binance_window")
        hybrid = sim.AB_VARIANT_BY_ID["btc-historical-hybrid"]
        self.assertEqual(hybrid["directionSignalSource"], "chainlink_twap")
        self.assertEqual(hybrid["lockMaxSum"], sim.LIVE_MIRROR_LOCK_MAX_SUM)
        self.assertEqual(hybrid["stakePct"], sim.LIVE_MIRROR_STAKE_PCT)
        self.assertEqual(hybrid["maxPairBudgetUsd"], sim.LIVE_MIRROR_MAX_PAIR_BUDGET_USD)
        self.assertEqual(hybrid["minCashReserveUsd"], sim.LIVE_MIRROR_MIN_CASH_RESERVE_USD)
        self.assertEqual(hybrid["minDepthMultiplier"], sim.LIVE_MIRROR_DEPTH_MULTIPLIER)
        self.assertEqual(hybrid["stabilitySeconds"], sim.LIVE_MIRROR_STABILITY_SECONDS)

    def test_historical_hybrid_prioritizes_direct_pair(self):
        self._set_binance_signal()
        received_at = time.monotonic()
        up_book = {"quoteSource": "websocket", "receivedAtMonotonic": received_at, "tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}
        down_book = {"quoteSource": "websocket", "receivedAtMonotonic": received_at, "tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}

        sim.simulate_trading("btc-historical-hybrid", "btc-window", up_book, down_book, 5.0, None)

        pos = sim.ab_states["btc-historical-hybrid"]["position"]
        self.assertIsNotNone(pos)
        self.assertTrue(pos["hedged"])
        self.assertGreater(pos["lockedPnl"], 0)

    def test_historical_hybrid_falls_back_to_chainlink_late_direction(self):
        self._set_chainlink_signal(opening=100.0, current=100.5)
        up_book = self._fresh_ws_book({"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": []})
        down_book = {"quoteSource": "rest_fallback", "tickSize": 0.01, "asks": [{"price": 0.40, "size": 1_000.0}], "bids": []}

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
        up_book = self._fresh_ws_book({"tickSize": 0.01, "asks": [{"price": 0.61, "size": 1_000.0}], "bids": [{"price": 0.60, "size": 1_000.0}]})
        down_book = {"tickSize": 0.01, "asks": [{"price": 0.30, "size": 1_000.0}], "bids": [{"price": 0.29, "size": 1_000.0}]}  # 便宜到能鎖利
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        self.assertFalse(sim.ab_states["btc-binance-late-direction"]["position"]["hedged"])
        sim.simulate_trading("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=4.0, fair=None)
        self.assertFalse(sim.ab_states["btc-binance-late-direction"]["position"]["hedged"])

    def test_late_direction_allows_original_market_disagreement_behavior(self):
        self._set_binance_signal(opening=100.0, current=99.5)
        up_book = {"tickSize": 0.01, "asks": [{"price": 0.98, "size": 1_000.0}], "bids": [{"price": 0.97, "size": 1_000.0}]}
        down_book = self._fresh_ws_book({"tickSize": 0.01, "asks": [{"price": 0.20, "size": 1_000.0}], "bids": []})
        sim._try_late_direction_entry("btc-binance-late-direction", "btc-window", up_book, down_book, remaining_seconds=5.0)
        pos = sim.ab_states["btc-binance-late-direction"]["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Down")

    def test_btc_15m_uses_only_dump_then_hedge_variant(self):
        self.assertNotIn("btc-4h", [asset["id"] for asset in sim.ASSETS])
        variants = [v for v in sim.AB_VARIANTS if v["assetId"] == "btc-15m"]
        self.assertEqual([v["id"] for v in variants], ["btc-15m-dump-then-hedge"])
        variant = variants[0]
        self.assertTrue(variant["dumpThenHedge"])
        self.assertEqual(variant["lookbackSeconds"], 3.0)
        self.assertEqual(variant["minMovePct"], 15.0)
        self.assertEqual(variant["entryWindowSeconds"], 120.0)
        self.assertEqual(variant["targetShares"], 5.0)
        self.assertEqual(variant["lockMaxSum"], 0.95)
        self.assertEqual(variant["minNetPerShare"], 0.01)

    @staticmethod
    def _timed_book(ask, observed_at, size=1_000.0):
        return {
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "asks": [{"price": ask, "size": size}],
            "bids": [{"price": max(0.01, ask - 0.01), "size": size}],
            "quoteSource": "websocket",
            "receivedAtMonotonic": observed_at,
        }

    def _trigger_15m_up_dump(self, slug="btc-updown-15m-test", remaining=895.0):
        vid = "btc-15m-dump-then-hedge"
        sim.simulate_trading(
            vid, slug, self._timed_book(0.80, 100.0), self._timed_book(0.20, 100.0),
            remaining, None,
        )
        with patch.object(sim.time, "monotonic", return_value=103.1):
            sim.simulate_trading(
                vid, slug, self._timed_book(0.64, 103.1), self._timed_book(0.20, 103.1),
                remaining - 3.1, None,
            )
        return sim.ab_states[vid]

    def test_btc_15m_enters_dropped_leg_in_first_two_minutes(self):
        st = self._trigger_15m_up_dump()
        pos = st["position"]
        self.assertIsNotNone(pos)
        self.assertEqual(pos["side"], "Up")
        self.assertEqual(pos["shares"], 5.0)
        self.assertAlmostEqual(pos["signalDropPct"], 20.0)
        self.assertEqual(pos["strategyMode"], "dump_then_hedge")
        self.assertEqual(st["dumpHedgeStats"]["leg1Entries"], 1)

    def test_btc_15m_does_not_enter_after_first_two_minutes(self):
        st = self._trigger_15m_up_dump(remaining=770.0)
        self.assertIsNone(st["position"])
        diag = st["windowDiagnostics"][0]
        self.assertEqual(diag["lastReason"], "outside_dump_entry_window")

    def test_btc_15m_requires_full_five_share_leg1_depth(self):
        vid = "btc-15m-dump-then-hedge"
        slug = "btc-updown-15m-shallow"
        sim.simulate_trading(
            vid, slug, self._timed_book(0.80, 100.0), self._timed_book(0.20, 100.0),
            895.0, None,
        )
        with patch.object(sim.time, "monotonic", return_value=103.1):
            sim.simulate_trading(
                vid, slug, self._timed_book(0.64, 103.1, size=4.0),
                self._timed_book(0.20, 103.1), 891.9, None,
            )
        self.assertIsNone(sim.ab_states[vid]["position"])
        self.assertEqual(sim.ab_states[vid]["dumpHedgeStats"]["leg1Rejected"], 1)

    def test_btc_15m_completes_fee_aware_opposite_leg_and_can_repeat(self):
        vid = "btc-15m-dump-then-hedge"
        slug = "btc-updown-15m-complete"
        st = self._trigger_15m_up_dump(slug=slug)
        with patch.object(sim.time, "monotonic", return_value=103.2):
            sim.simulate_trading(
                vid, slug, self._timed_book(0.64, 103.2), self._timed_book(0.20, 103.2),
                891.8, None,
            )
        self.assertIsNone(st["position"])
        self.assertEqual(len(st["pendingSettlements"]), 1)
        completed = st["pendingSettlements"][0]
        self.assertTrue(completed["hedged"])
        self.assertGreater(completed["lockedPnl"], 0)
        self.assertEqual(st["dumpHedgeStats"]["completedCycles"], 1)

        # 完成後清掉三秒歷史；同一個舊訊號不能在下一個 tick 立刻重複進場。
        with patch.object(sim.time, "monotonic", return_value=103.3):
            sim.simulate_trading(
                vid, slug, self._timed_book(0.64, 103.3), self._timed_book(0.20, 103.3),
                891.7, None,
            )
        self.assertIsNone(st["position"])

    def test_btc_15m_does_not_hedge_when_price_sum_or_net_is_not_safe(self):
        vid = "btc-15m-dump-then-hedge"
        slug = "btc-updown-15m-no-hedge"
        st = self._trigger_15m_up_dump(slug=slug)
        with patch.object(sim.time, "monotonic", return_value=103.2):
            sim.simulate_trading(
                vid, slug, self._timed_book(0.64, 103.2), self._timed_book(0.28, 103.2),
                891.8, None,
            )
        self.assertIsNotNone(st["position"])
        self.assertFalse(st["position"]["hedged"])
        self.assertEqual(st["windowDiagnostics"][0]["lastReason"], "dump_hedge_sum_above_maximum")

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

    def test_websocket_book_updates_are_coalesced_off_the_reader_path(self):
        async def scenario():
            old_event = sim._ws_tick_event
            old_task = sim._ws_tick_dispatch_task
            before_updates = sim._ws_perf["bookUpdates"]
            before_coalesced = sim._ws_perf["coalescedUpdates"]
            sim._pending_ws_price_ticks.clear()
            sim._pending_ws_tick_queued_at.clear()
            sim._ws_tick_event = asyncio.Event()
            # Mark a dispatcher as active without starting its loop; this lets
            # the test inspect and manually drain the coalesced queue.
            sim._ws_tick_dispatch_task = asyncio.current_task()
            dispatched = []
            try:
                with patch.object(sim, "_on_ws_price_tick", side_effect=dispatched.append):
                    sim._ws_apply_message({
                        "event_type": "book",
                        "asset_id": "coalesced-token",
                        "bids": [{"price": "0.39", "size": "5"}],
                        "asks": [{"price": "0.40", "size": "5"}],
                    })
                    sim._ws_apply_message({
                        "event_type": "price_change",
                        "price_changes": [{
                            "asset_id": "coalesced-token",
                            "side": "SELL",
                            "price": "0.40",
                            "size": "7",
                        }],
                    })
                    self.assertEqual(dispatched, [])
                    self.assertEqual(sim._pending_ws_price_ticks, {"coalesced-token"})
                    self.assertEqual(sim._drain_ws_price_ticks(), 1)
                    self.assertEqual(dispatched, ["coalesced-token"])
                    self.assertEqual(sim._ws_perf["bookUpdates"] - before_updates, 2)
                    self.assertEqual(sim._ws_perf["coalescedUpdates"] - before_coalesced, 1)
            finally:
                sim._pending_ws_price_ticks.clear()
                sim._pending_ws_tick_queued_at.clear()
                sim._ws_tick_event = old_event
                sim._ws_tick_dispatch_task = old_task

        asyncio.run(scenario())

    def test_live_action_runs_before_deferred_ws_simulation(self):
        events = []

        def live_listener(_token_id):
            events.append("live")
            return True

        async def scenario():
            sim.register_ws_price_listener(live_listener)
            try:
                with patch.object(
                    sim, "_run_ws_simulation_tick", side_effect=lambda _token: events.append("sim")
                ):
                    sim._on_ws_price_tick("priority-token")
                    self.assertEqual(events, ["live"])
                    await asyncio.sleep(0)
                    self.assertEqual(events, ["live", "sim"])
            finally:
                sim.unregister_ws_price_listener(live_listener)
                sim._pending_simulation_ticks.clear()

        asyncio.run(scenario())

    def test_live_action_runs_before_deferred_chainlink_simulation(self):
        events = []
        ms = sim.markets_state["btc"]
        ms.update({
            "market": {"slug": "btc-window"},
            "upTokenId": "btc-up-token",
            "upBook": {"asks": [{"price": 0.4, "size": 10}]},
            "downBook": {"asks": [{"price": 0.5, "size": 10}]},
        })

        def live_listener(_token_id):
            events.append("live")
            return True

        async def scenario():
            sim.register_ws_price_listener(live_listener)
            try:
                with (
                    patch.object(sim, "_capture_window_open_chainlink_twap"),
                    patch.object(
                        sim, "_run_chainlink_simulation_tick",
                        side_effect=lambda _aid: events.append("sim"),
                    ),
                ):
                    sim._on_chainlink_twap_tick()
                    self.assertEqual(events, ["live"])
                    await asyncio.sleep(0)
                    self.assertEqual(events, ["live", "sim"])
            finally:
                sim.unregister_ws_price_listener(live_listener)
                sim._pending_simulation_ticks.clear()

        asyncio.run(scenario())

    def test_live_action_runs_before_deferred_binance_simulation(self):
        events = []
        ms = sim.markets_state["btc"]
        ms.update({"market": {"slug": "btc-window"}, "upTokenId": "btc-up-token"})

        def live_listener(_token_id):
            events.append("live")
            return True

        async def scenario():
            sim.register_ws_price_listener(live_listener)
            try:
                with (
                    patch.object(sim, "get_binance_ws_price", return_value=100.5),
                    patch.object(
                        sim, "_run_binance_simulation_tick",
                        side_effect=lambda _aid: events.append("sim"),
                    ),
                ):
                    sim._on_binance_price_tick("BTCUSDT")
                    self.assertEqual(events, ["live"])
                    await asyncio.sleep(0)
                    self.assertEqual(events, ["live", "sim"])
            finally:
                sim.unregister_ws_price_listener(live_listener)
                sim._pending_simulation_ticks.clear()

        asyncio.run(scenario())

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

    def test_btc_15m_dump_variant_validates_the_used_leg_inside_strategy(self):
        now = 100.0
        up = {"quoteSource": "websocket", "receivedAtMonotonic": now - 0.10}
        down = {"quoteSource": "websocket", "receivedAtMonotonic": now - 0.75}
        dump = sim.AB_VARIANT_BY_ID["btc-15m-dump-then-hedge"]
        with patch.object(sim.time, "monotonic", return_value=now):
            self.assertTrue(sim._variant_books_are_coherent("btc-15m", dump, up, down))

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
    def test_inventory_rotation_is_an_independent_sim_only_variant(self):
        variant = sim.AB_VARIANT_BY_ID["btc-inventory-rotation"]
        self.assertTrue(variant["inventoryRotation"])
        self.assertTrue(variant["simOnly"])
        row = next(
            item for item in sim.build_ab_leaderboard()
            if item["id"] == "btc-inventory-rotation"
        )
        self.assertEqual(row["strategyType"], "rotation")
        self.assertEqual(row["sliceShares"], 5.0)
        self.assertEqual(variant["maxResidualShares"], 5.0)
        self.assertEqual(variant["minEntryEdge"], 0.04)
        self.assertTrue(variant["requireChainlinkConfirm"])
        self.assertGreater(
            variant["minEntryEdge"]
            + variant["residualRiskPremium"]
            + variant["futureHedgeFeeReserve"],
            0.06,
        )

    def test_inventory_rotation_accumulates_then_pairs_only_at_a_profit(self):
        variant_id = "btc-inventory-rotation"
        variant = sim.AB_VARIANT_BY_ID[variant_id]
        up_book = self._fresh_ws_book({
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "bids": [{"price": 0.29, "size": 100.0}],
            "asks": [{"price": 0.30, "size": 100.0}],
        })
        expensive_down = self._fresh_ws_book({
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "bids": [{"price": 0.78, "size": 100.0}],
            "asks": [{"price": 0.80, "size": 100.0}],
        })
        profitable_down = self._fresh_ws_book({
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "bids": [{"price": 0.58, "size": 100.0}],
            "asks": [{"price": 0.60, "size": 100.0}],
        })
        with (
            patch.object(sim, "_simulation_books_are_coherent", return_value=True),
            patch.object(sim, "get_chainlink_twap_signal", return_value={
                "current": 100.5, "opening": 100.0, "observedAt": 1,
                "ageSeconds": 0.0, "windowSeconds": 60,
            }),
            patch.dict(variant, {"actionCooldownSeconds": 0.0}),
        ):
            sim.simulate_trading(
                variant_id, "btc-window", up_book, expensive_down, 100.0,
                {"fairUp": 0.80, "fairDown": 0.20},
            )
            state = sim.ab_states[variant_id]
            self.assertEqual(state["position"]["upShares"], 5.0)
            self.assertEqual(state["position"]["downShares"], 0.0)

            # Even a high Down model edge must not bypass the profitable-pai
            # gate when buying Down would lock a loss against the held Up lot.
            sim.simulate_trading(
                variant_id, "btc-window", up_book, expensive_down, 90.0,
                {"fairUp": 0.01, "fairDown": 0.99},
            )
            self.assertEqual(state["position"]["fillCount"], 1)

            # A stronger signal on the already-held side must not average the
            # residual from five shares up to ten while the hedge is expensive.
            sim.simulate_trading(
                variant_id, "btc-window", up_book, expensive_down, 85.0,
                {"fairUp": 0.99, "fairDown": 0.01},
            )
            self.assertEqual(state["position"]["upShares"], 5.0)
            self.assertEqual(state["position"]["fillCount"], 1)

            sim.simulate_trading(
                variant_id, "btc-window", up_book, profitable_down, 80.0,
                {"fairUp": 0.50, "fairDown": 0.50},
            )
            position = state["position"]
            self.assertEqual(position["upShares"], 5.0)
            self.assertEqual(position["downShares"], 5.0)
            self.assertEqual(position["pairedShares"], 5.0)
            self.assertEqual(position["residualShares"], 0.0)
            self.assertGreater(position["lockedPnl"], 0.0)
            self.assertAlmostEqual(
                sim._settle_pnl(position, "Up"),
                sim._settle_pnl(position, "Down"),
            )

    def test_inventory_rotation_requires_chainlink_to_confirm_new_direction(self):
        variant_id = "btc-inventory-rotation"
        variant = sim.AB_VARIANT_BY_ID[variant_id]
        up_book = self._fresh_ws_book({
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "bids": [{"price": 0.29, "size": 100.0}],
            "asks": [{"price": 0.30, "size": 100.0}],
        })
        down_book = self._fresh_ws_book({
            "tickSize": 0.01,
            "minOrderSize": 5.0,
            "bids": [{"price": 0.68, "size": 100.0}],
            "asks": [{"price": 0.70, "size": 100.0}],
        })
        with (
            patch.object(sim, "_simulation_books_are_coherent", return_value=True),
            patch.object(sim, "get_chainlink_twap_signal", return_value={
                "current": 99.5, "opening": 100.0, "observedAt": 1,
                "ageSeconds": 0.0, "windowSeconds": 60,
            }),
            patch.dict(variant, {"actionCooldownSeconds": 0.0}),
        ):
            sim.simulate_trading(
                variant_id, "btc-window", up_book, down_book, 100.0,
                {"fairUp": 0.90, "fairDown": 0.10},
            )
        self.assertIsNone(sim.ab_states[variant_id]["position"])



if __name__ == "__main__":
    unittest.main()
