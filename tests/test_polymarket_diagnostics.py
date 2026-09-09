import asyncio
import copy
import time
import unittest
from unittest.mock import AsyncMock, patch

import polymarket_diagnostics as diag
import polymarket_server as sim


def book(price, received):
    return {"bids": [{"price": price - .01, "size": 100}],
            "asks": [{"price": price, "size": 100}],
            "quoteSource": "websocket", "receivedAtMonotonic": received,
            "tickSize": .01, "minOrderSize": 5}


class DecisionEvidenceTests(unittest.IsolatedAsyncioTestCase):
    def test_snapshots_are_frozen_and_reveal_superseded_book(self):
        old, latest = book(.90, 100), book(.97, 101)
        item = {"windowSlug": "window"}
        with diag.evaluation("SIM", "btc-historical-hybrid", "window", "poll",
                old, old, 6, {}, {}, lambda: (latest, latest)):
            diag.record(item, "entered", {"entryDecisionPrice": .92})
        sample = item["decisionEvents"][0]
        self.assertFalse(sample["matchesLatestWs"])
        self.assertEqual(sample["upBook"]["asks"][0]["price"], .90)
        self.assertEqual(sample["latestWsUpBook"]["asks"][0]["price"], .97)
        old["asks"][0]["price"] = .10
        self.assertEqual(sample["upBook"]["asks"][0]["price"], .90)
        self.assertNotIn("decisionSamples", diag.public_summary(item))

    def test_sampling_is_bounded_but_retains_entry_evidence(self):
        b = book(.45, time.monotonic())
        item = {"windowSlug": "window"}
        with diag.evaluation("DRY-RUN", "btc-historical-hybrid", "window", "market_ws",
                b, b, 6, {}, {}, lambda: (b, b)):
            diag.record(item, "entered", {})
            for _ in range(100):
                diag.record(item, "pair_price_sum_above_maximum", {})
            self.assertEqual(len(item["decisionSamples"]), 2)
            for i in range(100):
                diag.record(item, f"reason_{i}", {})
        self.assertEqual(len(item["decisionSamples"]), diag.MAX_SAMPLES)
        self.assertEqual(item["decisionEvents"][0]["reason"], "entered")

    def test_bad_diagnostic_does_not_interrupt_strategy(self):
        b = book(.45, time.monotonic())
        item = {"windowSlug": "window"}
        with diag.evaluation("SIM", "btc-historical-hybrid", "window", "poll",
                b, b, 6, {}, {}, lambda: (_ for _ in ()).throw(ValueError())):
            diag.record(item, "entered", {})
        self.assertEqual(item["decisionEvidenceError"], "ValueError")

    def test_non_target_variant_is_not_sampled(self):
        b = book(.45, time.monotonic())
        item = {"windowSlug": "window"}
        with diag.evaluation("SIM", "btc-main", "window", "poll",
                b, b, 6, {}, {}, lambda: (b, b)):
            diag.record(item, "entered", {})
        self.assertNotIn("decisionSamples", item)

    def test_old_windows_drop_full_evidence(self):
        items = [
            {"decisionSamples": [i], "decisionEvents": [i], "aggregate": i}
            for i in range(diag.MAX_EVIDENCE_WINDOWS + 2)
        ]
        diag.trim_old_window_evidence(items)
        self.assertIn("decisionSamples", items[diag.MAX_EVIDENCE_WINDOWS - 1])
        self.assertNotIn("decisionSamples", items[diag.MAX_EVIDENCE_WINDOWS])
        self.assertEqual(items[-1]["aggregate"], diag.MAX_EVIDENCE_WINDOWS + 1)

    def test_recent_window_samples_are_trimmed_to_current_limit(self):
        items = [{"decisionSamples": list(range(diag.MAX_SAMPLES + 5))}]
        diag.trim_old_window_evidence(items)
        self.assertEqual(
            items[0]["decisionSamples"], list(range(5, diag.MAX_SAMPLES + 5))
        )

    def test_high_frequency_ws_does_not_suppress_poll_sample(self):
        b = book(.45, time.monotonic())
        item = {"windowSlug": "window"}
        for source in ("market_ws", "market_ws", "poll", "chainlink", "binance"):
            with diag.evaluation("SIM", "btc-historical-hybrid", "window", source,
                    b, b, 6, {}, {}, lambda: (b, b)):
                diag.record(item, "outside_entry_window", {})
        self.assertEqual([s["trigger"] for s in item["decisionSamples"]],
                         ["market_ws", "poll", "chainlink", "binance"])

    async def test_concurrent_evaluations_keep_their_own_context(self):
        async def run(source):
            item = {"windowSlug": "window"}
            b = book(.45, time.monotonic())
            with diag.evaluation("SIM", "btc-historical-hybrid", "window", source,
                    b, b, 6, {}, {}, lambda: (b, b)):
                await asyncio.sleep(0)
                diag.record(item, "entered", {})
            return item["decisionEvents"][0]
        a, b = await asyncio.gather(run("poll"), run("chainlink"))
        self.assertEqual((a["trigger"], b["trigger"]), ("poll", "chainlink"))
        self.assertNotEqual(a["evaluationId"], b["evaluationId"])

    async def test_poll_refreshes_ws_book_after_waiting_for_spot(self):
        # WS advances during REST I/O; poll must evaluate and retain the newer snapshot.
        now = time.monotonic()
        old_up, old_down = book(.90, now), book(.08, now)
        new_up, new_down = book(.97, now + .01), book(.02, now + .01)
        market = {"slug": "btc-window", "outcomes": '["Up", "Down"]',
                  "clobTokenIds": '["up", "down"]'}
        ms = copy.deepcopy(sim.markets_state["btc"])
        ms.update(market=market, windowEndsAt=(sim.real_now()+6)*1000)
        variant = sim.AB_VARIANT_BY_ID["btc-historical-hybrid"]
        evidence = {"windowSlug": "btc-window"}

        async def fetch_spot(*args):
            await asyncio.sleep(0)
            ms["upBook"], ms["downBook"] = new_up, new_down
            return {"price": 100, "changePct": 0}

        def observe(vid, slug, up, down, remaining, fair, **kwargs):
            with sim.decision_evaluation("SIM", vid, slug, kwargs["evaluation_source"],
                    up, down, remaining, {}):
                diag.record(evidence, "entered", {})

        with (patch.dict(sim.markets_state, {"btc": ms}),
              patch.object(sim, "AB_VARIANT_BY_ID", {variant["id"]: variant}),
              patch.object(sim, "fetch_active_market", AsyncMock(return_value=market)),
              patch.object(sim, "_ws_set_wanted_tokens", AsyncMock()),
              patch.object(sim, "_ws_ensure_meta", AsyncMock()),
              patch.object(sim, "_get_book_ws_or_rest", AsyncMock(side_effect=[old_up, old_down])),
              patch.object(sim, "_get_midpoint_ws_or_rest", AsyncMock(return_value=.5)),
              patch.object(sim, "fetch_spot_price", side_effect=fetch_spot),
              patch.object(sim, "fetch_klines", AsyncMock(return_value=[])),
              patch.object(sim, "get_binance_ws_price", return_value=None),
              patch.object(sim, "estimate_fair_up", return_value=None),
              patch.object(sim, "_simulation_books_are_coherent", return_value=True),
              patch.object(sim, "_variant_books_are_coherent", return_value=True),
              patch.object(sim, "simulate_trading", side_effect=observe),
              patch.object(sim, "persist_quote"),
              patch.object(sim, "_ws_get_book", side_effect=lambda tid: new_up if tid=="up" else new_down)):
            await sim._fetch_one_asset(None, next(a for a in sim.ASSETS if a["id"] == "btc"))
        sample = evidence["decisionEvents"][0]
        self.assertEqual(sample["trigger"], "poll")
        self.assertTrue(sample["matchesLatestWs"])
        self.assertEqual(sample["upBook"]["asks"][0]["price"], .97)
        self.assertEqual(
            sample["latestWsUpBook"]["fingerprint"], sample["upBook"]["fingerprint"]
        )
