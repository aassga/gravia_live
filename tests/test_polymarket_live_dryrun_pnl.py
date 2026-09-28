# -*- coding: utf-8 -*-
"""DRY-RUN 總收益（2026-09-29 依使用者要求）。

真實的「總收益」是現金基礎（餘額 − 起始基準），DRY-RUN 沒有真實金流，所以用已結算交易的
pnlEstimate 加總。必須跟真實統計完全分開：_backfill_win_loss 排除 DRY-RUN 是既有決定
（2026-09-14），這裡不能把它弄壞。
"""
import unittest

import polymarket_live_status_server as status


def _trade(pnl, stake=10.0, fees=0.3, dry=True, exit_time=1000.0):
    return {"pnlEstimate": pnl, "stakeUsd": stake, "feesEstimate": fees,
            "dryRun": dry, "exitTime": exit_time}


class DryRunStatsTests(unittest.TestCase):
    def test_no_trades_returns_empty_shape(self):
        r = status._dry_run_stats({"trades": []})
        self.assertEqual(r["dryRunTradeCount"], 0)
        self.assertIsNone(r["dryRunTotalPnl"])

    def test_sums_only_dry_run_trades(self):
        r = status._dry_run_stats({"trades": [
            _trade(5.0), _trade(-2.0), _trade(100.0, dry=False),      # 真實那筆不能算進來
        ]})
        self.assertEqual(r["dryRunTradeCount"], 2)
        self.assertAlmostEqual(r["dryRunTotalPnl"], 3.0)

    def test_open_positions_are_excluded(self):
        """跟真實那邊「不含未平倉部位」一致：沒有 exitTime 就不算。"""
        r = status._dry_run_stats({"trades": [_trade(5.0), _trade(9.0, exit_time=None)]})
        self.assertEqual(r["dryRunTradeCount"], 1)
        self.assertAlmostEqual(r["dryRunTotalPnl"], 5.0)

    def test_win_rate_roi_and_averages(self):
        r = status._dry_run_stats({"trades": [
            _trade(6.0, stake=10.0), _trade(-2.0, stake=10.0),
            _trade(4.0, stake=20.0), _trade(-8.0, stake=10.0),
        ]})
        self.assertEqual(r["dryRunTradeCount"], 4)
        self.assertAlmostEqual(r["dryRunTotalPnl"], 0.0)
        self.assertAlmostEqual(r["dryRunStakeTotal"], 50.0)
        self.assertAlmostEqual(r["dryRunWinRatePct"], 50.0)          # 2 勝 / 4 筆有損益
        self.assertAlmostEqual(r["dryRunRoiPct"], 0.0)
        self.assertAlmostEqual(r["dryRunAvgPnl"], 0.0)
        self.assertAlmostEqual(r["dryRunFeesTotal"], 1.2)

    def test_zero_pnl_trades_do_not_count_in_win_rate_denominator(self):
        r = status._dry_run_stats({"trades": [_trade(3.0), _trade(0.0)]})
        self.assertAlmostEqual(r["dryRunWinRatePct"], 100.0)

    def test_roi_is_none_when_nothing_staked(self):
        r = status._dry_run_stats({"trades": [_trade(1.0, stake=0.0)]})
        self.assertIsNone(r["dryRunRoiPct"])

    def test_timestamps_span_the_trades(self):
        r = status._dry_run_stats({"trades": [_trade(1.0, exit_time=500.0), _trade(1.0, exit_time=900.0)]})
        self.assertEqual((r["dryRunFirstAt"], r["dryRunLastAt"]), (500.0, 900.0))


class RealStatsAreUnaffectedTests(unittest.TestCase):
    def test_backfill_still_ignores_dry_run(self):
        state = {"trades": [_trade(5.0), _trade(-3.0), _trade(7.0, dry=False), _trade(-1.0, dry=False)]}
        status._backfill_win_loss(state)
        self.assertEqual((state["winningTrades"], state["losingTrades"]), (1, 1))   # 只有真實那兩筆


if __name__ == "__main__":
    unittest.main()
