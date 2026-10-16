"""實盤優勢判準：樣本不足不下結論、毛優勢顯著才動、判準門檻本身被鎖住。

2026-10-17 依使用者要求新增。這些測試的重點不只是「函式會不會跑」，而是把判準的門檻
（150 筆、1 個標準誤）當成契約鎖住——以後若有人調門檻，必須明確改這個檔，不能默默放寬。
"""
import unittest

import polymarket_live_edge_check as ec


def _trades(n, pnl, fee, stake, entry, wins=None, trade_type="directional", vid="v1"):
    """造 n 筆成交；wins 給定時前 wins 筆是獲利、其餘是 9 倍的虧損（模擬領先方策略的賠率）。"""
    out = []
    for i in range(n):
        p = pnl if (wins is None or i < wins) else -abs(pnl) * 9
        out.append({"pnlEstimate": p, "feesEstimate": fee, "stakeUsd": stake,
                    "entryPrice": entry, "tradeType": trade_type, "variantId": vid})
    return {"trades": out}


class LiveEdgeCheckTests(unittest.TestCase):
    def test_thresholds_are_locked(self):
        # 判準的門檻是契約。要改必須動這個測試，不能默默放寬。
        self.assertEqual(ec.MIN_TRADES, 150)
        self.assertEqual(ec.GROSS_SE_MARGIN, 1.0)        # 與 sim doctor 同一個標準
        self.assertEqual(ec.BREAKEVEN_MARGIN_PP, 1.0)

    def test_below_min_trades_never_concludes(self):
        # 最重要的一條：樣本不足時，即使數字很漂亮也不準下結論（避免 40 筆就換策略）
        stats = ec.collect(_trades(ec.MIN_TRADES - 1, +2.0, 0.2, 40.0, 0.93), "v1")
        code, why = ec.verdict(stats)
        self.assertEqual(code, "observe")
        self.assertIn(str(ec.MIN_TRADES), why)
        self.assertGreater(stats["grossPerStake"], 0)     # 數字是正的，但還是不給結論

    def test_keep_when_gross_edge_clears_one_se(self):
        stats = ec.collect(_trades(ec.MIN_TRADES, +2.0, 0.2, 40.0, 0.93), "v1")
        self.assertEqual(ec.verdict(stats)[0], "keep")

    def test_switch_when_gross_edge_is_negative_beyond_one_se(self):
        stats = ec.collect(_trades(ec.MIN_TRADES, -2.0, 0.2, 40.0, 0.90), "v1")
        code, why = ec.verdict(stats)
        self.assertEqual(code, "switch")
        self.assertIn("扣費前", why)                      # 理由必須是毛優勢，不是淨損益

    def test_observe_when_confidence_interval_straddles_zero(self):
        # 10% 的單是 9 倍虧損 → 毛每注接近 0、標準誤大 → 不要動
        stats = ec.collect(_trades(ec.MIN_TRADES, +2.0, 0.2, 40.0, 0.90, wins=135), "v1")
        code, why = ec.verdict(stats)
        self.assertEqual(code, "observe")
        self.assertIn("跨過 0", why)

    def test_only_counts_current_variant(self):
        # 換策略後舊紀錄還留在狀態檔裡，不能混進來算
        state = _trades(ec.MIN_TRADES, +2.0, 0.2, 40.0, 0.93, vid="v1")
        state["trades"] += _trades(80, -5.0, 0.2, 40.0, 0.88, vid="old")["trades"]
        self.assertEqual(ec.collect(state, "v1")["n"], ec.MIN_TRADES)
        self.assertEqual(ec.collect(state, "old")["n"], 80)
        self.assertEqual(ec.collect(state, None)["n"], ec.MIN_TRADES + 80)

    def test_breakeven_is_mean_entry_price_and_stops_counted(self):
        # 二元市場的打平勝率 ≈ 進場價；停損出場用 tradeType 判斷（實盤紀錄沒有 exitReason 欄位）
        stats = ec.collect(_trades(10, +1.0, 0.2, 40.0, 0.93, trade_type="early_exit"), "v1")
        self.assertAlmostEqual(stats["breakeven"], 0.93)
        self.assertEqual(stats["stops"], 10)

    def test_gross_excludes_fees_and_net_includes_them(self):
        stats = ec.collect(_trades(10, -1.0, 0.5, 100.0, 0.90), "v1")
        self.assertAlmostEqual(stats["netTotal"], -10.0)
        self.assertAlmostEqual(stats["feeTotal"], 5.0)
        self.assertAlmostEqual(stats["grossTotal"], -5.0)       # 毛 = 淨 + 手續費
        self.assertAlmostEqual(stats["netPerStake"], -0.01)
        self.assertAlmostEqual(stats["grossPerStake"], -0.005)

    def test_empty_state_is_safe(self):
        stats = ec.collect({"trades": []}, "v1")
        self.assertEqual(stats["n"], 0)
        self.assertEqual(ec.verdict(stats)[0], "observe")
        self.assertIn("實盤優勢判準", ec.format_report(stats))


if __name__ == "__main__":
    unittest.main()
