import unittest

import polymarket_weekly_scan as scan


def _t(wallet, side, price, size, rel, buy=True):
    return {"proxyWallet": wallet, "side": "BUY" if buy else "SELL", "outcome": side,
            "price": str(price), "size": str(size), "timestamp": str(1_000_000 + rel)}


class WeeklyScanTests(unittest.TestCase):
    def test_classifier_recognises_late_favorite_and_both_sides(self):
        ws = 1_000_000
        fav = scan.classify_wallet_window(ws, "Up", [_t("a", "Up", 0.96, 20, 255)])
        self.assertEqual(fav["kind"], "late_favorite")
        self.assertTrue(fav["won"])
        self.assertAlmostEqual(fav["pnl"], 20 * (1 - 0.96), places=6)
        self.assertEqual(fav["tBeforeClose"], 45)
        lock = scan.classify_wallet_window(ws, "Down", [_t("b", "Up", 0.48, 10, 30), _t("b", "Down", 0.49, 10, 55)])
        self.assertEqual(lock["kind"], "both_sides_lock")
        self.assertAlmostEqual(lock["pnl"], 10 - (4.8 + 4.9), places=6)
        stopped = scan.classify_wallet_window(ws, "Down", [_t("c", "Up", 0.95, 10, 250), _t("c", "Up", 0.60, 10, 270, buy=False)])
        self.assertEqual(stopped["kind"], "late_favorite")   # 買單符合買領先方型態；賣出記在 sold
        self.assertTrue(stopped["sold"])
        self.assertAlmostEqual(stopped["pnl"], 6.0 - 9.5, places=6)
        early_sold = scan.classify_wallet_window(ws, "Down", [_t("e", "Up", 0.55, 10, 100), _t("e", "Up", 0.50, 10, 150, buy=False)])
        self.assertEqual(early_sold["kind"], "buy_then_sell")
        mid = scan.classify_wallet_window(ws, "Up", [_t("d", "Down", 0.40, 5, 120)])
        self.assertEqual(mid["kind"], "mid_directional")
        self.assertFalse(mid["won"])

    def test_analyze_and_compare_produce_suggestions(self):
        ws = 1_000_000
        trades = []
        for i in range(12):                      # 12 個「機器人」窗口：買 0.96 領先方、贏
            trades.append(_t("bot", "Up", 0.96, 10, 250 + (i % 5)))
        trades += [_t("r1", "Down", 0.30, 5, 100), _t("r2", "Up", 0.99, 5, 280), _t("r3", "Up", 0.91, 5, 262)]
        windows = [{"slug": "btc-updown-5m-%d" % ws, "start": ws, "outcome": "Up", "trades": trades}]
        report = scan.analyze(windows)
        kinds = {k["kind"]: k for k in report["kinds"]}
        self.assertIn("late_favorite", kinds)
        self.assertEqual(report["lateFavorite"]["n"], 3)          # bot + r2 + r3（同一窗口同錢包算一次）
        for b in report["lateFavorite"]["priceBuckets"]:
            self.assertIn("pnlPerShareMedian", b)
            self.assertGreaterEqual(b["pnlPerShare"], -1.0)
        self.assertEqual(report["botCount"], 0)                   # 只有 1 個窗口，沒人達到 10 窗門檻
        cfg = {"lateFavoriteEnabled": True, "lateFavoriteMinPrice": 0.95, "lateFavoriteMaxPrice": 0.97,
               "lateFavoriteStopLossPrice": 0.6, "lateFavoriteMinRemaining": 5, "lateFavoriteWindowSeconds": 60}
        suggestions = scan.compare(report, cfg)
        titles = [s["title"] for s in suggestions]
        self.assertIn("買價區間", titles)
        self.assertIn("停損", titles)
        for s in suggestions:
            self.assertTrue(s["pros"] and s["cons"])
        md = scan.render_markdown(report, suggestions, cfg, 24)
        self.assertIn("最多人使用的型態", md)
        msgs = scan.render_telegram(report, suggestions, 24)
        self.assertTrue(all(len(m) <= 4000 for m in msgs))
        self.assertIn("不會自動更改", md)


if __name__ == "__main__":
    unittest.main()
