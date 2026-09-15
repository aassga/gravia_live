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
            self.assertIn("breakEvenWinRate", b)            # 2026-09-15：賠率／打平勝率
            self.assertGreaterEqual(b["pnlPerShare"], -1.0)
        pnls = [k["pnl"] for k in report["kinds"]]
        self.assertEqual(pnls, sorted(pnls, reverse=True))     # 型態依粗估 PnL 排序
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
        self.assertIn("依粗估 PnL 排序", md)
        msgs = scan.render_telegram(report, suggestions, 24)
        self.assertTrue(all(len(m) <= 4000 for m in msgs))
        self.assertIn("不會自動更改", md)
        self.assertIn("最賺型態", msgs[0])

    def test_negative_ev_bucket_is_flagged(self):
        # 0.96 買領先方：贏 24 次各 +0.04、輸 2 次各 -0.96 → 勝率 92% 但打平需 96%，應標 ❌
        ws = 1_000_000
        windows = []
        for i in range(26):
            outcome = "Down" if i < 2 else "Up"
            windows.append({"slug": f"w{i}", "start": ws + i * 300, "outcome": outcome,
                            "trades": [_t(f"w{i}", "Up", 0.96, 10, i * 300 + 250)]})
        report = scan.analyze(windows)
        b = report["lateFavorite"]["priceBuckets"][0]
        self.assertTrue(b["negativeEV"])
        self.assertAlmostEqual(b["lossesPerWin"], 24.0, places=3)
        self.assertGreater(b["breakEvenWinRate"], b["winRate"])
        self.assertIn("❌", scan.risk_text(b))
        cfg = {"lateFavoriteEnabled": True, "lateFavoriteMinPrice": 0.95, "lateFavoriteMaxPrice": 0.98}
        titles = [s["title"] for s in scan.compare(report, cfg)]
        self.assertIn("勝率高卻長期賠錢的區間", titles)
        self.assertEqual(scan.kind_label("late_favorite"), f"最後 {scan.LATE_SECONDS} 秒買領先方（>=0.88）")


    def test_market_kind_classification_and_discovery_rendering(self):
        self.assertEqual(scan.classify_market_kind("btc-updown-5m-1", None), "crypto_window")
        self.assertEqual(scan.classify_market_kind("btc-updown-15m-1", None), "crypto_window")
        self.assertEqual(scan.classify_market_kind("sea-tor-rom-2026-09-14-rom", "2099-01-01T00:00:00Z"), "long_dated")
        self.assertEqual(scan.classify_market_kind("will-the-fed-cut", "2000-01-01T00:00:00Z"), "event_soon")
        disc = {"kinds": {"event_soon": 1}, "markets": [{
            "slug": "x", "question": "Fed cut?", "event": "Fed", "kind": "event_soon", "volume24h": 1e6, "liquidity": 5e5,
            "endDate": "2026-09-16", "trades": 900, "wallets": 300, "medianTradeUsd": 40.0, "buyShare": 0.9,
            "favoriteShare": 0.6, "bothSidesShare": 0.05, "priceMedian": 0.9,
            "favN": 40, "favMtmPerShare": -0.12, "favWorstUsd": -300.0, "favTotalUsd": -800.0}]}
        msgs = scan.render_discovery_telegram(disc)
        self.assertTrue(any("跟領先方" in m for m in msgs))
        self.assertTrue(any("👎 跟領先方按現價" in m for m in msgs))   # 2026-09-15：看淨利不看占比
        self.assertEqual(scan._current_prices({"outcomes": '["Yes","No"]', "outcomePrices": '["0.9","0.1"]'}), {"Yes": 0.9, "No": 0.1})
        self.assertTrue(any("👍" in m and "👎" in m for m in msgs))
        self.assertTrue(all(len(m) <= 4000 for m in msgs))
        self.assertIn("btc-15m", scan.MARKETS)


if __name__ == "__main__":
    unittest.main()
