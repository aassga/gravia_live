import os
import unittest

os.environ.setdefault("TG_BOT_TOKEN", "test-token")
os.environ.setdefault("TG_ALLOWED_USER_IDS", "1366798524")

import polymarket_tg_bot as bot  # noqa: E402


def _live(**over):
    base = {
        "strategyExecutionEnabled": True,
        "balanceUsdc": 54.76,
        "baselineBalance": 30.64,
        "baselineSetAt": 1789269222.0,
        "totalPnl": 24.12,
        "strategyConfig": {"label": "BTC 最後 60 秒買領先方（≥0.90）", "stakePct": 25.0, "lateFavoriteStopLossPrice": 0.6},
        "strategyState": {
            "halted": False,
            "position": None,
            "updatedAt": 1789300000.0,
            "totalPnlEstimate": -52.54,
            "totalFeesEstimate": 3.4,
            "totalTrades": 51,
            "winningTrades": 46,
            "losingTrades": 5,
            "trades": [
                {"windowSlug": "btc-updown-5m-1", "exitTime": 1789300000.0, "side": "Up", "entryPrice": 0.96,
                 "shares": 16.0, "outcome": "Up", "pnlEstimate": 0.5, "tradeType": "directional", "dryRun": False},
                {"windowSlug": "btc-updown-5m-0", "exitTime": 1789299000.0, "side": "Down", "entryPrice": 0.97,
                 "shares": 25.5, "outcome": "EarlyExit", "pnlEstimate": -19.74, "tradeType": "early_exit",
                 "exitReason": "favorite_stop_loss", "dryRun": False},
                {"windowSlug": "btc-updown-5m-x", "exitTime": 1789298000.0, "side": "Up", "entryPrice": 0.95,
                 "shares": 10.0, "outcome": "Up", "pnlEstimate": 0.4, "tradeType": "directional", "dryRun": True},
            ],
        },
    }
    base.update(over)
    return base


class TelegramBotTests(unittest.TestCase):
    def test_whitelist_only_allows_configured_user(self):
        self.assertTrue(bot.is_allowed({"message": {"from": {"id": 1366798524}}}))
        self.assertFalse(bot.is_allowed({"message": {"from": {"id": 42}}}))
        self.assertFalse(bot.is_allowed({"message": {}}))

    def test_status_and_pnl_formatting(self):
        status = bot.format_status(_live())
        self.assertIn("REAL 真實下單", status)
        self.assertIn("部位：空手", status)
        self.assertIn("$54.76", status)
        pnl = bot.format_pnl(_live())
        self.assertIn("+24.12", pnl)
        self.assertIn("51 筆", pnl)
        self.assertNotIn("勝率", pnl)                 # 2026-09-15：以收益為主，不看勝率
        self.assertIn("最差 -19.74", pnl)

    def test_trades_only_lists_real_orders(self):
        text = bot.format_trades(_live(), 10)
        self.assertIn("最近 2 筆真單", text)
        self.assertIn("favorite_stop_loss", text)
        self.assertNotIn("0.950", text)  # dry-run 那筆不列

    def test_alerts_only_on_halt_and_mode_switch(self):
        prev = _live()
        cur = _live()
        cur["strategyState"] = dict(prev["strategyState"])
        cur["strategyState"]["halted"] = True
        cur["strategyState"]["haltReason"] = "entry_order_unconfirmed"
        cur["strategyState"]["position"] = {"side": "Up", "shares": 16.0, "entryPrice": 0.96, "dryRun": False,
                                            "entryOrderId": "0xabc", "windowSlug": "btc-updown-5m-9", "entryTime": 1789301000.0,
                                            "stakeUsd": 15.4, "strategy": "late_favorite"}
        cur["strategyState"]["trades"] = [
            {"windowSlug": "btc-updown-5m-2", "exitTime": 1789301000.0, "side": "Down", "entryPrice": 0.96,
             "shares": 12.0, "outcome": "Down", "pnlEstimate": 0.4, "tradeType": "directional", "dryRun": False},
        ] + prev["strategyState"]["trades"]
        alerts = bot.diff_alerts(prev, cur)
        self.assertTrue(any("停機" in a for a in alerts))
        # 2026-09-14 依使用者要求：新部位要推、結算不推
        self.assertTrue(any("新部位（REAL）" in a and "Up" in a for a in alerts))
        self.assertFalse(any("結算" in a for a in alerts))
        self.assertEqual(bot.diff_alerts(None, cur), [])
        self.assertEqual(bot.diff_alerts(cur, cur), [])
        cur2 = dict(cur); cur2["strategyExecutionEnabled"] = False
        self.assertTrue(any("DRY-RUN" in a for a in bot.diff_alerts(cur, cur2)))

    def test_live_toggle_env_writer_and_keyboards(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, ".env")
            with open(p, "w", encoding="utf-8") as f:
                f.write("POLY_STAKE_PCT=15\nPOLY_STRATEGY_ARMED=false\nPOLY_LIVE_ASSET_ID=btc-15m")
            bot.write_env_flag("POLY_STRATEGY_ARMED", "true", p)
            text = open(p, encoding="utf-8").read()
            self.assertIn("POLY_STRATEGY_ARMED=true\n", text)
            self.assertIn("POLY_LIVE_ASSET_ID=btc-15m", text)
            self.assertEqual(text.count("POLY_STRATEGY_ARMED="), 1)
            bot.write_env_flag("POLY_NEW_FLAG", "1", p)
            self.assertTrue(open(p, encoding="utf-8").read().endswith("POLY_NEW_FLAG=1\n"))
        self.assertEqual(bot.live_toggle_keyboard(True)[0][0]["callback_data"], "live:dry")
        self.assertEqual(bot.live_toggle_keyboard(False)[0][0]["callback_data"], "live:real")
        self.assertEqual(bot.live_confirm_keyboard("real")[0][0]["callback_data"], "live:real:confirm")
        self.assertIn("/live", bot.HELP_TEXT)

    def test_sim_formatting_sorts_by_pnl(self):
        sim = {"assetList": [{"id": "btc", "label": "BTC"}, {"id": "btc-15m", "label": "BTC 15m"}], "abVariants": [
            {"assetId": "btc", "label": "A", "totalPnl": 1.0, "totalTrades": 3, "winRate": 66.6, "hasPosition": False},
            {"assetId": "btc", "label": "B", "totalPnl": 9.0, "totalTrades": 5, "winRate": 80.0, "hasPosition": True, "enabledAt": 1789300000.0},
            {"assetId": "btc-15m", "label": "C", "totalPnl": 99.0, "totalTrades": 1, "winRate": None, "hasPosition": False},
        ]}
        text = bot.format_sim(sim)                       # 預設：所有資產
        self.assertNotIn("勝率", text)
        self.assertIn("平均", text)
        self.assertLess(text.index("B"), text.index("A"))
        self.assertIn("BTC 15m", text); self.assertIn("C", text)
        self.assertIn("持倉中", text)
        self.assertIn("自 09-13", text)                      # 2026-09-15：啟用時間（台北）
        only = bot.format_sim(sim, "btc")                # 指定資產
        self.assertNotIn("BTC 15m", only)


if __name__ == "__main__":
    unittest.main()
