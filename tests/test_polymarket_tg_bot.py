import json
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

    def test_strategy_switch_helpers(self):
        # 2026-09-20 /strategy：候選＝lateFavorite 且非 simOnly；名稱；env 更新（①主 .env vs ②③輕量模擬盤）；TG 選單改名
        import tempfile
        sim = {"assetList": [{"id": "btc"}, {"id": "btc-15m"}],
               "abVariants": [
                   {"id": "btc-15m-auto-60-90s-098-099", "assetId": "btc-15m", "label": "⚙ BTC15m 60~90", "lateFavorite": True, "simOnly": False,
                    "favoriteWindowSeconds": 90.0, "favoriteMinRemaining": 60.0, "favoriteMinPrice": 0.98, "favoriteMaxPrice": 0.99, "favoriteStopLossPrice": None,
                    "totalPnl": 50.0, "totalTrades": 40, "roi": 1.5},
                   {"id": "btc-auto-45-60s-098-099", "assetId": "btc", "label": "⚙ BTC 45~60", "lateFavorite": True, "simOnly": False,
                    "favoriteWindowSeconds": 60.0, "favoriteMinRemaining": 45.0, "favoriteMinPrice": 0.98, "favoriteMaxPrice": 0.99, "favoriteStopLossPrice": 0.60,
                    "totalPnl": 20.0, "totalTrades": 90, "roi": 0.4},
                   {"id": "btc-mid-favorite-087-095", "assetId": "btc", "label": "sim only", "lateFavorite": True, "simOnly": True, "totalPnl": 99.0},
                   {"id": "btc-follow-x", "assetId": "btc", "label": "follow", "followWallets": ["0x"], "simOnly": False, "totalPnl": 5.0},
               ]}
        rows = bot.strategy_candidates(sim)
        self.assertEqual([v["id"] for v in rows], ["btc-auto-45-60s-098-099", "btc-15m-auto-60-90s-098-099"])   # 依資產順序，simOnly／跟單不列
        self.assertEqual(bot.instance_short_name(2, rows[1]), "實盤③BTC15m-60~90s-098")
        self.assertEqual(bot.instance_short_name(0, rows[0]), "實盤①BTC5m-45~60s-098")
        kb = bot.strategy_list_keyboard(2, rows, "btc-15m-auto-60-90s-098-099")
        self.assertTrue(kb[1][0]["text"].startswith("★ "))
        self.assertEqual(kb[0][0]["callback_data"], "strat:2:0")
        self.assertEqual(kb[-1][0]["callback_data"], "strat:cancel")
        self.assertEqual(bot.strategy_confirm_keyboard(2, 1)[0][0]["callback_data"], "strat:2:1:confirm")
        with tempfile.TemporaryDirectory() as d:
            main_env = os.path.join(d, ".env"); env3 = os.path.join(d, ".env.live3")
            with open(main_env, "w", encoding="utf-8") as f:
                f.write("POLY_SIM_ASSETS=btc,eth-alt\nPOLY_LIVE_VARIANT_ID=old\nTG_LIVE_INSTANCES=實盤①X|ws://a|" + main_env + "|gravia.service|/tmp/a.json;實盤②Y|ws://b|/tmp/b.env|gravia-live2.service|/tmp/b.json;實盤③Z|ws://c|" + env3 + "|gravia-live3.service|/tmp/c.json\n")
            with open(env3, "w", encoding="utf-8") as f:
                f.write("POLY_LIVE_ASSET_ID=btc-15m\nPOLY_SIM_ASSETS=btc-15m\nPOLY_STAKE_PCT=30\n")
            # ③：輕量模擬盤只跑該變體
            upd = bot.strategy_env_updates({"env": env3}, rows[1], main_env=main_env)
            self.assertEqual(upd["POLY_LIVE_VARIANT_ID"], "btc-15m-auto-60-90s-098-099")
            self.assertEqual((upd["POLY_SIM_ASSETS"], upd["POLY_SIM_ONLY_VARIANTS"], upd["POLY_LIVE_FAVORITE_STOP_LOSS_PRICE"]), ("btc-15m", "btc-15m-auto-60-90s-098-099", "0"))
            # ①：主 .env 只在資產不在清單時補上；停損 0.60
            upd = bot.strategy_env_updates({"env": main_env}, rows[1], main_env=main_env)
            self.assertEqual(upd["POLY_SIM_ASSETS"], "btc,eth-alt,btc-15m")
            self.assertNotIn("POLY_SIM_ONLY_VARIANTS", upd)
            upd = bot.strategy_env_updates({"env": main_env}, rows[0], main_env=main_env)
            self.assertNotIn("POLY_SIM_ASSETS", upd)
            self.assertEqual(upd["POLY_LIVE_FAVORITE_STOP_LOSS_PRICE"], "0.60")
            # 改名只動第 idx 段
            value = bot.rename_live_instance(2, "實盤③BTC15m-60~90s-098", main_env=main_env)
            self.assertTrue(value.startswith("實盤①X|"))
            self.assertIn(";實盤③BTC15m-60~90s-098|ws://c|", value)
            self.assertIn("TG_LIVE_INSTANCES=實盤①X|", open(main_env, encoding="utf-8").read())
        self.assertEqual(bot.live_toggle_keyboard(True, 1)[-1][0]["callback_data"], "strat:1")

    def test_stake_helpers(self):
        # 2026-09-20 /stake：範圍 0.5～30、鍵盤 callback、/live 選單多「每注 %」
        self.assertEqual(bot.parse_stake_pct("10"), 10.0)
        self.assertEqual(bot.parse_stake_pct("12.5"), 12.5)
        self.assertEqual(bot.parse_stake_pct("100"), 100.0)
        self.assertIsNone(bot.parse_stake_pct("101"))
        self.assertIsNone(bot.parse_stake_pct("0"))
        self.assertIsNone(bot.parse_stake_pct("abc"))
        kb = bot.stake_pct_keyboard(2, "30")
        self.assertEqual(kb[0][0]["callback_data"], "stake:2:5")
        self.assertEqual(kb[1][2]["text"], "★ 30%")
        self.assertEqual(kb[2][2]["callback_data"], "stake:2:100")
        self.assertEqual(kb[-1][0]["callback_data"], "stake:cancel")
        self.assertEqual(bot.stake_confirm_keyboard(1, 12.5)[0][0]["callback_data"], "stake:1:12.5:confirm")
        self.assertEqual(bot.live_toggle_keyboard(False, 0)[-1][1]["callback_data"], "stake:0")

    def test_stop_helpers(self):
        # 2026-09-20 /stop：值解析、分組、覆寫檔寫入、鍵盤 callback
        import tempfile
        self.assertEqual(bot.parse_stop_price("0"), 0.0)
        self.assertEqual(bot.parse_stop_price("0.60"), 0.6)
        self.assertIsNone(bot.parse_stop_price("1.2")); self.assertIsNone(bot.parse_stop_price("x"))
        sim = {"assetList": [{"id": "btc", "label": "BTC"}],
               "abVariants": [{"id": "a", "assetId": "btc", "label": "A（0.98～0.99、不停損）", "lateFavorite": True, "favoriteStopLossPrice": None, "totalPnl": 1, "totalTrades": 2},
                              {"id": "b", "assetId": "btc", "label": "B（停損 0.60）", "lateFavorite": True, "favoriteStopLossPrice": 0.6, "totalPnl": 5, "totalTrades": 3},
                              {"id": "c", "assetId": "btc", "label": "C", "openMomentum": True, "totalPnl": 9},
                              {"id": "d", "assetId": "btc", "label": "D", "twoSidedMaker": True, "totalPnl": 99}]}
        groups = bot.stop_variants(sim)
        self.assertEqual([v["id"] for v in groups["btc"]], ["c", "b", "a"])   # 單腿策略都列（含 openMomentum）；兩腿做市不列
        kb = bot.stop_value_keyboard("btc", 0, 0.6)
        self.assertEqual(kb[0][0]["text"], "不停損"); self.assertEqual(kb[0][3]["text"], "★ 0.60")
        self.assertEqual(kb[0][3]["callback_data"], "stop:s:btc:0:0.60")
        self.assertEqual(bot.stop_confirm_keyboard("btc", 0, "0")[0][0]["callback_data"], "stop:c:btc:0:0")
        # 2026-09-22 金額停損
        self.assertEqual(bot.parse_stop_usd("2"), 2.0); self.assertEqual(bot.parse_stop_usd("0"), 0.0); self.assertIsNone(bot.parse_stop_usd("x"))
        kb = bot.stop_value_keyboard("btc", 0, 0.6, 2)
        flat = [b for row in kb[2:-1] for b in row]
        self.assertEqual(flat[0]["text"], "不設金額"); self.assertEqual(len(flat), 17)
        star = next(b for b in flat if b["text"].startswith("★ ")); self.assertEqual((star["text"], star["callback_data"]), ("★ $2", "stop:m:btc:0:2"))
        self.assertEqual(kb[-1][0]["callback_data"], "stop:cancel")
        self.assertEqual(bot.stop_confirm_keyboard("btc", 0, "5", "usd")[0][0]["callback_data"], "stop:k:btc:0:5")
        # 直接打字：/stop 1 $2 → 金額；/stop 3 0.88 → 價格；/stop 1 0 → 不設價格停損；/stop 1 $0 → 不設金額
        self.assertEqual(bot.parse_stop_args(["/stop", "1", "$2"]), ("1", "usd", 2.0))
        self.assertEqual(bot.parse_stop_args(["/stop", "3", "0.88"]), ("3", "price", 0.88))
        self.assertEqual(bot.parse_stop_args(["/stop", "1", "0"]), ("1", "price", 0.0))
        self.assertEqual(bot.parse_stop_args(["/stop", "1", "$0"]), ("1", "usd", 0.0))
        self.assertEqual(bot.parse_stop_args(["/stop", "eth-15m-auto-60-90s-098-099", "3usd"]), ("eth-15m-auto-60-90s-098-099", "usd", 3.0))
        self.assertIsNone(bot.parse_stop_args(["/stop", "1", "abc"])); self.assertIsNone(bot.parse_stop_args(["/stop"]))
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ov.json")
            bot.write_variant_override("a", 0.6, path); data = bot.write_variant_override("b", 0.0, path)
            self.assertEqual(data, {"a": {"favoriteStopLossPrice": 0.6}, "b": {"favoriteStopLossPrice": None}})
            data = bot.write_variant_override("a", 2.0, path, key="favoriteStopLossUsd")
            self.assertEqual(data["a"], {"favoriteStopLossPrice": 0.6, "favoriteStopLossUsd": 2.0})
            self.assertEqual(json.load(open(path, encoding="utf-8"))["b"]["favoriteStopLossPrice"], None)

    def test_new_real_loss_and_stake_alert(self):
        # 2026-09-20 推播：新的真實虧損單、餘額不足 5 股
        t_loss = {"windowSlug": "w2", "exitTime": 2, "dryRun": False, "pnlEstimate": -9.0}
        t_win = {"windowSlug": "w1", "exitTime": 1, "dryRun": False, "pnlEstimate": 0.2}
        prev = {"strategyState": {"trades": [t_win]}}; cur = {"strategyState": {"trades": [t_loss, t_win]}}
        self.assertEqual(bot.new_real_loss(prev, cur), t_loss)
        self.assertIsNone(bot.new_real_loss(cur, cur))                                   # 同一筆不重複
        self.assertIsNone(bot.new_real_loss(prev, {"strategyState": {"trades": [dict(t_loss, dryRun=True)]}}))
        self.assertIsNone(bot.new_real_loss(None, cur))
        snap = {"balanceUsdc": 23.56, "strategyExecutionEnabled": True, "strategyState": {"position": None}}
        self.assertIsNone(bot.stake_alert_text(snap, 22, "實盤①"))
        self.assertIn("只買得到 4 股", bot.stake_alert_text(snap, 20, "實盤①"))
        self.assertIsNone(bot.stake_alert_text(dict(snap, strategyExecutionEnabled=False), 20, "實盤①"))   # DRY-RUN 不吵

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
        self.assertEqual(bot.live_toggle_keyboard(True)[0][0]["callback_data"], "live:0:dry")
        self.assertEqual(bot.live_toggle_keyboard(False, 1)[0][0]["callback_data"], "live:1:real")
        self.assertEqual(bot.live_confirm_keyboard("real", 1)[0][0]["callback_data"], "live:1:real:confirm")
        self.assertEqual(len(bot.LIVE_INSTANCES), 1)                       # 沒設 TG_LIVE_INSTANCES → 單實盤
        os.environ["TG_LIVE_INSTANCES"] = "實盤A|ws://a|/tmp/a.env|gravia.service gravia-status.service|/tmp/a.json;實盤B|ws://b|/tmp/b.env|gravia-live2.service|/tmp/b.json"
        try:
            insts = bot._parse_live_instances()
        finally:
            del os.environ["TG_LIVE_INSTANCES"]
        self.assertEqual([i["name"] for i in insts], ["實盤A", "實盤B"])
        self.assertEqual(insts[1]["services"], ["gravia-live2.service"])
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
