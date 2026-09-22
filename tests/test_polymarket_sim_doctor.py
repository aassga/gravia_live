# -*- coding: utf-8 -*-
"""模擬盤自動體檢：出場回放與挑選、虧損歸因、無成交放寬、套用動作。"""
import json
import os
import sqlite3
import tempfile
import time
import unittest

import polymarket_sim_doctor as doctor


def _mkdb(path, rid=1):
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE sim_meta (key TEXT PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO sim_meta VALUES ('shared_config', ?)", (json.dumps({"runId": rid, "startBalance": 500.0}),))
    db.execute("CREATE TABLE sim_trades (id INTEGER PRIMARY KEY, run_id INTEGER, variant_id TEXT, exit_time REAL, trade_json TEXT)")
    db.execute("CREATE TABLE sim_window_diagnostics (id INTEGER PRIMARY KEY, run_id INTEGER, variant_id TEXT, diagnostic_json TEXT, last_seen REAL)")
    db.execute("CREATE TABLE sim_quotes (id INTEGER PRIMARY KEY, run_id INTEGER, ts REAL, asset_id TEXT, window_slug TEXT, remaining_seconds REAL, up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL, fair_up REAL, spot_price REAL)")
    db.execute("CREATE TABLE sim_state (variant_id TEXT PRIMARY KEY, run_id INTEGER, state_json TEXT, updated_at REAL)")
    db.commit()
    return db


def _trade(slug, pnl, entry=0.98, shares=50.0, t=1000.0, side="Up", reason=None, outcome=None):
    cost = entry * shares + doctor.taker_fee(shares, entry)
    return {"windowSlug": slug, "side": side, "entryPrice": entry, "shares": shares, "stakeUsd": cost,
            "entryFee": doctor.taker_fee(shares, entry), "pnl": pnl, "entryTime": t, "exitTime": t + 60,
            "exitReason": reason, "outcome": outcome or (side if pnl > 0 else "Down")}


class DoctorTests(unittest.TestCase):
    def test_replay_and_stats(self):
        t = _trade("w1", -49.0)                       # 輸掉整注
        path = [(1001.0, 0.90), (1002.0, 0.70), (1003.0, 0.20), (1004.0, 0.01)]
        self.assertEqual(doctor.replay_trade(t, path, None, None), -49.0)          # 不停損：照實際
        stopped = doctor.replay_trade(t, path, 0.80, None)                          # bid 0.70 <= 0.80 → 賣 0.69
        self.assertAlmostEqual(stopped, 0.69 * 50 - doctor.taker_fee(50, 0.69) - t["stakeUsd"], places=4)
        self.assertGreater(stopped, -49.0)
        usd = doctor.replay_trade(t, path, None, 5.0)                               # 帳面虧 $5 就賣（0.90 時已 ~-4.6，0.70 時觸發）
        self.assertGreater(usd, -49.0); self.assertLess(usd, 0)
        win = _trade("w2", 0.9)
        self.assertEqual(doctor.replay_trade(win, [(1001.0, 0.99)], 0.80, None), 0.9)   # 沒觸發：照實際
        mean, se = doctor._stats([1.0, 1.0, 1.0]); self.assertEqual((round(mean, 3), round(se, 3)), (1.0, 0.0))

    def test_best_exit_config_requires_statistical_margin(self):
        # 18 筆小贏 + 2 筆大輸（輸的在路徑上會先跌到 0.60）→ 停損明顯較好
        items = []
        for i in range(18):
            items.append({"trade": _trade(f"w{i}", 0.9, t=1000.0 + i), "path": [(1001.0 + i, 0.99)]})
        for i in (18, 19):
            items.append({"trade": _trade(f"w{i}", -49.0, t=1000.0 + i),
                          "path": [(1001.0 + i, 0.90), (1002.0 + i, 0.60), (1003.0 + i, 0.02)]})
        cmp = doctor.best_exit_config(items, None, None)
        self.assertTrue(cmp["change"])
        self.assertIsNotNone(cmp["best"]["stopPrice"] or cmp["best"]["stopUsd"])
        self.assertGreater(cmp["best"]["mean"], cmp["current"]["mean"])
        self.assertGreater(cmp["margin"], cmp["threshold"])
        # 全贏、沒有虧損樣本 → 不該改（任何停損只會更差或一樣）
        allwin = [{"trade": _trade(f"x{i}", 0.9, t=1000.0 + i), "path": [(1001.0 + i, 0.99)]} for i in range(20)]
        self.assertFalse(doctor.best_exit_config(allwin, None, None)["change"])

    def test_loss_reasons(self):
        items = [
            {"trade": _trade("a", -49.0), "path": [(1.0, 0.95), (2.0, 0.02)]},                       # 跳空
            {"trade": _trade("b", -3.0, reason="favorite_stop_loss", outcome="Down"), "path": [(1.0, 0.80)]},
            {"trade": _trade("c", -3.0, reason="favorite_stop_loss", outcome="Up"), "path": [(1.0, 0.80)]},  # 假停損
            {"trade": _trade("d", 0.9), "path": [(1.0, 0.99)]},
        ]
        r = doctor.loss_reasons(items)
        self.assertEqual(r.get("進場後跳空翻面"), 1)
        self.assertEqual(r.get("停損出場（真翻面）"), 1)
        self.assertEqual(r.get("假停損（結算其實會贏）"), 1)
        self.assertNotIn("行情緩步反向", r)

    def test_plan_loosening(self):
        v = {"lateFavorite": True, "favoriteMinPrice": 0.98, "favoriteMaxPrice": 0.99, "favoriteWindowSeconds": 60.0, "favoriteMinRemaining": 45.0}
        diags = [{"reasonCounts": {"outside_entry_window": 9000, "favorite_no_leader": 500}}] * 3
        plan = doctor.plan_loosening(v, diags)
        self.assertEqual((plan["param"], plan["to"]), ("favoriteMinPrice", 0.96))
        only_window = [{"reasonCounts": {"outside_entry_window": 9000}}] * 3
        plan = doctor.plan_loosening(v, only_window)
        self.assertEqual((plan["param"], plan["to"]), ("favoriteWindowSeconds", 90.0))
        self.assertAlmostEqual(plan["extra"]["favoriteMinRemaining"], 30.0, places=3)
        cap = doctor.plan_loosening(dict(v, favoriteMinPrice=0.80), diags)
        self.assertIsNone(cap["param"])                                            # 已到下限
        money = doctor.plan_loosening(v, [{"reasonCounts": {"insufficient_budget": 50}}] * 3)
        self.assertIsNone(money["param"]); self.assertIn("資金", money["note"])
        lead = doctor.plan_loosening(dict(v, favoriteMinLeadPct=0.02), [{"reasonCounts": {"favorite_lead_below_minimum": 80}}])
        self.assertEqual((lead["param"], lead["to"]), ("favoriteMinLeadPct", 0.01))

    def test_diagnose_and_apply(self):
        with tempfile.TemporaryDirectory() as d:
            dbp = os.path.join(d, "sim.sqlite3"); db = _mkdb(dbp)
            # A：18 勝 2 敗，敗的有跌到 0.60 → retune
            for i in range(20):
                pnl = 0.9 if i < 18 else -49.0
                t = _trade(f"btc-updown-5m-{1000 + i}", pnl, t=1000.0 + i * 10)
                db.execute("INSERT INTO sim_trades (run_id, variant_id, exit_time, trade_json) VALUES (1,'A',?,?)", (t["exitTime"], json.dumps(t)))
                path = [(t["entryTime"] + 1, 0.99)] if pnl > 0 else [(t["entryTime"] + 1, 0.90), (t["entryTime"] + 2, 0.60), (t["entryTime"] + 3, 0.02)]
                for ts, bid in path:
                    db.execute("INSERT INTO sim_quotes (run_id, ts, asset_id, window_slug, up_bid, down_bid) VALUES (1,?,?,?,?,?)",
                               (ts, "btc", t["windowSlug"], bid, 1 - bid))
            # B：沒成交，卡在 favorite_no_leader
            for i in range(40):
                db.execute("INSERT INTO sim_window_diagnostics (run_id, variant_id, diagnostic_json, last_seen) VALUES (1,'B',?,?)",
                           (json.dumps({"windowSlug": f"w{i}", "reasonCounts": {"outside_entry_window": 500, "favorite_no_leader": 60}}), 1.0))
            db.commit(); db.close()
            variants = [
                {"id": "A", "label": "A 策略", "assetId": "btc", "lateFavorite": True, "favoriteStopLossPrice": None, "favoriteStopLossUsd": None},
                {"id": "B", "label": "B 策略", "assetId": "btc", "lateFavorite": True, "favoriteMinPrice": 0.98, "favoriteMaxPrice": 0.99},
                {"id": "C", "label": "兩腿鎖利", "assetId": "btc"},
            ]
            ro = sqlite3.connect(dbp)
            recs = doctor.diagnose(variants, ro, 1)
            ro.close()
            self.assertEqual([r["id"] for r in recs], ["A", "B"])          # 兩腿變體不處理
            a = next(r for r in recs if r["id"] == "A"); b = next(r for r in recs if r["id"] == "B")
            self.assertEqual(a["action"], "retune"); self.assertIn("停損", a["detail"])
            self.assertEqual(a["lossReasons"].get("進場後跳空翻面"), 2)
            self.assertEqual(b["action"], "loosen"); self.assertEqual(b["loosen"]["param"], "favoriteMinPrice")
            # 套用：A 寫覆寫、B 改規格並記放寬次數
            auto = [{"id": "A"}, {"id": "B", "favoriteMinPrice": 0.98}]
            disabled, overrides = [], {}
            summary = doctor.apply_actions(recs, auto, disabled, overrides)
            self.assertEqual(sorted(summary["changed"]), ["A", "B"]); self.assertEqual(summary["killed"], [])
            self.assertIn("favoriteStopLossPrice", overrides["A"])
            self.assertEqual(next(s for s in auto if s["id"] == "B")["favoriteMinPrice"], 0.96)
            self.assertEqual(next(s for s in auto if s["id"] == "B")["autoTuneLoosenCount"], 1)
            # kill：移出 auto、加進停用、清掉覆寫
            recs2 = [{"id": "A", "action": "kill", "detail": ""}]
            doctor.apply_actions(recs2, auto, disabled, overrides)
            self.assertEqual(disabled, ["A"]); self.assertNotIn("A", overrides)
            self.assertEqual([s["id"] for s in auto], ["B"])
            # 清紀錄
            msg = doctor.reset_variant_records("A", dbp, os.path.join(d, "bk"))
            self.assertIn("清空 20 筆成交", msg)
            chk = sqlite3.connect(dbp)
            self.assertEqual(chk.execute("select count(*) from sim_trades where variant_id='A'").fetchone()[0], 0)
            self.assertIn("enabledAt", json.loads(chk.execute("select state_json from sim_state where variant_id='A'").fetchone()[0]))
            chk.close()

    def test_kill_when_no_config_is_profitable(self):
        # 40 筆、全部小輸：任何停損都救不回 → kill
        items_trades = [_trade(f"w{i}", -1.0, t=1000.0 + i) for i in range(40)]
        with tempfile.TemporaryDirectory() as d:
            dbp = os.path.join(d, "sim.sqlite3"); db = _mkdb(dbp)
            for t in items_trades:
                db.execute("INSERT INTO sim_trades (run_id, variant_id, exit_time, trade_json) VALUES (1,'K',?,?)", (t["exitTime"], json.dumps(t)))
                db.execute("INSERT INTO sim_quotes (run_id, ts, asset_id, window_slug, up_bid, down_bid) VALUES (1,?,?,?,?,?)",
                           (t["entryTime"] + 1, "btc", t["windowSlug"], 0.96, 0.03))
            db.commit(); db.close()
            ro = sqlite3.connect(dbp)
            recs = doctor.diagnose([{"id": "K", "label": "K", "assetId": "btc", "lateFavorite": True}], ro, 1)
            ro.close()
            self.assertEqual(recs[0]["action"], "kill"); self.assertIn("負期望", recs[0]["detail"])


if __name__ == "__main__":
    unittest.main()
