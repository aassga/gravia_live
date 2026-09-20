# -*- coding: utf-8 -*-
"""polymarket_tg_tools：ROI 排行、停損回放、鏡像報告、重製、停用清單、餘額檢查。"""
import json
import os
import sqlite3
import tempfile
import time
import unittest

import polymarket_tg_tools as tools


def _mkdb(path, run_id=1):
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE sim_meta (key TEXT PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO sim_meta VALUES ('shared_config', ?)", (json.dumps({"runId": run_id, "startBalance": 500.0}),))
    db.execute("CREATE TABLE sim_trades (id INTEGER PRIMARY KEY, run_id INTEGER, variant_id TEXT, exit_time REAL, trade_json TEXT)")
    db.execute("CREATE TABLE sim_window_diagnostics (id INTEGER PRIMARY KEY, run_id INTEGER, variant_id TEXT, diagnostic_json TEXT, last_seen REAL)")
    db.execute("CREATE TABLE sim_quotes (id INTEGER PRIMARY KEY, run_id INTEGER, ts REAL, asset_id TEXT, window_slug TEXT, remaining_seconds REAL, up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL, fair_up REAL, spot_price REAL)")
    db.execute("CREATE TABLE sim_state (variant_id TEXT PRIMARY KEY, run_id INTEGER, state_json TEXT, updated_at REAL)")
    db.commit(); return db


def _trade(slug, side, entry, shares, pnl, t):
    return {"windowSlug": slug, "side": side, "entryPrice": entry, "shares": shares, "stakeUsd": entry * shares, "pnl": pnl, "entryTime": t, "exitTime": t + 60}


class ToolsTests(unittest.TestCase):
    def test_roi_and_stop_replay(self):
        with tempfile.TemporaryDirectory() as d:
            dbp = os.path.join(d, "sim.sqlite3"); db = _mkdb(dbp)
            now = time.time()
            for i in range(12):
                slug = f"btc-updown-5m-{1000 + i}"
                won = i != 5
                t = _trade(slug, "Up", 0.98, 100, 1.2 if won else -98.0, now - 3600 + i * 300)
                db.execute("INSERT INTO sim_trades (run_id, variant_id, exit_time, trade_json) VALUES (1, 'btc-auto-45-60s-098-099', ?, ?)", (t["exitTime"], json.dumps(t)))
                # 進場後 bid：贏的維持 0.97（第 2 筆曾跌到 0.85 = 假停損），輸的一路到 0.01
                for k, bid in enumerate(([0.97, 0.85, 0.97] if i == 2 else [0.97, 0.97]) if won else [0.6, 0.2, 0.01]):
                    db.execute("INSERT INTO sim_quotes (run_id, ts, asset_id, window_slug, remaining_seconds, up_bid, up_ask, down_bid, down_ask) VALUES (1,?,?,?,?,?,?,?,?)",
                               (t["entryTime"] + 5 + k * 5, "btc", slug, 50 - k * 5, bid, bid + 0.01, 1 - bid - 0.01, 1 - bid))
            db.commit(); db.close()
            rows = tools.roi_rows(dbp, min_trades=10)
            self.assertEqual(len(rows), 1); r = rows[0]
            self.assertEqual(r["n"], 12); self.assertAlmostEqual(r["winRate"], 100 * 11 / 12, places=3)
            self.assertAlmostEqual(r["lossesPerWin"], 98.0 / 1.2, places=3)
            text = tools.roi_table(dbp, {"btc-auto-45-60s-098-099": "BTC 45~60"})
            self.assertIn("BTC 45~60", text); self.assertIn("1輸=81.7贏", text)
            n, reps = tools.stop_replay_rows(dbp, "btc-auto-45-60s-098-099", "btc")
            self.assertEqual(n, 12)
            by = {r["stop"]: r for r in reps}
            self.assertEqual((by[None]["wins"], by[None]["losses"]), (11, 1))
            self.assertEqual(by[0.88]["falseStops"], 1)                 # 第 2 筆跌到 0.85 被掃
            self.assertEqual(by[0.60]["falseStops"], 0)
            self.assertGreater(by[0.60]["net"], by[None]["net"])          # 真反轉在 0.6 砍掉 → 淨變好
            self.assertIn("停損回放", tools.stop_replay_table(dbp, "btc-auto-45-60s-098-099", "btc", "X"))

    def test_mirror_reset_disable_and_stake_check(self):
        with tempfile.TemporaryDirectory() as d:
            dbp = os.path.join(d, "sim.sqlite3"); db = _mkdb(dbp)
            now = time.time(); s1, s2, s3 = f"btc-updown-5m-{int(now) - 900}", f"btc-updown-5m-{int(now) - 600}", f"btc-updown-5m-{int(now) - 300}"
            for slug in (s1, s2):
                t = _trade(slug, "Up", 0.98, 10, 0.2, now - 500); db.execute("INSERT INTO sim_trades (run_id, variant_id, exit_time, trade_json) VALUES (1,'v1',?,?)", (t["exitTime"], json.dumps(t)))
            db.execute("INSERT INTO sim_window_diagnostics (run_id, variant_id, diagnostic_json, last_seen) VALUES (1,'v1',?,?)", (json.dumps({"windowSlug": s3, "firstSeenAt": now - 200, "lastReason": "favorite_no_leader"}), now))
            db.execute("INSERT INTO sim_state VALUES ('v1', 1, ?, ?)", (json.dumps({"totalPnl": 0.4}), now)); db.commit(); db.close()
            st = os.path.join(d, "live.json")
            json.dump({"trades": [{"windowSlug": s1, "dryRun": False, "entryTime": now - 500, "pnlEstimate": 0.1}, {"windowSlug": s3, "dryRun": False, "entryTime": now - 200, "pnlEstimate": 0.1}],
                       "windowDiagnostics": [{"windowSlug": s2, "firstSeenAt": now - 600, "lastReason": "mirror_budget_too_small"}]}, open(st, "w", encoding="utf-8"))
            rep = tools.mirror_report(dbp, st, "v1", 1.0, "實盤①")
            self.assertIn("兩邊都進 1 · 只實盤 1 · 只模擬 1", rep)
            self.assertIn("favorite_no_leader", rep); self.assertIn("mirror_budget_too_small", rep)
            # reset sim variant
            msg = tools.reset_sim_variant(dbp, "v1", os.path.join(d, "bk"))
            self.assertIn("成交 2 筆", msg)
            db = sqlite3.connect(dbp)
            self.assertEqual(db.execute("select count(*) from sim_trades where variant_id='v1'").fetchone()[0], 0)
            fresh = json.loads(db.execute("select state_json from sim_state where variant_id='v1'").fetchone()[0])
            self.assertIn("enabledAt", fresh); self.assertEqual(fresh["peakPortfolio"], 500.0); db.close()
            self.assertTrue(os.listdir(os.path.join(d, "bk")))
            # reset live states
            live = os.path.join(d, "l1.json"); base = os.path.join(d, "l1_baseline.json")
            json.dump({"position": None, "pendingSettlements": [], "trades": [{"dryRun": False}], "halted": True}, open(live, "w")); json.dump({"baselineBalance": 1}, open(base, "w"))
            msg = tools.reset_live_states([live], [base], os.path.join(d, "bk"))
            self.assertIn("重設", msg); self.assertFalse(os.path.exists(base))
            self.assertEqual(json.load(open(live))["halted"], False)
            json.dump({"position": {"dryRun": False}, "pendingSettlements": [], "trades": []}, open(live, "w"))
            with self.assertRaises(RuntimeError):
                tools.reset_live_states([live], [], os.path.join(d, "bk"))
            # disable list
            dp = os.path.join(d, "dis.json")
            self.assertEqual(tools.set_variant_disabled(dp, "a", True), ["a"])
            self.assertEqual(tools.set_variant_disabled(dp, "b", True), ["a", "b"])
            self.assertEqual(tools.set_variant_disabled(dp, "a", False), ["b"])
            self.assertEqual(tools.disabled_variants(dp), ["b"])
        # stake check：餘額 23.56、22% → 5 股 OK；20% → 4 股 NG
        self.assertTrue(tools.stake_shares_check(23.56, 0, 22)["ok"])
        chk = tools.stake_shares_check(23.56, 0, 20)
        self.assertFalse(chk["ok"]); self.assertEqual(chk["shares"], 4); self.assertAlmostEqual(chk["minPctNeeded"], 4.95 / 23.56 * 100, places=3)
        self.assertEqual(tools.stake_shares_check(18.6, 4.95, 22)["shares"], 5)   # 算法 B：另一盤持倉成本算進總資產


if __name__ == "__main__":
    unittest.main()
