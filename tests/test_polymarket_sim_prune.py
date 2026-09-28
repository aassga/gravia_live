# -*- coding: utf-8 -*-
"""資料庫清理：只刪過期報價與診斷，成交與狀態不動。"""
import os
import sqlite3
import tempfile
import time
import unittest

import polymarket_sim_prune as prune


class PruneTests(unittest.TestCase):
    def _mkdb(self, path):
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE sim_quotes (id INTEGER PRIMARY KEY, ts REAL, asset_id TEXT, window_slug TEXT, up_bid REAL)")
        db.execute("CREATE TABLE sim_window_diagnostics (id INTEGER PRIMARY KEY, variant_id TEXT, diagnostic_json TEXT, last_seen REAL)")
        db.execute("CREATE TABLE sim_trades (id INTEGER PRIMARY KEY, variant_id TEXT, window_slug TEXT, exit_time REAL, trade_json TEXT)")
        db.execute("CREATE TABLE sim_state (variant_id TEXT PRIMARY KEY, state_json TEXT)")
        now = time.time()
        for i in range(10):                                    # 5 筆新、5 筆舊（10 天前）
            ts = now - (1 if i < 5 else 10) * 86400
            db.execute("INSERT INTO sim_quotes (ts, asset_id, window_slug, up_bid) VALUES (?,?,?,?)", (ts, "btc", f"w{i}", 0.5))
            db.execute("INSERT INTO sim_window_diagnostics (variant_id, diagnostic_json, last_seen) VALUES (?,?,?)", ("v", "{}", ts))
        db.execute("INSERT INTO sim_trades (variant_id, window_slug, exit_time, trade_json) VALUES ('v','w0',?, '{}')", (now - 10 * 86400,))
        db.execute("INSERT INTO sim_state VALUES ('v', '{}')")
        db.commit(); db.close()

    def test_prune_keeps_recent_and_never_touches_trades(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "sim.sqlite3")
            self._mkdb(p)
            dry = prune.prune(p, keep_days=7, dry_run=True)
            self.assertEqual((dry["quotesOld"], dry["quotesTotal"], dry["diagnosticsOld"]), (5, 10, 5))
            db = sqlite3.connect(p)
            self.assertEqual(db.execute("select count(*) from sim_quotes").fetchone()[0], 10)   # dry-run 不刪
            db.close()

            res = prune.prune(p, keep_days=7)
            self.assertEqual(res["quotesOld"], 5)
            db = sqlite3.connect(p)
            self.assertEqual(db.execute("select count(*) from sim_quotes").fetchone()[0], 5)
            self.assertEqual(db.execute("select count(*) from sim_window_diagnostics").fetchone()[0], 5)
            self.assertEqual(db.execute("select count(*) from sim_trades").fetchone()[0], 1)     # 成交保留（即使 10 天前）
            self.assertEqual(db.execute("select count(*) from sim_state").fetchone()[0], 1)
            db.close()

            again = prune.prune(p, keep_days=7)                                                  # 再跑一次沒東西可刪
            self.assertEqual(again["quotesOld"], 0)

    def test_keep_days_zero_clears_all_quotes_but_keeps_trades(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "sim.sqlite3")
            self._mkdb(p)
            prune.prune(p, keep_days=0)
            db = sqlite3.connect(p)
            self.assertEqual(db.execute("select count(*) from sim_quotes").fetchone()[0], 0)
            self.assertEqual(db.execute("select count(*) from sim_trades").fetchone()[0], 1)
            db.close()


if __name__ == "__main__":
    unittest.main()
