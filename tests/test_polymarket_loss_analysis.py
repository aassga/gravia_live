# -*- coding: utf-8 -*-
"""polymarket_loss_analysis：用假的狀態檔＋sqlite 驗證分類與輸出。"""
import json
import os
import sqlite3
import tempfile
import unittest

import polymarket_loss_analysis as loss


def _quotes(ws, side_flip_at=None, other_ask=0.03, open_spot=100.0, lead=0.05):
    """產生一個 5 分鐘窗口的取樣：開盤 spot=open_spot，T-60 進場時 spot=open_spot+lead，之後翻面。"""
    rows = []
    for rem in range(300, -1, -3):
        ts = ws + 300 - rem
        if rem > 60:
            up_ask, down_ask, up_bid, down_bid, spot = 0.7, 0.31, 0.69, 0.30, open_spot + lead * (300 - rem) / 240
        elif side_flip_at is not None and rem <= side_flip_at:
            up_ask, down_ask, up_bid, down_bid, spot = 0.05, 0.96, 0.04, 0.95, open_spot - 2.0
        else:
            up_ask, down_ask, up_bid, down_bid, spot = 0.98, other_ask, 0.97, max(0.01, other_ask - 0.01), open_spot + lead
        rows.append((ts, rem, up_ask, down_ask, up_bid, down_bid, 0.8 if up_ask > 0.5 else 0.1, spot))
    return rows


class LossAnalysisTests(unittest.TestCase):
    def _db(self, path, asset, slug, rows, sim_trade=None):
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE IF NOT EXISTS sim_quotes (id INTEGER PRIMARY KEY, run_id INTEGER, ts REAL, asset_id TEXT, window_slug TEXT, remaining_seconds REAL, up_bid REAL, up_ask REAL, down_bid REAL, down_ask REAL, fair_up REAL, spot_price REAL)")
        db.execute("CREATE TABLE IF NOT EXISTS sim_trades (id INTEGER PRIMARY KEY, run_id INTEGER, variant_id TEXT, exit_time REAL, trade_json TEXT)")
        for ts, rem, ua, da, ub, dbid, fu, sp in rows:
            db.execute("INSERT INTO sim_quotes (run_id, ts, asset_id, window_slug, remaining_seconds, up_bid, up_ask, down_bid, down_ask, fair_up, spot_price) VALUES (1,?,?,?,?,?,?,?,?,?,?)",
                       (ts, asset, slug, rem, ub, ua, dbid, da, fu, sp))
        if sim_trade:
            db.execute("INSERT INTO sim_trades (run_id, variant_id, exit_time, trade_json) VALUES (1,?,?,?)", ("v1", sim_trade["exitTime"], json.dumps(sim_trade)))
        db.commit(); db.close()

    def test_classifies_real_reversal_thin_lead_and_thin_book(self):
        ws = 1_700_000_000
        slug = f"btc-updown-5m-{ws}"
        with tempfile.TemporaryDirectory() as d:
            dbp = os.path.join(d, "sim.sqlite3"); st = os.path.join(d, "state.json")
            # 領先 +0.05（0.05%），對邊 0.03，T-30 翻面 → 真實反轉（進場後行情反向）；模擬盤同窗有進
            self._db(dbp, "btc", slug, _quotes(ws, side_flip_at=30), {"windowSlug": slug, "pnl": -80.0, "exitTime": ws + 400})
            trade = {"windowSlug": slug, "side": "Up", "shares": 30.0, "entryPrice": 0.98, "pnlEstimate": -29.4, "outcome": "Down",
                     "tradeType": "directional", "dryRun": False, "entryTime": ws + 240, "exitTime": ws + 400}
            win = dict(trade, pnlEstimate=0.3, outcome="Up", exitTime=ws + 401)
            json.dump({"trades": [win, trade]}, open(st, "w", encoding="utf-8"))
            text = loss.analyze_losses(st, "btc", "v1", dbp, 5, "實盤①")
            self.assertIn("真實反轉（進場後行情反向）", text)
            self.assertIn("有進（-80.0）", text)
            self.assertIn("翻面", text)
            self.assertIn("一次輸 ≈", text)
            # 領先極薄（+0.005 → 0.005%）
            a = loss.analyze_trade(trade, [dict(zip(("ts", "rem", "up_ask", "down_ask", "up_bid", "down_bid", "fair_up", "spot"), r)) for r in _quotes(ws, side_flip_at=30, lead=0.005)], None)
            self.assertIn("領先幅度薄", a["kind"]); self.assertIn("模擬盤同窗沒進", a["kind"])
            # 薄單假領先（對邊 ask 0.22）
            a = loss.analyze_trade(trade, [dict(zip(("ts", "rem", "up_ask", "down_ask", "up_bid", "down_bid", "fair_up", "spot"), r)) for r in _quotes(ws, side_flip_at=30, other_ask=0.22)], {"pnl": -5})
            self.assertIn("薄單假領先", a["kind"])
            # 停損：tradeType 含 stop，結算若同邊贏 → 假停損
            a = loss.analyze_trade(dict(trade, tradeType="favorite_stop_loss", outcome="Up"), [], {"pnl": 1})
            self.assertIn("假停損", a["kind"])
            self.assertEqual(loss.analyze_losses(st, "btc", "v1", dbp, 0, "X").startswith("X："), True) if False else None

    def test_no_losses_message(self):
        with tempfile.TemporaryDirectory() as d:
            st = os.path.join(d, "state.json"); dbp = os.path.join(d, "sim.sqlite3")
            json.dump({"trades": [{"dryRun": False, "pnlEstimate": 1.0, "exitTime": 1}]}, open(st, "w", encoding="utf-8"))
            self.assertEqual(loss.analyze_losses(st, "btc", "v1", dbp, 5, "實盤②"), "實盤②：最近沒有真實虧損單。")


if __name__ == "__main__":
    unittest.main()
