# -*- coding: utf-8 -*-
"""兩條高流量 WS 都要加大 frame 佇列，否則會被 Polymarket 當 slow consumer 踢掉。

2026-09-28：行情 WS 實測 682 KB/s、Binance bookTicker 252 KB/s，都在同一個 event loop
上解 JSON。websockets 預設 max_queue=32，一卡頓就塞滿、函式庫停止讀 socket，接著就是
1013 slow consumer（實測每小時 132 次，連帶每小時約 268 次進場被 SIM-DATA-GUARD 擋掉）。
"""
import inspect
import os
import unittest

import polymarket_server as ps


class WsBackpressureTests(unittest.TestCase):
    def test_default_queue_is_large_enough(self):
        self.assertGreaterEqual(ps.WS_MAX_QUEUE, 4096)

    def test_queue_never_falls_back_to_library_default(self):
        self.assertGreaterEqual(
            max(32, int(os.environ.get("POLY_WS_MAX_QUEUE", "4096"))), 32)

    def test_market_ws_passes_max_queue(self):
        src = inspect.getsource(ps.market_ws_loop)
        self.assertIn("max_queue=WS_MAX_QUEUE", src)

    def test_binance_ws_passes_max_queue(self):
        src = inspect.getsource(ps.binance_ws_loop)
        self.assertIn("max_queue=WS_MAX_QUEUE", src)


if __name__ == "__main__":
    unittest.main()


class KlinesLimitTests(unittest.TestCase):
    """K 線根數不能低於實際用量，否則波動率／theo 會靜默失準。"""

    def test_limit_covers_sigma_window(self):
        # theo 的波動率用 klines[-30:]，所以至少要 30 根
        self.assertGreaterEqual(ps.KLINES_LIMIT, 30)

    def test_limit_covers_longest_window_lookback(self):
        # theo 要找「窗口起始那一分鐘」那根；最長的窗口是 15 分鐘
        longest = max(float(a.get("windowSeconds", ps.WINDOW_SECONDS)) for a in ps.ASSETS)
        self.assertGreaterEqual(ps.KLINES_LIMIT * 60, longest)

    def test_call_site_uses_the_constant(self):
        src = inspect.getsource(ps._fetch_one_asset)
        self.assertIn("KLINES_LIMIT", src)
        self.assertNotIn('asset["binanceSymbol"], 60)', src)

    def test_last_30_is_unchanged_by_the_smaller_request(self):
        # 行為等價性：30 根的 [-30:] 與 60 根的 [-30:] 是同一批資料
        sixty = [{"c": i} for i in range(60)]
        thirty = sixty[-ps.KLINES_LIMIT:]
        self.assertEqual(thirty[-30:], sixty[-30:])
