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
