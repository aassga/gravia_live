import json
import os
import tempfile
import unittest

import polymarket_autopilot as ap
import polymarket_weekly_scan as scan


def _t(wallet, side, price, size, rel, buy=True):
    return {"proxyWallet": wallet, "side": "BUY" if buy else "SELL", "outcome": side,
            "price": str(price), "size": str(size), "timestamp": str(1_000_000 + rel)}


class AutopilotTests(unittest.TestCase):
    def test_candidates_come_from_positive_cells_only(self):
        scan.WINDOW_SECONDS, scan.LATE_SECONDS = 300, 60
        windows = []
        for i in range(120):            # 0.93 買、結算前 35s、全部贏 → 0.92～0.95 × 30～45s 一格 n=120
            windows.append({"slug": f"s{i}", "start": 1_000_000 + i * 300, "outcome": "Up",
                            "trades": [_t(f"w{i}", "Up", 0.93, 10, i * 300 + 265)]})
        for i in range(120, 240):       # 0.985 買、結算前 10s、全部輸 → 0.98+ × 5～15s 一格為負，不能成為候選
            windows.append({"slug": f"s{i}", "start": 1_000_000 + i * 300, "outcome": "Down",
                            "trades": [_t(f"l{i}", "Up", 0.985, 10, i * 300 + 290)]})
        cands = scan.late_favorite_candidates(windows, "eth")
        self.assertEqual(len(cands), 1)
        top = cands[0]
        self.assertEqual(top["assetId"], "eth-alt")
        self.assertEqual((top["favoriteMinPrice"], top["favoriteMaxPrice"]), (0.92, 0.95))
        self.assertEqual((top["favoriteMinRemaining"], top["favoriteWindowSeconds"]), (30.0, 45.0))
        self.assertEqual(top["asset"]["slugPrefix"], "eth-updown-5m-")
        self.assertEqual(top["id"], "eth-alt-auto-30-45s-092-095")
        self.assertAlmostEqual(top["stats"]["totalPnl"], 120 * 10 * 0.07, places=4)   # 主要看總收益
        self.assertEqual(top["stats"]["score"], top["stats"]["totalPnl"])

    def test_pick_new_and_disable(self):
        cands = [
            {"id": "a1", "market": "eth", "stats": {"score": 5}},
            {"id": "a2", "market": "eth", "stats": {"score": 4}},
            {"id": "b1", "market": "sol", "stats": {"score": 3}},
            {"id": "c1", "market": "xrp", "stats": {"score": 2}},
            {"id": "d1", "market": "btc", "stats": {"score": 1}},
        ]
        chosen = ap.pick_new_variants(cands, existing_ids={"c1"}, disabled_ids={"b1"}, max_total=3, max_per_market=1)
        self.assertEqual([c["id"] for c in chosen], ["a1", "d1"])          # a2 同市場第二個、b1 已停用、c1 已存在
        chosen = ap.pick_new_variants(cands, existing_ids={"c1"}, disabled_ids={"b1"})   # 預設：不限
        self.assertEqual([c["id"] for c in chosen], ["a1", "a2", "d1"])
        self.assertEqual((ap.MAX_ADD_PER_RUN, ap.MAX_ADD_PER_MARKET), (None, None))
        # 2026-09-17 白名單：不在白名單的市場（doge-15m）不加
        # 2026-09-23：白名單預設為空 = 不限市場；傳 markets 才限制
        cands2 = cands + [{"id": "e1", "market": "doge-15m", "stats": {"score": 99}}]
        chosen = ap.pick_new_variants(cands2, existing_ids=set(), disabled_ids=set())
        self.assertIn("e1", [c["id"] for c in chosen])
        chosen = ap.pick_new_variants(cands2, existing_ids=set(), disabled_ids=set(), markets={"btc"})
        self.assertNotIn("e1", [c["id"] for c in chosen])
        self.assertEqual(ap.CANDIDATE_MARKETS, set())                        # 2026-09-23：不限市場
        self.assertIsNone(ap.MAX_ADD_PER_MARKET_DEFAULT)                     # 也不限每個市場加幾組
        many = [{"id": f"m{i}", "market": "doge-15m", "stats": {"score": 100 - i}} for i in range(6)]
        self.assertEqual(len(ap.pick_new_variants(many, set(), set())), 6)    # 全部都加
        self.assertEqual(len(ap.pick_new_variants(many, {"m0", "m1"}, {"m2"})), 3)   # 已存在／已停用的跳過
        sims = [{"id": "x", "totalPnl": -350.0, "totalTrades": 9}, {"id": "y", "totalPnl": -349.9}, {"id": "z", "totalPnl": 12}]
        self.assertEqual([d["id"] for d in ap.pick_disable(sims, set())], ["x"])
        self.assertEqual(ap.pick_disable(sims, {"x"}), [])

    def test_server_loads_auto_variants_and_new_assets(self):
        # polymarket_server 啟動時讀 sim_auto_variants.json：未知資產（doge-15m）會自動加進目錄
        with tempfile.TemporaryDirectory() as d:
            auto = os.path.join(d, "auto.json"); dis = os.path.join(d, "dis.json")
            json.dump([{"id": "doge-15m-auto-30-45s-092-095", "assetId": "doge-15m",
                        "label": "⚙ 自動 DOGE 15m", "favoriteWindowSeconds": 135, "favoriteMinRemaining": 90,
                        "favoriteMinPrice": 0.92, "favoriteMaxPrice": 0.95,
                        "asset": {"id": "doge-15m", "label": "DOGE 15m", "slugPrefix": "doge-updown-15m-", "windowSeconds": 900}},
                       {"id": "eth-alt-auto-30-45s-092-095", "assetId": "eth-alt", "label": "x",
                        "favoriteWindowSeconds": 45, "favoriteMinRemaining": 30, "favoriteMinPrice": 0.92, "favoriteMaxPrice": 0.95}],
                      open(auto, "w", encoding="utf-8"))
            json.dump(["eth-alt-auto-30-45s-092-095"], open(dis, "w", encoding="utf-8"))
            env = dict(os.environ, POLY_SIM_AUTO_VARIANTS_FILE=auto, POLY_SIM_DISABLED_VARIANTS_FILE=dis,
                       POLY_SIM_ASSETS="btc,btc-15m,eth-alt", POLY_SIM_DISABLED_VARIANTS="")
            import subprocess, sys
            code = ("import polymarket_server as s, json;"
                    "print(json.dumps({'assets':[a['id'] for a in s.ASSETS],"
                    "'ids':[v['id'] for v in s.AB_VARIANTS if v.get('auto')],"
                    "'auto':[v for v in s.AB_VARIANTS if v['id']=='doge-15m-auto-30-45s-092-095'][0]}))")
            out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=120,
                                 cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.assertEqual(out.returncode, 0, out.stderr[-2000:])
            data = json.loads(out.stdout.strip().splitlines()[-1])
            self.assertIn("doge-15m", data["assets"])
            self.assertEqual(data["ids"], ["doge-15m-auto-30-45s-092-095"])      # eth 那個在停用清單
            self.assertTrue(data["auto"]["lateFavorite"]); self.assertFalse(data["auto"]["simOnly"])   # 2026-09-16 開放實盤選用
            self.assertEqual(data["auto"]["favoriteMinRemaining"], 90.0)

    def test_render_telegram_mentions_live_untouched(self):
        result = {"hours": 24, "markets": {"eth": {"best": None}}, "added": [], "disabled": [], "restart": "none"}
        msgs = ap.render_telegram(result)
        self.assertTrue(any("實盤設定未動" in m for m in msgs))
        self.assertTrue(ap.AUTO_ADD_ENABLED)                                   # 2026-09-23 依使用者要求：掃描後自動新增
        self.assertEqual(ap.SCAN_HOURS, 24.0)                                  # 每 3 天跑一次，每次取 24h 樣本
        self.assertFalse(any("自動新增已關閉" in m for m in msgs))            # 2026-09-23：自動新增已開啟
        self.assertTrue(all(len(m) <= 4000 for m in msgs))


if __name__ == "__main__":
    unittest.main()
