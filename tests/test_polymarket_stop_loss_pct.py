"""比例停損：門檻隨部位縮放，而固定金額門檻不會。

2026-10-19 依使用者要求把金額停損改成比例停損。原本的缺陷：健檢本來就是用
USD_STOP_FRACTIONS（成本的 2%~60%）挑門檻，但寫入前乘上「當時的中位成本」換成美元，
之後部位隨現金複利成長，那個凍住的美元值就相對越來越緊——參數自己在漂移。

這裡最重要的測試是 test_pct_threshold_scales_with_position_size：它直接對比
同一個比例在大小部位上的行為，以及同一個金額在大小部位上的行為。
"""
import math
import unittest

import polymarket_server as sim
import polymarket_sim_doctor as doctor


def _pos(shares, entry, fee=1.0):
    notional = shares * entry
    return {"side": "Up", "shares": float(shares), "entryPrice": float(entry),
            "entryNotional": notional, "entryFee": fee, "stakeUsd": notional + fee,
            "hedged": False, "windowSlug": "w", "entryTime": 0.0}


def _book(bid, size=10_000):
    return {"tickSize": 0.01, "minOrderSize": 1,
            "bids": [{"price": bid, "size": size}], "asks": [{"price": bid + 0.01, "size": size}]}


class StopPctTriggerTests(unittest.TestCase):
    def test_pct_threshold_scales_with_position_size(self):
        # 比例停損：大小部位都在「虧掉下注額 37%」時觸發
        for shares in (20, 200, 2000):
            pos = _pos(shares, 0.88)
            stake = pos["stakeUsd"]
            # bid 剛好讓帳面虧損 ~= 37% 的下注額
            target = -0.37 * stake
            lo, hi = 0.001, 0.88
            for _ in range(60):                       # 二分找出對應的 bid
                mid = (lo + hi) / 2
                mark = mid * shares - sim.taker_fee(shares, mid) - (pos["entryNotional"] + pos["entryFee"])
                if mark < target: lo = mid
                else: hi = mid
            just_below = lo                            # 略低於門檻 -> 該觸發
            r, _, mark = sim._stop_trigger(pos, _book(just_below), None, None, 0.37)
            self.assertEqual(r, "favorite_stop_loss_pct", f"{shares} 股應觸發")
            self.assertLessEqual(mark, -0.37 * stake + 1e-6)
            # 虧損只有 10% 時不該觸發
            r2, _, _ = sim._stop_trigger(_pos(shares, 0.88), _book(0.80), None, None, 0.37)
            self.assertIsNone(r2, f"{shares} 股、只虧一點不該觸發")

    def test_fixed_usd_threshold_does_not_scale(self):
        # 對照：同一個 $7.85 門檻，在小部位是 37%、在大部位只有 3.7%——這就是原本的缺陷
        small = _pos(20, 0.88)
        big = _pos(200, 0.88)
        self.assertAlmostEqual(7.85 / small["stakeUsd"], 0.4220, places=3)
        self.assertAlmostEqual(7.85 / big["stakeUsd"], 0.04435, places=4)
        # 大部位只要跌一點點（帳面虧 $7.85）就被掃出，遠早於比例門檻
        r_usd, _, _ = sim._stop_trigger(big, _book(0.835), None, 7.85)
        self.assertEqual(r_usd, "favorite_stop_loss_usd")
        r_pct, _, _ = sim._stop_trigger(big, _book(0.835), None, None, 0.37)
        self.assertIsNone(r_pct, "同樣的跌幅，比例門檻不該觸發")

    def test_pct_takes_precedence_over_usd(self):
        pos = _pos(100, 0.88)
        r, _, _ = sim._stop_trigger(pos, _book(0.30), None, 7.85, 0.37)
        self.assertEqual(r, "favorite_stop_loss_pct")

    def test_zero_or_none_pct_disabled(self):
        pos = _pos(100, 0.88)
        for pct in (None, 0, 0.0):
            r, _, _ = sim._stop_trigger(pos, _book(0.05), None, None, pct)
            self.assertIsNone(r, f"pct={pct!r} 應視為關閉")

    def test_falls_back_to_paid_cost_when_stake_missing(self):
        pos = _pos(100, 0.88)
        pos.pop("stakeUsd")
        r, _, _ = sim._stop_trigger(pos, _book(0.30), None, None, 0.37)
        self.assertEqual(r, "favorite_stop_loss_pct")


class StopPctOverrideTests(unittest.TestCase):
    def setUp(self):
        self.vid = "btc-15m-last10-30-092-095"   # 必須是單腿變體
        self.v = sim.AB_VARIANT_BY_ID[self.vid]
        self.orig = {k: self.v.get(k) for k in
                     ("favoriteStopLossPct", "favoriteStopLossUsd", "label", "noStop")}

        def restore():
            for k, val in self.orig.items():
                if val is None and k in ("favoriteStopLossPct",):
                    self.v.pop(k, None)
                else:
                    self.v[k] = val
        self.addCleanup(restore)

    def test_override_sets_pct_and_clears_usd(self):
        self.v["label"] = "測試變體"
        self.v["favoriteStopLossUsd"] = 7.85
        sim.apply_variant_overrides({self.vid: {"favoriteStopLossPct": 0.37}})
        self.assertEqual(self.v["favoriteStopLossPct"], 0.37)
        self.assertIsNone(self.v["favoriteStopLossUsd"], "設了比例就該清掉金額，不能兩套並存")
        self.assertIn("下注額37%", self.v["label"].replace(" ", ""))

    def test_override_clearing_pct_removes_label_suffix(self):
        self.v["label"] = "測試變體"
        sim.apply_variant_overrides({self.vid: {"favoriteStopLossPct": 0.4}})
        sim.apply_variant_overrides({self.vid: {"favoriteStopLossPct": None}})
        self.assertIsNone(self.v["favoriteStopLossPct"])
        self.assertEqual(self.v["label"], "測試變體")


class SnapshotExposureTests(unittest.TestCase):
    def test_snapshot_carries_stop_pct(self):
        # 健檢是從快照的 abVariants 讀「目前設定」。漏掉 favoriteStopLossPct 的話它會把現況
        # 誤判成「不停損」，拿最佳設定去跟錯的基準比較，然後亂調參數——所以這個欄位必須在。
        vid = "btc-15m-last10-30-092-095"
        v = sim.AB_VARIANT_BY_ID[vid]
        orig = v.get("favoriteStopLossPct")
        v["favoriteStopLossPct"] = 0.38
        self.addCleanup(lambda: v.__setitem__("favoriteStopLossPct", orig)
                        if orig is not None else v.pop("favoriteStopLossPct", None))
        payload = sim.build_snapshot() if hasattr(sim, "build_snapshot") else None
        if payload is None:
            self.skipTest("找不到快照建構函式，改由下面的欄位清單測試覆蓋")
        row = next(r for r in payload["abVariants"] if r["id"] == vid)
        self.assertEqual(row["favoriteStopLossPct"], 0.38)

    def test_snapshot_field_is_listed_in_source(self):
        # 不依賴能不能在測試環境建出完整快照：直接確認欄位有被列進 payload
        import pathlib
        src = pathlib.Path(sim.__file__).read_text(encoding="utf-8")
        self.assertIn('"favoriteStopLossPct": v.get("favoriteStopLossPct")', src)


class DoctorStopPctTests(unittest.TestCase):
    def _trade_and_path(self, shares=100, entry=0.88, dip_to=0.40):
        trade = {"shares": shares, "entryPrice": entry, "entryFee": 1.0,
                 "stakeUsd": shares * entry + 1.0, "side": "Up"}
        path = [(0.0, entry), (1.0, 0.70), (2.0, dip_to), (3.0, 0.95)]
        return trade, path

    def test_replay_honours_pct(self):
        trade, path = self._trade_and_path()
        cost = doctor.trade_cost(trade)
        held = doctor.replay_trade(trade, path, None, None)          # 不停損：抱到結算
        stopped = doctor.replay_trade(trade, path, None, None, None, 0.37)
        self.assertNotEqual(held, stopped)
        self.assertGreaterEqual(stopped, -cost)                      # 停損後虧損有上限

    def test_candidate_grid_is_fractions_not_dollars(self):
        # 健檢的候選應該是比例；不同大小的部位會挑出同一個比例（而不是同一個金額）
        items = []
        for shares in (50, 500):
            t, pth = self._trade_and_path(shares=shares)
            items.append({"trade": t, "path": pth})
        cmp = doctor.best_exit_config(items, None, None, None)
        self.assertIsNotNone(cmp["best"])
        self.assertIsNone(cmp["best"]["stopUsd"], "最佳設定不該再是金額停損")
        self.assertIn("stopPct", cmp["best"])
        for s in doctor.USD_STOP_FRACTIONS:
            self.assertLessEqual(s, 0.60)                            # 網格是比例（<=60%）

    def test_fmt_stop_describes_pct(self):
        self.assertIn("下注額 37%", doctor._fmt_stop({"stopPrice": None, "stopUsd": None, "stopPct": 0.37}))
        self.assertEqual(doctor._fmt_stop({"stopPrice": None, "stopUsd": None, "stopPct": None}), "不停損")


if __name__ == "__main__":
    unittest.main()


class LiveUsdStopFallbackTests(unittest.TestCase):
    """2026-10-21：實盤的金額停損要 fallback 到變體，否則模擬盤有停損、實盤沒有。"""

    def test_source_has_variant_fallback(self):
        import pathlib
        src = pathlib.Path(__file__).resolve().parents[1] / "polymarket_live_strategy.py"
        text = src.read_text(encoding="utf-8")
        # 這四個常數都該有「env 沒設就吃變體」的 fallback，金額停損原本漏掉
        for field in ("favoriteStopLossUsd", "favoriteStopLossPct",
                      "favoriteMinPrice", "favoriteMaxPrice"):
            self.assertIn(f'_LIVE_VARIANT.get("{field}"', text, field)
