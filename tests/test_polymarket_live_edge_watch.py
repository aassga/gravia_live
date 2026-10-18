"""實盤判準的自動推播：什麼時候該吵、什麼時候要安靜。

2026-10-18 依使用者要求自動化。這裡的重點是「安靜」—— 絕大多數日子結論都是 observe，
如果每天都推播，這個機制很快就會被當成雜訊忽略，那就等於沒有。
"""
import json
import os
import tempfile
import unittest

import polymarket_live_edge_check as ec
import polymarket_live_edge_watch as ew


class EdgeWatchNotifyTests(unittest.TestCase):
    def test_quiet_when_verdict_unchanged(self):
        # 最重要的一條：同樣的結論不要天天吵
        prev = {"variantId": "v1", "verdict": "observe", "matured": False}
        notify, why = ew.decide_notify(prev, "v1", "observe", matured=False)
        self.assertFalse(notify, why)

    def test_notify_when_sample_matures(self):
        # 樣本剛達到門檻 → 判準開始生效，要通知（即使結論還是 observe）
        prev = {"variantId": "v1", "verdict": "observe", "matured": False}
        notify, why = ew.decide_notify(prev, "v1", "observe", matured=True)
        self.assertTrue(notify)
        self.assertIn(str(ec.MIN_TRADES), why)

    def test_quiet_once_already_matured_and_same_verdict(self):
        prev = {"variantId": "v1", "verdict": "observe", "matured": True}
        notify, _ = ew.decide_notify(prev, "v1", "observe", matured=True)
        self.assertFalse(notify)

    def test_notify_when_verdict_changes(self):
        for before, after in (("observe", "switch"), ("observe", "keep"), ("keep", "switch")):
            prev = {"variantId": "v1", "verdict": before, "matured": True}
            notify, why = ew.decide_notify(prev, "v1", after, matured=True)
            self.assertTrue(notify, f"{before} -> {after}")
            self.assertIn(after, why)

    def test_variant_switch_resets_quietly(self):
        # 換策略後樣本歸零，不能拿舊策略的結論當比較基準、也不該因此推播
        prev = {"variantId": "old", "verdict": "switch", "matured": True}
        notify, why = ew.decide_notify(prev, "new", "observe", matured=False)
        self.assertFalse(notify)
        self.assertIn("靜默", why)

    def test_variant_switch_notifies_if_new_one_already_has_a_verdict(self):
        prev = {"variantId": "old", "verdict": "observe", "matured": True}
        notify, why = ew.decide_notify(prev, "new", "switch", matured=True)
        self.assertTrue(notify)
        self.assertIn("換策略", why)

    def test_first_ever_run_is_quiet_while_sample_is_small(self):
        notify, _ = ew.decide_notify({}, "v1", "observe", matured=False)
        self.assertFalse(notify)


class EdgeWatchStateTests(unittest.TestCase):
    def test_state_round_trip_and_missing_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "s.json")
            self.assertEqual(ew.load_watch_state(p), {})          # 檔案不存在不該爆
            ew.save_watch_state({"variantId": "v1", "verdict": "keep"}, p)
            self.assertEqual(ew.load_watch_state(p)["verdict"], "keep")
            self.assertFalse(os.path.exists(p + ".tmp"))           # 原子寫入後不留 tmp

    def test_corrupt_state_is_treated_as_no_history(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "s.json")
            with open(p, "w", encoding="utf-8") as f:
                f.write("{ not json")
            self.assertEqual(ew.load_watch_state(p), {})


class EdgeWatchRunTests(unittest.TestCase):
    def _run_with(self, trades, variant_id, prev=None):
        """跑一次 run()，攔下 TG 推播，回傳 (有沒有推, 推的內容, 存下來的狀態)。"""
        sent = []
        with tempfile.TemporaryDirectory() as d:
            live = os.path.join(d, "live.json")
            with open(live, "w", encoding="utf-8") as f:
                json.dump({"trades": trades}, f)
            watch = os.path.join(d, "watch.json")
            if prev is not None:
                ew.save_watch_state(prev, watch)
            old = (ew.LIVE_STATE_FILE, ew.STATE_FILE, ew.send_telegram,
                   os.environ.get("POLY_LIVE_VARIANT_ID"))
            ew.LIVE_STATE_FILE, ew.STATE_FILE = live, watch
            ew.send_telegram = lambda text: sent.append(text)
            os.environ["POLY_LIVE_VARIANT_ID"] = variant_id
            try:
                rc = ew.run()
                self.assertEqual(rc, 0)
                return bool(sent), (sent[0] if sent else ""), ew.load_watch_state(watch)
            finally:
                ew.LIVE_STATE_FILE, ew.STATE_FILE, ew.send_telegram = old[0], old[1], old[2]
                if old[3] is None:
                    os.environ.pop("POLY_LIVE_VARIANT_ID", None)
                else:
                    os.environ["POLY_LIVE_VARIANT_ID"] = old[3]

    @staticmethod
    def _trades(n, pnl, vid):
        return [{"pnlEstimate": pnl, "feesEstimate": 0.2, "stakeUsd": 40.0,
                 "entryPrice": 0.93, "tradeType": "directional", "variantId": vid}
                for _ in range(n)]

    def test_small_sample_runs_quietly_and_records_state(self):
        notified, _, state = self._run_with(self._trades(10, +2.0, "v1"), "v1")
        self.assertFalse(notified)
        self.assertEqual(state["verdict"], "observe")
        self.assertEqual(state["n"], 10)
        self.assertFalse(state["matured"])

    def test_matured_sample_pushes_once_then_stays_quiet(self):
        trades = self._trades(ec.MIN_TRADES, +2.0, "v1")
        notified, text, state = self._run_with(trades, "v1",
                                               prev={"variantId": "v1", "verdict": "observe", "matured": False})
        self.assertTrue(notified)
        self.assertIn("實盤優勢判準", text)
        self.assertEqual(state["verdict"], "keep")
        # 第二天同樣的資料 → 不該再推
        notified2, _, _ = self._run_with(trades, "v1", prev=state)
        self.assertFalse(notified2)

    def test_missing_variant_id_errors_out(self):
        old = os.environ.pop("POLY_LIVE_VARIANT_ID", None)
        try:
            self.assertEqual(ew.run(), 2)
        finally:
            if old is not None:
                os.environ["POLY_LIVE_VARIANT_ID"] = old


if __name__ == "__main__":
    unittest.main()
