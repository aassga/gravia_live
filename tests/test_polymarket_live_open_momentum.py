# -*- coding: utf-8 -*-
"""實盤「中段動能方向性」進場判斷（2026-09-29 依使用者要求新增，實盤①用）。

這條是真錢路徑，所以每一道閘門都要有測試：時間窗、動能門檻、價格區間、方向選擇。
依專案慣例用 patch.object 修補模組常數（不動 POLY_LIVE_* 環境變數——那些常數是 import
時固定的，改環境變數會污染同一個行程裡的其他測試）。書本新鮮度／下單計畫／預算換算
是既有共用元件，這裡用替身隔離，只驗證這個策略自己的判斷規則。
"""
import inspect
import os
import unittest
from unittest import mock

import polymarket_live_strategy as strategy

WINDOW_SECONDS = 300.0
MIN_ELAPSED, MAX_ELAPSED = 60.0, 120.0
MIN_PRICE, MAX_PRICE = 0.45, 0.60
MIN_MOVE_PCT = 0.02


def _book(ask, bid=None):
    bid = ask - 0.01 if bid is None else bid
    return {
        "asks": [{"price": ask, "size": 5000}],
        "bids": [{"price": bid, "size": 5000}],
        "minOrderSize": 1,
        "quoteSource": "websocket",
    }


def _klines(prev_close, last_close):
    return [{"t": 0, "o": prev_close, "h": prev_close, "l": prev_close, "c": prev_close, "v": 1},
            {"t": 60000, "o": prev_close, "h": last_close, "l": prev_close, "c": last_close, "v": 1}]


class OpenMomentumPlanTests(unittest.TestCase):
    def setUp(self):
        self.remaining = WINDOW_SECONDS - 90.0       # T+90s，落在 60~120 之間
        patches = [
            mock.patch.object(strategy, "LIVE_ASSET_ID", "doge"),
            mock.patch.object(strategy, "OPEN_MOMENTUM_WINDOW_SECONDS", WINDOW_SECONDS),
            mock.patch.object(strategy, "OPEN_MOMENTUM_MIN_ELAPSED", MIN_ELAPSED),
            mock.patch.object(strategy, "OPEN_MOMENTUM_MAX_ELAPSED", MAX_ELAPSED),
            mock.patch.object(strategy, "OPEN_MOMENTUM_MIN_PRICE", MIN_PRICE),
            mock.patch.object(strategy, "OPEN_MOMENTUM_MAX_PRICE", MAX_PRICE),
            mock.patch.object(strategy, "OPEN_MOMENTUM_MIN_MOVE_PCT", MIN_MOVE_PCT),
            mock.patch.object(strategy, "_live_direction_book_is_fresh", return_value=True),
            mock.patch.object(strategy, "_target_pair_order", return_value=(15.0, 15.0)),
            mock.patch.object(strategy, "_buy_plan", side_effect=lambda side, book, shares, *a, **k: {
                "side": side, "shares": shares, "limitPrice": float(book["asks"][0]["price"])}),
            mock.patch.object(strategy, "record_live_window_diagnostic", return_value={}),
        ]
        for p in patches:
            p.start(); self.addCleanup(p.stop)
        self._old_klines = strategy.sim.markets_state["doge"].get("klines")
        self.addCleanup(lambda: strategy.sim.markets_state["doge"].__setitem__("klines", self._old_klines))
        strategy.sim.markets_state["doge"]["klines"] = _klines(0.10, 0.1002)   # +0.2%，高於門檻

    def plan(self, up_ask=0.55, down_ask=0.45, remaining=None):
        return strategy._open_momentum_plan(
            _book(up_ask), _book(down_ask),
            self.remaining if remaining is None else remaining,
            cash=100.0, diagnostic_slug="doge-updown-5m-1")

    # ── 時間窗 ────────────────────────────────────────────────────────────
    def test_too_early_is_rejected(self):
        self.assertIsNone(self.plan(remaining=WINDOW_SECONDS - 30.0))     # T+30s

    def test_too_late_is_rejected(self):
        self.assertIsNone(self.plan(remaining=WINDOW_SECONDS - 150.0))    # T+150s

    def test_inside_window_is_accepted(self):
        self.assertIsNotNone(self.plan())

    def test_window_boundaries_are_inclusive(self):
        self.assertIsNotNone(self.plan(remaining=WINDOW_SECONDS - MIN_ELAPSED))
        self.assertIsNotNone(self.plan(remaining=WINDOW_SECONDS - MAX_ELAPSED))

    # ── 動能門檻與方向 ────────────────────────────────────────────────────
    def test_momentum_below_threshold_is_rejected(self):
        strategy.sim.markets_state["doge"]["klines"] = _klines(0.10, 0.100001)   # +0.001%
        self.assertIsNone(self.plan())

    def test_positive_momentum_buys_up(self):
        self.assertEqual(self.plan()["side"], "Up")

    def test_negative_momentum_buys_down(self):
        strategy.sim.markets_state["doge"]["klines"] = _klines(0.10, 0.0998)     # -0.2%
        self.assertEqual(self.plan()["side"], "Down")

    def test_missing_klines_is_rejected(self):
        strategy.sim.markets_state["doge"]["klines"] = []
        self.assertIsNone(self.plan())

    def test_unusable_klines_do_not_raise(self):
        strategy.sim.markets_state["doge"]["klines"] = [{"c": 0.0}, {"c": 0.0}]   # 會 ZeroDivisionError
        self.assertIsNone(self.plan())

    # ── 價格區間 0.45~0.60 ────────────────────────────────────────────────
    def test_price_above_maximum_is_rejected(self):
        self.assertIsNone(self.plan(up_ask=MAX_PRICE + 0.01))

    def test_price_below_minimum_is_rejected(self):
        self.assertIsNone(self.plan(up_ask=MIN_PRICE - 0.01))

    def test_price_at_boundaries_is_accepted(self):
        self.assertIsNotNone(self.plan(up_ask=MAX_PRICE))
        self.assertIsNotNone(self.plan(up_ask=MIN_PRICE))

    def test_stale_book_is_rejected(self):
        with mock.patch.object(strategy, "_live_direction_book_is_fresh", return_value=False):
            self.assertIsNone(self.plan())

    # ── 股數 ──────────────────────────────────────────────────────────────
    def test_shares_are_whole_and_within_budget(self):
        plan = self.plan(up_ask=0.50)
        self.assertEqual(plan["shares"], float(int(plan["shares"])))
        self.assertLessEqual(plan["shares"] * 0.50, 15.0 + 1e-9)

    def test_below_minimum_shares_is_rejected(self):
        with mock.patch.object(strategy, "_target_pair_order", return_value=(0.0, 0.0)):
            self.assertIsNone(self.plan())


class OpenMomentumWiringTests(unittest.TestCase):
    """設定與接線：確認閘門互斥、dry-run 預設、變體參數來源。"""

    def test_dry_run_balance_defaults_to_100_usdc(self):
        self.assertEqual(strategy.DRY_RUN_BALANCE_USD, 100.0)

    def test_single_leg_path_is_mutually_exclusive_with_momentum(self):
        src = inspect.getsource(strategy)
        self.assertIn("not OPEN_MOMENTUM_ENABLED", src.split("SINGLE_LEG_ENTRY_ENABLED = (")[1][:200])

    def test_both_entry_paths_are_wired_and_guarded_once_per_window(self):
        # evaluate_and_act 只是包裝，真正的決策主體是 _evaluate_and_act_impl
        poll = inspect.getsource(strategy._evaluate_and_act_impl)
        self.assertIn("_try_open_momentum_entry", poll)
        ws = inspect.getsource(strategy._on_ws_tick_sync_impl)
        self.assertIn("_open_momentum_plan", ws)
        self.assertIn("openMomentumWindowSlug", ws)
        self.assertIn("openMomentumWindowSlug", inspect.getsource(strategy._try_open_momentum_entry))

    def test_no_stop_loss_is_configured_for_this_strategy(self):
        v = strategy.sim.AB_VARIANT_BY_ID["doge-mid-momentum-hold"]
        self.assertIsNone(v.get("directionStopLossPrice"))


class SimOnlyGateTests(unittest.TestCase):
    """simOnly 是「禁止真實下單」那道閘：只有 DOGE 這一組解除，其餘資產維持純模擬。"""

    def test_only_doge_is_opened_for_live(self):
        opened = [v["id"] for v in strategy.sim.AB_VARIANTS
                  if v["id"].endswith("mid-momentum-hold") and not v.get("simOnly")]
        self.assertEqual(opened, ["doge-mid-momentum-hold"])

    def test_doge_variant_carries_the_expected_parameters(self):
        v = strategy.sim.AB_VARIANT_BY_ID["doge-mid-momentum-hold"]
        self.assertTrue(v["openMomentum"])
        self.assertEqual((v["openMinElapsedSeconds"], v["openMaxElapsedSeconds"]), (MIN_ELAPSED, MAX_ELAPSED))
        self.assertEqual((v["openMinPrice"], v["openMaxPrice"]), (MIN_PRICE, MAX_PRICE))
        self.assertEqual(v["openMinMovePct"], MIN_MOVE_PCT)


class LiveDashboardWiringTests(unittest.TestCase):
    """看板只顯示現行策略真的用到的設定（2026-09-29：移除單注上限／現金保留，隱藏鎖利上限）。"""

    def _read(self, relpath):
        import os
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, relpath), encoding="utf-8") as f:
            return f.read()

    def test_status_server_reports_momentum_config(self):
        src = self._read("polymarket_live_status_server.py")
        for key in ("openMomentumEnabled", "openMomentumMinElapsed", "openMomentumMaxElapsed",
                    "openMomentumMinPrice", "openMomentumMaxPrice", "openMomentumMinMovePct",
                    "openMomentumStopLossPrice"):
            self.assertIn(key, src, key)

    def test_dashboard_hides_lock_sum_when_momentum_is_active(self):
        page = self._read(os.path.join("web", "polymarket_live.html")) if False else self._read("web/polymarket_live.html")
        self.assertIn("const momOn = !!cfg.openMomentumEnabled;", page)
        self.assertIn("cfg.directPairEnabled !== false && !momOn", page)

    def test_dashboard_shows_momentum_parameters(self):
        page = self._read("web/polymarket_live.html")
        self.assertIn("openMomentumMinMovePct", page)
        self.assertIn("openMomentumStopLossPrice", page)


if __name__ == "__main__":
    unittest.main()


class OpenMomentumHoldsToSettlementTests(unittest.IsolatedAsyncioTestCase):
    """2026-09-29 回歸：動能部位必須抱到結算。

    DRY-RUN 實測（20:11:01 進場、0.8 秒後就被掃出場）發現部位會掉進既有的
    market_bid_above_model_value 提早出場路徑。這個分支是看 pos["strategy"]，不是看
    OPEN_MOMENTUM_ENABLED，所以就算測試環境的實盤資產是 btc 也測得到。
    """

    def _position(self, strategy_name):
        return {
            "side": "Up", "shares": 27.0, "windowSlug": "w1", "hedged": False,
            "dryRun": True, "entryPrice": 0.55, "entryLimitPrice": 0.55,
            "strategy": strategy_name,
        }

    async def _run(self, strategy_name):
        fair = {"fairUp": 0.10, "fairDown": 0.90}        # 模型價值遠低於市場買價 → 會觸發提早出場
        book = _book(0.56, bid=0.95)                     # 可賣價很高
        with mock.patch.object(strategy, "live_state", {"position": self._position(strategy_name)}), \
             mock.patch.object(strategy, "_close_position", new_callable=mock.AsyncMock) as close, \
             mock.patch.object(strategy, "_hedge_position", new_callable=mock.AsyncMock) as hedge, \
             mock.patch.object(strategy, "_buy_plan", return_value=None), \
             mock.patch.object(strategy, "_sell_plan", return_value={
                 "side": "Up", "shares": 27.0, "limitPrice": 0.95,
                 "riskNotional": 27.0 * 0.95, "fee": 0.0}), \
             mock.patch.object(strategy, "_strategy_cash", new_callable=mock.AsyncMock, return_value=100.0), \
             mock.patch.object(strategy, "record_live_window_diagnostic", return_value={}), \
             mock.patch.object(strategy.sim, "state", {"upBook": book, "downBook": _book(0.45)}):
            await strategy._evaluate_and_act_impl("w1", mock.MagicMock(), 120.0, fair, True)
        return close, hedge

    async def test_momentum_position_is_not_closed_early(self):
        close, hedge = await self._run("open_momentum")
        close.assert_not_awaited()
        hedge.assert_not_awaited()

    async def test_control_an_unguarded_strategy_would_be_closed(self):
        """對照組：沒有專屬分支的部位確實會被提早出場——證明上面那個測試真的有效。"""
        close, _ = await self._run("some_other_strategy")
        close.assert_awaited()


class WsPathHedgeGuardTests(unittest.IsolatedAsyncioTestCase):
    """2026-09-29 回歸：WS 快速路徑也不能補腿。

    第一次修正只擋了 3 秒輪詢路徑，DRY-RUN 實測 20:31:42 進場、20:32:07 就被 WS 路徑的
    補腿接走（「第二腿 Down limit=$0.400」）。WS 路徑有自己的出場分派器，當時沒有
    open_momentum 分支。
    """

    def _pos(self, strategy_name):
        return {"side": "Up", "shares": 28.0, "windowSlug": "w1", "hedged": False,
                "dryRun": True, "entryPrice": 0.55, "entryLimitPrice": 0.55,
                "strategy": strategy_name}

    async def _run_ws_hedge(self, strategy_name):
        lock = __import__("asyncio").Lock()
        with mock.patch.object(strategy, "live_state", {"position": self._pos(strategy_name)}), \
             mock.patch.object(strategy, "_hedge_position", new_callable=mock.AsyncMock) as hedge:
            await strategy._run_ws_hedge({"side": "Down", "shares": 28.0, "limitPrice": 0.40},
                                         True, "w1", lock)
        return hedge

    async def test_momentum_is_never_hedged_by_the_ws_path(self):
        hedge = await self._run_ws_hedge("open_momentum")
        hedge.assert_not_awaited()

    async def test_every_hold_to_settlement_strategy_is_protected(self):
        for name in strategy.HOLD_TO_SETTLEMENT_STRATEGIES:
            with self.subTest(strategy=name):
                hedge = await self._run_ws_hedge(name)
                hedge.assert_not_awaited()

    async def test_control_a_pair_strategy_is_still_hedged(self):
        """對照組：兩腿鎖利策略仍然要能補腿——證明守衛沒有擋錯。"""
        hedge = await self._run_ws_hedge("direct_pair")
        hedge.assert_awaited()


class WsDispatcherWiringTests(unittest.TestCase):
    def test_ws_dispatcher_has_a_momentum_branch(self):
        src = inspect.getsource(strategy._on_ws_tick_sync_impl)
        self.assertIn('pos.get("strategy") == "open_momentum"', src)

    def test_poll_dispatcher_also_has_one(self):
        src = inspect.getsource(strategy._evaluate_and_act_impl)
        self.assertIn('pos.get("strategy") == "open_momentum"', src)


class MomentumSimParityTests(unittest.TestCase):
    """2026-09-29「實盤務必與模擬盤保持一致」稽核後補的測試。

    模擬盤的 openMomentum 分支出場是：
        if not _try_late_favorite_take_profit(...): _try_late_favorite_stop_loss(...)
    讀的是 favoriteTakeProfitPrice / favoriteStopLossPrice / favoriteStopLossUsd，
    而且在呼叫時才讀（體檢調參每 5 秒熱載入覆寫）。
    """

    def _pos(self):
        return {"side": "Up", "shares": 28.0, "windowSlug": "w1", "hedged": False,
                "dryRun": True, "entryPrice": 0.55, "entryLimitPrice": 0.55,
                "strategy": "open_momentum"}

    def test_no_stop_configured_means_hold(self):
        with mock.patch.object(strategy, "_LIVE_VARIANT", {}):
            tp, sp = strategy._momentum_exit_plans(self._pos(), _book(0.55), _book(0.45))
        self.assertIsNone(tp)
        self.assertIsNone(sp)

    def test_usd_stop_from_the_variant_is_honoured(self):
        """體檢調參寫的是 favoriteStopLossUsd（sol／btc-15m 已經被設過 $2.53／$7.31）。"""
        variant = {"favoriteStopLossUsd": 1.0}
        pos = self._pos()
        # best bid 0.20 → 帳面虧損遠大於 $1
        with mock.patch.object(strategy, "_LIVE_VARIANT", variant), \
             mock.patch.object(strategy, "_live_direction_book_is_fresh", return_value=True), \
             mock.patch.object(strategy, "_sell_plan", return_value={
                 "side": "Up", "shares": 28.0, "limitPrice": 0.20, "riskNotional": 5.6, "fee": 0.0}), \
             mock.patch.object(strategy, "_aggressive_sell_plan", return_value=None):
            tp, sp = strategy._momentum_exit_plans(pos, _book(0.55, bid=0.20), _book(0.45))
        self.assertIsNone(tp)
        self.assertIsNotNone(sp)

    def test_variant_changes_take_effect_without_restart(self):
        """同一個 dict 物件被就地改寫（apply_variant_overrides 的做法）後，下一次呼叫就要看到。"""
        variant = {}
        pos = self._pos()
        with mock.patch.object(strategy, "_LIVE_VARIANT", variant), \
             mock.patch.object(strategy, "_live_direction_book_is_fresh", return_value=True), \
             mock.patch.object(strategy, "_sell_plan", return_value={
                 "side": "Up", "shares": 28.0, "limitPrice": 0.20, "riskNotional": 5.6, "fee": 0.0}), \
             mock.patch.object(strategy, "_aggressive_sell_plan", return_value=None):
            self.assertIsNone(strategy._momentum_exit_plans(pos, _book(0.55, bid=0.20), _book(0.45))[1])
            variant["favoriteStopLossUsd"] = 1.0          # 體檢調參就地改寫
            self.assertIsNotNone(strategy._momentum_exit_plans(pos, _book(0.55, bid=0.20), _book(0.45))[1])

    def test_take_profit_wins_over_stop(self):
        """模擬盤是「先停利，沒觸發才看停損」，順序不能反。"""
        variant = {"favoriteTakeProfitPrice": 0.90, "favoriteStopLossUsd": 1.0}
        with mock.patch.object(strategy, "_LIVE_VARIANT", variant), \
             mock.patch.object(strategy, "_live_direction_book_is_fresh", return_value=True), \
             mock.patch.object(strategy, "_sell_plan", return_value={
                 "side": "Up", "shares": 28.0, "limitPrice": 0.95, "riskNotional": 26.6, "fee": 0.0}):
            tp, sp = strategy._momentum_exit_plans(self._pos(), _book(0.96, bid=0.95), _book(0.45))
        self.assertIsNotNone(tp)
        self.assertIsNone(sp)


class DryRunCompoundingTests(unittest.TestCase):
    """DRY-RUN 要跟模擬盤一樣複利（看板也寫著「複利」）。
    實測 24 筆累計 +13.36，下注卻一直停在 $15 附近 —— 原本固定回傳 DRY_RUN_BALANCE_USD。"""

    def test_cash_grows_with_realized_pnl(self):
        state = {"trades": [
            {"dryRun": True, "exitTime": 1.0, "pnlEstimate": 10.0},
            {"dryRun": True, "exitTime": 2.0, "pnlEstimate": -3.0},
        ], "position": None, "pendingSettlements": []}
        with mock.patch.object(strategy, "live_state", state):
            self.assertAlmostEqual(strategy._dry_run_cash(), strategy.DRY_RUN_BALANCE_USD + 7.0)

    def test_real_trades_do_not_affect_dry_run_cash(self):
        state = {"trades": [{"dryRun": False, "exitTime": 1.0, "pnlEstimate": 99.0}],
                 "position": None, "pendingSettlements": []}
        with mock.patch.object(strategy, "live_state", state):
            self.assertAlmostEqual(strategy._dry_run_cash(), strategy.DRY_RUN_BALANCE_USD)

    def test_open_position_cost_is_subtracted(self):
        state = {"trades": [], "position": None, "pendingSettlements": []}
        with mock.patch.object(strategy, "live_state", state), \
             mock.patch.object(strategy, "_position_paid_cost", return_value=20.0):
            state["position"] = {"dryRun": True}
            self.assertAlmostEqual(strategy._dry_run_cash(), strategy.DRY_RUN_BALANCE_USD - 20.0)

    def test_never_negative(self):
        state = {"trades": [{"dryRun": True, "exitTime": 1.0, "pnlEstimate": -9999.0}],
                 "position": None, "pendingSettlements": []}
        with mock.patch.object(strategy, "live_state", state):
            self.assertEqual(strategy._dry_run_cash(), 0.0)
