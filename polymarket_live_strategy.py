"""
Polymarket BTC Up/Down · 真實自動下單策略
─────────────────────────────────────
判斷邏輯跟 polymarket_server.py（紙上模擬）完全一致，而且是直接 import 過來共用，
不是另外複製一份常數——這樣模擬版調整門檻時，真實版自動跟著同步，不會有兩邊邏輯
慢慢長歪、之後改一邊忘記改另一邊的問題：
    - 有一邊價格 <= SIM_ENTRY_MAX_PRICE 時，先買那一邊
    - 等兩邊合計成本 <= SIM_LOCK_MAX_SUM 時，買另一邊配對鎖利
    - 鎖不到就抱到期，等結算
差別只在於：這支程式會真的送出真實訂單、真的動用真實資金。

跟模擬版的重要差異（誠實列出，不是同一套東西）：
    1. 模擬版用「中價」(midpoint) 算損益，這裡真實下單用「真實訂單簿的最佳賣價」，
       因為要保證真的買得到——實際成本一定比模擬版看到的中價貴一點點，這是真實市場的摩擦成本。
    2. 下單一律用 FOK（全成交或取消），不會有部分成交卡著的殘留掛單。
    3. 【尚未驗證】贏的那一邊，Polymarket 是否會自動把 conditional token 兌換回 USDC，
       還是需要額外呼叫一次 redeem 才會真的入帳，這裡沒有十足把握，程式只會「估算」損益，
       實際錢有沒有真的到帳，請以 web/polymarket_live.html 顯示的真實餘額為準，不要只信這裡的估算。

安全機制：
    - 完全遵守 polymarket_live_trader.py 的 LIVE_TRADING 開關：.env 裡是 false 時，
      整個策略迴圈都是 dry-run（只記錄「本來會下什麼單」，不會真的呼叫下單 API），
      可以安全跑過一整輪邏輯，不花一毛錢。
    - 每次進場/鎖利前都重新查一次真實餘額，下注金額 = 真實資產組合 × 設定的百分比（跟模擬版同一套複利邏輯）。
    - 同一時間只會有一個真實部位，不會疊加下注。

啟動方式：
    py polymarket_live_strategy.py

真實損益請開 web/polymarket_live.html 查看（由 polymarket_live_status_server.py 提供，
會自動反映這支程式產生的真實掛單／成交紀錄）。
"""

import asyncio
import logging
import os
import sys
import time
from decimal import Decimal, ROUND_DOWN

import aiohttp

import polymarket_server as sim   # 共用市場資料抓取邏輯 + 策略門檻常數，保證跟模擬版一致
import polymarket_live_trader as live

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket_live_strategy")

POLL_INTERVAL = 3

STAKE_PCT     = float(os.environ.get("POLY_STAKE_PCT", "3.0"))
MIN_STAKE_PCT = 0.5
MAX_STAKE_PCT = 25.0
STAKE_PCT     = max(MIN_STAKE_PCT, min(MAX_STAKE_PCT, STAKE_PCT))

live_state = {
    "position":           None,  # {windowSlug, side, entryPrice, shares, hedged, hedgeSide, hedgePrice, hedgeShares, dryRun}
    "pendingSettlements": [],
    "totalPnlEstimate":   0.0,   # 估算值，實際到帳金額請看真實損益頁面的餘額
    "totalTrades":        0,
}


def best_ask(book: dict) -> float | None:
    asks = book.get("asks") or []
    if not asks:
        return None
    return min(a["price"] for a in asks)


def safe_order_size(stake_usd: float, price: float) -> float:
    """Polymarket 要求：市價買單的金額(maker amount)最多 2 位小數、股數(taker amount)最多 4 位小數。

    一開始只把股數捨到 4 位小數還是不夠：就算股數本身乾淨（例如 5.2333），
    股數 × 價格 算出來的金額還是常常會超過 2 位小數（5.2333 × 0.30 = 1.56999），一樣會被拒絕。
    數學上唯一保證「股數 × 任意 2 位小數價格」一定落在乾淨分（cent）上的做法，是把股數捨去到整數股——
    整數 × 2 位小數 必然還是 2 位小數，不會再超標。代價是犧牲一點資金效率（下注金額會比目標略低，
    捨去而不是四捨五入，確保永遠不會超過預算），但在這種小額下注的規模下，差異可忽略。"""
    price_dec = Decimal(str(price))
    cost_dec = Decimal(str(round(stake_usd, 2)))
    shares_dec = (cost_dec / price_dec).to_integral_value(rounding=ROUND_DOWN)
    return float(shares_dec)


def get_real_portfolio() -> float:
    """下注金額的基準：真實現金餘額。刻意不把「目前未平倉部位的估值」算進來，
    避免虛報還沒真的到手的錢，寧可算保守一點。"""
    balance = live.get_usdc_balance()
    return int(balance.get("balance", 0)) / 1_000_000


async def enter_position_live(slug: str, side: str, book: dict) -> None:
    price = best_ask(book)
    if price is None or price <= 0:
        log.warning(f"[LIVE] {side} 目前沒有賣單可以吃，跳過這次進場")
        return

    portfolio = get_real_portfolio()
    stake_usd = portfolio * (STAKE_PCT / 100)
    if stake_usd < 1.0:
        log.warning(f"[LIVE] 資產組合 ${portfolio:.2f} 太小，預估下注 ${stake_usd:.2f} 低於 $1，跳過這次進場")
        return

    shares = safe_order_size(stake_usd, price)
    if shares * price < 1.0:
        # 股數捨去到整數後，實際金額可能又跌回 $1 門檻以下（例如目標 $1.05、價格 $0.6 → 只能買 1 股 = $0.6）
        log.info(f"[LIVE] {side} @ ${price:.3f} 捨去到整數股後只剩 ${shares*price:.2f}，低於 $1 最低下單金額，跳過")
        return

    up_id, down_id = sim._market_tokens(sim.state["market"])
    token_id = up_id if side == "Up" else down_id

    resp = live.place_limit_order(token_id, "BUY", price, shares, order_type="FOK")
    filled = bool(resp.get("dry_run")) or resp.get("success", True)

    if resp.get("dry_run"):
        log.info(f"[LIVE][DRY-RUN] 模擬進場 {side} @ ${price:.3f} 股數={shares:.2f}（${stake_usd:.2f}）")
    elif not filled:
        log.error(f"[LIVE] 進場下單失敗，不建立部位：{resp}")
        return
    else:
        log.warning(f"[LIVE] ★ 真實進場 {side} @ ${price:.3f} 股數={shares:.2f}（${stake_usd:.2f}）")

    live_state["position"] = {
        "windowSlug":  slug,
        "side":        side,
        "entryPrice":  price,
        "shares":      shares,
        "stakeUsd":    stake_usd,
        "entryTime":   time.time(),
        "hedged":      False,
        "hedgeSide":   None,
        "hedgePrice":  None,
        "hedgeShares": 0.0,
        "dryRun":      bool(resp.get("dry_run")),
    }


async def hedge_position_live(other_side: str, book: dict) -> None:
    pos = live_state["position"]
    price = best_ask(book)
    if price is None or price <= 0:
        return

    shares = pos["shares"]  # 配對相同股數才能鎖住保證利潤

    if shares * price < 1.0:
        # Polymarket 對 marketable BUY 訂單有 $1 最低金額限制。另一邊價格如果已經跌到
        # 用原本股數乘下去不到 $1（通常代表這邊快輸了、我方那邊快贏了），送單一定會被拒絕，
        # 送了也白送——直接放棄這次鎖利，抱著等結算就好，不用一直重試到窗口結束。
        log.info(f"[LIVE] {other_side} @ ${price:.3f} × {shares:.2f} 股 = ${shares*price:.2f}，"
                 f"低於 $1 最低下單金額，放棄鎖利，抱到結算")
        return

    up_id, down_id = sim._market_tokens(sim.state["market"])
    token_id = up_id if other_side == "Up" else down_id

    resp = live.place_limit_order(token_id, "BUY", price, shares, order_type="FOK")
    filled = bool(resp.get("dry_run")) or resp.get("success", True)

    if resp.get("dry_run"):
        log.info(f"[LIVE][DRY-RUN] 模擬鎖利 {other_side} @ ${price:.3f}")
    elif not filled:
        log.error(f"[LIVE] 鎖利下單失敗，這個部位會維持方向性曝險，沒有鎖到：{resp}")
        return
    else:
        locked = shares * (1 - pos["entryPrice"] - price)
        log.warning(f"[LIVE] ★ 真實鎖利 {other_side} @ ${price:.3f}　估算鎖定利潤=${locked:+.2f}")

    pos["hedged"]      = True
    pos["hedgeSide"]   = other_side
    pos["hedgePrice"]  = price
    pos["hedgeShares"] = shares


def _settle_pnl_estimate(pos: dict, outcome: str) -> float:
    """跟模擬版 _settle_pnl() 同一套算法，僅供參考——實際到帳請以真實餘額為準。"""
    if pos["hedged"]:
        return pos["shares"] * (1 - pos["entryPrice"] - pos["hedgePrice"])
    won = (outcome == pos["side"])
    return pos["shares"] * (1 - pos["entryPrice"]) if won else -(pos["shares"] * pos["entryPrice"])


async def retry_pending_settlements(session: aiohttp.ClientSession) -> None:
    if not live_state["pendingSettlements"]:
        return
    still_pending = []
    for pos in live_state["pendingSettlements"]:
        outcome = await sim.fetch_outcome(session, pos["windowSlug"])
        if outcome is None:
            still_pending.append(pos)
            continue
        pnl = _settle_pnl_estimate(pos, outcome)
        live_state["totalPnlEstimate"] += pnl
        live_state["totalTrades"] += 1
        tag = "(dry-run，非真實)" if pos.get("dryRun") else "(真實)"
        log.warning(
            f"[LIVE] {tag} 結算 {pos['windowSlug']} 結果={outcome} "
            f"{'(已鎖利)' if pos['hedged'] else '(方向性)'} 估算PnL=${pnl:+.2f}　"
            f"→ 實際到帳金額請看 web/polymarket_live.html 的真實餘額"
        )
    live_state["pendingSettlements"] = still_pending


def queue_settlement(slug: str) -> None:
    pos = live_state["position"]
    if pos is not None and pos["windowSlug"] == slug:
        live_state["pendingSettlements"].append(pos)
    live_state["position"] = None


async def evaluate_and_act(slug: str, session: aiohttp.ClientSession) -> None:
    up_price, down_price = sim.state["upPrice"], sim.state["downPrice"]
    up_book, down_book = sim.state["upBook"], sim.state["downBook"]
    if up_price is None or down_price is None or up_price <= 0 or down_price <= 0:
        return

    pos = live_state["position"]

    if pos is None:
        if up_price <= sim.SIM_ENTRY_MAX_PRICE:
            await enter_position_live(slug, "Up", up_book)
        elif down_price <= sim.SIM_ENTRY_MAX_PRICE:
            await enter_position_live(slug, "Down", down_book)
        return

    if pos["hedged"] or pos["windowSlug"] != slug:
        return

    other_side  = "Down" if pos["side"] == "Up" else "Up"
    other_price = down_price if other_side == "Down" else up_price
    other_book  = down_book if other_side == "Down" else up_book
    if pos["entryPrice"] + other_price <= sim.SIM_LOCK_MAX_SUM:
        await hedge_position_live(other_side, other_book)


async def strategy_loop():
    log.info("=" * 60)
    log.info("  Polymarket BTC Up/Down · 真實自動下單策略")
    log.info(f"  LIVE_TRADING = {live.LIVE_TRADING}（false = 全程 dry-run，不會花錢）")
    log.info(f"  下注比例 = {STAKE_PCT:.1f}%（複利，跟真實資產組合連動）")
    log.info(f"  進場門檻 <= ${sim.SIM_ENTRY_MAX_PRICE}　鎖利門檻合計 <= ${sim.SIM_LOCK_MAX_SUM}")
    log.info("  真實損益請開 web/polymarket_live.html 查看")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                cur = sim.state["market"]
                new_market = await sim.fetch_active_market(session, "btc-updown-5m-")

                if new_market and (cur is None or new_market["slug"] != cur["slug"]):
                    if cur is not None:
                        queue_settlement(cur["slug"])
                    sim.state["market"] = new_market
                    log.info(f"[MARKET] 切換到新窗口 {new_market['slug']}")

                if sim.state["market"]:
                    up_id, down_id = sim._market_tokens(sim.state["market"])
                    if up_id and down_id:
                        up_price, down_price, up_book, down_book = await asyncio.gather(
                            sim.fetch_midpoint(session, up_id),
                            sim.fetch_midpoint(session, down_id),
                            sim.fetch_book(session, up_id),
                            sim.fetch_book(session, down_id),
                            return_exceptions=True,
                        )
                        if not isinstance(up_price, Exception):   sim.state["upPrice"] = up_price
                        if not isinstance(down_price, Exception): sim.state["downPrice"] = down_price
                        if not isinstance(up_book, Exception):    sim.state["upBook"] = up_book
                        if not isinstance(down_book, Exception):  sim.state["downBook"] = down_book

                        await evaluate_and_act(sim.state["market"]["slug"], session)

                await retry_pending_settlements(session)

            except Exception as e:
                log.error(f"策略迴圈錯誤：{e}")

            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(strategy_loop())
