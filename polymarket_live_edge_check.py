"""
實盤策略的優勢判準（純讀取；python polymarket_live_edge_check.py [狀態檔] [變體id]）。

2026-10-17 依使用者要求：把「這組策略還值不值得跑」的判準**先寫死在這裡**，而不是事後看
數字再找理由。之前換策略的決定都是事後論證，而 783 組對照已經證明「挑最近最賺的」比隨機
還差（r = -0.189），所以這裡刻意不看近期損益排名，只看兩件結構性的事：

  (1) 毛每注報酬（pnl + 手續費）/ 下注額。手續費是已知的固定拖累且 ∝ 進場價×(1−進場價)，
      真正的問題是「扣費前到底有沒有優勢」。判準與健檢一致：毛每注減掉 GROSS_SE_MARGIN 個
      標準誤仍為正，才算優勢是真的；加上一個標準誤仍為負，才算優勢確實不見了。中間地帶
      一律「繼續觀察」，不動。
  (2) 勝率 vs 打平勝率。二元市場的打平勝率約等於進場價（買 0.93 要贏 93% 才打平），
      這個落差是機制性的量，不是運氣。只當輔助證據，不單獨決策。

樣本不足 MIN_TRADES 筆之前不給結論——這是判準最重要的部分：避免 40 筆就下定論。

資料來源：實盤狀態檔的 trades（含 DRY-RUN），只取目前 variantId 的那些。
"""
from __future__ import annotations

import json
import math
import os
import sys

# ── 判準（2026-10-17 在資料產生前固定，之後要改必須明確說明理由並記進 commit）──────────
MIN_TRADES = 150          # 少於這麼多筆一律「繼續觀察」，不下結論
GROSS_SE_MARGIN = 1.0     # 與 polymarket_sim_doctor.GROSS_SE_MARGIN 同一個標準
BREAKEVEN_MARGIN_PP = 1.0 # 勝率低於打平勝率這麼多百分點，算輔助的負面證據

DEFAULT_STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket_live_strategy_state.json")


def _mean_se(values: list[float]) -> tuple[float, float]:
    """回傳 (平均, 標準誤)；不足 2 筆時標準誤為 0。"""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(var) / math.sqrt(n)


def collect(state: dict, variant_id: str | None) -> dict:
    """把狀態檔的成交整理成判準需要的統計量。variant_id=None 表示不過濾。"""
    trades = [t for t in (state.get("trades") or [])
              if variant_id is None or t.get("variantId") == variant_id]
    gross_rets: list[float] = []
    net_rets: list[float] = []
    entry_prices: list[float] = []
    wins = fees = net = gross = 0.0
    stops = 0
    for t in trades:
        pnl = float(t.get("pnlEstimate") or 0.0)
        fee = float(t.get("feesEstimate") or 0.0)
        stake = float(t.get("stakeUsd") or 0.0)
        net += pnl
        fees += fee
        gross += pnl + fee
        if pnl > 0:
            wins += 1
        if t.get("tradeType") == "early_exit":
            stops += 1
        if stake > 0:
            gross_rets.append((pnl + fee) / stake)
            net_rets.append(pnl / stake)
        if t.get("entryPrice"):
            entry_prices.append(float(t["entryPrice"]))
    g_mean, g_se = _mean_se(gross_rets)
    n_mean, _ = _mean_se(net_rets)
    breakeven = (sum(entry_prices) / len(entry_prices)) if entry_prices else 0.0
    return {
        "variantId": variant_id,
        "n": len(trades),
        "sized": len(gross_rets),
        "winRate": (wins / len(trades)) if trades else 0.0,
        "breakeven": breakeven,
        "grossTotal": gross,
        "netTotal": net,
        "feeTotal": fees,
        "grossPerStake": g_mean,
        "grossSe": g_se,
        "netPerStake": n_mean,
        "stops": stops,
    }


def verdict(stats: dict) -> tuple[str, str]:
    """回傳 (結論代碼, 說明)。結論只有三種：keep / switch / observe。"""
    n = stats["sized"]
    if n < MIN_TRADES:
        return "observe", (
            f"樣本 {n} 筆 < {MIN_TRADES} 筆，判準規定不下結論。"
            f"目前毛每注 {stats['grossPerStake']:+.4f}（僅供參考，不是依據）"
        )
    g, se = stats["grossPerStake"], stats["grossSe"]
    lo, hi = g - GROSS_SE_MARGIN * se, g + GROSS_SE_MARGIN * se
    if lo > 0:
        return "keep", (
            f"毛每注 {g:+.4f} − {GROSS_SE_MARGIN:g}×SE({se:.4f}) = {lo:+.4f} > 0"
            "：扣費前的優勢是真的，繼續跑"
        )
    if hi < 0:
        return "switch", (
            f"毛每注 {g:+.4f} + {GROSS_SE_MARGIN:g}×SE({se:.4f}) = {hi:+.4f} < 0"
            "：扣費前就沒有優勢，降費也救不回來，該換掉"
        )
    return "observe", (
        f"毛每注 {g:+.4f} ± {GROSS_SE_MARGIN:g}×SE({se:.4f}) 跨過 0"
        f"（{lo:+.4f} ~ {hi:+.4f}），還分不出有沒有優勢，不要動"
    )


def format_report(stats: dict) -> str:
    code, why = verdict(stats)
    icon = {"keep": "✅ 保留", "switch": "🔁 該換", "observe": "⏳ 繼續觀察"}[code]
    be_pp = (stats["winRate"] - stats["breakeven"]) * 100
    lines = [
        f"📐 實盤優勢判準：{stats['variantId'] or '（全部）'}",
        f"  成交 {stats['n']} 筆（有下注額的 {stats['sized']} 筆）",
        f"  勝率 {stats['winRate']*100:.1f}%　打平勝率 {stats['breakeven']*100:.1f}%"
        f"（≈ 均進場價）　落差 {be_pp:+.1f}pp",
        f"  毛每注 {stats['grossPerStake']:+.4f} ± {stats['grossSe']:.4f}（SE）",
        f"  淨每注 {stats['netPerStake']:+.4f}　手續費共 ${stats['feeTotal']:.2f}",
        f"  合計 毛 ${stats['grossTotal']:+.2f}　淨 ${stats['netTotal']:+.2f}　停損出場 {stats['stops']} 筆",
        "",
        f"  {icon}：{why}",
    ]
    if be_pp < -BREAKEVEN_MARGIN_PP:
        lines.append(
            f"  ⚠ 輔助證據：勝率比打平勝率低 {-be_pp:.1f}pp"
            f"（> {BREAKEVEN_MARGIN_PP:g}pp 門檻），結構上偏不利"
        )
    return "\n".join(lines)


def main() -> int:
    state_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_STATE
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)
    variant_id = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("POLY_LIVE_VARIANT_ID") or None
    print(format_report(collect(state, variant_id)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
