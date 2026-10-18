"""
每天跑一次實盤優勢判準，只在結論有變化時推播 TG（gravia-edge-watch.timer）。

2026-10-18 依使用者要求自動化。判準本身在 polymarket_live_edge_check.py，門檻寫死在那裡；
這支只負責「什麼時候該吵你」：

  推播：樣本剛達到 MIN_TRADES（判準開始生效）、或結論改變（observe → keep/switch 等）
  不推：結論沒變（絕大多數日子都是 observe → observe，保持安靜）
  換變體：樣本歸零重新累積，靜默重設基準；只有新變體一開始就有結論才推

狀態記在 live_edge_watch_state.json（不進版控），所以同一個結論不會天天吵。

用法：
    python polymarket_live_edge_watch.py                 # 照常：有變化才推
    python polymarket_live_edge_watch.py --force         # 不管有沒有變化都推（手動查看用）
    python polymarket_live_edge_watch.py --no-telegram   # 只印不推
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time

import httpx

import polymarket_live_edge_check as ec

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("edge-watch")

_HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.environ.get("POLY_EDGE_WATCH_STATE_FILE") or os.path.join(_HERE, "live_edge_watch_state.json")
LIVE_STATE_FILE = os.environ.get("POLY_LIVE_STATE_FILE") or os.path.join(_HERE, "polymarket_live_strategy_state.json")


def load_watch_state(path: str = STATE_FILE) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        log.warning(f"讀不到 {path}（{exc}），當成沒有歷史")
        return {}


def save_watch_state(data: dict, path: str = STATE_FILE) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def decide_notify(prev: dict, variant_id: str, code: str, matured: bool) -> tuple[bool, str]:
    """回傳 (要不要推, 推播的理由標題)。"""
    if prev.get("variantId") != variant_id:
        # 換了策略：樣本重新累積，不要用舊策略的結論當比較基準
        if matured:
            return True, "換策略後樣本已足，判準有結論"
        return False, "換策略，靜默重設基準"
    if matured and not prev.get("matured"):
        return True, f"樣本達到 {ec.MIN_TRADES} 筆，判準開始生效"
    if code != prev.get("verdict"):
        return True, f"結論改變：{prev.get('verdict')} → {code}"
    return False, "結論沒變"


def send_telegram(text: str) -> None:
    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    ids = [int(v) for v in os.environ.get("TG_ALLOWED_USER_IDS", "").replace(" ", "").split(",") if v.isdigit()]
    if not token or not ids:
        log.warning("TG 未設定，略過推播")
        return
    with httpx.Client(timeout=20) as c:
        for uid in ids:
            try:
                c.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": uid, "text": text[:4000]})
            except Exception as exc:
                log.warning(f"sendMessage failed: {exc}")


def run(force: bool = False, no_telegram: bool = False,
        live_state_file: str | None = None, watch_state_file: str | None = None) -> int:
    # 路徑在呼叫時才解析（不是用預設參數綁住模組變數），否則測試換不掉、會去動真實狀態檔。
    live_path = live_state_file or LIVE_STATE_FILE
    watch_path = watch_state_file or STATE_FILE
    variant_id = os.environ.get("POLY_LIVE_VARIANT_ID", "").strip()
    if not variant_id:
        log.error("POLY_LIVE_VARIANT_ID 沒設，不知道要看哪一組")
        return 2
    with open(live_path, "r", encoding="utf-8") as f:
        live_state = json.load(f)

    stats = ec.collect(live_state, variant_id)
    code, _ = ec.verdict(stats)
    matured = stats["sized"] >= ec.MIN_TRADES
    report = ec.format_report(stats)
    log.info("判準結論：%s（樣本 %d 筆）", code, stats["sized"])
    print(report)

    prev = load_watch_state(watch_path)
    notify, why = decide_notify(prev, variant_id, code, matured)
    if force:
        notify, why = True, "手動執行（--force）"

    if notify and not no_telegram:
        send_telegram(f"{report}\n\n（推播原因：{why}）")
        log.info("已推播：%s", why)
    elif notify:
        log.info("有變化但 --no-telegram，沒推：%s", why)
    else:
        log.info("安靜：%s", why)

    save_watch_state({
        "variantId": variant_id,
        "verdict": code,
        "matured": matured,
        "n": stats["sized"],
        "grossPerStake": stats["grossPerStake"],
        "checkedAt": time.time(),
        "lastNotifiedAt": time.time() if notify else prev.get("lastNotifiedAt"),
        "lastNotifyReason": why if notify else prev.get("lastNotifyReason"),
    }, watch_path)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="不管有沒有變化都推播")
    ap.add_argument("--no-telegram", action="store_true", help="只印不推")
    args = ap.parse_args()
    return run(force=args.force, no_telegram=args.no_telegram)


if __name__ == "__main__":
    raise SystemExit(main())
