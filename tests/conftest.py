import os

# 測試需要完整的變體清單（btc-main 等用來驗證共用的鎖利邏輯）；正式運行時預設停用的組別在這裡全部啟用。
os.environ.setdefault("POLY_SIM_DISABLED_VARIANTS", "")

# 2026-09-28：測試不吃專案 .env 的執行時設定（.env 現在會被載入，POLY_SIM_ASSETS 等會讓測試環境漂移）。
# 2026-09-29：加入 doge —— 實盤①的「中段動能方向性」是 DOGE 那一組，
# polymarket_live_strategy 在 import 時就會 sim.markets_state[LIVE_ASSET_ID]，清單裡沒有會 KeyError。
os.environ["POLY_SIM_ASSETS"] = os.environ.get("POLY_SIM_ASSETS_TEST", "btc,btc-15m,eth,doge")
os.environ.pop("POLY_SIM_AUTO_VARIANTS_FILE", None)
os.environ["POLY_SIM_AUTO_VARIANTS_FILE"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "no_such_auto_variants.json")
os.environ["POLY_SIM_VARIANT_OVERRIDES_FILE"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "no_such_overrides.json")
