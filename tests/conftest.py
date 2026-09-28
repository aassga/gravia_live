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

# 2026-09-29：實盤注碼的兩個上限也要隔離。load_dotenv() 是 override=False，所以這裡先設定就會
# 勝過 .env；否則操作者改 .env（例如依要求移除單注上限／現金保留）會讓 14 個實盤測試與 3 個
# 「模擬盤鏡像實盤注碼」測試一起紅掉——那些測試驗的是換算邏輯，不該綁在機器的執行時設定上。
# 這裡固定成程式預設值。
os.environ["POLY_MAX_PAIR_BUDGET_USD"] = os.environ.get("POLY_MAX_PAIR_BUDGET_USD_TEST", "25.0")
os.environ["POLY_MIN_CASH_RESERVE_USD"] = os.environ.get("POLY_MIN_CASH_RESERVE_USD_TEST", "5.0")

# 2026-09-29：實盤鎖定的資產／變體也要隔離。「*-live-lock」這組模擬變體只會為 POLY_LIVE_ASSET_ID
# 那個資產生成，所以操作者把實盤①換成 DOGE 之後，測試裡的 btc-live-lock 就不存在了（KeyError），
# 連帶 14 個實盤測試也跟著換策略而失敗。測試固定用 btc ＋ 歷史混合策略，與實際跑哪一組無關。
os.environ["POLY_LIVE_ASSET_ID"] = os.environ.get("POLY_LIVE_ASSET_ID_TEST", "btc")
os.environ["POLY_LIVE_VARIANT_ID"] = os.environ.get("POLY_LIVE_VARIANT_ID_TEST", "btc-historical-hybrid")
