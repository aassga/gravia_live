import os

# 測試需要完整的變體清單（btc-main 等用來驗證共用的鎖利邏輯）；正式運行時預設停用的組別在這裡全部啟用。
os.environ.setdefault("POLY_SIM_DISABLED_VARIANTS", "")
