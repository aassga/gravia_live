"""
模擬盤資料庫清理（2026-09-28 依使用者要求，每天跑一次）。

`sim_quotes` 每秒寫入，資產越多長越快（8 個資產約每天 400～600 MB），磁碟滿會讓模擬盤直接崩掉。
這支只刪「保留天數以前」的報價與窗口診斷，成交紀錄（sim_trades）與變體狀態永遠保留：

    - sim_quotes：只有體檢回放出場設定時會用到進場後的路徑，保留 KEEP_DAYS 天就夠。
    - sim_window_diagnostics：只有「為什麼沒進場」的分析會用，同樣保留 KEEP_DAYS 天。
    - 刪完若回收空間夠多才 VACUUM（重寫整個檔案，需要等量暫存空間，磁碟快滿時反而危險）。

用法：python polymarket_sim_prune.py [--keep-days 7] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import time

log = logging.getLogger("sim-prune")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("POLY_SIM_DB_PATH", os.path.join(HERE, "polymarket_sim.sqlite3"))
KEEP_DAYS = float(os.environ.get("POLY_SIM_PRUNE_KEEP_DAYS", "7"))
VACUUM_MIN_FREE_MB = 400      # 預估可回收超過這個量才值得 VACUUM
VACUUM_MIN_DISK_HEADROOM = 3  # 剩餘空間要有資料庫的幾倍才敢 VACUUM（它需要等量暫存）


def db_size_mb(path: str) -> float:
    total = 0
    for suffix in ("", "-wal", "-shm"):
        try:
            total += os.path.getsize(path + suffix)
        except OSError:
            pass
    return total / 1048576


def prune(db_path: str = DB_PATH, keep_days: float = KEEP_DAYS, dry_run: bool = False) -> dict:
    cutoff = time.time() - keep_days * 86400
    before = db_size_mb(db_path)
    db = sqlite3.connect(db_path, timeout=60)
    try:
        db.execute("PRAGMA busy_timeout=60000")
        old_quotes = db.execute("SELECT COUNT(*) FROM sim_quotes WHERE ts < ?", (cutoff,)).fetchone()[0]
        total_quotes = db.execute("SELECT COUNT(*) FROM sim_quotes").fetchone()[0]
        try:
            old_diag = db.execute(
                "SELECT COUNT(*) FROM sim_window_diagnostics WHERE COALESCE(last_seen, 0) < ?", (cutoff,)).fetchone()[0]
        except sqlite3.Error:
            old_diag = 0
        result = {"keepDays": keep_days, "quotesTotal": total_quotes, "quotesOld": old_quotes,
                  "diagnosticsOld": old_diag, "sizeBeforeMb": round(before, 1), "vacuumed": False}
        if dry_run:
            log.info(f"dry-run：報價 {old_quotes:,}/{total_quotes:,} 筆、診斷 {old_diag:,} 筆超過 {keep_days:g} 天，"
                     f"資料庫 {before:.0f} MB")
            return result
        if old_quotes:
            db.execute("DELETE FROM sim_quotes WHERE ts < ?", (cutoff,))
        if old_diag:
            db.execute("DELETE FROM sim_window_diagnostics WHERE COALESCE(last_seen, 0) < ?", (cutoff,))
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        freed_est = before - db_size_mb(db_path)
        # VACUUM 需要跟資料庫等量的暫存空間，磁碟不夠寬裕就跳過（空間會留給之後的寫入重用）
        free_mb = shutil.disk_usage(os.path.dirname(db_path) or ".").free / 1048576
        cur = db_size_mb(db_path)
        if old_quotes and free_mb > cur * VACUUM_MIN_DISK_HEADROOM and (before - cur) < VACUUM_MIN_FREE_MB:
            log.info("VACUUM 重整檔案中…")
            db.execute("VACUUM")
            result["vacuumed"] = True
        result["sizeAfterMb"] = round(db_size_mb(db_path), 1)
        result["freedMb"] = round(before - db_size_mb(db_path), 1)
        log.info(f"已刪除報價 {old_quotes:,} 筆、診斷 {old_diag:,} 筆（保留 {keep_days:g} 天）；"
                 f"資料庫 {before:.0f} → {result['sizeAfterMb']:.0f} MB"
                 f"{'（含 VACUUM）' if result['vacuumed'] else ''}")
        return result
    finally:
        db.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-days", type=float, default=KEEP_DAYS)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    prune(DB_PATH, args.keep_days, args.dry_run)


if __name__ == "__main__":
    main()
