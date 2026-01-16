#!/usr/bin/env python3
"""learning_backfill.py

Purpose:
- Backfill learning_stats + threshold_adjustments for the last N days.

This fixes the earlier error:
  sqlite3.OperationalError: no such column: day_utc
by using the correct column name used in this project: learning_stats.date

Usage:
  python learning_backfill.py --db /path/to/bot_data.db --days 14

Optional:
  --clear   (delete existing learning_stats rows for the target days before backfill)

Notes:
- Safe to run multiple times.
- It does NOT require .env (Zero env).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone, timedelta

from database import DatabaseManager
from learning_engine import run_learning_cycle


def _utc_today() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _day_str(dt: datetime) -> str:
    return dt.date().isoformat()


def _table_cols(conn, table: str):
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [r[1] for r in rows]
    except Exception:
        return []


def _delete_learning_day(conn, day: str) -> None:
    cols = _table_cols(conn, "learning_stats")
    col = "date" if "date" in cols else ("day_utc" if "day_utc" in cols else None)
    if not col:
        return
    conn.execute(f"DELETE FROM learning_stats WHERE {col}=?", (day,))


def _ensure_zero_row(db: DatabaseManager, day: str, strategy: str, exit_profile: str) -> None:
    # Insert only if not exists (PK prevents dupes)
    with db.lock:
        db.conn.execute(
            """
            INSERT OR IGNORE INTO learning_stats
                (date, strategy, exit_profile, trades, wins, losses, pnl, avg_confidence, created_at, created_at_ms)
            VALUES
                (?, ?, ?, 0, 0, 0, 0.0, 0.0, ?, ?)
            """,
            (day, strategy, exit_profile, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), int(datetime.now(timezone.utc).timestamp() * 1000))
        )
        db.conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to bot_data.db")
    ap.add_argument("--days", type=int, default=14, help="How many days back (UTC) to backfill")
    ap.add_argument("--clear", action="store_true", help="Clear existing learning_stats for those days first")
    args = ap.parse_args()

    db = DatabaseManager(args.db)
    db.create_tables()

    # Build day list (inclusive)
    days = max(1, int(args.days))
    start = _utc_today() - timedelta(days=days - 1)
    day_list = [_day_str(start + timedelta(days=i)) for i in range(days)]

    if args.clear:
        try:
            with db.lock:
                for d in day_list:
                    _delete_learning_day(db.conn, d)
                db.conn.commit()
        except Exception as e:
            print(f"[WARN] Could not clear existing rows: {e}", file=sys.stderr)

    # Run learning aggregation + adjustment computation (writes real stats from CLOSED trades)
    res = run_learning_cycle(db, window_days=days, write_empty_today=False)

    # Ensure the dashboard always has visible rows even if no closed trades
    # We only create missing rows (INSERT OR IGNORE), so we don't overwrite real stats.
    try:
        profiles = set()
        for p in (db.get_setting("pump_hunter_exit_profile"), db.get_setting("strategy_override_exit_profile")):
            if p:
                profiles.add(str(p).strip())
        if not profiles:
            profiles = {"DEFAULT"}

        for d in day_list:
            for mode in ("scalp", "swing"):
                for ep in profiles:
                    _ensure_zero_row(db, d, mode, ep)
    except Exception as e:
        print(f"[WARN] Could not write placeholder rows: {e}", file=sys.stderr)

    print("\n✅ Learning Backfill DONE")
    print(f"Days: {days} (UTC)")
    print(f"learning_stats_upserts: {res.get('learning_stats_upserts')}")
    print(f"adjustments_written: {res.get('adjustments_written')}")
    print(f"window_days: {res.get('window_days')}")


if __name__ == "__main__":
    main()
