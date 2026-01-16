#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Link ai_samples rows (trade_id is NULL) to trades.

DB schema notes:
- ai_samples.features (TEXT JSON) not features_json
- trades uses signal (not side)
- trades has explain (TEXT JSON) and timestamp_ms

Linking strategy:
1) If ai_samples.inbox_id exists: find trade by symbol + trades.explain containing inbox_id
2) Else: find nearest trade by symbol within a time window around ai_samples.created_at_ms

Updates: ai_samples.trade_id (+ optionally entry_price if empty)
"""
import argparse, sqlite3, os
from _common import get_cols, has_col, safe_int, safe_float

def find_trade_by_inbox(conn, symbol: str, inbox_id: int):
    patterns = [
        f'%"inbox_id": {inbox_id}%',
        f'%"inbox_id":{inbox_id}%',
        f'%inbox_id": {inbox_id}%',
        f'%inbox_id":{inbox_id}%',
    ]
    for pat in patterns:
        row = conn.execute(
            """SELECT id, symbol, signal, entry_price, timestamp_ms, explain
                 FROM trades
                 WHERE symbol=? AND explain LIKE ?
                 ORDER BY id DESC
                 LIMIT 1""",
            (symbol, pat),
        ).fetchone()
        if row:
            return row
    return None

def find_trade_by_time(conn, symbol: str, created_ms: int, window_ms: int):
    lo = created_ms - window_ms
    hi = created_ms + window_ms
    return conn.execute(
        """SELECT id, symbol, signal, entry_price, timestamp_ms, explain
             FROM trades
             WHERE symbol=? AND timestamp_ms BETWEEN ? AND ?
             ORDER BY ABS(timestamp_ms - ?) ASC, id DESC
             LIMIT 1""",
        (symbol, lo, hi, created_ms),
    ).fetchone()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='bot_data.db')
    ap.add_argument('--window-minutes', type=int, default=180)
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"❌ DB not found: {args.db}")
        raise SystemExit(2)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    ai_cols = get_cols(conn, 'ai_samples')
    tr_cols = get_cols(conn, 'trades')

    created_ms_col = 'created_at_ms' if has_col(ai_cols,'created_at_ms') else None
    inbox_col = 'inbox_id' if has_col(ai_cols,'inbox_id') else None
    entry_col = 'entry_price' if has_col(ai_cols,'entry_price') else None

    if not created_ms_col:
        print("❌ ai_samples missing created_at_ms; cannot time-link.")
        print("ai_samples columns:", ai_cols)
        raise SystemExit(2)

    samples = conn.execute(
        f"""SELECT id, symbol, {created_ms_col} AS created_ms
              {', '+inbox_col if inbox_col else '' }
              {', '+entry_col if entry_col else '' }
              FROM ai_samples
              WHERE trade_id IS NULL
              ORDER BY id ASC"""
    ).fetchall()

    if not samples:
        print("✅ No ai_samples rows with trade_id=NULL. Nothing to link.")
        return

    window_ms = args.window_minutes * 60 * 1000

    linked = 0
    for s in samples:
        sid = s['id']
        symbol = s['symbol']
        created_ms = safe_int(s['created_ms'], 0)
        inbox_id = safe_int(s[inbox_col], 0) if inbox_col and s[inbox_col] is not None else None

        trade = None
        if inbox_id:
            trade = find_trade_by_inbox(conn, symbol, inbox_id)
        if trade is None and created_ms > 0:
            trade = find_trade_by_time(conn, symbol, created_ms, window_ms)

        if trade is None:
            print(f"⚠️ Unmatched sample_id={sid} symbol={symbol} inbox_id={inbox_id}")
            continue

        tid = trade['id']
        if args.apply:
            conn.execute("UPDATE ai_samples SET trade_id=? WHERE id=?", (tid, sid))
            if entry_col and (s[entry_col] is None or safe_float(s[entry_col], 0.0) == 0.0):
                conn.execute("UPDATE ai_samples SET entry_price=? WHERE id=?", (safe_float(trade['entry_price'], 0.0), sid))
            conn.commit()

        print(f"🧩 Linked ai_sample id={sid} ({symbol}) -> trade_id={tid}")
        linked += 1

    if args.apply:
        print(f"✅ Linked {linked}/{len(samples)} samples (DB updated)")
    else:
        print(f"DRY RUN: would link {linked}/{len(samples)} samples. Add --apply to update DB.")

if __name__ == '__main__':
    main()
