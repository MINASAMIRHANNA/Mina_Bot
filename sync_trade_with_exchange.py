#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tools/sync_trade_with_exchange.py

If a trade is OPEN in bot_data.db but the exchange position is flat (positionAmt==0),
mark the trade as CLOSED in the DB, set close_price/closed_at/pnl (best-effort),
and cancel any remaining open orders for that symbol.

Usage:
  python tools/sync_trade_with_exchange.py --trade-id 32
  python tools/sync_trade_with_exchange.py --trade-id 32 --dry-run
"""

import os
import sys
import argparse
import sqlite3
import time
from datetime import datetime, timezone

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def utc_now_ms() -> int:
    return int(time.time() * 1000)

def parse_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default

def resolve_db_path() -> str:
    try:
        from config import cfg
        raw = getattr(cfg, "DB_FILE", None) or os.getenv("DB_FILE") or "./bot_data.db"
    except Exception:
        raw = os.getenv("DB_FILE") or "./bot_data.db"
    raw = str(raw)
    if os.path.isabs(raw):
        return raw
    return os.path.abspath(os.path.join(PROJECT_ROOT, raw))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-id", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db_path = resolve_db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)

    from trading_executor import init_client
    c = init_client(force=True)
    if c is None:
        print("[ERR] Binance client not available (paper mode or missing keys).")
        sys.exit(2)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM trades WHERE id=?", (args.trade_id,))
        row = cur.fetchone()
        if not row:
            print(f"[ERR] trade id {args.trade_id} not found in trades table.")
            sys.exit(3)

        t = dict(row)
        sym = (t.get("symbol") or "").strip()
        status = (t.get("status") or "").upper()
        print(f"[DB] id={args.trade_id} symbol={sym} status={status}")

        pos = c.futures_position_information(symbol=sym) or []
        amt = 0.0
        entry_price = 0.0
        if isinstance(pos, list):
            for p in pos:
                pa = parse_float(p.get("positionAmt"), 0.0)
                if abs(pa) > 0:
                    amt = pa
                    entry_price = parse_float(p.get("entryPrice"), 0.0)
                    break
        print(f"[EXCH] positionAmt={amt} entryPrice={entry_price}")

        if abs(amt) > 0:
            print("[INFO] Position still open on exchange; no DB changes.")
            return

        try:
            if not args.dry_run:
                c.futures_cancel_all_open_orders(symbol=sym)
            print("[EXCH] cancel_all_open_orders: OK")
        except Exception as e:
            print(f"[WARN] cancel_all_open_orders failed: {e}")

        close_price = 0.0
        try:
            tick = c.futures_symbol_ticker(symbol=sym)
            close_price = parse_float((tick or {}).get("price"), 0.0)
        except Exception:
            close_price = 0.0

        realized = 0.0
        commissions = 0.0
        start_ms = 0
        try:
            start_ms = int(t.get("timestamp_ms") or 0) - 5 * 60 * 1000
            if start_ms < 0:
                start_ms = 0
        except Exception:
            start_ms = 0

        try:
            fills = c.futures_account_trades(symbol=sym, startTime=start_ms, limit=1000) or []
            for f in fills:
                realized += parse_float(f.get("realizedPnl"), 0.0)
                commissions += parse_float(f.get("commission"), 0.0)
        except Exception:
            pass

        net = realized - commissions
        print(f"[CALC] close_price≈{close_price} realized≈{realized} commission≈{commissions} net≈{net}")

        if status == "CLOSED":
            print("[INFO] DB already CLOSED. Nothing to do.")
            return

        if args.dry_run:
            print("[DRY] Would update DB trade as CLOSED.")
            return

        cur.execute(
            "UPDATE trades SET status='CLOSED', close_price=?, close_reason=?, closed_at=?, closed_at_ms=?, pnl=? WHERE id=?",
            (close_price if close_price > 0 else None, "EXCHANGE_FLAT_SYNC", utc_now_iso(), utc_now_ms(), net, args.trade_id)
        )
        conn.commit()
        print("[OK] DB updated: trade marked CLOSED (EXCHANGE_FLAT_SYNC).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
