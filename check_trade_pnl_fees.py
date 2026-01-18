#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""tools/check_trade_pnl_fees.py

Compute realized PnL, commissions, and funding fees for a trade using Binance Futures history,
and cross-check with bot_data.db.

Examples:
  python tools/check_trade_pnl_fees.py --trade-id 32
  python tools/check_trade_pnl_fees.py --trade-id 32 --window-minutes 1440
  python tools/check_trade_pnl_fees.py --symbol 1000WHYUSDT --window-minutes 240
"""

import os
import sys
import argparse
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def utc_now_ms() -> int:
    return int(time.time() * 1000)

def parse_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None

def detect_trade_table(conn: sqlite3.Connection) -> str:
    if table_exists(conn, "trades"):
        return "trades"
    for t in ("trade_state", "positions", "paper_trades"):
        if table_exists(conn, t):
            return t
    raise RuntimeError("Could not detect a trades table (expected trades/trade_state/positions).")

def resolve_db_path() -> str:
    try:
        from config import cfg
        raw = getattr(cfg, "DB_FILE", None) or os.getenv("DB_FILE") or "./bot_data.db"
    except Exception:
        raw = os.getenv("DB_FILE") or "./bot_data.db"

    raw = str(raw)
    if os.path.isabs(raw):
        return raw
    # IMPORTANT: resolve relative paths against PROJECT_ROOT (not tools/)
    return os.path.abspath(os.path.join(PROJECT_ROOT, raw))

def fetch_trade_from_db(db_path: str, trade_id: int) -> Optional[Dict[str, Any]]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found at {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        table = detect_trade_table(conn)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} WHERE id=?", (trade_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def init_binance_client():
    from trading_executor import init_client
    return init_client(force=True)

def fmt_money(x: float) -> str:
    try:
        return f"{x:,.4f}"
    except Exception:
        return str(x)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-id", type=int, default=None)
    ap.add_argument("--symbol", type=str, default=None)
    ap.add_argument("--window-minutes", type=int, default=720)
    ap.add_argument("--no-binance", action="store_true", help="Only read DB; do not query Binance")
    args = ap.parse_args()

    db_path = resolve_db_path()
    print(f"[INFO] Project root: {PROJECT_ROOT}")
    print(f"[INFO] Using DB: {db_path} (exists={os.path.exists(db_path)})")

    trade = None
    sym = (args.symbol or "").strip()

    if args.trade_id is not None:
        trade = fetch_trade_from_db(db_path, args.trade_id)
        if not trade:
            print(f"[ERR] Trade id {args.trade_id} not found in DB.")
            # Common cause: script created a NEW empty DB at tools/bot_data.db
            maybe_wrong = os.path.join(THIS_DIR, "bot_data.db")
            if os.path.exists(maybe_wrong):
                print(f"[HINT] You have {maybe_wrong}. If that was created by mistake, delete it.")
            print("[HINT] Run: find . -maxdepth 3 -name 'bot_data.db' -print -exec ls -lh {} \\;")
            sys.exit(2)
        sym = (trade.get("symbol") or "").strip()

    if not sym:
        print("[ERR] Provide --trade-id or --symbol.")
        sys.exit(2)

    now_ms = utc_now_ms()
    win_ms = int(max(5, args.window_minutes) * 60 * 1000)
    start_ms = now_ms - win_ms

    if trade:
        ts_ms = trade.get("timestamp_ms") or trade.get("open_time_ms") or trade.get("ts_ms")
        try:
            ts_ms = int(ts_ms) if ts_ms else None
        except Exception:
            ts_ms = None
        if ts_ms and ts_ms > 0:
            start_ms = max(0, ts_ms - 5 * 60 * 1000)

    print(f"[INFO] Symbol={sym} window_start_utc={datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).isoformat()}")

    if trade:
        status = trade.get("status")
        direction = trade.get("direction") or trade.get("ai_vote") or trade.get("signal") or ""
        entry = trade.get("entry_price")
        qty = trade.get("quantity")
        print(f"[DB] id={trade.get('id')} status={status} direction_hint={direction} entry={entry} qty={qty}")

    if args.no_binance:
        return

    c = init_binance_client()
    if c is None:
        print("[ERR] Binance client is None. Are you in PAPER mode or missing API keys?")
        print("      If you are LIVE on testnet, ensure BINANCE_API_KEY/BINANCE_API_SECRET are set and PAPER_TRADING=false.")
        sys.exit(3)

    # Exchange position snapshot
    try:
        pos = c.futures_position_information(symbol=sym)
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
    except Exception as e:
        print(f"[WARN] Could not fetch position info: {e}")

    # Account trades (fills)
    def get_account_trades(start, end) -> List[Dict[str, Any]]:
        try:
            return c.futures_account_trades(symbol=sym, startTime=start, endTime=end, limit=1000) or []
        except TypeError:
            return c.futures_account_trades(symbol=sym, startTime=start, limit=1000) or []
        except Exception:
            try:
                return c.futures_account_trades(symbol=sym, limit=1000) or []
            except Exception:
                return []

    fills = get_account_trades(start_ms, now_ms)
    # Filter (in case API returned older)
    filtered = []
    for f in fills:
        try:
            tms = int(f.get("time") or f.get("timestamp") or 0)
        except Exception:
            tms = 0
        if tms and tms < start_ms:
            continue
        filtered.append(f)
    fills = filtered

    realized = 0.0
    commissions = 0.0
    qty_buy = qty_sell = 0.0
    notional_buy = notional_sell = 0.0

    for f in fills:
        qty_f = parse_float(f.get("qty"), 0.0)
        price_f = parse_float(f.get("price"), 0.0)
        side = str(f.get("side") or "").upper()
        realized += parse_float(f.get("realizedPnl"), 0.0)
        commissions += parse_float(f.get("commission"), 0.0)
        if side == "BUY":
            qty_buy += qty_f
            notional_buy += qty_f * price_f
        elif side == "SELL":
            qty_sell += qty_f
            notional_sell += qty_f * price_f

    # Funding fees (best-effort)
    funding = 0.0
    try:
        inc = c.futures_income_history(symbol=sym, incomeType="FUNDING_FEE", startTime=start_ms, endTime=now_ms, limit=1000) or []
        for r in inc:
            funding += parse_float(r.get("income"), 0.0)
    except TypeError:
        try:
            inc = c.futures_income_history(symbol=sym, incomeType="FUNDING_FEE", startTime=start_ms, limit=1000) or []
            for r in inc:
                funding += parse_float(r.get("income"), 0.0)
        except Exception:
            pass
    except Exception:
        pass

    net = realized + funding - commissions

    print("\n========== SUMMARY ==========")
    print(f"fills_count: {len(fills)}")
    print(f"realized_pnl: {fmt_money(realized)} USDT")
    print(f"commission:   {fmt_money(commissions)} USDT")
    print(f"funding_fee:  {fmt_money(funding)} USDT")
    print(f"net_est:      {fmt_money(net)} USDT   (realized + funding - commission)")
    if qty_buy > 0:
        print(f"avg_buy:      {notional_buy/qty_buy:.10f} qty_buy={qty_buy:,.0f}")
    if qty_sell > 0:
        print(f"avg_sell:     {notional_sell/qty_sell:.10f} qty_sell={qty_sell:,.0f}")

    if fills:
        print("\nLast fills:")
        for f in fills[-5:]:
            tms = int(f.get("time") or 0)
            ts = datetime.fromtimestamp(tms/1000, tz=timezone.utc).isoformat() if tms else "?"
            print(f" - {ts} side={f.get('side')} qty={f.get('qty')} price={f.get('price')} realizedPnl={f.get('realizedPnl')} commission={f.get('commission')}")

if __name__ == "__main__":
    main()
