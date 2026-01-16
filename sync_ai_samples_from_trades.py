#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sync_ai_samples_from_trades.py (patched v2)

Fixes:
- sqlite3.Row has no .get(): use dict(row) or row['col'].
- Recompute return_pct even when exit/pnl already exist (via --recompute-return).
- Computes return_pct as Notional ROI: pnl / abs(entry_price * qty) * 100.

Usage:
  python sync_ai_samples_from_trades.py --db bot_data.db --lookback-hours 720 --recompute-return --apply
"""

import argparse, sqlite3, os, time
from _common import get_cols, has_col, safe_float

def utc_ms() -> int:
    return int(time.time()*1000)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='bot_data.db')
    ap.add_argument('--lookback-hours', type=int, default=168)
    ap.add_argument('--recompute-return', action='store_true',
                    help='Recompute return_pct for rows where it is missing/0 OR sign-mismatched vs pnl.')
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"❌ DB not found: {args.db}")
        raise SystemExit(2)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    a_cols = get_cols(conn,'ai_samples')
    t_cols = get_cols(conn,'trades')

    if not has_col(a_cols,'trade_id'):
        print("❌ ai_samples missing trade_id")
        return

    # trades cols
    pnl_col = 'pnl' if has_col(t_cols,'pnl') else None
    close_col = 'close_price' if has_col(t_cols,'close_price') else None
    entry_col = 'entry_price' if has_col(t_cols,'entry_price') else None
    qty_col = 'quantity' if has_col(t_cols,'quantity') else ('qty' if has_col(t_cols,'qty') else None)
    reason_col = 'close_reason' if has_col(t_cols,'close_reason') else None
    status_col = 'status' if has_col(t_cols,'status') else None
    closed_at_col = 'closed_at' if has_col(t_cols,'closed_at') else None

    if not (pnl_col and close_col and entry_col and qty_col and status_col):
        print("❌ trades schema missing required columns (need pnl, close_price, entry_price, quantity/qty, status)")
        return

    # ai_samples cols to update
    up_cols = []
    for c in ('exit_price','pnl','return_pct','close_reason','closed_at_ms','status'):
        if has_col(a_cols,c):
            up_cols.append(c)
    if not up_cols:
        print("⚠️ No updatable outcome columns exist in ai_samples.")
        return

    params = []
    trade_filter = ""
    if args.lookback_hours > 0 and closed_at_col:
        cutoff = time.time() - args.lookback_hours*3600
        cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))
        trade_filter = f" AND COALESCE(NULLIF(t.{closed_at_col},''),'') >= ?"
        params.append(cutoff_iso)

    # Base need: missing exit/pnl
    need_expr = "(a.exit_price IS NULL OR a.exit_price=0 OR a.pnl IS NULL)"
    if args.recompute_return and has_col(a_cols,'return_pct'):
        # Also if return_pct missing/0 OR sign mismatch with pnl
        need_expr += " OR (a.return_pct IS NULL OR a.return_pct=0 OR ((a.pnl>0 AND a.return_pct<0) OR (a.pnl<0 AND a.return_pct>0)))"

    select_reason = f", t.{reason_col} as close_reason" if reason_col else ""

    q = f"""
    SELECT a.id as ai_id, a.trade_id as trade_id,
           t.symbol as symbol,
           t.{entry_col} as entry_price,
           t.{close_col} as exit_price,
           t.{qty_col} as qty,
           t.{pnl_col} as pnl
           {select_reason}
    FROM ai_samples a
    JOIN trades t ON t.id = a.trade_id
    WHERE t.{status_col}='CLOSED'
      AND ({need_expr})
      {trade_filter}
    ORDER BY a.id ASC
    """

    rows = conn.execute(q, params).fetchall()
    if not rows:
        print("✅ No ai_samples needing sync.")
        return

    updates = []
    for row in rows:
        r = dict(row)  # make .get available
        entry = safe_float(r.get('entry_price'), 0.0)
        exitp = safe_float(r.get('exit_price'), 0.0)
        qty = safe_float(r.get('qty'), 0.0)
        pnl = safe_float(r.get('pnl'), 0.0)

        notional = abs(entry * qty)
        ret_pct = None
        if notional > 0:
            ret_pct = (pnl / notional) * 100.0

        updates.append({
            'ai_id': r['ai_id'],
            'trade_id': r['trade_id'],
            'symbol': r.get('symbol'),
            'exit': exitp,
            'pnl': pnl,
            'ret': ret_pct,
            'close_reason': r.get('close_reason'),
            'closed_at_ms': utc_ms(),
            'status': 'CLOSED',
        })

    for u in updates:
        ret_show = None if u['ret'] is None else round(u['ret'], 6)
        print(f"🧠 Recomputed ai_sample id={u['ai_id']} trade_id={u['trade_id']} {u['symbol']} pnl={u['pnl']} ret%={ret_show}")

    if not args.apply:
        print(f"DRY RUN: would update {len(updates)} ai_samples. Add --apply to write.")
        return

    set_parts=[f"{c}=?" for c in up_cols]
    sql_up = f"UPDATE ai_samples SET {', '.join(set_parts)} WHERE id=?"

    for u in updates:
        vals=[]
        for c in up_cols:
            if c=='exit_price': vals.append(u['exit'])
            elif c=='pnl': vals.append(u['pnl'])
            elif c=='return_pct': vals.append(u['ret'])
            elif c=='close_reason': vals.append(u['close_reason'])
            elif c=='closed_at_ms': vals.append(u['closed_at_ms'])
            elif c=='status': vals.append(u['status'])
            else: vals.append(None)
        vals.append(u['ai_id'])
        conn.execute(sql_up, vals)

    conn.commit()
    print(f"✅ Updated {len(updates)} ai_samples (DB updated)")
    conn.close()

if __name__ == '__main__':
    main()
