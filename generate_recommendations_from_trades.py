#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""generate_recommendations_from_trades.py

Generate strategy recommendations from CLOSED trades.

What it does:
- Reads CLOSED trades (last N or lookback-hours).
- Groups by (market_type, strategy_tag, exit_profile).
- Computes: n, winrate, total_pnl, bias (LONG/SHORT/MIXED), score.
- Writes ./recommendations_generated.json
- Optionally inserts into SQLite recommendations table in a way that fits BOTH schemas:
  - Old/keyed schema: key/category/current_value/suggested_value/reason/status/date_utc/created_at_ms
  - New/strategy schema: strategy_tag/exit_profile/market_type/bias/n/winrate/total_pnl/details_json/score/created_at

IMPORTANT:
- The project uses the `recommendations` table for config recommendations too.
  So --clear will ONLY clear category='strategy' rows when possible.
"""

import argparse, sqlite3, os, json, time, math
from collections import defaultdict
from _common import get_cols, has_col, safe_float, direction_from_signal, utc_now_iso

def now_date_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='bot_data.db')
    ap.add_argument('--lookback-hours', type=int, default=0, help='If >0, use trades closed in last N hours (requires closed_at).')
    ap.add_argument('--n', type=int, default=80, help='Last N CLOSED trades (fallback if closed_at missing).')
    ap.add_argument('--top', type=int, default=10)
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--clear', action='store_true')
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print(f"❌ DB not found: {args.db}")
        raise SystemExit(2)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    tr_cols = get_cols(conn,'trades')
    # columns in trades
    st_col = 'strategy_tag' if has_col(tr_cols,'strategy_tag') else None
    ep_col = 'exit_profile' if has_col(tr_cols,'exit_profile') else None
    mt_col = 'market_type' if has_col(tr_cols,'market_type') else None
    pnl_col = 'pnl' if has_col(tr_cols,'pnl') else None
    signal_col = 'signal' if has_col(tr_cols,'signal') else None
    closed_at_col = 'closed_at' if has_col(tr_cols,'closed_at') else None

    if not pnl_col:
        print("❌ trades table missing pnl column; cannot build recommendations.")
        return

    # Fetch closed trades
    where = "WHERE status='CLOSED'"
    params = []
    if args.lookback_hours > 0 and closed_at_col:
        # timestamps stored as ISO strings; compare lexicographically
        cutoff = time.time() - args.lookback_hours*3600
        cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))
        where += f" AND COALESCE(NULLIF({closed_at_col},''), '') >= ?"
        params.append(cutoff_iso)

    sel_cols = ["id","symbol",pnl_col]
    if mt_col: sel_cols.append(mt_col)
    if st_col: sel_cols.append(st_col)
    if ep_col: sel_cols.append(ep_col)
    if signal_col: sel_cols.append(signal_col)
    if closed_at_col: sel_cols.append(closed_at_col)

    sql = f"SELECT {', '.join(sel_cols)} FROM trades {where} ORDER BY id DESC LIMIT ?"
    params.append(args.n)
    rows = conn.execute(sql, params).fetchall()

    if not rows:
        print("⚠️ No CLOSED trades found in selection.")
        return

    # Group
    groups = defaultdict(lambda: {"n":0,"wins":0,"pnl":0.0,"long":0,"short":0,"samples":[]})
    for r in rows:
        market = (r[mt_col] if mt_col else "futures") or "futures"
        st = (r[st_col] if st_col else None) or "UNKNOWN"
        ep = (r[ep_col] if ep_col else None) or "UNKNOWN"
        pnl = safe_float(r[pnl_col], 0.0)

        d = None
        if signal_col:
            d = direction_from_signal(r[signal_col])
        # Direction counts only if recognized
        if d == +1: 
            groups[(market, st, ep)]["long"] += 1
        elif d == -1:
            groups[(market, st, ep)]["short"] += 1

        g = groups[(market, st, ep)]
        g["n"] += 1
        g["pnl"] += pnl
        if pnl > 0:
            g["wins"] += 1
        g["samples"].append({"trade_id": r["id"], "symbol": r["symbol"], "pnl": pnl})

    recs = []
    for (market, st, ep), g in groups.items():
        n = g["n"]
        winrate = (g["wins"] / n * 100.0) if n else 0.0
        total_pnl = g["pnl"]

        # Bias: based on direction counts
        long_c, short_c = g["long"], g["short"]
        bias = "UNKNOWN"
        if long_c and not short_c:
            bias = "LONG"
        elif short_c and not long_c:
            bias = "SHORT"
        elif long_c and short_c:
            if long_c > short_c * 1.25:
                bias = "LONG"
            elif short_c > long_c * 1.25:
                bias = "SHORT"
            else:
                bias = "MIXED"

        # Conservative score: normalize by sqrt(n) so tiny-n doesn't dominate
        score = (total_pnl / max(1.0, math.sqrt(n)))

        recs.append({
            "market_type": market,
            "bias": bias,
            "strategy_tag": st,
            "exit_profile": ep,
            "n": n,
            "winrate": round(winrate, 2),
            "total_pnl": round(total_pnl, 6),
            "score": round(score, 6),
            "examples": g["samples"][:10],
        })

    recs.sort(key=lambda x: x["score"], reverse=True)
    top = recs[:args.top]

    print("\n🏁 Top Strategy Recommendations\n")
    for i, r in enumerate(top, 1):
        print(f"{i:2d}. {r['market_type']} | {r['bias']} | {r['strategy_tag']} | {r['exit_profile']}  | n={r['n']} win%={r['winrate']} totalPnL={r['total_pnl']} score={r['score']}")
    out_json = os.path.abspath("recommendations_generated.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"generated_at": utc_now_iso(), "items": top}, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Wrote JSON: {out_json}")

    if not args.apply:
        print("(tip) Add --apply to insert into DB.")
        return

    # Insert into recommendations table (best-effort) respecting mixed schema
    reco_cols = get_cols(conn,'recommendations')
    if not reco_cols:
        print("⚠️ recommendations table missing.")
        return

    # Clear only strategy category rows when possible
    if args.clear:
        if has_col(reco_cols,'category'):
            conn.execute("DELETE FROM recommendations WHERE category='strategy'")
        else:
            conn.execute("DELETE FROM recommendations")
        conn.commit()

    insert_cols = []
    # keyed schema
    for c in ('date_utc','created_at_ms','key','current_value','suggested_value','category','reason','score','status','approved_at_ms'):
        if has_col(reco_cols,c):
            insert_cols.append(c)
    # new schema extras
    for c in ('created_at','strategy_tag','exit_profile','market_type','bias','n','winrate','total_pnl','details_json'):
        if has_col(reco_cols,c) and c not in insert_cols:
            insert_cols.append(c)

    if not insert_cols:
        print("⚠️ Cannot insert; recommendations schema has none of expected columns.")
        print(reco_cols)
        return

    sql_ins = f"INSERT INTO recommendations({','.join(insert_cols)}) VALUES({','.join(['?']*len(insert_cols))})"
    now_iso = utc_now_iso()
    now_ms = int(time.time()*1000)
    date_utc = now_date_utc()

    for rank, r in enumerate(top, 1):
        payload = json.dumps(r, ensure_ascii=False)
        vals = []
        for c in insert_cols:
            if c == 'date_utc': vals.append(date_utc)
            elif c == 'created_at_ms': vals.append(now_ms)
            elif c == 'created_at': vals.append(now_iso)
            elif c == 'key': vals.append(f"STRATEGY_REC_{rank}")
            elif c == 'category': vals.append('strategy')
            elif c == 'current_value': vals.append(None)
            elif c == 'suggested_value': vals.append(json.dumps({
                "market_type": r['market_type'],
                "bias": r['bias'],
                "strategy_tag": r['strategy_tag'],
                "exit_profile": r['exit_profile'],
            }, ensure_ascii=False))
            elif c == 'reason': vals.append(f"n={r['n']} win%={r['winrate']} totalPnL={r['total_pnl']} bias={r['bias']}")
            elif c == 'status': vals.append('PENDING')
            elif c == 'approved_at_ms': vals.append(None)
            elif c == 'score': vals.append(r['score'])
            elif c == 'strategy_tag': vals.append(r['strategy_tag'])
            elif c == 'exit_profile': vals.append(r['exit_profile'])
            elif c == 'market_type': vals.append(r['market_type'])
            elif c == 'bias': vals.append(r['bias'])
            elif c == 'n': vals.append(r['n'])
            elif c == 'winrate': vals.append(r['winrate'])
            elif c == 'total_pnl': vals.append(r['total_pnl'])
            elif c == 'details_json': vals.append(payload)
            else: vals.append(None)
        conn.execute(sql_ins, vals)

    conn.commit()
    print(f"✅ Inserted {len(top)} strategy recommendations into DB (category='strategy').")

if __name__ == '__main__':
    main()
