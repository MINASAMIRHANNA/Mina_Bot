#!/usr/bin/env python3
import os
import sys
import json
import time
import sqlite3
import argparse
from datetime import datetime

def now():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def list_tables(conn):
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()
    return [r["name"] for r in rows]

def table_cols(conn, table: str):
    cols = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return [c["name"] for c in cols]

def safe_select(conn, table: str, limit: int = 20, order_col: str = None):
    cols = table_cols(conn, table)
    if not cols:
        return []
    order_sql = ""
    if order_col and order_col in cols:
        order_sql = f" ORDER BY {order_col} DESC "
    elif "id" in cols:
        order_sql = " ORDER BY id DESC "
    elif "created_at" in cols:
        order_sql = " ORDER BY created_at DESC "
    elif "timestamp" in cols:
        order_sql = " ORDER BY timestamp DESC "
    q = f"SELECT * FROM {table}{order_sql} LIMIT ?;"
    rows = conn.execute(q, (limit,)).fetchall()
    return [dict(r) for r in rows]

def kv_settings(conn):
    if "settings" not in list_tables(conn):
        return {}
    cols = table_cols(conn, "settings")
    if "key" in cols and "value" in cols:
        rows = conn.execute("SELECT key, value FROM settings;").fetchall()
        return {r["key"]: r["value"] for r in rows}
    return {}

def count_by(conn, table: str, col: str):
    cols = table_cols(conn, table)
    if col not in cols:
        return {}
    rows = conn.execute(f"SELECT {col} as k, COUNT(*) as c FROM {table} GROUP BY {col};").fetchall()
    return {str(r["k"]): int(r["c"]) for r in rows}

def find_trade_table(conn):
    # Prefer 'trades' if exists
    tables = list_tables(conn)
    if "trades" in tables:
        return "trades"
    # fallback: first table containing 'trade'
    for t in tables:
        if "trade" in t.lower():
            return t
    return None

def open_trades(conn, trade_table: str):
    cols = table_cols(conn, trade_table)
    if "status" not in cols:
        return []
    rows = conn.execute(f"SELECT * FROM {trade_table} WHERE status='OPEN' ORDER BY id DESC LIMIT 200;").fetchall()
    return [dict(r) for r in rows]

def last_commands(conn):
    if "commands" not in list_tables(conn):
        return []
    cols = table_cols(conn, "commands")
    # show most important cols first
    want = [c for c in ["id","cmd","status","created_at","updated_at","params","payload","result","error","message"] if c in cols]
    if not want:
        return safe_select(conn, "commands", limit=30)
    q = f"SELECT {', '.join(want)} FROM commands ORDER BY id DESC LIMIT 30;"
    rows = conn.execute(q).fetchall()
    return [dict(r) for r in rows]

def detect_learning(conn):
    tables = list_tables(conn)
    out = {}
    # ai_samples (closed-loop)
    if "ai_samples" in tables:
        out["ai_samples_count"] = conn.execute("SELECT COUNT(*) as c FROM ai_samples;").fetchone()["c"]
        cols = table_cols(conn, "ai_samples")
        label_col = None
        for c in ["label","y","outcome","target","result_label"]:
            if c in cols:
                label_col = c
                break
        if label_col:
            out["ai_samples_labeled_count"] = conn.execute(
                f"SELECT COUNT(*) as c FROM ai_samples WHERE {label_col} IS NOT NULL AND {label_col}!='';"
            ).fetchone()["c"]
        out["ai_samples_recent"] = safe_select(conn, "ai_samples", limit=10)
    else:
        out["ai_samples_count"] = 0

    # model events/log tables (if exist)
    for t in tables:
        if any(k in t.lower() for k in ["model", "trainer", "training", "ml"]):
            out.setdefault("model_related_tables", []).append(t)

    return out

def detect_recommendations(conn):
    tables = list_tables(conn)
    rec_tables = []
    for t in tables:
        tl = t.lower()
        if any(k in tl for k in ["recommend", "leader", "paper_arena", "paper", "strategy_reco", "arena", "tournament"]):
            rec_tables.append(t)
    rec_tables = sorted(set(rec_tables))
    data = {}
    for t in rec_tables[:10]:
        data[t] = {
            "count": conn.execute(f"SELECT COUNT(*) as c FROM {t};").fetchone()["c"],
            "recent": safe_select(conn, t, limit=10)
        }
    return {"tables": rec_tables, "data": data}

def summarize_trades(conn, trade_table: str):
    cols = table_cols(conn, trade_table)
    summary = {}
    if "status" in cols:
        summary["by_status"] = count_by(conn, trade_table, "status")
    # Try PnL fields
    pnl_cols = [c for c in ["pnl","realized_pnl","net_pnl","profit","profit_usdt","roi","pnl_usdt"] if c in cols]
    # closed trades stats
    if "status" in cols:
        if pnl_cols:
            c = pnl_cols[0]
            try:
                row = conn.execute(
                    f"SELECT COUNT(*) as n, "
                    f"AVG(CASE WHEN {c} IS NOT NULL THEN {c} END) as avg_pnl, "
                    f"SUM(CASE WHEN {c} > 0 THEN 1 ELSE 0 END) as wins, "
                    f"SUM(CASE WHEN {c} <= 0 THEN 1 ELSE 0 END) as losses "
                    f"FROM {trade_table} WHERE status='CLOSED';"
                ).fetchone()
                summary["closed_stats"] = {k: (float(row[k]) if row[k] is not None else None) for k in row.keys()}
            except Exception:
                summary["closed_stats"] = None
    summary["recent_trades"] = safe_select(conn, trade_table, limit=20)
    return summary

def binance_check(cfg, verbose=True):
    """
    Try to connect to Binance Futures and fetch:
      - wallet balance
      - open positions
      - open orders
    """
    try:
        from binance.client import Client
    except Exception as e:
        return {"ok": False, "error": f"python-binance not available: {e}"}

    api_key = getattr(cfg, "API_KEY", None)
    api_secret = getattr(cfg, "API_SECRET", None)
    use_testnet = bool(getattr(cfg, "USE_TESTNET", False))
    if not api_key or not api_secret:
        return {"ok": False, "error": "Missing API_KEY/API_SECRET in config/.env"}

    client = Client(api_key, api_secret, testnet=use_testnet)
    if use_testnet:
        # Futures Testnet base
        client.FUTURES_URL = "https://demo-fapi.binance.com"

    out = {"ok": True, "use_testnet": use_testnet}
    try:
        acct = client.futures_account()
        out["wallet_balance"] = float(acct.get("totalWalletBalance", 0.0))
        out["unrealized_pnl"] = float(acct.get("totalUnrealizedProfit", 0.0))
    except Exception as e:
        out["account_error"] = str(e)

    try:
        pos = client.futures_position_information()
        open_pos = []
        for p in pos:
            amt = float(p.get("positionAmt", 0.0))
            if amt != 0.0:
                open_pos.append({
                    "symbol": p.get("symbol"),
                    "positionAmt": amt,
                    "entryPrice": float(p.get("entryPrice", 0.0)),
                    "markPrice": float(p.get("markPrice", 0.0)),
                    "unrealizedProfit": float(p.get("unRealizedProfit", p.get("unrealizedProfit", 0.0)) or 0.0),
                    "leverage": p.get("leverage"),
                    "positionSide": p.get("positionSide", "BOTH"),
                })
        out["open_positions_count"] = len(open_pos)
        out["open_positions"] = open_pos
    except Exception as e:
        out["positions_error"] = str(e)

    try:
        oo = client.futures_get_open_orders()
        out["open_orders_count"] = len(oo)
        out["open_orders_sample"] = oo[:20]
    except Exception as e:
        out["orders_error"] = str(e)

    return out

def main():
    ap = argparse.ArgumentParser(description="Crypto Bot Audit (DB + Binance cross-check)")
    ap.add_argument("--db", default="./bot_data.db", help="Path to bot_data.db")
    ap.add_argument("--out", default="./bot_audit_report.json", help="Output JSON report path")
    ap.add_argument("--models-dir", default="./models", help="Models directory to inspect")
    ap.add_argument("--no-binance", action="store_true", help="Skip Binance API check")
    args = ap.parse_args()

    report = {"generated_at": now(), "db_path": os.path.abspath(args.db)}
    if not os.path.exists(args.db):
        print(f"❌ DB not found: {args.db}")
        sys.exit(1)

    conn = connect_db(args.db)

    report["tables"] = list_tables(conn)
    report["settings"] = kv_settings(conn)

    # Config flags (from your project)
    try:
        from config import cfg
        report["config"] = {
            "USE_TESTNET": bool(getattr(cfg, "USE_TESTNET", False)),
            "PAPER_TRADING": bool(getattr(cfg, "PAPER_TRADING", False)),
            "REQUIRE_DASHBOARD_APPROVAL": bool(getattr(cfg, "REQUIRE_DASHBOARD_APPROVAL", False)),
            "AUTO_APPROVE_LIVE": bool(getattr(cfg, "AUTO_APPROVE_LIVE", False)),
            "AUTO_APPROVE_PAPER": bool(getattr(cfg, "AUTO_APPROVE_PAPER", False)),
            "DB_FILE": os.path.abspath(getattr(cfg, "DB_FILE", args.db)),
        }
    except Exception as e:
        report["config_error"] = str(e)

    # Commands
    report["commands_recent"] = last_commands(conn)

    # Trades
    trade_table = find_trade_table(conn)
    report["trade_table"] = trade_table
    if trade_table:
        report["trades_summary"] = summarize_trades(conn, trade_table)
        report["open_trades"] = open_trades(conn, trade_table)
    else:
        report["trades_summary"] = None
        report["open_trades"] = []

    # Signals / inbox (if exists)
    for t in ["signal_inbox", "signals", "pump_candidates", "analysis_results"]:
        if t in report["tables"]:
            report[f"{t}_count"] = conn.execute(f"SELECT COUNT(*) as c FROM {t};").fetchone()["c"]
            report[f"{t}_recent"] = safe_select(conn, t, limit=20)

    # Learning
    report["learning"] = detect_learning(conn)

    # Recommendations / Paper arena
    report["recommendations"] = detect_recommendations(conn)

    # Models folder scan
    models_info = {"models_dir": os.path.abspath(args.models_dir), "files": []}
    if os.path.isdir(args.models_dir):
        for fn in sorted(os.listdir(args.models_dir)):
            if fn.lower().endswith((".pkl", ".joblib", ".json")):
                p = os.path.join(args.models_dir, fn)
                st = os.stat(p)
                models_info["files"].append({
                    "name": fn,
                    "size": st.st_size,
                    "mtime": datetime.utcfromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S UTC")
                })
    report["models"] = models_info

    # Binance cross-check
    if not args.no_binance:
        try:
            from config import cfg
            report["binance"] = binance_check(cfg)
        except Exception as e:
            report["binance"] = {"ok": False, "error": str(e)}

    # Quick diagnosis hints
    hints = []
    cfgd = report.get("config", {})
    if cfgd.get("PAPER_TRADING") is True:
        hints.append("⚠️ PAPER_TRADING=True: أي صفقات من الداشبورد لن تظهر على Binance. اجعل PAPER_TRADING=false ثم Restart البوت.")
    if report.get("binance", {}).get("ok") and report.get("open_trades") is not None:
        db_open = len(report.get("open_trades", []))
        ex_open = int(report["binance"].get("open_positions_count", 0) or 0)
        if db_open == 0 and ex_open > 0:
            hints.append("⚠️ Binance فيها positions مفتوحة لكن DB مفيهاش OPEN trades: دي Orphan positions. شغّل execution_monitor ORPHAN_SYNC أو اعمل sync/reopen.")
    report["hints"] = hints

    # Save
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("✅ Audit completed.")
    print(f"Report saved to: {os.path.abspath(args.out)}")

    # Print small human summary
    print("\n===== SUMMARY =====")
    print(f"Time: {report['generated_at']}")
    if "config" in report:
        print("Config:", report["config"])
    if trade_table:
        by_status = report.get("trades_summary", {}).get("by_status", {})
        print(f"Trades by status: {by_status}")
        print(f"DB OPEN trades: {len(report.get('open_trades', []))}")
    if "binance" in report and report["binance"].get("ok"):
        print(f"Binance open positions: {report['binance'].get('open_positions_count')}")
        print(f"Wallet: {report['binance'].get('wallet_balance')} | UPnL: {report['binance'].get('unrealized_pnl')}")
    if report.get("hints"):
        print("\nHints:")
        for h in report["hints"]:
            print("-", h)

if __name__ == "__main__":
    main()
