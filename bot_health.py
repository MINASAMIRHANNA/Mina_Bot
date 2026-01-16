#!/usr/bin/env python3
"""
bot_health.py

Human-friendly runtime report (UTC project).
This script is safe to run even if some components are missing; it prints warnings instead of crashing.

It helps answer:
- Is the DB reachable, and which DB file is being used?
- Are there multiple bot_data.db files that could cause "dashboard shows empty" confusion?
- Are there active trades / recent logs / recent equity snapshots?
- Can Binance endpoints be reached (public; private if keys present)?
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone

from config import cfg
from database import DatabaseManager
from binance.client import Client

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ok(msg: str) -> None:
    print(f"[{GREEN}OK{RESET}] {msg}")


def _warn(msg: str) -> None:
    print(f"[{YELLOW}WARN{RESET}] {msg}")


def _fail(msg: str) -> None:
    print(f"[{RED}FAIL{RESET}] {msg}")


def _find_db_files(base: Path) -> list[Path]:
    out: list[Path] = []
    for p in [base / "bot_data.db", base / "dashboard" / "bot_data.db"]:
        if p.exists():
            out.append(p)
    return out


def _query_one(db: DatabaseManager, sql: str):
    lock = getattr(db, "lock", None)
    cur = getattr(db, "cursor", None)
    if cur is None:
        return None
    try:
        if lock:
            with lock:
                cur.execute(sql)
                return cur.fetchone()
        cur.execute(sql)
        return cur.fetchone()
    except Exception:
        return None


def check_health():
    print(f"\n🏥 {YELLOW}RUNNING BOT HEALTH CHECK...{RESET}")
    print(f"UTC now: {utc_iso()}")
    print("=" * 60)

    # 0) DB file clarity (prevents the "empty dashboard" confusion)
    project_root = Path(__file__).resolve().parent
    db_files = _find_db_files(project_root)
    cfg_db = getattr(cfg, "DB_FILE", str(project_root / "bot_data.db"))
    print(f"Configured DB_FILE: {cfg_db}")
    if len(db_files) > 1:
        _warn("Multiple bot_data.db files detected:")
        for p in db_files:
            print("   -", p)
        _warn("Make sure BOTH bot and dashboard use the SAME bot_data.db (run from the same project root)")
    elif len(db_files) == 1:
        _ok(f"DB file found: {db_files[0].relative_to(project_root)}")
    else:
        _warn("No bot_data.db found yet (it will be created on first run)")

    print("-" * 60)

    # 1) DATABASE CHECK
    try:
        db = DatabaseManager()
        _ok("Database connection")
    except Exception as e:
        _fail(f"Database connection failed: {e}")
        return

    # Active trades
    try:
        active = db.get_all_active_trades() if hasattr(db, "get_all_active_trades") else []
        _ok(f"Active trades: {len(active)}")
    except Exception as e:
        _warn(f"Active trades read failed: {e}")

    # Recent logs
    try:
        logs = db.get_logs(limit=3) if hasattr(db, "get_logs") else []
        if logs:
            _ok("Recent logs:")
            for row in logs:
                # row could be dict or tuple depending on implementation
                print("   -", row)
        else:
            _warn("No logs yet")
    except Exception as e:
        _warn(f"Logs read failed: {e}")

    # Recent equity snapshot
    snap = _query_one(db, "SELECT timestamp, total_balance, unrealized_pnl FROM equity_history ORDER BY id DESC LIMIT 1;")
    if snap:
        _ok(f"Latest equity snapshot: ts={snap[0]} equity={snap[1]} unreal={snap[2]}")
    else:
        _warn("No equity snapshots found yet (execution_monitor should create them when running)")

    # Risk / governance settings
    kill = None
    try:
        if hasattr(db, "get_setting"):
            kill = db.get_setting("kill_switch", default="0")
            mode = db.get_setting("mode", default="TEST")
            req = db.get_setting("require_dashboard_approval", default="1")
            _ok(f"Settings: kill_switch={kill} | mode={mode} | require_approval={req}")
    except Exception:
        pass

    print("-" * 60)

    # 2) BINANCE CHECK
    api_key = getattr(cfg, "BINANCE_API_KEY", None) or ""
    api_secret = getattr(cfg, "BINANCE_API_SECRET", None) or ""
    testnet = bool(getattr(cfg, "USE_TESTNET", True))

    try:
        client = Client(api_key, api_secret, testnet=testnet)
        _ = client.futures_exchange_info()
        _ok("Binance public futures reachable")
    except Exception as e:
        _warn(f"Binance public futures failed: {e}")
        client = None

    if api_key and api_secret and client is not None:
        try:
            _ = client.futures_account()
            _ok("Binance futures credentials OK")
        except Exception as e:
            _warn(f"Binance credentials check failed: {e}")
    else:
        _warn("Binance keys not set — skipped private account checks")

    print("-" * 60)

    # 3) CORE FILES CHECK
    required_files = [
        "main.py",
        "database.py",
        "config.py",
        "trading_executor.py",
        "risk_monitor.py",
        "risk_manager.py",
        "signal_engine.py",
        "scorer.py",
        "fetcher.py",
        "indicators.py",
        "pump_detector.py",
    ]
    missing = [f for f in required_files if not (project_root / f).exists()]
    if not missing:
        _ok("Core files present")
    else:
        _warn("Missing core files: " + ", ".join(missing))

    models_dir = project_root / "models"
    if models_dir.exists():
        model_files = list(models_dir.glob("*.pkl")) + list(models_dir.glob("*.joblib"))
        if model_files:
            _ok(f"Models present: {len(model_files)} file(s)")
        else:
            _warn("Models folder exists but no model files found (OK if rule-based/paper)")
    else:
        _warn("No ./models folder (OK if not trained yet)")

    print("=" * 60)
    print(f"{GREEN}Health Check Complete.{RESET}\n")


if __name__ == "__main__":
    check_health()
