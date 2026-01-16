#!/usr/bin/env python3
"""
health_check.py

Quick pre-flight checks for crypto_bot (UTC project).

What it checks:
- Config load and key flags
- SQLite DB connectivity
- Binance public endpoints (and private futures account if keys are present)
- Presence/loadability of ML models (optional)

Exit codes:
- 0: OK (or OK with warnings)
- 1: One or more critical failures
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

try:
    from config import cfg
except Exception as e:
    print("[FAIL] config import failed:", e)
    sys.exit(1)

# Optional deps
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

try:
    import joblib  # type: ignore
except Exception:
    joblib = None  # type: ignore

try:
    from database import DatabaseManager
except Exception as e:
    print("[FAIL] database import failed:", e)
    sys.exit(1)

try:
    from binance.client import Client
except Exception as e:
    print("[FAIL] python-binance import failed:", e)
    sys.exit(1)


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ok(msg: str) -> None:
    print(f"[OK]   {msg}")


def _warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")


def main() -> int:
    print("🩺 crypto_bot health check")
    print("UTC now:", utc_iso())
    print(f"Mode: {'TESTNET' if getattr(cfg, 'USE_TESTNET', True) else 'MAINNET'} | PAPER_TRADING={getattr(cfg, 'PAPER_TRADING', True)}")
    print(f"DB_FILE: {getattr(cfg, 'DB_FILE', os.getenv('DB_FILE', '(default bot_data.db next to database.py)'))}")
    print("-" * 60)

    failures = 0

    # ---------------- DB ----------------
    try:
        db = DatabaseManager()
        # Minimal read calls (defensive across versions)
        if hasattr(db, "get_logs"):
            _ = db.get_logs(limit=1)
        if hasattr(db, "get_all_active_trades"):
            _ = db.get_all_active_trades()
        _ok("Database connection + basic queries")
    except Exception as e:
        failures += 1
        _fail(f"Database error: {e}")

    # ---------------- Binance (public) ----------------
    api_key = getattr(cfg, "BINANCE_API_KEY", None) or ""
    api_secret = getattr(cfg, "BINANCE_API_SECRET", None) or ""
    testnet = bool(getattr(cfg, "USE_TESTNET", True))
    try:
        client = Client(api_key, api_secret, testnet=testnet)
        # Public endpoint (works without keys)
        _ = client.futures_exchange_info()
        _ok("Binance public futures endpoints reachable (exchange_info)")
    except Exception as e:
        failures += 1
        _fail(f"Binance public futures endpoint failed: {e}")
        client = None

    # ---------------- Binance (private) ----------------
    if (getattr(cfg, "BINANCE_API_KEY", None) and getattr(cfg, "BINANCE_API_SECRET", None)) and client is not None:
        try:
            _ = client.futures_account()
            _ok("Binance futures credentials OK (futures_account)")
        except Exception as e:
            _warn(f"Binance futures private endpoint failed (keys present): {e}")
            _warn("If using Testnet, ensure keys are from testnet.binancefuture.com and USE_TESTNET=true")
    else:
        _warn("Binance keys not set — skipped private futures_account check (OK for public-data runs)")

    # ---------------- Models ----------------
    models_dir = Path(getattr(cfg, "BASE_DIR", Path(__file__).resolve().parent)).joinpath("models")
    candidates = []
    if models_dir.exists():
        candidates = sorted(models_dir.glob("*.joblib")) + sorted(models_dir.glob("*.pkl"))
    if not candidates:
        _warn(f"No model files found in {models_dir} (OK if you're running rule-based / paper)")
    else:
        # Try loading one model to confirm environment
        if joblib is None:
            _warn("joblib not available; cannot test model load")
        else:
            try:
                m = joblib.load(str(candidates[0]))
                _ok(f"Model file loads OK: {candidates[0].name} ({type(m).__name__})")
            except Exception as e:
                _warn(f"Model load error for {candidates[0].name}: {e}")

    # ---------------- Dashboard (optional) ----------------
    dash_url = os.getenv("DASHBOARD_URL") or os.getenv("DASHBOARD_BASE_URL") or ""
    if dash_url and requests is not None:
        try:
            r = requests.get(dash_url.rstrip("/") + "/health", timeout=3)
            if r.status_code == 200:
                _ok("Dashboard /health reachable")
            else:
                _warn(f"Dashboard /health returned {r.status_code}")
        except Exception as e:
            _warn(f"Dashboard check failed: {e}")
    elif dash_url:
        _warn("requests not installed; skipped dashboard HTTP check")

    print("-" * 60)
    if failures == 0:
        print("✅ Health check finished: OK")
        return 0
    print(f"❌ Health check finished: {failures} critical failure(s)")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
