#!/usr/bin/env python3

"""tools/runtime_diag.py — Runtime diagnostics (Safe / Best-effort)

يشخّص (Best-effort):
- بيأكد إنك شغال على DB واحدة فقط (cfg.DB_FILE + DB pinning state)
- بيطبع أهم إعدادات التشغيل (cfg + DB settings)
- SQLite integrity_check + وجود الجداول الأساسية وعدد rows
- اختبار وصول الداشبورد (/healthz, /api/healthz, /api/settings, /api/signals)
- /publish smoke test (اختياري مع token)

Usage:
  python tools/runtime_diag.py

Tip:
- شغّل Dashboard الأول، ثم bot + monitor، وبعدها شغّل السكربت.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def p(label: str, value) -> None:
    print(f"{label}: {value}")


def main() -> int:
    # Run from project root regardless of caller CWD
    os.chdir(str(PROJECT_ROOT))
    sys.path.insert(0, str(PROJECT_ROOT))

    p("[DIAG] cwd", os.getcwd())

    # ------------------------------------------------------------------
    # Import config + cfg
    # ------------------------------------------------------------------
    try:
        import config as config_mod
        from config import cfg
    except Exception as e:
        print("[DIAG] ❌ failed to import config:", e)
        return 2

    p("[DIAG] config.py", str(Path(config_mod.__file__).resolve()))
    p("[DIAG] dotenv_path", getattr(config_mod, "_DOTENV_PATH", None))
    p("[DIAG] DOTENV_OVERRIDE", os.getenv("DOTENV_OVERRIDE"))

    p("[DIAG] DB_FILE(raw)", getattr(cfg, "DB_FILE", ""))
    try:
        p("[DIAG] DB_FILE(abs)", str(Path(cfg.DB_FILE).expanduser().resolve()))
    except Exception:
        p("[DIAG] DB_FILE(abs)", "<resolve_failed>")

    # Sprint 2+: DB pinning diagnostics
    try:
        from db_path import describe_db_state

        st = describe_db_state()
        p("[DIAG] db_pin_file", st.get("pin_file"))
        p("[DIAG] db_pinned", st.get("pinned"))
        p("[DIAG] db_found", st.get("found"))
        p("[DIAG] db_duplicates", st.get("duplicates"))
    except Exception as e:
        p("[DIAG] db_pin_state", f"<unavailable: {e}>")

    # Print important cfg values (safe)
    keys = [
        "USE_TESTNET",
        "PAPER_TRADING",
        "ENABLE_LIVE_TRADING",
        "RUN_MODE",
        "BINANCE_FUTURES_REST_BASE",
        "BINANCE_FUTURES_WS_BASE",
        "WS_CHUNK_SIZE",
        "WS_CALLBACK_CONCURRENCY",
        "MAX_CONCURRENT_TRADES",
        "MAX_CONCURRENT_TASKS",
        "HEARTBEAT_INTERVAL",
        "DASHBOARD_URL",
        "DASHBOARD_PUBLISH_URL",
        "DASHBOARD_ENVELOPE",
        "BOT_NAME",
        "LOG_DIR",
    ]
    for k in keys:
        p(f"[DIAG] cfg.{k}", getattr(cfg, k, None))

    # Simple heuristic warnings
    try:
        use_testnet = bool(getattr(cfg, "USE_TESTNET", False))
        paper = bool(getattr(cfg, "PAPER_TRADING", False))
        live = bool(getattr(cfg, "ENABLE_LIVE_TRADING", False))
        if use_testnet and live and not paper:
            print("[DIAG] ⚠️ USE_TESTNET=True but PAPER_TRADING=False and ENABLE_LIVE_TRADING=True. Ensure this is intentional.")
    except Exception:
        pass

    try:
        ws_base = str(getattr(cfg, "BINANCE_FUTURES_WS_BASE", "") or "")
        if "stream.binancefuture.com" in ws_base:
            print("[DIAG] ⚠️ BINANCE_FUTURES_WS_BASE looks unusual. Recommended (testnet) = wss://fstream.binancefuture.com")
    except Exception:
        pass

    # ------------------------------------------------------------------
    # DB manager view of the path + DB-backed settings sample
    # ------------------------------------------------------------------
    db = None
    try:
        from database import DatabaseManager

        db = DatabaseManager(db_file=cfg.DB_FILE)
        p("[DIAG] DatabaseManager.db_file", getattr(db, "db_file", ""))
        p("[DIAG] settings_version", db.get_setting("__settings_version__"))

        sample_settings = [
            "mode",
            "kill_switch",
            "kill_switch_reason",
            "require_dashboard_approval",
            "auto_approve_live",
            "auto_approve_paper",
            "max_concurrent_trades",
            "cooldown_after_trade_sec",
            "cooldown_after_sl_sec",
            "inbox_dedupe_sec",
            "ensure_protection_orders",
            "ensure_protection_every_sec",
            "hard_tp_enabled",
            "exchange_trailing_enabled",
            "exchange_trailing_activation_roi",
            "exchange_trailing_callback_rate",
        ]
        for k in sample_settings:
            p(f"[DIAG] db.setting[{k}]", db.get_setting(k))

    except Exception as e:
        print("[DIAG] ❌ failed to init DatabaseManager:", e)

    # ------------------------------------------------------------------
    # SQLite checks (integrity + tables + counts)
    # ------------------------------------------------------------------
    try:
        import sqlite3

        db_path = Path(cfg.DB_FILE).expanduser().resolve()
        if not db_path.exists():
            print(f"[DIAG] ❌ DB file not found: {db_path}")
            return 3

        con = sqlite3.connect(str(db_path))
        cur = con.cursor()

        try:
            ok = cur.execute("PRAGMA integrity_check;").fetchone()
            p("[DIAG] integrity_check", ok[0] if ok else ok)
        except Exception as e:
            print("[DIAG] ❌ integrity_check failed:", e)

        tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
        p("[DIAG] tables_count", len(tables))

        must = [
            "settings",
            "settings_audit",
            "trades",
            "signal_inbox",
            "commands",
            "rejections",
            "logs",
            "equity_history",
            "trade_state",
            "decision_traces",
        ]
        missing = [t for t in must if t not in tables]
        if missing:
            print("[DIAG] ⚠️ missing tables:", missing)

        def count(tbl: str):
            try:
                return cur.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
            except Exception:
                return None

        for tbl in must:
            print(f"[DIAG] rows {tbl}: {count(tbl)}")

        # Extra sanity
        try:
            open_trades = cur.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN';").fetchone()[0]
            p("[DIAG] open_trades", open_trades)

            # warn if cap reached
            try:
                cap = 0
                if db is not None:
                    try:
                        cap = int(db.get_setting("max_concurrent_trades") or 0)
                    except Exception:
                        cap = 0
                if not cap:
                    cap = int(getattr(cfg, "MAX_CONCURRENT_TRADES", 0) or 0)
                if cap and open_trades >= cap:
                    print(f"[DIAG] ⚠️ open_trades ({open_trades}) >= max_concurrent_trades ({cap}). Bot may refuse new trades until you close some.")
            except Exception:
                pass
        except Exception:
            pass

        con.close()

    except Exception as e:
        print("[DIAG] ❌ DB check failed:", e)
        return 4

    # ------------------------------------------------------------------
    # Dashboard reachability + publish
    # ------------------------------------------------------------------
    try:
        import requests

        base = (getattr(cfg, "DASHBOARD_URL", "") or "http://127.0.0.1:8000").rstrip("/")
        pub = (getattr(cfg, "DASHBOARD_PUBLISH_URL", "") or (base + "/publish")).rstrip("/")
        token = (getattr(cfg, "DASHBOARD_PUBLISH_TOKEN", "") or "").strip()

        for path in ("/healthz", "/api/healthz", "/api/settings", "/api/signals"):
            url = base + path
            try:
                r = requests.get(url, timeout=2)
                print(f"[DIAG] dashboard GET {path}: {r.status_code}")
                if path in ("/api/settings", "/api/signals") and r.status_code == 401:
                    print("[DIAG] ℹ️ 401 is expected if you are not logged in (open /login in browser).")
            except Exception as e:
                print(f"[DIAG] dashboard GET {path}: FAILED ({e})")

        headers = {}
        if token:
            headers["x-dashboard-token"] = token
        payload = {"event": "DIAG", "timestamp": time.time(), "data": {"msg": "runtime_diag publish test"}}
        try:
            r = requests.post(pub, json=payload, headers=headers, timeout=2)
            print(f"[DIAG] dashboard POST /publish: {r.status_code}")
        except Exception as e:
            print(f"[DIAG] dashboard POST /publish: FAILED ({e})")

    except Exception as e:
        print("[DIAG] ⚠️ requests not available or dashboard unreachable:", e)

    print("[DIAG] ✅ done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
