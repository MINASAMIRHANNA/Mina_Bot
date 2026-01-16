#!/usr/bin/env python3

"""
runtime_diag.py — تشخيص تشغيل فعلي (Server واحد)

يشخّص:
- .env path اللي اتقري فعلاً (من config.py)
- DB_FILE (raw + abs) للبوت
- Integrity check للـ SQLite
- وجود الجداول الأساسية وعدد rows
- فحص وصول الداشبورد (/healthz + /api/settings + /api/signals)
- اختبار publish (best-effort)

شغّله من فولدر Crypto_bot:
  python tools/runtime_diag.py

نصيحة: شغّل الداشبورد أولاً ثم شغّل البوت، وبعدها شغّل السكربت.
"""
import os, sys, json, time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

def p(label, value):
    print(f"{label}: {value}")

def main():
    os.chdir(str(PROJECT_ROOT))
    p("[DIAG] cwd", os.getcwd())

    # config / env
    try:
        import config as config_mod
        from config import cfg
        p("[DIAG] config.py", str(Path(config_mod.__file__).resolve()))
        p("[DIAG] dotenv_path", getattr(config_mod, "_DOTENV_PATH", None))
        p("[DIAG] DOTENV_OVERRIDE", os.getenv("DOTENV_OVERRIDE"))
        p("[DIAG] DB_FILE(raw)", cfg.DB_FILE)
        p("[DIAG] DB_FILE(abs)", str(Path(cfg.DB_FILE).expanduser().resolve()))
        p("[DIAG] LOG_DIR", cfg.LOG_DIR)
        p("[DIAG] DASHBOARD_URL", os.getenv("DASHBOARD_URL", ""))
        p("[DIAG] DASHBOARD_PUBLISH_URL", os.getenv("DASHBOARD_PUBLISH_URL", ""))
    except Exception as e:
        print("[DIAG] ❌ failed to import config:", e)
        return 2

    # DB checks
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

        # tables
        tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
        p("[DIAG] tables_count", len(tables))
        must = ["settings","settings_audit","trades","signal_inbox","commands","rejections","equity_history","pump_candidates"]
        missing = [t for t in must if t not in tables]
        if missing:
            print("[DIAG] ⚠️ missing tables:", missing)

        def count(tbl):
            try:
                return cur.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
            except Exception:
                return None

        for tbl in must:
            c = count(tbl)
            print(f"[DIAG] rows {tbl}: {c}")

        # check mismatch patterns
        try:
            open_wait = cur.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND signal='WAIT';").fetchone()[0]
            p("[DIAG] open_trades_with_WAIT", open_wait)
        except Exception:
            pass

        con.close()
    except Exception as e:
        print("[DIAG] ❌ DB check failed:", e)
        return 4

    # Dashboard reachability (best-effort)
    try:
        import requests
        base = os.getenv("DASHBOARD_URL", "http://127.0.0.1:8000").rstrip("/")
        for path in ("/healthz","/api/healthz","/api/settings","/api/signals"):
            url = base + path
            try:
                r = requests.get(url, timeout=2)
                print(f"[DIAG] dashboard GET {path}: {r.status_code}")
            except Exception as e:
                print(f"[DIAG] dashboard GET {path}: FAILED ({e})")

        # publish test (best-effort; might require auth)
        pub = os.getenv("DASHBOARD_PUBLISH_URL", base + "/publish")
        token = os.getenv("DASHBOARD_PUBLISH_TOKEN","").strip()
        headers = {}
        if token:
            headers["x-dashboard-token"] = token
        try:
            payload = {"event":"DIAG","timestamp":time.time(),"data":{"msg":"runtime_diag publish test"}}
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
