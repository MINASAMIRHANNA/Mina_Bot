"""
project_doctor.py (v2)

- Robust diagnostics for the crypto_bot project (bot + dashboard + database).
- UTC everywhere.
- Dashboard check is tolerant to non-JSON /healthz (e.g., HTML redirect to /login).
- Safe maintenance actions are available via CLI or can be wired into the dashboard.

Usage:
  python project_doctor.py --dashboard-url http://localhost:8000
  python project_doctor.py --json
  python project_doctor.py --action prune_logs --param days=14
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


# ---------------------------------------------------------------------
# UTC helpers
# ---------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    dt = dt or utc_now()
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------
# Project root / imports
# ---------------------------------------------------------------------
THIS_FILE = Path(__file__).resolve()


def find_project_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "config.py").exists() and (p / "database.py").exists():
            return p
    return start


PROJECT_ROOT = find_project_root(THIS_FILE.parent)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import cfg  # type: ignore
except Exception:
    cfg = None  # type: ignore

try:
    from database import DatabaseManager  # type: ignore
except Exception:
    DatabaseManager = None  # type: ignore


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------
def resolve_db_path() -> Path:
    if cfg is not None and getattr(cfg, "DB_FILE", None):
        return Path(getattr(cfg, "DB_FILE")).expanduser()
    root_db = PROJECT_ROOT / "bot_data.db"
    if root_db.exists():
        return root_db

    dash_db = PROJECT_ROOT / "dashboard" / "bot_data.db"
    if dash_db.exists():
        return dash_db

    return root_db


def connect_db(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def ensure_min_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at TEXT,
          version INTEGER DEFAULT 0,
          source TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          timestamp TEXT,
          level TEXT,
          message TEXT,
          meta TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS commands (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          cmd TEXT NOT NULL,
          params TEXT,
          status TEXT DEFAULT 'PENDING',
          created_at TEXT,
          updated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_inbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at TEXT,
          status TEXT DEFAULT 'PENDING_APPROVAL',
          symbol TEXT,
          side TEXT,
          payload_json TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_status ON signal_inbox(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_created ON signal_inbox(created_at)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rejections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id INTEGER,
          created_at TEXT,
          symbol TEXT,
          reason TEXT,
          note TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          timestamp TEXT,
          total_balance REAL,
          unrealized_pnl REAL,
          source TEXT,
          meta TEXT,
          timestamp_ms INTEGER,
          date_utc TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_equity_history_ts ON equity_history(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_equity_history_date ON equity_history(date_utc)")
    con.commit()


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    r = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return r is not None


def safe_count(con: sqlite3.Connection, table: str) -> Optional[int]:
    if not table_exists(con, table):
        return None
    try:
        r = con.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        return int(r["c"] or 0)
    except Exception:
        return None


def safe_last_ts(con: sqlite3.Connection, table: str, col_candidates: List[str]) -> Optional[str]:
    if not table_exists(con, table):
        return None
    cols = [r["name"] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    col = next((c for c in col_candidates if c in cols), None)
    if not col:
        return None
    try:
        r = con.execute(f"SELECT {col} AS ts FROM {table} ORDER BY rowid DESC LIMIT 1").fetchone()
        if not r:
            return None
        return str(r["ts"]) if r["ts"] is not None else None
    except Exception:
        return None


def db_integrity_quick_check(con: sqlite3.Connection) -> Tuple[bool, str]:
    try:
        r = con.execute("PRAGMA quick_check").fetchone()
        if not r:
            return True, "ok"
        msg = str(r[0])
        return (msg.lower() == "ok"), msg
    except Exception as e:
        return False, f"error: {e}"


def db_wal_state(db_path: Path) -> Dict[str, Any]:
    return {
        "db": str(db_path),
        "exists": db_path.exists(),
        "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "wal_exists": (db_path.with_suffix(db_path.suffix + "-wal")).exists(),
        "shm_exists": (db_path.with_suffix(db_path.suffix + "-shm")).exists(),
    }


# ---------------------------------------------------------------------
# HTTP helpers (tolerant)
# ---------------------------------------------------------------------
def http_get(url: str, timeout: float = 2.5) -> Tuple[bool, int, str, str]:
    """
    Returns: ok, status_code, content_type, text_snippet
    """
    if requests is None:
        return False, 0, "", "requests not installed"
    try:
        r = requests.get(url, timeout=timeout, headers={"Accept": "application/json, text/plain, */*"})
        ct = (r.headers.get("content-type") or "").lower()
        txt = (r.text or "").strip()
        return (r.status_code < 400), r.status_code, ct, txt[:240]
    except Exception as e:
        return False, 0, "", str(e)


def dashboard_check(base_url: str) -> Dict[str, Any]:
    base_url = base_url.rstrip("/")
    ok, code, ct, snippet = http_get(f"{base_url}/healthz")
    data: Optional[Dict[str, Any]] = None
    json_ok = False

    if ok and ("application/json" in ct):
        try:
            # parse again with requests .json if possible
            if requests is not None:
                rr = requests.get(f"{base_url}/healthz", timeout=2.5)
                data = rr.json()
                json_ok = True
        except Exception:
            json_ok = False

    # If ok but not JSON, it's usually a redirect/login HTML or plain text
    return {
        "base_url": base_url,
        "reachable": bool(ok),
        "status_code": code,
        "content_type": ct,
        "json_ok": json_ok,
        "healthz": data,
        "snippet": snippet if not json_ok else None,
        "hint": (
            "If this is HTML/redirect, make sure /healthz is PUBLIC in dashboard/app.py and returns JSON."
            if ok and not json_ok else None
        ),
    }


def binance_public_check() -> Dict[str, Any]:
    if requests is None:
        return {"ok": False, "message": "requests not installed"}
    try:
        r1 = requests.get("https://api.binance.com/api/v3/ping", timeout=2.5)
        spot_ok = (r1.status_code == 200)
    except Exception as e:
        return {"ok": False, "spot_ok": False, "message": str(e)}
    try:
        r2 = requests.get("https://fapi.binance.com/fapi/v1/ping", timeout=2.5)
        fut_ok = (r2.status_code == 200)
    except Exception:
        fut_ok = False
    return {"ok": bool(spot_ok and fut_ok), "spot_ok": spot_ok, "futures_ok": fut_ok, "message": "ok" if (spot_ok or fut_ok) else "failed"}


# ---------------------------------------------------------------------
# Status inference
# ---------------------------------------------------------------------
def infer_bot_liveness(db: Dict[str, Any]) -> Dict[str, Any]:
    now = utc_now()

    def parse_any(ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            s = str(ts)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            try:
                x = float(ts)
                if x > 10_000_000_000:
                    return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(x, tz=timezone.utc)
            except Exception:
                return None

    last_log = parse_any(db.get("last_log_time"))
    last_snap = parse_any(db.get("last_snapshot_time"))

    threshold_sec = 180
    indicators = []
    alive = False

    if last_log:
        age = int((now - last_log).total_seconds())
        indicators.append({"name": "logs", "last": utc_iso(last_log), "age_sec": age})
        if age <= threshold_sec:
            alive = True

    if last_snap:
        age = int((now - last_snap).total_seconds())
        indicators.append({"name": "equity_snapshot", "last": utc_iso(last_snap), "age_sec": age})
        if age <= threshold_sec:
            alive = True

    return {"alive_inferred": alive, "threshold_sec": threshold_sec, "indicators": indicators}


# ---------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------
def collect_status(
    dashboard_url: Optional[str] = None,
    include_sensitive: bool = False,
    check_binance: bool = True,
) -> Dict[str, Any]:
    status: Dict[str, Any] = {}
    status["time_utc"] = utc_iso()
    status["project_root"] = str(PROJECT_ROOT)

    status["runtime"] = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "cwd": os.getcwd(),
    }

    cfg_summary: Dict[str, Any] = {}
    if cfg is not None:
        for k in [
            "TIMEZONE",
            "DB_FILE",
            "USE_TESTNET",
            "PAPER_TRADING",
            "SNAPSHOT_ENABLED",
            "EQUITY_SNAPSHOT_EVERY_SEC",
            "SNAPSHOT_RETENTION_DAYS",
            "REQUIRE_DASHBOARD_APPROVAL",
            "AUTO_APPROVE_PAPER",
            "AUTO_APPROVE_LIVE",
            "WS_CHUNK_SIZE",
            "MAX_CONCURRENT_TRADES",
            "MAX_CONCURRENT_TASKS",
            "LOG_LEVEL",
        ]:
            if hasattr(cfg, k):
                cfg_summary[k] = getattr(cfg, k)

        for sk in ["BINANCE_API_KEY", "BINANCE_API_SECRET", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
            if hasattr(cfg, sk):
                v = getattr(cfg, sk)
                cfg_summary[sk] = (v if include_sensitive else ("SET" if v else "MISSING"))
    else:
        cfg_summary["loaded"] = False
    status["config"] = cfg_summary

    db_path = resolve_db_path()
    db: Dict[str, Any] = {}
    db.update(db_wal_state(db_path))

    candidates = [
        PROJECT_ROOT / "bot_data.db",
        PROJECT_ROOT / "dashboard" / "bot_data.db",
        db_path,
    ]
    uniq: List[Path] = []
    for p in candidates:
        if p not in uniq:
            uniq.append(p)
    db["known_db_files"] = [{"path": str(p), "exists": p.exists(), "size_bytes": p.stat().st_size if p.exists() else 0} for p in uniq]

    db["integrity_ok"] = None
    db["integrity_message"] = "not_checked"

    if db_path.exists():
        try:
            con = connect_db(db_path)
            ensure_min_schema(con)

            ok, msg = db_integrity_quick_check(con)
            db["integrity_ok"] = ok
            db["integrity_message"] = msg

            db["counts"] = {
                "logs": safe_count(con, "logs"),
                "trades": safe_count(con, "trades"),
                "equity_history": safe_count(con, "equity_history"),
                "signal_inbox": safe_count(con, "signal_inbox"),
                "commands": safe_count(con, "commands"),
                "rejections": safe_count(con, "rejections"),
            }

            db["last_log_time"] = safe_last_ts(con, "logs", ["timestamp", "time", "created_at"])
            db["last_snapshot_time"] = safe_last_ts(con, "equity_history", ["timestamp", "created_at"])
            db["last_signal_time"] = safe_last_ts(con, "signal_inbox", ["created_at"])
            db["last_command_time"] = safe_last_ts(con, "commands", ["created_at", "updated_at"])

            try:
                rows = con.execute("SELECT key, value, updated_at FROM settings").fetchall()
                db["settings_count"] = len(rows)
                keys = {"kill_switch", "mode", "require_dashboard_approval", "auto_approve_live", "auto_approve_paper", "paper_daily_enabled"}
                db["settings_ops"] = {r["key"]: r["value"] for r in rows if r["key"] in keys}
            except Exception:
                db["settings_count"] = None
                db["settings_ops"] = {}

            con.close()
        except Exception as e:
            db["error"] = str(e)
    else:
        db["error"] = "database file not found"

    status["database"] = db
    status["bot"] = infer_bot_liveness(db)

    if dashboard_url:
        status["dashboard"] = dashboard_check(dashboard_url)
    else:
        status["dashboard"] = {"reachable": None, "message": "not checked (no url provided)"}

    if check_binance:
        status["binance"] = binance_public_check()
    else:
        status["binance"] = {"ok": None, "message": "skipped"}

    problems: List[str] = []
    if not db.get("exists"):
        problems.append("DB file missing")
    if db.get("exists") and db.get("integrity_ok") is False:
        problems.append(f"DB integrity not ok: {db.get('integrity_message')}")

    dash = status.get("dashboard", {})
    if dashboard_url and dash.get("reachable") is False:
        problems.append("Dashboard not reachable")
    elif dashboard_url and dash.get("reachable") is True and dash.get("json_ok") is False:
        problems.append("Dashboard reachable but /healthz is not JSON (likely redirect/login or old app.py)")

    if status.get("binance", {}).get("ok") is False:
        problems.append("Binance ping failed")

    status["summary"] = {"ok": len(problems) == 0, "problems": problems}
    return status


# ---------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------
@dataclass
class MaintenanceResult:
    ok: bool
    message: str
    details: Dict[str, Any]


def _parse_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _db_execute(db_path: Path, sql: str, params: Tuple[Any, ...] = ()) -> int:
    con = connect_db(db_path)
    ensure_min_schema(con)
    cur = con.execute(sql, params)
    con.commit()
    n = cur.rowcount if cur.rowcount is not None else 0
    con.close()
    return n


def insert_command(cmd: str, params: Optional[Dict[str, Any]] = None) -> int:
    db_path = resolve_db_path()
    con = connect_db(db_path)
    ensure_min_schema(con)
    now = utc_iso()
    cur = con.execute(
        "INSERT INTO commands(cmd,params,status,created_at,updated_at) VALUES(?,?,?,?,?)",
        (cmd, json.dumps(params or {}, ensure_ascii=False), "PENDING", now, now),
    )
    con.commit()
    cid = int(cur.lastrowid)
    con.close()
    return cid


def set_setting(key: str, value: str, source: str = "ops") -> None:
    db_path = resolve_db_path()
    con = connect_db(db_path)
    ensure_min_schema(con)
    now = utc_iso()
    con.execute(
        """INSERT INTO settings(key,value,updated_at,version,source)
           VALUES(?,?,?,?,?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at, version=version+1, source=excluded.source
        """,
        (key, value, now, 0, source),
    )
    con.commit()
    con.close()


def run_maintenance(action: str, **kwargs: Any) -> MaintenanceResult:
    db_path = resolve_db_path()
    try:
        action = (action or "").strip().lower()

        if action == "prune_equity_history":
            days = _parse_int(kwargs.get("days"), 30)
            cutoff = (utc_now() - timedelta(days=days)).date().isoformat()
            n1 = 0
            try:
                n1 = _db_execute(db_path, "DELETE FROM equity_history WHERE date_utc IS NOT NULL AND date_utc < ?", (cutoff,))
            except Exception:
                n1 = 0
            cutoff_iso = utc_iso(utc_now() - timedelta(days=days))
            n2 = 0
            try:
                n2 = _db_execute(db_path, "DELETE FROM equity_history WHERE timestamp IS NOT NULL AND timestamp < ?", (cutoff_iso,))
            except Exception:
                n2 = 0
            return MaintenanceResult(True, f"Pruned equity_history older than {days} days", {"deleted": n1 + n2})

        if action == "prune_logs":
            days = _parse_int(kwargs.get("days"), 14)
            cutoff_iso = utc_iso(utc_now() - timedelta(days=days))
            n = _db_execute(db_path, "DELETE FROM logs WHERE timestamp IS NOT NULL AND timestamp < ?", (cutoff_iso,))
            return MaintenanceResult(True, f"Pruned logs older than {days} days", {"deleted": n})

        if action == "prune_signals":
            keep_last = _parse_int(kwargs.get("keep_last"), 500)
            con = connect_db(db_path)
            ensure_min_schema(con)
            r = con.execute("SELECT id FROM signal_inbox ORDER BY id DESC LIMIT 1 OFFSET ?", (keep_last,)).fetchone()
            if not r:
                con.close()
                return MaintenanceResult(True, "No signals to prune", {"deleted": 0})
            cutoff_id = int(r["id"])
            cur = con.execute("DELETE FROM signal_inbox WHERE id <= ?", (cutoff_id,))
            con.commit()
            n = cur.rowcount if cur.rowcount is not None else 0
            con.close()
            return MaintenanceResult(True, f"Pruned signals (kept last {keep_last})", {"deleted": n, "cutoff_id": cutoff_id})

        if action == "clear_commands":
            days = _parse_int(kwargs.get("days"), 7)
            cutoff_iso = utc_iso(utc_now() - timedelta(days=days))
            n = _db_execute(
                db_path,
                "DELETE FROM commands WHERE status IN ('DONE','FAILED','CANCELLED') AND updated_at IS NOT NULL AND updated_at < ?",
                (cutoff_iso,),
            )
            return MaintenanceResult(True, f"Cleared finished commands older than {days} days", {"deleted": n})

        if action == "reset_kill_switch":
            set_setting("kill_switch", "0", source="ops")
            return MaintenanceResult(True, "kill_switch reset to 0", {})

        if action == "send_command":
            cmd = str(kwargs.get("cmd", "")).strip().upper()
            if not cmd:
                return MaintenanceResult(False, "cmd is required", {})
            params = kwargs.get("params")
            if isinstance(params, str):
                try:
                    params = json.loads(params)
                except Exception:
                    params = {"raw": params}
            if not isinstance(params, dict):
                params = {}
            cid = insert_command(cmd, params)
            return MaintenanceResult(True, f"Command queued: {cmd}", {"command_id": cid})

        if action == "vacuum_db":
            con = connect_db(db_path)
            con.execute("VACUUM")
            con.close()
            return MaintenanceResult(True, "VACUUM completed", {})

        return MaintenanceResult(False, f"Unknown action: {action}", {"supported": [
            "prune_equity_history","prune_logs","prune_signals","clear_commands","reset_kill_switch","send_command","vacuum_db"
        ]})

    except Exception as e:
        return MaintenanceResult(False, str(e), {})


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Crypto Bot Project Doctor (status + safe maintenance)")
    ap.add_argument("--dashboard-url", default=(getattr(cfg, "DASHBOARD_URL", "") if cfg is not None else ""), help="e.g. http://localhost:8000")
    ap.add_argument("--json", action="store_true", help="print JSON")
    ap.add_argument("--no-binance", action="store_true", help="skip binance ping")
    ap.add_argument("--include-sensitive", action="store_true", help="include sensitive values (unsafe)")
    ap.add_argument("--action", default="", help="maintenance action (e.g. prune_logs)")
    ap.add_argument("--param", action="append", default=[], help="maintenance param key=value (repeatable)")
    args = ap.parse_args()

    if args.action:
        kwargs: Dict[str, Any] = {}
        for kv in args.param:
            if "=" in kv:
                k, v = kv.split("=", 1)
                kwargs[k.strip()] = v.strip()
        res = run_maintenance(args.action, **kwargs)
        out = {"ok": res.ok, "message": res.message, "details": res.details}
        print(json.dumps(out, indent=2, ensure_ascii=False))
        sys.exit(0 if res.ok else 2)

    dash = args.dashboard_url.strip() or None
    st = collect_status(
        dashboard_url=dash,
        include_sensitive=args.include_sensitive,
        check_binance=not args.no_binance
    )

    if args.json:
        print(json.dumps(st, indent=2, ensure_ascii=False))
        return

    # Pretty output
    print(f"[ProjectDoctor] time_utc={st['time_utc']}")
    print(f"  project_root: {st['project_root']}")
    db = st.get("database", {})
    print(f"  db: {db.get('db')} (exists={db.get('exists')}, size={db.get('size_bytes')})")
    print(f"  db integrity: {db.get('integrity_message')}")
    print(f"  bot alive (inferred): {st.get('bot', {}).get('alive_inferred')} (last_log={db.get('last_log_time')}, last_snapshot={db.get('last_snapshot_time')})")
    print(f"  dashboard: {st.get('dashboard')}")
    print(f"  binance: {st.get('binance')}")
    if st.get("summary", {}).get("ok"):
        print("  ✅ summary: OK")
    else:
        print("  ❌ summary problems:")
        for p in st.get("summary", {}).get("problems", []):
            print("   -", p)


if __name__ == "__main__":
    main()
