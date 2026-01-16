#!/usr/bin/env python3
"""
db_doctor.py - Crypto Bot SQLite Diagnostics (Zero-ENV friendly)

Usage:
  python db_doctor.py --db "/path/to/bot_data.db"
  python db_doctor.py --db "./bot_data.db" --out "./db_report" --samples 25

Outputs:
  - report.md (human readable)
  - summary.json (structured)
  - tables.csv (table stats)
  - samples/*.csv (small safe samples)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


SENSITIVE_KEY_RE = re.compile(r"(api[_-]?key|secret|token|password|pass|private|key_present)", re.I)
LIKELY_SENSITIVE_VALUE_RE = re.compile(r"^[A-Za-z0-9_\-]{20,}$")  # heuristic


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_iso(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_maybe(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        # interpret as unix seconds if small, ms if large
        v = float(s)
        if v > 1e12:  # ms
            return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
        if v > 1e9:  # seconds
            return datetime.fromtimestamp(v, tz=timezone.utc)
        return None
    if not isinstance(s, str):
        return None

    t = s.strip()
    if not t:
        return None

    # Normalize Z
    t = t.replace("Z", "+00:00")

    # Some values look like "2026-01-14T13:08:00+00:00"
    try:
        return datetime.fromisoformat(t).astimezone(timezone.utc)
    except Exception:
        return None


def age_seconds(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    return int((utc_now() - dt).total_seconds())


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def table_columns(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [dict(r) for r in rows]


def has_column(cols: List[Dict[str, Any]], name: str) -> bool:
    return any((c.get("name") == name) for c in cols)


def get_row_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])
    except Exception:
        return -1


def pick_time_columns(cols: List[Dict[str, Any]]) -> List[str]:
    time_cols = []
    for c in cols:
        n = (c.get("name") or "").lower()
        if any(k in n for k in ["created", "updated", "time", "utc", "ts", "date", "heartbeat", "last_"]):
            time_cols.append(c["name"])
    return time_cols[:12]  # cap


def min_max_for_column(conn: sqlite3.Connection, table: str, col: str) -> Tuple[Any, Any]:
    try:
        row = conn.execute(f"SELECT MIN({col}) AS mn, MAX({col}) AS mx FROM {table}").fetchone()
        return row["mn"], row["mx"]
    except Exception:
        return None, None


def safe_write_csv(path: Path, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in headers})


def redact_setting(key: str, value: Any) -> Any:
    if key is None:
        return value
    k = str(key)
    if SENSITIVE_KEY_RE.search(k):
        if value is None:
            return ""
        v = str(value)
        # show only presence / tail
        if len(v) <= 6:
            return "***"
        return f"***{v[-4:]}"  # keep last 4 chars
    return value


def fetch_settings_map(conn: sqlite3.Connection) -> Dict[str, Any]:
    # try common shapes
    for tbl in ["settings", "bot_settings"]:
        if tbl in list_tables(conn):
            cols = table_columns(conn, tbl)
            if has_column(cols, "key") and has_column(cols, "value"):
                rows = conn.execute(f"SELECT key, value FROM {tbl}").fetchall()
                out = {}
                for r in rows:
                    out[str(r["key"])] = r["value"]
                return out
    return {}


def fetch_recent_rows(conn: sqlite3.Connection, table: str, limit: int = 20) -> List[Dict[str, Any]]:
    cols = table_columns(conn, table)
    colnames = [c["name"] for c in cols]
    order_col = None
    for candidate in ["id", "created_at", "created_utc", "ts_utc", "updated_utc", "created_at_ms"]:
        if candidate in colnames:
            order_col = candidate
            break
    sql = f"SELECT * FROM {table}"
    if order_col:
        sql += f" ORDER BY {order_col} DESC"
    sql += f" LIMIT {int(limit)}"
    try:
        rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def find_table_by_columns(conn: sqlite3.Connection, required: List[str]) -> Optional[str]:
    required_set = set(required)
    for t in list_tables(conn):
        cols = [c["name"] for c in table_columns(conn, t)]
        if required_set.issubset(set(cols)):
            return t
    return None


@dataclass
class ComponentHealth:
    name: str
    ok: bool
    last_dt: Optional[datetime]
    age_sec: Optional[int]
    note: str = ""


def component_from_settings(settings: Dict[str, Any], key: str, name: str) -> ComponentHealth:
    v = settings.get(key)
    dt = parse_iso_maybe(v)
    age = age_seconds(dt)
    ok = age is not None and age < 120  # 2 minutes default
    return ComponentHealth(name=name, ok=ok, last_dt=dt, age_sec=age, note=f"from settings[{key}]")


def component_from_pump_state(conn: sqlite3.Connection) -> ComponentHealth:
    if "pump_hunter_state" not in list_tables(conn):
        return ComponentHealth("pump_hunter", False, None, None, "missing table pump_hunter_state")
    try:
        row = conn.execute("SELECT last_heartbeat_ms, last_scan_ms, last_scan_count, last_error FROM pump_hunter_state WHERE id=1").fetchone()
        if not row:
            return ComponentHealth("pump_hunter", False, None, None, "no row id=1")
        dt = parse_iso_maybe(row["last_heartbeat_ms"])
        age = age_seconds(dt)
        ok = age is not None and age < 120
        note = f"last_scan_count={row['last_scan_count']} last_error={row['last_error']}"
        return ComponentHealth("pump_hunter", ok, dt, age, note)
    except Exception as e:
        return ComponentHealth("pump_hunter", False, None, None, f"error reading pump_hunter_state: {e}")


def analyze_commands(conn: sqlite3.Connection) -> Dict[str, Any]:
    tbl = "commands" if "commands" in list_tables(conn) else None
    if not tbl:
        return {"present": False, "note": "missing commands table"}
    cols = [c["name"] for c in table_columns(conn, tbl)]
    status_col = "status" if "status" in cols else None
    created_col = "created_at" if "created_at" in cols else ("created_utc" if "created_utc" in cols else None)
    out: Dict[str, Any] = {"present": True, "counts": {}, "oldest_pending_age_sec": None, "recent_errors": []}

    if status_col:
        rows = conn.execute(f"SELECT {status_col} AS s, COUNT(*) AS c FROM {tbl} GROUP BY {status_col}").fetchall()
        out["counts"] = {str(r["s"]): int(r["c"]) for r in rows}

        # oldest pending
        if created_col:
            r = conn.execute(
                f"SELECT MIN({created_col}) AS mn FROM {tbl} WHERE {status_col} IN ('PENDING','NEW','QUEUED')"
            ).fetchone()
            dt = parse_iso_maybe(r["mn"])
            out["oldest_pending_age_sec"] = age_seconds(dt)

        # recent errors
        if "error" in cols:
            errs = conn.execute(
                f"SELECT id, cmd, {status_col} AS status, error, {created_col} AS created FROM {tbl} "
                f"WHERE {status_col} IN ('ERROR','FAILED') ORDER BY id DESC LIMIT 10"
            ).fetchall()
            out["recent_errors"] = [dict(e) for e in errs]
    else:
        out["note"] = "commands table has no status column"
    return out


def analyze_trades(conn: sqlite3.Connection) -> Dict[str, Any]:
    # Try to detect trade table
    trade_tbl = None
    for name_guess in ["trades", "trade_log", "open_trades", "paper_trades"]:
        if name_guess in list_tables(conn):
            trade_tbl = name_guess
            break
    if not trade_tbl:
        trade_tbl = find_table_by_columns(conn, ["symbol", "status"])  # fallback

    if not trade_tbl:
        return {"present": False, "note": "could not find trades-like table"}

    cols = [c["name"] for c in table_columns(conn, trade_tbl)]
    out: Dict[str, Any] = {"present": True, "table": trade_tbl, "counts": {}, "data_issues": []}

    # status breakdown
    if "status" in cols:
        rows = conn.execute(f"SELECT status AS s, COUNT(*) AS c FROM {trade_tbl} GROUP BY status").fetchall()
        out["counts"]["by_status"] = {str(r["s"]): int(r["c"]) for r in rows}

    # open trades issues
    where_open = "status='OPEN'" if "status" in cols else "1=1"
    issue_checks = []
    if "side" in cols:
        issue_checks.append(("missing_side", f"SELECT COUNT(*) c FROM {trade_tbl} WHERE {where_open} AND (side IS NULL OR TRIM(side)='')"))
    if "qty" in cols:
        issue_checks.append(("missing_qty", f"SELECT COUNT(*) c FROM {trade_tbl} WHERE {where_open} AND (qty IS NULL OR qty=0)"))
    if "stop_loss" in cols:
        issue_checks.append(("missing_sl", f"SELECT COUNT(*) c FROM {trade_tbl} WHERE {where_open} AND (stop_loss IS NULL OR stop_loss=0)"))
    if "opened_utc" in cols:
        issue_checks.append(("missing_opened_utc", f"SELECT COUNT(*) c FROM {trade_tbl} WHERE {where_open} AND (opened_utc IS NULL OR TRIM(opened_utc)='')"))
    if "closed_utc" in cols:
        issue_checks.append(("closed_missing_closed_utc", f"SELECT COUNT(*) c FROM {trade_tbl} WHERE status='CLOSED' AND (closed_utc IS NULL OR TRIM(closed_utc)='')"))

    for name, sql in issue_checks:
        try:
            c = int(conn.execute(sql).fetchone()["c"])
            if c:
                out["data_issues"].append({"issue": name, "count": c})
        except Exception:
            pass

    # sample open trades
    try:
        sample = conn.execute(f"SELECT * FROM {trade_tbl} WHERE {where_open} ORDER BY id DESC LIMIT 10").fetchall()
        out["open_sample"] = [dict(r) for r in sample]
    except Exception:
        out["open_sample"] = []

    return out


def analyze_learning(conn: sqlite3.Connection, settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Heuristic learning/AI usage check:
      - is enable_learning TRUE?
      - does decision_traces show ai_vote != OFF and ai_confidence > 0?
      - does analysis_results get populated (paper eval)?
      - do model files exist? (via settings keys if present)
    """
    out: Dict[str, Any] = {"learning_enabled_setting": None, "ai_usage": {}, "paper_eval": {}, "models": {}, "notes": []}

    # learning flag
    for k in ["enable_learning", "learning_enabled"]:
        if k in settings:
            out["learning_enabled_setting"] = str(settings.get(k))
            break

    # decision traces
    dt_tbl = None
    for guess in ["decision_traces", "decision_trace", "decision_log"]:
        if guess in list_tables(conn):
            dt_tbl = guess
            break
    if dt_tbl:
        cols = [c["name"] for c in table_columns(conn, dt_tbl)]
        # compute distributions from last N
        rows = fetch_recent_rows(conn, dt_tbl, limit=500)
        ai_vote_on = 0
        ai_conf_pos = 0
        total = len(rows)
        gate_counts: Dict[str, int] = {}
        decision_counts: Dict[str, int] = {}
        strategy_counts: Dict[str, int] = {}
        for r in rows:
            gate = str(r.get("gate", ""))
            decision = str(r.get("decision", ""))
            strategy = str(r.get("strategy", "")) or str(r.get("strategy_tag", ""))
            gate_counts[gate] = gate_counts.get(gate, 0) + 1
            decision_counts[decision] = decision_counts.get(decision, 0) + 1
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

            av = str(r.get("ai_vote", ""))
            if av and av.upper() != "OFF":
                ai_vote_on += 1
            try:
                ac = float(r.get("ai_confidence") or 0.0)
                if ac > 0.0:
                    ai_conf_pos += 1
            except Exception:
                pass

        out["ai_usage"] = {
            "decision_traces_table": dt_tbl,
            "sample_size": total,
            "ai_vote_non_off": ai_vote_on,
            "ai_confidence_pos": ai_conf_pos,
            "gate_top": sorted(gate_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            "decision_top": sorted(decision_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            "strategy_top": sorted(strategy_counts.items(), key=lambda x: x[1], reverse=True)[:10],
        }

        if total > 0 and ai_vote_on == 0 and ai_conf_pos == 0:
            out["notes"].append("AI يبدو غير مستخدم في decision_traces (ai_vote OFF و ai_confidence=0 في العينة).")
    else:
        out["notes"].append("No decision_traces table found (لا يمكن تقييم استعمال AI من traces).")

    # paper eval / analysis_results
    ar_tbl = "analysis_results" if "analysis_results" in list_tables(conn) else None
    if ar_tbl:
        cnt = get_row_count(conn, ar_tbl)
        cols = [c["name"] for c in table_columns(conn, ar_tbl)]
        # last time
        time_col = None
        for c in ["created_at", "created_utc", "ts_utc", "updated_utc"]:
            if c in cols:
                time_col = c
                break
        mn, mx = (None, None)
        if time_col:
            mn, mx = min_max_for_column(conn, ar_tbl, time_col)
        out["paper_eval"] = {
            "analysis_results_count": cnt,
            "time_col": time_col,
            "min": mn,
            "max": mx,
        }
        if cnt == 0:
            out["notes"].append("analysis_results موجود لكن فاضي: Paper evaluation/tournament قد لا يكون شغال أو لم يكتب نتائج بعد.")
    else:
        out["notes"].append("No analysis_results table found (Paper evaluation قد يكون غير مفعل أو schema مختلف).")

    # models status in settings (common key)
    for k in ["models_status", "model_status", "models_state"]:
        if k in settings:
            v = settings.get(k)
            try:
                js = json.loads(v) if isinstance(v, str) else v
                out["models"] = js
            except Exception:
                out["models"] = {"raw": str(v)[:500]}
            break

    return out


def analyze_protection_audit(settings: Dict[str, Any]) -> Dict[str, Any]:
    # tries to parse protection_audit_status key (common in your build)
    out = {"present": False}
    for k in ["protection_audit_status", "protection_audit", "protection_status"]:
        if k in settings and settings.get(k):
            out["present"] = True
            out["key"] = k
            raw = settings.get(k)
            try:
                js = json.loads(raw) if isinstance(raw, str) else raw
                issues = js.get("issues", {}) if isinstance(js, dict) else {}
                out["counts"] = {
                    "missing_sl": len(issues.get("missing_sl", []) or []),
                    "missing_tp": len(issues.get("missing_tp", []) or []),
                    "missing_trail": len(issues.get("missing_trail", []) or []),
                    "orphan_orders": len(issues.get("orphan_orders", []) or []),
                    "duplicates": len(issues.get("duplicates", []) or []),
                }
                out["updated_utc"] = js.get("updated_utc", "")
            except Exception:
                out["raw_prefix"] = str(raw)[:400]
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./bot_data.db", help="Path to bot_data.db")
    ap.add_argument("--out", default="", help="Output dir (default: ./db_diagnostics_<timestamp>)")
    ap.add_argument("--samples", type=int, default=20, help="Sample rows per critical table")
    args = ap.parse_args()

    db_path = os.path.abspath(args.db)
    if not os.path.exists(db_path):
        print(f"[ERROR] DB not found: {db_path}")
        return 2

    out_dir = Path(args.out.strip()) if args.out.strip() else Path(f"./db_diagnostics_{utc_now().strftime('%Y%m%d_%H%M%S')}")
    ensure_dir(out_dir)
    ensure_dir(out_dir / "samples")

    # DB file stats
    st = os.stat(db_path)
    db_info = {
        "db_path": db_path,
        "size_bytes": st.st_size,
        "mtime_utc": safe_iso(datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)),
        "generated_utc": safe_iso(utc_now()),
    }

    conn = connect(db_path)
    tables = list_tables(conn)

    # table stats
    table_stats: List[Dict[str, Any]] = []
    for t in tables:
        cols = table_columns(conn, t)
        rc = get_row_count(conn, t)
        time_cols = pick_time_columns(cols)
        time_ranges = []
        for c in time_cols:
            mn, mx = min_max_for_column(conn, t, c)
            # keep raw min/max, also try parse age
            mx_dt = parse_iso_maybe(mx)
            time_ranges.append({
                "col": c,
                "min": mn,
                "max": mx,
                "max_age_sec": age_seconds(mx_dt) if mx_dt else None,
            })

        table_stats.append({
            "table": t,
            "rows": rc,
            "cols": len(cols),
            "time_ranges": time_ranges[:5],
        })

    # write tables.csv
    with (out_dir / "tables.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["table", "rows", "cols"])
        for s in table_stats:
            w.writerow([s["table"], s["rows"], s["cols"]])

    settings = fetch_settings_map(conn)

    # redact settings for output
    settings_redacted = {k: redact_setting(k, v) for k, v in settings.items()}

    # component health
    comps: List[ComponentHealth] = []
    # common heartbeats
    for k, name in [
        ("bot_heartbeat", "bot"),
        ("execution_monitor_heartbeat", "execution_monitor"),
        ("risk_supervisor_heartbeat", "risk_supervisor"),
        ("data_manager_heartbeat", "data_manager"),
    ]:
        if k in settings:
            comps.append(component_from_settings(settings, k, name))
    # pump hunter from table if available
    comps.append(component_from_pump_state(conn))

    commands_info = analyze_commands(conn)
    trades_info = analyze_trades(conn)
    learning_info = analyze_learning(conn, settings)
    protection_info = analyze_protection_audit(settings)

    # Critical table presence check
    critical_tables = [
        "settings", "commands", "signal_inbox", "decision_traces", "analysis_results",
        "pump_hunter_state", "pump_candidates", "positions_status", "logs"
    ]
    presence = {t: (t in tables) for t in critical_tables}

    # Export safe samples for key tables
    for t in ["commands", "signal_inbox", "positions_status", "pump_candidates", "pump_hunter_state"]:
        if t in tables:
            rows = fetch_recent_rows(conn, t, limit=args.samples)
            if rows:
                headers = list(rows[0].keys())
                safe_write_csv(out_dir / "samples" / f"{t}.csv", headers, rows)

    # settings sample (redacted)
    if settings:
        rows = [{"key": k, "value": settings_redacted.get(k, "")} for k in sorted(settings_redacted.keys())]
        safe_write_csv(out_dir / "samples" / "settings_redacted.csv", ["key", "value"], rows)

    # decision traces small sample (limit)
    dt_tbl = None
    for guess in ["decision_traces", "decision_trace", "decision_log"]:
        if guess in tables:
            dt_tbl = guess
            break
    if dt_tbl:
        rows = fetch_recent_rows(conn, dt_tbl, limit=min(args.samples, 50))
        if rows:
            headers = list(rows[0].keys())
            safe_write_csv(out_dir / "samples" / f"{dt_tbl}_sample.csv", headers, rows)

    # Compose markdown report
    lines: List[str] = []
    lines.append("# DB Diagnostics Report")
    lines.append("")
    lines.append(f"- Generated (UTC): **{db_info['generated_utc']}**")
    lines.append(f"- DB Path: `{db_info['db_path']}`")
    lines.append(f"- DB Size: {db_info['size_bytes']:,} bytes")
    lines.append(f"- DB Modified (UTC): {db_info['mtime_utc']}")
    lines.append("")
    lines.append("## Critical Tables Presence")
    for k in sorted(presence.keys()):
        lines.append(f"- {k}: {'✅' if presence[k] else '❌'}")
    lines.append("")

    lines.append("## Component Health (Heartbeats)")
    for c in comps:
        lines.append(
            f"- **{c.name}**: {'✅ OK' if c.ok else '❌ OFFLINE'} | last={safe_iso(c.last_dt)} | age_sec={c.age_sec} | {c.note}"
        )
    lines.append("")

    lines.append("## Commands Queue")
    lines.append(f"- Present: {commands_info.get('present')}")
    if commands_info.get("present"):
        lines.append(f"- Counts: {commands_info.get('counts')}")
        lines.append(f"- Oldest pending age (sec): {commands_info.get('oldest_pending_age_sec')}")
        if commands_info.get("recent_errors"):
            lines.append("- Recent errors (last 10):")
            for e in commands_info["recent_errors"]:
                # trim error field
                err = str(e.get("error", ""))[:200]
                lines.append(f"  - id={e.get('id')} cmd={e.get('cmd')} status={e.get('status')} created={e.get('created')} err=`{err}`")
    lines.append("")

    lines.append("## Trades / Positions Integrity")
    lines.append(f"- Trades present: {trades_info.get('present')}")
    if trades_info.get("present"):
        lines.append(f"- Table: **{trades_info.get('table')}**")
        lines.append(f"- Counts: {trades_info.get('counts')}")
        if trades_info.get("data_issues"):
            lines.append("- Data issues:")
            for it in trades_info["data_issues"]:
                lines.append(f"  - {it['issue']}: {it['count']}")
    lines.append("")

    lines.append("## Protection Audit (if enabled)")
    lines.append(f"- Present: {protection_info.get('present')}")
    if protection_info.get("present"):
        lines.append(f"- Key: {protection_info.get('key')}")
        lines.append(f"- Updated UTC: {protection_info.get('updated_utc')}")
        lines.append(f"- Issue counts: {protection_info.get('counts')}")
    lines.append("")

    lines.append("## Learning / AI Assessment (Heuristic)")
    lines.append(f"- enable_learning setting: {learning_info.get('learning_enabled_setting')}")
    if learning_info.get("ai_usage"):
        au = learning_info["ai_usage"]
        lines.append(f"- decision_traces table: {au.get('decision_traces_table')}")
        lines.append(f"- sample_size: {au.get('sample_size')}")
        lines.append(f"- ai_vote_non_off: {au.get('ai_vote_non_off')}")
        lines.append(f"- ai_confidence_pos: {au.get('ai_confidence_pos')}")
        lines.append(f"- gate_top: {au.get('gate_top')}")
        lines.append(f"- decision_top: {au.get('decision_top')}")
        lines.append(f"- strategy_top: {au.get('strategy_top')}")
    if learning_info.get("paper_eval"):
        pe = learning_info["paper_eval"]
        lines.append(f"- analysis_results_count: {pe.get('analysis_results_count')}")
        lines.append(f"- analysis_results time_col: {pe.get('time_col')} min={pe.get('min')} max={pe.get('max')}")
    if learning_info.get("notes"):
        lines.append("- Notes:")
        for n in learning_info["notes"]:
            lines.append(f"  - {n}")
    lines.append("")

    lines.append("## Settings Snapshot (Redacted)")
    # show a short subset of important keys
    important_keys = [
        "mode", "USE_TESTNET", "PAPER_TRADING", "require_dashboard_approval",
        "auto_approve_live", "auto_approve_paper",
        "min_conf_scalp", "min_conf_swing", "gate_atr_min_pct", "gate_adx_min",
        "cooldown_after_trade_sec", "cooldown_after_sl_sec",
        "exchange_trailing_enabled", "exchange_trailing_activation_roi", "exchange_trailing_callback_rate",
        "pump_hunter_enabled", "paper_tournament_enabled"
    ]
    for k in important_keys:
        if k in settings_redacted:
            lines.append(f"- {k}: `{settings_redacted.get(k)}`")
    lines.append("")
    lines.append("## Outputs")
    lines.append(f"- report.md: `{(out_dir / 'report.md').resolve()}`")
    lines.append(f"- summary.json: `{(out_dir / 'summary.json').resolve()}`")
    lines.append(f"- tables.csv: `{(out_dir / 'tables.csv').resolve()}`")
    lines.append(f"- samples/: `{(out_dir / 'samples').resolve()}`")
    lines.append("")

    report_md = "\n".join(lines)
    (out_dir / "report.md").write_text(report_md, encoding="utf-8")

    summary = {
        "db_info": db_info,
        "tables": table_stats,
        "presence": presence,
        "components": [c.__dict__ | {"last_utc": safe_iso(c.last_dt)} for c in comps],
        "commands": commands_info,
        "trades": trades_info,
        "learning": learning_info,
        "protection_audit": protection_info,
        "settings_redacted": settings_redacted,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    print(f"[OK] Report generated in: {out_dir.resolve()}")
    print(f"     - report.md")
    print(f"     - summary.json")
    print(f"     - tables.csv")
    print(f"     - samples/")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
