#!/usr/bin/env python3
# db_full_audit.py
# Usage:
#   python db_full_audit.py --db ./bot_data.db --out ./db_audit_out
#
# Output:
#   ./db_audit_out/audit_report.json
#   ./db_audit_out/audit_report.txt
#   ./db_audit_out/schema.sql
#   ./db_audit_out/dumps/*.csv

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


REDACT_KEY_PATTERNS = [
    r"API_KEY", r"API_SECRET", r"SECRET", r"TOKEN", r"PASSWORD", r"PRIVATE", r"KEY",
]
REDACT_KEY_REGEX = re.compile("|".join(REDACT_KEY_PATTERNS), re.IGNORECASE)

# Heartbeats in settings (as used in your existing diagnostics)
HEARTBEAT_KEYS = {
    "bot": ["bot_heartbeat", "heartbeat", "main_heartbeat"],
    "execution_monitor": ["execution_monitor_heartbeat"],
    "risk_supervisor": ["risk_supervisor_heartbeat"],
    "data_manager": ["data_manager_heartbeat"],
}

PAGE_CONTRACTS = {
    "analytics": {
        "requires_tables": ["trades", "rejections"],
    },
    "paper": {
        "requires_tables": ["analysis_results"],
    },
    "pump": {
        "requires_tables": ["pump_hunter_state", "pump_candidates"],
    },
    "strategy_lab": {
        "requires_tables": ["decision_traces"],
    },
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def to_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def try_parse_dt(val: Any) -> Optional[datetime]:
    """
    Convert common DB representations to datetime UTC:
    - ms epoch (>= 1e12)
    - sec epoch (>= 1e9)
    - ISO strings with Z / +00:00
    - 'YYYY-MM-DD HH:MM:SS' (assumed UTC)
    """
    if val is None:
        return None

    # numeric epoch?
    if isinstance(val, (int, float)):
        # Ignore "obviously not time" small numbers
        if val >= 1e12:  # ms
            try:
                return datetime.fromtimestamp(val / 1000.0, tz=timezone.utc)
            except Exception:
                return None
        if val >= 1e9:  # sec
            try:
                return datetime.fromtimestamp(val, tz=timezone.utc)
            except Exception:
                return None
        return None

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None

        # Normalize Z
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s

        # Handle 'YYYY-MM-DD HH:MM:SS'
        if " " in s2 and "T" not in s2:
            s2 = s2.replace(" ", "T")

        # If no tz info, assume UTC
        if re.match(r"^\d{4}-\d{2}-\d{2}T", s2) and not re.search(r"([+-]\d{2}:\d{2})$", s2):
            # add UTC offset
            s2 = s2 + "+00:00"

        try:
            return datetime.fromisoformat(s2).astimezone(timezone.utc)
        except Exception:
            return None

    return None


def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def list_tables(con: sqlite3.Connection) -> List[str]:
    cur = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [r["name"] for r in cur.fetchall()]


def table_info(con: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return [dict(r) for r in cur.fetchall()]


def table_count(con: sqlite3.Connection, table: str) -> int:
    cur = con.execute(f"SELECT COUNT(*) AS c FROM {table}")
    return int(cur.fetchone()["c"])


def dump_schema(con: sqlite3.Connection) -> str:
    cur = con.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name")
    return "\n;\n".join([r["sql"] for r in cur.fetchall()]) + ";\n"


def pick_time_cols(cols: List[str]) -> List[str]:
    """
    Heuristic: pick likely time columns (limit 4).
    """
    priority = [
        "timestamp_ms", "timestamp",
        "created_at_ms", "created_at",
        "updated_at_ms", "updated_at",
        "closed_at_ms", "closed_at",
        "signal_time_ms", "signal_time",
        "last_heartbeat_ms", "heartbeat", "last_scan_ms",
        "date_utc", "date",
    ]
    chosen = []
    lower_cols = {c.lower(): c for c in cols}
    for p in priority:
        if p.lower() in lower_cols and lower_cols[p.lower()] not in chosen:
            chosen.append(lower_cols[p.lower()])

    # add extra candidates if still short
    if len(chosen) < 4:
        for c in cols:
            cl = c.lower()
            if any(k in cl for k in ["time", "timestamp", "created", "updated", "heartbeat", "closed", "date"]) and c not in chosen:
                chosen.append(c)
            if len(chosen) >= 4:
                break

    return chosen[:4]


def get_min_max(con: sqlite3.Connection, table: str, col: str) -> Tuple[Any, Any]:
    try:
        cur = con.execute(f"SELECT MIN({col}) AS mn, MAX({col}) AS mx FROM {table}")
        r = cur.fetchone()
        return r["mn"], r["mx"]
    except Exception:
        return None, None


def compute_time_ranges(con: sqlite3.Connection, table: str, cols: List[str]) -> List[Dict[str, Any]]:
    now = utc_now()
    out = []
    for col in cols:
        mn, mx = get_min_max(con, table, col)
        mn_dt = try_parse_dt(mn)
        mx_dt = try_parse_dt(mx)
        age = None
        if mx_dt is not None:
            age = int((now - mx_dt).total_seconds())
        out.append({
            "col": col,
            "min": mn if mn_dt is None else to_iso(mn_dt),
            "max": mx if mx_dt is None else to_iso(mx_dt),
            "max_age_sec": age,
            "raw_min": mn if mn_dt is None else None,
            "raw_max": mx if mx_dt is None else None,
        })
    return out


def read_settings(con: sqlite3.Connection) -> Dict[str, str]:
    if "settings" not in list_tables(con):
        return {}
    cur = con.execute("SELECT key, value FROM settings")
    return {r["key"]: (r["value"] if r["value"] is not None else "") for r in cur.fetchall()}


def redact_settings(settings: Dict[str, str]) -> Dict[str, str]:
    out = {}
    for k, v in settings.items():
        if REDACT_KEY_REGEX.search(k):
            # keep last 4 chars (if any) for debugging
            tail = v[-4:] if isinstance(v, str) and len(v) >= 4 else ""
            out[k] = f"***{tail}"
        else:
            out[k] = v
    return out


def heartbeat_status(settings: Dict[str, str]) -> List[Dict[str, Any]]:
    now = utc_now()
    out = []

    def get_first_present(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in settings and str(settings[k]).strip():
                return settings[k]
        return None

    for name, keys in HEARTBEAT_KEYS.items():
        raw = get_first_present(keys)
        dt = try_parse_dt(raw)
        age = None
        ok = False
        if dt:
            age = int((now - dt).total_seconds())
            ok = age <= 30
        out.append({
            "name": name,
            "ok": ok,
            "age_sec": age,
            "last_utc": to_iso(dt),
            "source": "settings",
            "keys": keys,
        })
    return out


def dump_table_csv(con: sqlite3.Connection, table: str, out_dir: Path, limit: int = 200) -> Optional[str]:
    try:
        cur = con.execute(f"SELECT * FROM {table} LIMIT ?", (limit,))
        rows = cur.fetchall()
        if not rows:
            return None
        cols = rows[0].keys()
        csv_path = out_dir / f"{table}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow([r[c] for c in cols])
        return str(csv_path)
    except Exception:
        return None


def sql_count_where(con: sqlite3.Connection, table: str, where: str) -> int:
    try:
        cur = con.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE {where}")
        return int(cur.fetchone()["c"])
    except Exception:
        return 0


def trades_audit(con: sqlite3.Connection) -> Dict[str, Any]:
    if "trades" not in list_tables(con):
        return {"present": False}

    stats = {"present": True}
    cur = con.execute("SELECT status, COUNT(*) AS c FROM trades GROUP BY status ORDER BY c DESC")
    stats["by_status"] = {r["status"]: int(r["c"]) for r in cur.fetchall()}

    # Key field completeness
    checks = {
        "open_missing_market_type": "status='OPEN' AND (market_type IS NULL OR market_type='')",
        "open_missing_strategy_tag": "status='OPEN' AND (strategy_tag IS NULL OR strategy_tag='')",
        "open_missing_ai_vote": "status='OPEN' AND (ai_vote IS NULL OR ai_vote='')",
        "open_missing_exit_profile": "status='OPEN' AND (exit_profile IS NULL OR exit_profile='')",
        "open_missing_qty": "status='OPEN' AND (quantity IS NULL OR quantity=0)",
        "open_missing_sl": "status='OPEN' AND (stop_loss IS NULL OR stop_loss=0)",
    }
    stats["data_issues"] = {k: sql_count_where(con, "trades", w) for k, w in checks.items()}

    # Sample open trades
    try:
        cur2 = con.execute(
            "SELECT id, symbol, signal, entry_price, quantity, stop_loss, take_profits, status, timestamp, "
            "market_type, strategy_tag, exit_profile, leverage, ai_vote, confidence, explain "
            "FROM trades WHERE status='OPEN' ORDER BY timestamp DESC LIMIT 10"
        )
        stats["open_sample"] = [dict(r) for r in cur2.fetchall()]
    except Exception:
        stats["open_sample"] = []

    return stats


def paper_eval_audit(con: sqlite3.Connection) -> Dict[str, Any]:
    if "analysis_results" not in list_tables(con):
        return {"present": False}
    # Try common columns
    cols = [c["name"] for c in table_info(con, "analysis_results")]
    horizon_cols = [c for c in ["price_after_15m", "price_after_1h", "price_after_4h"] if c in cols]
    outcome_cols = [c for c in ["outcome_15m", "outcome_1h", "outcome_4h"] if c in cols]

    out: Dict[str, Any] = {"present": True, "rows": table_count(con, "analysis_results")}
    out["horizon_cols_found"] = horizon_cols
    out["outcome_cols_found"] = outcome_cols

    if not horizon_cols and not outcome_cols:
        out["warning"] = "No known evaluation columns found."
        return out

    # Coverage counts (non-null)
    coverage = {}
    for c in horizon_cols + outcome_cols:
        coverage[c] = sql_count_where(con, "analysis_results", f"{c} IS NOT NULL AND {c} != ''")
    out["non_null_counts"] = coverage

    # Freshness from signal_time / signal_time_ms
    time_cols = pick_time_cols(cols)
    out["time_ranges"] = compute_time_ranges(con, "analysis_results", time_cols[:3])

    return out


def pump_audit(con: sqlite3.Connection) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    # pump_hunter_state
    if "pump_hunter_state" in list_tables(con):
        try:
            cur = con.execute("SELECT last_heartbeat_ms, last_scan_ms, last_scan_count, last_error FROM pump_hunter_state LIMIT 1")
            r = cur.fetchone()
            now = utc_now()
            hb_dt = try_parse_dt(r["last_heartbeat_ms"])
            scan_dt = try_parse_dt(r["last_scan_ms"])
            out["pump_hunter_state"] = {
                "present": True,
                "last_heartbeat_utc": to_iso(hb_dt),
                "heartbeat_age_sec": int((now - hb_dt).total_seconds()) if hb_dt else None,
                "last_scan_utc": to_iso(scan_dt),
                "last_scan_age_sec": int((now - scan_dt).total_seconds()) if scan_dt else None,
                "last_scan_count": r["last_scan_count"],
                "last_error": r["last_error"],
            }
        except Exception as e:
            out["pump_hunter_state"] = {"present": True, "error": str(e)}
    else:
        out["pump_hunter_state"] = {"present": False}

    # pump_candidates
    if "pump_candidates" in list_tables(con):
        cols = [c["name"] for c in table_info(con, "pump_candidates")]
        tr = compute_time_ranges(con, "pump_candidates", pick_time_cols(cols)[:2])
        out["pump_candidates"] = {
            "present": True,
            "rows": table_count(con, "pump_candidates"),
            "time_ranges": tr,
        }
    else:
        out["pump_candidates"] = {"present": False}

    return out


def decision_traces_audit(con: sqlite3.Connection, sample_size: int = 500) -> Dict[str, Any]:
    if "decision_traces" not in list_tables(con):
        return {"present": False}

    out: Dict[str, Any] = {"present": True, "rows": table_count(con, "decision_traces"), "sample_size": sample_size}

    try:
        # detect columns
        cols = [c["name"] for c in table_info(con, "decision_traces")]
        out["time_ranges"] = compute_time_ranges(con, "decision_traces", pick_time_cols(cols)[:2])

        # top distributions (use fallbacks if col names differ)
        decision_col = "decision" if "decision" in cols else None
        gate_col = "gate" if "gate" in cols else None
        strategy_col = "strategy" if "strategy" in cols else None
        ai_vote_col = "ai_vote" if "ai_vote" in cols else None
        ai_conf_col = "ai_confidence" if "ai_confidence" in cols else None

        def top(col: str) -> List[List[Any]]:
            cur = con.execute(
                f"SELECT COALESCE({col}, '') AS v, COUNT(*) AS c FROM "
                f"(SELECT {col} FROM decision_traces ORDER BY created_at_ms DESC LIMIT ?) "
                f"GROUP BY v ORDER BY c DESC LIMIT 10",
                (sample_size,)
            )
            return [[r["v"], int(r["c"])] for r in cur.fetchall()]

        if gate_col:
            out["gate_top"] = top(gate_col)
        if decision_col:
            out["decision_top"] = top(decision_col)
        if strategy_col:
            out["strategy_top"] = top(strategy_col)

        # ai usage
        if ai_vote_col:
            cur = con.execute(
                f"SELECT COUNT(*) AS c FROM (SELECT {ai_vote_col} AS v FROM decision_traces ORDER BY created_at_ms DESC LIMIT ?) "
                f"WHERE v IS NOT NULL AND v != '' AND UPPER(v) != 'OFF'",
                (sample_size,)
            )
            out["ai_vote_non_off"] = int(cur.fetchone()["c"])
        if ai_conf_col:
            cur = con.execute(
                f"SELECT COUNT(*) AS c FROM (SELECT {ai_conf_col} AS v FROM decision_traces ORDER BY created_at_ms DESC LIMIT ?) "
                f"WHERE v IS NOT NULL AND v > 0",
                (sample_size,)
            )
            out["ai_confidence_pos"] = int(cur.fetchone()["c"])

    except Exception as e:
        out["error"] = str(e)

    return out


def learning_audit(con: sqlite3.Connection, settings: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out["enable_learning_setting"] = settings.get("enable_learning", "")

    # learning_stats
    out["learning_stats"] = {
        "present": "learning_stats" in list_tables(con),
        "rows": table_count(con, "learning_stats") if "learning_stats" in list_tables(con) else None,
    }
    out["threshold_adjustments"] = {
        "present": "threshold_adjustments" in list_tables(con),
        "rows": table_count(con, "threshold_adjustments") if "threshold_adjustments" in list_tables(con) else None,
    }

    # models usage flag (if stored in settings)
    out["use_closed_loop_models_setting"] = settings.get("use_closed_loop_models", "")

    return out


def protection_audit_from_settings(settings: Dict[str, str]) -> Dict[str, Any]:
    raw = settings.get("protection_audit_status", "")
    if not raw:
        return {"present": False}
    try:
        j = json.loads(raw)
        return {
            "present": True,
            "counts": {
                "missing_sl": j.get("missing_sl"),
                "missing_tp": j.get("missing_tp"),
                "missing_trail": j.get("missing_trail"),
                "orphan_orders": j.get("orphan_orders"),
                "duplicates": j.get("duplicates"),
            },
            "updated_utc": j.get("updated_utc"),
        }
    except Exception:
        return {"present": True, "parse_error": True, "raw_preview": raw[:200]}


def page_readiness(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Report page readiness using presence + freshness signals.
    """
    tables_present = {t["table"]: True for t in report.get("tables", [])}
    now = utc_now()

    def get_table_age(table: str) -> Optional[int]:
        for t in report.get("tables", []):
            if t.get("table") == table:
                trs = t.get("time_ranges") or []
                # pick smallest age among time ranges (best chance it's real freshness)
                ages = [tr.get("max_age_sec") for tr in trs if isinstance(tr.get("max_age_sec"), int)]
                return min(ages) if ages else None
        return None

    readiness = {}
    for page, contract in PAGE_CONTRACTS.items():
        req = contract["requires_tables"]
        missing = [t for t in req if not tables_present.get(t)]
        if missing:
            readiness[page] = {"status": "FAIL", "missing_tables": missing}
            continue

        # extra logic
        status = "OK"
        notes: List[str] = []

        if page == "pump":
            hb_age = None
            ph = report.get("pump", {}).get("pump_hunter_state", {})
            if isinstance(ph, dict):
                hb_age = ph.get("heartbeat_age_sec")
            cand_age = get_table_age("pump_candidates")
            if hb_age is None or hb_age > 30:
                status = "WARN"
                notes.append(f"pump_hunter heartbeat is stale (age={hb_age})")
            if cand_age is None or cand_age > 3600:
                status = "WARN"
                notes.append(f"pump_candidates not fresh (age={cand_age})")

        if page == "paper":
            # if evaluation columns exist but mostly null -> warn
            pe = report.get("paper_eval", {})
            nn = (pe.get("non_null_counts") or {}) if isinstance(pe, dict) else {}
            if nn:
                total = pe.get("rows") or 0
                # if none of eval cols have any non-null
                if total and all((nn.get(k) or 0) == 0 for k in nn.keys()):
                    status = "WARN"
                    notes.append("analysis_results has 0 non-null evaluation fields -> leaderboard may be empty")
            else:
                notes.append("Could not compute evaluation coverage")

        if page == "analytics":
            tr_age = get_table_age("trades")
            rj_age = get_table_age("rejections")
            if (tr_age is not None and tr_age > 3600) and (rj_age is not None and rj_age > 3600):
                status = "WARN"
                notes.append("trades & rejections seem stale (>1h)")

        if page == "strategy_lab":
            dt = report.get("decision_traces", {})
            if isinstance(dt, dict) and dt.get("present"):
                decision_top = dt.get("decision_top") or []
                strategy_top = dt.get("strategy_top") or []
                # if all NO_TRADE in sample
                if decision_top and len(decision_top) == 1 and decision_top[0][0] == "NO_TRADE":
                    status = "WARN"
                    notes.append("decision_traces sample shows 100% NO_TRADE")
                # if most strategies blank
                if strategy_top and strategy_top[0][0] == "" and strategy_top[0][1] >= int(dt.get("sample_size", 500) * 0.8):
                    status = "WARN"
                    notes.append("strategy field mostly empty in decision_traces sample")

        readiness[page] = {"status": status, "notes": notes}

    return readiness


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite db (e.g., ./bot_data.db)")
    ap.add_argument("--out", required=True, help="Output directory (e.g., ./db_audit_out)")
    ap.add_argument("--dump-limit", type=int, default=200, help="Rows to dump per key table to CSV")
    ap.add_argument("--decision-sample", type=int, default=500, help="Sample size for decision_traces analysis")
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()
    ensure_dir(out_dir)
    dumps_dir = out_dir / "dumps"
    ensure_dir(dumps_dir)

    if not db_path.exists():
        print(f"[ERROR] DB not found: {db_path}")
        sys.exit(1)

    con = connect(db_path)
    try:
        now = utc_now()

        tbls = list_tables(con)
        settings = read_settings(con)
        settings_redacted = redact_settings(settings)

        # schema
        schema_sql = dump_schema(con)
        (out_dir / "schema.sql").write_text(schema_sql, encoding="utf-8")

        tables_report = []
        for t in tbls:
            cols = [c["name"] for c in table_info(con, t)]
            cnt = table_count(con, t)
            time_cols = pick_time_cols(cols)
            tr = compute_time_ranges(con, t, time_cols)
            tables_report.append({
                "table": t,
                "rows": cnt,
                "cols": len(cols),
                "time_cols": time_cols,
                "time_ranges": tr,
            })

        # presence signals
        presence = {
            "settings": "settings" in tbls,
            "commands": "commands" in tbls,
            "signal_inbox": "signal_inbox" in tbls,
            "decision_traces": "decision_traces" in tbls,
            "analysis_results": "analysis_results" in tbls,
            "pump_hunter_state": "pump_hunter_state" in tbls,
            "pump_candidates": "pump_candidates" in tbls,
            "positions_status": "positions_status" in tbls,
            "logs": "logs" in tbls,
        }

        # audits
        components = heartbeat_status(settings)
        trades = trades_audit(con)
        paper_eval = paper_eval_audit(con)
        pump = pump_audit(con)
        decision_traces = decision_traces_audit(con, sample_size=args.decision_sample)
        learning = learning_audit(con, settings)
        protection = protection_audit_from_settings(settings)

        # commands quick
        commands_report = {"present": "commands" in tbls}
        if "commands" in tbls:
            cur = con.execute("SELECT status, COUNT(*) AS c FROM commands GROUP BY status ORDER BY c DESC")
            commands_report["counts"] = {r["status"]: int(r["c"]) for r in cur.fetchall()}
            # pending age (best effort)
            try:
                cur2 = con.execute(
                    "SELECT created_at, created_at_ms FROM commands WHERE status IN ('PENDING','ERROR') ORDER BY COALESCE(created_at_ms,0) ASC LIMIT 1"
                )
                r = cur2.fetchone()
                if r:
                    dt = try_parse_dt(r["created_at_ms"]) or try_parse_dt(r["created_at"])
                    commands_report["oldest_pending_age_sec"] = int((now - dt).total_seconds()) if dt else None
                else:
                    commands_report["oldest_pending_age_sec"] = None
            except Exception:
                commands_report["oldest_pending_age_sec"] = None

        # dump key tables for quick manual inspection
        dump_targets = [
            "trades", "rejections", "analysis_results", "decision_traces",
            "pump_candidates", "pump_hunter_state", "settings_audit", "logs"
        ]
        dumps = {}
        for t in dump_targets:
            if t in tbls:
                p = dump_table_csv(con, t, dumps_dir, limit=args.dump_limit)
                if p:
                    dumps[t] = p

        report = {
            "db_info": {
                "db_path": str(db_path),
                "size_bytes": db_path.stat().st_size,
                "mtime_utc": to_iso(datetime.fromtimestamp(db_path.stat().st_mtime, tz=timezone.utc)),
                "generated_utc": to_iso(now),
            },
            "presence": presence,
            "tables": tables_report,
            "components": components,
            "commands": commands_report,
            "trades": trades,
            "paper_eval": paper_eval,
            "pump": pump,
            "decision_traces": decision_traces,
            "learning": learning,
            "protection_audit": protection,
            "settings_redacted": settings_redacted,
            "dumps": dumps,
        }

        report["page_readiness"] = page_readiness(report)

        # Write JSON
        (out_dir / "audit_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

        # Write TXT summary (human-friendly)
        lines = []
        lines.append("=== DB FULL AUDIT SUMMARY ===")
        lines.append(f"DB: {db_path}")
        lines.append(f"Generated (UTC): {to_iso(now)}")
        lines.append("")
        lines.append("== Presence ==")
        for k, v in presence.items():
            lines.append(f"- {k}: {'OK' if v else 'MISSING'}")
        lines.append("")
        lines.append("== Components (settings heartbeats) ==")
        for c in components:
            lines.append(f"- {c['name']}: {'OK' if c['ok'] else 'STALE'} age_sec={c['age_sec']} last={c['last_utc']}")
        lines.append("")
        lines.append("== Trades ==")
        if trades.get("present"):
            lines.append(f"- by_status: {trades.get('by_status')}")
            lines.append(f"- data_issues: {trades.get('data_issues')}")
        lines.append("")
        lines.append("== Paper Eval ==")
        lines.append(json.dumps(paper_eval, indent=2, ensure_ascii=False))
        lines.append("")
        lines.append("== Pump ==")
        lines.append(json.dumps(pump, indent=2, ensure_ascii=False))
        lines.append("")
        lines.append("== Decision Traces ==")
        lines.append(json.dumps(decision_traces, indent=2, ensure_ascii=False))
        lines.append("")
        lines.append("== Learning ==")
        lines.append(json.dumps(learning, indent=2, ensure_ascii=False))
        lines.append("")
        lines.append("== Protection Audit ==")
        lines.append(json.dumps(protection, indent=2, ensure_ascii=False))
        lines.append("")
        lines.append("== Page Readiness ==")
        lines.append(json.dumps(report["page_readiness"], indent=2, ensure_ascii=False))
        lines.append("")
        lines.append("== Key Table Freshness (top 12 by row count) ==")
        top_tbls = sorted(tables_report, key=lambda x: x.get("rows", 0), reverse=True)[:12]
        for t in top_tbls:
            ages = [tr.get("max_age_sec") for tr in (t.get("time_ranges") or []) if isinstance(tr.get("max_age_sec"), int)]
            best_age = min(ages) if ages else None
            lines.append(f"- {t['table']}: rows={t['rows']} best_age_sec={best_age}")
        (out_dir / "audit_report.txt").write_text("\n".join(lines), encoding="utf-8")

        print(f"[OK] Wrote: {out_dir / 'audit_report.json'}")
        print(f"[OK] Wrote: {out_dir / 'audit_report.txt'}")
        print(f"[OK] Wrote: {out_dir / 'schema.sql'}")
        print(f"[OK] Dumps dir: {dumps_dir}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
