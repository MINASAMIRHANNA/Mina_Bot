#!/usr/bin/env python3
"""bot_learning_audit.py

Produces a single, human-friendly report (stdout + JSON file) about:
- command pipeline health (dashboard -> commands -> bot)
- trades integrity (OPEN/CLOSED, missing fields, close_price=0, missing signal)
- learning pipeline progress (ai_samples, learning_stats, recommendations)
- paper tournament evaluation progress (analysis_results matured outcomes)
- strategy suggestion (scalp vs swing) based on evaluated outcomes

It does NOT require Binance API keys.

Usage:
  python bot_learning_audit.py --db ./bot_data.db --lookback-hours 24
  python bot_learning_audit.py --db ./bot_data.db --lookback-hours 24 --write-json ./learning_audit_report.json
  python bot_learning_audit.py --db ./bot_data.db --fix-closed-at   # optional safe fix

"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def parse_iso(s: Any) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        # Accept both "Z" and "+00:00"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def ms_to_dt(ms: Any) -> Optional[datetime]:
    try:
        ms_i = int(ms)
        if ms_i <= 0:
            return None
        return datetime.fromtimestamp(ms_i / 1000.0, tz=UTC)
    except Exception:
        return None


def to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def has_table(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def fetchall(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetchone(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else None


@dataclass
class Suggestion:
    key: str
    why: str
    value: Any = None


def summarize_outcomes(rows: List[Dict[str, Any]], outcome_col: str) -> Dict[str, int]:
    counts = {"WIN": 0, "LOSS": 0, "BREAKEVEN": 0, "PENDING": 0}
    for r in rows:
        v = (r.get(outcome_col) or "").strip().upper()
        if v in counts:
            counts[v] += 1
        else:
            counts["PENDING"] += 1
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./bot_data.db", help="Path to bot_data.db")
    ap.add_argument("--lookback-hours", type=float, default=24.0, help="Lookback window for analysis_results (hours)")
    ap.add_argument("--write-json", default="./learning_audit_report.json", help="Where to write JSON report")
    ap.add_argument("--fix-closed-at", action="store_true", help="(Optional) If CLOSED trades missing closed_at, fill it safely from closed_at_ms/updated_at/updated_at_ms")

    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    report: Dict[str, Any] = {
        "generated_at_utc": iso(utc_now()),
        "db_path": args.db,
        "tables": {},
        "health": {},
        "counts": {},
        "issues": [],
        "suggestions": [],
        "strategy_suggestion": None,
    }

    # --- Tables presence ---
    for t in [
        "trades",
        "commands",
        "settings",
        "ai_samples",
        "learning_stats",
        "recommendations",
        "analysis_results",
        "rejections",
        "signal_inbox",
    ]:
        report["tables"][t] = has_table(conn, t)

    # --- Key counts ---
    def count(table: str) -> int:
        if not has_table(conn, table):
            return 0
        r = fetchone(conn, f"SELECT COUNT(*) AS c FROM {table}")
        return int(r["c"]) if r else 0

    report["counts"].update({
        "trades": count("trades"),
        "commands": count("commands"),
        "ai_samples": count("ai_samples"),
        "learning_stats": count("learning_stats"),
        "recommendations": count("recommendations"),
        "analysis_results": count("analysis_results"),
        "rejections": count("rejections"),
        "signal_inbox": count("signal_inbox"),
    })

    # --- Settings snapshot (safe keys) ---
    safe_setting_keys = [
        "use_testnet",
        "paper_trading",
        "require_dashboard_approval",
        "auto_approve_live",
        "auto_approve_paper",
        "max_concurrent_trades",
        "min_confidence_to_trade",
        "min_score_to_trade",
        "gate_adx_min",
        "gate_atr_min_pct",
        "market_regime_enabled",
        "paper_tournament_enabled",
        "paper_eval_enabled",
        "paper_reco_enabled",
        "enable_learning",
    ]
    if has_table(conn, "settings"):
        placeholders = ",".join("?" for _ in safe_setting_keys)
        rows = fetchall(
            conn,
            f"SELECT key,value FROM settings WHERE key IN ({placeholders})",
            tuple(safe_setting_keys),
        )
        report["health"]["settings"] = {r["key"]: r["value"] for r in rows}

    # --- Trades integrity ---
    if has_table(conn, "trades"):
        cols = set(table_columns(conn, "trades"))
        report["health"]["trades_columns"] = sorted(cols)

        by_status = fetchall(conn, "SELECT status, COUNT(*) AS c FROM trades GROUP BY status")
        report["health"]["trades_by_status"] = {r.get("status") or "(null)": int(r["c"]) for r in by_status}

        # suspicious CLOSED rows
        suspicious: List[Dict[str, Any]] = []
        recent = fetchall(conn, "SELECT * FROM trades ORDER BY id DESC LIMIT 200")
        for r in recent:
            status = (r.get("status") or "").upper()
            if status != "CLOSED":
                continue
            close_price = to_float(r.get("close_price"), 0.0)
            pnl = to_float(r.get("pnl"), 0.0)
            signal = (r.get("signal") or "").strip()
            closed_at = (r.get("closed_at") or "").strip() if "closed_at" in cols else ""
            closed_at_ms = r.get("closed_at_ms") if "closed_at_ms" in cols else None

            if close_price <= 0.0 or abs(pnl) > 50.0 or not signal or (not closed_at and not closed_at_ms):
                suspicious.append({
                    "id": r.get("id"),
                    "symbol": r.get("symbol"),
                    "signal": signal,
                    "status": status,
                    "entry_price": r.get("entry_price"),
                    "close_price": r.get("close_price"),
                    "pnl": r.get("pnl"),
                    "closed_at": r.get("closed_at") if "closed_at" in cols else None,
                    "closed_at_ms": r.get("closed_at_ms") if "closed_at_ms" in cols else None,
                    "explain": r.get("explain"),
                })

        report["health"]["trades_suspicious_closed_200"] = suspicious[:30]
        if suspicious:
            report["issues"].append(
                f"Found {len(suspicious)} suspicious CLOSED trade rows in last 200 (missing signal/closed_at or close_price=0 or huge pnl)."
            )

        # OPEN trades missing SL/TP fields
        sl_col = "stop_loss" if "stop_loss" in cols else None
        tp_col = "take_profit" if "take_profit" in cols else None
        open_rows = fetchall(conn, "SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC LIMIT 200")
        missing_protection = []
        for r in open_rows:
            sl = to_float(r.get(sl_col), 0.0) if sl_col else 0.0
            tp = to_float(r.get(tp_col), 0.0) if tp_col else 0.0
            if sl <= 0.0 and tp <= 0.0:
                missing_protection.append({
                    "id": r.get("id"),
                    "symbol": r.get("symbol"),
                    "signal": r.get("signal"),
                })
        report["health"]["open_trades_missing_sl_tp"] = missing_protection[:30]
        if missing_protection:
            report["issues"].append(
                f"OPEN trades with no SL/TP stored in DB: {len(missing_protection)} (bot may still set exchange orders, but DB is empty)."
            )

        # Optional fix: fill closed_at where missing
        if args.fix_closed_at and ("closed_at" in cols or "closed_at_ms" in cols):
            fixed = 0
            for r in recent:
                if (r.get("status") or "").upper() != "CLOSED":
                    continue
                closed_at_s = (r.get("closed_at") or "").strip() if "closed_at" in cols else ""
                closed_at_ms = r.get("closed_at_ms") if "closed_at_ms" in cols else None
                if closed_at_s or (closed_at_ms and int(closed_at_ms) > 0):
                    continue

                # best-effort candidates
                candidate = None
                if "updated_at_ms" in cols and r.get("updated_at_ms"):
                    candidate = ms_to_dt(r.get("updated_at_ms"))
                if not candidate and "updated_at" in cols and r.get("updated_at"):
                    candidate = parse_iso(r.get("updated_at"))
                if not candidate and "created_at_ms" in cols and r.get("created_at_ms"):
                    candidate = ms_to_dt(r.get("created_at_ms"))
                if not candidate and "created_at" in cols and r.get("created_at"):
                    candidate = parse_iso(r.get("created_at"))
                if not candidate:
                    candidate = utc_now()

                conn.execute(
                    "UPDATE trades SET closed_at=?, closed_at_ms=COALESCE(closed_at_ms, ?) WHERE id=?",
                    (iso(candidate), int(candidate.timestamp() * 1000), r.get("id")),
                )
                fixed += 1

            if fixed:
                conn.commit()
                report["health"]["fix_closed_at"] = {"updated": fixed}
                report["issues"].append(f"Applied fix: filled closed_at for {fixed} CLOSED trades that were missing it.")

    # --- Commands recent ---
    if has_table(conn, "commands"):
        recent_cmds = fetchall(
            conn,
            "SELECT id, cmd, status, created_at, substr(params,1,160) AS params_short FROM commands ORDER BY id DESC LIMIT 15",
        )
        report["health"]["commands_recent_15"] = recent_cmds
        undone = fetchone(conn, "SELECT COUNT(*) AS c FROM commands WHERE status NOT IN ('DONE','ERROR')")
        if undone and int(undone["c"]) > 0:
            report["issues"].append(f"There are {int(undone['c'])} commands not DONE/ERROR yet (bot might be offline or command monitor stuck).")

    # --- Learning tables ---
    if has_table(conn, "ai_samples"):
        by_status = fetchall(conn, "SELECT status, COUNT(*) AS c FROM ai_samples GROUP BY status")
        report["health"]["ai_samples_by_status"] = {r.get("status") or "(null)": int(r["c"]) for r in by_status}
        pending = fetchall(conn, "SELECT id, symbol, side, ai_vote, status, created_at, trade_id, inbox_id FROM ai_samples WHERE status!='CLOSED' ORDER BY id DESC LIMIT 20")
        report["health"]["ai_samples_recent_non_closed_20"] = pending

        # if we have lots of PENDING but trade_id NULL, learning won't progress
        pending_unlinked = [r for r in pending if r.get("trade_id") in (None, "", 0)]
        if pending_unlinked and report["counts"]["ai_samples"]:
            report["issues"].append(
                "ai_samples exist but many are not linked to trades (trade_id is NULL). Closed-loop training requires linking inbox_id -> trade_id."
            )

    if has_table(conn, "analysis_results"):
        lookback_ms = int(args.lookback_hours * 3600 * 1000)
        now_ms = int(utc_now().timestamp() * 1000)
        start_ms = now_ms - lookback_ms

        ar = fetchall(
            conn,
            """
            SELECT id,symbol,decision,strategy_tag,signal_time_ms,outcome_15m,outcome_1h,outcome_4h
            FROM analysis_results
            WHERE signal_time_ms >= ?
            ORDER BY id DESC
            LIMIT 5000
            """,
            (start_ms,),
        )
        report["health"]["analysis_results_lookback_count"] = len(ar)
        # decision breakdown
        dec_counts: Dict[str, int] = {}
        for r in ar:
            d = (r.get("decision") or "").upper() or "(null)"
            dec_counts[d] = dec_counts.get(d, 0) + 1
        report["health"]["analysis_results_decision_counts"] = dec_counts

        # outcomes maturity
        report["health"]["analysis_outcomes_15m"] = summarize_outcomes(ar, "outcome_15m")
        report["health"]["analysis_outcomes_1h"] = summarize_outcomes(ar, "outcome_1h")
        report["health"]["analysis_outcomes_4h"] = summarize_outcomes(ar, "outcome_4h")

        # strategy suggestion: compare scalp (1h) vs swing (4h) using strategy_tag
        def tag_stats(tag: str, outcome_col: str) -> Dict[str, Any]:
            rows = [x for x in ar if (x.get("strategy_tag") or "") == tag]
            evaluated = [x for x in rows if (x.get(outcome_col) or "").strip().upper() in ("WIN", "LOSS", "BREAKEVEN")]
            if not evaluated:
                return {"tag": tag, "evaluated": 0, "winrate": None}
            wins = sum(1 for x in evaluated if (x.get(outcome_col) or "").strip().upper() == "WIN")
            return {"tag": tag, "evaluated": len(evaluated), "winrate": round((wins / len(evaluated)) * 100.0, 1)}

        scalp = tag_stats("scalp", "outcome_1h")
        swing = tag_stats("swing", "outcome_4h")
        report["health"]["strategy_eval_stats"] = {"scalp_1h": scalp, "swing_4h": swing}

        # Decide
        chosen = None
        reason = None
        if scalp["evaluated"] >= 5 and swing["evaluated"] >= 5:
            chosen = "scalp" if (scalp["winrate"] or 0) >= (swing["winrate"] or 0) else "swing"
            reason = f"based on winrate (scalp={scalp['winrate']}% over {scalp['evaluated']} evals, swing={swing['winrate']}% over {swing['evaluated']} evals)"
        elif scalp["evaluated"] >= 5:
            chosen = "scalp"
            reason = f"swing not enough evaluated 4h samples yet (swing eval={swing['evaluated']}); scalp has {scalp['evaluated']} evals"
        elif swing["evaluated"] >= 5:
            chosen = "swing"
            reason = f"scalp not enough evaluated 1h samples yet (scalp eval={scalp['evaluated']}); swing has {swing['evaluated']} evals"
        else:
            # not enough data
            chosen = None
            reason = "not enough evaluated samples yet; wait until at least 5 outcomes are labeled for scalp(1h) or swing(4h)"

        report["strategy_suggestion"] = {
            "suggested": chosen,
            "reason": reason,
            "scalp": scalp,
            "swing": swing,
        }

        # If almost everything is NEUTRAL, suggest reducing WAIT gates
        total = len(ar)
        non_neutral = total - dec_counts.get("NEUTRAL", 0)
        if total >= 200 and (non_neutral / max(total, 1)) < 0.03:
            report["suggestions"].append({
                "key": "reduce_wait",
                "value": {"min_confidence_to_trade": "45-50", "min_score_to_trade": "0.15-0.18", "gate_adx_min": "10", "gate_atr_min_pct": "0.10"},
                "why": f"Signal rate is very low: {non_neutral}/{total} ({round(non_neutral*100/max(total,1),2)}%)."
            })

        # Long/Short bias
        short_n = dec_counts.get("SHORT", 0)
        long_n = dec_counts.get("LONG", 0)
        if short_n + long_n > 0:
            bias = "SHORT" if short_n > long_n else "LONG" if long_n > short_n else "BALANCED"
            report["health"]["long_short_bias"] = {
                "bias": bias,
                "long": long_n,
                "short": short_n,
                "ratio_short": round(short_n / max(short_n + long_n, 1), 3),
            }

    # --- Suggestions for learning pipeline ---
    if report["counts"].get("ai_samples", 0) < 50:
        report["suggestions"].append({
            "key": "closed_loop_learning_progress",
            "value": {"CLOSED_LOOP_MIN_SAMPLES": "30 (early-stage) then raise to 200"},
            "why": f"ai_samples={report['counts'].get('ai_samples',0)} is very low; closed-loop trainer won't have enough samples." 
        })

    # Write JSON
    with open(args.write_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # Pretty print summary
    print("\n================ BOT LEARNING AUDIT ================")
    print(f"DB: {args.db}")
    print(f"Generated (UTC): {report['generated_at_utc']}")

    print("\n--- Counts ---")
    for k, v in report["counts"].items():
        print(f"{k:16s}: {v}")

    if report["health"].get("settings"):
        print("\n--- Key Settings (from DB) ---")
        for k in sorted(report["health"]["settings"].keys()):
            print(f"{k:28s}: {report['health']['settings'][k]}")

    if report["health"].get("trades_by_status"):
        print("\n--- Trades by Status ---")
        for k, v in report["health"]["trades_by_status"].items():
            print(f"{k:10s}: {v}")

    if report.get("strategy_suggestion"):
        ss = report["strategy_suggestion"]
        print("\n--- Strategy Suggestion (from analysis_results) ---")
        print(f"Suggested: {ss['suggested']} | Reason: {ss['reason']}")
        print(f"Scalp(1h): eval={ss['scalp']['evaluated']} winrate={ss['scalp']['winrate']}")
        print(f"Swing(4h): eval={ss['swing']['evaluated']} winrate={ss['swing']['winrate']}")

    if report.get("issues"):
        print("\n--- Issues found ---")
        for it in report["issues"][:20]:
            print(f"- {it}")

    if report.get("suggestions"):
        print("\n--- Suggestions ---")
        for s in report["suggestions"][:20]:
            print(f"- {s['key']}: {s['why']} -> {s.get('value')}")

    print(f"\n✅ JSON written: {args.write_json}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
