"""
analyzer.py

Lightweight analytics / reporting utility for crypto_bot.

Goals:
- Produce a useful daily/weekly report from the bot SQLite DB.
- Work even if some tables/columns are missing (defensive).
- Use UTC everywhere.

This module is intentionally read-mostly; it will only store a single JSON report
under settings key `last_analysis_report_json` (and log a short summary) when possible.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from database import DatabaseManager

# Optional: use cfg.DB_FILE when available
try:
    from config import cfg  # type: ignore
except Exception:  # pragma: no cover
    cfg = None  # type: ignore


# -----------------------------
# Helpers
# -----------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(dt: Optional[datetime] = None) -> str:
    dt = dt or _utc_now()
    # Render as ISO 8601 with Z
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_db_file() -> str:
    # Zero-ENV: cfg.DB_FILE -> fallback local bot_data.db
    if cfg is not None and getattr(cfg, "DB_FILE", None):
        return str(cfg.DB_FILE)
    return "bot_data.db"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(_get_db_file())


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    row = con.execute(q, (table,)).fetchone()
    return bool(row)


def _safe_read_sql(con: sqlite3.Connection, query: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(query, con, params=params)
    except Exception:
        return pd.DataFrame()


def _to_utc_dt(series: pd.Series) -> pd.Series:
    # Accepts ISO strings with Z or without TZ; forces UTC
    try:
        return pd.to_datetime(series, utc=True, errors="coerce")
    except Exception:
        return pd.Series([pd.NaT] * len(series))


def _wins_losses_pnl(df: pd.DataFrame, pnl_col: str = "pnl") -> Dict[str, Any]:
    if df.empty or pnl_col not in df.columns:
        return {"count": 0, "wins": 0, "losses": 0, "win_rate": None, "pnl_sum": 0.0, "pnl_avg": None}
    pnl = pd.to_numeric(df[pnl_col], errors="coerce").fillna(0.0)
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    count = int(len(pnl))
    win_rate = (wins / count) if count else None
    return {
        "count": count,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "pnl_sum": float(pnl.sum()),
        "pnl_avg": float(pnl.mean()) if count else None,
    }


def _bin_confidence(x: float) -> str:
    # 0.0-1.0 -> "0.0-0.1", ...; clamp
    x = 0.0 if x is None else float(x)
    x = max(0.0, min(1.0, x))
    lo = int(x * 10) / 10
    hi = lo + 0.1
    if hi > 1.0:
        hi = 1.0
    return f"{lo:.1f}-{hi:.1f}"


def _try_parse_json(s: Any) -> Dict[str, Any]:
    if not s or not isinstance(s, str):
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


# -----------------------------
# Analyses
# -----------------------------
def analyze_rejections(days: int = 14) -> Dict[str, Any]:
    """Analyze reject reasons from `rejections` table."""
    report: Dict[str, Any] = {"days": days, "by_reason": [], "by_symbol": [], "total": 0}

    with get_connection() as con:
        if not _table_exists(con, "rejections"):
            report["note"] = "Table `rejections` not found."
            return report

        df = _safe_read_sql(
            con,
            "SELECT symbol, reason, value, threshold, timestamp FROM rejections ORDER BY id DESC",
        )
        if df.empty:
            report["note"] = "No rejections found."
            return report

        df["ts"] = _to_utc_dt(df["timestamp"])
        cutoff = _utc_now() - timedelta(days=days)
        df = df[df["ts"] >= cutoff]
        report["total"] = int(len(df))

        if df.empty:
            report["note"] = "No rejections in time window."
            return report

        # By reason
        by_reason = (
            df.groupby("reason", dropna=False)
            .size()
            .sort_values(ascending=False)
            .reset_index(name="count")
        )
        report["by_reason"] = by_reason.head(10).to_dict(orient="records")

        # By symbol
        by_symbol = (
            df.groupby("symbol", dropna=False)
            .size()
            .sort_values(ascending=False)
            .reset_index(name="count")
        )
        report["by_symbol"] = by_symbol.head(10).to_dict(orient="records")

    return report


def golden_hours_analysis(days: int = 30) -> Dict[str, Any]:
    """Find best/worst UTC hours by entry timestamp for CLOSED trades."""
    report: Dict[str, Any] = {"days": days, "top_hours": [], "bottom_hours": [], "total_closed": 0}

    with get_connection() as con:
        if not _table_exists(con, "trades"):
            report["note"] = "Table `trades` not found."
            return report

        df = _safe_read_sql(
            con,
            "SELECT timestamp, pnl, market_type, strategy_tag, exit_profile FROM trades WHERE status='CLOSED' ORDER BY id DESC",
        )
        if df.empty:
            report["note"] = "No CLOSED trades."
            return report

        df["ts"] = _to_utc_dt(df["timestamp"])
        cutoff = _utc_now() - timedelta(days=days)
        df = df[df["ts"] >= cutoff]
        report["total_closed"] = int(len(df))

        if df.empty:
            report["note"] = "No CLOSED trades in time window."
            return report

        df["hour_utc"] = df["ts"].dt.hour
        df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)

        agg = (
            df.groupby("hour_utc")
            .agg(trades=("pnl", "size"), pnl_sum=("pnl", "sum"), pnl_avg=("pnl", "mean"), wins=("pnl", lambda s: int((s > 0).sum())))
            .reset_index()
        )
        agg["win_rate"] = agg["wins"] / agg["trades"]
        agg = agg.sort_values("pnl_sum", ascending=False)

        report["top_hours"] = agg.head(5).to_dict(orient="records")
        report["bottom_hours"] = agg.tail(5).sort_values("pnl_sum", ascending=True).to_dict(orient="records")

    return report


def ai_confidence_check(days: int = 30) -> Dict[str, Any]:
    """Check how AI confidence correlates with outcomes."""
    report: Dict[str, Any] = {"days": days, "bins": [], "total_closed": 0, "note": None}

    with get_connection() as con:
        if not _table_exists(con, "trades"):
            report["note"] = "Table `trades` not found."
            return report

        df = _safe_read_sql(
            con,
            "SELECT timestamp, pnl, ai_confidence, confidence, market_type, strategy_tag FROM trades WHERE status='CLOSED' ORDER BY id DESC",
        )
        if df.empty:
            report["note"] = "No CLOSED trades."
            return report

        df["ts"] = _to_utc_dt(df["timestamp"])
        cutoff = _utc_now() - timedelta(days=days)
        df = df[df["ts"] >= cutoff]
        report["total_closed"] = int(len(df))

        if df.empty:
            report["note"] = "No CLOSED trades in time window."
            return report

        # prefer ai_confidence if available; else fallback to confidence
        conf = pd.to_numeric(df.get("ai_confidence"), errors="coerce")
        if conf.isna().all():
            conf = pd.to_numeric(df.get("confidence"), errors="coerce")
        df["conf"] = conf.fillna(0.0).clip(0.0, 1.0)

        df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
        df["bin"] = df["conf"].apply(_bin_confidence)

        agg = (
            df.groupby("bin")
            .agg(trades=("pnl", "size"), pnl_sum=("pnl", "sum"), pnl_avg=("pnl", "mean"), wins=("pnl", lambda s: int((s > 0).sum())))
            .reset_index()
        )
        agg["win_rate"] = agg["wins"] / agg["trades"]
        # sort by bin numeric start
        agg["_k"] = agg["bin"].str.split("-").str[0].astype(float)
        agg = agg.sort_values("_k").drop(columns=["_k"])

        report["bins"] = agg.to_dict(orient="records")

    return report


def pump_performance(days: int = 60) -> Dict[str, Any]:
    """Evaluate pump_score vs outcomes (from trades.explain json or analysis_results.pump_score)."""
    report: Dict[str, Any] = {"days": days, "groups": [], "source": None, "note": None}

    with get_connection() as con:
        cutoff = _utc_now() - timedelta(days=days)

        # Prefer analysis_results if present and populated
        if _table_exists(con, "analysis_results"):
            df = _safe_read_sql(
                con,
                "SELECT signal_time, pump_score, pnl FROM analysis_results WHERE pump_score IS NOT NULL ORDER BY id DESC",
            )
            if not df.empty and "signal_time" in df.columns:
                df["ts"] = _to_utc_dt(df["signal_time"])
                df = df[df["ts"] >= cutoff]
                if not df.empty:
                    report["source"] = "analysis_results"
                    return _pump_score_group_report(df, report)

        # Fallback: parse trades.explain json
        if not _table_exists(con, "trades"):
            report["note"] = "No trades table."
            return report

        df = _safe_read_sql(
            con,
            "SELECT timestamp, pnl, explain FROM trades WHERE status='CLOSED' ORDER BY id DESC",
        )
        if df.empty:
            report["note"] = "No CLOSED trades."
            return report

        df["ts"] = _to_utc_dt(df["timestamp"])
        df = df[df["ts"] >= cutoff]
        if df.empty:
            report["note"] = "No CLOSED trades in time window."
            return report

        pump_scores: List[Optional[float]] = []
        for s in df["explain"].tolist():
            obj = _try_parse_json(s)
            ps = obj.get("pump_score")
            try:
                pump_scores.append(float(ps) if ps is not None else None)
            except Exception:
                pump_scores.append(None)

        df["pump_score"] = pump_scores
        df = df.dropna(subset=["pump_score"])
        if df.empty:
            report["note"] = "No pump_score found in explain or analysis_results."
            return report

        report["source"] = "trades.explain"
        return _pump_score_group_report(df, report)


def _pump_score_group_report(df: pd.DataFrame, report: Dict[str, Any]) -> Dict[str, Any]:
    df = df.copy()
    df["pnl"] = pd.to_numeric(df.get("pnl"), errors="coerce").fillna(0.0)
    df["pump_score"] = pd.to_numeric(df.get("pump_score"), errors="coerce").fillna(0.0)

    # Grouping
    bins = [0, 40, 60, 75, 90, 101]
    labels = ["0-39", "40-59", "60-74", "75-89", "90-100"]
    df["group"] = pd.cut(df["pump_score"], bins=bins, labels=labels, right=False, include_lowest=True)

    agg = (
        df.groupby("group")
        .agg(trades=("pnl", "size"), pnl_sum=("pnl", "sum"), pnl_avg=("pnl", "mean"), wins=("pnl", lambda s: int((s > 0).sum())))
        .reset_index()
    )
    agg["win_rate"] = agg["wins"] / agg["trades"]
    report["groups"] = agg.to_dict(orient="records")
    report["total"] = int(len(df))
    return report


# -----------------------------
# Report orchestration
# -----------------------------
def generate_full_report(store: bool = True) -> Dict[str, Any]:
    report = {
        "generated_at_utc": _utc_iso(),
        "db_file": _get_db_file(),
        "rejections": analyze_rejections(days=14),
        "golden_hours": golden_hours_analysis(days=30),
        "ai_confidence": ai_confidence_check(days=30),
        "pump_performance": pump_performance(days=60),
    }

    # Create a few concise suggestions
    suggestions: List[str] = []

    # Rejections
    rej = report["rejections"]
    if rej.get("total", 0) > 0 and rej.get("by_reason"):
        top_reason = rej["by_reason"][0]
        suggestions.append(f"Most common rejection reason (last {rej['days']}d): {top_reason.get('reason')} ({top_reason.get('count')}).")

    # Golden hours
    gh = report["golden_hours"]
    if gh.get("total_closed", 0) > 10 and gh.get("top_hours"):
        best = gh["top_hours"][0]
        suggestions.append(f"Best UTC hour by PnL (last {gh['days']}d): {best.get('hour_utc')}:00 (pnl_sum={best.get('pnl_sum'):.2f}).")

    # Confidence bins
    ac = report["ai_confidence"]
    if ac.get("bins"):
        # find best bin by pnl_sum
        try:
            best_bin = max(ac["bins"], key=lambda r: float(r.get("pnl_sum", 0)))
            suggestions.append(f"Best confidence bin: {best_bin.get('bin')} (pnl_sum={best_bin.get('pnl_sum'):.2f}, win_rate={best_bin.get('win_rate'):.2%}).")
        except Exception:
            pass

    # Pump groups
    pp = report["pump_performance"]
    if pp.get("groups"):
        try:
            best_group = max(pp["groups"], key=lambda r: float(r.get("pnl_sum", 0)))
            suggestions.append(f"Pump-score group with best pnl_sum: {best_group.get('group')} (pnl_sum={best_group.get('pnl_sum'):.2f}).")
        except Exception:
            pass

    report["suggestions"] = suggestions

    if store:
        _store_report(report)

    # Print a human-friendly summary
    _print_report(report)
    return report


def _store_report(report: Dict[str, Any]) -> None:
    """Store the report JSON into settings key + log a short summary."""
    try:
        db = DatabaseManager()
        db.set_setting("last_analysis_report_json", json.dumps(report))
        db.set_setting("last_analysis_report_utc", report.get("generated_at_utc", _utc_iso()))
        # Optional: add a short log line
        if report.get("suggestions"):
            db.log(f"[ANALYZER] Stored report. Top: {report['suggestions'][0]}", level="INFO")
        else:
            db.log("[ANALYZER] Stored report.", level="INFO")
    except Exception:
        # fail silently: analyzer should not crash
        pass


def _print_report(report: Dict[str, Any]) -> None:
    print("=" * 44)
    print("📊 TRADEPRO ANALYST REPORT")
    print("=" * 44)
    print(f"UTC: {report.get('generated_at_utc')}")
    print(f"DB : {report.get('db_file')}")
    print()

    # Rejections
    rej = report.get("rejections", {})
    print("1) Reject Analysis")
    if rej.get("total", 0) == 0:
        print(f"   - No recent rejections. {rej.get('note','')}".strip())
    else:
        print(f"   - Total (last {rej.get('days')}d): {rej.get('total')}")
        for r in (rej.get("by_reason") or [])[:5]:
            print(f"     • {r.get('reason')}: {r.get('count')}")

    print()

    # Golden hours
    gh = report.get("golden_hours", {})
    print("2) Golden Hours (UTC)")
    if gh.get("total_closed", 0) == 0:
        print(f"   - No closed trades. {gh.get('note','')}".strip())
    else:
        print(f"   - Closed trades (last {gh.get('days')}d): {gh.get('total_closed')}")
        if gh.get("top_hours"):
            best = gh["top_hours"][0]
            print(f"   - Best hour: {best.get('hour_utc')}:00  pnl_sum={best.get('pnl_sum'):.2f}  win_rate={best.get('win_rate'):.2%}")
        if gh.get("bottom_hours"):
            worst = gh["bottom_hours"][0]
            print(f"   - Worst hour: {worst.get('hour_utc')}:00  pnl_sum={worst.get('pnl_sum'):.2f}  win_rate={worst.get('win_rate'):.2%}")

    print()

    # AI confidence
    ac = report.get("ai_confidence", {})
    print("3) AI Confidence vs Outcome")
    if ac.get("total_closed", 0) == 0:
        print(f"   - No closed trades. {ac.get('note','')}".strip())
    else:
        print(f"   - Closed trades (last {ac.get('days')}d): {ac.get('total_closed')}")
        # Print a compact view (top 3 bins by pnl_sum)
        bins = ac.get("bins") or []
        if bins:
            best_bins = sorted(bins, key=lambda r: float(r.get("pnl_sum", 0)), reverse=True)[:3]
            for b in best_bins:
                print(f"     • bin {b.get('bin')}: trades={b.get('trades')} pnl_sum={b.get('pnl_sum'):.2f} win_rate={b.get('win_rate'):.2%}")

    print()

    # Pump performance
    pp = report.get("pump_performance", {})
    print("4) Pump Detector Performance")
    if not pp.get("groups"):
        print(f"   - {pp.get('note','No pump data available.')}".strip())
    else:
        print(f"   - Source: {pp.get('source')}  (n={pp.get('total')})")
        for g in pp.get("groups", []):
            print(f"     • {g.get('group')}: trades={g.get('trades')} pnl_sum={g.get('pnl_sum'):.2f} win_rate={g.get('win_rate'):.2%}")

    print()

    print("✅ Suggestions")
    for s in report.get("suggestions", []) or ["(no suggestions yet — need more closed trades/data)"]:
        print(f"   - {s}")
    print("=" * 44)


if __name__ == "__main__":
    # Default: generate and store report
    generate_full_report(store=True)
