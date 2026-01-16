"""
learning_engine.py

Daily learning/adjustment utilities.

This module is intentionally lightweight and defensive:
- Works even if the DB schema is older (missing helper methods/tables).
- Uses UTC for all day boundaries (project rule).
- Stores learning stats + threshold adjustments in DB when possible, otherwise falls back to DB settings.

Expected DB capabilities (best case):
- db.get_all_closed_trades() -> list[dict]
- db.set_setting(key, value) / db.get_setting(key, default=None)
- db.log(message, level="INFO") (optional)

If sqlite connection/cursor/lock are exposed (DatabaseManager in this project does),
we also create/update:
- learning_stats
- threshold_adjustments
"""

from __future__ import annotations

from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import math


# ==========================================================
# Helpers (UTC + parsing)
# ==========================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_day_str(dt: Optional[datetime] = None) -> str:
    d = (dt or _utc_now()).date()
    return d.strftime("%Y-%m-%d")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return float(int(x))
        if isinstance(x, (int, float)):
            if math.isnan(x):
                return default
            return float(x)
        s = str(x).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _parse_ts_to_utc_date(value: Any) -> Optional[str]:
    """Parse a timestamp-like value into UTC day string YYYY-MM-DD."""
    if value is None:
        return None

    # numeric epoch (seconds or milliseconds)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            v = float(value)
            if v > 1e12:  # ms
                dt = datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
            elif v > 1e9:  # seconds (likely)
                dt = datetime.fromtimestamp(v, tz=timezone.utc)
            else:
                return None
            return _utc_day_str(dt)
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    # common ISO patterns
    try:
        # allow 'Z'
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return _utc_day_str(dt)
    except Exception:
        pass

    # last resort: try substring YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def _log(db: Any, msg: str, level: str = "INFO") -> None:
    try:
        if hasattr(db, "log") and callable(getattr(db, "log")):
            db.log(msg, level)
        else:
            print(f"[{level}] {msg}")
    except Exception:
        print(f"[{level}] {msg}")


def _db_has_sqlite_handles(db: Any) -> bool:
    return all(hasattr(db, a) for a in ("conn", "cursor", "lock"))


def _ensure_learning_tables(db: Any) -> None:
    if not _db_has_sqlite_handles(db):
        return
    try:
        with db.lock:
            db.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS learning_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day_utc TEXT NOT NULL,
                    strategy TEXT,
                    exit_profile TEXT,
                    trades INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    win_rate REAL DEFAULT 0.0,
                    pnl_sum REAL DEFAULT 0.0,
                    avg_pnl REAL DEFAULT 0.0,
                    avg_confidence REAL DEFAULT 0.0,
                    created_at TEXT
                )
                """
            )
            # unique constraint via unique index (used by REPLACE/UPSERT)
            db.cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_stats_key ON learning_stats(day_utc, strategy, exit_profile)"
            )

            db.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS threshold_adjustments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day_utc TEXT NOT NULL,
                    strategy TEXT,
                    exit_profile TEXT,
                    trades INTEGER DEFAULT 0,
                    win_rate REAL DEFAULT 0.0,
                    adjustment INTEGER DEFAULT 0,
                    meta TEXT,
                    created_at TEXT
                )
                """
            )
            db.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_threshold_adjust_day ON threshold_adjustments(day_utc)"
            )
            db.conn.commit()
    except Exception as e:
        _log(db, f"learning_engine: failed to ensure tables: {e}", "WARNING")


def _set_setting(db: Any, key: str, value: Any) -> None:
    """Store setting as string (JSON when needed)."""
    try:
        v = value
        if not isinstance(v, str):
            v = json.dumps(v, ensure_ascii=False)
        if hasattr(db, "set_setting") and callable(getattr(db, "set_setting")):
            db.set_setting(key, v)
        else:
            # last resort: sqlite direct
            if _db_has_sqlite_handles(db):
                with db.lock:
                    db.cursor.execute(
                        "INSERT OR REPLACE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                        (key, v, _utc_now().isoformat().replace("+00:00", "Z")),
                    )
                    db.conn.commit()
    except Exception as e:
        _log(db, f"learning_engine: failed to set setting {key}: {e}", "WARNING")


def _get_setting(db: Any, key: str, default: Any = None) -> Any:
    try:
        if hasattr(db, "get_setting") and callable(getattr(db, "get_setting")):
            return db.get_setting(key, default)
    except Exception:
        pass
    return default


# ==========================================================
# DAILY PERFORMANCE ANALYSIS
# ==========================================================
def analyze_daily_performance(db: Any, day_utc: Optional[str] = None) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Analyze closed trades for a specific UTC day and aggregate performance per (strategy, exit_profile).

    Returns:
      { (strategy, exit_profile): {
          'trades': int, 'wins': int, 'losses': int,
          'win_rate': float, 'pnl_sum': float, 'avg_pnl': float,
          'avg_confidence': float,
        }, ... }
    """
    day = day_utc or _utc_day_str()
    stats = defaultdict(lambda: {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "pnl_sum": 0.0,
        "confidence": [],  # list[float]
    })

    # Pull trades (best effort)
    trades: List[Dict[str, Any]] = []
    try:
        if hasattr(db, "get_all_closed_trades") and callable(getattr(db, "get_all_closed_trades")):
            trades = db.get_all_closed_trades() or []
        elif hasattr(db, "get_closed_trades") and callable(getattr(db, "get_closed_trades")):
            trades = db.get_closed_trades() or []
    except Exception as e:
        _log(db, f"learning_engine: failed to fetch closed trades: {e}", "WARNING")
        trades = []

    for r in trades:
        if not isinstance(r, dict):
            continue

        closed_day = _parse_ts_to_utc_date(
            r.get("closed_at")
            or r.get("close_time")
            or r.get("closed_time")
            or r.get("timestamp")
            or r.get("updated_at")
        )
        if closed_day != day:
            continue

        strategy = (r.get("strategy") or "unknown").strip() if isinstance(r.get("strategy"), str) else (r.get("strategy") or "unknown")
        exit_profile = (r.get("exit_profile") or "default").strip() if isinstance(r.get("exit_profile"), str) else (r.get("exit_profile") or "default")
        key = (str(strategy), str(exit_profile))

        pnl = _safe_float(r.get("realized_pnl"), None)
        if pnl is None:
            pnl = _safe_float(r.get("pnl"), 0.0)

        conf = _safe_float(r.get("confidence"), None)
        if conf is None:
            conf = _safe_float(r.get("ml_confidence"), 0.0)

        stats[key]["trades"] += 1
        stats[key]["pnl_sum"] += pnl
        if pnl > 0:
            stats[key]["wins"] += 1
        elif pnl < 0:
            stats[key]["losses"] += 1

        if conf is not None:
            stats[key]["confidence"].append(conf)

    # Finalize derived fields
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for key, s in stats.items():
        t = int(s["trades"])
        wins = int(s["wins"])
        losses = int(s["losses"])
        pnl_sum = float(s["pnl_sum"])
        win_rate = (wins / t) if t else 0.0
        avg_pnl = (pnl_sum / t) if t else 0.0
        conf_list = s.get("confidence") or []
        avg_conf = (sum(conf_list) / len(conf_list)) if conf_list else 0.0
        out[key] = {
            "trades": t,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "pnl_sum": pnl_sum,
            "avg_pnl": avg_pnl,
            "avg_confidence": avg_conf,
        }

    return out


# ==========================================================
# STORE LEARNING STATS
# ==========================================================
def store_learning_stats(db: Any, stats: Dict[Tuple[str, str], Dict[str, Any]], day_utc: Optional[str] = None) -> None:
    """
    Persist learning stats. Prefers sqlite table `learning_stats`, otherwise falls back to a settings blob.
    """
    day = day_utc or _utc_day_str()
    _ensure_learning_tables(db)

    # DB table path
    if _db_has_sqlite_handles(db):
        try:
            now_iso = _utc_now().isoformat().replace("+00:00", "Z")
            with db.lock:
                for (strategy, exit_profile), s in stats.items():
                    trades = int(s.get("trades", 0) or 0)
                    wins = int(s.get("wins", 0) or 0)
                    losses = int(s.get("losses", 0) or 0)
                    win_rate = float(s.get("win_rate", 0.0) or 0.0)
                    pnl_sum = float(s.get("pnl_sum", 0.0) or 0.0)
                    avg_pnl = float(s.get("avg_pnl", 0.0) or 0.0)
                    avg_conf = float(s.get("avg_confidence", 0.0) or 0.0)

                    # Insert/replace (requires unique index)
                    db.cursor.execute(
                        """
                        INSERT OR REPLACE INTO learning_stats
                        (day_utc, strategy, exit_profile, trades, wins, losses, win_rate, pnl_sum, avg_pnl, avg_confidence, created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (day, str(strategy), str(exit_profile), trades, wins, losses, win_rate, pnl_sum, avg_pnl, avg_conf, now_iso),
                    )
                db.conn.commit()
            return
        except Exception as e:
            _log(db, f"learning_engine: failed to persist learning_stats table: {e}", "WARNING")

    # Fallback: settings blob
    blob: Dict[str, Any] = {}
    for (strategy, exit_profile), s in stats.items():
        blob[f"{strategy}::{exit_profile}"] = s
    _set_setting(db, f"learning_stats:{day}", blob)
    _set_setting(db, "learning_stats_latest_day_utc", day)


# ==========================================================
# ADAPT THRESHOLDS (simple rule-based)
# ==========================================================
def adapt_thresholds(db: Any, day_utc: Optional[str] = None) -> Dict[Tuple[str, str], int]:
    """
    Produce threshold adjustments per (strategy, exit_profile) based on today's performance.

    Rule (default):
      - win_rate < 0.40 => +5 (stricter)
      - win_rate > 0.60 => -5 (more aggressive)
      - else => 0

    The unit "5" is intentionally kept as an integer "points" (not a direct percentage),
    so downstream code can interpret it as it prefers (e.g., +0.05 on ENTRY_THRESHOLD).
    """
    day = day_utc or _utc_day_str()
    min_trades = int(_safe_float(_get_setting(db, "learning_min_trades", 10), 10))

    stats = analyze_daily_performance(db, day)
    adjustments: Dict[Tuple[str, str], int] = {}

    for key, s in stats.items():
        trades = int(s.get("trades", 0) or 0)
        if trades < min_trades:
            adjustments[key] = 0
            continue

        win_rate = float(s.get("win_rate", 0.0) or 0.0)
        if win_rate < 0.40:
            adjustments[key] = +5
        elif win_rate > 0.60:
            adjustments[key] = -5
        else:
            adjustments[key] = 0

    # Persist adjustments
    _ensure_learning_tables(db)

    # Preferred: call method if available
    if hasattr(db, "store_threshold_adjustments") and callable(getattr(db, "store_threshold_adjustments")):
        try:
            db.store_threshold_adjustments(adjustments)
            return adjustments
        except Exception as e:
            _log(db, f"learning_engine: db.store_threshold_adjustments failed: {e}", "WARNING")

    # Store into sqlite table (history) + settings (current)
    try:
        now_iso = _utc_now().isoformat().replace("+00:00", "Z")
        if _db_has_sqlite_handles(db):
            with db.lock:
                for (strategy, exit_profile), adj in adjustments.items():
                    s = stats.get((strategy, exit_profile), {})
                    db.cursor.execute(
                        """
                        INSERT INTO threshold_adjustments
                        (day_utc, strategy, exit_profile, trades, win_rate, adjustment, meta, created_at)
                        VALUES (?,?,?,?,?,?,?,?)
                        """,
                        (
                            day,
                            str(strategy),
                            str(exit_profile),
                            int(s.get("trades", 0) or 0),
                            float(s.get("win_rate", 0.0) or 0.0),
                            int(adj),
                            json.dumps({"pnl_sum": s.get("pnl_sum", 0.0), "avg_pnl": s.get("avg_pnl", 0.0)}, ensure_ascii=False),
                            now_iso,
                        ),
                    )
                db.conn.commit()
    except Exception as e:
        _log(db, f"learning_engine: failed to write threshold_adjustments table: {e}", "WARNING")

    # Always keep a "current" blob for fast reads
    blob = {f"{k[0]}::{k[1]}": v for k, v in adjustments.items()}
    _set_setting(db, "threshold_adjustments", blob)
    _set_setting(db, "threshold_adjustments_day_utc", day)

    return adjustments


# ==========================================================
# CLI (optional)
# ==========================================================
if __name__ == "__main__":
    try:
        from database import DatabaseManager

        db = DatabaseManager()
        day = _utc_day_str()
        stats = analyze_daily_performance(db, day)
        store_learning_stats(db, stats, day)
        adj = adapt_thresholds(db, day)
        print(json.dumps({"day_utc": day, "stats": stats, "adjustments": {f'{k[0]}::{k[1]}': v for k, v in adj.items()}}, indent=2))
    except Exception as e:
        print(f"learning_engine CLI error: {e}")
