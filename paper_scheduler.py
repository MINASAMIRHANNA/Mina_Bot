# paper_scheduler.py
"""Paper Tournament daily scheduler.

This module runs the Paper Tournament daily routine once per UTC day, at a configured
UTC time (HH:MM). It is intentionally lightweight and thread-based so it can be
started from `main.py` or anywhere else without requiring an async loop.

Key settings (stored in DB `settings` table, editable from dashboard later):
- paper_daily_enabled: "1"/"0" (default: 1)
- paper_daily_time_utc: "HH:MM" (default: 00:05)
- paper_daily_last_run_utc: "YYYY-MM-DD" (set after a successful trigger)
Back-compat:
- paper_daily_last_reco_utc: legacy key; we read/write it too to avoid breaking older code.
"""

from __future__ import annotations

import inspect
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_today_str() -> str:
    return _utc_now().date().isoformat()


def _parse_hhmm(s: str) -> Tuple[int, int]:
    """Parse HH:MM into (hour, minute). Defaults to (0, 5) on failure."""
    try:
        hh, mm = (s or "").strip().split(":")
        hh_i = max(0, min(23, int(hh)))
        mm_i = max(0, min(59, int(mm)))
        return hh_i, mm_i
    except Exception:
        return 0, 5


def _boolish(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _safe_log(db: Any, msg: str, level: str = "INFO") -> None:
    """Log to DB if possible; otherwise print."""
    try:
        if hasattr(db, "log") and callable(getattr(db, "log")):
            db.log(msg, level)
        else:
            print(f"[{level}] {msg}")
    except Exception:
        # last resort
        try:
            print(f"[{level}] {msg}")
        except Exception:
            pass


def _get_setting(db: Any, key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        if hasattr(db, "get_setting") and callable(getattr(db, "get_setting")):
            v = db.get_setting(key)
            return v if v is not None else default
    except Exception:
        pass
    return default


def _set_setting(db: Any, key: str, value: str) -> None:
    try:
        if hasattr(db, "set_setting") and callable(getattr(db, "set_setting")):
            db.set_setting(key, value)
    except Exception:
        pass


def _call_run_daily(run_daily_fn: Callable[..., Any], db: Any) -> None:
    """Call run_daily_fn with backward-compatible signature handling."""
    try:
        sig = inspect.signature(run_daily_fn)
        if "allow_reco" in sig.parameters:
            run_daily_fn(db, allow_reco=True)
        else:
            run_daily_fn(db)
    except TypeError:
        # Fallback if signature introspection is misleading
        try:
            run_daily_fn(db, allow_reco=True)
        except Exception:
            run_daily_fn(db)


def start_paper_scheduler(
    db: Any,
    run_daily_fn: Callable[..., Any],
    *,
    poll_seconds: int = 10,
    default_time_utc: str = "00:05",
    stop_event: Optional[threading.Event] = None,
) -> threading.Thread:
    """Start the paper daily scheduler in a daemon thread.

    Args:
        db: DatabaseManager-like instance (must have get_setting/set_setting/log ideally).
        run_daily_fn: function that runs the paper daily routine. Accepts either:
            - run_daily_fn(db)
            - run_daily_fn(db, allow_reco=True)
        poll_seconds: scheduler polling interval (seconds).
        default_time_utc: default HH:MM UTC run time if not found in settings.
        stop_event: optional threading.Event for stopping the loop.

    Returns:
        The started daemon Thread.
    """
    poll_seconds = max(1, int(poll_seconds or 10))
    _stop = stop_event or threading.Event()

    def loop() -> None:
        _safe_log(db, f"[PAPER_SCHED] started (poll={poll_seconds}s)")
        while not _stop.is_set():
            try:
                enabled = _boolish(_get_setting(db, "paper_daily_enabled", "1"), True)
                if not enabled:
                    time.sleep(poll_seconds)
                    continue

                run_time = _get_setting(db, "paper_daily_time_utc", default_time_utc) or default_time_utc
                hh, mm = _parse_hhmm(run_time)

                now = _utc_now()
                today = now.date().isoformat()

                # Backward compatible last run key
                last_run = _get_setting(db, "paper_daily_last_run_utc") or _get_setting(db, "paper_daily_last_reco_utc")
                already_ran_today = (last_run == today)

                due = (now.hour > hh) or (now.hour == hh and now.minute >= mm)

                if due and not already_ran_today:
                    _safe_log(db, f"[PAPER_DAILY] trigger at {now.isoformat()}Z (scheduled {hh:02d}:{mm:02d} UTC)")
                    # Set before executing to avoid double-trigger if process restarts mid-run.
                    _set_setting(db, "paper_daily_last_run_utc", today)
                    _set_setting(db, "paper_daily_last_reco_utc", today)  # legacy key
                    try:
                        _call_run_daily(run_daily_fn, db)
                        _safe_log(db, "[PAPER_DAILY] completed", level="INFO")
                    except Exception as e:
                        _safe_log(db, f"[PAPER_DAILY] error: {e}", level="ERROR")

            except Exception as e:
                _safe_log(db, f"[PAPER_SCHED] error: {e}", level="ERROR")

            # small sleep with stop check
            for _ in range(poll_seconds):
                if _stop.is_set():
                    break
                time.sleep(1)

        _safe_log(db, "[PAPER_SCHED] stopped", level="INFO")

    t = threading.Thread(target=loop, daemon=True, name="paper_scheduler")
    t.start()
    return t


def stop_paper_scheduler(stop_event: threading.Event) -> None:
    """Signal a previously provided stop_event to stop the scheduler loop."""
    try:
        stop_event.set()
    except Exception:
        pass
