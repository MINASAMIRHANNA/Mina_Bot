"""config.py (Zero-ENV, DB-backed)

✅ Goal (per user requirement): **Zero .env / Zero OS environment variables**.
All runtime configuration is stored in SQLite (settings table in bot_data.db)
so the Dashboard is the single source of truth.

Key design points:
- No dependency on python-dotenv.
- Backward compatible: other modules still import `from config import cfg`.
- Safe defaults if DB/settings are missing.
- UTC is enforced everywhere.

Settings storage:
- SQLite table: settings(key TEXT PRIMARY KEY, value TEXT)

Conventions:
- Dashboard "Env Secrets" panel writes keys like:
  BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_BOT_TOKEN, ...
  USE_TESTNET, ENABLE_LIVE_TRADING, HEARTBEAT_INTERVAL, ...
- We also support legacy keys if they exist (e.g. PAPER_TRADING).

"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------
# DB path discovery (no env)
# ---------------------------------------------------------------------
_BASE_DIR = Path(__file__).resolve().parent


def _discover_db_file() -> str:
    """Find the most likely bot_data.db.

    Priority:
    1) bot_data.db in current working directory
    2) bot_data.db next to this config.py
    3) bot_data.db in project root (parent)
    4) fallback to config.py dir (will be created)
    """
    candidates = [
        Path(os.getcwd()) / "bot_data.db",
        _BASE_DIR / "bot_data.db",
        _BASE_DIR.parent / "bot_data.db",
        _BASE_DIR.parent.parent / "bot_data.db",
    ]
    for c in candidates:
        try:
            if c.exists():
                return str(c)
        except Exception:
            continue
    return str(_BASE_DIR / "bot_data.db")


def _read_settings(db_file: str) -> Dict[str, str]:
    """Read settings table (best-effort)."""
    settings: Dict[str, str] = {}
    try:
        con = sqlite3.connect(db_file, timeout=5)
        try:
            con.execute(
                "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"
            )
            rows = con.execute("SELECT key, value FROM settings").fetchall()
            for k, v in rows:
                if k is None:
                    continue
                settings[str(k)] = "" if v is None else str(v)
        finally:
            con.close()
    except Exception:
        return settings
    return settings


def _as_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    s = str(v)
    return s if s.strip() != "" else default


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _as_int(v: Any, default: int) -> int:
    try:
        if v is None or str(v).strip() == "":
            return int(default)
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _as_float(v: Any, default: float) -> float:
    try:
        if v is None or str(v).strip() == "":
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


class Config:
    """DB-backed configuration container."""

    def __init__(self) -> None:
        self.DB_FILE: str = _discover_db_file()
        self.TIMEZONE: str = "UTC"
        self._settings: Dict[str, str] = {}
        self.reload()

    # -------------------------
    # Public helpers
    # -------------------------
    def get_raw(self, key: str, default: Optional[str] = None) -> Optional[str]:
        if key in self._settings:
            return self._settings.get(key)
        # also allow lowercase fallback
        lk = key.lower()
        if lk in self._settings:
            return self._settings.get(lk)
        return default

    def reload(self) -> None:
        self._settings = _read_settings(self.DB_FILE)

        # --- Credentials ---
        self.BINANCE_API_KEY = _as_str(self.get_raw("BINANCE_API_KEY") or self.get_raw("binance_api_key"), "")
        self.BINANCE_API_SECRET = _as_str(self.get_raw("BINANCE_API_SECRET") or self.get_raw("binance_api_secret"), "")

        self.TELEGRAM_BOT_TOKEN = _as_str(self.get_raw("TELEGRAM_BOT_TOKEN") or self.get_raw("TG_BOT_TOKEN"), "")
        self.TELEGRAM_CHAT_ID = _as_str(self.get_raw("TELEGRAM_CHAT_ID"), "")
        self.TELEGRAM_ERROR_CHAT_ID = _as_str(self.get_raw("TELEGRAM_ERROR_CHAT_ID"), "")

        # --- System / Mode ---
        self.USE_TESTNET = _as_bool(self.get_raw("USE_TESTNET"), True)

        # Dashboard writes ENABLE_LIVE_TRADING; legacy uses PAPER_TRADING.
        paper_legacy = self.get_raw("PAPER_TRADING")
        if paper_legacy is not None and str(paper_legacy).strip() != "":
            self.PAPER_TRADING = _as_bool(paper_legacy, True)
            self.ENABLE_LIVE_TRADING = not self.PAPER_TRADING
        else:
            self.ENABLE_LIVE_TRADING = _as_bool(self.get_raw("ENABLE_LIVE_TRADING"), False)
            self.PAPER_TRADING = not bool(self.ENABLE_LIVE_TRADING)

        self.RUN_MODE = _as_str(self.get_raw("RUN_MODE"), "")
        self.HEARTBEAT_INTERVAL = _as_int(self.get_raw("HEARTBEAT_INTERVAL"), 14400)
        self.WS_CHUNK_SIZE = _as_int(self.get_raw("WS_CHUNK_SIZE"), 50)
        self.WS_CALLBACK_CONCURRENCY = _as_int(self.get_raw("WS_CALLBACK_CONCURRENCY"), 0)

        # --- REST/WS Bases ---
        default_rest = "https://demo-fapi.binance.com" if self.USE_TESTNET else "https://fapi.binance.com"
        default_ws = "wss://stream.binancefuture.com" if self.USE_TESTNET else "wss://fstream.binance.com"
        self.BINANCE_FUTURES_REST_BASE = _as_str(self.get_raw("BINANCE_FUTURES_REST_BASE"), default_rest).rstrip("/")
        self.BINANCE_FUTURES_WS_BASE = _as_str(self.get_raw("BINANCE_FUTURES_WS_BASE"), default_ws).rstrip("/")

        # --- Risk ---
        self.TRAIL_TRIGGER = _as_float(self.get_raw("TRAIL_TRIGGER"), 0.01)
        self.DEFAULT_RISK_PCT = _as_float(self.get_raw("DEFAULT_RISK_PCT"), 1.0)
        self.LEVERAGE_SCALP = _as_int(self.get_raw("LEVERAGE_SCALP"), 20)
        self.LEVERAGE_SWING = _as_int(self.get_raw("LEVERAGE_SWING"), 10)
        self.MAX_LEVERAGE = _as_int(self.get_raw("MAX_LEVERAGE"), 20)

        # --- Strategy / Scanner ---
        self.SL_MULTIPLIER = _as_float(self.get_raw("SL_MULTIPLIER"), 1.5)
        self.TP_MULTIPLIER = _as_float(self.get_raw("TP_MULTIPLIER"), 2.0)
        self.ENTRY_THRESHOLD = _as_float(self.get_raw("ENTRY_THRESHOLD"), 0.2)
        self.WATCHLIST_VOL_LIMIT = _as_int(self.get_raw("WATCHLIST_VOL_LIMIT"), 150)
        self.WATCHLIST_GAINERS_LIMIT = _as_int(self.get_raw("WATCHLIST_GAINERS_LIMIT"), 50)
        self.MAX_CONCURRENT_TRADES = _as_int(self.get_raw("MAX_CONCURRENT_TRADES"), 10)
        self.MAX_CONCURRENT_TASKS = _as_int(self.get_raw("MAX_CONCURRENT_TASKS"), 30)
        self.STARTUP_SCAN_LIMIT = _as_int(self.get_raw("STARTUP_SCAN_LIMIT"), 50)

        # --- Webhooks / Dashboard publish ---
        self.WEBHOOK_URL = _as_str(self.get_raw("WEBHOOK_URL") or self.get_raw("webhook_url"), "")
        self.BOT_NAME = _as_str(self.get_raw("BOT_NAME"), "TradePro_v1")

        self.DASHBOARD_URL = _as_str(self.get_raw("DASHBOARD_URL"), "http://localhost:8000")
        self.DASHBOARD_PUBLISH_URL = _as_str(self.get_raw("DASHBOARD_PUBLISH_URL"), "")
        self.DASHBOARD_PUBLISH_TOKEN = _as_str(self.get_raw("DASHBOARD_PUBLISH_TOKEN"), "")
        self.DASHBOARD_ENVELOPE = _as_bool(self.get_raw("DASHBOARD_ENVELOPE"), False)

        # --- Websocket runtime knobs (migrated from env -> DB) ---
        self.FUTURES_WS_TIMEFRAMES = _as_str(self.get_raw("FUTURES_WS_TIMEFRAMES"), "1m,5m,1h")
        self.SPOT_WS_TIMEFRAME = _as_str(self.get_raw("SPOT_WS_TIMEFRAME"), "1m")
        self.FUTURES_WS_ENABLED = _as_bool(self.get_raw("FUTURES_WS_ENABLED"), True)
        self.SPOT_WS_ENABLED = _as_bool(self.get_raw("SPOT_WS_ENABLED"), True)
        self.FUTURES_SYMBOL_LIMIT = _as_int(self.get_raw("FUTURES_SYMBOL_LIMIT"), 0)
        self.SPOT_SYMBOL_LIMIT = _as_int(self.get_raw("SPOT_SYMBOL_LIMIT"), 0)
        self.RELOAD_MODELS_COOLDOWN_SEC = _as_int(self.get_raw("RELOAD_MODELS_COOLDOWN_SEC"), 60)

        # --- DataManager / OI knobs ---
        self.OI_LOG_ERRORS = _as_bool(self.get_raw("OI_LOG_ERRORS"), False)
        self.OI_ERROR_THROTTLE_SEC = _as_float(self.get_raw("OI_ERROR_THROTTLE_SEC"), 60.0)
        self.OI_SYMBOL_DELAY_SEC = _as_float(self.get_raw("OI_SYMBOL_DELAY_SEC"), 1.0)
        self.OI_CYCLE_DELAY_SEC = _as_float(self.get_raw("OI_CYCLE_DELAY_SEC"), 30.0)
        self.OI_BASELINE_WINDOW_SEC = _as_float(self.get_raw("OI_BASELINE_WINDOW_SEC"), 300.0)

        # --- Model flags ---
        self.AI_DISABLE_SHORT = _as_bool(self.get_raw("AI_DISABLE_SHORT"), True)
        self.USE_CLOSED_LOOP_MODELS = _as_bool(self.get_raw("USE_CLOSED_LOOP_MODELS"), False)

        # --- Snapshot / Paper tournament (already DB-driven elsewhere; keep defaults here) ---
        self.SNAPSHOT_ENABLED = _as_bool(self.get_raw("SNAPSHOT_ENABLED") or self.get_raw("snapshot_enabled"), True)
        self.EQUITY_SNAPSHOT_EVERY_SEC = _as_int(self.get_raw("EQUITY_SNAPSHOT_EVERY_SEC") or self.get_raw("equity_snapshot_every_sec"), 60)
        self.SNAPSHOT_RETENTION_DAYS = _as_int(self.get_raw("SNAPSHOT_RETENTION_DAYS") or self.get_raw("snapshot_retention_days"), 30)

        self.PAPER_TOURNAMENT_ENABLED = _as_bool(self.get_raw("PAPER_TOURNAMENT_ENABLED") or self.get_raw("paper_tournament_enabled"), True)

        # --- Misc ---
        self.DISABLE_AUDIT_LAB = _as_bool(self.get_raw("DISABLE_AUDIT_LAB") or self.get_raw("disable_audit_lab"), False)

    def to_dict(self, include_secrets: bool = False) -> Dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        if not include_secrets:
            for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "TELEGRAM_BOT_TOKEN"):
                if d.get(k):
                    d[k] = "***"
        return d


# Singleton
cfg = Config()
