"""
risk_monitor.py

Risk Supervisor / Guardian process.

Goals:
- Enforce project rules: UTC everywhere.
- Monitor account equity and stop trading when risk limits hit.
- On trigger: flip bot to TEST mode + set kill_switch + attempt to close positions.
- Log equity snapshots for charts (compatible with both old and new DB schemas).

This module is designed to be safe even if some optional DB methods are missing.
"""

from __future__ import annotations

import time
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from binance.client import Client


class _NoPingClient(Client):
    """Disable spot ping() during init to avoid Spot-Testnet 502 killing futures modules."""
    def ping(self):
        try:
            return {}
        except Exception:
            return {}

def _futures_rest_base(use_testnet: bool) -> str:
    env = str(getattr(cfg, "BINANCE_FUTURES_REST_BASE", "") or "").strip()
    if env:
        return env.rstrip("/")
    return "https://demo-fapi.binance.com" if use_testnet else "https://fapi.binance.com"

def _configure_futures_endpoints(c: Client, use_testnet: bool) -> None:
    rest_base = _futures_rest_base(use_testnet)
    candidates = {
        "FUTURES_URL": f"{rest_base}/fapi",
        "FUTURES_TESTNET_URL": f"{rest_base}/fapi",
        "FUTURES_DATA_URL": f"{rest_base}/futures/data",
        "FUTURES_TESTNET_DATA_URL": f"{rest_base}/futures/data",
    }
    for attr, url in candidates.items():
        if hasattr(c, attr):
            try:
                setattr(c, attr, url)
            except Exception:
                pass


from config import cfg
from database import DatabaseManager

# ----------------------------
# Defaults (can be overridden by DB settings or env/config)
# ----------------------------
DEFAULT_MAX_DAILY_LOSS_USD = 50.0         # absolute loss vs day-start balance
DEFAULT_MAX_DRAWDOWN_PCT = 0.05           # equity drawdown vs day-start balance (0.05 = 5%)
DEFAULT_INTERVAL_SEC = 10                # polling interval
DEFAULT_AUTO_RESET_DAILY = False         # keep kill_switch until manually cleared


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_date_key(prefix: str = "balance") -> str:
    # Use UTC day boundary
    return f"{prefix}_{_utc_now().strftime('%Y-%m-%d')}"


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _env_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


class RiskSupervisor:
    """
    Runs in a loop and enforces risk limits.

    It stores day-start balance in DB settings using a UTC key: balance_YYYY-MM-DD.
    It writes a kill switch in DB settings: kill_switch = "1" (enabled) or "0"/None (disabled).
    """

    def __init__(self) -> None:
        self.db = DatabaseManager()
        self.client: Optional[Client] = None

        self.current_day_key = _utc_date_key("balance")
        self.start_balance = 0.0
        self.shutdown_triggered = False

        self.max_daily_loss_usd = self._get_setting_float("max_daily_loss_usd", DEFAULT_MAX_DAILY_LOSS_USD)
        self.max_drawdown_pct = self._get_setting_float("max_drawdown_pct", DEFAULT_MAX_DRAWDOWN_PCT)
        self.interval_sec = int(self._get_setting_float("risk_monitor_interval_sec", float(DEFAULT_INTERVAL_SEC)))
        self.auto_reset_daily = _env_bool(self._get_setting("auto_reset_daily_killswitch", str(DEFAULT_AUTO_RESET_DAILY)),
                                          DEFAULT_AUTO_RESET_DAILY)

        self._init_client()
        self._load_or_set_day_start_balance()

        try:
            self.db.log(
                f"RiskSupervisor started | start_balance={self.start_balance:.2f} | "
                f"max_daily_loss_usd={self.max_daily_loss_usd} | max_drawdown_pct={self.max_drawdown_pct} | "
                f"interval_sec={self.interval_sec}",
                level="INFO",
            )
        except Exception:
            pass

        print(f"🛡️ Risk Supervisor (UTC): Day Start Balance = ${self.start_balance:.2f}")

    # ----------------------------
    # DB setting helpers
    # ----------------------------
    def _get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        try:
            v = self.db.get_setting(key)
            return v if v is not None else default
        except Exception:
            return default

    def _get_setting_float(self, key: str, default: float) -> float:
        return _to_float(self._get_setting(key), default)

    # ----------------------------
    # Binance client
    # ----------------------------
    def _init_client(self) -> None:
        try:
            if not cfg.BINANCE_API_KEY or not cfg.BINANCE_API_SECRET:
                raise ValueError("Missing BINANCE API credentials")
            self.client = _NoPingClient(cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET, testnet=False)
            _configure_futures_endpoints(self.client, bool(getattr(cfg, "USE_TESTNET", True)))
        except Exception as e:
            self.client = None
            print(f"⚠️ Risk Supervisor Init Warn: {e}")

    def _futures_account(self) -> Optional[Dict[str, Any]]:
        if not self.client:
            return None
        try:
            return self.client.futures_account()
        except Exception:
            return None

    # ----------------------------
    # Day start balance (UTC)
    # ----------------------------
    def _load_or_set_day_start_balance(self) -> None:
        # If stored already, use it. Otherwise set to current wallet balance.
        stored = self._get_setting(self.current_day_key)
        if stored:
            self.start_balance = _to_float(stored, 0.0)
            return

        acct = self._futures_account()
        if not acct:
            self.start_balance = 0.0
            return

        bal = _to_float(acct.get("totalWalletBalance"), 0.0)
        self.start_balance = bal
        try:
            self.db.set_setting(self.current_day_key, str(bal))
        except Exception:
            pass


    def get_daily_start_balance(self) -> float:
        """Compatibility helper used by older tooling."""
        try:
            return float(getattr(self, "day_start_balance", 0.0) or 0.0)
        except Exception:
            return 0.0
    def _handle_day_rollover_if_needed(self) -> None:
        new_key = _utc_date_key("balance")
        if new_key == self.current_day_key:
            return

        # New UTC day: update start balance & optionally reset kill switch.
        self.current_day_key = new_key
        self.shutdown_triggered = False
        self._load_or_set_day_start_balance()

        if self.auto_reset_daily:
            try:
                self.db.set_setting("kill_switch", "0")
                self.db.set_setting("mode", "TEST")  # keep safe default unless user sets LIVE explicitly
            except Exception:
                pass

        try:
            self.db.log(f"UTC day rollover | new_start_balance={self.start_balance:.2f}", level="INFO")
        except Exception:
            pass

    # ----------------------------
    # Emergency actions
    # ----------------------------
    def _send_telegram(self, msg: str) -> None:
        try:
            if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
                return
            requests.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception:
            pass

    def _enqueue_close_all(self, reason: str) -> None:
        # Ask main bot loop to close all positions (if it implements it),
        # without relying on this monitor's ability to place orders.
        try:
            if hasattr(self.db, "add_command"):
                self.db.add_command("CLOSE_ALL_POSITIONS", {"reason": reason, "ts": _utc_now().isoformat()})
        except Exception:
            pass

    def _attempt_close_positions_direct(self) -> None:
        """Try to close all open futures positions directly (reduceOnly market orders)."""
        if not self.client:
            return
        try:
            positions = self.client.futures_position_information()
        except Exception:
            return

        for p in positions or []:
            try:
                amt = _to_float(p.get("positionAmt"), 0.0)
                if abs(amt) < 1e-12:
                    continue

                symbol = p.get("symbol")
                if not symbol:
                    continue

                side = "SELL" if amt > 0 else "BUY"
                qty = str(abs(amt))  # Binance returns positionAmt as a string typically

                params: Dict[str, Any] = {
                    "symbol": symbol,
                    "side": side,
                    "type": "MARKET",
                    "quantity": qty,
                    "reduceOnly": True,
                }

                # In hedge mode, positionSide may be LONG/SHORT. If present, pass it through.
                ps = p.get("positionSide")
                if ps and ps in {"LONG", "SHORT"}:
                    params["positionSide"] = ps

                self.client.futures_create_order(**params)
                try:
                    self.db.log(f"RiskSupervisor closed position: {symbol} {side} {qty} reduceOnly", level="WARN")
                except Exception:
                    pass
            except Exception:
                continue

    def emergency_shutdown(self, reason: str, details: Optional[Dict[str, Any]] = None) -> None:
        if self.shutdown_triggered:
            return
        self.shutdown_triggered = True

        # Determine if we are truly LIVE (otherwise avoid forcing kill_switch in TEST/PAPER mode)
        try:
            mode = str(self._get_setting('mode', 'TEST') or 'TEST').strip().upper()
        except Exception:
            mode = 'TEST'
        try:
            paper_flag = bool(getattr(cfg, 'PAPER_TRADING', False))
        except Exception:
            paper_flag = True
        try:
            enable_live = bool(getattr(cfg, 'ENABLE_LIVE_TRADING', False))
        except Exception:
            enable_live = False

        is_live = (mode == 'LIVE') and enable_live and (not paper_flag)

        # Always record reason
        try:
            self.db.set_setting('kill_switch_reason', str(reason))
        except Exception:
            pass

        if is_live:
            # Mark kill switch + safer mode
            try:
                self.db.set_setting('kill_switch', '1')
            except Exception:
                pass

            try:
                self.db.set_setting('mode', 'TEST')
            except Exception:
                pass

        action_txt = (
            'Action: kill_switch=ON, mode=TEST, closing positions.'
            if is_live else
            'Action: TEST/PAPER mode detected; no forced shutdown (tune max_daily_loss_usd / max_drawdown_pct as needed).'
        )
        msg = (
            "🚨 **RISK STOP** 🚨\n"
            f"Reason: {reason}\n"
            f"{action_txt}"
        )

        try:
            self.db.log(msg, level='ERROR' if is_live else 'WARN')
        except Exception:
            pass

        if details:
            try:
                self.db.log(f"RISK DETAILS: {details}", level='ERROR' if is_live else 'WARN')
            except Exception:
                pass

        # Telegram only for LIVE to avoid spam during backtests/paper tests
        if is_live:
            self._send_telegram(msg)

        # Close positions (best effort) only in LIVE mode
        if is_live:
            self._enqueue_close_all(reason)
            self._attempt_close_positions_direct()

        print(msg)

    # ----------------------------
    # Equity snapshot logging (DB compatibility)
    # ----------------------------
    def _log_equity_snapshot(self, equity: float, unrealized: float) -> None:
        # Prefer new API
        if hasattr(self.db, "add_equity_snapshot"):
            try:
                self.db.add_equity_snapshot(total_balance=equity, unrealized_pnl=unrealized, source="risk_monitor")
                return
            except Exception:
                pass

        # Fallback: insert into equity_history(timestamp,total_balance,unrealized_pnl)
        try:
            ts = _utc_now().isoformat().replace("+00:00", "Z")
            cur = getattr(self.db, "cursor", None)
            conn = getattr(self.db, "conn", None)
            lock = getattr(self.db, "lock", None)

            if not (cur and conn):
                return

            # Some DBs might have extra columns; insert the basic ones.
            def _do():
                cur.execute(
                    "INSERT INTO equity_history (timestamp, total_balance, unrealized_pnl) VALUES (?, ?, ?)",
                    (ts, float(equity), float(unrealized)),
                )
                conn.commit()

            if lock:
                with lock:
                    _do()
            else:
                _do()
        except Exception:
            pass

    # ----------------------------
    # Main loop
    # ----------------------------
    def run(self) -> None:
        if not self.client:
            print("⚠️ Risk Supervisor: No Binance client, exiting.")
            try:
                self.db.set_setting("risk_supervisor_ok", "0", bump_version=False, audit=False)
                self.db.set_setting("risk_supervisor_last_error", "NO_BINANCE_CLIENT", bump_version=False, audit=False)
            except Exception:
                pass
            return

        while True:
            self._handle_day_rollover_if_needed()

            # Doctor heartbeat
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                self.db.set_setting("risk_supervisor_heartbeat", now_iso, bump_version=False, audit=False)
                self.db.set_setting("risk_supervisor_ok", "1", bump_version=False, audit=False)
            except Exception:
                pass

            try:
                acct = self._futures_account()
                if not acct:
                    time.sleep(self.interval_sec)
                    continue

                wallet = _to_float(acct.get("totalWalletBalance"), 0.0)
                unrealized = _to_float(acct.get("totalUnrealizedProfit"), 0.0)
                equity = _to_float(acct.get("totalMarginBalance"), 0.0)
                # Doctor: store latest balances
                try:
                    self.db.set_setting("risk_supervisor_wallet_usd", str(wallet), bump_version=False, audit=False)
                    self.db.set_setting("risk_supervisor_equity_usd", str(equity), bump_version=False, audit=False)
                    self.db.set_setting("risk_supervisor_last_error", "", bump_version=False, audit=False)
                except Exception:
                    pass


                # Risk checks only if we have a valid day-start balance
                if self.start_balance > 0 and not self.shutdown_triggered:
                    daily_delta = wallet - self.start_balance
                    if daily_delta < -abs(self.max_daily_loss_usd):
                        self.emergency_shutdown(
                            f"Daily Loss Hit (-${abs(daily_delta):.2f})",
                            details={"start_balance": self.start_balance, "wallet": wallet, "equity": equity},
                        )

                    dd = 1.0 - (equity / self.start_balance) if self.start_balance else 0.0
                    if dd > abs(self.max_drawdown_pct):
                        self.emergency_shutdown(
                            f"Max Equity Drawdown Hit ({dd*100:.2f}%)",
                            details={"start_balance": self.start_balance, "wallet": wallet, "equity": equity},
                        )

                # Log for chart / dashboard
                if getattr(cfg, "SNAPSHOT_ENABLED", True):
                    self._log_equity_snapshot(equity=equity, unrealized=unrealized)

                # Optional pruning (lightweight: once per hour)
                if hasattr(self.db, "prune_equity_history"):
                    now = _utc_now()
                    if now.minute == 0 and now.second < self.interval_sec:
                        try:
                            self.db.prune_equity_history(retention_days=getattr(cfg, "SNAPSHOT_RETENTION_DAYS", 30))
                        except Exception:
                            pass

                time.sleep(self.interval_sec)

            except Exception as e:
                try:
                    self.db.set_setting("risk_supervisor_last_error", str(e), bump_version=False, audit=False)
                    self.db.set_setting("risk_supervisor_ok", "0", bump_version=False, audit=False)
                except Exception:
                    pass
                time.sleep(self.interval_sec)


if __name__ == "__main__":
    RiskSupervisor().run()
