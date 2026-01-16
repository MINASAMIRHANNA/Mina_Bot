"""risk_manager.py

Lightweight execution-time risk gate.

This module is intentionally conservative and dependency-light.
It is designed to be a *final* check right before placing an order.

Key rules (compatible with project vision):
- Always respect kill-switch (DB setting: kill_switch=1/true).
- Enforce max open trades and simple correlation limits.
- Enforce per-symbol cooldown to reduce churn.
- Enforce notional caps and sanity checks.

Notes:
- Reads most knobs from DB settings so the Dashboard can tune without code changes.
- Uses UTC for all internal timestamps.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _to_int(v: Any, default: int) -> int:
    try:
        if v is None or str(v).strip() == "":
            return default
        return int(float(str(v).strip()))
    except Exception:
        return default


def _to_float(v: Any, default: float) -> float:
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(str(v).strip())
    except Exception:
        return default


@dataclass
class AllowTradeResult:
    allowed: bool
    reason: str = ""


class RiskManager:
    """Final execution-time risk gate.

    The project historically called:
        allow_trade(symbol, signal, qty, price) -> bool

    We keep that API for backward compatibility, and also expose
    allow_trade_with_reason for better diagnostics.
    """

    def __init__(self, db, cfg: Any = None):
        self.db = db
        self.cfg = cfg  # optional config singleton (config.cfg)

    # ======================================================
    # MAIN GATE (backward compatible)
    # ======================================================
    def allow_trade(self, symbol: str, signal: str, qty: float, price: float) -> bool:
        res = self.allow_trade_with_reason(symbol, signal, qty, price)
        return res.allowed

    def allow_trade_with_reason(self, symbol: str, signal: str, qty: float, price: float) -> AllowTradeResult:
        symbol = str(symbol or "").upper().strip()
        sig = str(signal or "").upper().strip()

        # Basic sanity
        if not symbol:
            return self._deny("missing_symbol")
        if qty is None or price is None:
            return self._deny("missing_qty_or_price")
        if _to_float(qty, 0.0) <= 0 or _to_float(price, 0.0) <= 0:
            return self._deny("invalid_qty_or_price")

        # Kill switch first
        if not self._check_kill_switch():
            return self._deny("kill_switch_active")

        # Daily/behavioral risk guards
        if not self._check_daily_loss():
            return self._deny("daily_loss_limit")
        if not self._check_consecutive_losses():
            return self._deny("consecutive_losses")

        # Mode guard (optional): if dashboard forces TEST, block live execution.
        if not self._check_mode_allows_execution():
            return self._deny("mode_blocks_execution")

        # Caps & gates
        if not self._check_max_open_trades():
            return self._deny("max_open_trades_reached")

        if not self._check_symbol_cooldown(symbol):
            return self._deny("symbol_cooldown")

        if not self._check_notional_cap(qty, price):
            return self._deny("trade_notional_cap")

        if not self._check_correlation(sig, symbol):
            return self._deny("correlation_limit")

        return AllowTradeResult(True, "ok")

    # ======================================================
    # Public helpers (safe to call from other modules)
    # ======================================================
    def calc_qty_from_risk(
        self,
        balance_usd: float,
        entry_price: float,
        stop_loss_price: float,
        risk_pct: Optional[float] = None,
        max_notional_usd: Optional[float] = None,
    ) -> float:
        """Position sizing helper.

        qty = (balance * risk_pct) / abs(entry - stop_loss)

        - balance_usd: account equity
        - risk_pct: percent of balance to risk per trade (e.g., 0.01 = 1%)
        - max_notional_usd: optional cap on position notional (qty*price)
        """
        bal = _to_float(balance_usd, 0.0)
        entry = _to_float(entry_price, 0.0)
        sl = _to_float(stop_loss_price, 0.0)
        if bal <= 0 or entry <= 0 or sl <= 0:
            return 0.0

        if risk_pct is None:
            risk_pct = _to_float(self._s("risk_per_trade_pct"), 0.01)

        risk_pct = max(0.0, min(0.10, _to_float(risk_pct, 0.01)))  # clamp to 0..10%
        risk_usd = bal * risk_pct
        per_unit_loss = abs(entry - sl)
        if per_unit_loss <= 0:
            return 0.0
        qty = risk_usd / per_unit_loss

        # Optional notional cap
        if max_notional_usd is None:
            max_notional_usd = _to_float(self._s("max_trade_notional_usd"), 0.0)
        cap = _to_float(max_notional_usd, 0.0)
        if cap > 0:
            qty = min(qty, cap / entry)

        return max(0.0, float(qty))

    def clamp_leverage(self, lev: Any, default: int = 1) -> int:
        max_lev = 20
        if self.cfg is not None and hasattr(self.cfg, "MAX_LEVERAGE"):
            max_lev = _to_int(getattr(self.cfg, "MAX_LEVERAGE"), max_lev)
        lev_i = _to_int(lev, default)
        return max(1, min(max_lev, lev_i))

    # ======================================================
    # Internal gates
    # ======================================================
    def _s(self, key: str) -> Any:
        """Read a setting from DB (string) with safe fallback."""
        try:
            if hasattr(self.db, "get_setting"):
                return self.db.get_setting(key)
        except Exception:
            pass
        return None

    def _log(self, msg: str, level: str = "RISK") -> None:
        try:
            if hasattr(self.db, "log"):
                self.db.log(msg, level)
        except Exception:
            pass

    def _deny(self, reason: str) -> AllowTradeResult:
        self._log(f"⛔ Trade blocked: {reason}", "RISK")
        return AllowTradeResult(False, reason)

    def _check_kill_switch(self) -> bool:
        ks = self._s("kill_switch")
        if _to_bool(ks, False):
            self._log("⛔ Kill switch active (kill_switch=1).", "RISK")
            return False
        return True

    def _check_mode_allows_execution(self) -> bool:
        """Optional guard: if mode is forced to TEST, block live execution."""
        mode = self._s("mode")
        if mode is None:
            return True
        m = str(mode).strip().upper()
        # In this project, mode may be LIVE/TEST/PAPER etc.
        if m in {"TEST"}:
            # Risk manager cannot know whether this call is for paper or live execution,
            # so we only block if a setting explicitly requests it.
            block = _to_bool(self._s("block_execution_when_test"), False)
            if block:
                self._log("⛔ Execution blocked because mode=TEST and block_execution_when_test=1.", "RISK")
                return False
        return True

    def _get_open_trades(self) -> List[Dict[str, Any]]:
        """Fetch open trades from DB using any available method."""
        for meth in ("get_open_trades", "get_all_active_trades"):
            if hasattr(self.db, meth):
                try:
                    return list(getattr(self.db, meth)() or [])
                except Exception:
                    continue
        # Fallback: nothing
        return []

    def _check_max_open_trades(self) -> bool:
        open_trades = self._get_open_trades()
        # Setting override; else cfg; else default 10
        max_open = _to_int(self._s("max_open_trades"), None)  # may be None
        if max_open is None:
            if self.cfg is not None and hasattr(self.cfg, "MAX_CONCURRENT_TRADES"):
                max_open = _to_int(getattr(self.cfg, "MAX_CONCURRENT_TRADES"), 10)
            else:
                max_open = 10

        if len(open_trades) >= max_open:
            self._log(f"⛔ Max open trades reached: {len(open_trades)}/{max_open}", "RISK")
            return False
        return True


        def _check_daily_loss(self) -> bool:
            """Stop trading if realized PnL for today breaches max_daily_loss."""
            max_loss = _to_float(self._s("max_daily_loss"), 0.0)
            if max_loss <= 0:
                return True
            # Prefer DB helper if present
            try:
                day = _utc_now().strftime("%Y-%m-%d")
                if hasattr(self.db, "get_daily_pnl"):
                    pnl = _to_float(self.db.get_daily_pnl(day), 0.0)
                else:
                    pnl = 0.0
            except Exception:
                pnl = 0.0

            if pnl <= -abs(max_loss):
                self._log(f"⛔ Daily loss limit hit: pnl={pnl:.2f} <= -{abs(max_loss):.2f}")
                return False
            return True

        def _check_consecutive_losses(self) -> bool:
            """Stop trading if last N closed trades are losses."""
            max_losses = _to_int(self._s("max_consecutive_losses"), 0)
            if max_losses <= 0:
                return True
            try:
                losses = []
                if hasattr(self.db, "get_recent_losses"):
                    losses = list(self.db.get_recent_losses(limit=max_losses) or [])
                # if DB doesn't implement, fail open
            except Exception:
                losses = []
            if len(losses) >= max_losses:
                self._log(f"⛔ Consecutive losses ({max_losses}) — trading paused")
                return False
            return True
    def _check_symbol_cooldown(self, symbol: str) -> bool:
        """Block repeated entries for same symbol within a cooldown window."""
        mins = _to_int(self._s("cooldown_minutes"), 3)
        if mins <= 0:
            return True
        cooldown = timedelta(minutes=mins)

        # Prefer explicit trade_state storage if available
        try:
            if hasattr(self.db, "get_trade_state") and hasattr(self.db, "set_trade_state"):
                key = f"last_entry_{symbol}"
                last = self.db.get_trade_state(key)
                if last:
                    try:
                        last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
                        if _utc_now() - last_dt < cooldown:
                            self._log(f"⛔ Cooldown active for {symbol} ({mins}m)", "RISK")
                            return False
                    except Exception:
                        pass
                # Update timestamp now (best effort). If later blocked by other checks, that's ok.
                self.db.set_trade_state(key, _utc_now().isoformat().replace("+00:00", "Z"))
        except Exception:
            # If trade_state not available, do a weaker check based on open trades only.
            pass

        return True

    def _check_notional_cap(self, qty: float, price: float) -> bool:
        cap = _to_float(self._s("max_trade_notional_usd"), 0.0)
        if cap <= 0:
            return True
        notional = _to_float(qty, 0.0) * _to_float(price, 0.0)
        if notional > cap:
            self._log(f"⛔ Notional cap exceeded: {notional:.2f} > {cap:.2f}", "RISK")
            return False
        return True

    def _check_correlation(self, signal: str, symbol: str) -> bool:
        """Very simple correlation guard: limit number of open trades in same direction."""
        open_trades = self._get_open_trades()
        direction = "LONG" if "LONG" in signal else "SHORT" if "SHORT" in signal else ""
        if not direction:
            return True

        same_dir = []
        for t in open_trades:
            try:
                if str(t.get("symbol", "")).upper() == symbol:
                    continue
                tsig = str(t.get("signal", "") or "").upper()
                if direction in tsig:
                    same_dir.append(t)
            except Exception:
                continue

        max_same_dir = _to_int(self._s("max_same_direction"), 2)
        if max_same_dir > 0 and len(same_dir) >= max_same_dir:
            self._log(f"⛔ Correlation risk: {len(same_dir)} trades already in {direction} (limit {max_same_dir})", "RISK")
            return False

        return True
