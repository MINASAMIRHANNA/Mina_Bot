import time
import json
from datetime import datetime, timezone

from binance.client import Client
from config import cfg
from database import DatabaseManager
from trading_executor import ensure_stop_loss, ensure_hard_tp, ensure_trailing_stop


import os

class _NoPingClient(Client):
    """Disable spot ping() during init to avoid Spot-Testnet 502 killing futures modules."""
    def ping(self):
        try:
            return {}
        except Exception:
            return {}

def _futures_rest_base(use_testnet: bool) -> str:
    env = os.getenv("BINANCE_FUTURES_REST_BASE", "").strip()
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



class ExecutionMonitor:
    """
    Monitors real execution state from Binance Futures and syncs it with DB.

    Features:
    - Periodically checks active trades against Binance position info.
    - When a position is closed, closes the trade in DB and clears trade_state.
    - Periodic equity snapshots (UTC) for Snapshot/Audit features.
      Uses DatabaseManager.add_equity_snapshot() when available; falls back to
      direct insert into equity_history for older DB versions.

    Notes:
    - This monitor is intended for Futures. Spot tracking can be added separately.
    """

    CHECK_INTERVAL = 5  # seconds

    def __init__(self):
        self.db = DatabaseManager()

        api_key = getattr(cfg, "BINANCE_API_KEY", None) or ""
        api_secret = getattr(cfg, "BINANCE_API_SECRET", None) or ""
        self.client = _NoPingClient(api_key, api_secret, testnet=False)
        _configure_futures_endpoints(self.client, bool(getattr(cfg, "USE_TESTNET", True)))

        self.running = True

        # Restart request tracking (from dashboard)
        self._last_restart_req = None

        # Positions cache (for Project Doctor)
        self._positions_cache = {}  # symbol -> snapshot dict
        # Orphan position reconciliation / safe close confirmation
        self._missing_pos_count = {}  # symbol -> consecutive cycles with no position (avoid false closes)
        try:
            self.confirm_position_closed_cycles = int(float(self.db.get_setting("confirm_position_closed_cycles") or 3))
            if self.confirm_position_closed_cycles < 2:
                self.confirm_position_closed_cycles = 2
        except Exception:
            self.confirm_position_closed_cycles = 3

        try:
            self.sync_orphan_positions = str(self.db.get_setting("sync_orphan_positions") or "TRUE").strip().upper() in ("1","TRUE","YES","ON","Y")
        except Exception:
            self.sync_orphan_positions = True

        try:
            self.orphan_sync_every_sec = int(float(self.db.get_setting("orphan_sync_every_sec") or 15))
            if self.orphan_sync_every_sec < 10:
                self.orphan_sync_every_sec = 10
        except Exception:
            self.orphan_sync_every_sec = 15
        self._last_orphan_sync_ts = 0.0


        # REST API metrics (rate limits / latency)
        self._api_metrics = {
            "updated_utc": None,
            "used_weight_1m": None,
            "last": {},
            "errors_total": 0,
            "rate_limit_hits": 0,
        }

        # Protection audit (orphan orders / missing protection)
        try:
            self.protection_audit_enabled = str(self.db.get_setting("protection_audit_enabled") or "TRUE").strip().upper() in ("1","TRUE","YES","ON","Y")
        except Exception:
            self.protection_audit_enabled = True
        try:
            self.protection_audit_every_sec = int(float(self.db.get_setting("protection_audit_every_sec") or 30))
            if self.protection_audit_every_sec < 5:
                self.protection_audit_every_sec = 5
        except Exception:
            self.protection_audit_every_sec = 30
        self._last_protection_audit_ts = 0.0


        # Snapshot controls (safe defaults if config values are missing)
        self.snapshot_enabled = bool(getattr(cfg, "SNAPSHOT_ENABLED", True))
        self.snapshot_every_sec = int(getattr(cfg, "EQUITY_SNAPSHOT_EVERY_SEC", 60))
        if self.snapshot_every_sec < 5:
            self.snapshot_every_sec = 5  # avoid excessive DB writes
        self.snapshot_retention_days = int(getattr(cfg, "SNAPSHOT_RETENTION_DAYS", 30))

        # Protection-order reconciliation (dashboard settings)
        # When enabled, we periodically ensure STOP_MARKET (and optional TP) orders exist on exchange
        # so positions stay protected even after restarts.
        try:
            self.ensure_protection_orders = str(self.db.get_setting("ensure_protection_orders") or "1").strip().upper() in ("1","TRUE","YES","ON","Y")
        except Exception:
            self.ensure_protection_orders = True
        try:
            self.ensure_protection_every_sec = int(float(self.db.get_setting("ensure_protection_every_sec") or 60))
            if self.ensure_protection_every_sec < 10:
                self.ensure_protection_every_sec = 10
        except Exception:
            self.ensure_protection_every_sec = 60
        self._last_ensure_ts_by_symbol = {}
        self._last_partial_remain_by_symbol = {}

        self._last_snapshot_ts = 0.0
        self._last_prune_ts = 0.0
        self._prune_every_sec = 6 * 3600  # safety net (daily_routine also prunes)

    def _reload_runtime_settings(self):
        """Reload frequently changed flags from DB (dashboard-controlled)."""
        try:
            self.ensure_protection_orders = str(self.db.get_setting("ensure_protection_orders") or "TRUE").strip().upper() in ("1","TRUE","YES","ON","Y")
        except Exception:
            pass
        try:
            v = int(float(self.db.get_setting("ensure_protection_every_sec") or self.ensure_protection_every_sec))
            if v < 10:
                v = 10
            self.ensure_protection_every_sec = v
        except Exception:
            pass
        try:
            self.protection_audit_enabled = str(self.db.get_setting("protection_audit_enabled") or "TRUE").strip().upper() in ("1","TRUE","YES","ON","Y")
        except Exception:
            pass
        try:
            v = int(float(self.db.get_setting("protection_audit_every_sec") or self.protection_audit_every_sec))
            if v < 5:
                v = 5
            self.protection_audit_every_sec = v
        except Exception:
            pass

    def stop(self):
        self.running = False

    # ======================================================
    # MAIN LOOP
    # ======================================================
    def run(self):
        print("🧭 Execution Monitor ONLINE")
        while self.running:
            try:
                # heartbeat for dashboard/health checks (do not bump config version)
                try:
                    self.db.set_setting(
                        "execution_monitor_heartbeat",
                        datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        source="execution_monitor",
                        bump_version=False,
                        audit=False,
                    )
                except Exception:
                    pass

                # reload dashboard-controlled flags
                self._reload_runtime_settings()

                # Restart request (from dashboard). Works best when monitor runs under a loop/supervisor.
                try:
                    req = str(self.db.get_setting("restart_monitor_req") or "").strip()
                    if req and req != str(self._last_restart_req or ""):
                        self._last_restart_req = req
                        try:
                            self.db.set_setting(
                                "restart_monitor_ack",
                                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                                source="execution_monitor",
                                bump_version=False,
                                audit=False,
                            )
                        except Exception:
                            pass
                        print("[EXEC_MON] 🔁 Restart requested from Dashboard. Exiting now...")
                        os._exit(3)
                except Exception:
                    pass


                ts = datetime.now().strftime("%H:%M:%S")
                print(f"🔍 [{ts}] Starting Execution Check...")
                try:
                    mode_now_dbg = str(self.db.get_setting("mode") or "TEST").upper()
                except Exception:
                    mode_now_dbg = "TEST"
                print(f"🧭 [MONITOR] Mode={mode_now_dbg}")
                active_trades = self.db.get_all_active_trades() if hasattr(self.db, "get_all_active_trades") else []

                # If DB shows no OPEN trades but exchange still has open positions, reconcile them.
                if getattr(self, "sync_orphan_positions", True) and len(active_trades) == 0:
                    try:
                        synced = self._maybe_sync_orphan_positions(verbose=True)
                        if synced:
                            active_trades = self.db.get_all_active_trades() if hasattr(self.db, "get_all_active_trades") else []
                            print(f"🧩 [MONITOR] Synced/Reopened from exchange: {synced}")
                    except Exception as e:
                        print(f"[EXEC_MON] Orphan sync error: {e}")
                print(f"📊 [MONITOR] Active Trades in DB: {len(active_trades)}")

                for trade in active_trades:
                    self._check_trade(trade, verbose=True)

                # Write positions snapshot for Project Doctor (best-effort)
                try:
                    ps = {
                        "updated_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        "count": len(self._positions_cache),
                        "positions": list(self._positions_cache.values()),
                    }
                    self.db.set_setting("positions_status", json.dumps(ps), source="execution_monitor", bump_version=False, audit=False)
                except Exception:
                    pass

                # Also persist API metrics for Doctor (rate limits/latency)
                try:
                    self.db.set_setting("api_metrics_execmon", json.dumps(self._api_metrics), source="execution_monitor", bump_version=False, audit=False)
                except Exception:
                    pass

                # Protection audit (orphan/missing protection)
                self._maybe_protection_audit(active_trades)

                # Snapshot + housekeeping
                self._maybe_take_snapshot(active_trades_count=len(active_trades))
                self._maybe_prune_snapshots()

                # Equity (throttled) for live visibility
                try:
                    now_ts = time.time()
                    if (now_ts - float(getattr(self, '_last_equity_log_ts', 0.0) or 0.0)) >= 10:
                        wallet, unreal, equity = self._get_futures_equity()
                        self._last_equity_log_ts = now_ts
                        self._last_equity_vals = (wallet, unreal, equity)
                    else:
                        wallet, unreal, equity = getattr(self, '_last_equity_vals', (0.0, 0.0, 0.0))
                    if equity and float(equity) != 0.0:
                        print(f"💰 [EQUITY] Wallet: {float(wallet):.2f} USDT | Unrealized PnL: {float(unreal):+.2f} USDT | Total: {float(equity):.2f} USDT")
                except Exception:
                    pass

                print(f"😴 [{ts}] Cycle Complete. Sleeping...")
                time.sleep(self.CHECK_INTERVAL)
            except KeyboardInterrupt:
                print("[EXEC_MON] Stopped by user")
                break
            except Exception as e:
                print(f"[EXEC_MON] Error: {e}")
                time.sleep(self.CHECK_INTERVAL)

    # ======================================================
    # CHECK SINGLE TRADE
    # ======================================================
    def _check_trade(self, trade, verbose: bool = False):
        symbol = trade.get("symbol")
        trade_id = trade.get("id")
        if not symbol or trade_id is None:
            return

        try:
            positions = self._api_call("position_info", self.client.futures_position_information, symbol=symbol)

            pos_amt = 0.0
            pos_hit = None
            for p in positions:
                amt = float(p.get("positionAmt", 0) or 0)
                if abs(amt) > 0:
                    pos_amt = amt
                    pos_hit = p
                    break

            # cache positions snapshot for doctor page
            try:
                snap = None
                for p in positions or []:
                    amt = float(p.get("positionAmt", 0) or 0)
                    if abs(amt) > 0:
                        snap = {
                            "symbol": symbol,
                            "positionAmt": amt,
                            "entryPrice": float(p.get("entryPrice", 0) or 0),
                            "markPrice": float(p.get("markPrice", 0) or 0),
                            "unrealizedProfit": float(p.get("unRealizedProfit", 0) or p.get("unrealizedProfit", 0) or 0),
                            "leverage": p.get("leverage"),
                            "updated_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        }
                        break
                if snap:
                    self._positions_cache[symbol] = snap
                else:
                    # no open position => remove cache if present
                    if symbol in self._positions_cache:
                        self._positions_cache.pop(symbol, None)
            except Exception:
                pass

            qty = float(trade.get("quantity", 0) or 0)
            # Reset false-close counter when position is present
            if abs(pos_amt) >= 1e-12:
                self._missing_pos_count.pop(symbol, None)

            if verbose:
                try:
                    side_txt = "LONG" if pos_amt > 0 else "SHORT"
                    size = abs(float(pos_amt))
                    entry = float((pos_hit or {}).get("entryPrice", 0) or 0)
                    mark = float((pos_hit or {}).get("markPrice", 0) or 0)
                    pnl = float((pos_hit or {}).get("unRealizedProfit", 0) or (pos_hit or {}).get("unrealizedProfit", 0) or 0)
                    if mark <= 0:
                        try:
                            mark = float(self._get_last_price(symbol) or 0)
                        except Exception:
                            pass
                    print(f"  -- Analyzing {symbol} (ID: {trade_id})")
                    if entry > 0:
                        print(f"     📍 Position: {side_txt} | Size: {size:g} | Entry: {entry:g} | Mark: {mark:g} | PnL: {pnl:+.2f}")
                    else:
                        print(f"     📍 Position: {side_txt} | Size: {size:g} | Mark: {mark:g} | PnL: {pnl:+.2f}")
                except Exception:
                    print(f"  -- Analyzing {symbol} (ID: {trade_id})")

            # ---------------- POSITION CLOSED ----------------
            if abs(pos_amt) < 1e-12:
                # Note: Binance may return [] for futures_position_information(symbol=...) when the position is flat.
                # We should still be able to reconcile DB, but with extra confirmation cycles to avoid API glitches.
                pos_info_missing = False
                try:
                    pos_list = positions if isinstance(positions, list) else []
                except Exception:
                    pos_list = []

                # Fallback: fetch all positions and filter by symbol (helps on some testnet quirks)
                if not pos_list:
                    try:
                        all_pos = self._api_call(lambda: self.client.futures_position_information(), "futures_position_information(all)")
                        pos_list = [p for p in (all_pos or []) if str(p.get("symbol", "")) == symbol]
                    except Exception:
                        pos_list = []

                if not pos_list:
                    pos_info_missing = True
                    if verbose:
                        print(f"     ⚠️ position_info empty for {symbol} — treating as flat (will confirm).")

                cnt = int(self._missing_pos_count.get(symbol, 0) or 0) + 1
                self._missing_pos_count[symbol] = cnt
                base_need = int(getattr(self, "confirm_position_closed_cycles", 3) or 3)
                need = base_need + (2 if pos_info_missing else 0)
                if verbose:
                    print(f"     ⚠️ No position detected for {symbol} (confirm {cnt}/{need})")
                if cnt < need:
                    return

                # confirmed
                self._missing_pos_count.pop(symbol, None)

                price = self._get_last_price(symbol)
                try:
                    self.db.close_trade(
                        trade_id,
                        reason="POSITION_CLOSED",
                        close_price=price,
                        explain={"source": "execution_monitor"},
                    )
                except Exception:
                    # Older DB versions may not support explain
                    self.db.close_trade(trade_id, reason="POSITION_CLOSED", close_price=price)



                # cleanup protective reduceOnly / closePosition orders to avoid affecting future trades
                try:
                    orders = self.client.futures_get_open_orders(symbol=symbol) or []
                    for o in orders:
                        try:
                            otype = str(o.get("type", "")).upper()
                            if otype not in ("STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"):
                                continue
                            cp = o.get("closePosition")
                            ro = o.get("reduceOnly")
                            if (cp is not None and str(cp).lower() in ("true","1")) or (ro is not None and str(ro).lower() in ("true","1")):
                                oid = o.get("orderId")
                                if oid is not None:
                                    self.client.futures_cancel_order(symbol=symbol, orderId=oid)
                        except Exception:
                            continue
                except Exception:
                    pass

                # If the exchange is already flat, any remaining open orders for this symbol are orphans.
                # Cancel all to prevent a stale SL/TP from affecting the next trade.
                try:
                    self.client.futures_cancel_all_open_orders(symbol=symbol)
                except Exception:
                    pass

                # clear state by symbol (NOT trade_id)
                try:
                    self.db.clear_trade_state(symbol)
                except Exception:
                    pass

                print(f"✅ Trade closed: {symbol}")
                return


            # ---------------- ENSURE PROTECTION ORDERS ----------------
            try:
                mode_now = str(self.db.get_setting("mode") or "TEST").upper()
                if mode_now == "LIVE":
                    ensure_enabled = self.ensure_protection_orders
                    try:
                        ensure_enabled = str(self.db.get_setting("ensure_protection_orders") or ("1" if self.ensure_protection_orders else "0")).strip().upper() in ("1","TRUE","YES","ON","Y")
                    except Exception:
                        pass
                    if ensure_enabled:
                        now = time.time()
                        last = float(self._last_ensure_ts_by_symbol.get(symbol, 0.0) or 0.0)
                        every = int(self.ensure_protection_every_sec or 60)
                        try:
                            every = int(float(self.db.get_setting("ensure_protection_every_sec") or every))
                        except Exception:
                            pass
                        if every < 10:
                            every = 10
                        if (now - last) >= every:
                            self._last_ensure_ts_by_symbol[symbol] = now
                            side_txt = "LONG" if pos_amt > 0 else "SHORT"
                            sl = float(trade.get("stop_loss", 0) or 0)
                            if sl > 0:
                                if verbose:
                                    try:
                                        print(f"     🛡️ Syncing SL for {symbol} at target price: {sl:g}")
                                    except Exception:
                                        pass
                                ok_sl = ensure_stop_loss(symbol, side_txt, sl, execution_allowed=True)
                                if verbose:
                                    try:
                                        if ok_sl:
                                            print(f"     [EXEC] ✅ SL ensured for {symbol} at {sl:g}")
                                        else:
                                            print(f"     [EXEC] ⚠️ SL not set/updated for {symbol} (check logs)")
                                    except Exception:
                                        pass
                            try:
                                hard_tp = str(self.db.get_setting("hard_tp_enabled") or "0").strip().upper() in ("1","TRUE","YES","ON","Y")
                            except Exception:
                                hard_tp = False
                            if hard_tp:
                                tps = trade.get("take_profits") or []
                                if isinstance(tps, (list, tuple)) and len(tps) > 0:
                                    try:
                                        tp = float(tps[-1])
                                        if verbose:
                                            try:
                                                print(f"     🎯 Syncing TP for {symbol} at target price: {tp:g}")
                                            except Exception:
                                                pass
                                        ok_tp = ensure_hard_tp(symbol, side_txt, tp, execution_allowed=True)
                                        if verbose:
                                            try:
                                                if ok_tp:
                                                    print(f"     [EXEC] ✅ TP ensured for {symbol} at {tp:g}")
                                                else:
                                                    print(f"     [EXEC] ⚠️ TP not set/updated for {symbol} (check logs)")
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass

                            # -------- Exchange Trailing Stop (native) --------
                            try:
                                ex_trail_enabled = str(self.db.get_setting("exchange_trailing_enabled") or "0").strip().upper() in ("1","TRUE","YES","ON","Y")
                            except Exception:
                                ex_trail_enabled = False

                            if ex_trail_enabled:
                                try:
                                    act_roi = float(self.db.get_setting("exchange_trailing_activation_roi") or 0.01)
                                except Exception:
                                    act_roi = 0.01
                                if act_roi < 0:
                                    act_roi = 0.0
                                if act_roi > 1:
                                    act_roi = 1.0

                                try:
                                    cb = float(self.db.get_setting("exchange_trailing_callback_rate") or 0.3)
                                except Exception:
                                    cb = 0.3

                                wt = str(self.db.get_setting("exchange_trailing_working_type") or "MARK_PRICE").upper().strip()
                                if wt not in ("MARK_PRICE", "CONTRACT_PRICE"):
                                    wt = "MARK_PRICE"

                                entry = float(trade.get("entry_price", 0) or 0)
                                if entry > 0:
                                    # pick working price to align with workingType
                                    px = 0.0
                                    try:
                                        if wt == "MARK_PRICE":
                                            mp = self.client.futures_mark_price(symbol=symbol)
                                            px = float((mp or {}).get("markPrice") or 0.0)
                                        else:
                                            px = float(self._get_last_price(symbol) or 0.0)
                                    except Exception:
                                        px = float(self._get_last_price(symbol) or 0.0)

                                    if px > 0:
                                        if side_txt == "LONG":
                                            roi = (px / entry) - 1.0
                                        else:
                                            roi = (entry / px) - 1.0

                                        if roi >= act_roi:
                                            ensure_trailing_stop(
                                                symbol,
                                                side_txt,
                                                abs(pos_amt),
                                                activation_price=px,
                                                callback_rate=cb,
                                                working_type=wt,
                                                execution_allowed=True,
                                            )
            except Exception:
                pass
            # ---------------- PARTIAL CLOSE DETECT ----------------
            if qty > 0:
                remain = abs(pos_amt)
                delta = qty - remain
                tol = max(1e-8, qty * 0.002)  # ignore tiny diffs (rounding/stepSize)
                if delta > tol:
                    last = self._last_partial_remain_by_symbol.get(symbol)
                    if last is None or abs(last - remain) > tol:
                        self._last_partial_remain_by_symbol[symbol] = remain
                        try:
                            self.db.log(
                                f"Partial close detected {symbol}: remaining {remain}",
                                level="INFO",
                            )
                        except Exception:
                            pass

        except Exception as e:
            print(f"[EXEC_MON] Check failed {symbol}: {e}")

    # ======================================================
    # PRICE FETCH
    # ======================================================
    def _get_last_price(self, symbol):
        try:
            mark = self.client.futures_mark_price(symbol=symbol)
            return float(mark.get("markPrice", 0) or 0)
        except Exception:
            return 0.0

    @staticmethod
    def _utc_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # ======================================================
    # SNAPSHOTS
    # ======================================================
    # ======================================================
    # API METRICS (Rate limits / latency)
    # ======================================================
    def _api_call(self, name: str, fn, *args, **kwargs):
        t0 = time.time()
        try:
            res = fn(*args, **kwargs)
            ok = True
            err = ""
        except Exception as e:
            res = None
            ok = False
            err = str(e)

        dt_ms = int((time.time() - t0) * 1000)
        weight = None
        try:
            hdrs = getattr(getattr(self.client, "response", None), "headers", {}) or {}
            w = hdrs.get("x-mbx-used-weight-1m") or hdrs.get("X-MBX-USED-WEIGHT-1M")
            if w is not None:
                weight = int(w)
        except Exception:
            weight = None

        # classify rate-limit style errors
        if not ok:
            self._api_metrics["errors_total"] = int(self._api_metrics.get("errors_total") or 0) + 1
            if "429" in err or "418" in err or "-1003" in err or "too many requests" in err.lower():
                self._api_metrics["rate_limit_hits"] = int(self._api_metrics.get("rate_limit_hits") or 0) + 1

        self._api_metrics["updated_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        if weight is not None:
            self._api_metrics["used_weight_1m"] = weight
        self._api_metrics.setdefault("last", {})[name] = {
            "ok": bool(ok),
            "latency_ms": dt_ms,
            "used_weight_1m": weight,
            "err": err[:220] if err else "",
            "ts_utc": self._api_metrics["updated_utc"],
        }

        return res


    def _maybe_sync_orphan_positions(self, verbose: bool = False) -> int:
        """Best-effort reconciliation:
        - If exchange has open futures positions but DB has 0 OPEN trades, reopen/sync so dashboard + monitor match reality.
        - Also avoids poisoning learning by incorrectly closing trades on transient API glitches.
        """
        try:
            if not getattr(self, "sync_orphan_positions", True):
                return 0
            now_ts = time.time()
            if (now_ts - float(getattr(self, "_last_orphan_sync_ts", 0.0) or 0.0)) < float(getattr(self, "orphan_sync_every_sec", 15) or 15):
                return 0
            self._last_orphan_sync_ts = now_ts

            positions = self._api_call("positions_all", self.client.futures_position_information)
            if not isinstance(positions, list) or not positions:
                return 0

            # Collect open positions (net view). In hedge mode there can be multiple rows per symbol.
            by_symbol = {}
            for p in positions:
                try:
                    sym = str(p.get("symbol", "")).upper().strip()
                    if not sym:
                        continue
                    amt = float(p.get("positionAmt", 0) or 0.0)
                except Exception:
                    continue
                if abs(amt) < 1e-12:
                    continue
                prev = by_symbol.get(sym)
                if prev is None:
                    by_symbol[sym] = p
                else:
                    try:
                        prev_amt = float(prev.get("positionAmt", 0) or 0.0)
                    except Exception:
                        prev_amt = 0.0
                    if abs(amt) > abs(prev_amt):
                        by_symbol[sym] = p

            if not by_symbol:
                return 0

            # helper: check if an OPEN trade exists for symbol
            def _get_open_trade_id(sym: str):
                try:
                    with self.db.lock:
                        cur = self.db.conn.cursor()
                        cur.execute("SELECT id FROM trades WHERE symbol=? AND status='OPEN' ORDER BY timestamp_ms DESC LIMIT 1", (sym,))
                        row = cur.fetchone()
                        return int(row[0]) if row else None
                except Exception:
                    return None

            synced = 0
            for sym, p in by_symbol.items():
                open_id = _get_open_trade_id(sym)
                try:
                    amt = float(p.get("positionAmt", 0) or 0.0)
                except Exception:
                    amt = 0.0
                qty = abs(float(amt))
                try:
                    entry = float(p.get("entryPrice", 0) or 0.0)
                except Exception:
                    entry = 0.0
                ps = str(p.get("positionSide", "BOTH") or "BOTH").upper()
                if ps == "SHORT":
                    sig = "SELL"
                elif ps == "LONG":
                    sig = "BUY"
                else:
                    sig = "BUY" if amt > 0 else "SELL"

                if open_id:
                    try:
                        with self.db.lock:
                            self.db.cursor.execute(
                                "UPDATE trades SET quantity=?, entry_price=?, signal=COALESCE(NULLIF(signal,''), ?) WHERE id=?",
                                (float(qty), float(entry), sig, int(open_id))
                            )
                            self.db.conn.commit()
                    except Exception:
                        pass
                    continue

                # Try to reopen last trade for this symbol (if it was falsely closed)
                last = None
                try:
                    if hasattr(self.db, "get_last_trade_for_symbol"):
                        last = self.db.get_last_trade_for_symbol(sym)
                except Exception:
                    last = None

                reopened = False
                if last and str(last.get("status", "")).upper() != "OPEN":
                    try:
                        tid = int(last.get("id"))
                        with self.db.lock:
                            self.db.cursor.execute(
                                "UPDATE trades SET status='OPEN', close_price=NULL, close_reason=NULL, closed_at=NULL, closed_at_ms=NULL, pnl=0 WHERE id=?",
                                (tid,)
                            )
                            self.db.cursor.execute(
                                "UPDATE trades SET quantity=?, entry_price=?, signal=? WHERE id=?",
                                (float(qty), float(entry), sig, tid)
                            )
                            self.db.conn.commit()
                        reopened = True
                        synced += 1
                        if verbose:
                            print(f"🧩 [ORPHAN_SYNC] Reopened {sym} trade_id={tid} ({sig} qty={qty:g})")
                    except Exception:
                        reopened = False

                if reopened:
                    continue

                # Otherwise insert a new OPEN trade
                try:
                    payload = {
                        "symbol": sym,
                        "signal": sig,
                        "entry_price": float(entry),
                        "quantity": float(qty),
                        "stop_loss": 0.0,
                        "take_profits": [],
                        "market_type": "futures",
                        "strategy_tag": "ORPHAN_SYNC",
                        "exit_profile": "ORPHAN",
                        "leverage": None,
                        "confidence": 0.0,
                        "explain": {"source": "orphan_sync", "positionSide": ps},
                        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    }
                    tid = self.db.add_trade(payload) if hasattr(self.db, "add_trade") else None
                    synced += 1
                    if verbose:
                        print(f"🧩 [ORPHAN_SYNC] Inserted {sym} trade_id={tid} ({sig} qty={qty:g})")
                except Exception:
                    continue

            return synced
        except Exception:
            return 0


    # ======================================================
    # PROTECTION AUDIT (Orphan orders / missing SL/TP/Trail)
    # ======================================================
    def _maybe_protection_audit(self, active_trades):
        try:
            if not self.protection_audit_enabled:
                return
            now = time.time()
            if (now - float(self._last_protection_audit_ts or 0.0)) < float(self.protection_audit_every_sec or 30):
                return
            self._last_protection_audit_ts = now

            # pull ALL open orders once (cheaper)
            open_orders = self._api_call("open_orders_all", self.client.futures_get_open_orders) or []
            # pull ALL positions once (to avoid false 'orphan' when DB is stale)
            pos_all = self._api_call("positions_all", self.client.futures_position_information) or []

            pos_by_symbol = {}
            try:
                for p in pos_all:
                    s = str(p.get("symbol") or "").upper()
                    if not s:
                        continue
                    try:
                        amt = float(p.get("positionAmt") or 0.0)
                    except Exception:
                        amt = 0.0
                    if abs(amt) > 0.0:
                        pos_by_symbol[s] = {
                            "positionAmt": amt,
                            "entryPrice": float(p.get("entryPrice") or 0.0) if p.get("entryPrice") is not None else 0.0,
                            "unRealizedProfit": float(p.get("unRealizedProfit") or 0.0) if p.get("unRealizedProfit") is not None else 0.0,
                            "marginType": p.get("marginType"),
                            "leverage": p.get("leverage"),
                        }
            except Exception:
                pos_by_symbol = {}

            orders_by_symbol = {}
            for o in (open_orders or []):
                s = str(o.get("symbol") or "").upper()
                if not s:
                    continue
                orders_by_symbol.setdefault(s, []).append(o)

            trades_by_symbol = {}
            for t in (active_trades or []):
                s = str(t.get("symbol") or "").upper()
                if s:
                    trades_by_symbol[s] = t

            symbols = sorted(set(list(orders_by_symbol.keys()) + list(pos_by_symbol.keys()) + list(trades_by_symbol.keys())))

            audit = {
                "updated_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "symbols_checked": symbols,
                "counts": {
                    "symbols": len(symbols),
                    "positions": len(pos_by_symbol),
                    "open_orders": len(open_orders or []),
                },
                "issues": {
                    "missing_sl": [],
                    "missing_tp": [],
                    "missing_trail": [],
                    "duplicates": [],
                    "orphan_orders": [],
                },
                "per_symbol": {},
            }

            # settings that affect expectations
            hard_tp = str(self.db.get_setting("hard_tp_enabled") or "TRUE").strip().upper() in ("1","TRUE","YES","ON","Y")
            exch_trail = str(self.db.get_setting("exchange_trailing_enabled") or "FALSE").strip().upper() in ("1","TRUE","YES","ON","Y")
            try:
                act_roi = float(self.db.get_setting("exchange_trailing_activation_roi") or 0.01)
            except Exception:
                act_roi = 0.01

            def _is_protective(o):
                typ = str(o.get("type") or "").upper()
                return typ in ("STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET")

            for s in symbols:
                pos = pos_by_symbol.get(s)
                has_pos = pos is not None
                t = trades_by_symbol.get(s)
                ol = orders_by_symbol.get(s, [])
                prot = [o for o in ol if _is_protective(o)]

                # summarize counts
                type_counts = {}
                for o in prot:
                    typ = str(o.get("type") or "").upper()
                    type_counts[typ] = type_counts.get(typ, 0) + 1

                # expectation based on DB trade
                exp_sl = False
                exp_tp = False
                exp_trail = False
                sl_price = None
                if has_pos and self.ensure_protection_orders and t:
                    try:
                        sl = float(t.get("stop_loss") or 0.0)
                    except Exception:
                        sl = 0.0
                    if sl > 0:
                        exp_sl = True
                        sl_price = sl
                    if hard_tp:
                        # any configured TP list => expect at least one TP order
                        tp_list = t.get("take_profits")
                        if isinstance(tp_list, str):
                            try:
                                tp_list = json.loads(tp_list)
                            except Exception:
                                tp_list = []
                        if isinstance(tp_list, list) and len(tp_list) > 0:
                            exp_tp = True

                    # exchange trailing expected only after activation ROI met
                    if exch_trail:
                        try:
                            entry = float(t.get("entry_price") or 0.0)
                        except Exception:
                            entry = 0.0
                        # best effort mark price
                        mark = None
                        try:
                            mp = self._api_call(f"mark_price_{s}", self.client.futures_mark_price, symbol=s) or {}
                            mark = float(mp.get("markPrice") or 0.0)
                        except Exception:
                            mark = None
                        if entry > 0 and mark and mark > 0:
                            roi = (mark - entry) / entry if float(pos.get("positionAmt") or 0.0) > 0 else (entry - mark) / entry
                            if roi >= act_roi:
                                exp_trail = True

                has_sl = any(str(o.get("type") or "").upper() in ("STOP","STOP_MARKET") for o in prot)
                has_tp = any(str(o.get("type") or "").upper() in ("TAKE_PROFIT","TAKE_PROFIT_MARKET") for o in prot)
                has_trail = any(str(o.get("type") or "").upper() == "TRAILING_STOP_MARKET" for o in prot)

                if exp_sl and not has_sl:
                    audit["issues"]["missing_sl"].append({"symbol": s, "expected_sl": sl_price})
                if exp_tp and not has_tp:
                    audit["issues"]["missing_tp"].append({"symbol": s})
                if exp_trail and not has_trail:
                    audit["issues"]["missing_trail"].append({"symbol": s})

                # duplicates (more than one protective order type)
                dups = {k: v for k, v in type_counts.items() if v and v > 1}
                if dups:
                    audit["issues"]["duplicates"].append({"symbol": s, "types": dups})

                # orphan protective orders (no position AND no active DB trade)
                if (not has_pos) and (t is None) and prot:
                    # keep lightweight list
                    oo = []
                    for o in prot[:30]:
                        oo.append({
                            "orderId": o.get("orderId"),
                            "type": o.get("type"),
                            "side": o.get("side"),
                            "reduceOnly": o.get("reduceOnly"),
                            "closePosition": o.get("closePosition"),
                            "stopPrice": o.get("stopPrice"),
                            "activationPrice": o.get("activatePrice") or o.get("activationPrice"),
                            "price": o.get("price"),
                            "origQty": o.get("origQty"),
                            "status": o.get("status"),
                        })
                    audit["issues"]["orphan_orders"].append({"symbol": s, "count": len(prot), "orders": oo})

                # per-symbol summary for UI
                audit["per_symbol"][s] = {
                    "has_position": bool(has_pos),
                    "position": pos_by_symbol.get(s) if has_pos else None,
                    "db_trade": {
                        "id": t.get("id") if t else None,
                        "side": t.get("side") if t else None,
                        "qty": t.get("qty") if t else None,
                        "entry_price": t.get("entry_price") if t else None,
                        "stop_loss": t.get("stop_loss") if t else None,
                    } if t else None,
                    "open_orders": {
                        "total": len(ol),
                        "protective": len(prot),
                        "types": type_counts,
                    },
                }

            # persist to DB settings for Doctor
            try:
                self.db.set_setting("protection_audit_status", json.dumps(audit), source="execution_monitor", bump_version=False, audit=False)
            except Exception:
                pass

        except Exception:
            # never crash monitor due to audit
            return


    def _maybe_take_snapshot(self, active_trades_count: int = 0) -> None:
        if not self.snapshot_enabled:
            return

        now = time.time()
        if (now - self._last_snapshot_ts) < self.snapshot_every_sec:
            return

        wallet, unreal, equity = self._get_futures_equity()
        if equity <= 0:
            self._last_snapshot_ts = now
            return

        meta = {
            "source": "execution_monitor",
            "active_trades": int(active_trades_count),
            "testnet": bool(getattr(cfg, "USE_TESTNET", True)),
            "wallet_balance": wallet,
            "unrealized_pnl": unreal,
        }
        self._save_equity_snapshot(total_balance=equity, unrealized_pnl=unreal, meta=meta)
        self._last_snapshot_ts = now

    def _get_futures_equity(self):
        """
        Returns (wallet_balance, unrealized_pnl, equity/total_margin_balance) in USDT.
        Best-effort across Binance client variants.
        """
        # Preferred: futures_account() if keys are present
        try:
            acc = self.client.futures_account()
            wallet = float(acc.get("totalWalletBalance", 0) or 0)
            unreal = float(acc.get("totalUnrealizedProfit", 0) or 0)
            equity = float(acc.get("totalMarginBalance", 0) or 0)
            if equity <= 0:
                equity = wallet + unreal
            return wallet, unreal, equity
        except Exception:
            pass

        # Fallback: futures_account_balance() (public-ish)
        try:
            bals = self.client.futures_account_balance()
            for b in bals:
                if str(b.get("asset", "")).upper() == "USDT":
                    wallet = float(b.get("balance", 0) or 0)
                    return wallet, 0.0, wallet
        except Exception:
            pass

        return 0.0, 0.0, 0.0

    def _save_equity_snapshot(self, total_balance: float, unrealized_pnl: float, meta: dict) -> None:
        # New API (database.new.py)
        try:
            if hasattr(self.db, "add_equity_snapshot"):
                self.db.add_equity_snapshot(
                    total_balance=float(total_balance),
                    unrealized_pnl=float(unrealized_pnl),
                    source="execution_monitor",
                    meta=meta,
                )
                return
        except Exception:
            pass

        # Legacy fallback insert
        try:
            ts = self._utc_iso()
            lock = getattr(self.db, "lock", None)
            cur = getattr(self.db, "cursor", None)
            conn = getattr(self.db, "conn", None)
            if cur is None or conn is None:
                return
            sql = "INSERT INTO equity_history (timestamp, total_balance, unrealized_pnl) VALUES (?, ?, ?)"
            if lock:
                with lock:
                    cur.execute(sql, (ts, float(total_balance), float(unrealized_pnl)))
                    conn.commit()
            else:
                cur.execute(sql, (ts, float(total_balance), float(unrealized_pnl)))
                conn.commit()
        except Exception:
            pass

    def _maybe_prune_snapshots(self) -> None:
        now = time.time()
        if (now - self._last_prune_ts) < self._prune_every_sec:
            return

        retention_days = int(self.snapshot_retention_days or 0)
        if retention_days <= 0:
            self._last_prune_ts = now
            return

        try:
            if hasattr(self.db, "prune_equity_history"):
                self.db.prune_equity_history(retention_days=retention_days)
        except Exception:
            pass

        self._last_prune_ts = now

# =========================================
# RUN
# =========================================
if __name__ == "__main__":
    ExecutionMonitor().run()
