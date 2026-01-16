"""
trading_executor.py

Binance execution primitives used by the bot.

Project rules:
- UTC handled at higher layers (database/logging). Executor stays stateless.
- Governance: by default, functions accept `execution_allowed=True`. Callers (main.py / dashboard approval flow)
  should pass `execution_allowed=False` when a signal is only a recommendation.
- Backward compatible: existing function names/signatures remain; new parameters are optional and appended.

Notes:
- PAPER_TRADING is read dynamically from cfg each call (so cfg.reload() / dashboard toggles are respected).
"""

from __future__ import annotations

import time
import math
from typing import Any, Dict, Optional, Tuple

from binance.client import Client


class _NoPingClient(Client):
    """Disable spot ping() during init to avoid Spot-Testnet 502 killing futures modules."""
    def ping(self):
        try:
            return {}
        except Exception:
            return {}


# ==========================================================
# BINANCE ENDPOINTS (Futures) - testnet-safe
# Official Futures Testnet REST base: https://demo-fapi.binance.com
# ==========================================================

def _futures_rest_base(use_testnet: bool) -> str:
    v = str(getattr(cfg, "BINANCE_FUTURES_REST_BASE", "") or "")
    if v.strip():
        return v.strip().rstrip("/")
    return "https://demo-fapi.binance.com" if use_testnet else "https://fapi.binance.com"

def _configure_futures_endpoints(client: Client, use_testnet: bool) -> None:
    rest_base = _futures_rest_base(use_testnet)
    candidates = {
        "FUTURES_URL": f"{rest_base}/fapi",
        "FUTURES_TESTNET_URL": f"{rest_base}/fapi",
        "FUTURES_DATA_URL": f"{rest_base}/futures/data",
        "FUTURES_TESTNET_DATA_URL": f"{rest_base}/futures/data",
    }
    for attr, url in candidates.items():
        if hasattr(client, attr):
            try:
                setattr(client, attr, url)
            except Exception:
                pass
from binance.exceptions import BinanceAPIException
from binance.helpers import round_step_size  # [ADDED] Essential for rounding

from config import cfg

# ==========================================================
# BINANCE CLIENT (lazy-ish init)
# ==========================================================
client: Optional[Client] = None
# [ADDED] Cache for exchange info to avoid repeated API calls
_exchange_info = {}


def _public_price(symbol: str) -> float:
    """Best-effort public price for PAPER mode (no auth keys required)."""
    try:
        from binance.client import Client as _C
        c = _C(None, None)
        # Futures mark price
        try:
            r = c.futures_mark_price(symbol=symbol)
            mp = float(r.get("markPrice") or 0.0)
            if mp > 0:
                return mp
        except Exception:
            pass
        # Spot ticker fallback
        try:
            r2 = c.get_symbol_ticker(symbol=symbol)
            p = float(r2.get("price") or 0.0)
            if p > 0:
                return p
        except Exception:
            pass
    except Exception:
        pass
    return 0.0




# ==========================================================
# POSITION MODE (Hedge vs One-way) helpers [ADDED]
# ==========================================================
_position_mode_cache = {"dual": None, "ts": 0.0}

def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "t")

def _is_hedge_mode(ttl_sec: float = 60.0) -> bool:
    """Return True if account is in Hedge Mode (dualSidePosition=True). Cached."""
    global _position_mode_cache
    if client is None:
        return False
    now = time.time()
    try:
        if _position_mode_cache.get("dual") is not None and (now - float(_position_mode_cache.get("ts") or 0.0)) < ttl_sec:
            return bool(_position_mode_cache.get("dual"))
    except Exception:
        pass
    try:
        r = client.futures_get_position_mode()  # type: ignore[union-attr]
        dual = bool((r or {}).get("dualSidePosition"))
        _position_mode_cache = {"dual": dual, "ts": now}
        return dual
    except Exception:
        _position_mode_cache = {"dual": False, "ts": now}
        return False

def _pos_side_param(position_side: str) -> Optional[str]:
    ps = str(position_side or "").upper().strip()
    return ps if ps in ("LONG", "SHORT") else None

def _get_mark_price(symbol: str) -> float:
    try:
        if client is None:
            return 0.0
        r = client.futures_mark_price(symbol=symbol)  # type: ignore[union-attr]
        return float((r or {}).get("markPrice") or 0.0)
    except Exception:
        return 0.0

def _get_last_price(symbol: str) -> float:
    try:
        if client is None:
            return 0.0
        t = client.futures_symbol_ticker(symbol=symbol)  # type: ignore[union-attr]
        return float((t or {}).get("price") or 0.0)
    except Exception:
        try:
            return float(_public_price(symbol) or 0.0)
        except Exception:
            return 0.0


# ==========================================================
# MODE CACHE (LIVE vs PAPER) [FIX]
# ==========================================================
_mode_cache: str = "PAPER"
_mode_cache_ts: float = 0.0
_dbm = None

def _get_mode_cached(ttl_sec: float = 2.0) -> str:
    """Read `mode` from DB settings with lightweight caching.

    Expected values: LIVE / PAPER / TEST (case-insensitive).
    Safety default: PAPER if missing/invalid or DB unavailable.
    """
    global _mode_cache, _mode_cache_ts, _dbm
    now = time.time()
    if (now - _mode_cache_ts) < ttl_sec and _mode_cache:
        return _mode_cache

    try:
        # Lazy import to avoid any startup circulars
        from database import DatabaseManager
        if _dbm is None:
            _dbm = DatabaseManager(getattr(cfg, "DB_FILE", None))
        mode = _dbm.get_setting("mode", "PAPER")
        if isinstance(mode, str):
            mode = mode.strip().upper()
        else:
            mode = "PAPER"

        if mode not in {"LIVE", "PAPER", "TEST"}:
            mode = "PAPER"

        _mode_cache = mode
        _mode_cache_ts = now
        return _mode_cache
    except Exception:
        _mode_cache_ts = now
        _mode_cache = "PAPER"
        return _mode_cache


def _is_paper() -> bool:
    """Effective paper mode.

    Safety:
    - PAPER_TRADING=True => always paper (no live orders).
    - Otherwise, DB setting `mode` controls LIVE vs TEST/PAPER.
    """
    try:
        if bool(getattr(cfg, "PAPER_TRADING", False)):
            return True
    except Exception:
        return True
    try:
        mode = _get_mode_cached()
        return mode != "LIVE"
    except Exception:
        return True


def init_client(force: bool = False) -> Optional[Client]:
    """Initialize (or re-initialize) the Binance client.

    Returns:
        Client instance or None (if missing keys or init fails).
    """
    global client
    if client is not None and not force:
        return client

    # Don't init network client in paper/TEST mode
    if _is_paper():
        if force:
            # allow force re-init in case mode switched to LIVE; re-check below
            pass
        else:
            client = None
            print("📝 PAPER TRADING ACTIVE — No real money will be used")
            return None

    api_key = getattr(cfg, "BINANCE_API_KEY", None) or ""
    api_secret = getattr(cfg, "BINANCE_API_SECRET", None) or ""

    if not api_key or not api_secret:
        if not _is_paper():
            print("[CRITICAL] Missing BINANCE_API_KEY / BINANCE_API_SECRET for LIVE trading.")
            client = None
            return None

    try:
        client = _NoPingClient(api_key, api_secret)
        _configure_futures_endpoints(client, bool(getattr(cfg, "USE_TESTNET", False)))
        print(f"[SYSTEM] Executor Connected | Testnet={getattr(cfg, 'USE_TESTNET', True)}")
        if _is_paper():
            print("📝 PAPER TRADING ACTIVE — No real money will be used")
        return client
    except Exception as e:
        print(f"[CRITICAL] Binance client initialization failed: {e}")
        client = None
        return None


# Do initial init once at import (safe).
init_client(force=False)


# [MODIFIED] Added this helper as requested to check DB mode
def is_paper_trading():
    """Backward compatible alias for _is_paper()."""
    return _is_paper()



# ==========================================================
# PRECISION & FORMATTING HELPERS [ADDED]
# ==========================================================
def _get_symbol_precision(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch and cache precision rules for a symbol."""
    global _exchange_info
    if client is None:
        return None

    # If cache is empty, fetch all symbols once (efficient)
    if not _exchange_info:
        try:
            info = client.futures_exchange_info()
            for s in info['symbols']:
                _exchange_info[s['symbol']] = {
                    'price_precision': int(s['pricePrecision']),
                    'qty_precision': int(s['quantityPrecision']),
                    'tick_size': float(next((f['tickSize'] for f in s['filters'] if f['filterType'] == 'PRICE_FILTER'), 0)),
                    'step_size': float(next((f['stepSize'] for f in s['filters'] if f['filterType'] == 'LOT_SIZE'), 0))
                }
        except Exception as e:
            print(f"[WARN] Failed to fetch exchange info: {e}")
            return None

    return _exchange_info.get(symbol)


def _format_val(val: float, step_size: float, precision: int) -> float:
    """Round value according to Binance step_size/tick_size rules."""
    if not val or not step_size:
        return float(val)
    return float(round_step_size(val, step_size))

def _to_fixed_str(val: float, precision: int) -> Optional[str]:
    """Return a fixed-point decimal string (no scientific notation) for Binance params."""
    try:
        v = float(val)
    except Exception:
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    try:
        p = int(precision)
    except Exception:
        p = 8
    if p < 0:
        p = 8
    # fixed decimal, trim trailing zeros/dot
    s = f"{v:.{p}f}"
    s = s.rstrip("0").rstrip(".")
    return s or None



# ==========================================================
# INTERNAL GUARDS
# ==========================================================
def _client_guard():
    paper = is_paper_trading()  # Use the DB check
    if client is None and not paper:
        return False, {'ok': False, 'error': 'Binance client not initialized'}, paper
    return True, None, paper


def _execution_guard(execution_allowed: bool) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Central governance guard."""
    if not execution_allowed:
        return False, {"ok": False, "error": "Execution not allowed (pending approval)"}
    return True, None


def _clamp_leverage(leverage: int) -> int:
    max_lev = int(getattr(cfg, "MAX_LEVERAGE", 20))
    try:
        lev = int(leverage)
    except Exception:
        lev = 1
    if lev < 1:
        lev = 1
    if lev > max_lev:
        lev = max_lev
    return lev


# ==========================================================
# LEVERAGE (FUTURES)
# ==========================================================
def set_leverage(symbol: str, leverage: int, *, execution_allowed: bool = True) -> Dict[str, Any]:
    if is_paper_trading():
        return {"ok": True}

    ok, err = _execution_guard(execution_allowed)
    if not ok:
        return err  # type: ignore[return-value]

    ok, err, _ = _client_guard()
    if not ok:
        return err  # type: ignore[return-value]

    lev = _clamp_leverage(leverage)

    try:
        client.futures_change_leverage(symbol=symbol, leverage=lev)  # type: ignore[union-attr]
        return {"ok": True, "result": {"leverage": lev}}
    except BinanceAPIException as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": f"Unexpected error: {e}"}


# ==========================================================
# MARKET ORDER
# ==========================================================
def place_market_order(
    symbol: str,
    side: str,
    qty: float,
    sl_price: Optional[float] = None,
    market_type: str = "futures",
    *,
    position_side: str = "BOTH",
    execution_allowed: bool = True,
) -> Dict[str, Any]:
    """Place a market order in spot or futures."""
    side = str(side).upper().strip()
    if side not in {"BUY", "SELL"}:
        return {"ok": False, "error": f"Invalid side: {side}"}

    try:
        qty_f = float(qty)
    except Exception:
        return {"ok": False, "error": f"Invalid qty: {qty}"}
    if qty_f <= 0:
        return {"ok": False, "error": f"qty must be > 0 (got {qty_f})"}

    # ---------------- PAPER MODE ----------------
    if is_paper_trading():
        print(f"[PAPER] 🛒 {side} {qty_f} {symbol} | SL={sl_price} | market={market_type}")

        return {
            "ok": True,
            "result": {
                "orderId": "PAPER_SIM",
                "avgPrice": _public_price(symbol),
                "executedQty": qty_f,
            },
        }

    # ---------------- LIVE MODE ----------------
    ok, err = _execution_guard(execution_allowed)
    if not ok:
        return err  # type: ignore[return-value]

    ok, err, _ = _client_guard()
    if not ok:
        return err  # type: ignore[return-value]

    # [ADDED] Fetch precision info and round quantity
    prec_info = _get_symbol_precision(symbol)
    if prec_info:
        qty_f = _format_val(qty_f, prec_info['step_size'], prec_info['qty_precision'])
        print(f"[DEBUG] Rounded Qty: {qty} -> {qty_f} for {symbol}")

    market_type = str(market_type).lower().strip()
    try:
        # -------- SPOT --------
        if market_type == "spot":
            order = client.create_order(  # type: ignore[union-attr]
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=qty_f,
            )
            executed = float(order.get("executedQty", 0) or 0)
            quote = float(order.get("cummulativeQuoteQty", 0) or 0)
            avg_price = quote / executed if executed > 0 else 0.0
            return {"ok": True, "result": {"orderId": order.get("orderId"), "avgPrice": avg_price, "executedQty": executed}}

        # -------- FUTURES (default) --------
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty_f,
        }
        if position_side and str(position_side).upper() in {"LONG", "SHORT", "BOTH"}:
            params["positionSide"] = str(position_side).upper()

        order = client.futures_create_order(**params)  # type: ignore[union-attr]

        _ex_qty = 0.0
        try:
            _ex_qty = float(order.get("executedQty") or 0.0)
        except Exception:
            _ex_qty = 0.0
        if _ex_qty <= 0:
            _ex_qty = qty_f


        avg_price = float(order.get("avgPrice", 0) or 0.0)
        if avg_price <= 0:
            fills = order.get("fills", []) or []
            if fills:
                try:
                    avg_price = float(fills[0].get("price", 0) or 0.0)
                except Exception:
                    pass
        # 2) Place Initial Stop Loss (optional)
        if sl_price and float(sl_price) > 0:
            sl_side = "SELL" if side == "BUY" else "BUY"

            # Round SL price to tick size
            final_sl = float(sl_price)
            if prec_info:
                final_sl = _format_val(final_sl, prec_info["tick_size"], prec_info["price_precision"])

            # Sanity check to avoid "Order would immediately trigger" (-2021)
            try:
                mp = _get_mark_price(symbol) or avg_price or 0.0
                tick = float(prec_info.get("tick_size") or 0.0) if prec_info else 0.0
                buf = max(tick * 2.0, mp * 0.001) if mp > 0 else (tick * 2.0)

                if side == "BUY" and mp > 0 and final_sl >= (mp - buf):
                    raise ValueError(f"SL too close/invalid for LONG (sl={final_sl}, mark={mp})")
                if side == "SELL" and mp > 0 and final_sl <= (mp + buf):
                    raise ValueError(f"SL too close/invalid for SHORT (sl={final_sl}, mark={mp})")

                pprec = int(prec_info.get("price_precision", 8) or 8) if prec_info else 8
                sl_str = _to_fixed_str(final_sl, pprec)
                if not sl_str:
                    raise ValueError(f"Invalid SL price after formatting: {final_sl}")
                client.futures_create_order(  # type: ignore[union-attr]
                    symbol=symbol,
                    side=sl_side,
                    type="STOP_MARKET",
                    stopPrice=sl_str,
                    closePosition=True,
                )
            except Exception as e:
                print(f"[WARN] Could not place Initial SL for {symbol} at {final_sl}: {e}")

        return {
            "ok": True,
            "result": {
                "orderId": order.get("orderId"),
                "avgPrice": avg_price,
                "executedQty": (_ex_qty if _ex_qty > 0 else qty_f),
            },
        }

    except BinanceAPIException as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": f"Unexpected error: {e}"}


# ==========================================================
# MOVE STOP LOSS (FUTURES)
# ==========================================================
def move_stop_loss(symbol: str, new_sl_price: float, *, execution_allowed: bool = True) -> bool:
    """Update the active STOP_MARKET order (used for trailing stops)."""
    if is_paper_trading():
        print(f"[PAPER] 🛡️ MOVE SL {symbol} → {new_sl_price}")
        return True

    ok, err = _execution_guard(execution_allowed)
    if not ok:
        return False

    ok, err, _ = _client_guard()
    if not ok:
        return False

    try:
        # 1) Cancel existing STOP_MARKET orders
        orders = client.futures_get_open_orders(symbol=symbol)  # type: ignore[union-attr]
        for o in orders:
            if o.get("type") == "STOP_MARKET":
                try:
                    client.futures_cancel_order(symbol=symbol, orderId=o.get("orderId"))  # type: ignore[union-attr]
                except Exception:
                    pass

        # 2) Detect position direction
        positions = client.futures_position_information(symbol=symbol)  # type: ignore[union-attr]
        amt = 0.0
        for p in positions:
            try:
                pa = float(p.get("positionAmt", 0) or 0)
            except Exception:
                pa = 0.0
            if pa != 0:
                amt = pa
                break

        if amt == 0:
            return False

        side = "SELL" if amt > 0 else "BUY"

        # [ADDED] Round new SL Price
        prec_info = _get_symbol_precision(symbol)
        final_sl = float(new_sl_price)
        if prec_info:
            final_sl = _format_val(final_sl, prec_info['tick_size'], prec_info['price_precision'])

        # 3) Place new STOP_MARKET
        pprec = int(prec_info.get("price_precision", 8) or 8) if prec_info else 8
        sl_str = _to_fixed_str(final_sl, pprec)
        if not sl_str:
            print(f"[ERROR] move_stop_loss invalid stopPrice {final_sl} for {symbol}")
            return False
        client.futures_create_order(  # type: ignore[union-attr]
            symbol=symbol,
            side=side,
            type="STOP_MARKET",
            stopPrice=sl_str,
            closePosition=True,
        )
        return True

    except Exception as e:
        print(f"[ERROR] move_stop_loss failed for {symbol}: {e}")
        return False


# ==========================================================
# CLOSE POSITION (FUTURES)
# ==========================================================
def close_open_position(symbol: str, *, execution_allowed: bool = True) -> Dict[str, Any]:
    if is_paper_trading():
        print(f"[PAPER] 💰 CLOSE {symbol}")
        return {"ok": True, "result": {"avgPrice": _public_price(symbol)}}

    ok, err = _execution_guard(execution_allowed)
    if not ok:
        return err  # type: ignore[return-value]

    ok, err, _ = _client_guard()
    if not ok:
        return err  # type: ignore[return-value]

    try:
        # 1) Cancel open orders
        try:
            client.futures_cancel_all_open_orders(symbol=symbol)  # type: ignore[union-attr]
        except Exception:
            pass

        # 2) Get position amount
        positions = client.futures_position_information(symbol=symbol)  # type: ignore[union-attr]
        amt = 0.0
        for p in positions:
            try:
                pa = float(p.get("positionAmt", 0) or 0)
            except Exception:
                pa = 0.0
            if pa != 0:
                amt = pa
                break

        if amt == 0:
            return {"ok": True, "result": {"avgPrice": _public_price(symbol)}}

        # 3) Market close
        side = "SELL" if amt > 0 else "BUY"
        order = client.futures_create_order(  # type: ignore[union-attr]
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=abs(amt),
            reduceOnly=True,
        )

        avg_price = float(order.get("avgPrice", 0) or 0.0)
        if avg_price <= 0:
            fills = order.get("fills", []) or []
            if fills:
                try:
                    avg_price = float(fills[0].get("price", 0) or 0.0)
                except Exception:
                    pass

        return {"ok": True, "result": {"avgPrice": avg_price}}

    except BinanceAPIException as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": f"Unexpected error: {e}"}


# ==========================================================
# HARD TP (FUTURES)
# ==========================================================
def place_hard_tp(
    symbol: str,
    position_side: str,
    tp_price: float,
    market_type: str = "futures",
    *,
    execution_allowed: bool = True,
) -> bool:
    """Place TAKE_PROFIT_MARKET closePosition order (futures only)."""
    if is_paper_trading():
        print(f"[PAPER] ⚡ HARD TP {symbol} @ {tp_price}")
        return True

    ok, err = _execution_guard(execution_allowed)
    if not ok:
        return False

    ok, err, _ = _client_guard()
    if not ok:
        return False

    if str(market_type).lower().strip() != "futures":
        return False

    # [ADDED] Round TP Price
    prec_info = _get_symbol_precision(symbol)
    final_tp = float(tp_price)
    if prec_info:
        final_tp = _format_val(final_tp, prec_info['tick_size'], prec_info['price_precision'])

    try:
        side = "SELL" if str(position_side).upper() == "LONG" else "BUY"
        pprec = int(prec_info.get("price_precision", 8) or 8) if prec_info else 8
        tp_str = _to_fixed_str(final_tp, pprec)
        if not tp_str:
            raise ValueError(f"Invalid TP price after formatting: {final_tp}")
        client.futures_create_order(  # type: ignore[union-attr]
            symbol=symbol,
            side=side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=tp_str,
            closePosition=True,
        )
        return True
    except Exception as e:
        print(f"[WARN] Hard TP failed: {e}")
        return False



def ensure_stop_loss(
    symbol: str,
    position_side: str,
    sl_price: float,
    *,
    execution_allowed: bool = True,
    tolerance_pct: float = 0.001,
) -> bool:
    """
    Ensure exactly ONE protective STOP_MARKET closePosition order exists (idempotent).
    - Cancels duplicates / wrong-side SLs.
    - In Hedge Mode, includes positionSide (LONG/SHORT) when available.
    """
    try:
        sl_price = float(sl_price or 0.0)
    except Exception:
        sl_price = 0.0
    if sl_price <= 0:
        return True

    if is_paper_trading():
        print(f"[PAPER] 🛡️ ENSURE SL {symbol} @ {sl_price}")
        return True

    ok, _ = _execution_guard(execution_allowed)
    if not ok:
        return False

    ok, _, _ = _client_guard()
    if not ok:
        return False

    ps = str(position_side or "").upper().strip()
    if ps in ("LONG", "BUY"):
        close_side = "SELL"
    elif ps in ("SHORT", "SELL"):
        close_side = "BUY"
    else:
        close_side = "SELL"

    hedge = _is_hedge_mode()
    ps_param = _pos_side_param(ps)

    # --- precision / rounding ---
    try:
        info = _get_symbol_precision(symbol) or {}
        tick = float(info.get("tick_size", 0) or 0.0)
        pprec = int(info.get("price_precision", 8) or 8)
        sl_fmt = float(_format_val(sl_price, tick, pprec)) if tick else float(round(sl_price, pprec))
    except Exception:
        pprec = 8
        sl_fmt = float(sl_price)

    sl_str = _to_fixed_str(sl_fmt, pprec)
    if not sl_str:
        print(f"[EXEC] ensure_stop_loss invalid stopPrice {sl_fmt} for {symbol}")
        return False

    # --- fetch existing open orders ---
    try:
        orders = client.futures_get_open_orders(symbol=symbol) or []  # type: ignore[union-attr]
    except Exception:
        orders = []

    protective = []
    for o in orders:
        try:
            t = str(o.get("type", "")).upper()
            if t not in ("STOP_MARKET", "STOP"):
                continue
            if not _truthy(o.get("closePosition")):
                continue

            # In Hedge Mode, accept only matching positionSide if provided
            if hedge and ps_param:
                op = str(o.get("positionSide") or "").upper().strip()
                if op and op != ps_param:
                    continue

            protective.append(o)
        except Exception:
            continue

    # Split correct-side vs wrong-side
    correct = []
    wrong = []
    for o in protective:
        try:
            oside = str(o.get("side") or "").upper().strip()
        except Exception:
            oside = ""
        if oside == close_side:
            correct.append(o)
        else:
            wrong.append(o)

    # Cancel any wrong-side protective SLs (always)
    for o in wrong:
        try:
            oid = o.get("orderId")
            if oid is not None:
                client.futures_cancel_order(symbol=symbol, orderId=oid)  # type: ignore[union-attr]
        except Exception:
            pass

    # If any correct order is close enough, keep ONE and cancel duplicates
    best = None
    best_diff = None
    for o in correct:
        try:
            sp = float(o.get("stopPrice", 0) or 0.0)
        except Exception:
            sp = 0.0
        if sp <= 0:
            continue
        denom = max(1e-9, sp)
        rel = abs(sp - sl_fmt) / denom
        if rel <= float(tolerance_pct):
            diff = abs(sp - sl_fmt)
            if best is None or (best_diff is not None and diff < best_diff):
                best = o
                best_diff = diff

    if best is not None:
        # cancel duplicates, keep best
        for o in correct:
            if o is best:
                continue
            try:
                oid = o.get("orderId")
                if oid is not None:
                    client.futures_cancel_order(symbol=symbol, orderId=oid)  # type: ignore[union-attr]
            except Exception:
                pass
        return True

    # Otherwise cancel all remaining correct-side protective SLs
    for o in correct:
        try:
            oid = o.get("orderId")
            if oid is not None:
                client.futures_cancel_order(symbol=symbol, orderId=oid)  # type: ignore[union-attr]
        except Exception:
            pass

    # Create the new SL
    try:
        params: Dict[str, Any] = dict(
            symbol=symbol,
            side=close_side,
            type="STOP_MARKET",
            stopPrice=sl_str,
            closePosition=True,
        )
        if hedge and ps_param:
            params["positionSide"] = ps_param

        client.futures_create_order(**params)  # type: ignore[union-attr]
        return True
    except Exception as e:
        print(f"[EXEC] ensure_stop_loss failed {symbol}: {e}")
        return False


def ensure_hard_tp(symbol: str, position_side: str, tp_price: float, *, execution_allowed: bool = True, tolerance_pct: float = 0.001) -> bool:
    """
    Ensure a protective TAKE_PROFIT_MARKET closePosition order exists at (approximately) tp_price.
    If missing or materially different, it will cancel existing TAKE_PROFIT_MARKET closePosition orders and recreate.
    """
    try:
        tp_price = float(tp_price or 0.0)
    except Exception:
        tp_price = 0.0
    if tp_price <= 0:
        return True

    if is_paper_trading():
        print(f"[PAPER] 🧲 ENSURE TP {symbol} @ {tp_price}")
        return True

    ok, err = _execution_guard(execution_allowed)
    if not ok:
        return False

    ok, err, _ = _client_guard()
    if not ok:
        return False

    ps = str(position_side or "").upper().strip()
    if ps in ("LONG", "BUY"):
        side = "SELL"
    elif ps in ("SHORT", "SELL"):
        side = "BUY"
    else:
        side = "SELL"

    try:
        info = _get_symbol_precision(symbol) or {}
        tick = float(info.get("tick_size", 0) or 0.0)
        pprec = int(info.get("price_precision", 8) or 8)
        tp_fmt = float(_format_val(tp_price, tick, pprec)) if tick else float(round(tp_price, pprec))
    except Exception:
        pprec = 8
        tp_fmt = float(tp_price)

    tp_str = _to_fixed_str(tp_fmt, pprec)
    if not tp_str:
        print(f"[EXEC] ensure_hard_tp invalid stopPrice {tp_fmt} for {symbol}")
        return False

    try:
        orders = client.futures_get_open_orders(symbol=symbol) or []
    except Exception:
        orders = []

    existing = []
    for o in orders:
        try:
            if str(o.get("type", "")).upper() == "TAKE_PROFIT_MARKET" and str(o.get("closePosition", "")).lower() == "true":
                existing.append(o)
        except Exception:
            continue

    for o in existing:
        try:
            sp = float(o.get("stopPrice", 0) or 0.0)
        except Exception:
            sp = 0.0
        if sp > 0:
            denom = max(1e-9, sp)
            if abs(sp - tp_fmt) / denom <= float(tolerance_pct):
                return True

    for o in existing:
        try:
            oid = o.get("orderId")
            if oid is not None:
                client.futures_cancel_order(symbol=symbol, orderId=oid)
        except Exception:
            pass

    try:
        client.futures_create_order(
            symbol=symbol,
            side=side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=tp_str,
            closePosition=True,
        )
        return True
    except Exception as e:
        print(f"[EXEC] ensure_hard_tp failed {symbol}: {e}")
        return False


def ensure_trailing_stop(
    symbol: str,
    position_side: str,
    qty: float,
    *,
    activation_price: Optional[float] = None,
    callback_rate: float = 0.3,
    working_type: str = "MARK_PRICE",
    price_protect: bool = True,
    execution_allowed: bool = True,
    tolerance_pct: float = 0.001,
) -> bool:
    """
    Ensure an exchange-native TRAILING_STOP_MARKET reduceOnly order exists.

    Important:
    - In Hedge Mode, `positionSide` MUST be sent (LONG/SHORT).
    - activationPrice formatting is fixed-point (no scientific notation).
    """
    try:
        qty = float(qty or 0.0)
    except Exception:
        qty = 0.0
    if qty <= 0:
        return True

    try:
        cb = float(callback_rate or 0.0)
    except Exception:
        cb = 0.0
    if cb < 0.1:
        cb = 0.1
    if cb > 5.0:
        cb = 5.0
    # Binance expects callbackRate in 0.1% increments.
    cb = math.ceil(float(cb) * 10 - 1e-9) / 10.0
    cb_str = f"{cb:.1f}"

    if is_paper_trading():
        ap_txt = f"{activation_price:.6f}" if activation_price else "AUTO"
        print(f"[PAPER] 🪁 ENSURE TRAIL {symbol} qty={qty} act={ap_txt} cb={cb}%")
        return True

    ok, _ = _execution_guard(execution_allowed)
    if not ok:
        return False

    ok, _, _ = _client_guard()
    if not ok:
        return False

    ps = str(position_side or "").upper().strip()
    if ps in ("LONG", "BUY"):
        side = "SELL"
    elif ps in ("SHORT", "SELL"):
        side = "BUY"
    else:
        print(f"[EXEC] ensure_trailing_stop invalid position_side={position_side} for {symbol}")
        return False

    hedge = _is_hedge_mode()
    ps_param = _pos_side_param(ps)

    wt = str(working_type or "MARK_PRICE").upper().strip()
    if wt not in ("MARK_PRICE", "CONTRACT_PRICE"):
        wt = "MARK_PRICE"

    # ---------------- Symbol precision / formatting ----------------
    try:
        info = _get_symbol_precision(symbol) or {}
        step = float(info.get("step_size", 0) or 0.0)
        qprec = int(info.get("qty_precision", 8) or 8)
        tick = float(info.get("tick_size", 0) or 0.0)
        pprec = int(info.get("price_precision", 8) or 8)
    except Exception:
        step, tick, qprec, pprec = 0.0, 0.0, 8, 8

    def _fmt_px(p: float) -> float:
        try:
            p = float(p or 0.0)
        except Exception:
            return 0.0
        if p <= 0:
            return 0.0
        try:
            if tick and tick > 0:
                return float(_format_val(p, tick, pprec))
        except Exception:
            pass
        return float(round(p, pprec))

    # qty
    try:
        qty_fmt = float(_format_val(qty, step, qprec)) if step else float(round(qty, qprec))
    except Exception:
        qty_fmt = float(qty)
    qty_str = _to_fixed_str(qty_fmt, qprec) or str(qty_fmt)

    # ---------------- Latest prices (mark + last) ----------------
    mark_fmt = _fmt_px(_get_mark_price(symbol))
    last_fmt = _fmt_px(_get_last_price(symbol))

    prices = [p for p in (mark_fmt, last_fmt) if p > 0]
    if not prices:
        print(f"[EXEC] ensure_trailing_stop cannot fetch latest price for {symbol}")
        return False

    # conservative ref that satisfies inequality even if Binance uses a different "latest"
    ref = (min(prices) if side == "BUY" else max(prices))

    # activation input
    try:
        ap_in = float(activation_price or 0.0)
    except Exception:
        ap_in = 0.0

    # --- Why -2021 happens on tiny-priced symbols ---
    # For TRAILING_STOP_MARKET, Binance can reject the order as "would immediately trigger" (-2021)
    # when the activationPrice step (tick) is *bigger (in %)* than the callbackRate distance.
    # Example: price=0.00001910, tick=0.00000010 => 1 tick ~= 0.52%.
    # If callbackRate=0.30%, ANY valid activationPrice (>= 1 tick away) looks like it would trigger.
    # So we auto-raise callbackRate just enough to be compatible with the tick size.

    tick_pct = 0.0
    if ref > 0 and tick and tick > 0:
        try:
            tick_pct = (tick / ref) * 100.0
        except Exception:
            tick_pct = 0.0

    if tick_pct > 0:
        # Keep a small safety margin above 1 tick.
        min_cb = max(0.1, (tick_pct * 1.20) + 0.05)
        if cb < min_cb:
            old_cb = cb
            cb = min(min_cb, 5.0)
            # Log once per call (helps the user understand why callback changed)
            print(
                f"[EXEC] ensure_trailing_stop {symbol}: callbackRate raised {old_cb:.2f}% -> {cb:.2f}% "
                f"(tick≈{tick_pct:.2f}% of price)"
            )

    # Binance expects callbackRate in 0.1% increments (e.g., 0.7 not 0.68).
    # Passing values like 0.68 can be rejected as "Invalid callBack rate".
    try:
        cb_before = float(cb)
    except Exception:
        cb_before = cb
    try:
        cb = float(cb)
    except Exception:
        cb = 0.3
    cb = max(0.1, min(5.0, cb))
    cb = math.ceil(cb * 10 - 1e-9) / 10.0
    cb_str = f"{cb:.1f}"
    if isinstance(cb_before, (int, float)) and abs(float(cb_before) - float(cb)) > 1e-12:
        print(f"[EXEC] ensure_trailing_stop {symbol}: callbackRate quantized -> {cb_str}% (step=0.1)")
    # Activation gap: keep it as tight as possible (1 tick) so we don't trip immediate-trigger checks.
    if tick and tick > 0:
        min_gap = tick * 1.0
    else:
        # Fallback when tick is unknown
        min_gap = max(ref * 0.001, ref * 0.0005)

    def _compute_activation(attempt: int) -> tuple[float, Optional[str]]:
        gap = min_gap * float(attempt + 1)
        ap = ap_in

        if ap <= 0:
            ap = (ref - gap) if side == "BUY" else (ref + gap)
        else:
            if side == "BUY" and ap >= ref:
                ap = ref - gap
            if side == "SELL" and ap <= ref:
                ap = ref + gap

        ap_fmt = _fmt_px(ap)

        # Enforce strict inequality after rounding.
        # IMPORTANT: for tiny-priced coins, 1 tick can be a large % move.
        # Keep activation as close as possible; if Binance still rejects with -2021,
        # we will raise callbackRate (above) rather than pushing activation far away.
        if tick and tick > 0:
            if side == "BUY" and ap_fmt >= ref:
                ap_fmt = _fmt_px(ref - tick * (1.0 + float(attempt)))
            if side == "SELL" and ap_fmt <= ref:
                ap_fmt = _fmt_px(ref + tick * (1.0 + float(attempt)))
        else:
            pct = max((cb / 100.0) * 0.25, 0.001)
            if side == "BUY" and ap_fmt >= ref:
                ap_fmt = _fmt_px(ref * (1.0 - pct * (1.0 + float(attempt))))
            if side == "SELL" and ap_fmt <= ref:
                ap_fmt = _fmt_px(ref * (1.0 + pct * (1.0 + float(attempt))))

        if ap_fmt <= 0:
            return 0.0, None

        ap_str = _to_fixed_str(ap_fmt, pprec)
        return ap_fmt, ap_str

    # ---------------- Scan existing trailing orders ----------------
    try:
        orders = client.futures_get_open_orders(symbol=symbol) or []  # type: ignore[union-attr]
    except Exception:
        orders = []

    existing = []
    for o in orders:
        try:
            if str(o.get("type", "")).upper() != "TRAILING_STOP_MARKET":
                continue
            if str(o.get("side", "")).upper() != side:
                continue
            ro = o.get("reduceOnly")
            if ro is not None and str(ro).lower() not in ("true", "1"):
                continue
            if hedge and ps_param:
                op = str(o.get("positionSide") or "").upper().strip()
                if op and op != ps_param:
                    continue
            existing.append(o)
        except Exception:
            continue

    # Keep an existing one if it's close enough (and cancel duplicates)
    ap_fmt0, _ = _compute_activation(0)
    keep = None
    for o in existing:
        try:
            o_cb = float(o.get("callbackRate", 0) or 0.0)
        except Exception:
            o_cb = 0.0
        cb_ok = abs(o_cb - cb) <= 0.05

        ap_ok = True
        try:
            o_ap = float(o.get("activatePrice", 0) or o.get("activationPrice", 0) or 0.0)
        except Exception:
            o_ap = 0.0
        if o_ap > 0 and ap_fmt0 > 0:
            denom = max(1e-9, o_ap)
            ap_ok = abs(o_ap - ap_fmt0) / denom <= float(tolerance_pct)

        if cb_ok and ap_ok:
            keep = o
            break

    if keep is not None:
        # cancel duplicates
        for o in existing:
            if o is keep:
                continue
            try:
                oid = o.get("orderId")
                if oid is not None:
                    client.futures_cancel_order(symbol=symbol, orderId=oid)  # type: ignore[union-attr]
            except Exception:
                pass
        return True

    # Cancel old trailing orders (ours)
    for o in existing:
        try:
            oid = o.get("orderId")
            if oid is not None:
                client.futures_cancel_order(symbol=symbol, orderId=oid)  # type: ignore[union-attr]
        except Exception:
            pass

    # ---------------- Create new trailing order (with -2021 retry + callbackRate retry) ----------------
    last_err = None

    # Binance sometimes rejects callbackRate with "Invalid callBack rate" unless it matches allowed steps.
    # We start with cb (already quantized) and, if rejected, gently bump it up in 0.1% increments.
    def _cb_try_list(base: float) -> list[float]:
        out: list[float] = []
        try:
            b = float(base)
        except Exception:
            b = 0.3
        b = max(0.1, min(5.0, b))
        for i in range(6):  # base .. base+0.5
            v = min(5.0, b + 0.1 * i)
            v = math.ceil(v * 10 - 1e-9) / 10.0
            if v not in out:
                out.append(v)
        return out

    cb_candidates = _cb_try_list(cb)

    for attempt in range(4):
        ap_fmt, ap_str = _compute_activation(attempt)
        if not ap_str:
            last_err = f"bad activation (attempt {attempt})"
            continue

        for j, cb_try in enumerate(cb_candidates):
            cb_try_str = f"{cb_try:.1f}"

            params = dict(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=qty_str,
                callbackRate=cb_try_str,
                reduceOnly=True,
                workingType=wt,
                activationPrice=ap_str,
            )
            if price_protect:
                params["priceProtect"] = True

            try:
                client.futures_create_order(**params)
                if attempt > 0 or j > 0:
                    print(
                        f"[EXEC] ensure_trailing_stop {symbol}: ✅ accepted "
                        f"(attempt={attempt}, cb={cb_try_str}%, ref={_to_fixed_str(ref, pprec) or ref}, ap={ap_str})"
                    )
                return True

            except BinanceAPIException as e:
                msg = getattr(e, "message", None) or str(e)
                code = getattr(e, "code", None)
                last_err = msg
                low = str(msg).lower()

                # 1) Callback-rate rejection -> try next cb candidate
                if ("invalid" in low and "callback" in low) and j < (len(cb_candidates) - 1):
                    if j == 0:
                        print(
                            f"[EXEC] ensure_trailing_stop {symbol}: callbackRate rejected at {cb_try_str}% -> trying higher values"
                        )
                    continue

                # 2) -2021: activationPrice would immediately trigger -> widen activation gap (outer retry)
                if (code == -2021 or "immediately trigger" in low) and attempt < 2:
                    if attempt == 0:
                        print(
                            f"[EXEC] ensure_trailing_stop {symbol}: retrying (-2021) "
                            f"side={side} wt={wt} ref={_to_fixed_str(ref, pprec) or ref} ap={ap_str} "
                            f"(mark={_to_fixed_str(mark_fmt, pprec) or mark_fmt}, last={_to_fixed_str(last_fmt, pprec) or last_fmt}, tick={tick})"
                        )
                    elif attempt == 1:
                        # Some symbols behave better with CONTRACT_PRICE vs MARK_PRICE.
                        wt = "CONTRACT_PRICE" if wt == "MARK_PRICE" else "MARK_PRICE"
                    break  # go to next activation attempt

                print(
                    f"[EXEC] ensure_trailing_stop Binance error {symbol}: {msg} "
                    f"(side={side} wt={wt} ref={_to_fixed_str(ref, pprec) or ref} ap={ap_str} cb={cb_try_str}%)"
                )
                return False

            except Exception as e:
                last_err = str(e)
                print(f"[EXEC] ensure_trailing_stop failed {symbol}: {e}")
                return False

        # if we exhausted callback candidates, widen activation and retry (outer loop continues)

    print(f"[EXEC] ensure_trailing_stop failed {symbol}: {last_err}")
    return False
