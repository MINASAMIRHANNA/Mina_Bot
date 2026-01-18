import asyncio
import requests
import threading
import json
import psutil
import time  # <--- تأكد إن السطر ده موجود في main.py
from datetime import datetime, timezone
import os
import re


# --- Strict schema validation (dashboard ↔ bot) ---
try:
    from schema_models import ExecuteSignalPayload, SignalInboxItem
    _HAS_SCHEMA_MODELS = True
except Exception:
    ExecuteSignalPayload = None
    SignalInboxItem = None
    _HAS_SCHEMA_MODELS = False
from fetcher import fetch_futures_raw, fetch_spot_raw, fetch_klines
from indicators import ema, rsi, volume_spike, atr, adx
from scorer import technical_module_score, derivatives_module_score
from signal_engine import combine_scores, map_to_signal
from dispatcher import send_telegram_signal, send_error_alert, send_to_external_api
from model_manager import ModelManager
from config import cfg

# --- Optional external sentiment circuit breaker ---
try:
    from external_data import check_news_circuit_breaker
except Exception:
    check_news_circuit_breaker = None

from feature_extractor import extract_features

# Paper Tournament (optional)
try:
    from paper_tournament import generate_paper_decisions, PaperEvaluator, run_daily_paper_tournament
except Exception:
    generate_paper_decisions = None
    PaperEvaluator = None
    run_daily_paper_tournament = None

from realtime_futures_ws import start_futures_sockets
from realtime_ws import start_kline_socket
from data_manager import DataManager
from database import DatabaseManager
from pump_detector import detect_pre_pump
from trading_executor import place_market_order, close_open_position, move_stop_loss, place_hard_tp, set_leverage
from risk_monitor import RiskSupervisor

# ==========================================================
# SETUP
# ==========================================================
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

db = DatabaseManager()
# Sprint 9: unify run mode to avoid conflicts (TEST/LIVE/PAPER)
try:
    _mode_db = db.get_setting("mode") or getattr(cfg, "RUN_MODE", "TEST")
except Exception:
    _mode_db = getattr(cfg, "RUN_MODE", "TEST")
try:
    _mode_final = cfg.apply_run_mode(str(_mode_db), source="db")
except Exception:
    _mode_final = str(_mode_db or "TEST").strip().upper()
try:
    db.sync_legacy_mode_keys(_mode_final, source="main.startup")
except Exception:
    pass

MODEL = ModelManager("./models")
dm = DataManager()

def _mode_allows_orders(mode: str) -> bool:
    m = str(mode or '').strip().upper()
    return m in ('LIVE','TEST')

analysis_semaphore = asyncio.Semaphore(cfg.MAX_CONCURRENT_TASKS)
TF_LIST = None  # will be set from env below
PROCESSING_COINS = set()

# ==========================================================
# SPRINT 5: IDEMPOTENCY + DEDUPE KEYS
# ==========================================================

_INTERVAL_MS_MAP = {
    '1m': 60_000,
    '3m': 180_000,
    '5m': 300_000,
    '15m': 900_000,
    '30m': 1_800_000,
    '1h': 3_600_000,
    '2h': 7_200_000,
    '4h': 14_400_000,
    '6h': 21_600_000,
    '8h': 28_800_000,
    '12h': 43_200_000,
    '1d': 86_400_000,
}

def _interval_to_ms(tf: str) -> int:
    tf = str(tf or '').strip()
    if tf in _INTERVAL_MS_MAP:
        return _INTERVAL_MS_MAP[tf]
    # fallback: try number+unit like 10m
    try:
        n = int(tf[:-1])
        unit = tf[-1]
        if unit == 'm':
            return n * 60_000
        if unit == 'h':
            return n * 3_600_000
        if unit == 'd':
            return n * 86_400_000
    except Exception:
        pass
    return 0

def _candle_close_ms_from_kline(kline, df, tf: str) -> int:
    # Prefer WS kline closeTime (Binance: 'T' close time ms, 't' open time ms)
    try:
        if isinstance(kline, dict):
            for key in ('T', 'closeTime', 'ct'):
                v = kline.get(key)
                if v is not None:
                    vv = int(v)
                    if vv > 0:
                        return vv
            # Some payloads: open time 't' then add interval
            v = kline.get('t')
            if v is not None:
                ot = int(v)
                step = _interval_to_ms(tf)
                if ot > 0 and step > 0:
                    return ot + step
    except Exception:
        pass

    # Fallback: use last df index (open_time) + interval
    try:
        if df is not None and hasattr(df, 'index') and len(df.index) > 0:
            ot = int(df.index[-1].value // 1_000_000)
            step = _interval_to_ms(tf)
            if ot > 0 and step > 0:
                return ot + step
            return ot
    except Exception:
        pass

    # last resort: now
    try:
        return int(time.time() * 1000)
    except Exception:
        return 0

def _make_signal_key(symbol: str, market_type: str, tf: str, strategy_tag: str, side_txt: str, candle_close_ms: int) -> str:
    sym = str(symbol or '').upper().strip()
    mt = str(market_type or '').lower().strip()
    tfv = str(tf or '').strip()
    st = str(strategy_tag or '').strip()
    sd = str(side_txt or '').upper().strip()
    cc = int(candle_close_ms or 0)
    return f"{sym}|{mt}|{tfv}|{st}|{sd}|{cc}"


# --- BTC Market Regime cache (for market context & features) ---
BTC_CACHE = {
    "1h": {"df": None, "ts": 0.0},
    "4h": {"df": None, "ts": 0.0},
}


_LAST_RESTART_BOT_REQ = None  # token from dashboard to request restart

# --- Dashboard approval throttle (avoid spam) ---
_LAST_PENDING_SENT = {}  # key -> last unix ts
_PENDING_COOLDOWN_SEC = float(getattr(cfg, "PENDING_COOLDOWN_SEC", 60) or 60)

# --- Decision Trace (Explainability) ---
_LAST_DECISION_TRACE_TS = {}  # key -> last unix ts
_LAST_TRACE_PRUNE_TS = 0.0


def _trace_enabled() -> bool:
    try:
        return str(db.get_setting("decision_trace_enabled") or "TRUE").strip().upper() in ("1","TRUE","YES","ON","Y")
    except Exception:
        return True


def _trace_every_sec() -> int:
    try:
        v = int(float(db.get_setting("decision_trace_every_sec") or 60))
        return max(1, min(v, 600))
    except Exception:
        return 60


def _maybe_add_decision_trace(trace: dict, throttle_key: str = "") -> None:
    """Best-effort: store a decision trace row for Doctor / explainability."""
    global _LAST_TRACE_PRUNE_TS
    try:
        if not _trace_enabled():
            return
        key = throttle_key or f"{trace.get('symbol','')}|{trace.get('interval','')}|{trace.get('market_type','')}"
        now = time.time()
        every = _trace_every_sec()
        last = float(_LAST_DECISION_TRACE_TS.get(key) or 0.0)
        if (now - last) < every:
            return
        _LAST_DECISION_TRACE_TS[key] = now

        if hasattr(db, "add_decision_trace"):
            db.add_decision_trace(trace)

        # prune occasionally
        if (now - float(_LAST_TRACE_PRUNE_TS or 0.0)) > 3600:
            _LAST_TRACE_PRUNE_TS = now
            if hasattr(db, "prune_decision_traces"):
                db.prune_decision_traces(int(float(db.get_setting("decision_trace_max_rows") or 20000)))
    except Exception:
        return


# --- Perf metrics (Latency / Performance) ---
_LAST_PERF_TS = {}  # key -> last unix ts
_LAST_PERF_PRUNE_TS = 0.0


def _perf_enabled() -> bool:
    try:
        return str(db.get_setting("perf_enabled") or "TRUE").strip().upper() in ("1", "TRUE", "YES", "ON", "Y")
    except Exception:
        return True


def _perf_every_sec() -> int:
    try:
        v = int(float(db.get_setting("perf_every_sec") or 30))
        return max(1, min(v, 600))
    except Exception:
        return 30


def _maybe_add_perf_metric(event: dict, throttle_key: str = "") -> None:
    """Best-effort: store a perf metric row for Doctor (latency per symbol)."""
    global _LAST_PERF_PRUNE_TS
    try:
        if not _perf_enabled():
            return
        key = throttle_key or f"{event.get('symbol','')}|{event.get('interval','')}|{event.get('market_type','')}"
        now = time.time()
        every = _perf_every_sec()
        last = float(_LAST_PERF_TS.get(key) or 0.0)
        if (now - last) < every:
            return
        _LAST_PERF_TS[key] = now

        if hasattr(db, "add_perf_metric"):
            db.add_perf_metric(event)

        # prune occasionally
        if (now - float(_LAST_PERF_PRUNE_TS or 0.0)) > 3600:
            _LAST_PERF_PRUNE_TS = now
            if hasattr(db, "prune_perf_metrics"):
                db.prune_perf_metrics(int(float(db.get_setting("perf_max_rows") or 200000)))
    except Exception:
        return

def _normalize_order_side(raw):
    """Return BUY/SELL or None from various payload forms."""
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None
    if s in {"BUY", "LONG", "BULL", "1", "+1"}:
        return "BUY"
    if s in {"SELL", "SHORT", "BEAR", "-1"}:
        return "SELL"
    # sometimes embedded in longer strings
    if "LONG" in s or "BUY" in s:
        return "BUY"
    if "SHORT" in s or "SELL" in s:
        return "SELL"
    if "WAIT" in s or "HOLD" in s:
        return None
    return None

def _side_text(order_side: str) -> str:
    return "LONG" if str(order_side).upper() == "BUY" else "SHORT"


# ==========================================================
# PUMP DYNAMIC EXIT (Pump Hunter)
# ==========================================================
# In-memory state (per trade) to track peak/trough for trailing exits.
# Safe: if bot restarts, state resets to entry price and will rebuild.
PUMP_EXIT_STATE = {}

def _setting_bool(key: str, default: bool=False) -> bool:
    try:
        v = db.get_setting(key)
        if v is None:
            return default
        return str(v).strip().upper() in ("1", "TRUE", "YES", "Y", "ON")
    except Exception:
        return default

def _setting_float(key: str, default: float) -> float:
    try:
        v = db.get_setting(key)
        if v is None or str(v).strip() == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _truthy(v) -> bool:
    try:
        s = str(v).strip().lower()
    except Exception:
        return False
    return s in ("1", "true", "yes", "y", "on")


def _get_btc_df(tf: str, ttl_seconds: int = None):
    """Fetch BTCUSDT klines for a given timeframe with a small TTL cache."""
    try:
        tf = str(tf or "").strip()
    except Exception:
        tf = "1h"
    if tf not in BTC_CACHE:
        tf = "1h"

    now = time.time()
    cached = BTC_CACHE.get(tf) or {"df": None, "ts": 0.0}

    # Default TTLs: BTC 1h updates slowly; 4h even slower.
    if ttl_seconds is None:
        ttl_seconds = 300 if tf == "1h" else 900

    try:
        if cached.get("df") is not None and (now - float(cached.get("ts") or 0.0)) < float(ttl_seconds):
            return cached.get("df")
    except Exception:
        pass

    # Refresh
    try:
        btc_df = fetch_klines("BTCUSDT", tf, 500, "futures")
        if btc_df is not None and not btc_df.empty:
            BTC_CACHE[tf] = {"df": btc_df, "ts": now}
            return btc_df
    except Exception:
        pass

    return cached.get("df")


def _compute_btc_regime(tf: str):
    """Return (label, trend_strength, adx_value) based on BTC EMA20/EMA50 and ADX."""
    btc_df = _get_btc_df(tf)
    if btc_df is None or getattr(btc_df, "empty", True):
        return "UNKNOWN", 0.0, 0.0

    price = 0.0
    try:
        price = float(btc_df["close"].iloc[-1])
    except Exception:
        price = 0.0

    ema20 = _safe_value(ema(btc_df, 20), -1, 0.0)
    ema50 = _safe_value(ema(btc_df, 50), -1, 0.0)
    adx_v = _safe_value(adx(btc_df, 14), -1, 0.0)

    trend = (ema20 - ema50) / price if price > 0 else 0.0

    # Thresholds (DB settings or defaults)
    try:
        bull_th = float(db.get_setting("market_regime_bull_th") or 0.003)
    except Exception:
        bull_th = 0.003
    try:
        bear_th = float(db.get_setting("market_regime_bear_th") or 0.003)
    except Exception:
        bear_th = 0.003
    try:
        adx_strong = float(db.get_setting("market_regime_adx_strong") or 20)
    except Exception:
        adx_strong = 20.0

    if trend > bull_th:
        return ("BULL_STRONG" if adx_v >= adx_strong else "BULL"), float(trend), float(adx_v)
    if trend < -bear_th:
        return ("BEAR_STRONG" if adx_v >= adx_strong else "BEAR"), float(trend), float(adx_v)
    return "SIDEWAYS", float(trend), float(adx_v)


def _market_regime_for_strategy(algo: str):
    """For scalp use BTC 1H, for swing use BTC 4H."""
    a = (algo or "").lower()
    tf = "1h" if "scalp" in a else "4h"
    label, trend, adx_v = _compute_btc_regime(tf)
    return {"tf": tf, "label": label, "trend": float(trend), "adx": float(adx_v)}


def _pump_state_get(trade_id: int, entry: float, price: float, is_long: bool) -> dict:
    st = PUMP_EXIT_STATE.get(trade_id)
    if not st:
        st = {"peak": float(entry), "trough": float(entry), "last_ts": time.time()}
    if is_long:
        if price > st["peak"]:
            st["peak"] = float(price)
    else:
        if price < st["trough"]:
            st["trough"] = float(price)
    st["last_ts"] = time.time()
    PUMP_EXIT_STATE[trade_id] = st
    return st

def _pump_state_drop(trade_key):
    try:
        PUMP_EXIT_STATE.pop(trade_key, None)
    except Exception:
        pass


def _manage_pump_dynamic_exit(symbol: str, active_trade: dict, df, price: float) -> bool:
    """Dynamic exit manager for pump-style trades."""
    try:
        mode_now = str(db.get_setting("mode") or "TEST").upper()
        entry = float(active_trade.get("entry_price") or 0.0)
        if entry <= 0:
            return False
        trade_id = int(active_trade.get("id") or 0)
        signal = str(active_trade.get("signal") or "")
        current_sl = float(active_trade.get("stop_loss") or 0.0)
        ts_ms = int(active_trade.get("timestamp_ms") or 0)

        is_long = ("LONG" in signal) or ("BUY" in signal and "SELL" not in signal)
        is_short = ("SHORT" in signal) or ("SELL" in signal and "BUY" not in signal)
        if not (is_long or is_short):
            return False

        roi = (price - entry) / entry if is_long else (entry - price) / entry

        # 0) Stop-loss hit (for PAPER/TEST we must simulate it)
        if current_sl and current_sl > 0:
            if is_long and price <= current_sl:
                cp = price
                if _mode_allows_orders(mode_now):
                    res = close_open_position(symbol)
                    if res.get("ok"):
                        cp = float(res.get("result", {}).get("avgPrice") or price)
                db.close_trade(trade_id or symbol, "STOP_LOSS", cp, explain={"pump_exit": {"type": "sl_hit", "roi": roi}})
                log(f"🛑 SL HIT {symbol} @ {cp:.6f} (ROI {roi*100:.2f}%)", "TRADE")
                return True
            if is_short and price >= current_sl:
                cp = price
                if _mode_allows_orders(mode_now):
                    res = close_open_position(symbol)
                    if res.get("ok"):
                        cp = float(res.get("result", {}).get("avgPrice") or price)
                db.close_trade(trade_id or symbol, "STOP_LOSS", cp, explain={"pump_exit": {"type": "sl_hit", "roi": roi}})
                log(f"🛑 SL HIT {symbol} @ {cp:.6f} (ROI {roi*100:.2f}%)", "TRADE")
                return True

        # Settings
        be_trigger = _setting_float("pump_exit_be_trigger_roi", 0.004)
        be_buffer  = _setting_float("pump_exit_be_buffer_pct", 0.0004)
        trail_trigger = _setting_float("pump_exit_trail_trigger_roi", 0.007)
        trail_pct = _setting_float("pump_exit_trail_pct", 0.007)
        use_atr = _setting_bool("pump_exit_use_atr", True)
        atr_mult = _setting_float("pump_exit_trail_atr_mult", 1.8)
        retrace_pct = _setting_float("pump_exit_retrace_pct", 0.012)
        min_profit_exhaust = _setting_float("pump_exit_min_profit_for_exhaust", 0.006)
        max_minutes = _setting_float("pump_exit_max_minutes", 35.0)
        min_gap = _setting_float("pump_exit_min_gap_pct", 0.001)

        st = _pump_state_get(trade_id or hash(symbol), entry, price, is_long=is_long)
        peak = float(st["peak"])
        trough = float(st["trough"])

        # 1) Exhaustion close
        if roi >= min_profit_exhaust:
            if is_long and peak > 0:
                retrace = (peak - price) / peak
                if retrace >= retrace_pct:
                    cp = price
                    if _mode_allows_orders(mode_now):
                        res = close_open_position(symbol)
                        if res.get("ok"):
                            cp = float(res.get("result", {}).get("avgPrice") or price)
                    db.close_trade(trade_id or symbol, "PUMP_EXHAUST", cp, explain={"pump_exit": {"type": "retrace", "roi": roi, "retrace": retrace, "peak": peak}})
                    log(f"💥 PUMP EXHAUST {symbol}: retrace {retrace*100:.2f}% -> CLOSE @ {cp:.6f}", "TRADE")
                    return True
            if is_short and trough > 0:
                retrace = (price - trough) / trough
                if retrace >= retrace_pct:
                    cp = price
                    if _mode_allows_orders(mode_now):
                        res = close_open_position(symbol)
                        if res.get("ok"):
                            cp = float(res.get("result", {}).get("avgPrice") or price)
                    db.close_trade(trade_id or symbol, "PUMP_EXHAUST", cp, explain={"pump_exit": {"type": "retrace", "roi": roi, "retrace": retrace, "trough": trough}})
                    log(f"💥 PUMP EXHAUST {symbol}: retrace {retrace*100:.2f}% -> CLOSE @ {cp:.6f}", "TRADE")
                    return True

        # 2) Time-based exit
        if ts_ms and max_minutes > 0:
            age_min = (int(time.time()*1000) - ts_ms) / 60000.0
            if age_min >= max_minutes and roi > 0:
                cp = price
                if _mode_allows_orders(mode_now):
                    res = close_open_position(symbol)
                    if res.get("ok"):
                        cp = float(res.get("result", {}).get("avgPrice") or price)
                db.close_trade(trade_id or symbol, "PUMP_TIMEOUT", cp, explain={"pump_exit": {"type": "timeout", "roi": roi, "age_min": age_min}})
                log(f"⏱️ PUMP TIMEOUT {symbol}: age {age_min:.1f}m -> CLOSE @ {cp:.6f}", "TRADE")
                return True

        # 3) Breakeven + Trailing SL
        new_sl = None

        if roi >= be_trigger:
            if is_long:
                be_sl = entry * (1.0 + be_buffer)
                new_sl = be_sl if new_sl is None else max(new_sl, be_sl)
            else:
                be_sl = entry * (1.0 - be_buffer)
                new_sl = be_sl if new_sl is None else min(new_sl, be_sl)

        if roi >= trail_trigger:
            atr_val = 0.0
            try:
                atr_val = float(_safe_value(atr(df, 14), 0.0))
            except Exception:
                atr_val = 0.0

            if use_atr and atr_val > 0:
                if is_long:
                    t_sl = peak - (atr_mult * atr_val)
                else:
                    t_sl = trough + (atr_mult * atr_val)
            else:
                if is_long:
                    t_sl = peak * (1.0 - max(0.001, trail_pct))
                else:
                    t_sl = trough * (1.0 + max(0.001, trail_pct))

            if is_long:
                new_sl = t_sl if new_sl is None else max(new_sl, t_sl)
            else:
                new_sl = t_sl if new_sl is None else min(new_sl, t_sl)

        if new_sl is not None and new_sl > 0:
            if is_long:
                candidate = new_sl if current_sl <= 0 else max(current_sl, new_sl)
                candidate = min(candidate, price * (1.0 - min_gap))
            else:
                candidate = new_sl if current_sl <= 0 else min(current_sl, new_sl)
                candidate = max(candidate, price * (1.0 + min_gap))

            if current_sl <= 0 or abs(candidate - current_sl) / max(1e-9, current_sl) >= 0.001:
                if _mode_allows_orders(mode_now):
                    try:
                        move_stop_loss(symbol, float(candidate))
                    except Exception:
                        pass
                db.update_stop_loss(symbol, float(candidate))
                log(f"🧲 Pump Exit SL Update {symbol}: SL {current_sl:.6f} -> {candidate:.6f} | ROI {roi*100:.2f}%", "TRADE")
                return True

        return False

    except Exception:
        return False


# ==========================================================
# HELPERS

# ====================
# RUNTIME CONFIG (Zero-ENV / DB-backed via cfg)
# ====================

def _parse_csv_list(raw) -> list:
    """Parse '1m,5m 1h' style strings into a clean list."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw).strip()
    if not s:
        return []
    parts = re.split(r'[\,\s]+', s)
    return [p.strip() for p in parts if p.strip()]

def _limit_symbols(symbols, limit: int):
    try:
        lim = int(limit)
    except Exception:
        return symbols
    if lim <= 0:
        return symbols
    return list(symbols)[:lim]

def _load_runtime_knobs_from_cfg() -> None:
    """Load runtime knobs from cfg into module globals.

    Note: WebSocket subscriptions are created at startup. Changing timeframes/limits at runtime
    will not automatically resubscribe unless you restart the bot.
    We still reload these globals so command handlers & other logic use latest values.
    """
    global TF_LIST, SPOT_WS_TF, FUTURES_WS_ENABLED, SPOT_WS_ENABLED
    global FUTURES_SYMBOL_LIMIT, SPOT_SYMBOL_LIMIT, RELOAD_MODELS_COOLDOWN_SEC

    TF_LIST = _parse_csv_list(getattr(cfg, 'FUTURES_WS_TIMEFRAMES', '1m,5m,1h'))
    SPOT_WS_TF = str(getattr(cfg, 'SPOT_WS_TIMEFRAME', '1m'))
    FUTURES_WS_ENABLED = bool(getattr(cfg, 'FUTURES_WS_ENABLED', True))
    SPOT_WS_ENABLED = bool(getattr(cfg, 'SPOT_WS_ENABLED', True))

    try:
        FUTURES_SYMBOL_LIMIT = int(getattr(cfg, 'FUTURES_SYMBOL_LIMIT', 0) or 0)
    except Exception:
        FUTURES_SYMBOL_LIMIT = 0
    try:
        SPOT_SYMBOL_LIMIT = int(getattr(cfg, 'SPOT_SYMBOL_LIMIT', 0) or 0)
    except Exception:
        SPOT_SYMBOL_LIMIT = 0
    try:
        RELOAD_MODELS_COOLDOWN_SEC = int(getattr(cfg, 'RELOAD_MODELS_COOLDOWN_SEC', 60) or 60)
    except Exception:
        RELOAD_MODELS_COOLDOWN_SEC = 60


# --- WebSocket runtime knobs (DB-backed) ---
TF_LIST = []
SPOT_WS_TF = '1m'
FUTURES_WS_ENABLED = True
SPOT_WS_ENABLED = True
FUTURES_SYMBOL_LIMIT = 0
SPOT_SYMBOL_LIMIT = 0
RELOAD_MODELS_COOLDOWN_SEC = 60

_load_runtime_knobs_from_cfg()

# Cooldown state for model reload (avoids NameError in command handler)
_last_reload_ts: float = 0.0


# ==========================================================
def log(msg, level="INFO"):
    db.log(msg, level)
    print(f"[{level}] {msg}")
    if level in ("ERROR", "CRITICAL"):
        send_error_alert(cfg.TELEGRAM_CHAT_ID, msg)


def _safe_value(x, idx: int = -1, default: float = 0.0) -> float:
    """Safely read a numeric value from a pandas Series/DataFrame slice or a scalar."""
    try:
        # pandas Series / DataFrame
        if hasattr(x, "iloc"):
            v = x.iloc[idx]
            # if DataFrame row/col result is a Series
            if hasattr(v, "iloc") and not isinstance(v, (int, float)):
                try:
                    v = v.iloc[-1]
                except Exception:
                    pass
            return float(v)
        return float(x)
    except Exception:
        return float(default)


def send_to_dashboard(payload):
    """Best-effort publish to dashboard (non-blocking)."""
    try:
        import threading
        base = getattr(cfg, "DASHBOARD_URL", None) or "http://localhost:8000"
        url = base.rstrip("/") + "/publish"
        token = getattr(cfg, "DASHBOARD_PUBLISH_TOKEN", None) or ""
        headers = {"x-dashboard-token": token} if token else None
        def _post():
            try:
                requests.post(url, json=payload, headers=headers, timeout=2.0)
            except Exception:
                pass
        threading.Thread(target=_post, daemon=True).start()
    except Exception:
        pass

def calculate_quantity(usd, price):
    if price <= 0: return 0.0
    return round(usd / price, 4)

# ==========================================================
# LOGIC GATES
# ==========================================================
def market_gate(df, price, symbol):
    # Optional external circuit breaker (e.g., extreme fear / crash proxy)
    try:
        if check_news_circuit_breaker and _truthy(db.get_setting("external_circuit_breaker_enabled") or "FALSE"):
            halt, reason = check_news_circuit_breaker()
            if halt:
                db.log_rejection(symbol, f"CIRCUIT_BREAKER:{reason}", 1.0, 0.0)
                return "BLOCK"
    except Exception:
        pass

    atr_len = int(db.get_setting("ind_atr_len") or 14)
    adx_len = int(db.get_setting("ind_adx_len") or 14)

    atr_v = _safe_value(atr(df, atr_len), -1)
    adx_v = _safe_value(adx(df, adx_len), -1)
    vol = _safe_value(volume_spike(df), -1)
    atr_pct = atr_v / price if price > 0 else 0
    # جلب الإعدادات من الداشبورد
    min_atr = float(db.get_setting("gate_atr_min_pct") or 0.003)
    min_adx = float(db.get_setting("gate_adx_min") or 10)
    
    if atr_pct < min_atr:
        db.log_rejection(symbol, "LOW_VOLATILITY (ATR)", atr_pct, min_atr)
        return "BLOCK"
    if adx_v < min_adx and not vol:
        db.log_rejection(symbol, "WEAK_TREND (ADX)", adx_v, min_adx)
        return "WAIT"
    return "TRADE"

def select_strategy(df, price, gate):
    if gate != "TRADE": return "NO_TRADE"
    atr_len = int(db.get_setting("ind_atr_len") or 14)
    adx_len = int(db.get_setting("ind_adx_len") or 14)
    ma_fast_len = int(db.get_setting("ind_ma_fast") or 50)

    atr_v = _safe_value(atr(df, atr_len), -1)
    adx_v = _safe_value(adx(df, adx_len), -1)
    vol = _safe_value(volume_spike(df), -1)
    atr_pct = atr_v / price if price > 0 else 0
    
    ema_now = _safe_value(ema(df, ma_fast_len), -1)
    ema_prev = _safe_value(ema(df, ma_fast_len), -5)
    slope = (ema_now - ema_prev) / ema_prev if ema_prev else 0

    if atr_pct >= 0.006 and vol: return "SCALP"
    if adx_v >= 18 and abs(slope) >= 0.002: return "SWING"
    return "NO_TRADE"

def select_exit_profile(df, price, mode):
    atr_len = int(db.get_setting("ind_atr_len") or 14)
    adx_len = int(db.get_setting("ind_adx_len") or 14)

    atr_v = _safe_value(atr(df, atr_len), -1)
    adx_v = _safe_value(adx(df, adx_len), -1)
    vol = _safe_value(volume_spike(df), -1)
    atr_pct = atr_v / price if price > 0 else 0

    if mode == "scalp":
        return "SCALP_PUMP" if vol and atr_pct > 0.01 else "SCALP_FAST"
    return "SWING_TREND" if adx_v >= 25 else "SWING_REVERSAL"

def apply_exit_profile(entry, atr_v, profile, signal):
    is_long = "LONG" in signal
    sl_scalp = float(db.get_setting("sl_scalp_mult") or 0.8)
    tp_scalp = float(db.get_setting("tp_scalp_mult") or 1.2)
    sl_swing = float(db.get_setting("sl_swing_mult") or 1.5)
    tp_swing = float(db.get_setting("tp_swing_mult") or 2.0)

    def sl(x): return entry - x if is_long else entry + x
    def tp(x): return entry + x if is_long else entry - x

    if profile == "SCALP_FAST" or profile == "SCALP_PUMP":
        return sl(atr_v * sl_scalp), [tp(atr_v * tp_scalp)]

    if profile == "SWING_TREND" or profile == "SWING_REVERSAL":
        return sl(atr_v * sl_swing), [tp(atr_v * tp_swing), tp(atr_v * (tp_swing * 2))]
    return sl(atr_v), [tp(atr_v * 1.5)]

def hybrid_signal_decision(final_score, rule_signal, ai_vote, ai_confidence, mode, gate, exit_profile):
    """Hybrid trade decision.

    IMPORTANT: Uses *AI confidence* (0..1 or 0..100) as the primary driver.
    DB thresholds are stored as percent (e.g., 55/65).

    Returns: ("TRADE"|"NO_TRADE", confidence_pct)
    """
    # 1) Normalize AI confidence
    conf = 0.0
    try:
        conf = float(ai_confidence) if ai_confidence is not None else 0.0
    except Exception:
        conf = 0.0

    # Accept both 0..1 and 0..100 formats
    conf_pct = conf * 100.0 if 0.0 <= conf <= 1.0 else conf

    # 2) If AI is OFF/WAIT, treat confidence as low
    if ai_vote not in ("LONG", "SHORT"):
        conf_pct = max(0.0, conf_pct - 25.0)

    # 3) Small hybrid tweak: agreement with rule signal
    # NOTE: rule_signal can be BUY/SELL (or LONG/SHORT). Normalize both sides.
    try:
        rs = _normalize_order_side(rule_signal)  # BUY/SELL/None
        av = str(ai_vote or "").upper().strip()  # LONG/SHORT/WAIT/OFF
        if av in ("LONG", "SHORT") and rs in ("BUY", "SELL"):
            agree = (rs == "BUY" and av == "LONG") or (rs == "SELL" and av == "SHORT")
            conf_pct += 5.0 if agree else -7.5
    except Exception:
        pass

    # 4) Thresholds (percent) from DB
    min_conf_scalp = float(db.get_setting("min_conf_scalp") or 55)
    min_conf_swing = float(db.get_setting("min_conf_swing") or 65)
    min_conf = min_conf_scalp if mode == "scalp" else min_conf_swing

    # If market gate says WAIT, require slightly higher confidence
    if gate == "WAIT":
        min_conf += 5.0

    # Dynamic Threshold from DB (Learning/Calibration)
    min_conf += db.get_threshold_adjustment(mode, exit_profile)

    # Optional safety bump when signal strength is weak (score is near 0 in [-1..+1])
    try:
        fs = float(final_score)
        if abs(fs) < 0.15:
            min_conf += 5.0
    except Exception:
        pass

    conf_pct = max(0.0, min(100.0, float(conf_pct)))
    return ("TRADE", conf_pct) if conf_pct >= min_conf else ("NO_TRADE", conf_pct)



def hybrid_signal_decision_trace(final_score, rule_signal, ai_vote, ai_confidence, mode, gate, exit_profile, regime=None):
    """Same as hybrid_signal_decision but returns a detailed trace dict."""
    # Normalize AI confidence
    raw_conf = 0.0
    try:
        raw_conf = float(ai_confidence) if ai_confidence is not None else 0.0
    except Exception:
        raw_conf = 0.0
    conf_pct = raw_conf * 100.0 if 0.0 <= raw_conf <= 1.0 else raw_conf

    adjustments = []

    # --- Optional DB toggles for BTC regime conflict handling ---
    # regime_filter: OFF/0/FALSE/NO/DISABLED to disable regime handling
    # regime_conflict_policy: PENALIZE (default) | STRICT (legacy WAIT/BLOCK)
    regime_filter_enabled = True
    regime_policy = "PENALIZE"
    try:
        rf = str(db.get_setting("regime_filter") or "ON").strip().upper()
        if rf in ("OFF", "0", "FALSE", "NO", "DISABLED"):
            regime_filter_enabled = False
    except Exception:
        pass
    try:
        regime_policy = str(db.get_setting("regime_conflict_policy") or "PENALIZE").strip().upper()
    except Exception:
        regime_policy = "PENALIZE"
    regime_min_conf_bump = 0.0

    # AI OFF/WAIT penalty
    if str(ai_vote or '').upper() not in ("LONG", "SHORT"):
        conf_pct = max(0.0, conf_pct - 25.0)
        adjustments.append({"name": "ai_off_penalty", "delta": -25.0})

    # agreement tweak (normalize BUY/LONG and SELL/SHORT)
    def _norm_dir(x):
        xs = str(x or "").upper()
        if xs in ("BUY", "LONG"):
            return "LONG"
        if xs in ("SELL", "SHORT"):
            return "SHORT"
        return ""

    rs_dir = _norm_dir(rule_signal)
    av_dir = _norm_dir(ai_vote)
    if rs_dir and av_dir:
        if rs_dir == av_dir:
            conf_pct += 5.0
            adjustments.append({"name": "agree_bonus", "delta": 5.0})
        else:
            conf_pct -= 7.5
            adjustments.append({"name": "disagree_penalty", "delta": -7.5})

    # Market regime handling (optional): default is PENALIZE (raise required confidence)
    # instead of hard-blocking all counter-regime trades.
    if regime_filter_enabled:
        try:
            intended_dir = (av_dir or rs_dir)
            if intended_dir and regime and isinstance(regime, dict):
                lbl = str(regime.get("label") or "").upper()
                if lbl:
                    conflict = (
                        (intended_dir == "LONG" and lbl.startswith("BEAR")) or
                        (intended_dir == "SHORT" and lbl.startswith("BULL"))
                    )
                    if conflict:
                        is_strong = "STRONG" in lbl
                        m = str(mode or "").lower()

                        # Legacy behavior (STRICT): scalp -> WAIT, swing -> BLOCK when STRONG.
                        if regime_policy in ("STRICT", "HARD", "WAIT", "BLOCK"):
                            action = "WAIT"
                            if m != "scalp" and is_strong:
                                action = "BLOCK"
                            adjustments.append({
                                "name": f"market_regime_{action.lower()}",
                                "delta": 0.0,
                                "label": lbl,
                                "tf": str(regime.get("tf") or ""),
                                "trend": float(regime.get("trend") or 0.0),
                                "adx": float(regime.get("adx") or 0.0),
                                "policy": regime_policy,
                            })
                            decision = action
                            trace = {
                                "final_score": float(final_score or 0.0),
                                "confidence_pct": float(conf_pct or 0.0),
                                "min_conf": None,
                                "gate": gate,
                                "mode": mode,
                                "exit_profile": exit_profile,
                                "rule_signal": rule_signal,
                                "ai_vote": ai_vote,
                                "adjustments": adjustments,
                                "regime": regime,
                                "decision": decision,
                            }
                            return decision, conf_pct, trace

                        # Default (PENALIZE): require higher confidence, but still allow a trade.
                        regime_min_conf_bump = 7.5 if m == "scalp" else (15.0 if is_strong else 10.0)
                        adjustments.append({
                            "name": "market_regime_penalty",
                            "delta": float(regime_min_conf_bump),
                            "label": lbl,
                            "tf": str(regime.get("tf") or ""),
                            "trend": float(regime.get("trend") or 0.0),
                            "adx": float(regime.get("adx") or 0.0),
                            "policy": regime_policy,
                        })
        except Exception:
            pass

    # thresholds
    min_conf_scalp = float(db.get_setting("min_conf_scalp") or 55)
    min_conf_swing = float(db.get_setting("min_conf_swing") or 65)
    min_conf = min_conf_scalp if mode == "scalp" else min_conf_swing

    if regime_min_conf_bump:
        min_conf += float(regime_min_conf_bump)

    if gate == "WAIT":
        min_conf += 5.0
        adjustments.append({"name": "gate_wait_bump", "delta": 5.0})

    # learning/calibration adjustment
    try:
        adj = float(db.get_threshold_adjustment(mode, exit_profile) or 0.0)
    except Exception:
        adj = 0.0
    if abs(adj) > 1e-9:
        min_conf += adj
        adjustments.append({"name": "learning_adjustment", "delta": adj})

    # extra safety bump when score is weak/near-zero
    try:
        if abs(float(final_score or 0.0)) < 0.15:
            min_conf += 5.0
            adjustments.append({"name": "low_score_bump", "delta": 5.0})
    except Exception:
        pass

    conf_pct = max(0.0, min(100.0, float(conf_pct)))
    decision = "TRADE" if conf_pct >= min_conf else "NO_TRADE"

    trace = {
        "raw_ai_conf": raw_conf,
        "conf_pct": conf_pct,
        "min_conf": min_conf,
        "gate": gate,
        "mode": mode,
        "exit_profile": exit_profile,
        "rule_signal": rule_signal,
        "ai_vote": ai_vote,
        "adjustments": adjustments,
        "decision": decision,
    }
    return decision, conf_pct, trace

# ==========================================================
# CORE ANALYSIS ENGINE

# ==========================================================
def analyze_coin(symbol, tf, market_type="futures", kline=None):
    if symbol in PROCESSING_COINS: return
    PROCESSING_COINS.add(symbol)

    # Sprint 6: performance / latency monitoring (no strategy behavior change)
    t0_perf = time.perf_counter()
    t_fetch = None
    t_scores = None
    t_model = None
    perf_phase = "start"
    perf_err = None

    try:
        # 1. Fetch Data
        df = fetch_klines(symbol, tf, 500, market_type)
        if df is None or df.empty: return
        price = float(df["close"].iloc[-1])

        t_fetch = time.perf_counter()
        perf_phase = "fetched"

        # Sprint 5: stable candle close timestamp (used for idempotency/dedupe across restarts)
        candle_close_ms = _candle_close_ms_from_kline(kline, df, tf)

        # ----------------------------------------------------
                # ----------------------------------------------------
        # PHASE 1: MANAGE ACTIVE TRADES (DYNAMIC EXIT)
        # ----------------------------------------------------
        active_trade = db.get_active_trade(symbol)
        if active_trade:
            entry = float(active_trade.get('entry_price') or 0.0)
            current_sl = float(active_trade.get('stop_loss') or 0.0)
            signal = str(active_trade.get('signal') or "")
            exit_profile = str(active_trade.get('exit_profile') or "")
            strategy_tag = str(active_trade.get('strategy_tag') or "")
            mode_now = str(db.get_setting("mode") or "TEST").upper()

            is_pump = ("PUMP" in exit_profile.upper()) or ("PUMP" in strategy_tag.upper())
            if _setting_bool("pump_exit_enabled", True) and is_pump:
                _manage_pump_dynamic_exit(symbol, active_trade, df, price)
                return

            # Fallback: basic breakeven (legacy)
            if entry > 0:
                if 'LONG' in signal:
                    roi = (price - entry) / entry
                else:
                    roi = (entry - price) / entry

                trail_trigger = _setting_float("trail_trigger_roi", float(getattr(cfg, "TRAIL_TRIGGER", 0.005)))
                if roi > trail_trigger:
                    new_sl = entry  # Breakeven
                    needs_update = False
                    if 'LONG' in signal and (current_sl <= 0 or current_sl < entry):
                        needs_update = True
                    elif 'SHORT' in signal and (current_sl <= 0 or current_sl > entry):
                        needs_update = True

                    if needs_update:
                        log(f"🛡️ Trail Triggered for {symbol} (ROI: {roi*100:.2f}%) -> Moving SL to Breakeven", "TRADE")
                        if _mode_allows_orders(mode_now):
                            try:
                                move_stop_loss(symbol, new_sl)
                            except Exception:
                                pass
                        db.update_stop_loss(symbol, new_sl)

            return  # Exit function
# PHASE 2: FIND NEW TRADES
        # ----------------------------------------------------
        # Global cap on open trades (dashboard: max_concurrent_trades)
        try:
            max_trades = int(float(db.get_setting("max_concurrent_trades") or db.get_setting("max_open_trades") or getattr(cfg, "MAX_CONCURRENT_TRADES", 10) or 10))
            if len(db.get_all_active_trades()) >= max_trades:
                return
        except Exception:
            pass

        gate = market_gate(df, price, symbol)
        if gate == "BLOCK": pass 

        strategy = select_strategy(df, price, gate)
        if strategy == "NO_TRADE":
            _maybe_add_decision_trace({
                "symbol": symbol,
                "interval": tf,
                "market_type": market_type,
                "strategy": "",
                "gate": gate,
                "exit_profile": "",
                "decision": "NO_TRADE",
                "conf_pct": 0.0,
                "min_conf": None,
                "final_score": None,
                "rule_signal": "",
                "ai_vote": "OFF",
                "ai_confidence": 0.0,
                "pump_score": None,
                "rsi": None,
                "adx": None,
                "vol_spike": None,
                "funding": None,
                "oi_change": None,
                "trace": {"reason": "strategy_no_trade"},
            }, throttle_key=f"{symbol}|{tf}|{market_type}|no_trade")
            return

        algo = strategy.lower()

        # Market regime context (BTC): scalp uses 1H, swing uses 4H (optional via DB setting)
        regime = None
        btc_df_for_feats = None
        try:
            if _truthy(db.get_setting("market_regime_enabled") or "TRUE"):
                regime = _market_regime_for_strategy(algo)
                btc_df_for_feats = _get_btc_df(regime.get("tf"))
        except Exception:
            regime = None
            btc_df_for_feats = None

        reg_lbl = "OFF"
        reg_tf = ""
        try:
            if regime and isinstance(regime, dict):
                reg_lbl = str(regime.get("label") or "OFF")
                reg_tf = str(regime.get("tf") or "")
        except Exception:
            pass

        pump_score = detect_pre_pump(df, price)

        rsi_len = int(db.get_setting("ind_rsi_len") or 14)
        adx_len = int(db.get_setting("ind_adx_len") or 14)
        ma_fast_len = int(db.get_setting("ind_ma_fast") or 50)
        ma_slow_len = int(db.get_setting("ind_ma_slow") or 200)

        current_rsi = _safe_value(rsi(df, rsi_len), -1)
        current_adx = _safe_value(adx(df, adx_len), -1)
        current_vol_spike = int(_safe_value(volume_spike(df), -1))

        val_ma_fast = _safe_value(ema(df, ma_fast_len), -1)
        val_ma_slow = _safe_value(ema(df, ma_slow_len), -1)


        tech_score = technical_module_score(
            val_ma_fast,
            val_ma_slow,
            current_rsi,
            current_vol_spike,
            current_adx,
        )

        funding, oi = dm.get_derivatives_data(symbol) if market_type == "futures" else (0.0, 0.0)
        deriv_score = derivatives_module_score(funding, oi)

        final_score = combine_scores(
            {"technical": tech_score, "derivatives": deriv_score},
            algo, market_type
        )

        rule_signal = map_to_signal(final_score)

        # perf: time until scoring completed
        t_scores = time.perf_counter()
        perf_phase = "scored"

        # Indicators/scoring finished
        t_scores = time.perf_counter()
        perf_phase = "scored"

        # AI Prediction
        ai_vote = "OFF"
        ai_confidence = 0.0  # 0..1 (model output)
        latest_feats = None  # last feature row for closed-loop learning
        if MODEL.ready:
            feats = extract_features(df, btc_df=btc_df_for_feats)
            if not feats.empty:
                try:
                    latest_feats = feats.iloc[-1].to_dict()
                except Exception:
                    latest_feats = None
                pred = MODEL.predict(feats.iloc[-1:], market_type, algo)
                if pred:
                    try:
                        ai_vote = pred[0].get('signal', 'OFF')
                        ai_confidence = float(pred[0].get('confidence', 0.0) or 0.0)
                    except Exception:
                        ai_vote = pred[0].get('signal', 'OFF') if isinstance(pred[0], dict) else "OFF"
                        ai_confidence = 0.0

        t_model = time.perf_counter()
        perf_phase = "model"

        ai_conf_pct = (ai_confidence * 100.0) if 0.0 <= ai_confidence <= 1.0 else ai_confidence

        # perf: time until model step finished (even if MODEL is OFF)
        t_model = time.perf_counter()
        perf_phase = "modeled"

        # ======================================================
        # [CORRECTION] Apply Filters & Bonuses BEFORE Decision
        # ======================================================
        
        # 1. Pump Bonus (Dynamic from DB)
        pump_threshold = float(db.get_setting("pump_score_min") or 75)
        if pump_score >= pump_threshold: 
            final_score += 15  # Add to score before decision
        
        # 2. RSI Filter (Dynamic from DB)
        
        # ======================================================
        # PAPER TOURNAMENT: log virtual signals (does NOT affect trading)
        # ======================================================
        try:
            paper_enabled = (str(db.get_setting("paper_tournament_enabled") or "FALSE").upper() == "TRUE")
            if generate_paper_decisions and paper_enabled:
                # update rule_signal to match the latest final_score after bonuses
                rule_signal = map_to_signal(final_score)

                paper_decisions = generate_paper_decisions(
                    symbol=symbol,
                    market_type=market_type,
                    interval=tf,
                    df=df,
                    price=price,
                    tech_score=tech_score,
                    deriv_score=deriv_score,
                    vol_spike=current_vol_spike,
                    ai_vote=ai_vote,
                    ai_confidence=ai_confidence,
                    funding=funding,
                    oi_change=oi,
                    settings_getter=db.get_setting
                )
                rows = []
                for d in paper_decisions:
                    rows.append({
                        "symbol": d.symbol,
                        "signal_time_ms": d.signal_time_ms,
                        "market_type": d.market_type,
                        "interval": d.interval,
                        "strategy_tag": d.strategy_tag,
                        "decision": d.decision,
                        "entry_price": d.entry_price,
                        "final_score": d.final_score,
                        "pump_score": d.pump_score,
                        "confidence": d.confidence,
                        "tech_score": tech_score,
                        "deriv_score": deriv_score,
                        "vol_spike": current_vol_spike,
                        "ai_vote": ai_vote,
                        "ai_confidence": ai_confidence,
                        "funding": funding,
                        "oi_change": oi,
                    })
                db.add_paper_signals(rows)
        except Exception:
            pass

        rsi_limit = float(db.get_setting("rsi_max_buy") or 70)
        
        
        if current_rsi > rsi_limit:
            db.log_rejection(symbol, "RSI_OVERBOUGHT", current_rsi, rsi_limit)
            _maybe_add_decision_trace({
                "symbol": symbol,
                "interval": tf,
                "market_type": market_type,
                "strategy": algo,
                "gate": gate,
                "exit_profile": "",
                "decision": "NO_TRADE",
                "conf_pct": float(ai_conf_pct or 0.0),
                "min_conf": None,
                "final_score": float(final_score or 0.0),
                "rule_signal": rule_signal,
                "ai_vote": ai_vote,
                "ai_confidence": float(ai_confidence or 0.0),
                "pump_score": float(pump_score or 0.0),
                "rsi": float(current_rsi or 0.0),
                "adx": float(current_adx or 0.0),
                "vol_spike": int(current_vol_spike or 0),
                "funding": float(funding or 0.0),
                "oi_change": float(oi or 0.0),
                "trace": {"reason": "rsi_overbought", "rsi": float(current_rsi or 0.0), "limit": float(rsi_limit or 0.0)},
            }, throttle_key=f"{symbol}|{tf}|{market_type}|rsi")
            # RSI is too high, abort immediately
            return 

        # 3. Make Decision
        exit_profile = select_exit_profile(df, price, algo)
        decision, confidence, decision_trace = hybrid_signal_decision_trace(
            final_score, rule_signal, ai_vote, ai_confidence, algo, gate, exit_profile, regime=regime
        )

        # Save Strategy Decision Trace (Doctor)
        if decision != "TRADE":
            _maybe_add_decision_trace({
                "symbol": symbol,
                "interval": tf,
                "market_type": market_type,
                "strategy": algo,
                "gate": gate,
                "exit_profile": exit_profile,
                "decision": decision,
                "conf_pct": float(confidence or 0.0),
                "min_conf": float(decision_trace.get("min_conf") or 0.0) if isinstance(decision_trace, dict) else None,
                "final_score": float(final_score or 0.0),
                "rule_signal": rule_signal,
                "ai_vote": ai_vote,
                "ai_confidence": float(ai_confidence or 0.0),
                "pump_score": float(pump_score or 0.0),
                "rsi": float(current_rsi or 0.0),
                "adx": float(current_adx or 0.0),
                "vol_spike": int(current_vol_spike or 0),
                "funding": float(funding or 0.0),
                "oi_change": float(oi or 0.0),
                "trace": {
                    **(decision_trace if isinstance(decision_trace, dict) else {}),
                    "signals": {"technical": float(tech_score or 0.0), "derivatives": float(deriv_score or 0.0)},
                },
            }, throttle_key=f"{symbol}|{tf}|{market_type}|{algo}")

        # Check Decision
        if decision != "TRADE":
            log(f"📉 {symbol}: Score={final_score:.1f}, Mode={algo}, Regime={reg_lbl}({reg_tf}), AI={ai_vote}({ai_conf_pct:.0f}%), Conf={confidence:.1f}, Dec={decision}", "INFO")
            return

        # ----------------------------------------------------
        # PHASE 3: EXECUTION
        # ----------------------------------------------------
        sl, tps = apply_exit_profile(price, _safe_value(atr(df, 14), -1), exit_profile, rule_signal)
        
        base_size = float(db.get_setting("scalp_size_usd") if algo == "scalp" else db.get_setting("swing_size_usd"))
        max_trade = float(db.get_setting("max_trade_usd") or 2000)
        qty = calculate_quantity(min(base_size, max_trade), price)
        
        if qty <= 0: return

        explain = {
            "final_score": round(final_score, 2),
            "confidence": round(confidence, 2),
            "ai_vote": ai_vote,
            "strategy": algo,
            "exit_profile": exit_profile,
            "pump_score": pump_score,
            "signals": {"technical": round(tech_score, 2), "derivatives": round(deriv_score, 2)}
        }

        payload = {
            "symbol": symbol,
            "signal": rule_signal,
            "entry_price": price,
            "stop_loss": sl,
            "take_profits": tps,
            "quantity": qty,
            "confidence": confidence,
            "exit_profile": exit_profile,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "explain": explain,
            "leverage": int(float(db.get_setting("leverage_scalp") or getattr(cfg, "LEVERAGE_SCALP", 20))) if algo == "scalp" else int(float(db.get_setting("leverage_swing") or getattr(cfg, "LEVERAGE_SWING", 10))),
            "timeframe": str(tf or ""),
            "market_type": str(market_type or ""),
            "strategy_tag": str(strategy or algo or ""),
            "candle_close_ms": int(candle_close_ms or 0)
        }

        # --- Dashboard approval gate (no behavior change unless REQUIRE_DASHBOARD_APPROVAL=True) ---
        mode_local = db.get_setting("mode") or "TEST"
        require_approval = _setting_bool("require_dashboard_approval", bool(getattr(cfg, "REQUIRE_DASHBOARD_APPROVAL", True)))
        auto_approve_paper = _setting_bool("auto_approve_paper", bool(getattr(cfg, "AUTO_APPROVE_PAPER", True)))
        auto_approve_live = _setting_bool("auto_approve_live", bool(getattr(cfg, "AUTO_APPROVE_LIVE", False)))
        order_side = _normalize_order_side(rule_signal)
        # Strict: do not create PENDING or execute when decision is WAIT/invalid
        if order_side is None:
            return

        # Sprint 5: idempotency (dedupe same signal on same candle across restarts)
        try:
            if _setting_bool("dedupe_signal_keys", True):
                side_txt = _side_text(order_side)
                sig_key = _make_signal_key(symbol, market_type, tf, str(strategy or algo or ""), side_txt, int(candle_close_ms or 0))
                payload["signal_key"] = sig_key
                payload["dedupe_key"] = sig_key  # reuse for inbox
                # Skip if this exact signal was already executed/logged
                try:
                    if hasattr(db, 'has_trade_signal_key') and db.has_trade_signal_key(sig_key):
                        log(f"⏭️ DEDUPE skip (trade already exists for signal_key): {symbol} {tf} {side_txt}", "SYSTEM")
                        return
                except Exception:
                    pass
        except Exception:
            pass
        execution_allowed = True
        if require_approval:
            if _mode_allows_orders(mode_local):
                execution_allowed = auto_approve_live
            else:
                execution_allowed = auto_approve_paper

        # Do not create approval requests for WAIT/HOLD
        if require_approval and (order_side is None):
            return

        if require_approval and not execution_allowed:
            # ---- PENDING APPROVAL (strict, non-breaking) ----
            decision_txt = _side_text(order_side)  # LONG/SHORT
            pending_payload = dict(payload)
            pending_payload["status"] = "PENDING_APPROVAL"

            pending_payload["execution_allowed"] = False
            pending_payload["market_type"] = market_type
            pending_payload["side"] = decision_txt
            # Include derivatives meta + last feature snapshot (for closed-loop learning / dashboard)
            try:
                pending_payload["funding"] = float(funding or 0.0)
                pending_payload["funding_pct"] = float(funding or 0.0) * 100.0
            except Exception:
                pass
            try:
                pending_payload["oi_change"] = float(oi or 0.0)
                pending_payload["oi_change_pct"] = float(oi or 0.0) * 100.0
            except Exception:
                pass
            if latest_feats is not None and isinstance(latest_feats, dict) and latest_feats:
                pending_payload["features"] = latest_feats
            # Validate schema (best-effort) to prevent empty/invalid side
            if _HAS_SCHEMA_MODELS and SignalInboxItem is not None:
                try:
                    _ = SignalInboxItem(
                        symbol=symbol,
                        decision=decision_txt,
                        confidence=float(confidence or 0.0),
                        ai_vote=str(ai_vote or ""),
                        ai_confidence=float(ai_conf_pct or 0.0),
                        market_type=str(market_type or "futures"),
                        payload=dict(pending_payload),
                    )
                except Exception as _ve:
                    log(f"⚠️ Pending schema invalid for {symbol}: {_ve}", "SYSTEM")
                    return

            # Record in DB inbox (best-effort)
            inbox_id = None
            try:
                inbox_id = db.add_signal_inbox(
                    symbol=symbol,
                    timeframe=str(tf or ""),
                    strategy=str(strategy or algo or ""),
                    side=str(decision_txt or ""),
                    confidence=float(confidence or 0.0),
                    score=float(final_score or 0.0),
                    payload=dict(pending_payload),
                    status="PENDING_APPROVAL",
                    source="bot",
                    dedupe_key=str(pending_payload.get('dedupe_key') or pending_payload.get('signal_key') or ''),
                    candle_close_ms=int(pending_payload.get('candle_close_ms') or 0) or None,
                )
                pending_payload["inbox_id"] = inbox_id
            except Exception:
                pass

            # Closed-loop snapshot (best-effort)
            if inbox_id:
                try:
                    db.add_ai_sample(
                        symbol=symbol,
                        market_type=str(market_type or ""),
                        algo_mode=str(algo or ""),
                        timeframe=str(tf or ""),
                        strategy=str(strategy or algo or ""),
                        side=str(decision_txt or ""),
                        ai_vote=str(ai_vote or ""),
                        ai_confidence=float(ai_confidence or 0.0),
                        rule_score=float(final_score or 0.0),
                        confidence=float(confidence or 0.0),
                        features=(latest_feats or {}),
                        explain=dict(pending_payload),
                        inbox_id=int(inbox_id),
                        status="PENDING",
                    )
                except Exception:
                    pass


            # Throttle duplicates to avoid spamming dashboard
            key = f"{symbol}:{pending_payload.get('side')}:{market_type}"
            now_ts = time.time()
            last_ts = float(_LAST_PENDING_SENT.get(key, 0.0) or 0.0)
            if (now_ts - last_ts) >= _PENDING_COOLDOWN_SEC:
                _LAST_PENDING_SENT[key] = now_ts
                send_to_dashboard(pending_payload)
            log(f"🟡 PENDING APPROVAL {symbol} {pending_payload.get('side')} (Conf: {confidence:.1f}%)", "INFO")
            return

        mode = mode_local
        if _mode_allows_orders(mode):
            if market_type == 'futures':
                set_leverage(symbol, payload['leverage'])
            
            res = place_market_order(symbol, 'BUY' if 'LONG' in rule_signal else 'SELL', qty, sl_price=sl, market_type=market_type)
            
            if not res['ok']:
                log(f"❌ Execution Failed {symbol}: {res.get('error')}", "ERROR")
                return
            
            if res['result']['avgPrice'] > 0:
                payload['entry_price'] = res['result']['avgPrice']
            send_to_external_api("TRADE", payload)
            
            if market_type == "futures" and tps and _setting_bool("hard_tp_enabled", False):
                try:
                    tp_price = float((tps or [])[-1])
                    place_hard_tp(symbol, 'LONG' if 'LONG' in rule_signal else 'SHORT', tp_price, market_type)
                except Exception:
                    pass

        db.add_trade(payload)
        send_to_dashboard(payload)
        send_telegram_signal(cfg.TELEGRAM_CHAT_ID, payload)
        send_to_external_api("SIGNAL", payload)
        log(f"✅ EXECUTED {symbol} {rule_signal} (Conf: {confidence:.1f}%, AI={ai_vote}({ai_conf_pct:.0f}%))", "TRADE")

    except Exception as e:
        perf_err = str(e)
        log(f"{symbol} error: {e}", "ERROR")
    finally:
        # Perf metric row (best-effort) — recorded even on early returns.
        try:
            t_end = time.perf_counter()
            total_ms = int((t_end - t0_perf) * 1000)

            def _ms(a, b):
                try:
                    if a is None or b is None:
                        return None
                    return int((b - a) * 1000)
                except Exception:
                    return None

            fetch_ms = _ms(t0_perf, t_fetch) if t_fetch is not None else None
            score_ms = _ms(t_fetch, t_scores) if (t_fetch is not None and t_scores is not None) else None
            model_ms = _ms(t_scores, t_model) if (t_scores is not None and t_model is not None) else None

            meta = {
                "phase": perf_phase,
                "fetch_ms": fetch_ms,
                "score_ms": score_ms,
                "model_ms": model_ms,
            }
            if perf_err:
                meta["error"] = perf_err

            _maybe_add_perf_metric({
                "symbol": symbol,
                "interval": tf,
                "market_type": market_type,
                "stage": "analyze_coin",
                "duration_ms": total_ms,
                "ok": False if perf_err else True,
                "meta": meta,
            }, throttle_key=f"{symbol}|{tf}|{market_type}")
        except Exception:
            pass

        PROCESSING_COINS.discard(symbol)

# ==========================================================
# ASYNC WRAPPER
# ==========================================================
async def async_analyze_wrapper(symbol, tf, kline, market_type):
    """
    Wraps the synchronous analyze_coin function to be used 
    in async WebSocket callbacks without blocking the loop.
    """
    async with analysis_semaphore:
        await asyncio.to_thread(analyze_coin, symbol, tf, market_type, kline)

# ==========================================================
# BACKGROUND TASKS
# ==========================================================
def run_risk_monitor_thread():
    try:
        supervisor = RiskSupervisor()
        supervisor.run()
    except Exception as e:
        print(f"[WARN] Risk Monitor failed to start: {e}")



def _claim_pending_commands_filtered(db, *, only_cmds, limit: int = 50, worker: str = "worker"):
    """Claim only specific commands from the shared `commands` table.

    Why: both main.py and execution_monitor.py run concurrently.
    If one process claims *all* pending commands, it can accidentally consume
    commands intended for the other process (and mark them DONE).

    Returns a list of tuples: (id, cmd, params_json_text)
    """
    try:
        lim = int(limit)
    except Exception:
        lim = 50
    if lim <= 0:
        lim = 50

    cmd_list = []
    for c in (only_cmds or []):
        s = str(c or '').strip().upper()
        if s:
            cmd_list.append(s)

    if not cmd_list:
        return []

    try:
        lock = getattr(db, 'lock', None)
        conn = getattr(db, 'conn', None)
        if conn is None:
            return []

        # Legacy fallback: if status column doesn't exist, filter a simple pending list.
        try:
            cur = conn.cursor()
            cur.execute('PRAGMA table_info(commands)')
            cols = {r[1] for r in (cur.fetchall() or [])}
            if 'status' not in cols:
                pending = db.get_pending_commands() if hasattr(db, 'get_pending_commands') else []
                out = []
                for r in pending:
                    try:
                        if str(r[1] or '').strip().upper() in cmd_list:
                            out.append((r[0], r[1], (r[2] if len(r) > 2 else None)))
                    except Exception:
                        continue
                return out[:lim]
        except Exception:
            pass

        def _do_claim():
            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
            now_ms = int(time.time() * 1000)

            try:
                conn.execute('BEGIN IMMEDIATE')
            except Exception:
                pass

            cur = conn.cursor()
            placeholders = ','.join(['?'] * len(cmd_list))
            rows = cur.execute(
                f"SELECT id, cmd, params FROM commands WHERE status='PENDING' AND UPPER(cmd) IN ({placeholders}) ORDER BY id ASC LIMIT ?",
                tuple(cmd_list) + (lim,),
            ).fetchall()

            claimed = []
            for r in (rows or []):
                try:
                    cid = int(r[0])
                    upd = cur.execute(
                        "UPDATE commands SET status='IN_PROGRESS', claimed_by=?, claimed_at=?, claimed_at_ms=? WHERE id=? AND status='PENDING'",
                        (str(worker), now_iso, int(now_ms), int(cid)),
                    )
                    if (upd.rowcount or 0) == 1:
                        claimed.append((r[0], r[1], r[2]))
                except Exception:
                    continue

            conn.commit()
            return claimed

        if lock is not None:
            with lock:
                return _do_claim()
        return _do_claim()

    except Exception:
        return []
async def command_monitor_task():
    """Listens for manual commands from Dashboard"""
    log("📡 Command Monitor Active...", "SYSTEM")
    while True:
        try:
            # Claim only commands that main.py is responsible for.
            # IMPORTANT: execution_monitor.py owns protection/close/cleanup commands.
            only_cmds = (
                'RELOAD_MODELS',
                'RELOAD_CONFIG',
                'MANUAL_TRADE',
                'EXECUTE_SIGNAL',
                'CHECK_SIGNAL',
            )

            # Prefer DB-native filtered claiming (Sprint 8+). Fallback to local helper for older DB code.
            try:
                commands = db.claim_pending_commands(limit=50, worker='main', only_cmds=only_cmds)
            except TypeError:
                commands = _claim_pending_commands_filtered(
                    db,
                    only_cmds=only_cmds,
                    limit=50,
                    worker='main',
                )
            for cid, cmd, params_str in commands:
                cmd_ok = True
                cmd_err = ''
                try:
                    params = json.loads(params_str) if params_str else {}
                    
                    if cmd in ("RELOAD_MODELS", "RELOAD_CONFIG"):
                        global _last_reload_ts
                        now_ts = time.time()
                        if (now_ts - float(_last_reload_ts or 0.0)) < float(RELOAD_MODELS_COOLDOWN_SEC or 60):
                            log(f"⏳ RELOAD_MODELS ignored (cooldown {RELOAD_MODELS_COOLDOWN_SEC}s)", "SYSTEM")
                        else:
                            _last_reload_ts = now_ts
                            log("🔄 Reloading runtime config + AI models...", "SYSTEM")
                            try:
                                cfg.reload()
                                try:
                                    _load_runtime_knobs_from_cfg()
                                except Exception as _e:
                                    log(f"⚠️ Runtime knobs reload warning: {_e}", "SYSTEM")
                            except Exception as e:
                                log(f"⚠️ Config reload warning: {e}", "SYSTEM")
                            MODEL.reload()
                            log("✅ Config + AI Models Reloaded Successfully", "SYSTEM")

                    # [FIXED] FULL MANUAL TRADE LOGIC RESTORED
                    elif cmd == "MANUAL_TRADE":
                        sym = params.get('symbol')
                        side = params.get('side')
                        amt = float(params.get('amount', 0))
                        m_type = params.get('market_type', 'futures')
                        
                        log(f"⚠️ Manual Trade: {side} {sym} ${amt}", "SYSTEM")
                        
                        # 1. Fetch Price
                        df = fetch_klines(sym, '5m', limit=10, market_type=m_type)
                        if df is not None and not df.empty:
                            price = float(df['close'].iloc[-1])
                            qty = calculate_quantity(amt, price)
                            
                            # 2. Execute
                            mode = db.get_setting("mode") or "TEST"
                            if _mode_allows_orders(mode):
                                if m_type == 'futures': set_leverage(sym, 10)
                                res = place_market_order(sym, side, qty, market_type=m_type)
                                if res['ok']:
                                    log(f"✅ Manual Executed: {res['result']}", "TRADE")
                                else:
                                    log(f"❌ Execution Failed: {res.get('error')}", "ERROR")
                            else:
                                log(f"📝 PAPER Executed: {side} {qty} {sym}", "TRADE")
                            
                            # 3. Log to DB (Always log, even for paper/test)
                            payload = {
                                "symbol": sym, "signal": f"MANUAL_{side}", 
                                "entry_price": price, "quantity": qty,
                                "stop_loss": 0, "take_profits": [],
                                "confidence": 100, "exit_profile": "MANUAL",
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "explain": {"manual": True}
                            }
                            db.add_trade(payload)
                            send_to_dashboard(payload)
                    elif cmd == "EXECUTE_SIGNAL":
                        # Execute a dashboard-approved signal (BUY/SELL). Strict schema validation + non-blocking execution.
                        try:
                            inbox_id = params.get("inbox_id") or params.get("signal_inbox_id") or params.get("id")
                            exec_payload = params.get("payload") if isinstance(params.get("payload"), dict) else params

                            # Idempotency guard: if this inbox item was already executed, skip duplicate commands.
                            if inbox_id:
                                try:
                                    _row = db.get_signal_inbox(inbox_id)
                                    if _row and str(_row.get("status") or "").upper() == "EXECUTED":
                                        log(f"⏭️ EXECUTE_SIGNAL skipped (already executed): inbox_id={inbox_id}", "SYSTEM")
                                        continue
                                except Exception:
                                    pass

                            # If only inbox_id was provided, load the stored payload
                            if inbox_id and (not isinstance(exec_payload, dict) or not exec_payload.get("symbol")):
                                row = db.get_signal_inbox(inbox_id)
                                if row:
                                    stored = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                                    merged = {}
                                    # prefer explicit columns (side/timeframe/strategy/confidence/score) if present
                                    for k in ["symbol", "side", "timeframe", "strategy", "confidence", "score", "market_type"]:
                                        v = row.get(k)
                                        if v is not None and str(v).strip() != "":
                                            merged[k] = v
                                    if isinstance(stored, dict):
                                        merged.update(stored)
                                    exec_payload = merged


                            if not isinstance(exec_payload, dict):
                                raise ValueError("EXECUTE_SIGNAL payload must be a dict")

                            # Skip meaningless approvals
                            raw_dec = str(exec_payload.get("decision") or exec_payload.get("side") or exec_payload.get("signal") or "").strip().upper()
                            if raw_dec == "WAIT":
                                log("⏭️ EXECUTE_SIGNAL skipped (decision=WAIT)", "SYSTEM")
                                raise ValueError("Invalid side: WAIT")

                            # Validate + normalize
                            if _HAS_SCHEMA_MODELS and ExecuteSignalPayload is not None:
                                try:
                                    pl = ExecuteSignalPayload.model_validate(exec_payload) if hasattr(ExecuteSignalPayload, "model_validate") else ExecuteSignalPayload.parse_obj(exec_payload)
                                except Exception as ve:
                                    raise ValueError(f"Bad EXECUTE_SIGNAL payload: {ve}")
                                sym = pl.symbol
                                order_side = pl.normalized_side()  # BUY/SELL
                                m_type = pl.normalized_market_type()
                            else:
                                sym = (exec_payload.get("symbol") or exec_payload.get("s") or "").strip().upper()
                                if not sym:
                                    raise ValueError("Missing symbol in EXECUTE_SIGNAL payload")
                                m_type = str(exec_payload.get("market_type") or exec_payload.get("market") or "futures").lower()
                                order_side = _normalize_order_side(exec_payload.get("side") or exec_payload.get("decision") or exec_payload.get("signal"))
                                if order_side is None:
                                    raise ValueError(f"Invalid side: {exec_payload.get('side') or exec_payload.get('decision') or ''}")

                            
                            
                            # Sprint 5: idempotency for approved executions (skip if trade already exists for same signal_key)
                            try:
                                sk = str(exec_payload.get('signal_key') or exec_payload.get('dedupe_key') or '').strip()
                                if sk and hasattr(db, 'has_trade_signal_key') and db.has_trade_signal_key(sk):
                                    log(f"⏭️ EXECUTE_SIGNAL dedupe (signal_key already traded): {sym} key={sk}", "SYSTEM")
                                    # mark inbox executed to avoid re-queuing forever (best-effort)
                                    try:
                                        if inbox_id:
                                            db.mark_signal_inbox_executed(inbox_id, note='Skipped duplicate (signal_key)')
                                    except Exception:
                                        pass
                                    continue
                            except Exception:
                                pass

                            # Global cap guard: prevent command-driven executions from opening too many concurrent trades
                            try:
                                cap = int(db.get_setting("max_concurrent_trades") or getattr(cfg, "MAX_CONCURRENT_TRADES", 0) or 0)
                            except Exception:
                                cap = 0
                            if cap and cap > 0:
                                try:
                                    active_n = len(db.get_all_active_trades()) if hasattr(db, "get_all_active_trades") else 0
                                except Exception:
                                    active_n = 0
                                if active_n >= cap:
                                    log(f"⏭️ EXECUTE_SIGNAL skipped (cap reached): active={active_n} cap={cap} symbol={sym}", "SYSTEM")
                                    continue
                            # avoid double-execution
                            if db.get_active_trade(sym):
                                log(f"⏭️ EXECUTE_SIGNAL skipped (already active): {sym}", "SYSTEM")
                                continue


                            # --- Cross-process symbol lock + cooldown (prevents rapid re-entry / duplicate approvals) ---
                            sym_u = str(sym or "").upper().strip()
                            if not sym_u:
                                raise ValueError("Invalid/empty symbol")
                            sym = sym_u

                            # Acquire short lock to prevent concurrent executions for same symbol (even across processes)
                            try:
                                if not db.acquire_symbol_lock(sym, ttl_sec=None):
                                    log(f"⏭️ EXECUTE_SIGNAL skipped (symbol locked): {sym}", "SYSTEM")
                                    continue
                            except Exception:
                                pass

                            # Optional: block if execution_monitor snapshot says there is already an open position
                            try:
                                if str(db.get_setting("block_if_open_position") or "1").strip() in ("1", "true", "True", "YES", "yes"):
                                    pos_json = db.get_setting("positions_status") or ""
                                    if pos_json:
                                        pos_map = json.loads(pos_json) if isinstance(pos_json, str) else (pos_json or {})
                                        p = pos_map.get(sym) if isinstance(pos_map, dict) else None
                                        if isinstance(p, dict):
                                            amt = float(p.get("positionAmt") or p.get("amt") or 0.0)
                                            if abs(amt) > 0:
                                                log(f"⏭️ EXECUTE_SIGNAL skipped (open position snapshot): {sym} amt={amt}", "SYSTEM")
                                                continue
                            except Exception:
                                pass

                            # Cooldown based on last trade (prevents repeated entries within seconds/minutes)
                            try:
                                cd_trade = int(float(db.get_setting("cooldown_after_trade_sec") or 900))
                            except Exception:
                                cd_trade = 900
                            try:
                                cd_sl = int(float(db.get_setting("cooldown_after_sl_sec") or 1800))
                            except Exception:
                                cd_sl = 1800
                            cd_trade = max(0, cd_trade)
                            cd_sl = max(0, cd_sl)
                            try:
                                last = db.get_last_trade_for_symbol(sym)
                                if last:
                                    reason = str(last.get("close_reason") or "").upper()
                                    base_ms = int(last.get("closed_at_ms") or 0) or int(last.get("timestamp_ms") or 0) or 0
                                    if base_ms > 0:
                                        cd = cd_sl if ("STOP" in reason) else cd_trade
                                        if cd > 0:
                                            now_ms = int(time.time() * 1000)
                                            if (now_ms - base_ms) < (cd * 1000):
                                                log(f"⏳ EXECUTE_SIGNAL cooldown active for {sym}: wait {cd}s", "SYSTEM")
                                                continue
                            except Exception:
                                pass

                            # Quantity
                            def _first_float(d, keys, default=0.0):
                                for k in keys:
                                    if k in d and d.get(k) is not None:
                                        try:
                                            return float(d.get(k))
                                        except Exception:
                                            pass
                                return float(default)

                            qty = _first_float(exec_payload, ["quantity", "qty"], 0.0)
                            if qty <= 0:
                                usd_amount = _first_float(exec_payload, ["amount_usd", "usd_amount", "amount", "usd"], 0.0)
                                if usd_amount <= 0:
                                    usd_amount = float(db.get_setting("swing_size_usd") or 50)
                                dfq = fetch_klines(sym, "1m", limit=5, market_type=m_type)
                                px = float(dfq["close"].iloc[-1]) if (dfq is not None and not dfq.empty) else 0.0
                                qty = float(calculate_quantity(usd_amount, px) or 0.0)
                                if qty <= 0:
                                    raise ValueError("Unable to compute quantity for EXECUTE_SIGNAL")

                            sl_val = _first_float(exec_payload, ["stop_loss", "sl"], 0.0)
                            tps = exec_payload.get("take_profits") or exec_payload.get("tps") or []
                            leverage = exec_payload.get("leverage")

                            # Auto-generate SL/TP if missing (dashboard approvals often don't carry risk params)
                            if (sl_val is None) or (float(sl_val) <= 0.0):
                                try:
                                    strat_txt = str(exec_payload.get("strategy") or exec_payload.get("strategy_tag") or "").lower()
                                    if not strat_txt:
                                        strat_txt = str(exec_payload.get("mode") or "").lower()
                                    tf = str(exec_payload.get("timeframe") or "").strip()
                                    if not tf:
                                        tf = "5m" if "scalp" in strat_txt or "pump" in strat_txt else "15m"
                                    # Infer exit profile when not explicitly provided
                                    # =========================================
                                    # 🧠 Strategy Override (dashboard-approved learning recommendation)
                                    # - Applies ONLY when strategy_override_enabled=TRUE in settings
                                    # - We DO NOT force side here (safety). We only set exit_profile / tag metadata.
                                    # =========================================
                                    try:
                                        ov_enabled = str(db.get_setting('strategy_override_enabled') or '').strip().upper()
                                        if ov_enabled in ('1','TRUE','YES','ON'):
                                            ov_exit = str(db.get_setting('strategy_override_exit_profile') or '').strip()
                                            ov_tag  = str(db.get_setting('strategy_override_strategy_tag') or '').strip()
                                            ov_bias = str(db.get_setting('strategy_override_bias') or '').strip()
                                            ov_mkt  = str(db.get_setting('strategy_override_market_type') or '').strip()
                                            if ov_exit:
                                                exec_payload['exit_profile'] = ov_exit
                                            if ov_tag and not exec_payload.get('strategy_tag') and not exec_payload.get('strategy'):
                                                exec_payload['strategy_tag'] = ov_tag
                                            # metadata only (optional)
                                            if ov_bias:
                                                exec_payload.setdefault('bias_hint', ov_bias)
                                            if ov_mkt:
                                                exec_payload.setdefault('market_type_hint', ov_mkt)
                                    except Exception:
                                        pass

                                    exit_profile = str(exec_payload.get("exit_profile") or exec_payload.get("exitProfile") or "").strip().upper()
                                    if not exit_profile:
                                        if "pump" in strat_txt:
                                            exit_profile = "SCALP_PUMP"
                                        elif "scalp" in strat_txt:
                                            exit_profile = "SCALP_FAST"
                                        elif "swing" in strat_txt:
                                            exit_profile = "SWING_TREND"
                                        else:
                                            exit_profile = "SCALP_FAST"
                                        exec_payload["exit_profile"] = exit_profile

                                    entry_for_risk = float(exec_payload.get("entry_price") or exec_payload.get("entryPrice") or price or 0.0)
                                    if entry_for_risk <= 0:
                                        dfq = fetch_klines(sym, "1m", limit=2, market_type=m_type)
                                        entry_for_risk = float(dfq["close"].iloc[-1]) if (dfq is not None and not dfq.empty) else 0.0

                                    atr_v = 0.0
                                    try:
                                        dfr = fetch_klines(sym, tf, limit=200, market_type=m_type)
                                        if dfr is not None and not dfr.empty:
                                            av = atr(dfr, 14)
                                            # atr() may return float or Series depending on implementation
                                            atr_v = float(av.iloc[-1]) if hasattr(av, "iloc") else float(av)
                                    except Exception:
                                        atr_v = 0.0
                                    if atr_v <= 0 and entry_for_risk > 0:
                                        # fallback: ~0.25% of price as ATR proxy
                                        atr_v = entry_for_risk * 0.0025

                                    side_txt = _side_text(order_side)  # LONG/SHORT
                                    sl_auto, tps_auto = apply_exit_profile(entry_for_risk, atr_v, exit_profile, side_txt)
                                    try:
                                        sl_val = float(sl_auto) if sl_auto is not None else 0.0
                                    except Exception:
                                        sl_val = 0.0
                                    tps = tps_auto or []
                                    # persist into payload so DB stores them
                                    if sl_val and float(sl_val) > 0:
                                        exec_payload["stop_loss"] = float(sl_val)
                                    if tps:
                                        exec_payload["take_profits"] = tps
                                except Exception:
                                    pass

                            mode = str(db.get_setting("mode") or "TEST").upper()

                            exec_record = dict(exec_payload)
                            # carry idempotency metadata
                            if exec_payload.get('signal_key'):
                                exec_record['signal_key'] = exec_payload.get('signal_key')
                            if exec_payload.get('candle_close_ms'):
                                try:
                                    exec_record['candle_close_ms'] = int(exec_payload.get('candle_close_ms'))
                                except Exception:
                                    pass
                            if exec_payload.get('timeframe'):
                                exec_record['timeframe'] = exec_payload.get('timeframe')

                            # Trade rows should be OPEN so that risk limits / active trade checks work correctly.
                            exec_record["status"] = "OPEN"
                            exec_record["execution_allowed"] = True
                            exec_record["executed_at"] = datetime.now(timezone.utc).isoformat()
                            exec_record["side"] = _side_text(order_side)  # LONG/SHORT
                            exec_record["market_type"] = m_type
                            exec_record["quantity"] = qty
                            if sl_val > 0:
                                exec_record["stop_loss"] = sl_val

                            if _mode_allows_orders(mode):
                                if m_type == "futures" and leverage:
                                    try:
                                        await asyncio.to_thread(set_leverage, sym, int(leverage))
                                    except Exception:
                                        pass
                                res = await asyncio.to_thread(place_market_order, sym, order_side, qty, sl_price=float(sl_val) if sl_val > 0 else None, market_type=m_type)
                                if not res.get("ok"):
                                    raise ValueError(res.get("error") or "Execution failed")
                                # Use executedQty (rounded/actual) to keep DB in sync with exchange position
                                try:
                                    exq = float((res.get("result", {}) or {}).get("executedQty") or 0.0)
                                    if exq > 0:
                                        exec_record["quantity"] = exq
                                except Exception:
                                    pass
                                try:
                                    ap = res.get("result", {}).get("avgPrice")
                                    if ap and float(ap) > 0:
                                        exec_record["entry_price"] = float(ap)
                                except Exception:
                                    pass
                                # optional hard TP
                                try:
                                    if m_type == "futures" and tps:
                                        # Put hard TP on the LAST TP level (acts as a safety net, avoids capping profits too early).
                                        await asyncio.to_thread(place_hard_tp, sym, exec_record["side"], float((tps or [])[-1]), market_type=m_type)
                                except Exception:
                                    pass
                                log(f"✅ EXECUTE_SIGNAL executed LIVE: {sym} {order_side} qty={qty}", "TRADE")
                            else:
                                # For paper/test, set entry_price to current price for PnL correctness
                                try:
                                    if "entry_price" not in exec_record or not exec_record.get("entry_price"):
                                        dfq = fetch_klines(sym, "1m", limit=2, market_type=m_type)
                                        px = float(dfq["close"].iloc[-1]) if (dfq is not None and not dfq.empty) else 0.0
                                        if px > 0:
                                            exec_record["entry_price"] = px
                                except Exception:
                                    pass
                                log(f"📝 EXECUTE_SIGNAL paper/test: {sym} {order_side} qty={qty}", "TRADE")
                            trade_id = None
                            try:
                                trade_id = db.add_trade(exec_record)
                            except Exception:
                                trade_id = None

                            # Link closed-loop sample to the executed trade (best-effort)
                            if inbox_id and trade_id:
                                try:
                                    db.link_ai_sample_to_trade(inbox_id, trade_id, entry_price=exec_record.get("entry_price"))
                                except Exception:
                                    pass

                            send_to_dashboard(exec_record)
                            if inbox_id:
                                try:
                                    db.mark_signal_inbox_executed(inbox_id, note="Executed by bot")
                                except Exception:
                                    pass
                        except Exception as e:
                            log(f"❌ EXECUTE_SIGNAL failed: {e}", "ERROR")
                    elif cmd == "CHECK_SIGNAL":

                        sym = params.get('symbol')
                        m_type = params.get('market_type', 'futures')
                        log(f"🔎 Manual Analysis Requested for {sym}...", "SYSTEM")
                        
                        # Fetch Logic Gate Status manually to give immediate feedback
                        # (The async wrapper will also run, but this prints 'why' immediately)
                        df = fetch_klines(sym, "5m", 500, m_type)
                        if df is not None and not df.empty:
                            price = float(df["close"].iloc[-1])
                            gate = market_gate(df, price, sym)
                            strategy = select_strategy(df, price, gate)
                            log(f"📊 {sym} Report: Gate={gate} | Strategy={strategy}", "SYSTEM")
                            
                        asyncio.create_task(async_analyze_wrapper(sym, "5m", None, m_type))

                    # NOTE: Ops commands like CLOSE_TRADE / CLOSE_ALL_POSITIONS are handled by execution_monitor.py.
                    # main.py intentionally does NOT claim them (filtered claiming).

                except Exception as e:
                    cmd_ok = False
                    cmd_err = str(e)
                    log(f"Command Execution Error ({cmd}): {e}", "ERROR")
                finally:
                    try:
                        db.mark_command_done(cid, ok=cmd_ok, error=cmd_err)
                    except TypeError:
                        # backward-compatible fallback
                        db.mark_command_done(cid)
        except Exception as e:
            print(f"[WARN] Command Monitor Loop: {e}")
        await asyncio.sleep(2)

# ==========================================================
# MAIN LOOP
# ==========================================================
def system_health_check():
    """
    Background thread to monitor system health and update DB.
    """
    print("❤️ Heartbeat Thread Started...")
    while True:

        # Restart request (from dashboard). Works best when bot runs under a loop/supervisor.
        try:
            global _LAST_RESTART_BOT_REQ
            req = str(db.get_setting("restart_bot_req") or "").strip()
            if req and req != str(_LAST_RESTART_BOT_REQ or ""):
                _LAST_RESTART_BOT_REQ = req
                try:
                    db.set_setting(
                        "restart_bot_ack",
                        datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        bump_version=False,
                        audit=False,
                    )
                except Exception:
                    pass
                print("[SYSTEM] 🔁 Restart requested from Dashboard. Exiting now...")
                os._exit(3)
        except Exception:
            pass

        try:
            # 1. Heartbeat (I am alive)
            now_iso = datetime.now(timezone.utc).isoformat()
            db.set_setting("last_heartbeat", now_iso, bump_version=False, audit=False)
            db.set_setting("bot_heartbeat", now_iso, bump_version=False, audit=False)

            # 1b. Runtime flags (safe)
            try:
                db.set_setting("bot_use_testnet", str(bool(getattr(cfg, "USE_TESTNET", False))), bump_version=False, audit=False)
                db.set_setting("bot_paper_trading", str(bool(getattr(cfg, "PAPER_TRADING", False))), bump_version=False, audit=False)
            except Exception:
                pass

            # 1c. DataManager status (OI/Funding)
            try:
                dm_running = bool(getattr(dm, "running", False))
                db.set_setting("data_manager_ok", "1" if dm_running else "0", bump_version=False, audit=False)
                if dm_running:
                    db.set_setting("data_manager_heartbeat", now_iso, bump_version=False, audit=False)
                err_msg = getattr(dm, "_last_err_msg", "") or ""
                if err_msg:
                    db.set_setting("data_manager_last_error", err_msg, bump_version=False, audit=False)
            except Exception:
                pass
            
            

            # 1d. Futures WS status (best-effort)
            try:
                from realtime_futures_ws import get_ws_status as _get_ws_status
                ws = _get_ws_status() or {}
                db.set_setting("ws_status", json.dumps(ws), bump_version=False, audit=False)
            except Exception:
                pass

            # 1e. Models status (best-effort)
            try:
                model_dir = getattr(MODEL, "model_dir", "./models")
                loaded = []
                if getattr(MODEL, "futures_model", None) is not None:
                    loaded.append("futures")
                if getattr(MODEL, "scalp_model", None) is not None:
                    loaded.append("scalp")
                if getattr(MODEL, "spot_model", None) is not None:
                    loaded.append("spot")

                def _file_info(p):
                    try:
                        if not p or not os.path.exists(p):
                            return {"path": p, "exists": False}
                        st = os.stat(p)
                        return {
                            "path": p,
                            "exists": True,
                            "size": int(st.st_size),
                            "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat(),
                        }
                    except Exception:
                        return {"path": p, "exists": False}

                candidates = {
                    "futures_model": [os.path.join(model_dir, "futures_model.joblib"), os.path.join(model_dir, "futures_model.pkl")],
                    "scalp_model": [os.path.join(model_dir, "scalp_model.joblib"), os.path.join(model_dir, "scalp_model.pkl")],
                    "spot_model": [os.path.join(model_dir, "spot_model.joblib"), os.path.join(model_dir, "spot_model.pkl")],
                }
                model_files = {k: [_file_info(p) for p in v] for k, v in candidates.items()}

                models_status = {
                    "ready": bool(getattr(MODEL, "ready", False)),
                    "loaded": loaded,
                    "last_load_utc": getattr(MODEL, "last_load_utc", None),
                    "use_closed_loop_models": bool(getattr(cfg, "USE_CLOSED_LOOP_MODELS", False)),
                    "model_dir": model_dir,
                    "files": model_files,
                }
                db.set_setting("models_status", json.dumps(models_status), bump_version=False, audit=False)
            except Exception:
                pass
            # 2. Measure Binance Latency + Rate Limits (Futures)
            try:
                rest_base = str(getattr(cfg, 'BINANCE_FUTURES_REST_BASE', '') or '').strip()
                if not rest_base:
                    rest_base = 'https://demo-fapi.binance.com' if bool(getattr(cfg, 'USE_TESTNET', True)) else 'https://fapi.binance.com'
                rest_base = rest_base.rstrip('/')
                url = f"{rest_base}/fapi/v1/time"

                t1 = time.time()
                resp = requests.get(url, timeout=5)
                latency = int((time.time() - t1) * 1000)  # ms

                # Headers contain used weight in last minute (when available)
                used_weight = resp.headers.get("x-mbx-used-weight-1m") or resp.headers.get("X-MBX-USED-WEIGHT-1M")

                db.set_setting("api_latency", str(latency), bump_version=False, audit=False)
                db.set_setting("api_used_weight_1m", str(used_weight) if used_weight is not None else "", bump_version=False, audit=False)
                db.set_setting("api_last_status", str(resp.status_code), bump_version=False, audit=False)
                db.set_setting("api_endpoint", rest_base, bump_version=False, audit=False)
                db.set_setting("api_last_check_utc", datetime.now(timezone.utc).replace(microsecond=0).isoformat(), bump_version=False, audit=False)

                # rolling latency history (last 60 samples)
                try:
                    hist = json.loads(db.get_setting("api_latency_history") or "[]")
                    if not isinstance(hist, list):
                        hist = []
                except Exception:
                    hist = []
                hist.append(int(latency))
                if len(hist) > 60:
                    hist = hist[-60:]
                try:
                    db.set_setting("api_latency_history", json.dumps(hist), bump_version=False, audit=False)
                except Exception:
                    pass
            except Exception as _he:
                # keep old key updated for UI even if call fails
                try:
                    db.set_setting("api_last_status", "ERR", bump_version=False, audit=False)
                    db.set_setting("api_last_error", str(_he)[:220], bump_version=False, audit=False)
                except Exception:
                    pass

            # 3. System Resources
            cpu = psutil.cpu_percent(interval=1)
            ram = psutil.virtual_memory().percent
            db.set_setting("sys_cpu", cpu, bump_version=False, audit=False)
            db.set_setting("sys_ram", ram, bump_version=False, audit=False)

            # 4. Database Size (Optional check)
            # db_size = os.path.getsize("bot_data.db") / (1024*1024)
            # db.set_setting("db_size_mb", round(db_size, 2))

        except Exception as e:
            import traceback
            print(f"[HEALTH FAIL] {e}\n{traceback.format_exc()}")
        
        time.sleep(10) # Check every 30 seconds


async def housekeeping_task():
    """Periodic cleanup for in-memory state (single-server safe)."""
    while True:
        try:
            now = time.time()
            # prune pending throttle map
            try:
                for k, ts in list(_LAST_PENDING_SENT.items()):
                    if (now - float(ts or 0.0)) > max(600.0, float(_PENDING_COOLDOWN_SEC or 60) * 10.0):
                        _LAST_PENDING_SENT.pop(k, None)
            except Exception:
                pass

            # prune pump exit state by TTL (2 hours default) to avoid growth
            ttl = float(getattr(cfg, "PUMP_STATE_TTL_SEC", 7200) or 7200)
            try:
                for k, st in list(PUMP_EXIT_STATE.items()):
                    last_ts = 0.0
                    try:
                        last_ts = float(st.get("last_ts") or 0.0)
                    except Exception:
                        last_ts = 0.0
                    if last_ts and (now - last_ts) > ttl:
                        PUMP_EXIT_STATE.pop(k, None)
            except Exception:
                pass
        except Exception:
            pass
        await asyncio.sleep(60)


async def main():
    # 1. Start Risk Monitor
    # Housekeeping (in-memory TTL cleanup)
    asyncio.create_task(housekeeping_task())
    t = threading.Thread(target=run_risk_monitor_thread, daemon=True)
    t.start()

    # 1b. Start Paper Tournament services (Paper Arena evaluator + daily UTC recommendations)
    try:
        if run_daily_paper_tournament is not None:
            def _paper_loop():
                while True:
                    try:
                        enabled = str(db.get_setting("paper_tournament_enabled") or "FALSE").upper() == "TRUE"
                        if enabled:
                            # Runs evaluation frequently; recommendations run once/day after configured UTC time
                            run_daily_paper_tournament(db, allow_reco=True)
                    except Exception:
                        pass
                    try:
                        interval_min = int(float(db.get_setting("paper_eval_interval_min") or 15))
                    except Exception:
                        interval_min = 15
                    time.sleep(max(60, interval_min * 60))

            threading.Thread(target=_paper_loop, daemon=True).start()

        elif PaperEvaluator is not None:
            # Fallback evaluator (older approach)
            pe = PaperEvaluator(db, poll_seconds=60)
            pe.start()
    except Exception:
        pass

    # 2. Fetch Markets
    futures_all = fetch_futures_raw()
    futures = _limit_symbols(futures_all, FUTURES_SYMBOL_LIMIT) if FUTURES_SYMBOL_LIMIT else futures_all
    spot_all = fetch_spot_raw()
    spot = _limit_symbols(spot_all, SPOT_SYMBOL_LIMIT) if SPOT_SYMBOL_LIMIT else spot_all

    # 3. Start Data Manager
    dm.start(futures)

    # 4. Start Sockets
    if FUTURES_WS_ENABLED:
        loop.create_task(start_futures_sockets(futures, TF_LIST, lambda s, tf, k, m: async_analyze_wrapper(s, tf, k, m )))
    else:
        log("⚠️ Futures WS disabled by env", "SYSTEM")
    if SPOT_WS_ENABLED:
        loop.create_task(start_kline_socket(spot, SPOT_WS_TF, lambda s, tf, k: async_analyze_wrapper(s, tf, k, "spot")))
    else:
        log("⚠️ Spot WS disabled by env", "SYSTEM")
    
    loop.create_task(command_monitor_task())
    health_thread = threading.Thread(target=system_health_check, daemon=True)
    health_thread.start()
    log("🚀 GOLDEN BOT ONLINE (AI + RISK GUARD + ASYNC FIX)", "SYSTEM")
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    loop.run_until_complete(main())
