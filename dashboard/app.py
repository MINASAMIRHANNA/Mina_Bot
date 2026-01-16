"""
Dashboard (FastAPI)
- Login-protected UI + APIs
- Public: /healthz, /api/healthz, /status, /login, /publish (optionally token protected)
- Signal Inbox + approval workflow
- Analytics + Audit + Paper Arena
- Settings editor (writes to DB settings table)
"""

import os
import sys
import json
import math
import asyncio
import secrets
import requests
import subprocess
import re
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta


from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

# =========================================
# PATH FIX (so dashboard can import bot modules)
# =========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))          # .../crypto_bot/dashboard
PROJECT_ROOT = os.path.dirname(BASE_DIR)                       # .../crypto_bot
sys.path.append(PROJECT_ROOT)


# Bot imports (root level)
from database import DatabaseManager
from config import cfg
from binance.client import Client

from fetcher import fetch_klines, fetch_klines_window, fetch_klines_range
from indicators import ema, rsi, volume_spike, atr, adx
from pump_detector import detect_pre_pump

# Optional modules
try:
    from audit_lab import AuditLab  # should exist under dashboard/ or root (because PROJECT_ROOT is on sys.path)
except Exception:
    AuditLab = None

try:
    # if present (root level), gives rich project status + maintenance helpers
    from project_doctor import collect_status, run_maintenance
except Exception:
    collect_status = None
    run_maintenance = None

# =========================================
# INIT: DB + Templates + App
# =========================================
# Prefer single DB path (Zero-ENV): config.cfg decides bot_data.db
_DB_PATH = getattr(cfg, "DB_FILE", None)
if not _DB_PATH:
    cand = os.path.join(PROJECT_ROOT, "bot_data.db")
    if os.path.exists(cand):
        _DB_PATH = cand

db = DatabaseManager(db_file=_DB_PATH) if _DB_PATH else DatabaseManager()

lab = None
if AuditLab and not bool(getattr(cfg, "DISABLE_AUDIT_LAB", False)):
    try:
        lab = AuditLab()
    except Exception as e:
        print(f"[WARN] AuditLab disabled (init failed): {e}")
        lab = None

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

PUBLIC_PATHS = {
    "/login",
    "/healthz",
    "/api/healthz",
    "/status",
    "/publish",
}

def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _ms_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _safe_last(x, default: float = 0.0) -> float:
    """Safely get the last numeric value from pandas Series/ndarray/list/scalar."""
    try:
        if x is None:
            return default
        # pandas Series / DataFrame column
        if hasattr(x, "iloc"):
            v = x.iloc[-1]
            return float(v) if v is not None else default
        # list/tuple
        if isinstance(x, (list, tuple)):
            return float(x[-1]) if x else default
        # numpy array
        try:
            import numpy as np  # local import (optional)
            if isinstance(x, np.ndarray):
                return float(x.flat[-1]) if x.size else default
        except Exception:
            pass
        # scalar
        return float(x)
    except Exception:
        return default


def _normalize_side(raw) -> str | None:
    """Normalize various encodings to BUY/SELL. Returns None for invalid/WAIT."""
    try:
        if raw is None:
            return None
        s = str(raw).strip().upper()
        if not s:
            return None
        if s in {"WAIT", "HOLD", "NONE", "0"}:
            return None
        if s in {"BUY", "LONG", "BULL", "1", "+1"}:
            return "BUY"
        if s in {"SELL", "SHORT", "BEAR", "-1"}:
            return "SELL"

        # token-based parsing (avoids bugs like 'STRONG_SHORT' containing 'LONG' in 'STRONG')
        tokens = [t for t in re.split(r"[^A-Z]+", s) if t]
        buy_tokens = {"BUY", "LONG", "BULL"}
        sell_tokens = {"SELL", "SHORT", "BEAR"}

        has_buy = any(t in buy_tokens for t in tokens)
        has_sell = any(t in sell_tokens for t in tokens)

        if has_buy and has_sell:
            return None
        if has_sell:
            return "SELL"
        if has_buy:
            return "BUY"
        return None
    except Exception:
        return None


def _positions_status_map() -> Dict[str, Dict[str, Any]]:
    """Map symbol -> position snapshot from execution_monitor (best-effort)."""
    try:
        raw = db.get_setting("positions_status")
        if not raw:
            return {}
        if isinstance(raw, dict):
            j = raw
        else:
            j = json.loads(raw) if isinstance(raw, str) else json.loads(str(raw))
        pos_list = j.get("positions") or []
        out: Dict[str, Dict[str, Any]] = {}
        for p in pos_list:
            sym = str(p.get("symbol") or "").upper().strip()
            if sym:
                out[sym] = p
        return out
    except Exception:
        return {}



def _execmon_age_seconds() -> int:
    """How old is the execution_monitor heartbeat."""
    hb = db.get_setting("execution_monitor_heartbeat")
    if not hb:
        return 999999
    try:
        t = datetime.fromisoformat(str(hb).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - t.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return 999999


def _public_price(symbol: str, market_type: str = "futures") -> float:
    """Best-effort public price fetch (no auth needed)."""
    symbol = str(symbol).upper().strip()
    mt = str(market_type or "futures").lower().strip()
    try:
        if mt == "spot":
            r = requests.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": symbol}, timeout=6)
            if r.ok:
                return float(r.json().get("price") or 0.0)
        else:
            # Futures: mark price via premiumIndex is usually best.
            r = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": symbol}, timeout=6)
            if r.ok:
                j = r.json()
                for k in ("markPrice", "indexPrice", "lastPrice"):
                    if j.get(k) is not None:
                        return float(j.get(k) or 0.0)
            # fallback
            r2 = requests.get("https://fapi.binance.com/fapi/v1/ticker/price", params={"symbol": symbol}, timeout=6)
            if r2.ok:
                return float(r2.json().get("price") or 0.0)
    except Exception:
        pass
    return 0.0

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Dashboard Startup: Checking DB Connection...")
    try:
        _ensure_signal_inbox_table()
    except Exception as e:
        print(f"⚠️ signal_inbox init skipped: {e}")
    yield
    print("🛑 Dashboard Shutdown")

app = FastAPI(lifespan=lifespan)

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Always return JSON errors so the dashboard JS can parse failures.
    return JSONResponse(
        status_code=500,
        content={"status": "error", "detail": "Internal server error", "hint": "Check dashboard server logs."},
    )

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


# =========================================
# 🔒 AUTH MIDDLEWARE
# =========================================
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # static + public endpoints
        if path.startswith("/static") or path in PUBLIC_PATHS:
            return await call_next(request)

        # API endpoints return JSON 401 if not authed
        session_token = request.cookies.get("session_token")
        if session_token == "logged_in_secret_token":
            return await call_next(request)

        if path.startswith("/api"):
            return JSONResponse(status_code=401, content={"msg": "Unauthorized"})

        return RedirectResponse(url="/login")

app.add_middleware(AuthMiddleware)


# =========================================
# 🔑 LOGIN ROUTES
# =========================================
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_action(username: str = Form(...), password: str = Form(...)):
    # Zero-ENV: credentials are stored in DB settings (WEB_USERNAME / WEB_PASSWORD)
    valid_user = str(db.get_setting("WEB_USERNAME") or "admin")
    valid_pass = str(db.get_setting("WEB_PASSWORD") or "admin")

    if username == valid_user and password == valid_pass:
        resp = JSONResponse(content={"msg": "Success"})
        resp.set_cookie(key="session_token", value="logged_in_secret_token", httponly=True)
        return resp

    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login")
    resp.delete_cookie("session_token")
    return resp


# =========================================
# ❤️ HEALTH CHECK (Public, no redirects)
# =========================================
@app.get("/healthz")
async def healthz():
    """Public health endpoint used by project_doctor."""
    time_utc = _utc_iso_now()
    db_file = getattr(db, "db_file", None)

    db_ok = True
    db_error = None
    try:
        conn = getattr(db, "conn", None)
        if conn is None:
            raise RuntimeError("db.conn not available")
        conn.execute("SELECT 1").fetchone()
    except Exception as e:
        db_ok = False
        db_error = str(e)

    # Execution Monitor heartbeat (written by execution_monitor.py)
    exec_mon_last = None
    exec_mon_age_sec = None
    exec_mon_ok = False
    try:
        if hasattr(db, "get_setting"):
            hb = db.get_setting("execution_monitor_heartbeat")
            if hb:
                exec_mon_last = str(hb)
                try:
                    dt_hb = datetime.fromisoformat(exec_mon_last.replace("Z", "+00:00"))
                    exec_mon_age_sec = int((datetime.now(timezone.utc) - dt_hb.astimezone(timezone.utc)).total_seconds())
                    exec_mon_ok = exec_mon_age_sec <= 30
                except Exception:
                    exec_mon_ok = False
    except Exception:
        exec_mon_ok = False

    return {
        "ok": db_ok,
        "service": "dashboard",
        "time_utc": time_utc,
        "db": db_file,
        "db_ok": db_ok,
        "db_error": db_error,
        "execution_monitor_ok": exec_mon_ok,
        "execution_monitor_last_utc": exec_mon_last,
        "execution_monitor_age_sec": exec_mon_age_sec,
    }

@app.get("/api/healthz")
async def api_healthz():
    # alias for convenience
    return await healthz()


# =========================================
# 🩺 PROJECT DOCTOR (UI + API)
# =========================================
@app.get("/doctor", response_class=HTMLResponse)
async def doctor_page(request: Request):
    """Project Doctor page: shows runtime health + effective settings."""
    return templates.TemplateResponse("doctor.html", {"request": request})


def _parse_iso_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _age_sec(dt_obj):
    try:
        if not dt_obj:
            return None
        return int((datetime.now(timezone.utc) - dt_obj.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


@app.get("/api/doctor")
async def api_doctor():
    """Aggregated health + settings snapshot (login required)."""
    import time
    now_utc = _utc_iso_now()

    # --- DB health ---
    db_file = getattr(db, "db_file", None)
    db_ok = True
    db_error = None
    try:
        conn = getattr(db, "conn", None)
        if conn is None:
            raise RuntimeError("db.conn not available")
        conn.execute("SELECT 1").fetchone()
    except Exception as e:
        db_ok = False
        db_error = str(e)

    # --- Heartbeats from DB settings/state ---
    settings = {}
    try:
        if hasattr(db, "get_all_settings"):
            settings = db.get_all_settings() or {}
    except Exception:
        settings = {}

    # Bot heartbeat (written by main.py system_health_check)
    bot_hb = settings.get("last_heartbeat") or settings.get("bot_heartbeat")
    bot_dt = _parse_iso_dt(str(bot_hb)) if bot_hb else None
    bot_age = _age_sec(bot_dt)
    bot_ok = (bot_age is not None) and (bot_age <= 30)

    # Execution monitor heartbeat
    exec_hb = settings.get("execution_monitor_heartbeat")
    exec_dt = _parse_iso_dt(str(exec_hb)) if exec_hb else None
    exec_age = _age_sec(exec_dt)
    exec_ok = (exec_age is not None) and (exec_age <= 30)

    # Pump Hunter heartbeat (ms)
    pump_state = {}
    pump_ok = False
    pump_age = None
    try:
        if hasattr(db, "get_pump_hunter_state"):
            pump_state = db.get_pump_hunter_state() or {}
            hb_ms = int(pump_state.get("last_heartbeat_ms") or 0)
            if hb_ms > 0:
                pump_age = int((time.time() * 1000 - hb_ms) / 1000)
                pump_ok = pump_age <= 60
    except Exception:
        pump_state = {}
        pump_ok = False
        pump_age = None

    # Risk Supervisor heartbeat (optional)
    rs_hb = settings.get("risk_supervisor_heartbeat")
    rs_dt = _parse_iso_dt(str(rs_hb)) if rs_hb else None
    rs_age = _age_sec(rs_dt)
    rs_ok = (rs_age is not None) and (rs_age <= 60)
    rs_err = settings.get("risk_supervisor_last_error") or ""

    # DataManager status (optional)
    dm_ok = str(settings.get("data_manager_ok") or "").strip() in {"1", "true", "True", "YES", "yes"}
    dm_err = settings.get("data_manager_last_error") or ""
    dm_last = settings.get("data_manager_last_update_utc") or settings.get("data_manager_heartbeat")
    dm_dt = _parse_iso_dt(str(dm_last)) if dm_last else None
    dm_age = _age_sec(dm_dt)

    # --- WS / Models / Positions snapshots (best-effort) ---
    def _safe_json_loads(x):
        try:
            if x is None:
                return None
            if isinstance(x, (dict, list)):
                return x
            s = str(x).strip()
            if not s:
                return None
            return json.loads(s)
        except Exception:
            return None

    ws_status = _safe_json_loads(settings.get("ws_status")) or {}
    models_status = _safe_json_loads(settings.get("models_status")) or {}
    positions_status = _safe_json_loads(settings.get("positions_status")) or {}

    # DB open trades snapshot (best-effort)
    open_trades = []
    try:
        if hasattr(db, "get_all_active_trades"):
            open_trades = db.get_all_active_trades() or []
    except Exception:
        open_trades = []

    # trim open trades for UI
    open_trades_view = []
    try:
        for t in open_trades[:50]:
            open_trades_view.append({
                "id": t.get("id"),
                "symbol": t.get("symbol"),
                "side": t.get("side"),
                "qty": t.get("qty"),
                "entry_price": t.get("entry_price"),
                "stop_loss": t.get("stop_loss"),
                "take_profits": t.get("take_profits"),
                "status": t.get("status"),
                "market_type": t.get("market_type"),
                "opened_utc": t.get("opened_utc") or t.get("open_time_utc"),
            })
    except Exception:
        open_trades_view = []


    # System metrics (best-effort)
    api_latency_ms = settings.get("api_latency")
    sys_cpu = settings.get("sys_cpu")
    sys_ram = settings.get("sys_ram")


    # Rate limits / latency (best-effort)
    api_used_weight_1m = settings.get("api_used_weight_1m")
    api_last_status = settings.get("api_last_status")
    api_endpoint = settings.get("api_endpoint")
    api_last_check_utc = settings.get("api_last_check_utc")
    api_last_error = settings.get("api_last_error")
    api_latency_history = _safe_json_loads(settings.get("api_latency_history")) or []

    api_metrics_execmon = _safe_json_loads(settings.get("api_metrics_execmon")) or {}
    protection_audit_status = _safe_json_loads(settings.get("protection_audit_status")) or {}

    decision_traces_recent = []
    try:
        if hasattr(db, "get_recent_decision_traces"):
            decision_traces_recent = db.get_recent_decision_traces(limit=20) or []
    except Exception:
        decision_traces_recent = []

    # --- Effective mode sanity ---
    # Zero-ENV: expose runtime config snapshot from cfg (no real OS env).
    try:
        cfg.reload()
    except Exception:
        pass

    env_snapshot = {
        "TIMEZONE": getattr(cfg, "TIMEZONE", "UTC"),
        "DB_FILE": getattr(cfg, "DB_FILE", ""),
        "USE_TESTNET": str(bool(getattr(cfg, "USE_TESTNET", True))),
        "PAPER_TRADING": str(bool(getattr(cfg, "PAPER_TRADING", True))),
        "ENABLE_LIVE_TRADING": str(bool(getattr(cfg, "ENABLE_LIVE_TRADING", False))),
        "BINANCE_FUTURES_REST_BASE": getattr(cfg, "BINANCE_FUTURES_REST_BASE", ""),
        "BINANCE_FUTURES_WS_BASE": getattr(cfg, "BINANCE_FUTURES_WS_BASE", ""),
        "BINANCE_API_KEY_PRESENT": "1" if getattr(cfg, "BINANCE_API_KEY", "") else "0",
        "BINANCE_API_SECRET_PRESENT": "1" if getattr(cfg, "BINANCE_API_SECRET", "") else "0",
    }

    mode_db = str(settings.get("mode") or "").strip().upper()
    paper_env = str(env_snapshot.get("PAPER_TRADING") or "").strip().lower() in {"1","true","yes","y","on"}

    effective_trading = "PAPER" if paper_env else ("LIVE" if mode_db == "LIVE" else ("TEST" if mode_db else "UNKNOWN"))

    warnings = []
    if not db_ok:
        warnings.append(f"DB not OK: {db_error}")
    if not bot_ok:
        warnings.append("Bot heartbeat missing/stale (main.py not running or DB mismatch).")
    if not exec_ok:
        warnings.append("Execution Monitor heartbeat missing/stale (execution_monitor.py not running).")

    # Protection audit warnings (best-effort)
    try:
        iss = (protection_audit_status or {}).get("issues") or {}
        orphan = iss.get("orphan_orders") or []
        miss_sl = iss.get("missing_sl") or []
        miss_tp = iss.get("missing_tp") or []
        miss_tr = iss.get("missing_trail") or []
        dups = iss.get("duplicates") or []
        if orphan:
            warnings.append(f"Orphan protective orders detected: {len(orphan)} symbol(s) have protective orders with no position/trade.")
        if miss_sl:
            warnings.append(f"Missing SL protection on {len(miss_sl)} position(s) (ensure_protection_orders enabled).")
        if miss_tp:
            warnings.append(f"Missing TP orders on {len(miss_tp)} position(s) (hard_tp_enabled enabled).")
        if miss_tr:
            warnings.append(f"Missing Exchange Trailing Stop on {len(miss_tr)} position(s) after activation ROI.")
        if dups:
            warnings.append(f"Duplicate protective orders detected: {len(dups)} symbol(s) have multiple SL/TP/TRAIL orders.")
    except Exception:
        pass
    if paper_env and mode_db == "LIVE":
        warnings.append("Mismatch: mode=LIVE in DB but PAPER_TRADING=true in DB config (Live will be blocked for safety).")
    if (env_snapshot.get("USE_TESTNET","").strip().lower() in {"1","true","yes","y","on"}) and ("demo-fapi" not in (env_snapshot.get("BINANCE_FUTURES_REST_BASE","") or "") ):
        # Only a hint; endpoint can still be fine if blank and code defaults.
        warnings.append("Hint: USE_TESTNET=true but BINANCE_FUTURES_REST_BASE not set to demo-fapi (defaults are OK, but set it explicitly if you get errors).")
    if rs_err:
        warnings.append(f"RiskSupervisor last_error: {rs_err}")
    if dm_err:
        warnings.append(f"DataManager last_error: {dm_err}")
    if pump_state.get("last_error"):
        warnings.append(f"PumpHunter last_error: {pump_state.get('last_error')}")


    # WS / Models warnings (best-effort)
    try:
        sockets = (ws_status or {}).get("sockets") or {}
        connected = 0
        for s in sockets.values():
            if isinstance(s, dict) and s.get("connected"):
                connected += 1
        if sockets and connected == 0:
            warnings.append("Futures WS: no sockets connected.")
        err_total = int((ws_status or {}).get("errors_total") or 0)
        if err_total > 0 and err_total >= max(3, len(sockets) or 1):
            warnings.append(f"Futures WS: errors_total={err_total} (check network/streams load).")
    except Exception:
        pass

    try:
        if models_status and (not bool(models_status.get("ready"))):
            warnings.append("AI models: not ready (no models loaded).")
    except Exception:
        pass

    try:
        upd = (positions_status or {}).get("updated_utc")
        if upd:
            dtp = _parse_iso_dt(str(upd))
            agep = _age_sec(dtp)
            if agep is not None and agep > 60:
                warnings.append(f"Positions snapshot stale: age={agep}s (execution_monitor may be down).")
    except Exception:
        pass

    # Select key settings to show prominently
    key_settings = [
        "mode",
        "require_dashboard_approval",
        "auto_approve_live",
        "auto_approve_paper",
        "max_concurrent_trades",
        "scalp_size_usd",
        "swing_size_usd",
        "max_trade_usd",
        "leverage_scalp",
        "leverage_swing",
        "hard_tp_enabled",
        "trail_trigger_roi",
        "exchange_trailing_enabled",
        "exchange_trailing_activation_roi",
        "exchange_trailing_callback_rate",
        "exchange_trailing_working_type",
        "ensure_protection_orders",
        "ensure_protection_every_sec",
        "kill_switch",
    ]
    current_settings = {k: settings.get(k) for k in key_settings if k in settings}

    return {
        "ok": db_ok,
        "time_utc": now_utc,
        "db": {"file": db_file, "ok": db_ok, "error": db_error},
        "components": {
            "bot": {"ok": bot_ok, "last_utc": bot_hb, "age_sec": bot_age},
            "execution_monitor": {"ok": exec_ok, "last_utc": exec_hb, "age_sec": exec_age},
            "pump_hunter": {"ok": pump_ok, "age_sec": pump_age, "state": pump_state},
            "risk_supervisor": {"ok": rs_ok, "last_utc": rs_hb, "age_sec": rs_age, "last_error": rs_err},
            "data_manager": {"ok": dm_ok, "last_utc": dm_last, "age_sec": dm_age, "last_error": dm_err},
        },
        "metrics": {
            "api_latency_ms": api_latency_ms,
            "sys_cpu": sys_cpu,
            "sys_ram": sys_ram,
            "api_used_weight_1m": api_used_weight_1m,
            "api_last_status": api_last_status,
            "api_endpoint": api_endpoint,
            "api_last_check_utc": api_last_check_utc,
            "api_last_error": api_last_error,
            "api_latency_history": api_latency_history,
            "execmon_api": api_metrics_execmon,
        },
        "protection_audit": protection_audit_status,
        "decision_traces_recent": decision_traces_recent,
        "effective": {"trading_mode": effective_trading},
        "env": env_snapshot,
        "settings": current_settings,
        "settings_all": settings,
        "ws": ws_status,
        "models": models_status,
        "positions": {"exchange": positions_status, "db_open_trades": open_trades_view},
        "warnings": warnings,
    }


# =========================================
# 🧾 STATUS PAGE (Public)
# =========================================
@app.get("/status", response_class=HTMLResponse)
async def status_page(request: Request):
    """Human-friendly status page (no login required)."""
    try:
        data = await healthz()
    except Exception as e:
        data = {"ok": False, "error": str(e)}
    return templates.TemplateResponse("status.html", {"request": request, "status": data})


# =========================================
# 🌐 MAIN UI PAGES
# =========================================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    return templates.TemplateResponse("analytics.html", {"request": request})

@app.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    return templates.TemplateResponse("audit_lab.html", {"request": request})

@app.get("/paper", response_class=HTMLResponse)
async def paper_page(request: Request):
    return templates.TemplateResponse("paper_arena.html", {"request": request})


@app.get("/pump", response_class=HTMLResponse)
async def pump_page(request: Request):
    return templates.TemplateResponse("pump_hunter.html", {"request": request})


# =========================================
# ⚙️ SETTINGS API
# =========================================
BOOL_KEYS = {
    "enable_learning",
    "webhook_enable_signal",
    "webhook_enable_trade",
    "paper_tournament_enabled",
    "paper_daily_enabled",
    "paper_eval_enabled",
    "require_dashboard_approval",
    "auto_approve_live",
    "auto_approve_paper",
    "auto_approve_test",
    "fut_pump_enabled",
    "SPOT_WS_ENABLED",
    "FUTURES_WS_ENABLED",
    "pump_hunter_enabled",
    "pump_hunter_allow_ai_weak",
    "pump_hunter_mirror_inbox",
    "pump_exit_enabled",
    "pump_exit_use_atr",
    "hard_tp_enabled",
        "exchange_trailing_enabled",
        "ensure_protection_orders",
    "decision_trace_enabled",
    "protection_audit_enabled",

    # safe optional cleanup of orphan protective orders
    "orphan_orders_cleanup_enabled",
}

SETTING_RULES = {
    "scalp_size_usd": ("float", 1, 100000),
    "swing_size_usd": ("float", 1, 1000000),
    "max_trade_usd": ("float", 1, 5000000),
    "max_concurrent_trades": ("int", 1, 200),

    "leverage_scalp": ("int", 1, 125),
    "leverage_swing": ("int", 1, 125),
    "trail_trigger_roi": ("float", 0.0, 1.0),
    "exchange_trailing_activation_roi": ("float", 0.0, 1.0),
    "exchange_trailing_callback_rate": ("float", 0.1, 5.0),
    "ensure_protection_every_sec": ("int", 10, 3600),

    "decision_trace_every_sec": ("int", 1, 600),
    "protection_audit_every_sec": ("int", 5, 3600),

    # orphan order cleanup knobs
    "orphan_orders_cleanup_confirm_cycles": ("int", 1, 10),
    "orphan_orders_cleanup_max_per_cycle": ("int", 1, 500),

    "gate_atr_min_pct": ("float", 0.0, 0.05),
    "gate_adx_min": ("float", 0.0, 80.0),
    "rsi_max_buy": ("float", 50.0, 95.0),
    "pump_score_min": ("int", 0, 100),

    "paper_eval_interval_min": ("int", 1, 1440),
    "paper_leaderboard_days": ("int", 1, 365),
    "fut_pump_score_min": ("int", 0, 100),
    "fut_pump_oi_min": ("float", 0.0, 1e9),
    "fut_pump_funding_max": ("float", -1.0, 1.0),

    # HH:MM UTC
    "paper_daily_time_utc": ("hhmm", None, None),


    "pump_hunter_top_n": ("int", 1, 200),
    "pump_hunter_scan_sec": ("int", 2, 300),
    "pump_hunter_max_per_scan": ("int", 1, 100),
    "pump_hunter_pump_score_min": ("int", 0, 100),
    "pump_hunter_ai_conf_min": ("float", 0, 100),
    "pump_hunter_oi_change_min": ("float", 0, 10),
    "pump_hunter_funding_max_abs": ("float", 0, 0.2),
    "pump_hunter_leverage": ("int", 1, 125),
    "pump_exit_be_trigger_roi": ("float", 0, 5),
    "pump_exit_be_buffer_pct": ("float", 0, 1),
    "pump_exit_trail_trigger_roi": ("float", 0, 5),
    "pump_exit_trail_atr_mult": ("float", 0, 10),
    "pump_exit_trail_pct": ("float", 0, 1),
    "pump_exit_retrace_pct": ("float", 0, 1),
    "pump_exit_min_profit_for_exhaust": ("float", 0, 5),
    "pump_exit_max_minutes": ("int", 1, 1440),
}

def _to_bool_str(v: Any) -> str:
    s = str(v).strip().upper()
    return "TRUE" if s in ("1", "TRUE", "YES", "ON", "Y") else "FALSE"

def _validate_setting(k: str, v: Any) -> str:
    if k.startswith("__"):
        raise HTTPException(status_code=400, detail="Meta keys are not editable")

    if k in BOOL_KEYS:
        return _to_bool_str(v)

    # Normalize empty values
    s = "" if v is None else str(v).strip()
    rule = SETTING_RULES.get(k)
    if s == "":
        # For numeric/time keys, blank is invalid (prevents 500s from float('') / int(''))
        if rule and rule[0] in ("int", "float", "hhmm"):
            raise HTTPException(status_code=400, detail=f"{k} cannot be blank")
        return ""

    if not rule:
        return str(v)

    typ, lo, hi = rule

    try:
        if typ == "int":
            iv = int(float(s))
            if lo is not None and iv < lo:
                iv = lo
            if hi is not None and iv > hi:
                iv = hi
            return str(iv)

        if typ == "float":
            fv = float(s)
            if lo is not None and fv < lo:
                fv = lo
            if hi is not None and fv > hi:
                fv = hi
            return str(fv)

        if typ == "hhmm":
            if len(s) != 5 or s[2] != ":":
                raise HTTPException(status_code=400, detail="paper_daily_time_utc must be HH:MM")
            hh = int(s[:2]); mm = int(s[3:])
            if hh < 0 or hh > 23 or mm < 0 or mm > 59:
                raise HTTPException(status_code=400, detail="paper_daily_time_utc invalid time")
            return f"{hh:02d}:{mm:02d}"
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid value for {k}: {s}")

    return str(v)

@app.get("/api/settings")
async def get_settings():
    keys = [
        "mode",
        "kill_switch",
        "kill_switch_reason",
        "scalp_size_usd",
        "swing_size_usd",
        "max_trade_usd",
        "max_concurrent_trades",
        "leverage_scalp",
        "leverage_swing",
        "trail_trigger_roi",
        "require_dashboard_approval",
        "auto_approve_live",
        "auto_approve_paper",
        "hard_tp_enabled",
        "exchange_trailing_enabled",
        "exchange_trailing_activation_roi",
        "exchange_trailing_callback_rate",
        "exchange_trailing_working_type",
        "ensure_protection_orders",
        "ensure_protection_every_sec",
        "strategy_mode",
        "blacklist",
        "gate_atr_min_pct",
        "gate_adx_min",
        "rsi_max_buy",
        "pump_score_min",
        "paper_tournament_enabled",
        "paper_daily_enabled",
        "paper_daily_time_utc",
        "min_conf_scalp",
        "min_conf_swing",
        "sl_scalp_mult",
        "tp_scalp_mult",
        "sl_swing_mult",
        "tp_swing_mult",
        "ind_rsi_len",
        "ind_adx_len",
        "ind_ma_fast",
        "ind_ma_slow",
        "webhook_url",
        "webhook_enable_signal",
        "webhook_enable_trade",
        "ai_conf_scalp",
        "ai_conf_swing",
        "ai_rr_atr_mult",
        "ai_risk_atr_mult",
        "paper_eval_enabled",
        "paper_eval_horizon",
        "paper_eval_market",
        "paper_ai_weight",
        "paper_tech_weight",
        "paper_pump_weight",
        "paper_min_score",
        "paper_min_conf",
        "fut_pump_enabled",
        "fut_pump_min_oi_change",
        "fut_pump_vol_mult",
        "fut_pump_min_score",
        "entry_threshold",
        "trail_trigger",
        "leverage_scalp",
        "leverage_swing",
        "max_leverage",
        "sl_multiplier",
        "tp_multiplier",
        "pump_hunter_enabled",
        "pump_hunter_allow_ai_weak",
        "pump_hunter_market",
        "pump_hunter_interval",
        "pump_hunter_top_n",
        "pump_hunter_scan_sec",
        "pump_hunter_max_per_scan",
        "pump_hunter_pump_score_min",
        "pump_hunter_ai_conf_min",
        "pump_hunter_oi_change_min",
        "pump_hunter_funding_max_abs",
        "pump_hunter_exit_profile",
        "pump_hunter_leverage",
        "pump_hunter_mirror_inbox",
        "pump_exit_enabled",
        "pump_exit_use_atr",
        "pump_exit_be_trigger_roi",
        "pump_exit_be_buffer_pct",
        "pump_exit_trail_trigger_roi",
        "pump_exit_trail_atr_mult",
        "pump_exit_trail_pct",
        "pump_exit_retrace_pct",
        "pump_exit_min_profit_for_exhaust",
        "pump_exit_max_minutes",
        "cooldown_after_trade_sec",
        "cooldown_after_sl_sec",
        "inbox_dedupe_sec",
        "symbol_lock_ttl_sec",
        "block_if_open_position",
        "restart_bot_req",
        "restart_monitor_req",
]
    return {k: db.get_setting(k) for k in keys}

@app.post("/api/settings")
async def update_settings(request: Request):
    data = await request.json()

    # Telegram notify on mode change
    old_mode = db.get_setting("mode")
    new_mode = data.get("mode")
    if new_mode and new_mode != old_mode:
        try:
            token = cfg.TELEGRAM_BOT_TOKEN
            chat_id = cfg.TELEGRAM_CHAT_ID
            if token and chat_id:
                icon = "🛡️" if str(new_mode).upper() == "TEST" else "🔥"
                msg = f"{icon} MANUAL OVERRIDE\nTrading Mode changed to: {new_mode}"
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=5,
                )
        except Exception:
            pass

    for k, v in (data or {}).items():
        val = _validate_setting(k, v)
        db.set_setting(k, val, source="dashboard", bump_version=True, audit=True)

    return {"status": "ok"}


@app.get("/api/settings_all")
async def get_settings_all():
    """Return ALL settings from DB (dynamic)."""
    return db.get_all_settings() if hasattr(db, "get_all_settings") else {}


@app.post("/api/control/clear_kill_switch")
async def clear_kill_switch():
    """Disable kill_switch so the bot can trade again."""
    try:
        db.set_setting("kill_switch", "0", source="dashboard", bump_version=True, audit=True)
        db.set_setting("kill_switch_reason", "", source="dashboard", bump_version=True, audit=True)
    except Exception:
        pass
    return {"ok": True}


@app.post("/api/control/restart_bot")
async def restart_bot():
    """Request bot restart (requires running under loop/supervisor)."""
    token = str(int(time.time() * 1000))
    try:
        db.set_setting("restart_bot_req", token, source="dashboard", bump_version=False, audit=True)
    except Exception:
        pass
    return {"ok": True, "req": token}


@app.post("/api/control/restart_monitor")
async def restart_monitor():
    """Request execution_monitor restart (requires running under loop/supervisor)."""
    token = str(int(time.time() * 1000))
    try:
        db.set_setting("restart_monitor_req", token, source="dashboard", bump_version=False, audit=True)
    except Exception:
        pass
    return {"ok": True, "req": token}

@app.post("/api/test_webhook")
async def test_webhook(request: Request):
    """Send a test payload to a webhook URL (used by Webhook Integration tab)."""
    try:
        data = await request.json()
    except Exception:
        data = {}
    url = (data or {}).get("url") or db.get_setting("webhook_url")
    if not url:
        return {"ok": False, "msg": "❌ Please set a webhook URL first."}

    payload = {
        "type": "TEST",
        "ts": _utc_iso_now(),
        "msg": "Webhook test from dashboard",
    }

    try:
        r = requests.post(str(url), json=payload, timeout=8)
        if r.ok:
            return {"ok": True, "msg": "✅ Webhook reachable (HTTP %s)" % r.status_code}
        return {"ok": False, "msg": f"❌ Webhook failed (HTTP {r.status_code})"}
    except Exception as e:
        return {"ok": False, "msg": f"❌ Webhook error: {e}"}

@app.get("/api/env_secrets")
async def get_env_secrets():
    """Backward-compatible endpoint name.

    Stage A (Zero-ENV): secrets/config are stored in DB settings (not .env).
    """
    keys = [
        "BINANCE_API_KEY",
        "BINANCE_API_SECRET",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_ERROR_CHAT_ID",
        "USE_TESTNET",
        "ENABLE_LIVE_TRADING",
        "PAPER_TRADING",
        "RUN_MODE",
        "HEARTBEAT_INTERVAL",
        "WS_CHUNK_SIZE",
        "TRAIL_TRIGGER",
        "DEFAULT_RISK_PCT",
        "MAX_LEVERAGE",
        "LEVERAGE_SCALP",
        "LEVERAGE_SWING",
        "SL_MULTIPLIER",
        "TP_MULTIPLIER",
        "ENTRY_THRESHOLD",
        "WATCHLIST_VOL_LIMIT",
        "WATCHLIST_GAINERS_LIMIT",
        "MAX_CONCURRENT_TASKS",
        "STARTUP_SCAN_LIMIT",
        "BINANCE_FUTURES_REST_BASE",
        "BINANCE_FUTURES_WS_BASE",
        "DASHBOARD_PUBLISH_TOKEN",
        "DASHBOARD_URL",
        "BOT_NAME",
        "AI_DISABLE_SHORT",
        "USE_CLOSED_LOOP_MODELS",

        "DASHBOARD_PUBLISH_URL",
        "DASHBOARD_ENVELOPE",
        "FUTURES_WS_ENABLED",
        "SPOT_WS_ENABLED",
        "FUTURES_WS_TIMEFRAMES",
        "SPOT_WS_TIMEFRAME",
        "FUTURES_SYMBOL_LIMIT",
        "SPOT_SYMBOL_LIMIT",
        "WS_CALLBACK_CONCURRENCY",
        "RELOAD_MODELS_COOLDOWN_SEC",
        "SNAPSHOT_ENABLED",
        "EQUITY_SNAPSHOT_EVERY_SEC",
        "SNAPSHOT_RETENTION_DAYS",
        "PAPER_TOURNAMENT_ENABLED",
        "DISABLE_AUDIT_LAB",
        "OI_LOG_ERRORS",
        "OI_ERROR_THROTTLE_SEC",
        "OI_SYMBOL_DELAY_SEC",
        "OI_CYCLE_DELAY_SEC",
        "OI_BASELINE_WINDOW_SEC",
    ]
    out = {}
    for k in keys:
        v = db.get_setting(k)
        if v is None:
            v = db.get_setting(k.lower())
        if v is not None:
            out[k] = v
    return out


@app.post("/api/env_secrets")
async def update_env_secrets(request: Request):
    """Update DB-backed secrets/config.

    We accept the same payload shape the UI historically sent to modify .env.
    """
    data = await request.json()
    for key, value in (data or {}).items():
        try:
            # Allow clearing fields
            v = "" if value is None else str(value)
            db.set_setting(str(key), v, source="dashboard_env", bump_version=False, audit=True)
        except Exception:
            pass

    # Ask the bot to reload config/models so changes can take effect (best-effort).
    try:
        db.add_command("RELOAD_MODELS", {"reason": "DB_CONFIG_UPDATED"})
    except Exception:
        pass

    return {"status": "ok", "msg": "Settings Updated (DB)!"}


# =========================================
# 📊 DATA & REPORTS API
# =========================================
@app.get("/api/stats")
async def get_stats():
    return db.get_stats()

@app.get("/api/advanced_stats")
async def get_advanced_stats():
    stats = db.get_stats()
    try:
        all_trades = db.get_all_closed_trades()
    except Exception:
        all_trades = []

    winning_trades = [t for t in all_trades if (t.get("pnl", t.get("realized_pnl", 0)) or 0) > 0]
    total_volume = sum((t.get("entry_price", 0) or 0) * (t.get("quantity", 0) or 0) for t in all_trades) * 2
    total_fees = total_volume * 0.00075

    gross_profits = sum((t.get("pnl", t.get("realized_pnl", 0)) or 0) for t in winning_trades)
    gross_losses = abs(sum((t.get("pnl", t.get("realized_pnl", 0)) or 0) for t in all_trades if (t.get("pnl", t.get("realized_pnl", 0)) or 0) < 0))

    if gross_losses > 0:
        profit_factor = gross_profits / gross_losses
    elif gross_profits > 0:
        profit_factor = 99.9
    else:
        profit_factor = 0

    equity_curve = [d.get("total_balance", 0) for d in db.get_equity_curve(limit=200)]
    max_drawdown = 0.0
    if equity_curve:
        peak = equity_curve[0]
        for val in equity_curve:
            if val > peak:
                peak = val
            dd = (peak - val) / peak if peak > 0 else 0
            if dd > max_drawdown:
                max_drawdown = dd

    return {
        "pnl": stats.get("pnl", 0),
        "win_rate": stats.get("win_rate", 0),
        "total_fees": round(total_fees, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown": round(max_drawdown * 100, 2),
        "avg_duration": "Dynamic",
    }

@app.get("/api/full_report")
async def get_full_report():
    report = db.get_detailed_report()
    totals = report.get("totals", {})
    trades = totals.get("total_trades") or 0
    wins = totals.get("total_wins") or 0
    report["metrics"] = {
        "win_rate": round((wins / trades * 100), 1) if trades > 0 else 0,
        "net_pnl": round(totals.get("gross_pnl") or 0, 2),
        "total_trades": trades,
    }
    return report


# ===============================
# 📦 BASELINE (Stage A)
# ===============================

def _pnl_of_trade(t: dict) -> float:
    try:
        return float(t.get("pnl", t.get("realized_pnl", 0)) or 0)
    except Exception:
        return 0.0


def _trade_strategy(t: dict) -> str:
    return str(t.get("strategy") or t.get("strategy_tag") or t.get("algo_mode") or t.get("mode") or "UNKNOWN").upper()


def _trade_market(t: dict) -> str:
    return str(t.get("market_type") or t.get("market") or "FUTURES").upper()


def _trade_closed_dt(t: dict):
    raw = t.get("closed_utc") or t.get("close_time_utc") or t.get("close_time") or t.get("updated_utc")
    if not raw:
        return None
    try:
        return _parse_iso_dt(str(raw))
    except Exception:
        return None


def _baseline_summary_from_trades(trades: list) -> dict:
    total = len(trades)
    pnls = [_pnl_of_trade(t) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    net_pnl = sum(pnls)

    win_rate = (len(wins) / total * 100.0) if total else 0.0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (abs(sum(losses)) / len(losses)) if losses else 0.0

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = 99.9
    else:
        profit_factor = 0.0

    loss_rate = 100.0 - win_rate
    expectancy = (win_rate/100.0) * avg_win - (loss_rate/100.0) * avg_loss

    return {
        "total_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate, 2),
        "gross_profit": round(gross_profit, 4),
        "gross_loss": round(gross_loss, 4),
        "net_pnl": round(net_pnl, 4),
        "profit_factor": round(profit_factor, 3),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "expectancy": round(expectancy, 5),
    }


@app.get("/api/baseline_summary")
async def api_baseline_summary(days: int = 30):
    """Baseline summary for the last N days (default 30).

    Stage A purpose: give us a stable measurement baseline BEFORE changing strategies.
    """
    try:
        trades = db.get_all_closed_trades() or []
    except Exception:
        trades = []

    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days or 30))
    recent = []
    for t in trades:
        dt = _trade_closed_dt(t)
        if dt is None:
            continue
        try:
            if dt >= cutoff:
                recent.append(t)
        except Exception:
            pass

    # Group by strategy
    by_strategy = {}
    for t in recent:
        key = _trade_strategy(t)
        by_strategy.setdefault(key, []).append(t)

    by_market = {}
    for t in recent:
        key = _trade_market(t)
        by_market.setdefault(key, []).append(t)

    return {
        "time_utc": _utc_iso_now(),
        "window_days": int(days or 30),
        "summary": _baseline_summary_from_trades(recent),
        "by_strategy": {k: _baseline_summary_from_trades(v) for k, v in sorted(by_strategy.items())},
        "by_market": {k: _baseline_summary_from_trades(v) for k, v in sorted(by_market.items())},
    }


@app.get("/api/baseline_export.csv")
async def api_baseline_export_csv(days: int = 30):
    """Download a CSV of closed trades for the last N days."""
    try:
        trades = db.get_all_closed_trades() or []
    except Exception:
        trades = []

    cutoff = datetime.now(timezone.utc) - timedelta(days=int(days or 30))
    rows = []
    for t in trades:
        dt = _trade_closed_dt(t)
        if dt is None or dt < cutoff:
            continue
        rows.append({
            "closed_utc": (dt.replace(microsecond=0).isoformat().replace("+00:00", "Z") if dt else ""),
            "symbol": t.get("symbol"),
            "side": t.get("side"),
            "market_type": _trade_market(t),
            "strategy": _trade_strategy(t),
            "entry_price": t.get("entry_price"),
            "exit_price": t.get("exit_price"),
            "qty": t.get("quantity") or t.get("qty"),
            "pnl": _pnl_of_trade(t),
        })

    import csv
    from io import StringIO

    buff = StringIO()
    writer = csv.DictWriter(buff, fieldnames=list(rows[0].keys()) if rows else [
        "closed_utc","symbol","side","market_type","strategy","entry_price","exit_price","qty","pnl"
    ])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

    csv_bytes = buff.getvalue().encode("utf-8")
    filename = f"baseline_trades_last_{int(days or 30)}d.csv"
    return Response(content=csv_bytes, media_type="text/csv", headers={
        "Content-Disposition": f"attachment; filename={filename}"
    })

@app.get("/api/learning_matrix")
async def get_learning_matrix():
    stats = db.get_recent_learning(days=7)

    # FALLBACK_FROM_TRADES: if learning_stats is empty, compute from recent closed trades
    if not stats:
        try:
            closed = db.get_all_closed_trades()
        except Exception:
            closed = []
        from datetime import datetime, timezone, timedelta
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
        buckets = {}
        for tr in closed:
            ms = tr.get('closed_at_ms') or 0
            try:
                ms = int(ms)
            except Exception:
                ms = 0
            if ms and ms < cutoff_ms:
                continue
            strat = tr.get('strategy_tag') or 'unknown'
            prof = tr.get('exit_profile') or 'default'
            key = (strat, prof)
            b = buckets.setdefault(key, {'strategy': strat, 'exit_profile': prof, 'trades': 0, 'wins': 0, 'pnl': 0.0})
            b['trades'] += 1
            pnl_v = float(tr.get('pnl') or 0.0)
            b['pnl'] += pnl_v
            if pnl_v > 0:
                b['wins'] += 1
        stats = list(buckets.values())

    matrix = []
    for row in stats:
        strategy = row.get("strategy")
        profile = row.get("exit_profile")
        trades = row.get("trades", 0) or 0
        wins = row.get("wins", 0) or 0
        pnl = row.get("pnl", 0) or 0
        win_rate = (wins / trades * 100) if trades > 0 else 0
        adj = db.get_threshold_adjustment(strategy, profile)
        matrix.append(
            {
                "strategy": f"{strategy} ({profile})",
                "trades": trades,
                "win_rate": round(win_rate, 1),
                "pnl": pnl,
                "ai_adj": adj,
            }
        )
    return matrix

@app.get("/api/equity_chart")
async def get_equity_chart():
    data = db.get_equity_curve(limit=100)
    labels = []
    equity = []
    for d in data:
        ts = (d.get("timestamp") or "").split("T")
        labels.append(ts[1][:5] if len(ts) > 1 else (d.get("timestamp") or ""))
        equity.append(d.get("total_balance", 0))
    return {"labels": labels, "equity": equity}

@app.get("/api/logs")
async def get_logs():
    return db.get_logs(limit=50)


# =========================================
# 🖥️ SYSTEM HEALTH API
# =========================================
@app.get("/api/system_health")
async def get_system_health():
    last_beat = db.get_setting("last_heartbeat")
    is_online = False
    last_seen_seconds = 999999

    if last_beat:
        try:
            last_time = datetime.fromisoformat(str(last_beat).replace("Z", "+00:00"))
            if last_time.tzinfo is None:
                last_time = last_time.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            diff = now - last_time.astimezone(timezone.utc)
            last_seen_seconds = diff.total_seconds()
            if last_seen_seconds < 60:
                is_online = True
        except Exception:
            pass

    db_path = getattr(db, "db_file", None) or os.path.join(PROJECT_ROOT, "bot_data.db")
    db_size_mb = 0.0
    try:
        if os.path.exists(db_path):
            db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    except Exception:
        pass

    return {
        "online": is_online,
        "last_seen_seconds": int(last_seen_seconds),
        "latency": int(float(db.get_setting("api_latency") or 0)),
        "cpu": float(db.get_setting("sys_cpu") or 0),
        "ram": float(db.get_setting("sys_ram") or 0),
        "db_size": f"{db_size_mb:.2f} MB",
        "error_count": len(db.get_logs(limit=50)),
        "kill_switch": str(db.get_setting("kill_switch") or "0"),
        "kill_switch_reason": str(db.get_setting("kill_switch_reason") or ""),
        "execmon_last_seen_seconds": _execmon_age_seconds(),
    }


# =========================================
# 🔁 POSITIONS & COMMANDS
# =========================================
def get_real_positions() -> Dict[str, Dict[str, float]]:
    try:
        mode = db.get_setting("mode")
        if str(mode).upper() != "LIVE":
            return {}
        client = Client(cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET, testnet=cfg.USE_TESTNET)
        positions = client.futures_position_information()
        real_data: Dict[str, Dict[str, float]] = {}
        for p in positions:
            amt = float(p.get("positionAmt", 0))
            if amt != 0:
                real_data[p["symbol"]] = {
                    "entry": float(p.get("entryPrice", 0)),
                    "qty": abs(amt),
                    "pnl": float(p.get("unRealizedProfit", 0)),
                    "mark": float(p.get("markPrice", 0)),
                    "direction": "LONG" if amt > 0 else "SHORT",
                }
        return real_data
    except Exception as e:
        print(f"⚠️ Binance API Error: {e}")
        return {}

@app.get("/api/positions")
async def get_positions():
    db_trades = db.get_all_active_trades()
    real_pos = get_real_positions()
    ps_map = _positions_status_map()
    final_list = []

    for t in db_trades:
        sym = t.get("symbol")
        if real_pos and sym in real_pos:
            r = real_pos[sym]
            t["unrealized_pnl"] = r["pnl"]
            t["current_price"] = r["mark"]
            t["verified"] = True
            try:
                t["direction"] = r.get("direction")
            except Exception:
                t["direction"] = None
            # Infer direction from signal text only if not already provided by exchange/monitor
            if not t.get("direction") or str(t.get("direction")).upper() in {"", "UNKNOWN", "NONE"}:
                sig_u = str(t.get("signal") or "").upper()
                side = _normalize_side(sig_u)
                if side == "BUY":
                    t["direction"] = "LONG"
                elif side == "SELL":
                    t["direction"] = "SHORT"
                else:
                    t["direction"] = "UNKNOWN"
        elif ps_map and sym and str(sym).upper() in ps_map:
            p = ps_map[str(sym).upper()]
            try:
                t["current_price"] = float(p.get("markPrice") or p.get("mark") or p.get("mark_price") or 0.0)
            except Exception:
                pass
            try:
                t["unrealized_pnl"] = float(p.get("unrealizedProfit") or p.get("unRealizedProfit") or p.get("pnl") or 0.0)
            except Exception:
                pass
            try:
                amt = float(p.get("positionAmt") or 0.0)
                if amt > 0:
                    t["direction"] = "LONG"
                elif amt < 0:
                    t["direction"] = "SHORT"
            except Exception:
                pass
            t["verified"] = True
            t["verified_source"] = "execution_monitor"

        elif real_pos and sym not in real_pos and str(db.get_setting("mode")).upper() == "LIVE":
            # close stale trade
            try:
                db.close_trade(sym, "SYNC_FIX_DASHBOARD", t.get("entry_price", 0))
            except Exception:
                pass
            continue
        else:
            # Not LIVE verified (paper/test). Compute best-effort unrealized PnL from public prices.
            t["verified"] = False
            try:
                sym_u = str(t.get("symbol") or "").upper()
                entry = float(t.get("entry_price") or 0.0)
                qty = float(t.get("quantity") or 0.0)
                sig = str(t.get("signal") or "").upper()
                mt = str(t.get("market_type") or "futures").lower()

                cur = _public_price(sym_u, mt)
                if cur > 0:
                    t["current_price"] = cur
                    side_sig = _normalize_side(sig)
                    is_long = side_sig == "BUY"
                    is_short = side_sig == "SELL"
                    t["direction"] = "LONG" if is_long else ("SHORT" if is_short else "UNKNOWN")
                    pnl = 0.0
                    if entry > 0 and qty > 0:
                        if is_long and not is_short:
                            pnl = (cur - entry) * qty
                        elif is_short and not is_long:
                            pnl = (entry - cur) * qty
                    t["unrealized_pnl"] = pnl
            except Exception:
                pass

        # Infer direction from signal text only if not already provided by exchange/monitor
        if not t.get("direction") or str(t.get("direction")).upper() in {"", "UNKNOWN", "NONE"}:
            sig_u = str(t.get("signal") or "").upper()
            side = _normalize_side(sig_u)
            if side == "BUY":
                t["direction"] = "LONG"
            elif side == "SELL":
                t["direction"] = "SHORT"
            else:
                t["direction"] = "UNKNOWN"

        # Backward-compat: ensure `pnl` reflects OPEN unrealized PnL (some UIs read `pnl` only)
        try:
            if str(t.get("status", "")).upper() == "OPEN":
                up = t.get("unrealized_pnl")
                if up is not None:
                    t["pnl"] = float(up)
        except Exception:
            pass

        # Backward-compat: ensure `ai_confidence` is populated (some UIs expect it)
        try:
            if t.get("ai_confidence") is None and t.get("confidence") is not None:
                t["ai_confidence"] = float(t["confidence"])
            elif t.get("confidence") is None and t.get("ai_confidence") is not None:
                t["confidence"] = float(t["ai_confidence"])
        except Exception:
            pass

        final_list.append(t)

    return final_list

@app.post("/api/command")
async def send_command(request: Request):
    data = await request.json()
    db.add_command(data.get("cmd"), data.get("params", {}))
    return {"status": "ok"}


# =========================================
# 👁️ INSPECTOR
# =========================================
@app.post("/api/inspect")
async def inspect_coin(request: Request):
    try:
        data = await request.json()
        symbol = str(data.get("symbol", "")).upper().strip()
        if not symbol:
            return {"status": "error", "msg": "Missing symbol"}

        df = await asyncio.to_thread(fetch_klines, symbol, "5m", 500, "futures")
        if df is None or df.empty:
            return {"status": "error", "msg": "Could not fetch data (Invalid Symbol?)"}

        price = float(_safe_last(df["close"], default=0.0))
        atr_v = float(_safe_last(atr(df, 14), default=0.0))
        adx_v = float(_safe_last(adx(df, 14), default=0.0))
        rsi_v = float(_safe_last(rsi(df, 14), default=0.0))
        vol_spike_v = int(_safe_last(volume_spike(df), default=0))
        atr_pct = (atr_v / price) if price > 0 else 0.0

        cfg_atr = float(db.get_setting("gate_atr_min_pct") or 0.003)
        cfg_adx = float(db.get_setting("gate_adx_min") or 10)
        cfg_rsi = float(db.get_setting("rsi_max_buy") or 70)

        gate_status = "PASS"
        gate_reason = "All conditions met"
        if atr_pct < cfg_atr:
            gate_status = "BLOCK"
            gate_reason = f"Volatility Too Low ({atr_pct:.4f} < {cfg_atr})"
        elif adx_v < cfg_adx and not vol_spike_v:
            gate_status = "WAIT"
            gate_reason = f"Trend Too Weak (ADX {adx_v:.1f} < {cfg_adx})"
        elif rsi_v > cfg_rsi:
            gate_status = "BLOCK"
            gate_reason = f"RSI Overbought ({rsi_v:.1f} > {cfg_rsi})"

        return {
            "status": "ok",
            "symbol": symbol,
            "price": price,
            "indicators": {
                "rsi": round(rsi_v, 1),
                "adx": round(adx_v, 1),
                "atr_pct": round(atr_pct, 5),
                "volume_spike": bool(vol_spike_v),
            },
            "settings": {"max_rsi": cfg_rsi, "min_adx": cfg_adx, "min_atr": cfg_atr},
            "decision": {"gate": gate_status, "reason": gate_reason},
        }
    except Exception as e:
        return {"status": "error", "msg": str(e)}


# =========================================
# 🕰️ HISTORICAL SNAPSHOT (UTC)
# =========================================
def _parse_utc_minute(s: str) -> datetime:
    """Parse UTC minute string.
    Accepts: 'YYYY-MM-DD HH:MM' or ISO like 'YYYY-MM-DDTHH:MM' or '...Z'/'...+00:00'
    Returns timezone-aware UTC datetime.
    """
    if not s:
        raise ValueError("Missing datetime")
    s = str(s).strip()

    if "T" in s and " " not in s:
        s = s.replace("T", " ")

    s16 = s[:16] if len(s) >= 16 else s
    try:
        dt = datetime.strptime(s16, "%Y-%m-%d %H:%M")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)

@app.post("/api/snapshot")
async def snapshot_api(request: Request):
    """Return indicators + pump score for a symbol at a specific UTC time or across a range."""
    data = await request.json()
    symbol = str(data.get("symbol") or "").upper().strip()
    if not symbol:
        return {"status": "error", "msg": "Missing symbol"}

    market_type = str(data.get("market_type") or "futures").lower()
    interval = str(data.get("interval") or "5m").lower()
    mode = str(data.get("mode") or "at").lower()  # 'at' or 'range'

    try:
        if mode == "range":
            dt_from = _parse_utc_minute(data.get("from_utc"))
            dt_to = _parse_utc_minute(data.get("to_utc"))
            start_ms = int(dt_from.timestamp() * 1000)
            end_ms = int(dt_to.timestamp() * 1000)
            df = await asyncio.to_thread(fetch_klines_range, symbol, interval, start_ms, end_ms, 1500, market_type)
        else:
            dt_at = _parse_utc_minute(data.get("at_utc"))
            end_ms = int(dt_at.timestamp() * 1000)
            df = await asyncio.to_thread(fetch_klines_window, symbol, interval, end_ms, 500, market_type)

        if df is None or df.empty:
            return {"status": "error", "msg": "No data returned (symbol/market/interval?)"}

        price = float(_safe_last(df["close"], default=0.0))
        atr_v = float(_safe_last(atr(df, 14), default=0.0))
        adx_v = float(_safe_last(adx(df, 14), default=0.0))
        rsi_v = float(_safe_last(rsi(df, 14), default=0.0))
        vol_spk = int(_safe_last(volume_spike(df), default=0))
        atr_pct = (atr_v / price) if price > 0 else 0.0

        try:
            pump_score = int(detect_pre_pump(df, price))
        except Exception:
            pump_score = 0

        cfg_atr = float(db.get_setting("gate_atr_min_pct") or 0.003)
        cfg_adx = float(db.get_setting("gate_adx_min") or 10)
        cfg_rsi = float(db.get_setting("rsi_max_buy") or 70)

        gate_status = "PASS"
        reasons = []
        if atr_pct < cfg_atr:
            gate_status = "BLOCK"
            reasons.append(f"ATR% too low ({atr_pct:.4f} < {cfg_atr})")
        if adx_v < cfg_adx and not vol_spk:
            gate_status = "WAIT" if gate_status == "PASS" else gate_status
            reasons.append(f"ADX too low ({adx_v:.1f} < {cfg_adx}) and no volume spike")
        if rsi_v > cfg_rsi:
            gate_status = "BLOCK"
            reasons.append(f"RSI too high ({rsi_v:.1f} > {cfg_rsi})")

        payload: Dict[str, Any] = {
            "status": "ok",
            "symbol": symbol,
            "market_type": market_type,
            "interval": interval,
            "last_candle_utc": df.index[-1].to_pydatetime().astimezone(timezone.utc).isoformat(),
            "price": price,
            "indicators": {
                "rsi": round(rsi_v, 2),
                "adx": round(adx_v, 2),
                "atr_pct": round(atr_pct, 6),
                "volume_spike": bool(vol_spk),
                "pump_score": pump_score,
            },
            "settings": {"max_rsi": cfg_rsi, "min_adx": cfg_adx, "min_atr": cfg_atr},
            "decision": {"gate": gate_status, "reasons": reasons or ["All conditions met"]},
        }

        if mode == "range":
            tail = df.tail(200).copy()
            series = []
            rsi_s = rsi(tail, 14)
            adx_s = adx(tail, 14)
            atr_s = atr(tail, 14)
            vs_s = volume_spike(tail)
            for i in range(len(tail)):
                ts = tail.index[i].to_pydatetime().astimezone(timezone.utc).isoformat()
                c = float(tail["close"].iloc[i])
                av = float(atr_s.iloc[i])
                series.append(
                    {
                        "ts": ts,
                        "close": c,
                        "rsi": float(rsi_s.iloc[i]),
                        "adx": float(adx_s.iloc[i]),
                        "atr_pct": (av / c) if c > 0 else 0.0,
                        "vol_spike": int(vs_s.iloc[i]),
                    }
                )
            payload["series"] = series

        return payload

    except Exception as e:
        return {"status": "error", "msg": str(e)}


# =========================================================
# Protection audit API
# =========================================================
@app.get("/api/protection/audit")
async def api_protection_audit():
    """Return the latest protection audit produced by execution_monitor.

    NOTE: This is a lightweight alias so you don't have to parse /api/doctor.
    """
    try:
        settings = db.get_settings() or {}
        protection_audit_status = _safe_json_loads(settings.get("protection_audit_status")) or {}
        return {
            "ok": True,
            "updated_utc": (protection_audit_status or {}).get("updated_utc"),
            "protection_audit": protection_audit_status,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/protection/force")
async def api_protection_force():
    """Ask execution_monitor to run a protection audit ASAP.

    This sets a DB setting token that execution_monitor watches.
    """
    try:
        ts = int(time.time() * 1000)
        db.set_setting("protection_audit_force", str(ts), source="dashboard", bump_version=False, audit=False)
        return {"ok": True, "forced_at_ms": ts}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/protection/orphans/cleanup")
async def api_protection_orphans_cleanup():
    """Ask execution_monitor to cleanup orphan protective orders ASAP.

    This sets a DB token that execution_monitor watches inside the protection audit loop.
    """
    try:
        ts = int(time.time() * 1000)
        db.set_setting("orphan_orders_cleanup_force", str(ts), source="dashboard", bump_version=False, audit=False)
        return {"ok": True, "forced_at_ms": ts}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =========================================
# 📡 WEBSOCKET: live broadcast
# =========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

@app.websocket("/ws/signals")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# =========================================
# ✅ SIGNAL INBOX (approval workflow)
# =========================================
def _ensure_signal_inbox_table():
    with db.lock:
        cur = db.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT,
                received_at_ms INTEGER,
                source TEXT DEFAULT 'bot',
                status TEXT DEFAULT 'RECEIVED',
                symbol TEXT,
                timeframe TEXT,
                strategy TEXT,
                side TEXT,
                confidence REAL,
                score REAL,
                payload TEXT,
                approved_at_ms INTEGER,
                rejected_at_ms INTEGER,
                executed_at_ms INTEGER,
                note TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_status ON signal_inbox(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_received ON signal_inbox(received_at_ms)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_symbol ON signal_inbox(symbol)")
        db.conn.commit()

def _extract_field(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None and str(d[k]).strip() != "":
            return d[k]
    return default

def _store_signal_inbox(data: dict) -> int:
    _ensure_signal_inbox_table()
    received_at = _utc_iso_now()
    received_ms = _ms_now()

    status = str(_extract_field(data, "status", default="RECEIVED")).upper()
    if (data.get("execution_allowed") is False) and status in ("RECEIVED", "OK", "PUBLISHED", "SIGNAL"):
        status = "PENDING_APPROVAL"
    if status not in ("RECEIVED", "PENDING_APPROVAL", "APPROVED", "REJECTED", "EXECUTED"):
        status = "RECEIVED"

    symbol = _extract_field(data, "symbol", "pair", "s")
    strategy = _extract_field(data, "strategy", "strategy_name")
    side = _extract_field(data, "side", "signal", "direction")
    timeframe = _extract_field(data, "timeframe", "tf")
    # Guard: ignore totally empty events
    if not symbol and not strategy and not side and status == "RECEIVED":
        return 0
    confidence = _extract_field(data, "confidence", "conf", default=None)
    score = _extract_field(data, "score", "pump_score", default=None)

    payload = json.dumps(data, ensure_ascii=False)

    with db.lock:
        cur = db.conn.cursor()
        cur.execute(
            """
            INSERT INTO signal_inbox
            (received_at, received_at_ms, source, status, symbol, timeframe, strategy, side, confidence, score, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                received_at,
                received_ms,
                "bot",
                status,
                str(symbol) if symbol else None,
                str(timeframe) if timeframe else None,
                str(strategy) if strategy else None,
                str(side) if side else None,
                float(confidence) if confidence is not None else None,
                float(score) if score is not None else None,
                payload,
            ),
        )
        db.conn.commit()
        return int(cur.lastrowid)

def _fetch_signals(status: Optional[str] = None, limit: int = 50):
    _ensure_signal_inbox_table()
    limit = max(1, min(int(limit or 50), 500))
    with db.lock:
        cur = db.conn.cursor()
        if status:
            cur.execute(
                "SELECT * FROM signal_inbox WHERE status=? ORDER BY received_at_ms DESC LIMIT ?",
                (str(status).upper(), limit),
            )
        else:
            cur.execute("SELECT * FROM signal_inbox ORDER BY received_at_ms DESC LIMIT ?", (limit,))
        rows = cur.fetchall()

    out = []
    for r in rows:
        rr = dict(r)
        try:
            rr["payload"] = json.loads(rr.get("payload") or "{}")
        except Exception:
            rr["payload"] = {}
        out.append(rr)
    return out

def _get_signal_by_id(sid: int):
    _ensure_signal_inbox_table()
    with db.lock:
        cur = db.conn.cursor()
        cur.execute("SELECT * FROM signal_inbox WHERE id=?", (int(sid),))
        r = cur.fetchone()
    if not r:
        return None
    rr = dict(r)
    try:
        rr["payload"] = json.loads(rr.get("payload") or "{}")
    except Exception:
        rr["payload"] = {}
    return rr

def _update_signal_status(sid: int, status: str, note: Optional[str] = None) -> bool:
    _ensure_signal_inbox_table()
    sid = int(sid)
    st = str(status).upper()
    ts = _ms_now()

    fields = ["status=?"]
    vals: List[Any] = [st]

    if note is not None:
        fields.append("note=?")
        vals.append(str(note))
    if st == "APPROVED":
        fields.append("approved_at_ms=?"); vals.append(ts)
    if st == "REJECTED":
        fields.append("rejected_at_ms=?"); vals.append(ts)
    if st == "EXECUTED":
        fields.append("executed_at_ms=?"); vals.append(ts)

    vals.append(sid)

    with db.lock:
        cur = db.conn.cursor()
        cur.execute(f"UPDATE signal_inbox SET {', '.join(fields)} WHERE id=?", tuple(vals))
        db.conn.commit()
    return True

@app.get("/api/signals")
async def api_signals(status: str = None, limit: int = 50):
    return _fetch_signals(status=status, limit=limit)

@app.get("/api/signals/{signal_id}")
async def api_signal_one(signal_id: int):
    r = _get_signal_by_id(signal_id)
    if not r:
        raise HTTPException(status_code=404, detail="Signal not found")
    return r

@app.post("/api/signals/approve")
async def api_signal_approve(request: Request):
    """Approve a signal (typically from signal_inbox) and enqueue EXECUTE_SIGNAL.
    Safe behavior:
    - Merge DB row columns (symbol/side/strategy/timeframe/score/confidence) into payload if missing
    - Always attach inbox_id/_dashboard_signal_id for bot-side correlation
    - Reject approval if we still can't determine symbol or side
    """
    data = await request.json()
    sid = data.get("id")
    note = str(data.get("note") or "")
    req_payload = data.get("payload")

    rec = None
    if sid:
        rec = _get_signal_by_id(int(sid))
        if not rec:
            raise HTTPException(status_code=404, detail="Signal not found")

    # Start from record payload (may be empty) then overlay request payload (if provided)
    merged: dict = {}
    if rec and isinstance(rec.get("payload"), dict):
        merged.update(rec.get("payload") or {})
    if isinstance(req_payload, dict):
        merged.update(req_payload or {})

    # Fill missing essentials from the DB row columns
    if rec:
        for k, v in {
            "symbol": rec.get("symbol"),
            "side": rec.get("side"),
            "strategy": rec.get("strategy"),
            "timeframe": rec.get("timeframe"),
            "confidence": rec.get("confidence"),
            "score": rec.get("score"),
        }.items():
            if v is None:
                continue
            if merged.get(k) in (None, ""):
                merged[k] = v

    # Normalize side for safety (accept LONG/SHORT/BUY/SELL)
    side_raw = str(merged.get("side") or merged.get("signal") or merged.get("direction") or "").strip().upper()
    if side_raw in ("BUY", "LONG"):
        merged["side"] = "LONG"
    elif side_raw in ("SELL", "SHORT"):
        merged["side"] = "SHORT"
    elif side_raw:
        # keep but will be validated below
        merged["side"] = side_raw

    # Attach correlation ids
    if sid:
        merged["inbox_id"] = int(sid)
        merged["_dashboard_signal_id"] = int(sid)
    merged["_approved_by"] = "dashboard"
    merged["_approved_at"] = _utc_iso_now()

    # Validate essentials
    symbol = str(merged.get("symbol") or "").strip().upper()
    side = str(merged.get("side") or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="Missing symbol")
    if side not in ("LONG", "SHORT", "BUY", "SELL"):
        raise HTTPException(status_code=400, detail="Missing/invalid side")

    merged["symbol"] = symbol

    db.add_command("EXECUTE_SIGNAL", merged)

    if sid:
        _update_signal_status(int(sid), "APPROVED", note=note)

    return {"status": "ok"}

@app.post("/api/signals/reject")
async def api_signal_reject(request: Request):
    data = await request.json()
    sid = data.get("id")
    reason = str(data.get("reason") or "Rejected by dashboard")
    note = str(data.get("note") or "")

    if not sid:
        raise HTTPException(status_code=400, detail="Missing id")

    rec = _get_signal_by_id(int(sid))
    if not rec:
        raise HTTPException(status_code=404, detail="Signal not found")

    payload = rec.get("payload") or {}
    symbol = _extract_field(payload, "symbol", "pair", "s", default=rec.get("symbol"))

    try:
        db.log_rejection(str(symbol) if symbol else "UNKNOWN", reason, 0, 0)
    except Exception:
        pass

    _update_signal_status(int(sid), "REJECTED", note=note or reason)
    return {"status": "ok"}


# =========================================
# 📤 PUBLISH (Public, optional token)
# =========================================
@app.post("/publish")
async def publish_signal(request: Request):
    # Optional shared-secret protection for bot -> dashboard publishing
    required_token = str(getattr(cfg, "DASHBOARD_PUBLISH_TOKEN", "") or (db.get_setting("DASHBOARD_PUBLISH_TOKEN") or ""))
    if required_token:
        provided = request.headers.get("x-dashboard-token") or request.query_params.get("token")
        if provided != required_token:
            raise HTTPException(status_code=401, detail="Unauthorized publisher")

    data = await request.json()
    if not isinstance(data, dict):
        data = {"raw": data}

    # Avoid polluting signal_inbox with non-signal events (e.g., DIAG/HEARTBEAT)
    symbol = _extract_field(data, "symbol", "pair", "s")
    side = _extract_field(data, "side", "signal", "direction")
    strategy = _extract_field(data, "strategy", "strategy_name")
    status = str(_extract_field(data, "status", default="")).upper()

    should_store = bool(symbol) or (status in ("PENDING_APPROVAL", "APPROVED", "REJECTED", "EXECUTED")) or bool(strategy) or bool(side)
    inbox_id = 0
    if should_store:
        inbox_id = _store_signal_inbox(data)

    data["_dashboard_inbox_id"] = inbox_id

    await manager.broadcast(data)
    return {"status": "published", "id": inbox_id}


# =========================================
# 📈 ANALYTICS API (for analytics.html)
# =========================================
@app.get("/api/analytics")
async def api_analytics():
    """
    Analytics endpoint used by analytics.html.

    Notes:
    - Some bots close trades without setting closed_at. To avoid empty analytics, we treat a trade as
      closed if status != 'OPEN' OR closed_at is set, and we use COALESCE(closed_at, timestamp) for time.
    """
    errors: Dict[str, str] = {}
    conn = getattr(db, "conn", None)
    if conn is None:
        return {
            "rejections": [],
            "golden_hours": [],
            "calibration": [],
            "recent_trades": [],
            "generated_at": _utc_iso_now(),
            "errors": {"db": "Database connection not available"},
        }

    def _table_cols(table: str):
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return {r[1] for r in rows}
        except Exception:
            return set()

    trades_cols = _table_cols("trades")
    rej_cols = _table_cols("rejections")

    now_utc = datetime.utcnow()
    cutoff_14d = (now_utc - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cutoff_30d = (now_utc - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build a robust time expression for SQLite (string timestamps).
    time_expr = None
    if "closed_at" in trades_cols and "timestamp" in trades_cols:
        # If closed_at is missing/empty, fall back to timestamp.
        time_expr = "COALESCE(NULLIF(closed_at,''), timestamp)"
    elif "closed_at" in trades_cols:
        time_expr = "NULLIF(closed_at,'')"
    elif "timestamp" in trades_cols:
        time_expr = "timestamp"

    # Trade is "closed" if status != OPEN, or closed_at is set (for older schemas).
    where_closed_parts = []
    if "status" in trades_cols:
        where_closed_parts.append("status != 'OPEN'")
    if "closed_at" in trades_cols:
        where_closed_parts.append("(closed_at IS NOT NULL AND closed_at != '')")
    where_closed_base = "(" + " OR ".join(where_closed_parts) + ")" if where_closed_parts else "1=1"

    # 1) Rejections
    rejections = []
    if "reason" in rej_cols:
        try:
            if "timestamp" in rej_cols:
                rows = conn.execute(
                    """
                    SELECT reason, COUNT(*) AS count
                    FROM rejections
                    WHERE timestamp >= ?
                    GROUP BY reason
                    ORDER BY count DESC
                    LIMIT 10
                    """,
                    (cutoff_14d,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT reason, COUNT(*) AS count
                    FROM rejections
                    GROUP BY reason
                    ORDER BY count DESC
                    LIMIT 10
                    """
                ).fetchall()
            rejections = [{"reason": r["reason"], "count": int(r["count"] or 0)} for r in rows]
        except Exception as e:
            errors["rejections"] = str(e)
    else:
        errors["rejections"] = "rejections table/columns not available"

    # 2) Recent closed trades
    recent_trades = []
    if {"symbol", "signal"}.issubset(trades_cols):
        try:
            conf_expr = "0"
            if "ai_confidence" in trades_cols and "confidence" in trades_cols:
                conf_expr = "COALESCE(ai_confidence, confidence, 0)"
            elif "ai_confidence" in trades_cols:
                conf_expr = "COALESCE(ai_confidence, 0)"
            elif "confidence" in trades_cols:
                conf_expr = "COALESCE(confidence, 0)"

            # Use time_expr when possible; otherwise order by id.
            order_expr = time_expr if time_expr else ("timestamp" if "timestamp" in trades_cols else "id")

            where_parts = [where_closed_base]
            params = []
            if time_expr:
                where_parts.append(f"{time_expr} >= ?")
                params.append(cutoff_30d)

            where_sql = " AND ".join(where_parts) if where_parts else "1=1"

            q = f"""
                SELECT
                    {order_expr} AS closed_at,
                    symbol,
                    signal,
                    entry_price,
                    close_price,
                    pnl,
                    {conf_expr} AS confidence,
                    COALESCE(close_reason, 'CLOSED') AS close_reason
                FROM trades
                WHERE {where_sql}
                ORDER BY {order_expr} DESC
                LIMIT 50
            """
            rows = conn.execute(q, tuple(params)).fetchall()
            recent_trades = [
                {
                    "closed_at": r["closed_at"],
                    "symbol": r["symbol"],
                    "signal": r["signal"],
                    "entry_price": float(r["entry_price"] or 0),
                    "close_price": float(r["close_price"] or 0),
                    "pnl": float(r["pnl"] or 0),
                    "confidence": float(r["confidence"] or 0),
                    "close_reason": r["close_reason"] or "CLOSED",
                }
                for r in rows
            ]
        except Exception as e:
            errors["recent_trades"] = str(e)
    else:
        errors["recent_trades"] = "trades table/columns not available"

    # 3) Golden hours
    golden_hours = []
    try:
        if time_expr and "pnl" in trades_cols:
            where_sql = f"{where_closed_base} AND {time_expr} >= ?"
            rows = conn.execute(
                f"""
                SELECT
                    CAST(substr({time_expr}, 12, 2) AS INTEGER) AS hour,
                    SUM(COALESCE(pnl, 0)) AS total_pnl,
                    COUNT(*) AS trades
                FROM trades
                WHERE {where_sql}
                GROUP BY hour
                ORDER BY hour ASC
                """,
                (cutoff_30d,),
            ).fetchall()

            golden_hours = [
                {"hour": str(int(r["hour"])).zfill(2), "total_pnl": float(r["total_pnl"] or 0), "trades": int(r["trades"] or 0)}
                for r in rows
                if r["hour"] is not None
            ]
        else:
            errors["golden_hours"] = "closed_at/timestamp not available for golden hours"
    except Exception as e:
        errors["golden_hours"] = str(e)

    # 4) Calibration buckets
    calibration = []
    try:
        conf_col = "ai_confidence" if "ai_confidence" in trades_cols else ("confidence" if "confidence" in trades_cols else None)
        if conf_col and "pnl" in trades_cols and time_expr:
            where_sql = f"{where_closed_base} AND {time_expr} >= ?"
            rows = conn.execute(
                f"SELECT COALESCE({conf_col}, 0) AS conf, COALESCE(pnl, 0) AS pnl FROM trades WHERE {where_sql}",
                (cutoff_30d,),
            ).fetchall()

            # Bucket: [0-20,20-40,...,80-100]
            buckets = {f"{lo}-{lo+20}": {"bucket": f"{lo}-{lo+20}", "avg_pnl": 0.0, "trades": 0} for lo in range(0, 100, 20)}
            sums = {k: 0.0 for k in buckets}

            for r in rows:
                conf = float(r["conf"] or 0)
                pnl = float(r["pnl"] or 0)
                conf_pct = max(0.0, min(100.0, conf))
                lo = int(conf_pct // 20) * 20
                if lo >= 100:
                    lo = 80
                key = f"{lo}-{lo+20}"
                buckets[key]["trades"] += 1
                sums[key] += pnl

            for k, v in buckets.items():
                n = v["trades"]
                v["avg_pnl"] = (sums[k] / n) if n > 0 else 0.0

            calibration = list(buckets.values())
        else:
            errors["calibration"] = "confidence/pnl/time columns not available"
    except Exception as e:
        errors["calibration"] = str(e)

    return {
        "rejections": rejections,
        "golden_hours": golden_hours,
        "calibration": calibration,
        "recent_trades": recent_trades,
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "errors": errors,
    }


# =========================================
# 🧪 AUDIT API
# =========================================
@app.get("/api/audit")
async def get_audit(symbol: str, interval: str = "5m", start: str = None, end: str = None):
    if lab is None:
        return {"status": "error", "msg": "AuditLab not available"}
    data = lab.run_historical_audit(symbol.upper(), interval, start_str=start, end_str=end)
    return data


# =========================================
# 🏆 PAPER ARENA
# =========================================
@app.get("/api/paper/leaderboard")
async def paper_leaderboard(days: int = 7, horizon: str = "1h", market: str = "futures"):
    try:
        days = int(days)
    except Exception:
        days = 7
    horizon = horizon if horizon in ("15m", "1h", "4h") else "1h"
    market = market if market in ("futures", "spot") else "futures"
    items = db.get_paper_leaderboard(days=days, horizon=horizon, market_type=market)
    return {"status": "ok", "items": items}

@app.post("/api/paper/recommendations/generate")
async def paper_generate_recs(request: Request):
    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}

    days = int(body.get("days", 7))
    horizon = body.get("horizon", "1h")
    market = body.get("market", "futures")
    horizon = horizon if horizon in ("15m", "1h", "4h") else "1h"
    market = market if market in ("futures", "spot") else "futures"

    items = db.get_paper_leaderboard(days=days, horizon=horizon, market_type=market)
    date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    recs = []
    sq = next((x for x in items if x.get("strategy_tag") == "squeeze_long"), None)
    if sq and (sq.get("trades", 0) or 0) >= 15:
        cur = str(db.get_setting("fut_pump_enabled") or "FALSE").upper()
        win_rate = float(sq.get("win_rate") or 0)
        if win_rate >= 60 and cur != "TRUE":
            recs.append({
                "key": "fut_pump_enabled",
                "current_value": cur,
                "suggested_value": "TRUE",
                "category": "strategy",
                "reason": f"squeeze_long win_rate={win_rate}% over {sq['trades']} trades ({horizon})",
                "score": win_rate
            })
        if win_rate <= 45 and cur == "TRUE":
            recs.append({
                "key": "fut_pump_enabled",
                "current_value": cur,
                "suggested_value": "FALSE",
                "category": "strategy",
                "reason": f"squeeze_long underperforming win_rate={win_rate}% ({horizon})",
                "score": 100 - win_rate
            })

    ps = next((x for x in items if x.get("strategy_tag") == "pump_spot"), None)
    if ps and (ps.get("trades", 0) or 0) >= 30:
        cur = int(float(db.get_setting("pump_score_min") or 75))
        win_rate = float(ps.get("win_rate") or 0)
        if win_rate < 45:
            recs.append({
                "key": "pump_score_min",
                "current_value": str(cur),
                "suggested_value": str(min(95, cur + 5)),
                "category": "tuning",
                "reason": f"pump_spot win_rate low={win_rate}% → make filter stricter",
                "score": 60
            })
        elif win_rate > 60:
            recs.append({
                "key": "pump_score_min",
                "current_value": str(cur),
                "suggested_value": str(max(50, cur - 5)),
                "category": "tuning",
                "reason": f"pump_spot win_rate high={win_rate}% → allow more candidates",
                "score": 55
            })

    db.upsert_recommendations(date_utc, recs)
    return {"status": "ok", "date_utc": date_utc, "count": len(recs)}


@app.post("/api/paper/strategy/generate")
async def paper_generate_strategy_recs(request: Request):
    """Generate learning-based *strategy* recommendations from CLOSED trades and store them in DB.

    This runs the learning helper scripts (link -> sync -> generate) using the same Python env as the dashboard.
    """
    data = await request.json()
    n = int(data.get("n") or 120)
    top = int(data.get("top") or 10)
    lookback_hours = int(data.get("lookback_hours") or 720)  # 30 days default
    window_minutes = int(data.get("window_minutes") or 720)  # link window

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.abspath(cfg.DB_FILE)

    steps = []

    def _run(args):
        r = subprocess.run(
            [sys.executable] + args,
            cwd=project_root,
            capture_output=True,
            text=True
        )
        steps.append({
            "cmd": " ".join([sys.executable] + args),
            "returncode": r.returncode,
            "stdout": (r.stdout or "")[-4000:],
            "stderr": (r.stderr or "")[-4000:],
        })
        if r.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Learning step failed: {' '.join(args)}")
        return r

    # 1) Link ai_samples to trades (if needed)
    _run(["link_ai_samples_to_trades.py", "--db", db_path, "--window-minutes", str(window_minutes), "--apply"])
    # 2) Sync ai_samples outcomes + (optionally) recompute return_pct
    _run(["sync_ai_samples_from_trades.py", "--db", db_path, "--lookback-hours", str(lookback_hours), "--recompute-return", "--apply"])
    # 3) Generate strategy recommendations from trades
    _run(["generate_recommendations_from_trades.py", "--db", db_path, "--n", str(n), "--top", str(top), "--apply", "--clear"])

    # Return today's recommendations list (includes category='strategy')
    date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items = db.list_recommendations(date_utc)
    return {"status": "ok", "date_utc": date_utc, "steps": steps, "items": [dict(x) for x in items]}

@app.get("/api/paper/recommendations")
async def paper_list_recs(date_utc: str = ""):
    if not date_utc:
        date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    items = db.list_recommendations(date_utc)
    return {"status": "ok", "date_utc": date_utc, "items": [dict(x) for x in items]}

@app.post("/api/paper/recommendations/approve")
async def paper_approve_rec(request: Request):
    data = await request.json()
    rid = int(data.get("id", 0))

    # Read recommendation row so we can special-case strategy approvals
    row = None
    try:
        with db.lock:
            db.cursor.execute("SELECT * FROM recommendations WHERE id=?", (rid,))
            row = db.cursor.fetchone()
    except Exception:
        row = None

    ok, msg = db.approve_recommendation(rid)

    # If this is a *strategy* recommendation (generated from learning), apply it as an override
    # so new trades use the approved exit profile (and tag/bias as metadata).
    if row:
        r = dict(row)
        if str(r.get("category") or "").lower() == "strategy":
            try:
                # Enable override + store fields
                db.set_setting("strategy_override_enabled", "TRUE")
                if r.get("exit_profile"):
                    db.set_setting("strategy_override_exit_profile", str(r.get("exit_profile")))
                if r.get("strategy_tag"):
                    db.set_setting("strategy_override_strategy_tag", str(r.get("strategy_tag")))
                if r.get("bias"):
                    db.set_setting("strategy_override_bias", str(r.get("bias")))
                if r.get("market_type"):
                    db.set_setting("strategy_override_market_type", str(r.get("market_type")))

                # Ask bot to reload runtime config (safe even if already reloading)
                db.add_command("RELOAD_CONFIG", {"reason": "strategy_approved", "rec_id": rid}, source="dashboard")
                msg = (msg or "") + " | strategy override applied"
            except Exception as e:
                msg = (msg or "") + f" | strategy override failed: {e}"

    return {"status": "ok" if ok else "error", "msg": msg}


@app.post("/api/paper/recommendations/reject")
async def paper_reject_rec(request: Request):
    data = await request.json()
    rid = int(data.get("id", 0))
    ok = db.reject_recommendation(rid)
    return {"status": "ok" if ok else "error"}


# =========================================
# 🩺 Project status & maintenance (optional)
# =========================================


# =========================================
# 🚀 PUMP HUNTER (Standalone Script UI)
# =========================================
@app.get("/api/pump/status")
async def api_pump_status():
    st = db.get_pump_hunter_state()

    counts = {"WATCH": 0, "PENDING": 0, "APPROVED": 0, "REJECTED": 0, "EXECUTED": 0}
    try:
        with db.lock:
            db.cursor.execute("SELECT status, COUNT(*) AS c FROM pump_candidates GROUP BY status")
            for r in db.cursor.fetchall() or []:
                counts[str(r["status"] or "").upper()] = int(r["c"] or 0)
    except Exception:
        pass

    return {"status": "ok", "state": st, "counts": counts}


@app.post("/api/pump/candidates/promote")
async def api_pump_promote(request: Request):
    """Promote a WATCH candidate to PENDING with a chosen side (LONG/SHORT).

    This enables Pump Hunter to surface weak-AI candidates without executing them.
    The user can PROMOTE from the dashboard, then approve as usual.
    """
    data = await request.json()
    cid = int(data.get("id") or 0)
    note = str(data.get("note") or "")
    raw_side = data.get("side")
    if cid <= 0:
        return {"status": "error", "msg": "Missing id"}
    side = _normalize_side(raw_side)
    if not side:
        return {"status": "error", "msg": f"Invalid side: {raw_side!r} (expected LONG/SHORT or BUY/SELL)"}

    rec = db.get_pump_candidate(cid)
    if not rec:
        return {"status": "error", "msg": "Candidate not found"}

    ok = db.set_pump_candidate_vote(cid, vote=side, note=note)
    return {"status": "ok" if ok else "error"}

@app.get("/api/pump/candidates")
async def api_pump_candidates(limit: int = 50, status: str = "PENDING"):
    items = db.list_pump_candidates(limit=limit, status=status)
    return {"status": "ok", "items": items}

@app.post("/api/pump/candidates/approve")
async def api_pump_approve(request: Request):
    data = await request.json()
    cid = int(data.get("id") or 0)
    note = str(data.get("note") or "")
    if cid <= 0:
        return {"status": "error", "msg": "Missing id"}
    rec = db.get_pump_candidate(cid)
    if not rec:
        return {"status": "error", "msg": "Candidate not found"}

    payload = dict(rec.get("payload") or {})
    # Ensure minimum fields for EXECUTE_SIGNAL (Pump Hunter stores ai_vote instead of side)
    symbol = str(payload.get("symbol") or rec.get("symbol") or "").upper().strip()
    if not symbol:
        return {"status": "error", "msg": "Candidate payload missing symbol"}
    payload["symbol"] = symbol

    raw_side = (
        payload.get("side")
        or payload.get("decision")
        or payload.get("signal")
        or payload.get("action")
        or payload.get("direction")
        or payload.get("order_side")
        or payload.get("position_side")
        or payload.get("ai_vote")
    )
    side_norm = _normalize_side(raw_side)
    if side_norm:
        payload["side"] = side_norm


    # prefer futures by default
    mt = str(rec.get("market_type") or payload.get("market_type") or payload.get("market") or "futures").lower().strip()
    payload["market_type"] = mt or "futures"

    # normalize timeframe key (bot prefers 'timeframe')
    if not payload.get("timeframe"):
        tf = payload.get("interval") or payload.get("tf")
        if tf:
            payload["timeframe"] = tf

    # keep a 'confidence' value for consistency (Pump Hunter stores ai_confidence as percent)
    if payload.get("confidence") is None and payload.get("ai_confidence") is not None:
        payload["confidence"] = payload.get("ai_confidence")

    # mark approval metadata
    payload["_pump_candidate_id"] = cid
    payload["_approved_by"] = "dashboard"
    payload["_approved_at"] = _utc_iso_now()

    # hard validation: do not enqueue if side invalid/WAIT
    if not payload.get("side"):
        return {"status": "error", "msg": f"Invalid/unsupported side for execution: {raw_side!r} (expected LONG/SHORT or BUY/SELL)"}
    # push to bot command queue
    db.add_command("EXECUTE_SIGNAL", payload)
    db.set_pump_candidate_status(cid, "APPROVED", note=note)
    return {"status": "ok"}

@app.post("/api/pump/candidates/reject")
async def api_pump_reject(request: Request):
    data = await request.json()
    cid = int(data.get("id") or 0)
    note = str(data.get("note") or "")
    if cid <= 0:
        return {"status": "error", "msg": "Missing id"}
    ok = db.set_pump_candidate_status(cid, "REJECTED", note=note)
    return {"status": "ok" if ok else "error"}

@app.post("/api/pump/label")
async def api_pump_label(request: Request):
    data = await request.json()
    symbol = str(data.get("symbol") or "").upper().strip()
    ts_ms = int(data.get("timestamp_ms") or 0)
    label = str(data.get("label") or "PUMP")
    note = str(data.get("note") or "")
    if not symbol or ts_ms <= 0:
        return {"status": "error", "msg": "Missing symbol/timestamp_ms"}
    rid = db.add_pump_label(symbol, ts_ms, label=label, note=note)
    return {"status": "ok", "id": rid}

@app.get("/api/status")
async def api_status():
    if collect_status is None:
        raise HTTPException(status_code=500, detail="project_doctor.py not available")
    return collect_status(dashboard_url=None, include_sensitive=False, check_binance=True)

@app.post("/api/maintenance")
async def api_maintenance(request: Request):
    if run_maintenance is None:
        raise HTTPException(status_code=500, detail="project_doctor.py not available")
    body = await request.json()
    if not isinstance(body, dict) or "action" not in body:
        raise HTTPException(status_code=400, detail="Body must include action")
    action = str(body.get("action", ""))
    params = body.get("params", {}) or {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="params must be an object")
    res = run_maintenance(action, **params)
    return {"ok": res.ok, "message": res.message, "details": res.details}


# =========================================
# RUN (dev only)
# =========================================
if __name__ == "__main__":
    import uvicorn
    print("🔐 Dashboard protected. Login at http://localhost:8000/login")
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=8000, reload=False)