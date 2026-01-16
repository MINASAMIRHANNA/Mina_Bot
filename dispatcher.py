"""
dispatcher.py

Unified outbound dispatcher for:
- Telegram alerts/signals
- External webhook (SunSystems / custom integrations)
- Dashboard publish endpoint (optional helper)

Goals:
- Be resilient (never crash the trading loop)
- Non-blocking network I/O (threaded)
- UTC timestamps everywhere
- Backward-compatible with existing imports:
    from dispatcher import send_telegram_signal, send_error_alert, send_to_external_api
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from config import cfg

try:
    from database import DatabaseManager
except Exception:  # pragma: no cover
    DatabaseManager = None  # type: ignore


# ---------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------
_DB: Optional["DatabaseManager"] = None


def _get_db() -> Optional["DatabaseManager"]:
    global _DB
    if _DB is not None:
        return _DB
    if DatabaseManager is None:
        return None
    try:
        _DB = DatabaseManager()
        return _DB
    except Exception:
        return None


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_str(x: Any, default: str = "") -> str:
    try:
        s = str(x)
        return s if s else default
    except Exception:
        return default


def _as_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _confidence_percent(v: Any) -> float:
    c = _safe_float(v, 0.0)
    # Some parts of the code may treat confidence as 0..1
    if 0.0 <= c <= 1.0:
        return c * 100.0
    return c


def _telegram_post(token: str, payload: Dict[str, Any]) -> None:
    # Telegram API is very reliable; 5s is fine
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json=payload,
        timeout=5,
    )


# Minimal markdown escaping to avoid Telegram parse errors.
# (Not full MarkdownV2; just prevents common breaks.)
def _md_escape(s: str) -> str:
    if not s:
        return ""
    return (
        s.replace("_", "\\_")
         .replace("*", "\\*")
         .replace("[", "\\[")
         .replace("]", "\\]")
         .replace("`", "\\`")
    )


def _threaded(fn, *args, **kwargs) -> None:
    threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()


# ---------------------------------------------------------------------
# Public API (backward compatible)
# ---------------------------------------------------------------------
def send_telegram_signal(chat_id: Optional[str], data: Dict[str, Any]) -> None:
    """
    Sends a nicely formatted signal message to Telegram.

    Compatible with payloads that use:
      - symbol or coin
      - entry_price or entry (float or dict)
      - stop_loss, take_profits
      - leverage, confidence, exit_profile, timeframe, market_type, strategy
    """
    token = getattr(cfg, "TELEGRAM_BOT_TOKEN", "")
    if not token or not chat_id:
        return

    try:
        signal = _safe_str(data.get("signal") or data.get("side") or "")
        is_long = "LONG" in signal.upper() or signal.upper() == "BUY"

        icon_dir = "📈" if is_long else "📉"
        direction = "LONG" if is_long else "SHORT"

        symbol = _safe_str(data.get("symbol") or data.get("coin") or data.get("pair") or "UNKNOWN").upper()

        market_type = _safe_str(data.get("market_type") or "FUTURES").upper()
        market_icon = "🚀" if market_type == "FUTURES" else "🪙"

        strategy = _safe_str(data.get("strategy") or data.get("algo") or data.get("strategy_tag") or "SCALP").upper()
        tf = _safe_str(data.get("timeframe") or data.get("tf") or "5m")

        # Entry price can be float or dict range
        entry = data.get("entry_price", data.get("entry"))
        if isinstance(entry, dict):
            price = _safe_float(entry.get("low") or entry.get("close") or entry.get("price"), 0.0)
        else:
            price = _safe_float(entry, 0.0)

        qty = _safe_float(data.get("quantity") or data.get("qty"), 0.0)
        size_usd = int(price * qty) if price > 0 and qty > 0 else 0

        sl = _safe_float(data.get("stop_loss") or data.get("sl"), 0.0)

        tps = data.get("take_profits", data.get("targets", []))
        tps_list = _as_list(tps)
        tps_str = " | ".join([str(x) for x in tps_list]) if tps_list else "-"

        lev = data.get("leverage")
        lev_str = f"{int(_safe_float(lev, 1))}x" if lev is not None else "1x"

        score = _safe_float(data.get("score") or data.get("final_score") or 0.0, 0.0)
        conf = _confidence_percent(data.get("confidence") or data.get("ai_confidence") or 0.0)

        status = _safe_str(data.get("status") or "")
        status_line = f"\n🟡 Status: {_md_escape(status)}" if status else ""

        ts = _safe_str(data.get("timestamp") or _utc_iso())
        exit_profile = _safe_str(data.get("exit_profile") or "")

        extra = ""
        if exit_profile:
            extra += f"\n🎛 Exit: {_md_escape(exit_profile)}"

        msg = (
            f"{icon_dir} *#{_md_escape(symbol)} {direction}*\n"
            f"{market_icon} {market_type} | {strategy} | {tf}\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"💰 Entry: {price}\n"
            f"🛡 Stop: {sl}\n"
            f"🎯 Targets: {tps_str}\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Lev: {lev_str}\n"
            f"💵 Size: ${size_usd}\n"
            f"🧠 Score: {score:.2f}\n"
            f"🤖 Confidence: {conf:.1f}%"
            f"{extra}"
            f"{status_line}\n"
            f"🕒 UTC: {_md_escape(ts)}"
        )

        def _send():
            try:
                _telegram_post(token, {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
            except Exception as e:
                # Do not crash trading loop
                print(f"[TELEGRAM ERROR] {e}")

        _threaded(_send)

    except Exception as e:
        print(f"[TELEGRAM FORMAT ERROR] {e}")


def send_error_alert(chat_id: Optional[str], msg: str) -> None:
    """Sends a system alert message to Telegram (non-blocking)."""
    token = getattr(cfg, "TELEGRAM_BOT_TOKEN", "")
    if not token or not chat_id:
        return

    msg = _safe_str(msg)
    if not msg:
        return

    text = f"⚠️ *SYSTEM ALERT*\n{_md_escape(msg)}\n🕒 UTC: {_utc_iso()}"

    def _send():
        try:
            _telegram_post(token, {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
        except Exception as e:
            print(f"[TELEGRAM ALERT ERROR] {e}")

    _threaded(_send)


def send_to_external_api(event_type: str, payload: Dict[str, Any]) -> None:
    """
    Sends SIGNAL/TRADE/ALERT to your external program via webhook.

    Controlled by Dashboard settings in DB (if present):
      - webhook_url
      - webhook_enable_signal = "TRUE"/"FALSE"
      - webhook_enable_trade  = "TRUE"/"FALSE"

        """
    et = _safe_str(event_type).upper()
    if not et:
        return

    db = _get_db()

    # 1) URL
    url = None
    try:
        url = (db.get_setting("webhook_url") if db else None)  # type: ignore[attr-defined]
    except Exception:
        url = None
    url = url or getattr(cfg, "WEBHOOK_URL", "")
    if not url:
        return

    # 2) Enable flags
    def _bool_setting(key: str, env_key: str, default: bool) -> bool:
        val = None
        try:
            val = db.get_setting(key) if db else None  # type: ignore[attr-defined]
        except Exception:
            val = None
        # Zero-ENV: no os.getenv fallback
        if val is None:
            return default
        return str(val).strip().upper() in ("1", "TRUE", "YES", "ON", "Y")

    enable_sig = _bool_setting("webhook_enable_signal", "WEBHOOK_ENABLE_SIGNAL", True)
    enable_trade = _bool_setting("webhook_enable_trade", "WEBHOOK_ENABLE_TRADE", True)

    if et == "SIGNAL" and not enable_sig:
        return
    if et == "TRADE" and not enable_trade:
        return

    bot_name = getattr(cfg, "BOT_NAME", "TradePro_v1")

    final_payload = {
        "event": et,
        "bot_name": bot_name,
        "timestamp": _utc_iso(),
        "data": payload,
    }

    def _send():
        try:
            requests.post(url, json=final_payload, timeout=5)
        except Exception as e:
            print(f"[WEBHOOK ERROR] Failed to send {et}: {e}")

    _threaded(_send)


# ---------------------------------------------------------------------
# Optional helper: dashboard publish (safe to ignore if unused)
# ---------------------------------------------------------------------
def send_to_dashboard(payload: Dict[str, Any], event: str = "SIGNAL") -> None:
    """
    Publish payload to dashboard `/publish`.

    Uses DB-backed cfg values (Zero-ENV).
    """
    url = getattr(cfg, "DASHBOARD_PUBLISH_URL", "") or (getattr(cfg, "DASHBOARD_URL", "http://localhost:8000").rstrip("/") + "/publish")
    token = getattr(cfg, "DASHBOARD_PUBLISH_TOKEN", "") or None

    headers = {"Content-Type": "application/json"}
    if token:
        headers["x-dashboard-token"] = token

    body = payload
    # Some dashboards expect an envelope. Keep payload as-is by default, but allow envelope if requested.
    if str(getattr(cfg, "DASHBOARD_ENVELOPE", False)).strip().lower() in ("1","true","yes","on"):
        body = {"event": event, "data": payload, "timestamp": _utc_iso()}

    def _send():
        try:
            requests.post(url, json=body, headers=headers, timeout=3)
        except Exception as e:
            # Non-fatal
            print(f"[DASHBOARD ERROR] Failed to publish: {e}")

    _threaded(_send)
