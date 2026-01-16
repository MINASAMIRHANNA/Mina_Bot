import os
import sys
import socket
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

# Keep this module safe to import even if config isn't available.
try:
    from config import cfg  # type: ignore
except Exception:  # pragma: no cover
    cfg = None  # type: ignore


def _utc_now_iso() -> str:
    # 2026-01-01T12:34:56Z
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_setting(name: str, default: str = "") -> str:
    # Prefer cfg.* then env
    if cfg is not None and hasattr(cfg, name):
        v = getattr(cfg, name)
        if v is None:
            return default
        return str(v)
    return os.getenv(name, default)


def _send_telegram(text: str, token: str, chat_id: str, parse_mode: str = "Markdown") -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    # Short timeout to avoid blocking the bot
    requests.post(url, json=payload, timeout=5)


def send_alert(message: str, level: str = "INFO", extra: Optional[Dict[str, Any]] = None) -> None:
    """Send a Telegram alert.

    Backward-compatible:
    - Existing calls: send_alert("msg") still work.
    - You may also pass level/extra if you want richer logs.

    Reads credentials from:
    - cfg.TELEGRAM_BOT_TOKEN / cfg.TELEGRAM_CHAT_ID (preferred)
    - or env TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
    """
    try:
        token = _get_setting("TELEGRAM_BOT_TOKEN", "")
        chat_id = _get_setting("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return

        parse_mode = os.getenv("TELEGRAM_PARSE_MODE", "Markdown")

        host = socket.gethostname()
        ts = _utc_now_iso()

        # Keep formatting simple to avoid Markdown edge-cases.
        header = f"[{level}] {ts} | {host}"
        body = str(message).strip()

        if extra:
            # Render extra in key=value lines
            lines = []
            for k, v in extra.items():
                try:
                    lines.append(f"- {k}: {v}")
                except Exception:
                    lines.append(f"- {k}: <unprintable>")
            body = body + "\n\n" + "\n".join(lines)

        full_msg = header + "\n" + body
        _send_telegram(full_msg, token=token, chat_id=chat_id, parse_mode=parse_mode)
    except Exception:
        # Never crash the caller because of notification issues
        return


def send_error(message: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """Convenience wrapper for error-level alerts."""
    send_alert(message, level="ERROR", extra=extra)


def send_warning(message: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """Convenience wrapper for warning-level alerts."""
    send_alert(message, level="WARN", extra=extra)


if __name__ == "__main__":
    # Usage:
    #   python notify.py "message"
    # Optional:
    #   python notify.py "message" ERROR
    if len(sys.argv) > 1:
        msg = sys.argv[1]
        lvl = sys.argv[2] if len(sys.argv) > 2 else "INFO"
        send_alert(msg, level=lvl)
