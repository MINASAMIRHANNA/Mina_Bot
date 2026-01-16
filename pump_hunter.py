"""
Pump Hunter (standalone)

Runs as a separate process alongside the bot + dashboard:
- Scans Binance markets for "pre-pump" candidates using pump_detector + AI
- Writes results to bot_data.db table: pump_candidates
- (optional) also writes into signal_inbox as PENDING_APPROVAL

Usage:
  python pump_hunter.py
  python pump_hunter.py --once
"""

import argparse
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from database import DatabaseManager
from model_manager import ModelManager

# project modules (expected in your repo)
from fetcher import fetch_klines
from pump_detector import detect_pre_pump
from feature_extractor import extract_features


BINANCE_FUTURES = "https://fapi.binance.com"
BINANCE_SPOT = "https://api.binance.com"


def _utc_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


def _get_setting(db: DatabaseManager, key: str, default: Any = None) -> Any:
    v = db.get_setting(key)
    if v is None or str(v).strip() == "":
        return default
    return v


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on", "y"):
        return True
    if s in ("0", "false", "no", "off", "n"):
        return False
    return default


def _universe_top_symbols(market_type: str, top_n: int) -> List[str]:
    """
    Pull top symbols by quote volume from Binance public endpoints.
    """
    base = BINANCE_FUTURES if market_type == "futures" else BINANCE_SPOT
    url = f"{base}/{'fapi' if market_type=='futures' else 'api'}/v1/ticker/24hr"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    syms = []
    for it in data:
        sym = str(it.get("symbol") or "").upper()
        if not sym.endswith("USDT"):
            continue
        # exclude leveraged tokens and obvious non-spot pairs
        bad = any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"))
        if bad:
            continue
        qv = _safe_float(it.get("quoteVolume"), 0.0)
        syms.append((sym, qv))

    syms.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in syms[:max(5, top_n)]]


def _funding_rate(symbol: str) -> Optional[float]:
    try:
        url = f"{BINANCE_FUTURES}/fapi/v1/premiumIndex"
        r = requests.get(url, params={"symbol": symbol}, timeout=10)
        r.raise_for_status()
        j = r.json()
        return _safe_float(j.get("lastFundingRate"), 0.0)
    except Exception:
        return None


def _open_interest(symbol: str) -> Optional[float]:
    try:
        url = f"{BINANCE_FUTURES}/fapi/v1/openInterest"
        r = requests.get(url, params={"symbol": symbol}, timeout=10)
        r.raise_for_status()
        j = r.json()
        return _safe_float(j.get("openInterest"), 0.0)
    except Exception:
        return None


def _maybe_insert_signal_inbox(db: DatabaseManager, payload: Dict[str, Any]) -> None:
    """
    Optional mirror into dashboard signal_inbox table (if exists).
    """
    try:
        with db.lock:
            db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signal_inbox'")
            if not db.cursor.fetchone():
                return

            now_ms = _utc_ms()
            db.cursor.execute(
                """
                INSERT INTO signal_inbox
                (received_at, received_at_ms, source, status, symbol, timeframe, strategy, side, confidence, score, payload, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _utc_iso(),
                    now_ms,
                    "pump_hunter",
                    "PENDING_APPROVAL",
                    payload.get("symbol"),
                    payload.get("interval") or payload.get("timeframe") or "5m",
                    "PUMP_HUNTER",
                    payload.get("ai_vote") or payload.get("side") or payload.get("signal"),
                    _safe_float(payload.get("ai_confidence") or payload.get("confidence"), 0.0),
                    _safe_float(payload.get("pump_score"), 0.0),
                    json.dumps(payload, ensure_ascii=False),
                    "Pump Hunter candidate",
                )
            )
            db.conn.commit()
    except Exception:
        return


def scan_once(db: DatabaseManager, model: ModelManager) -> Tuple[int, List[Dict[str, Any]]]:
    market = str(_get_setting(db, "pump_hunter_market", "futures")).lower().strip()
    interval = str(_get_setting(db, "pump_hunter_interval", "5m")).strip()
    top_n = _safe_int(_get_setting(db, "pump_hunter_top_n", 120), 120)
    min_pump = _safe_int(_get_setting(db, "pump_hunter_pump_score_min", 65), 65)
    min_conf_pct = _safe_float(_get_setting(db, "pump_hunter_ai_conf_min", 65), 65.0)
    # If enabled, we will store candidates that pass pump filters even if AI vote/conf is weak,
    # but mark them as WATCH (manual promote from dashboard) instead of dropping them.
    allow_ai_weak = _as_bool(_get_setting(db, "pump_hunter_allow_ai_weak", "TRUE"), True)
    min_oi = _safe_float(_get_setting(db, "pump_hunter_oi_change_min", 0.02), 0.02)
    fund_max_abs = _safe_float(_get_setting(db, "pump_hunter_funding_max_abs", 0.0005), 0.0005)
    max_per_scan = _safe_int(_get_setting(db, "pump_hunter_max_per_scan", 8), 8)
    exit_profile = str(_get_setting(db, "pump_hunter_exit_profile", "SCALP_PUMP"))
    leverage = _safe_int(_get_setting(db, "pump_hunter_leverage", 10), 10)
    mirror_inbox = _as_bool(_get_setting(db, "pump_hunter_mirror_inbox", "TRUE"), True)

    symbols = _universe_top_symbols(market, top_n)

    found: List[Dict[str, Any]] = []
    now_ms = _utc_ms()

    for sym in symbols:
        try:
            df = fetch_klines(sym, interval, 260, market)
            if df is None or df.empty:
                continue

            price = _safe_float(df["close"].iloc[-1], 0.0)
            if price <= 0:
                continue

            pump_score = _safe_int(detect_pre_pump(df, price), 0)
            if pump_score < min_pump:
                continue

            feats = extract_features(df)
            if feats is None or feats.empty:
                continue

            pred = model.predict(feats.tail(1), market_type=market, algo_mode="swing")[0]
            ai_vote = str(pred.get("signal") or "WAIT").upper()
            ai_conf = _safe_float(pred.get("confidence"), 0.0)  # 0..1

            weak_reasons: List[str] = []
            if ai_vote == "WAIT":
                weak_reasons.append("ai_vote=WAIT")
            if ai_conf * 100.0 < min_conf_pct:
                weak_reasons.append(f"ai_conf<{min_conf_pct:.0f}%")

            is_watch = len(weak_reasons) > 0
            if is_watch and not allow_ai_weak:
                continue

            funding = None
            oi_change = None
            if market == "futures":
                funding = _funding_rate(sym)
                if funding is not None and abs(funding) > fund_max_abs:
                    # too extreme funding -> skip
                    continue

                oi = _open_interest(sym)
                if oi is not None and oi > 0:
                    _, delta = db.upsert_pump_hunter_oi(sym, oi, now_ms)
                    oi_change = delta
                    if delta is not None and delta < min_oi:
                        continue

            status = "WATCH" if is_watch else "PENDING"
            note_bits = [f"pre-pump score={pump_score}"]
            note_bits.append(f"ai={ai_vote}({ai_conf*100:.1f}%)")
            if is_watch:
                note_bits.append("WATCH:" + ",".join(weak_reasons))

            payload = {
                "source": "PUMP_HUNTER",
                "symbol": sym,
                "market_type": market,
                "interval": interval,
                "entry_price": price,
                "pump_score": pump_score,
                "ai_vote": ai_vote,
                "ai_confidence": round(ai_conf * 100.0, 2),  # store as percent for UI clarity
                "funding": funding,
                "oi_change": oi_change,
                "oi_change_pct": (float(oi_change) * 100.0) if oi_change is not None else None,
                "funding_pct": (float(funding) * 100.0) if funding is not None else None,
                "exit_profile": exit_profile,
                "leverage": leverage,
                "execution_allowed": False,
                "timestamp_utc": _utc_iso(),
                "watch": bool(is_watch),
                "watch_reason": ",".join(weak_reasons) if weak_reasons else "",
                "note": "; ".join(note_bits),
            }

            cid = db.add_pump_candidate(payload, status=status)
            payload["_pump_candidate_id"] = cid
            found.append(payload)

            # Only mirror actionable candidates to inbox. WATCH stays in pump_candidates until promoted.
            if (not is_watch) and mirror_inbox:
                _maybe_insert_signal_inbox(db, payload)

            if len(found) >= max(1, max_per_scan):
                break

        except Exception:
            continue

    db.update_pump_hunter_state(scan_ms=now_ms, scan_count=len(found), last_error="")
    return len(found), found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="./bot_data.db", help="path to bot_data.db")
    ap.add_argument("--once", action="store_true", help="run one scan then exit")
    args = ap.parse_args()

    db = DatabaseManager(args.db)
    model = ModelManager("./models")
    model.reload()

    print(f"[PumpHunter] DB: {args.db}")
    print(f"[PumpHunter] Time: {_utc_iso()} (UTC)")

    while True:
        try:
            enabled = _as_bool(_get_setting(db, "pump_hunter_enabled", "TRUE"), True)
            if not enabled:
                db.update_pump_hunter_state(heartbeat_ms=_utc_ms(), last_error="DISABLED")
                time.sleep(5)
                if args.once:
                    return
                continue

            hb = _utc_ms()
            db.update_pump_hunter_state(heartbeat_ms=hb)

            n, _ = scan_once(db, model)
            print(f"[PumpHunter] { _utc_iso() } scan: +{n} candidates")

        except Exception as e:
            db.update_pump_hunter_state(heartbeat_ms=_utc_ms(), last_error=str(e))
            print(f"[PumpHunter] ERROR: {e}")

        if args.once:
            return

        scan_sec = _safe_int(_get_setting(db, "pump_hunter_scan_sec", 20), 20)
        time.sleep(max(5, scan_sec))


if __name__ == "__main__":
    main()