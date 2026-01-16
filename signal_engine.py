from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from config import cfg


# ==========================================================
# Helpers
# ==========================================================
def _utc_now_iso() -> str:
    """UTC timestamp in ISO-8601 with 'Z'."""
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
        return int(x)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


# ==========================================================
# SCORE COMBINER
# ==========================================================
def combine_scores(scores: Dict[str, Any], algo_mode: str, market_type: str = "futures") -> float:
    """Combine module scores into a single score in range [-1, +1].

    Expected input keys (all optional):
      - technical: float in [-1, +1]
      - derivatives: float in [-1, +1]   (mostly relevant for futures)
      - volume_spike: int/bool           (0/1 or a small integer)
      - pump_score: float in [0,100]     (optional; spot/pump-hunting)

    algo_mode: 'scalp' or 'swing' (anything else will use balanced weights)
    market_type: 'futures' or 'spot'
    """
    tech = _safe_float(scores.get("technical", 0.0), 0.0)
    deriv = _safe_float(scores.get("derivatives", 0.0), 0.0)
    vol_spike = _safe_int(scores.get("volume_spike", 0), 0)
    pump_score = scores.get("pump_score", None)

    algo = (algo_mode or "").strip().lower()
    mkt = (market_type or "futures").strip().lower()

    # ---------------- SPOT (Pump Hunting) ----------------
    # In spot mode, derivatives are usually noise; volume spike + pump_score matter more.
    if mkt == "spot":
        w_tech = _safe_float(getattr(cfg, "SCORE_W_TECH_SPOT", 0.70), 0.70)
        w_deriv = _safe_float(getattr(cfg, "SCORE_W_DERIV_SPOT", 0.10), 0.10)
        w_pump = _safe_float(getattr(cfg, "SCORE_W_PUMP_SPOT", 0.20), 0.20)

        # Normalize pump_score [0..100] -> [-1..+1] with 50 as neutral
        pump_norm = 0.0
        if pump_score is not None:
            pump_norm = _clamp((_safe_float(pump_score, 50.0) - 50.0) / 50.0, -1.0, 1.0)

        score = (w_tech * tech) + (w_deriv * deriv) + (w_pump * pump_norm)

        # Volume spike bonus (small, bounded)
        if vol_spike:
            bonus = _safe_float(getattr(cfg, "VOLUME_SPIKE_BONUS_SPOT", 0.08), 0.08)
            score += bonus if score >= 0 else -bonus

        return _clamp(score, -1.0, 1.0)

    # ---------------- FUTURES / DERIVATIVES ----------------
    # Futures modes: blend technical + derivatives; scalp is more technical.
    if algo == "scalp":
        w_tech = _safe_float(getattr(cfg, "SCORE_W_TECH_SCALP", 0.75), 0.75)
        w_deriv = _safe_float(getattr(cfg, "SCORE_W_DERIV_SCALP", 0.25), 0.25)
    elif algo == "swing":
        w_tech = _safe_float(getattr(cfg, "SCORE_W_TECH_SWING", 0.60), 0.60)
        w_deriv = _safe_float(getattr(cfg, "SCORE_W_DERIV_SWING", 0.40), 0.40)
    else:
        w_tech = 0.65
        w_deriv = 0.35

    score = (w_tech * tech) + (w_deriv * deriv)

    # Volume spike bonus (bounded)
    if vol_spike:
        bonus = _safe_float(getattr(cfg, "VOLUME_SPIKE_BONUS", 0.05), 0.05)
        score += bonus if score >= 0 else -bonus

    return _clamp(score, -1.0, 1.0)


# ==========================================================
# SCORE → SIGNAL
# ==========================================================
def map_to_signal(score: float,
                  entry_threshold: Optional[float] = None,
                  strong_threshold: Optional[float] = None) -> str:
    """Map numeric score to discrete trade signal.

    Returns one of:
      - 'STRONG_LONG', 'LONG'
      - 'STRONG_SHORT', 'SHORT'
      - 'WAIT'

    This matches Paper Tournament normalization (it looks for 'LONG'/'SHORT').
    """
    s = _safe_float(score, 0.0)

    th = entry_threshold if entry_threshold is not None else _safe_float(getattr(cfg, "ENTRY_THRESHOLD", 0.2), 0.2)
    sth = strong_threshold if strong_threshold is not None else _safe_float(getattr(cfg, "STRONG_SIGNAL_THRESHOLD", 0.6), 0.6)

    if s >= sth:
        return "STRONG_LONG"
    if s <= -sth:
        return "STRONG_SHORT"
    if s >= th:
        return "LONG"
    if s <= -th:
        return "SHORT"
    return "WAIT"


# ==========================================================
# Optional: Leverage helper (used by main/executor when available)
# ==========================================================
def suggest_leverage(volatility_pct: float, strategy_mode: str = "scalp") -> int:
    """Suggest leverage based on volatility and configured caps.

    volatility_pct example: ATR% or (ATR/price). Values like 0.02 => 2%
    """
    v = _safe_float(volatility_pct, 0.0)
    strategy = (strategy_mode or "").strip().lower()

    # Extreme volatility → reduce leverage hard
    if v >= 0.05:
        return 5
    if v >= 0.03:
        return 10

    base = cfg.LEVERAGE_SCALP if strategy == "scalp" else cfg.LEVERAGE_SWING
    return max(1, min(int(base), int(getattr(cfg, "MAX_LEVERAGE", base))))


# ==========================================================
# Optional: Standardized payload builder (for dashboard / audit)
# ==========================================================
def build_signal_payload(symbol: str,
                         algo_mode: str,
                         market_type: str,
                         scores: Dict[str, Any],
                         timeframe: str = "5m",
                         exit_profile: str = "default",
                         meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create a stable, dashboard-friendly signal payload.

    NOTE: existing code may already build payloads; this is safe to adopt gradually.
    """
    final_score = combine_scores(scores, algo_mode, market_type)
    sig = map_to_signal(final_score)

    payload = {
        "timestamp_utc": _utc_now_iso(),
        "symbol": symbol,
        "market_type": (market_type or "futures").lower(),
        "algo_mode": (algo_mode or "scalp").lower(),
        "timeframe": timeframe,
        "exit_profile": exit_profile,
        "score": float(final_score),
        "signal": sig,
        "scores": {
            "technical": _safe_float(scores.get("technical", 0.0), 0.0),
            "derivatives": _safe_float(scores.get("derivatives", 0.0), 0.0),
            "volume_spike": _safe_int(scores.get("volume_spike", 0), 0),
        },
        # Governance fields (can be overwritten by main/dashboard)
        "status": "PENDING_APPROVAL" if getattr(cfg, "REQUIRE_DASHBOARD_APPROVAL", False) else "RECOMMENDED",
        "execution_allowed": bool(not getattr(cfg, "REQUIRE_DASHBOARD_APPROVAL", False)),
    }

    if scores.get("pump_score") is not None:
        payload["scores"]["pump_score"] = _safe_float(scores.get("pump_score"), 0.0)

    if meta:
        payload["meta"] = meta

    return payload
