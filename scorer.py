from __future__ import annotations

"""
scorer.py

Lightweight scoring modules used by main.py to turn indicator values into
normalized scores in [-1, +1].

Key design goals:
- Pure functions (no network calls, no DB access).
- Robust to missing/None/NaN inputs.
- Backward compatible public API:
    - technical_module_score(ma_fast, ma_slow, rsi, vol_spike, adx) -> float
    - derivatives_module_score(funding, oi) -> float

Notes on inputs:
- ma_fast / ma_slow: moving averages (floats)
- rsi: RSI value in [0..100]
- vol_spike: typically 0/1 (or a small integer / boolean)
- adx: ADX value in [0..100]
- funding: futures funding rate (e.g., 0.0001 == 0.01%)
- oi: open interest change ratio (e.g., 0.03 == +3%)
"""

from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:  # NaN
            return default
        if v in (float("inf"), float("-inf")):
            return default
        return v
    except Exception:
        return default


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def clamp(x: Any, low: float = -1.0, high: float = 1.0) -> float:
    """Clamp number to [low, high]."""
    xv = _safe_float(x, low)
    return max(low, min(high, xv))


# ---------------------------------------------------------------------
# TECHNICAL SCORE
# ---------------------------------------------------------------------
def technical_module_score(
    ma_fast: Any,
    ma_slow: Any,
    rsi: Any,
    vol_spike: Any,
    adx: Any,
) -> float:
    """
    Compute a normalized technical score in [-1, +1].

    The score blends:
    - Trend (MA fast vs slow)
    - Momentum (RSI around 50 baseline)
    - Trend strength (ADX as a multiplier)
    - Volume spike as a small directional bonus
    """
    ma_fast_v = _safe_float(ma_fast, 0.0)
    ma_slow_v = _safe_float(ma_slow, 0.0)
    rsi_v = _safe_float(rsi, 50.0)
    adx_v = _safe_float(adx, 0.0)

    # --- Trend score from MA spread (relative) ---
    # A 10% spread roughly saturates to +/-1
    if ma_slow_v and abs(ma_slow_v) > 1e-12:
        rel_spread = (ma_fast_v - ma_slow_v) / ma_slow_v
    else:
        rel_spread = 0.0
    trend_score = clamp(rel_spread * 10.0)

    # --- RSI score centered at 50 ---
    # 25 points away from 50 saturates to +/-1 (e.g., 75->+1, 25->-1)
    rsi_score = clamp((rsi_v - 50.0) / 25.0)

    # --- ADX multiplier (trend strength) ---
    # Low ADX => reduce impact; High ADX => slightly amplify
    if adx_v < 12.0:
        adx_mult = 0.55
    elif adx_v < 18.0:
        adx_mult = 0.75
    elif adx_v < 25.0:
        adx_mult = 1.00
    elif adx_v < 35.0:
        adx_mult = 1.15
    else:
        adx_mult = 1.30
    trend_score *= adx_mult
    trend_score = clamp(trend_score)

    # --- Volume spike bonus (small, directional) ---
    vs = _safe_float(vol_spike, 0.0)
    has_spike = vs >= 1.0  # compatible with int/bool
    vol_bonus = 0.0
    if has_spike:
        direction = _sign(trend_score) or _sign(rsi_score) or 1
        vol_bonus = 0.10 * direction  # small bonus, bounded

    # --- Combine (weights tuned to be stable) ---
    combined = (0.55 * trend_score) + (0.35 * rsi_score) + (0.10 * vol_bonus)
    return clamp(combined)


def technical_breakdown(
    ma_fast: Any,
    ma_slow: Any,
    rsi: Any,
    vol_spike: Any,
    adx: Any,
) -> Dict[str, float]:
    """Debug helper: return components used in technical_module_score()."""
    ma_fast_v = _safe_float(ma_fast, 0.0)
    ma_slow_v = _safe_float(ma_slow, 0.0)
    rsi_v = _safe_float(rsi, 50.0)
    adx_v = _safe_float(adx, 0.0)

    if ma_slow_v and abs(ma_slow_v) > 1e-12:
        rel_spread = (ma_fast_v - ma_slow_v) / ma_slow_v
    else:
        rel_spread = 0.0
    trend_raw = clamp(rel_spread * 10.0)

    rsi_score = clamp((rsi_v - 50.0) / 25.0)

    if adx_v < 12.0:
        adx_mult = 0.55
    elif adx_v < 18.0:
        adx_mult = 0.75
    elif adx_v < 25.0:
        adx_mult = 1.00
    elif adx_v < 35.0:
        adx_mult = 1.15
    else:
        adx_mult = 1.30

    trend_adj = clamp(trend_raw * adx_mult)

    vs = _safe_float(vol_spike, 0.0)
    has_spike = vs >= 1.0
    vol_bonus = 0.0
    if has_spike:
        direction = _sign(trend_adj) or _sign(rsi_score) or 1
        vol_bonus = 0.10 * direction

    combined = clamp((0.55 * trend_adj) + (0.35 * rsi_score) + (0.10 * vol_bonus))
    return {
        "trend_raw": trend_raw,
        "adx_multiplier": adx_mult,
        "trend_adj": trend_adj,
        "rsi_score": rsi_score,
        "vol_bonus": vol_bonus,
        "combined": combined,
    }


# ---------------------------------------------------------------------
# DERIVATIVES SCORE (Futures)
# ---------------------------------------------------------------------
def derivatives_module_score(
    funding: Any,
    oi: Any,
) -> float:
    """
    Compute derivatives sentiment score in [-1, +1] using:
    - funding rate (crowded longs/shorts)
    - open interest change (participation/conviction)

    Interpretation:
    - High positive funding => longs crowded => mildly bearish (negative score)
    - High negative funding => shorts crowded => mildly bullish (positive score)
    - Rising OI (>= +1.5% / +3%) => more participation => bullish
    - Falling OI (<= -1.5% / -3%) => unwind => bearish
    """
    funding_v = _safe_float(funding, 0.0)
    oi_v = _safe_float(oi, 0.0)

    # Funding thresholds (typical ranges; can be tuned later)
    funding_score = 0.0
    if funding_v >= 0.0008:
        funding_score = -0.5
    elif funding_v >= 0.0003:
        funding_score = -0.25
    elif funding_v <= -0.0008:
        funding_score = 0.5
    elif funding_v <= -0.0003:
        funding_score = 0.25

    # Open interest change thresholds (ratio, e.g. 0.03 == +3%)
    oi_score = 0.0
    if oi_v >= 0.03:
        oi_score = 0.5
    elif oi_v >= 0.015:
        oi_score = 0.25
    elif oi_v <= -0.03:
        oi_score = -0.5
    elif oi_v <= -0.015:
        oi_score = -0.25

    combined = funding_score + oi_score
    return clamp(combined)


def derivatives_breakdown(funding: Any, oi: Any) -> Dict[str, float]:
    """Debug helper: return components used in derivatives_module_score()."""
    funding_v = _safe_float(funding, 0.0)
    oi_v = _safe_float(oi, 0.0)

    funding_score = 0.0
    if funding_v >= 0.0008:
        funding_score = -0.5
    elif funding_v >= 0.0003:
        funding_score = -0.25
    elif funding_v <= -0.0008:
        funding_score = 0.5
    elif funding_v <= -0.0003:
        funding_score = 0.25

    oi_score = 0.0
    if oi_v >= 0.03:
        oi_score = 0.5
    elif oi_v >= 0.015:
        oi_score = 0.25
    elif oi_v <= -0.03:
        oi_score = -0.5
    elif oi_v <= -0.015:
        oi_score = -0.25

    return {
        "funding": funding_v,
        "oi": oi_v,
        "funding_score": funding_score,
        "oi_score": oi_score,
        "combined": clamp(funding_score + oi_score),
    }


__all__ = [
    "clamp",
    "technical_module_score",
    "derivatives_module_score",
    "technical_breakdown",
    "derivatives_breakdown",
]
