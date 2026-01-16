from indicators import ema, atr, volume_spike
import numpy as np


# ============================
# Safe value helpers
# ============================
def _last_val(v, default=0.0):
    try:
        if hasattr(v, "iloc"):
            x = v.iloc[-1]
            if hasattr(x, "iloc"):
                x = x.iloc[-1]
            return float(x)
        if isinstance(v, (list, tuple)):
            return float(v[-1]) if v else float(default)
        return float(v)
    except Exception:
        return float(default)

def _tail_series(v, n: int, default=0.0):
    """Return last n items as a pandas Series (works even if v is scalar)."""
    import pandas as pd
    try:
        if hasattr(v, "iloc"):
            return v.iloc[-n:]
        # scalar
        return pd.Series([float(v)] * n)
    except Exception:
        return pd.Series([float(default)] * n)

def detect_pre_pump(df, current_price):
    """
    Detect early pump conditions BEFORE breakout.
    Returns: score (0 → 100)
    """

    try:
        score = 0

        # ==================================================
        # 1️⃣ Volume Behavior (Smart)
        # ==================================================
        vol_spk = _tail_series(volume_spike(df), 5)
        vol_hits = vol_spk.sum()

        if vol_hits >= 3:
            score += 25
        elif vol_hits == 2:
            score += 15
        elif vol_hits == 1:
            score += 8

        # ==================================================
        # 2️⃣ Volatility Compression (ATR Squeeze)
        # ==================================================
        atr_vals = _tail_series(atr(df, 14), 20)
        if atr_vals.mean() > 0:
            atr_ratio = _last_val(atr_vals) / atr_vals.mean()

            if atr_ratio < 0.6:
                score += 25
            elif atr_ratio < 0.75:
                score += 15
            elif atr_ratio < 0.9:
                score += 8

        # ==================================================
        # 3️⃣ EMA Squeeze (Price Coiling)
        # ==================================================
        ema_fast = _last_val(ema(df, 20))
        ema_slow = _last_val(ema(df, 50))
        ema_dist = abs(ema_fast - ema_slow) / current_price

        if ema_dist < 0.0015:
            score += 20
        elif ema_dist < 0.0025:
            score += 10

        # ==================================================
        # 4️⃣ Micro Trend Shift (Early Momentum)
        # ==================================================
        close_now = df['close'].iloc[-1]
        close_prev = df['close'].iloc[-6]

        price_change = (close_now - close_prev) / close_prev

        if price_change > 0.006:
            score += 20
        elif price_change > 0.003:
            score += 12
        elif price_change > 0.0015:
            score += 6

        # ==================================================
        # 5️⃣ Fake Pump Filter (Very Important)
        # ==================================================
        # If price already extended, NOT a pre-pump
        if price_change > 0.02:
            score -= 15

        # ==================================================
        # 6️⃣ Direction Bias (Trend Alignment)
        # ==================================================
        ema_200 = _last_val(ema(df, 200))
        if close_now > ema_200:
            score += 5
        else:
            score -= 5

        # ==================================================
        # Final Clamp
        # ==================================================
        return max(0, min(int(score), 100))

    except Exception:
        return 0