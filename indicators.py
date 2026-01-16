import pandas as pd
import numpy as np
from typing import Union

# ==========================================================
# Helpers
# ==========================================================
def _series_from_df_or_series(x: Union[pd.DataFrame, pd.Series], col: str, length_fallback: int = 0) -> pd.Series:
    """Return a numeric Series from a DataFrame column or from a Series input.

    This makes the indicator functions resilient: some callers may pass a Series (e.g. df['close']).
    """
    if isinstance(x, pd.Series):
        s = x
    elif isinstance(x, pd.DataFrame):
        if col not in x.columns:
            return pd.Series(0.0, index=x.index)
        s = x[col]
    else:
        return pd.Series(np.zeros(length_fallback, dtype=float))

    s = pd.to_numeric(s, errors="coerce").astype(float)
    return s.replace([np.inf, -np.inf], np.nan).fillna(0.0)


# ==========================================================
# EMA
# ==========================================================
def ema(x: Union[pd.DataFrame, pd.Series], period: int, col: str = "close") -> pd.Series:
    s = _series_from_df_or_series(x, col, length_fallback=len(x) if hasattr(x, "__len__") else 0)
    p = int(period) if int(period) > 0 else 1
    if len(s) < p:
        return pd.Series(0.0, index=getattr(s, "index", None))
    return s.ewm(span=p, adjust=False).mean().fillna(0.0)


# ==========================================================
# RSI
# ==========================================================
def rsi(x: Union[pd.DataFrame, pd.Series], period: int = 14, col: str = "close") -> pd.Series:
    s = _series_from_df_or_series(x, col, length_fallback=len(x) if hasattr(x, "__len__") else 0)
    p = int(period) if int(period) > 0 else 14
    if len(s) < p:
        return pd.Series(0.0, index=s.index)
    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.rolling(p, min_periods=p).mean()
    avg_loss = loss.rolling(p, min_periods=p).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.replace([np.inf, -np.inf], 0.0).fillna(0.0)


# ==========================================================
# ATR
# ==========================================================
def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    # ATR needs OHLC columns; if not present, return zeros
    if not isinstance(df, pd.DataFrame) or not {"high", "low", "close"}.issubset(set(df.columns)):
        idx = df.index if isinstance(df, pd.DataFrame) else None
        return pd.Series(0.0, index=idx)

    high = _series_from_df_or_series(df, "high")
    low = _series_from_df_or_series(df, "low")
    close = _series_from_df_or_series(df, "close")

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    p = int(period) if int(period) > 0 else 14
    out = tr.rolling(p, min_periods=p).mean()
    return out.replace([np.inf, -np.inf], 0.0).fillna(0.0)


# ==========================================================
# ADX
# ==========================================================
def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    # ADX needs OHLC; if not present, return zeros
    if not isinstance(df, pd.DataFrame) or not {"high", "low", "close"}.issubset(set(df.columns)):
        idx = df.index if isinstance(df, pd.DataFrame) else None
        return pd.Series(0.0, index=idx)

    high = _series_from_df_or_series(df, "high")
    low = _series_from_df_or_series(df, "low")
    close = _series_from_df_or_series(df, "close")

    plus_dm = high.diff()
    minus_dm = low.diff().abs()

    plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
    minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    p = int(period) if int(period) > 0 else 14
    atr_val = tr.rolling(p, min_periods=p).mean()

    plus_di = 100 * pd.Series(plus_dm, index=df.index).rolling(p, min_periods=p).sum() / atr_val.replace(0.0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).rolling(p, min_periods=p).sum() / atr_val.replace(0.0, np.nan)

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0.0, np.nan)) * 100
    out = dx.rolling(p, min_periods=p).mean()
    return out.replace([np.inf, -np.inf], 0.0).fillna(0.0)


# ==========================================================
# Volume Spike
# ==========================================================
def volume_spike(
    x: Union[pd.DataFrame, pd.Series],
    window: int = 20,
    col: str = "volume",
    spike_mult: float = 2.0,
) -> pd.Series:
    """Return a 0/1 Series marking volume spikes.

    A spike is when current volume > rolling_mean(volume, window) * spike_mult.

    Notes:
    - Always returns a pandas Series aligned to the input index (fills NaN with 0).
    - Keeps backward compatibility with callers that expect `.iloc[...]`.
    """
    v = _series_from_df_or_series(x, col, length_fallback=len(x) if hasattr(x, "__len__") else 0)
    w = int(window) if int(window) > 0 else 20
    if len(v) == 0:
        return pd.Series([], dtype=int)
    # If not enough data for rolling window -> no spikes
    if len(v) < w:
        return pd.Series([0] * len(v), index=getattr(v, "index", None), dtype=int)

    ma = v.rolling(w, min_periods=w).mean()
    try:
        mult = float(spike_mult)
    except Exception:
        mult = 2.0
    if mult <= 0:
        mult = 2.0

    hits = (v > (ma * mult)).astype(int)
    hits = hits.fillna(0).astype(int)
    return hits


# ==========================================================
# Bollinger Bands
# ==========================================================
def get_bollinger_bands(
    x: Union[pd.DataFrame, pd.Series],
    period: int = 20,
    std_mult: float = 2.0,
    col: str = "close"
):
    s = _series_from_df_or_series(x, col, length_fallback=len(x) if hasattr(x, "__len__") else 0)
    p = int(period) if int(period) > 0 else 20
    if len(s) < p:
        zeros = pd.Series(0.0, index=s.index)
        return zeros, zeros

    ma = s.rolling(p, min_periods=p).mean()
    std = s.rolling(p, min_periods=p).std()

    lower = ma - (std * float(std_mult))
    upper = ma + (std * float(std_mult))

    return lower.replace([np.inf, -np.inf], 0.0).fillna(0.0), upper.replace([np.inf, -np.inf], 0.0).fillna(0.0)
