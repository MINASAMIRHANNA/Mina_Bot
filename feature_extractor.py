"""
feature_extractor.py (fixed)

Turns OHLCV candles into a stable feature schema for ML inference/training.

IMPORTANT:
- Feature names & order are immutable for already-trained models.
- This module must be resilient: accept df with various column spellings and never crash the bot.
"""

from __future__ import annotations

from typing import Optional, Sequence

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
# Series-based indicators (do NOT depend on indicators.py DataFrame API)
# ---------------------------------------------------------------------
def _ema_series(s: pd.Series, span: int) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce").astype(float)
    if span <= 0:
        return pd.Series(np.zeros(len(s), dtype=float), index=s.index)
    return s.ewm(span=int(span), adjust=False).mean().fillna(0.0)


def _rsi_series(s: pd.Series, period: int = 14) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce").astype(float)
    p = int(period) if int(period) > 0 else 14
    delta = s.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = gain.rolling(p, min_periods=p).mean()
    avg_loss = loss.rolling(p, min_periods=p).mean()

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _atr_series(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    high = pd.to_numeric(high, errors="coerce").astype(float)
    low = pd.to_numeric(low, errors="coerce").astype(float)
    close = pd.to_numeric(close, errors="coerce").astype(float)

    p = int(period) if int(period) > 0 else 14
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(p, min_periods=p).mean()
    return atr.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def get_bollinger_bands(df_or_series, period: int = 20, num_std: float = 2.0):
    """
    Returns (lower, upper) bands as Series.
    Accepts either:
      - DataFrame with 'close' column, OR
      - a close Series/array-like
    """
    if isinstance(df_or_series, pd.DataFrame):
        close = _get_col(df_or_series, ["close", "Close", "c"])
    else:
        close = pd.Series(df_or_series)

    close = pd.to_numeric(close, errors="coerce").astype(float)
    p = int(period) if int(period) > 0 else 20

    ma = close.rolling(window=p, min_periods=p).mean()
    std = close.rolling(window=p, min_periods=p).std()

    lower = (ma - (std * float(num_std))).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    upper = (ma + (std * float(num_std))).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return lower, upper


# ---------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------
def _get_col(df: pd.DataFrame, names: Sequence[str]) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series(np.zeros(len(df), dtype=float), index=df.index)


def _safe_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).fillna(0.0)


# ---------------------------------------------------------------------
# Main feature extraction
# ---------------------------------------------------------------------
def extract_features(df: pd.DataFrame, btc_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Convert raw OHLCV candles into the EXACT feature schema used by ML models.
    This function must never raise exceptions; it should return a DataFrame with stable columns.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=_EXPECTED_FEATURES)

    # Defensive copy (avoid mutating upstream df)
    df = df.copy()

    # Normalize base OHLCV
    close = _safe_float_series(_get_col(df, ["close", "Close", "c"]))
    high = _safe_float_series(_get_col(df, ["high", "High", "h"]))
    low = _safe_float_series(_get_col(df, ["low", "Low", "l"]))
    volume = _safe_float_series(_get_col(df, ["volume", "Volume", "v"]))

    df["close"] = close
    df["high"] = high
    df["low"] = low
    df["volume"] = volume

    # Extra Binance columns (if present)
    qav = _safe_float_series(_get_col(df, ["qav", "quote_asset_volume", "quoteVolume", "QAV"]))
    trades = _safe_float_series(_get_col(df, ["trades", "number_of_trades", "n_trades", "count"]))
    tbba = _safe_float_series(_get_col(df, ["tbba", "taker_buy_base_asset_volume", "takerBuyBaseVolume"]))
    tbqa = _safe_float_series(_get_col(df, ["tbqa", "taker_buy_quote_asset_volume", "takerBuyQuoteVolume"]))

    # Core indicators (Series-based)
    df["ema_20"] = _ema_series(close, 20)
    df["ema_50"] = _ema_series(close, 50)
    df["ema_200"] = _ema_series(close, 200)

    df["rsi_14"] = _rsi_series(close, 14)
    df["atr_14"] = _atr_series(high, low, close, 14)

    # RSI lag/delta
    df["rsi_lag_1"] = df["rsi_14"].shift(1).fillna(0.0)
    df["rsi_change"] = (df["rsi_14"] - df["rsi_lag_1"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Momentum
    df["pct_chg_5"] = close.pct_change(5).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Distance to EMA50
    denom = df["ema_50"].replace(0.0, np.nan)
    df["dist_ema_50"] = ((close - df["ema_50"]) / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Optional diagnostics (not part of ML schema)
    df["vol_ema_50"] = volume.rolling(50, min_periods=1).mean()
    df["rel_vol"] = (volume / df["vol_ema_50"].replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    lower, upper = get_bollinger_bands(close, 20, 2)
    df["bb_width"] = ((upper - lower) / df["ema_20"].replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # BTC context (stable columns). If btc_df is provided, align BTC close to df index (ffill) so it works across TFs.
    df["btc_corr"] = 0.0
    df["btc_trend"] = 0.0
    if btc_df is not None and isinstance(btc_df, pd.DataFrame) and len(btc_df) > 10:
        try:
            btc_close_raw = _safe_float_series(_get_col(btc_df, ["close", "Close", "c"]))
            # Try to align on datetime index; fallback safely to zeros if indices are unusable.
            try:
                idx = pd.to_datetime(df.index, errors="coerce")
                btc_idx = pd.to_datetime(btc_close_raw.index, errors="coerce")
                if idx.notna().sum() > 0 and btc_idx.notna().sum() > 0:
                    close_s = pd.Series(close.values, index=idx)
                    btc_s = pd.Series(btc_close_raw.values, index=btc_idx).sort_index()

                    # Align BTC to the coin candles (forward fill last known BTC value)
                    btc_aligned = btc_s.reindex(close_s.index, method="ffill")
                    if btc_aligned.isna().all():
                        btc_aligned = btc_s.reindex(close_s.index, method="nearest")
                    btc_aligned = btc_aligned.bfill().fillna(0.0)

                    # Correlation (rolling)
                    df["btc_corr"] = close_s.rolling(50, min_periods=10).corr(btc_aligned).fillna(0.0).values

                    # Trend proxy: normalized (EMA20 - EMA50)
                    btc_ema_20 = _ema_series(btc_aligned, 20)
                    btc_ema_50 = _ema_series(btc_aligned, 50)
                    denom = btc_aligned.replace(0.0, np.nan)
                    df["btc_trend"] = (((btc_ema_20 - btc_ema_50) / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)).values
                else:
                    df["btc_corr"] = 0.0
                    df["btc_trend"] = 0.0
            except Exception:
                df["btc_corr"] = 0.0
                df["btc_trend"] = 0.0
        except Exception:
            df["btc_corr"] = 0.0
            df["btc_trend"] = 0.0
    df["tbba"] = tbba
    df["tbqa"] = tbqa

    for col in _EXPECTED_FEATURES:
        if col not in df.columns:
            df[col] = 0.0

    result = df[_EXPECTED_FEATURES].copy()
    result.replace([np.inf, -np.inf], 0.0, inplace=True)
    result.fillna(0.0, inplace=True)
    return result


# ---------------------------------------------------------------------
# Immutable feature schema
# ---------------------------------------------------------------------
_EXPECTED_FEATURES = [
    "qav",
    "trades",
    "tbba",
    "tbqa",
    "ema_20",
    "ema_50",
    "ema_200",
    "rsi_14",
    "atr_14",
    "rsi_lag_1",
    "rsi_change",
    "pct_chg_5",
    "dist_ema_50",
    "btc_corr",
    "btc_trend",
]
