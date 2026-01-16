"""
auto_trainer.py

Build / refresh a labeled dataset for pump-like moves ("manual_pumps.csv").

Goals:
- Robust collection (works without API keys for public endpoints).
- UTC everywhere.
- Backward compatible with existing workflow/CLI usage.

Outputs:
- ./data/manual_pumps.csv
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

try:
    # Prefer fetcher helpers (public endpoints)
    from fetcher import fetch_futures_raw, fetch_klines, fetch_klines_range
except Exception:
    fetch_futures_raw = None
    fetch_klines = None
    fetch_klines_range = None

try:
    from config import cfg
except Exception:
    cfg = None

try:
    from feature_extractor import extract_features
except Exception:
    extract_features = None


DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(DATA_DIR, "manual_pumps.csv")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(pump_time_str: str) -> datetime:
    """
    Parse 'YYYY-MM-DDTHH:MM' as UTC.
    (Dashboard-friendly, unambiguous.)
    """
    dt = datetime.strptime(pump_time_str, "%Y-%m-%dT%H:%M")
    return dt.replace(tzinfo=timezone.utc)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    # Drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Ensure numeric open
    if "open" in df.columns:
        df = df[pd.to_numeric(df["open"], errors="coerce").notnull()]

    return df


def get_btc_context(limit: int = 1500) -> Optional[pd.DataFrame]:
    if fetch_klines is None:
        return None
    btc_df = fetch_klines("BTCUSDT", "5m", limit=limit, market_type="spot")
    if btc_df is not None and not btc_df.empty and "close" in btc_df.columns:
        return btc_df[["close"]].copy()
    return None


# ==========================================================
# MANUAL TEACHING (OPTIONAL CLI)
# ==========================================================
def learn_specific_pump(symbol: str, pump_time_str: str) -> None:
    """
    Adds a labeled window around a known pump time (UTC) to manual_pumps.csv.
    """
    if extract_features is None:
        print("❌ feature_extractor.extract_features not available.")
        return
    if fetch_klines_range is None and fetch_klines is None:
        print("❌ fetcher functions not available.")
        return

    print(f"\n🎓 MANUAL TRAINING → {symbol} @ {pump_time_str} (UTC)")

    try:
        pump_dt = _parse_utc(pump_time_str)
        start_dt = pump_dt - timedelta(days=4)
        end_dt = pump_dt + timedelta(days=1)

        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        # Prefer ranged fetch (futures)
        df = None
        if fetch_klines_range is not None:
            df = fetch_klines_range(symbol, "5m", start_ms, end_ms, market_type="futures")
        if (df is None or df.empty) and fetch_klines is not None:
            # Fallback: last N candles
            df = fetch_klines(symbol, "5m", limit=1500, market_type="futures")

        if df is None or df.empty:
            print("⚠️ No klines fetched.")
            return

        df = df.copy()
        df["symbol"] = symbol

        # Labeling: "pump" if +1.5% within next 3 candles
        df["future_close"] = df["close"].shift(-3)
        df["change"] = (df["future_close"] - df["close"]) / df["close"]
        df["target"] = (df["change"] > 0.015).astype(int)

        btc_df = get_btc_context()
        feats = extract_features(df, btc_df)
        df_final = pd.concat([df.reset_index(drop=True), feats.reset_index(drop=True)], axis=1)
        df_final = df_final.loc[:, ~df_final.columns.duplicated()]
        df_final["source"] = "manual_pump"
        df_final = df_final.dropna()

        keep = [
            "symbol",
            "open", "high", "low", "close", "volume",
            "qav", "trades", "tbba", "tbqa",
            "target",
            # features
            "ema_20", "ema_50", "ema_200",
            "rsi_14", "atr_14",
            "rsi_lag_1", "rsi_change",
            "pct_chg_5", "dist_ema_50",
            "btc_corr", "btc_trend",
            "source",
        ]
        df_final = df_final[[c for c in keep if c in df_final.columns]]

        if df_final.empty:
            print("⚠️ No usable rows after feature extraction.")
            return

        if os.path.exists(OUTPUT_FILE):
            try:
                old = clean_dataframe(pd.read_csv(OUTPUT_FILE))
                df_final = pd.concat([old, df_final], ignore_index=True)
            except Exception as e:
                print(f"⚠️ Could not merge with old CSV ({e}). Creating new file.")

        df_final.drop_duplicates(subset=["symbol", "open", "close", "volume"], inplace=True)
        df_final.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Added {len(df_final)} rows → {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Manual training failed: {e}")


# ==========================================================
# AUTOMATIC MARKET HARVEST
# ==========================================================
def harvest_market_data(
    max_symbols: int = 80,
    interval: str = "5m",
    limit: int = 1500,
    pump_gain_threshold: float = 0.015,
    lookahead_candles: int = 3,
) -> None:
    """
    Pull recent market data for many symbols, label "pumps", and append to dataset.

    Args:
      max_symbols: cap the number of symbols to scan (safety).
      interval: kline interval.
      limit: number of candles per symbol.
      pump_gain_threshold: label as pump if gain exceeds threshold in lookahead window.
      lookahead_candles: lookahead candles for labeling.
    """
    if extract_features is None:
        print("❌ feature_extractor.extract_features not available.")
        return
    if fetch_klines is None or fetch_futures_raw is None:
        print("❌ fetcher functions not available.")
        return

    print("\n🚜 STARTING MARKET DATA HARVEST (UTC)")

    tickers: List[str] = fetch_futures_raw() or []
    if not tickers:
        print("❌ No tickers fetched.")
        return

    tickers = tickers[: max_symbols]

    btc_df = get_btc_context()
    all_data = []
    total_rows = 0
    total_pumps = 0

    for i, symbol in enumerate(tickers):
        sys.stdout.write(f"\r[{i+1}/{len(tickers)}] {symbol}          ")
        sys.stdout.flush()

        df = fetch_klines(symbol, interval, limit=limit, market_type="futures")
        if df is None or df.empty:
            continue

        df = df.copy()
        df["symbol"] = symbol

        df["future_close"] = df["close"].shift(-lookahead_candles)
        df["change"] = (df["future_close"] - df["close"]) / df["close"]
        df["target"] = (df["change"] > pump_gain_threshold).astype(int)

        feats = extract_features(df, btc_df)
        df_final = pd.concat([df.reset_index(drop=True), feats.reset_index(drop=True)], axis=1)
        df_final = df_final.loc[:, ~df_final.columns.duplicated()]
        df_final["source"] = "market_harvest"
        df_final = df_final.dropna()

        keep = [
            "symbol",
            "open", "high", "low", "close", "volume",
            "qav", "trades", "tbba", "tbqa",
            "target",
            "ema_20", "ema_50", "ema_200",
            "rsi_14", "atr_14",
            "rsi_lag_1", "rsi_change",
            "pct_chg_5", "dist_ema_50",
            "btc_corr", "btc_trend",
            "source",
        ]
        df_final = df_final[[c for c in keep if c in df_final.columns]]

        if not df_final.empty:
            total_rows += len(df_final)
            total_pumps += int(df_final["target"].sum()) if "target" in df_final.columns else 0
            all_data.append(df_final)

        time.sleep(0.05)

    print(f"\n📊 Rows: {total_rows} | Pumps: {total_pumps}")

    if not all_data:
        print("⚠️ No data collected.")
        return

    new_df = pd.concat(all_data, ignore_index=True)

    if os.path.exists(OUTPUT_FILE):
        try:
            old = clean_dataframe(pd.read_csv(OUTPUT_FILE))
            new_df = pd.concat([old, new_df], ignore_index=True)
        except Exception as e:
            print(f"\n⚠️ Could not merge with old CSV ({e}). Creating new file.")

    new_df.drop_duplicates(subset=["symbol", "open", "close", "volume"], inplace=True)
    new_df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Dataset updated → {OUTPUT_FILE}")


if __name__ == "__main__":
    # Default: harvest
    harvest_market_data()
