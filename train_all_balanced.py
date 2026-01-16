"""
train_all_balanced.py

Train balanced classification models (spot/futures/scalp) from ./data/manual_pumps.csv

- Keeps the official feature set aligned with the bot's feature_extractor.
- Prefers XGBoost if installed; falls back to sklearn GradientBoosting.
- Saves models as joblib bundles containing:
    {"model": <estimator>, "expected_features": [...], "created_at_utc": "...", "name": "..."}
"""

from __future__ import annotations

import os
from config import cfg
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score

try:
    import xgboost as xgb  # optional
except Exception:
    xgb = None

try:
    from feature_extractor import extract_features
    from fetcher import fetch_klines
except Exception:
    print("❌ Critical import error. Ensure feature_extractor.py and fetcher.py are available.")
    sys.exit(1)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "manual_pumps.csv")
MODEL_DIR = os.path.join(BASE_DIR, "models")


# ==========================================================
# OFFICIAL FEATURE SET (MUST MATCH BOT)
# ==========================================================
FEATURES = [
    "qav", "trades", "tbba", "tbqa",
    "ema_20", "ema_50", "ema_200",
    "rsi_14", "atr_14",
    "rsi_lag_1", "rsi_change",
    "pct_chg_5", "dist_ema_50",
    "btc_corr", "btc_trend",
]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ==========================================================
# DATA PROCESSING PER SYMBOL
# ==========================================================
def _forward_max_next_high(df: pd.DataFrame, lookahead: int) -> pd.Series:
    """
    Max of the next `lookahead` highs (excluding current row).
    Compatible with older pandas (no FixedForwardWindowIndexer).
    """
    highs = df["high"].astype(float)
    # Build forward window max by shifting and taking max across columns
    mats = [highs.shift(-i) for i in range(1, lookahead + 1)]
    return pd.concat(mats, axis=1).max(axis=1)


def process_group(df: pd.DataFrame, btc_df: pd.DataFrame, pump_threshold: float, lookahead: int) -> pd.DataFrame:
    df = df.copy()
    feats = extract_features(df, btc_df)
    df = df.reset_index(drop=True)
    feats = feats.reset_index(drop=True)

    merged = pd.concat([df, feats], axis=1)
    merged = merged.loc[:, ~merged.columns.duplicated()]

    # Target: max future move within lookahead
    merged["future_high"] = _forward_max_next_high(merged, lookahead=lookahead)
    merged["potential_gain"] = (merged["future_high"] - merged["close"]) / merged["close"]

    merged["target"] = ((merged["potential_gain"] > pump_threshold) & (merged["rsi_14"] < 75)).astype(int)

    # Drop rows where future window doesn't exist
    return merged.iloc[:-lookahead].copy()


# ==========================================================
# TRAINING
# ==========================================================
def _build_model(scale_pos_weight: float):
    if xgb is not None:
        return xgb.XGBClassifier(
            n_estimators=400,
            learning_rate=0.03,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            eval_metric="logloss",
        )

    # Fallback: sklearn GradientBoosting (supports feature_importances_)
    from sklearn.ensemble import GradientBoostingClassifier
    # We'll approximate class weighting via sample_weight in fit.
    return GradientBoostingClassifier(random_state=42)


def train_model(name: str, path: str, df: pd.DataFrame) -> None:
    print(f"\n🚀 Training {name}")

    # Ensure features exist
    for f in FEATURES:
        if f not in df.columns:
            df[f] = 0.0

    X = df[FEATURES].astype(float)
    y = df["target"].astype(int)

    pos = int(y.sum())
    neg = int(len(y) - pos)

    if pos < 20:
        print("⚠️ Not enough positives. Skipping.")
        return

    scale = neg / max(pos, 1)

    split_mode = str(getattr(cfg, "get_raw", lambda k, d=None: d)("TRAIN_SPLIT_MODE", "random") or "random").lower()
    test_size = float(getattr(cfg, "get_raw", lambda k, d=None: d)("TRAIN_TEST_SIZE", "0.2") or 0.2)
    if split_mode in ("time", "timeseries", "ts"):
        # Use chronological split (best-effort). Prefer a time column if present; otherwise keep file order.
        df2 = df.copy()
        for col in ("open_time", "open_time_ms", "timestamp", "ts", "time"):
            if col in df2.columns:
                try:
                    df2[col] = pd.to_numeric(df2[col], errors="coerce")
                    df2 = df2.sort_values(col)
                    break
                except Exception:
                    pass
        X2 = df2[FEATURES].astype(float)
        y2 = df2["target"].astype(int)
        split = int(max(1, len(df2) * (1.0 - test_size)))
        X_train, X_test = X2.iloc[:split], X2.iloc[split:]
        y_train, y_test = y2.iloc[:split], y2.iloc[split:]
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, stratify=y, random_state=42, shuffle=True
        )

    model = _build_model(scale_pos_weight=scale)

    # If fallback model, use sample_weight to balance
    if xgb is None:
        sample_weight = np.where(y_train.values == 1, scale, 1.0)
        model.fit(X_train, y_train, sample_weight=sample_weight)

    # Optional probability calibration (can improve signal quality, reduces overconfidence)
    if str(getattr(cfg, "get_raw", lambda k, d=None: d)("CALIBRATE_PROBA", "false") or "false").lower() in ("1", "true", "yes"):
        try:
            from sklearn.calibration import CalibratedClassifierCV
            method = str(getattr(cfg, "get_raw", lambda k, d=None: d)("CALIBRATE_METHOD", "sigmoid") or "sigmoid").lower()
            cv = int(getattr(cfg, "get_raw", lambda k, d=None: d)("CALIBRATE_CV", "3") or 3)
            model = CalibratedClassifierCV(model, method=method, cv=cv)
            model.fit(X_train, y_train)
        except Exception as e:
            print(f"⚠️ Calibration skipped: {e}")
        proba = getattr(model, "predict_proba", None)
        y_pred = model.predict(X_test)
        if proba:
            y_prob = model.predict_proba(X_test)[:, 1]
        else:
            y_prob = y_pred.astype(float)
    else:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

    try:
        auc = roc_auc_score(y_test, y_prob)
    except Exception:
        auc = float("nan")
    acc = accuracy_score(y_test, y_pred)

    os.makedirs(MODEL_DIR, exist_ok=True)

    bundle: Dict[str, Any] = {
        "model": model,
        "expected_features": FEATURES,
        "created_at_utc": _utc_iso(),
        "name": name,
    }
    joblib.dump(bundle, path)

    print(f"✅ Saved → {path}")
    print(f"📈 Test Accuracy: {acc:.4f} | AUC: {auc:.4f} | Positives: {pos}/{len(y)}")

    # Feature importance
    if hasattr(model, "feature_importances_"):
        imp = pd.Series(getattr(model, "feature_importances_"), index=FEATURES)
        print("🧠 Top features:")
        print(imp.sort_values(ascending=False).head(6))


# ==========================================================
# MAIN
# ==========================================================
def run_training() -> None:
    print("🧠 TRAINING STARTED")

    if not os.path.exists(DATA_FILE):
        print(f"❌ Missing dataset: {DATA_FILE}")
        return

    df = pd.read_csv(DATA_FILE).fillna(0)

    if "symbol" not in df.columns:
        print("❌ CSV must contain 'symbol'")
        return

    # Prefetch BTC context once
    btc_df = fetch_klines("BTCUSDT", "5m", limit=2000, market_type="spot")
    if btc_df is None or btc_df.empty:
        btc_df = pd.DataFrame({"close": []})

    spot_parts, futures_parts, scalp_parts = [], [], []

    for sym, g in df.groupby("symbol"):
        if len(g) < 150:
            continue

        gg = g.copy().reset_index(drop=True)

        # Rebuild OHLCV numeric types (defensive)
        for c in ["open", "high", "low", "close", "volume"]:
            if c in gg.columns:
                gg[c] = pd.to_numeric(gg[c], errors="coerce")
        gg = gg.dropna(subset=["close", "high", "low"])

        if gg.empty:
            continue

        spot_parts.append(process_group(gg.copy(), btc_df, pump_threshold=0.015, lookahead=5))
        futures_parts.append(process_group(gg.copy(), btc_df, pump_threshold=0.020, lookahead=6))
        scalp_parts.append(process_group(gg.copy(), btc_df, pump_threshold=0.010, lookahead=3))

    if not spot_parts:
        print("❌ No training rows produced. Check dataset quality.")
        return

    df_spot = pd.concat(spot_parts, ignore_index=True)
    df_fut = pd.concat(futures_parts, ignore_index=True)
    df_scalp = pd.concat(scalp_parts, ignore_index=True)

    os.makedirs(MODEL_DIR, exist_ok=True)

    train_model("SPOT_MODEL", os.path.join(MODEL_DIR, "spot_model.pkl"), df_spot)
    train_model("FUTURES_MODEL", os.path.join(MODEL_DIR, "futures_model.pkl"), df_fut)
    train_model("SCALP_MODEL", os.path.join(MODEL_DIR, "scalp_model.pkl"), df_scalp)

    print("\n🎉 TRAINING COMPLETE")


if __name__ == "__main__":
    run_training()
