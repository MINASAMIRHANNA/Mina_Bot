#!/usr/bin/env python3
"""Deep Diagnostic (Golden) — quick offline checks.

Usage:
  python deep_diagnostic.py
"""
from __future__ import annotations

import os
import sys
import json
from datetime import datetime, timezone
import pandas as pd

# Make sure root is importable
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from database import DatabaseManager
from feature_extractor import extract_features
from model_manager import ModelManager

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def make_dummy_df(n: int = 300) -> pd.DataFrame:
    import numpy as np
    ts = pd.date_range("2026-01-01", periods=n, freq="1min", tz="UTC")
    price = 100 + np.cumsum(np.random.randn(n)) * 0.1
    hi = price * (1 + np.random.rand(n) * 0.002)
    lo = price * (1 - np.random.rand(n) * 0.002)
    op = price * (1 + (np.random.rand(n)-0.5) * 0.001)
    cl = price
    vol = np.abs(np.random.randn(n) * 1000) + 100
    return pd.DataFrame({"ts": ts, "open": op, "high": hi, "low": lo, "close": cl, "volume": vol})

def main():
    print(f"\n--- 🕵️ DEEP SYSTEM DIAGNOSTIC (UTC {utc_now()}) ---\n")

    errors = []

    # 1) Feature extractor
    try:
        df = make_dummy_df()
        feats = extract_features(df)
        if not isinstance(feats, pd.DataFrame) or feats.empty:
            raise RuntimeError("features empty")
        print(f"✅ Features: Generated {feats.shape[1]} columns, {feats.shape[0]} rows")
    except Exception as e:
        errors.append(f"Feature Extractor failed: {e}")
        print(f"❌ Feature Extractor: {e}")

    # 2) Model manager load
    mm = ModelManager()
    try:
        mm.load_models()
        print("✅ AI Load: Models loaded successfully")
    except Exception as e:
        errors.append(f"Model load failed: {e}")
        print(f"❌ AI Load: {e}")

    # 3) Prediction logic (mode kwarg compatibility)
    try:
        if 'feats' not in locals():
            df = make_dummy_df()
            feats = extract_features(df)
        X = feats.tail(50).copy()
        out = mm.predict(X, market_type="spot", mode="scalp")
        if not isinstance(out, list):
            raise RuntimeError("predict did not return list")
        print(f"✅ AI Logic: predict(mode=...) ok (returned {len(out)} items)")
    except Exception as e:
        errors.append(f"AI Logic failed: {e}")
        print(f"❌ AI Logic: {e}")

    # 4) Database
    try:
        db = DatabaseManager()
        # Basic sanity query
        cur = db.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
        _ = cur.fetchone()
        print("✅ Database: Connection OK")
    except Exception as e:
        errors.append(f"Database failed: {e}")
        print(f"❌ Database: {e}")

    print("\n==============================")
    if errors:
        print("🔴 ERRORS DETECTED:")
        for e in errors:
            print(" -", e)
        print("\nFix the items above before deploying.")
        sys.exit(1)
    else:
        print("🟢 ALL CHECKS PASSED.")
        sys.exit(0)

if __name__ == "__main__":
    main()
