"""
closed_loop_trainer.py

Closed-loop learning from REAL executed trades:
- Captures feature snapshots into DB table ai_samples when a signal becomes PENDING_APPROVAL.
- Links the sample to a trade when EXECUTE_SIGNAL executes it.
- Labels the sample on trade close (pnl sign) and trains optional *_model_cl.pkl models.

Safe by default:
- If not enough CLOSED samples exist, it prints SKIP and exits 0.
- It does NOT overwrite base models unless CLOSED_LOOP_OVERWRITE_BASE_MODELS=true.
- Bot will only load *_model_cl.pkl if USE_CLOSED_LOOP_MODELS=true.
"""

from __future__ import annotations

import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

try:
    import joblib
except Exception:  # pragma: no cover
    from sklearn.externals import joblib  # type: ignore

# Try XGBoost, fallback to sklearn
try:
    from xgboost import XGBClassifier  # type: ignore
    _HAS_XGB = True
except Exception:
    _HAS_XGB = False

from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.ensemble import GradientBoostingClassifier


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


def _db_file() -> str:
    # Prefer config.cfg.DB_FILE if available, otherwise env DB_FILE, otherwise default
    try:
        from config import cfg  # type: ignore
        v = getattr(cfg, "DB_FILE", None)
        if v:
            return str(v)
    except Exception:
        pass
    return str(os.getenv("DB_FILE", "./bot_data.db"))


def _load_closed_samples(conn: sqlite3.Connection) -> pd.DataFrame:
    q = """
    SELECT
        market_type, algo_mode, timeframe, strategy, side,
        ai_vote, ai_confidence, rule_score, confidence,
        features, pnl, entry_price, exit_price, close_reason, closed_at_ms
    FROM ai_samples
    WHERE status='CLOSED'
      AND features IS NOT NULL
      AND TRIM(features) <> ''
    ORDER BY closed_at_ms ASC
    """
    rows = conn.execute(q).fetchall()
    if not rows:
        return pd.DataFrame()

    cols = [d[0] for d in conn.execute(q).description]  # type: ignore
    df = pd.DataFrame(rows, columns=cols)

    # Parse features JSON
    feats_list: List[Dict] = []
    for s in df["features"].astype(str).tolist():
        try:
            d = json.loads(s) if s else {}
            if not isinstance(d, dict):
                d = {}
        except Exception:
            d = {}
        feats_list.append(d)

    X = pd.DataFrame(feats_list)
    # Ensure schema
    for c in FEATURES:
        if c not in X.columns:
            X[c] = 0.0
    X = X[FEATURES].replace([np.inf, -np.inf], 0.0).fillna(0.0)

    out = pd.concat([df.drop(columns=["features"]), X], axis=1)
    return out


def _target_key(row: pd.Series) -> str:
    mt = str(row.get("market_type") or "").lower().strip()
    am = str(row.get("algo_mode") or "").lower().strip()
    if mt == "spot":
        return "spot"
    if am == "scalp":
        return "scalp"
    return "futures"


def _train_one(df: pd.DataFrame, key: str, model_dir: str) -> Tuple[bool, str]:
    # y = win/loss by pnl sign
    y = (df["pnl"].astype(float) > 0.0).astype(int).values
    X = df[FEATURES].astype(float).values

    # Time split (last 20% test)
    n = len(df)
    split = int(n * 0.8)
    if split < 20:
        return False, "not enough rows for split"
    X_train, y_train = X[:split], y[:split]
    X_test, y_test = X[split:], y[split:]

    if _HAS_XGB:
        clf = XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.06,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_lambda=1.0,
            objective="binary:logistic",
            eval_metric="logloss",
            n_jobs=4,
            random_state=42,
        )
    else:
        clf = GradientBoostingClassifier(random_state=42)

    clf.fit(X_train, y_train)

    # Metrics
    try:
        p = clf.predict_proba(X_test)[:, 1]
        auc = float(roc_auc_score(y_test, p))
    except Exception:
        auc = float("nan")

    try:
        y_hat = clf.predict(X_test)
        acc = float(accuracy_score(y_test, y_hat))
    except Exception:
        acc = float("nan")

    bundle = {
        "model": clf,
        "expected_features": FEATURES,
        "created_at_utc": _utc_iso(),
        "name": f"{key}_model_cl",
        "source": "ai_samples",
        "train_rows": int(len(df)),
        "test_rows": int(len(y_test)),
        "metrics": {"auc": auc, "accuracy": acc},
    }

    os.makedirs(model_dir, exist_ok=True)
    out_path = os.path.join(model_dir, f"{key}_model_cl.pkl")
    joblib.dump(bundle, out_path)

    return True, f"saved={out_path} acc={acc:.4f} auc={auc:.4f} rows={len(df)}"


def main():
    db_file = _db_file()
    min_samples = int(os.getenv("CLOSED_LOOP_MIN_SAMPLES", "200"))
    overwrite_base = str(os.getenv("CLOSED_LOOP_OVERWRITE_BASE_MODELS", "false")).lower() in ("1", "true", "yes")

    conn = sqlite3.connect(db_file)
    try:
        df = _load_closed_samples(conn)
    finally:
        conn.close()

    if df is None or df.empty:
        print("CLOSED-LOOP TRAINING: SKIP (no closed samples yet)")
        return

    # Decide model key per row
    df = df.copy()
    df["model_key"] = df.apply(_target_key, axis=1)

    model_dir = os.path.join(os.getcwd(), "models")

    any_trained = False
    for key in ["spot", "futures", "scalp"]:
        sub = df[df["model_key"] == key].sort_values("closed_at_ms")
        if len(sub) < min_samples:
            print(f"[{key}] SKIP (have {len(sub)} < min_samples={min_samples})")
            continue

        ok, msg = _train_one(sub, key, model_dir)
        print(f"[{key}] {'OK' if ok else 'FAIL'}: {msg}")
        any_trained = any_trained or ok

        if ok and overwrite_base:
            # overwrite base models (optional, risky)
            src = os.path.join(model_dir, f"{key}_model_cl.pkl")
            dst = os.path.join(model_dir, f"{key}_model.pkl")
            try:
                import shutil as _sh
                _sh.copy2(src, dst)
                print(f"[{key}] OVERWROTE base model -> {dst}")
            except Exception as e:
                print(f"[{key}] overwrite failed: {e}")

    if any_trained:
        print("CLOSED-LOOP TRAINING: DONE")
    else:
        print("CLOSED-LOOP TRAINING: SKIP (not enough samples across all keys)")


if __name__ == "__main__":
    main()
