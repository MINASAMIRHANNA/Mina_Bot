import os
import time
from config import cfg
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np

import pandas as pd


def _utc_now_iso() -> str:
    # Always UTC, ISO8601 with Z suffix
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if np.isfinite(v):
            return v
        return default
    except Exception:
        return default


class ModelManager:
    """
    Lightweight model loader/predictor used by main.py.

    Expected behavior (for backward compatibility):
    - Exposes .ready boolean
    - Provides .predict(X_df, market_type, algo_mode) -> List[Dict] (each dict has 'signal')
    - Provides .reload()

    Model files supported (any of these):
    - futures_model.pkl / futures_model.joblib
    - spot_model.pkl    / spot_model.joblib
    - scalp_model.pkl   / scalp_model.joblib

    Each model can be:
    - a sklearn-like estimator (predict / predict_proba)
    - or a dict bundle with keys like {'model': estimator, 'expected_features': [...]}
    """
    DEFAULT_EXPECTED_FEATURES = [
        "qav", "trades", "tbba", "tbqa",
        "ema_20", "ema_50", "ema_200",
        "rsi_14", "atr_14",
        "rsi_lag_1", "rsi_change",
        "pct_chg_5", "dist_ema_50",
        "btc_corr", "btc_trend",
    ]

    def __init__(self, model_dir: Optional[str] = None):
        if model_dir is None:
            # Make model dir stable regardless of current working directory
            try:
                from config import cfg  # local import
                base_dir = getattr(cfg, "BASE_DIR", None) or os.path.dirname(os.path.abspath(__file__))
            except Exception:
                base_dir = os.path.dirname(os.path.abspath(__file__))
            model_dir = os.path.join(str(base_dir), "models")

        self.model_dir = str(model_dir)

        self.futures_model: Any = None
        self.scalp_model: Any = None
        self.spot_model: Any = None

        # If a bundle provides feature order, store per-model
        self._expected_features: Dict[str, List[str]] = {}

        self.ready = False
        os.makedirs(self.model_dir, exist_ok=True)
        self._load_models()

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------
    def _candidate_paths(self, base_name: str) -> List[str]:
        return [
            os.path.join(self.model_dir, f"{base_name}.joblib"),
            os.path.join(self.model_dir, f"{base_name}.pkl"),
        ]

    def _load_one(self, base_name: str) -> Tuple[Any, Optional[List[str]]]:
        for path in self._candidate_paths(base_name):
            if not os.path.exists(path):
                continue
            try:
                obj = joblib.load(path)
                # Bundle support
                if isinstance(obj, dict) and "model" in obj:
                    model = obj.get("model")
                    feats = obj.get("expected_features") or obj.get("features") or obj.get("feature_names")
                    if isinstance(feats, (list, tuple)) and feats:
                        return model, list(feats)
                    # fallback: sklearn attribute
                    return model, self._infer_features_from_model(model)
                # Plain estimator
                return obj, self._infer_features_from_model(obj)
            except Exception as e:
                print(f"[ModelManager] Failed to load {path}: {e}")
        return None, None

    def _infer_features_from_model(self, model: Any) -> Optional[List[str]]:
        try:
            feats = getattr(model, "feature_names_in_", None)
            if feats is not None:
                return list(feats)
        except Exception:
            pass
        return None

    def _load_models(self) -> None:
        self.futures_model, fut_feats = self._load_one("futures_model")
        self.scalp_model, scalp_feats = self._load_one("scalp_model")
        self.spot_model, spot_feats = self._load_one("spot_model")

        self._expected_features = {}
        if fut_feats:
            self._expected_features["futures"] = fut_feats
        if scalp_feats:
            self._expected_features["scalp"] = scalp_feats
        if spot_feats:
            self._expected_features["spot"] = spot_feats

        self.ready = any([self.futures_model is not None, self.scalp_model is not None, self.spot_model is not None])

        loaded = []
        if self.futures_model is not None:
            loaded.append("futures")
        if self.scalp_model is not None:
            loaded.append("scalp")
        if self.spot_model is not None:
            loaded.append("spot")
        print(f"[ModelManager] Models loaded: {', '.join(loaded) if loaded else 'none'}")
        try:
            self.loaded_models = list(loaded)
            self.last_load_utc = _utc_now_iso()
        except Exception:
            pass

    def reload(self) -> None:
        print("🔁 Reloading AI models...")
        self._load_models()

    def load_models(self) -> None:
        """Backward-compatible alias expected by diagnostics (deep_diagnostic / project_doctor)."""
        self.reload()

    # ------------------------------------------------------------------
    # Predict
    # ------------------------------------------------------------------
    def _select_model(self, market_type: str, algo_mode: str) -> Tuple[Any, str]:
        mt = (market_type or "").lower().strip()
        am = (algo_mode or "").lower().strip()

        # Prefer scalp model when algo says scalp
        if am == "scalp" and self.scalp_model is not None:
            return self.scalp_model, "scalp"
        if mt == "futures" and self.futures_model is not None:
            return self.futures_model, "futures"
        if mt in ("spot", "cash") and self.spot_model is not None:
            return self.spot_model, "spot"

        # Fallback order
        if self.spot_model is not None:
            return self.spot_model, "spot"
        if self.futures_model is not None:
            return self.futures_model, "futures"
        if self.scalp_model is not None:
            return self.scalp_model, "scalp"
        return None, "none"

    def _align_features(self, X: pd.DataFrame, model_key: str) -> pd.DataFrame:
        if X is None or getattr(X, "empty", True):
            return pd.DataFrame()

        expected = self._expected_features.get(model_key) or self.DEFAULT_EXPECTED_FEATURES

        Xc = X.copy()
        # Ensure all expected exist
        for col in expected:
            if col not in Xc.columns:
                Xc[col] = 0.0

        # Keep only expected columns in order
        Xc = Xc[expected]

        # Clean
        Xc = Xc.replace([np.inf, -np.inf], 0.0)
        Xc = Xc.fillna(0.0)

        # Ensure numeric
        for col in expected:
            Xc[col] = pd.to_numeric(Xc[col], errors="coerce").fillna(0.0)

        return Xc

    def _predict_prob_long(self, model: Any, X: pd.DataFrame) -> Optional[float]:
        """
        Return probability of LONG/positive class if available.
        """
        if model is None or X is None or X.empty:
            return None
        try:
            if hasattr(model, "predict_proba"):
                proba = model.predict_proba(X)
                # proba shape: (n, n_classes)
                if isinstance(proba, list):
                    proba = np.array(proba)
                if proba.ndim == 2:
                    # If binary, assume class 1 is "positive" (long)
                    if proba.shape[1] >= 2:
                        return _safe_float(proba[0, 1], None)
                    # If only one column, treat as prob long
                    if proba.shape[1] == 1:
                        return _safe_float(proba[0, 0], None)
        except Exception:
            pass
        return None

    def _predict_label(self, model: Any, X: pd.DataFrame) -> Optional[Any]:
        if model is None or X is None or X.empty:
            return None
        try:
            if hasattr(model, "predict"):
                y = model.predict(X)
                if isinstance(y, (list, tuple, np.ndarray)) and len(y) > 0:
                    return y[0]
                return y
        except Exception:
            pass
        return None

    def _map_to_signal(self, prob_long: Optional[float], label: Optional[Any]) -> Tuple[str, float, float, float]:
        """
        Returns: signal, confidence, prob_long, prob_short
        """
        # Use probability if available
        if prob_long is not None:
            prob_long = float(np.clip(prob_long, 0.0, 1.0))
            prob_short = 1.0 - prob_long
            # thresholds can be tuned later via settings; keep simple & stable
            if prob_long >= 0.60:
                return "LONG", prob_long, prob_long, prob_short
            if prob_long <= 0.40:
                # Many current models are trained as a *LONG opportunity* (binary) classifier.
                # In that case, low prob_long means "NO LONG" not necessarily "SHORT".
                if bool(getattr(cfg, 'AI_DISABLE_SHORT', True)):
                    return "WAIT", max(prob_long, prob_short), prob_long, prob_short
                return "SHORT", prob_short, prob_long, prob_short
            return "WAIT", max(prob_long, prob_short), prob_long, prob_short

        # Else use label
        if label is None:
            return "WAIT", 0.0, 0.0, 0.0

        # Common label conventions:
        # -1/0/1, "SHORT"/"LONG"/"WAIT", 0/1, "SELL"/"BUY"
        if isinstance(label, (int, float, np.integer, np.floating)):
            v = float(label)
            if v > 0:
                return "LONG", min(1.0, abs(v)), 0.55, 0.45
            if v < 0:
                return "SHORT", min(1.0, abs(v)), 0.45, 0.55
            return "WAIT", 0.0, 0.0, 0.0

        s = str(label).strip().upper()
        if s in ("LONG", "BUY", "BULL", "UP", "1"):
            return "LONG", 0.55, 0.55, 0.45
        if s in ("SHORT", "SELL", "BEAR", "DOWN", "-1"):
            return "SHORT", 0.55, 0.45, 0.55
        return "WAIT", 0.0, 0.0, 0.0

    def predict(self, X: pd.DataFrame, market_type: str = "spot", algo_mode: str = "swing", mode: str | None = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Predict an AI vote for the latest row in X.

        Returns a list with a single dict to match existing code style:
        [{'signal': 'LONG/SHORT/WAIT', 'confidence': 0.0..1.0, ...}]
        """
        # Backward-compatibility: some callers pass `mode=` instead of `algo_mode=`
        if mode is not None:
            algo_mode = str(mode)
        # Accept legacy kwarg alias as well
        if "strategy_mode" in kwargs and kwargs.get("strategy_mode") is not None:
            algo_mode = str(kwargs.get("strategy_mode"))
        if not self.ready:
            return []

        model, key = self._select_model(market_type, algo_mode)
        if model is None:
            return []

        Xc = self._align_features(X, key)
        if Xc.empty:
            return []

        prob_long = self._predict_prob_long(model, Xc)
        label = None if prob_long is not None else self._predict_label(model, Xc)

        signal, conf, pl, ps = self._map_to_signal(prob_long, label)

        return [{
            "signal": signal,
            "confidence": float(np.clip(conf, 0.0, 1.0)) if conf is not None else 0.0,
            "prob_long": float(pl) if pl is not None else 0.0,
            "prob_short": float(ps) if ps is not None else 0.0,
            "model_used": key,
            "timestamp_utc": _utc_now_iso(),
        }]
