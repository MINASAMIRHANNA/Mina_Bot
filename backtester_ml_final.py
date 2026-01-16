"""
backtester_ml_final.py

A robust ML-assisted backtester for crypto_bot.

Goals:
- Work with the project's existing modules:
  - fetcher.fetch_klines
  - feature_extractor.extract_features
  - model_manager.ModelManager
  - scorer.technical_module_score
  - signal_engine.map_to_signal
- Simulate LONG/SHORT trades on candle close with intrabar SL/TP checks.
- Keep all timestamps in UTC.
- Produce a clear performance summary + optional CSV export.

This is intentionally "simple but correct" — it is a validation tool for the pipeline,
not a high-frequency exchange-grade simulator.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from fetcher import fetch_klines
from feature_extractor import extract_features
from model_manager import ModelManager
from scorer import technical_module_score
from signal_engine import map_to_signal

# -----------------------------
# Defaults (safe)
# -----------------------------
INITIAL_BALANCE_USD = 1000.0
FEE_RATE = 0.0004  # 0.04% per side (approx futures taker). Change as you like.
SL_ATR_MULT = 1.5
TP_ATR_MULT = 2.0
MIN_CONFIDENCE = 0.60
WARMUP_BARS = 220  # enough for EMA200 + indicators


def _utc_iso(ts: Optional[pd.Timestamp] = None) -> str:
    if ts is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(ts, pd.Timestamp):
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.isoformat().replace("+00:00", "Z")
    return str(ts)


def _ensure_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required OHLCV columns exist with float dtype."""
    df = df.copy()
    # common column names
    rename_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc == "open" or lc == "o":
            rename_map[c] = "open"
        elif lc == "high" or lc == "h":
            rename_map[c] = "high"
        elif lc == "low" or lc == "l":
            rename_map[c] = "low"
        elif lc == "close" or lc == "c":
            rename_map[c] = "close"
        elif lc in {"volume", "v"}:
            rename_map[c] = "volume"
        elif lc in {"quote_asset_volume", "qav"}:
            rename_map[c] = "qav"
        elif lc in {"number_of_trades", "trades"}:
            rename_map[c] = "trades"
        elif lc in {"taker_buy_base_asset_volume", "tbba"}:
            rename_map[c] = "tbba"
        elif lc in {"taker_buy_quote_asset_volume", "tbqa"}:
            rename_map[c] = "tbqa"

    df = df.rename(columns=rename_map)

    required = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"OHLCV columns missing: {missing}. Columns: {list(df.columns)}")

    # extra optional columns used by feature extractor/training
    for opt, default in [("qav", 0.0), ("trades", 0.0), ("tbba", 0.0), ("tbqa", 0.0)]:
        if opt not in df.columns:
            df[opt] = default

    for c in ["open", "high", "low", "close", "volume", "qav", "trades", "tbba", "tbqa"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)

    # index as UTC datetime if possible
    if "time" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = df.set_index("time")
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is None:
        df.index = df.index.tz_localize("UTC")

    return df


@dataclass
class Trade:
    symbol: str
    side: str                 # LONG / SHORT
    entry_time_utc: str
    exit_time_utc: str
    entry_price: float
    exit_price: float
    qty: float
    pnl_usd: float
    pnl_pct: float
    fee_usd: float
    reason: str
    confidence: float
    sl: float
    tp: float


@dataclass
class Position:
    side: str
    entry_price: float
    qty: float
    sl: float
    tp: float
    entry_index: int
    entry_time: pd.Timestamp
    confidence: float
    reason: str


def _calc_atr_proxy(df: pd.DataFrame) -> float:
    """ATR proxy from last 14 bars (fallback if features missing)."""
    if len(df) < 15:
        return float("nan")
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    atr = pd.Series(tr).rolling(14).mean().iloc[-1]
    return float(atr) if atr is not None else float("nan")


def _max_drawdown(equity: np.ndarray) -> float:
    peak = -np.inf
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0.0
        if dd > mdd:
            mdd = dd
    return float(mdd)


def _sharpe(returns: np.ndarray, periods_per_year: float = 365.0 * 24.0 * 12.0) -> float:
    # periods_per_year default ≈ 5m candles; caller can override
    if returns.size < 2:
        return 0.0
    mu = np.nanmean(returns)
    sd = np.nanstd(returns)
    if sd == 0 or np.isnan(sd):
        return 0.0
    return float((mu / sd) * math.sqrt(periods_per_year))


def _feature_row(window_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Compute features for the window and return (X_last_row, aux_dict)."""
    feats = extract_features(window_df)
    last = feats.iloc[[-1]].copy()

    # Provide a small aux dict for downstream logic
    aux = {}
    for k in ["ema_20", "ema_50", "ema_200", "rsi_14", "atr_14"]:
        if k in last.columns:
            aux[k] = float(last[k].iloc[0]) if pd.notna(last[k].iloc[0]) else float("nan")

    # Some extractors may not compute atr_14 early; fallback
    if "atr_14" not in aux or np.isnan(aux.get("atr_14", np.nan)):
        aux["atr_14"] = _calc_atr_proxy(window_df)

    return last, aux


def backtest(
    symbol: str,
    interval: str,
    market_type: str = "futures",
    algo_mode: str = "scalp",
    limit: int = 1200,
    initial_balance_usd: float = INITIAL_BALANCE_USD,
    allow_short: bool = True,
    min_confidence: float = MIN_CONFIDENCE,
    sl_atr_mult: float = SL_ATR_MULT,
    tp_atr_mult: float = TP_ATR_MULT,
    fee_rate: float = FEE_RATE,
    export_csv: Optional[str] = None,
) -> Dict[str, object]:
    """
    Returns dict with summary + trades DataFrame + equity curve DataFrame.
    """
    raw = fetch_klines(symbol=symbol, interval=interval, limit=limit, market_type=market_type)
    df = _ensure_ohlcv(raw)
    if len(df) < WARMUP_BARS + 50:
        raise ValueError(f"Not enough candles ({len(df)}). Increase limit or change WARMUP_BARS.")

    model = ModelManager("./models")

    balance = float(initial_balance_usd)
    equity = []
    equity_times = []
    trades: List[Trade] = []
    pos: Optional[Position] = None

    # Determine annualization based on interval (rough)
    interval_map = {"1m": 365 * 24 * 60, "3m": 365 * 24 * 20, "5m": 365 * 24 * 12,
                    "15m": 365 * 24 * 4, "30m": 365 * 24 * 2, "1h": 365 * 24,
                    "4h": 365 * 6, "1d": 365}
    periods_per_year = float(interval_map.get(interval, 365 * 24 * 12))

    # Sim loop
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    idx = df.index

    for i in range(WARMUP_BARS, len(df)):
        ts = idx[i]
        price_close = float(closes[i])
        price_high = float(highs[i])
        price_low = float(lows[i])

        # Mark equity (unrealized not included for simplicity)
        equity.append(balance)
        equity_times.append(ts)

        # --- Exit logic first (intrabar SL/TP) ---
        if pos is not None:
            exited = False
            exit_price = None
            exit_reason = ""

            if pos.side == "LONG":
                if price_low <= pos.sl:
                    exit_price = pos.sl
                    exit_reason = "SL"
                    exited = True
                elif price_high >= pos.tp:
                    exit_price = pos.tp
                    exit_reason = "TP"
                    exited = True
            else:  # SHORT
                if price_high >= pos.sl:
                    exit_price = pos.sl
                    exit_reason = "SL"
                    exited = True
                elif price_low <= pos.tp:
                    exit_price = pos.tp
                    exit_reason = "TP"
                    exited = True

            if exited and exit_price is not None:
                # Fees on entry + exit
                notional_entry = pos.entry_price * pos.qty
                notional_exit = exit_price * pos.qty
                fee = (notional_entry + notional_exit) * fee_rate

                pnl = (exit_price - pos.entry_price) * pos.qty if pos.side == "LONG" else (pos.entry_price - exit_price) * pos.qty
                pnl_after_fee = pnl - fee
                balance += pnl_after_fee

                trades.append(
                    Trade(
                        symbol=symbol,
                        side=pos.side,
                        entry_time_utc=_utc_iso(pos.entry_time),
                        exit_time_utc=_utc_iso(ts),
                        entry_price=pos.entry_price,
                        exit_price=float(exit_price),
                        qty=pos.qty,
                        pnl_usd=float(pnl_after_fee),
                        pnl_pct=float(pnl_after_fee / notional_entry) if notional_entry > 0 else 0.0,
                        fee_usd=float(fee),
                        reason=f"{pos.reason}->{exit_reason}",
                        confidence=float(pos.confidence),
                        sl=float(pos.sl),
                        tp=float(pos.tp),
                    )
                )
                pos = None

        # --- Signal/Entry logic on close ---
        # Build window (we feed extractor with history up to i)
        window = df.iloc[: i + 1]
        X_last, aux = _feature_row(window)

        # AI prediction (defensive)
        ai_signal = "WAIT"
        confidence = 0.0
        if getattr(model, "ready", False):
            try:
                pred = model.predict(X_last, market_type=market_type, mode=algo_mode)
                if pred and isinstance(pred, list) and len(pred) > 0:
                    ai_signal = str(pred[0].get("signal", "WAIT")).upper()
                    confidence = float(pred[0].get("confidence", 0.0))
            except Exception:
                ai_signal = "WAIT"
                confidence = 0.0

        # Technical confirmation
        ema_fast = aux.get("ema_20", np.nan)
        ema_slow = aux.get("ema_50", np.nan)
        rsi_14 = aux.get("rsi_14", np.nan)
        atr_14 = aux.get("atr_14", np.nan)

        # ADX and volume spike aren't guaranteed in features; approximate as 0
        adx_val = float(X_last["adx_14"].iloc[0]) if "adx_14" in X_last.columns and pd.notna(X_last["adx_14"].iloc[0]) else 0.0
        vol_spike = int(X_last["vol_spike"].iloc[0]) if "vol_spike" in X_last.columns and pd.notna(X_last["vol_spike"].iloc[0]) else 0

        tech_score = technical_module_score(ema_fast, ema_slow, rsi_14, vol_spike, adx_val)
        tech_signal = map_to_signal(tech_score)  # STRONG_LONG/LONG/STRONG_SHORT/SHORT/NEUTRAL

        # Normalize tech signal to LONG/SHORT/WAIT
        tech_dir = "WAIT"
        if "LONG" in tech_signal:
            tech_dir = "LONG"
        elif "SHORT" in tech_signal:
            tech_dir = "SHORT"

        # Entry decision: require min confidence, and avoid conflicting tech
        if pos is None:
            # Size: use full balance as notional for simplicity (1x). You can change to risk-based sizing.
            notional = balance
            qty = notional / price_close if price_close > 0 else 0.0
            if qty <= 0:
                continue

            if ai_signal == "LONG" and confidence >= min_confidence and tech_dir != "SHORT":
                if np.isnan(atr_14) or atr_14 <= 0:
                    continue
                sl = price_close - sl_atr_mult * atr_14
                tp = price_close + tp_atr_mult * atr_14
                pos = Position(
                    side="LONG",
                    entry_price=price_close,
                    qty=qty,
                    sl=float(sl),
                    tp=float(tp),
                    entry_index=i,
                    entry_time=ts,
                    confidence=confidence,
                    reason=f"AI_LONG({confidence:.2f})+TECH({tech_signal})",
                )
            elif allow_short and ai_signal == "SHORT" and confidence >= min_confidence and tech_dir != "LONG":
                if np.isnan(atr_14) or atr_14 <= 0:
                    continue
                sl = price_close + sl_atr_mult * atr_14
                tp = price_close - tp_atr_mult * atr_14
                pos = Position(
                    side="SHORT",
                    entry_price=price_close,
                    qty=qty,
                    sl=float(sl),
                    tp=float(tp),
                    entry_index=i,
                    entry_time=ts,
                    confidence=confidence,
                    reason=f"AI_SHORT({confidence:.2f})+TECH({tech_signal})",
                )

    # Close at end (mark-to-market)
    if pos is not None:
        ts = idx[-1]
        exit_price = float(closes[-1])
        notional_entry = pos.entry_price * pos.qty
        notional_exit = exit_price * pos.qty
        fee = (notional_entry + notional_exit) * fee_rate
        pnl = (exit_price - pos.entry_price) * pos.qty if pos.side == "LONG" else (pos.entry_price - exit_price) * pos.qty
        pnl_after_fee = pnl - fee
        balance += pnl_after_fee

        trades.append(
            Trade(
                symbol=symbol,
                side=pos.side,
                entry_time_utc=_utc_iso(pos.entry_time),
                exit_time_utc=_utc_iso(ts),
                entry_price=pos.entry_price,
                exit_price=exit_price,
                qty=pos.qty,
                pnl_usd=float(pnl_after_fee),
                pnl_pct=float(pnl_after_fee / notional_entry) if notional_entry > 0 else 0.0,
                fee_usd=float(fee),
                reason=f"{pos.reason}->EOD",
                confidence=float(pos.confidence),
                sl=float(pos.sl),
                tp=float(pos.tp),
            )
        )
        pos = None

    # Build outputs
    trades_df = pd.DataFrame([asdict(t) for t in trades])
    equity_df = pd.DataFrame({"time_utc": pd.to_datetime(equity_times, utc=True), "equity_usd": np.array(equity, dtype=float)})

    # Perf metrics
    total_return = (balance - initial_balance_usd) / initial_balance_usd if initial_balance_usd > 0 else 0.0
    wins = int((trades_df["pnl_usd"] > 0).sum()) if not trades_df.empty else 0
    losses = int((trades_df["pnl_usd"] <= 0).sum()) if not trades_df.empty else 0
    win_rate = wins / max(wins + losses, 1)

    eq = equity_df["equity_usd"].values if not equity_df.empty else np.array([initial_balance_usd], dtype=float)
    mdd = _max_drawdown(eq)
    rets = pd.Series(eq).pct_change().dropna().values
    sharpe = _sharpe(rets, periods_per_year=periods_per_year)

    summary = {
        "symbol": symbol,
        "interval": interval,
        "market_type": market_type,
        "algo_mode": algo_mode,
        "initial_balance_usd": float(initial_balance_usd),
        "final_balance_usd": float(balance),
        "total_return_pct": float(total_return * 100.0),
        "trades": int(len(trades)),
        "win_rate_pct": float(win_rate * 100.0),
        "max_drawdown_pct": float(mdd * 100.0),
        "sharpe": float(sharpe),
        "generated_at_utc": _utc_iso(),
    }

    if export_csv:
        trades_df.to_csv(export_csv, index=False)

    return {"summary": summary, "trades": trades_df, "equity": equity_df}


def main() -> None:
    p = argparse.ArgumentParser(description="ML backtester for crypto_bot (UTC).")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="5m")
    p.add_argument("--market_type", default="futures", choices=["spot", "futures"])
    p.add_argument("--mode", default="scalp", choices=["scalp", "swing"])
    p.add_argument("--limit", type=int, default=1200)
    p.add_argument("--initial", type=float, default=INITIAL_BALANCE_USD)
    p.add_argument("--allow_short", action="store_true", default=False)
    p.add_argument("--min_conf", type=float, default=MIN_CONFIDENCE)
    p.add_argument("--sl_atr", type=float, default=SL_ATR_MULT)
    p.add_argument("--tp_atr", type=float, default=TP_ATR_MULT)
    p.add_argument("--fee", type=float, default=FEE_RATE)
    p.add_argument("--export_trades_csv", default=None)
    args = p.parse_args()

    out = backtest(
        symbol=args.symbol,
        interval=args.interval,
        market_type=args.market_type,
        algo_mode=args.mode,
        limit=args.limit,
        initial_balance_usd=args.initial,
        allow_short=args.allow_short,
        min_confidence=args.min_conf,
        sl_atr_mult=args.sl_atr,
        tp_atr_mult=args.tp_atr,
        fee_rate=args.fee,
        export_csv=args.export_trades_csv,
    )

    print("\n=== BACKTEST SUMMARY (UTC) ===")
    for k, v in out["summary"].items():
        print(f"{k}: {v}")

    if out["export_trades_csv"]:
        print(f"\nTrades exported to: {out['export_trades_csv']}")

    print("\nSample trades:")
    trades_df: pd.DataFrame = out["trades"]
    if trades_df.empty:
        print("(no trades)")
    else:
        print(trades_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
