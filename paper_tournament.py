import time
import threading
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime, timezone

from binance.client import Client
from config import cfg

from signal_engine import combine_scores, map_to_signal
from pump_detector import detect_pre_pump

try:
    from fetcher import fetch_klines_window
except Exception:
    fetch_klines_window = None



def _truthy(v: Any) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on"}


def _gs_bool(db, key: str, default: bool = False) -> bool:
    try:
        v = db.get_setting(key)
        if v is None:
            return bool(default)
        return _truthy(v)
    except Exception:
        return bool(default)


def _normalize_signal(sig: str) -> str:
    if not sig:
        return "NEUTRAL"
    s = str(sig).upper()
    if "LONG" in s:
        return "LONG"
    if "SHORT" in s:
        return "SHORT"
    return "NEUTRAL"


HORIZONS_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}

INTERVAL_MS = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}


@dataclass
class PaperDecision:
    symbol: str
    market_type: str
    interval: str
    strategy_tag: str
    decision: str
    entry_price: float
    signal_time_ms: int
    final_score: float
    pump_score: int
    confidence: float
    extra: Dict[str, Any]


def generate_paper_decisions(
    *,
    symbol: str,
    market_type: str,
    interval: str,
    df,
    price: float,
    tech_score: float,
    deriv_score: float,
    vol_spike: int,
    ai_vote: str = "OFF",
    ai_confidence: float = 0.0,
    funding: float = 0.0,
    oi_change: float = 0.0,
    settings_getter=None,
) -> List[PaperDecision]:
    """Create paper decisions for multiple strategy tags at the same candle close."""

    def gs(key: str, default: Any):
        if settings_getter is None:
            return default
        v = settings_getter(key)
        return default if v is None else v

    # timestamp from last candle index (already close time)
    try:
        last_dt = df.index[-1].to_pydatetime()
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        signal_time_ms = int(last_dt.timestamp() * 1000)
    except Exception:
        signal_time_ms = int(time.time() * 1000)

    pump_score = int(detect_pre_pump(df, price) or 0)

    scores = {"technical": float(tech_score or 0.0), "derivatives": float(deriv_score or 0.0), "volume_spike": int(vol_spike or 0)}
    scalp_score = float(combine_scores(scores, "scalp", market_type))
    swing_score = float(combine_scores(scores, "swing", market_type))

    scalp_sig = _normalize_signal(map_to_signal(scalp_score))
    swing_sig = _normalize_signal(map_to_signal(swing_score))

    out: List[PaperDecision] = []
    out.append(PaperDecision(
        symbol=symbol, market_type=market_type, interval=interval,
        strategy_tag="scalp", decision=scalp_sig,
        entry_price=float(price), signal_time_ms=signal_time_ms,
        final_score=scalp_score, pump_score=pump_score,
        confidence=float(ai_confidence or 0.0),
        extra={"ai_vote": ai_vote, "funding": funding, "oi_change": oi_change}
    ))
    out.append(PaperDecision(
        symbol=symbol, market_type=market_type, interval=interval,
        strategy_tag="swing", decision=swing_sig,
        entry_price=float(price), signal_time_ms=signal_time_ms,
        final_score=swing_score, pump_score=pump_score,
        confidence=float(ai_confidence or 0.0),
        extra={"ai_vote": ai_vote, "funding": funding, "oi_change": oi_change}
    ))

    # Spot pump hunting (long-only paper)
    if market_type == "spot":
        pump_min = int(gs("pump_score_min", 75))
        sig = "LONG" if pump_score >= pump_min else "NEUTRAL"
        out.append(PaperDecision(
            symbol=symbol, market_type=market_type, interval=interval,
            strategy_tag="pump_spot", decision=sig,
            entry_price=float(price), signal_time_ms=signal_time_ms,
            final_score=float(pump_score), pump_score=pump_score,
            confidence=float(pump_score),
            extra={"ai_vote": ai_vote}
        ))

    # Futures squeeze long-only (optional)
    if market_type == "futures":
        enabled = str(gs("fut_pump_enabled", "FALSE")).upper() == "TRUE"
        if enabled:
            ps_min = int(gs("fut_pump_score_min", gs("pump_score_min", 75)))
            oi_min = float(gs("fut_pump_oi_min", 0.015))
            funding_max = float(gs("fut_pump_funding_max", -0.0005))
            sig = "LONG" if (pump_score >= ps_min and float(oi_change or 0) >= oi_min and float(funding or 0) <= funding_max) else "NEUTRAL"
            out.append(PaperDecision(
                symbol=symbol, market_type=market_type, interval=interval,
                strategy_tag="squeeze_long", decision=sig,
                entry_price=float(price), signal_time_ms=signal_time_ms,
                final_score=float(pump_score), pump_score=pump_score,
                confidence=float(pump_score),
                extra={"ai_vote": ai_vote, "funding": funding, "oi_change": oi_change}
            ))

    return out


# ==========================================================
# RECOMMENDATIONS GENERATOR (SAFE)
# ==========================================================
def generate_recommendations(
    db,
    *,
    lookback_hours: int = 24,
    max_recs: int = 6,
    market_type: Optional[str] = None,
    horizon: Optional[str] = None,
    force: bool = True,
):
    """Generate safe, *actionable* recommendations and write them to DB.

    Notes:
      - Uses PAPER results stored in analysis_results + rejections.
      - Tries to work even with small sample sizes by auto-selecting
        the best available evaluated horizon (4h → 1h → 15m).
      - All suggestions are small (±1/±2) to avoid destabilizing the bot.
    """

    def _sfloat(x: Any, default: float = 0.0) -> float:
        try:
            return float(x)
        except Exception:
            return float(default)

    def _sint(x: Any, default: int = 0) -> int:
        try:
            return int(float(x))
        except Exception:
            return int(default)

    def gs(key: str, default: Any):
        try:
            v = db.get_setting(key)
            return default if v is None else v
        except Exception:
            return default

    def clamp_float(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(v)))

    def clamp_int(v: int, lo: int, hi: int) -> int:
        return max(lo, min(hi, int(v)))

    lookback_hours = clamp_int(_sint(lookback_hours, 24), 1, 24 * 30)
    max_recs = clamp_int(_sint(max_recs, 6), 1, 20)

    # Gate: allow disabling recommendations completely
    if not force:
        enabled = str(gs("paper_reco_enabled", "TRUE")).upper() == "TRUE"
        if not enabled:
            return {"status": "disabled", "count": 0, "items": []}

    now = datetime.now(timezone.utc)
    date_utc = now.strftime("%Y-%m-%d")
    end_ms = int(now.timestamp() * 1000)
    start_ms = end_ms - (lookback_hours * 60 * 60 * 1000)

    # Configurable minimums (defaults are intentionally low, so it starts working early)
    min_total = clamp_int(_sint(gs("reco_min_total", 10), 10), 5, 500)
    min_eval = clamp_int(_sint(gs("reco_min_evaluated", 8), 8), 3, 500)
    min_eval_strategy = clamp_int(_sint(gs("reco_min_eval_per_strategy", 6), 6), 3, 200)
    min_rej = clamp_int(_sint(gs("reco_rej_min", 20), 20), 10, 5000)

    # Optional forced horizon from caller
    if horizon is not None and horizon not in ("15m", "1h", "4h"):
        horizon = None

    if market_type is not None and market_type not in ("futures", "spot"):
        market_type = None

    recs: List[Dict[str, Any]] = []

    def add_rec(key: str, cur_val: Any, sug_val: Any, category: str, reason: str, score: float):
        # prevent duplicates per day
        for r in recs:
            if r.get("key") == key:
                return
        recs.append({
            "key": key,
            "current_value": str(cur_val),
            "suggested_value": str(sug_val),
            "category": category,
            "reason": reason,
            "score": float(clamp_float(score, 0.10, 0.95)),
        })

    def pick_horizon(agg: Dict[str, Any]) -> str:
        if horizon:
            return horizon
        # choose best evaluated horizon
        candidates = [("4h", _sint(agg.get("w4")) + _sint(agg.get("l4"))),
                      ("1h", _sint(agg.get("w1")) + _sint(agg.get("l1"))),
                      ("15m", _sint(agg.get("w15")) + _sint(agg.get("l15")))]
        for h, n in candidates:
            if n >= min_eval:
                return h
        # fallback: max available
        return max(candidates, key=lambda x: x[1])[0]

    # -------------------------------
    # A) Overall paper performance
    # -------------------------------
    params = [start_ms, end_ms]
    where_market = ""
    if market_type:
        where_market = " AND market_type=? "
        params.append(market_type)

    with db.lock:
        db.cursor.execute(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN decision NOT IN ('NEUTRAL','HOLD','SKIP','') THEN 1 ELSE 0 END) AS signals,

                SUM(CASE WHEN outcome_15m='WIN'  THEN 1 ELSE 0 END) AS w15,
                SUM(CASE WHEN outcome_15m='LOSS' THEN 1 ELSE 0 END) AS l15,
                SUM(CASE WHEN outcome_1h='WIN'  THEN 1 ELSE 0 END) AS w1,
                SUM(CASE WHEN outcome_1h='LOSS' THEN 1 ELSE 0 END) AS l1,
                SUM(CASE WHEN outcome_4h='WIN'  THEN 1 ELSE 0 END) AS w4,
                SUM(CASE WHEN outcome_4h='LOSS' THEN 1 ELSE 0 END) AS l4
            FROM analysis_results
            WHERE signal_time_ms BETWEEN ? AND ?
            {where_market}
        """, tuple(params))
        agg = db.cursor.fetchone() or {}

    total = _sint(agg.get("total"), 0)
    signals = _sint(agg.get("signals"), 0)
    signal_rate = (signals / max(1, total)) * 100.0

    chosen = pick_horizon(agg)
    if chosen == "4h":
        w, l = _sint(agg.get("w4")), _sint(agg.get("l4"))
    elif chosen == "1h":
        w, l = _sint(agg.get("w1")), _sint(agg.get("l1"))
    else:
        w, l = _sint(agg.get("w15")), _sint(agg.get("l15"))

    eval_n = w + l
    winrate = (w / max(1, eval_n)) * 100.0

    # Global tuning (small steps only)
    if total >= min_total and eval_n >= min_eval:
        if winrate < 45.0:
            cur_adx = _sint(gs("gate_adx_min", 10), 10)
            add_rec(
                "gate_adx_min",
                cur_adx,
                clamp_int(cur_adx + 1, 8, 35),
                "risk",
                f"Paper performance weak on {chosen} horizon (win-rate {winrate:.1f}% over {eval_n} evals). Tighten ADX trend filter slightly.",
                0.78,
            )

            cur_rsi = _sint(gs("rsi_max_buy", 70), 70)
            add_rec(
                "rsi_max_buy",
                cur_rsi,
                clamp_int(cur_rsi - 1, 60, 90),
                "risk",
                f"Losses are high on {chosen}: tighten RSI ceiling a bit to reduce overbought entries.",
                0.62,
            )

        # good performance but too few signals
        if winrate >= 60.0 and signal_rate < 6.0:
            cur_adx = _sint(gs("gate_adx_min", 10), 10)
            add_rec(
                "gate_adx_min",
                cur_adx,
                clamp_int(cur_adx - 1, 8, 35),
                "tuning",
                f"Strong paper performance (win-rate {winrate:.1f}% on {chosen}) but low signal-rate ({signal_rate:.1f}%). Loosen ADX slightly to catch more trades.",
                0.70,
            )

    # -------------------------------
    # B) Strategy-specific
    # -------------------------------
    params2 = [start_ms, end_ms]
    where_market2 = ""
    if market_type:
        where_market2 = " AND market_type=? "
        params2.append(market_type)

    # choose outcome column based on chosen horizon
    out_col = {"15m": "outcome_15m", "1h": "outcome_1h", "4h": "outcome_4h"}.get(chosen, "outcome_1h")

    with db.lock:
        db.cursor.execute(f"""
            SELECT
                strategy_tag,
                market_type,
                SUM(CASE WHEN {out_col}='WIN'  THEN 1 ELSE 0 END) AS w,
                SUM(CASE WHEN {out_col}='LOSS' THEN 1 ELSE 0 END) AS l,
                SUM(CASE WHEN decision NOT IN ('NEUTRAL','HOLD','SKIP','') THEN 1 ELSE 0 END) AS signals
            FROM analysis_results
            WHERE signal_time_ms BETWEEN ? AND ?
              AND strategy_tag IS NOT NULL
            {where_market2}
            GROUP BY strategy_tag, market_type
        """, tuple(params2))
        strat_rows = db.cursor.fetchall() or []

    def wr(wi: int, lo: int) -> float:
        return (wi / max(1, (wi + lo))) * 100.0

    # futures squeeze enable/disable and thresholds
    for r in strat_rows:
        try:
            tag = (r["strategy_tag"] or "")
            mt = (r["market_type"] or "")
            wi = _sint(r.get("w"), 0)
            lo = _sint(r.get("l"), 0)
            n = wi + lo
            if tag == "squeeze_long" and (market_type in (None, "futures")) and mt == "futures" and n >= min_eval_strategy:
                cur_en = str(gs("fut_pump_enabled", "FALSE")).upper()
                wrr = wr(wi, lo)
                if wrr >= 60.0 and cur_en != "TRUE":
                    add_rec(
                        "fut_pump_enabled", cur_en, "TRUE", "strategy",
                        f"squeeze_long performing well on {chosen} (win-rate {wrr:.1f}% over {n}). Enable it.",
                        0.72,
                    )
                if wrr <= 45.0 and cur_en == "TRUE":
                    add_rec(
                        "fut_pump_enabled", cur_en, "FALSE", "strategy",
                        f"squeeze_long underperforming on {chosen} (win-rate {wrr:.1f}% over {n}). Disable it to reduce noise.",
                        0.72,
                    )

                cur_ps = _sint(gs("fut_pump_score_min", gs("pump_score_min", 75)), 75)
                if wrr >= 58.0 and n < 40:
                    add_rec(
                        "fut_pump_score_min", cur_ps, clamp_int(cur_ps - 1, 60, 95), "tuning",
                        f"squeeze_long good win-rate {wrr:.1f}% → slightly lower fut_pump_score_min to find more setups.",
                        0.68,
                    )
                elif wrr < 48.0:
                    add_rec(
                        "fut_pump_score_min", cur_ps, clamp_int(cur_ps + 1, 60, 95), "risk",
                        f"squeeze_long weak win-rate {wrr:.1f}% → slightly raise fut_pump_score_min to filter weak squeezes.",
                        0.68,
                    )

            # spot pump threshold
            if tag == "pump_spot" and (market_type in (None, "spot")) and mt == "spot" and n >= min_eval_strategy:
                wrr = wr(wi, lo)
                cur_ps = _sint(gs("pump_score_min", 75), 75)
                if wrr >= 60.0 and n < 60:
                    add_rec(
                        "pump_score_min", cur_ps, clamp_int(cur_ps - 1, 60, 95), "tuning",
                        f"pump_spot strong on {chosen} (win-rate {wrr:.1f}% over {n}). Lower pump_score_min slightly for more early pumps.",
                        0.64,
                    )
                elif wrr <= 45.0:
                    add_rec(
                        "pump_score_min", cur_ps, clamp_int(cur_ps + 1, 60, 95), "risk",
                        f"pump_spot weak on {chosen} (win-rate {wrr:.1f}% over {n}). Raise pump_score_min slightly to cut noise.",
                        0.64,
                    )

        except Exception:
            continue

    # -------------------------------
    # C) Rejection-driven tuning (last 24h)
    # -------------------------------
    try:
        with db.lock:
            db.cursor.execute("""
                SELECT reason, COUNT(*) c
                FROM rejections
                WHERE timestamp >= datetime('now','-1 day')
                GROUP BY reason
                ORDER BY c DESC
                LIMIT 5
            """)
            rej_rows = db.cursor.fetchall() or []
    except Exception:
        rej_rows = []

    for rr in rej_rows:
        try:
            reason = str(rr["reason"] or "").upper()
            c = _sint(rr["c"], 0)
            if c < min_rej:
                continue

            # RSI too strict → loosen (only if not clearly losing)
            if "RSI" in reason and (eval_n < min_eval or winrate >= 45.0):
                cur = _sint(gs("rsi_max_buy", 70), 70)
                add_rec(
                    "rsi_max_buy", cur, clamp_int(cur + 1, 60, 90), "tuning",
                    f"High RSI rejections (c={c}) in last 24h. Loosen RSI ceiling slightly to avoid missing valid entries.",
                    0.60,
                )

            # ADX gate too strict → loosen ONLY when performance is decent
            if ("ADX" in reason or "TREND" in reason) and (eval_n < min_eval or winrate >= 55.0):
                cur = _sint(gs("gate_adx_min", 10), 10)
                add_rec(
                    "gate_adx_min", cur, clamp_int(cur - 1, 8, 35), "tuning",
                    f"Many ADX/trend rejections (c={c}) while performance isn't bad. Loosen ADX slightly to increase trade flow.",
                    0.58,
                )

            # Pump score blocks
            if ("PUMP" in reason or "SCORE" in reason):
                cur = _sint(gs("pump_score_min", 75), 75)
                if eval_n >= min_eval and winrate < 50.0:
                    add_rec(
                        "pump_score_min", cur, clamp_int(cur + 1, 60, 95), "risk",
                        f"Many pump-score rejections (c={c}) but performance is weak. Raise pump_score_min slightly to filter noise.",
                        0.56,
                    )
                else:
                    add_rec(
                        "pump_score_min", cur, clamp_int(cur - 1, 60, 95), "tuning",
                        f"Many pump-score rejections (c={c}). Lower pump_score_min slightly to catch earlier pumps.",
                        0.56,
                    )

        except Exception:
            continue

    # finalize + upsert
    recs = sorted(recs, key=lambda x: float(x.get("score", 0)), reverse=True)[:max_recs]
    db.upsert_recommendations(date_utc, recs)
    return {"status": "ok", "date_utc": date_utc, "count": len(recs), "items": recs, "horizon": chosen, "lookback_hours": lookback_hours}


def run_daily_paper_tournament(db, *, allow_reco: bool = True):
    """
    Daily paper job:
    - Evaluate pending paper signals (15m / 1h / 4h)
    - Create tuning recommendations (PENDING) for dashboard approve/reject
    """

    # --------------------------------------------------
    # Recommendations gate (once per UTC day AFTER HH:MM)
    # allow_reco=False is used by the scheduler for frequent evaluation runs.
    # --------------------------------------------------
    run_reco = False
    if allow_reco:
        try:
            hhmm = db.get_setting("paper_daily_time_utc") or "00:05"
            try:
                hh, mm = (hhmm or "00:05").strip().split(":")
                hh = int(hh)
                mm = int(mm)
            except Exception:
                hh, mm = 0, 5

            now_utc = datetime.now(timezone.utc)
            today = now_utc.strftime("%Y-%m-%d")
            last = (db.get_setting("paper_daily_last_run_utc") or db.get_setting("paper_daily_last_reco_utc") or "")

            # Feature gates
            if not _gs_bool(db, "paper_daily_enabled", True):
                run_reco = False
                raise Exception("paper_daily_enabled=FALSE")
            if not _gs_bool(db, "paper_tournament_enabled", _truthy(getattr(cfg, "PAPER_TOURNAMENT_ENABLED", True))):
                run_reco = False
                raise Exception("paper_tournament_enabled=FALSE")

            after_time = (now_utc.hour > hh) or (now_utc.hour == hh and now_utc.minute >= mm)
            if (today != last) and after_time:
                run_reco = True
                db.set_setting("paper_daily_last_run_utc", today)
                try:
                    db.set_setting("paper_daily_last_reco_utc", today)  # backward compat
                except Exception:
                    pass
        except Exception:
            run_reco = False

    if fetch_klines_window is None:
        try:
            db.log("[PAPER] fetch_klines_window not available; will fallback to Binance REST", level="WARN")
        except Exception:
            pass

    def pick_close(symbol, interval, market_type, end_ms):
        # Preferred: use fetcher (pandas) for consistent UTC handling
        if fetch_klines_window is not None:
            df = fetch_klines_window(symbol, interval, end_ms=end_ms, limit=2, market_type=market_type)
            if df is None or getattr(df, "empty", False):
                return None
            try:
                return float(df["close"].iloc[-1])
            except Exception:
                return None

        # Fallback: direct Binance REST (minimal)
        try:
            # cache one client instance
            if not hasattr(pick_close, "_client"):
                try:
                    pick_close._client = Client(cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET, testnet=cfg.USE_TESTNET)
                except Exception:
                    pick_close._client = Client(None, None, testnet=cfg.USE_TESTNET)
            c = pick_close._client
            step = INTERVAL_MS.get(interval, 5 * 60 * 1000)
            start_ms = int(end_ms) - (step * 3)
            if market_type == "spot":
                k = c.get_klines(symbol=symbol, interval=interval, startTime=start_ms, endTime=int(end_ms), limit=3)
            else:
                k = c.futures_klines(symbol=symbol, interval=interval, startTime=start_ms, endTime=int(end_ms), limit=3)
            if not k:
                return None
            return float(k[-1][4])
        except Exception:
            return None


    def pnl_pct(decision, entry, price):
        if not entry or entry <= 0 or not price or price <= 0:
            return 0.0
        d = (decision or "").upper()
        if d in ("NEUTRAL", "HOLD", "SKIP", ""):
            return 0.0
        if "SHORT" in d or d == "SELL":
            return (entry - price) / entry
        return (price - entry) / entry

    def to_outcome(ret_pct, decision):
        d = (decision or "").upper()
        if d in ("NEUTRAL", "HOLD", "SKIP", ""):
            return "SKIP"
        if ret_pct > 0:
            return "WIN"
        if ret_pct < 0:
            return "LOSS"
        return "FLAT"

    # -----------------------------------------
    # 1) Evaluate pending paper signals
    # -----------------------------------------
    if not _gs_bool(db, "paper_eval_enabled", True):
        try:
            db.log("[PAPER] paper_eval_enabled=FALSE (skip evaluation)", level="INFO")
        except Exception:
            pass
        pending = []
    else:
        pending = db.get_pending_paper_evaluations(limit=400) or []
    updated = 0

    for row in pending:
        try:
            r = dict(row)
            rec_id = int(r.get("id"))

            symbol = r.get("symbol")
            market_type = (r.get("market_type") or "futures")
            interval = (r.get("interval") or "5m")
            decision = r.get("decision") or "NEUTRAL"
            entry = float(r.get("entry_price") or 0.0)
            t0 = int(r.get("signal_time_ms") or 0)
            if not symbol or t0 <= 0 or entry <= 0:
                continue

            updates = {}

            t15 = t0 + 15 * 60 * 1000
            t1h = t0 + 60 * 60 * 1000
            t4h = t0 + 4 * 60 * 60 * 1000

            if not r.get("price_after_15m"):
                p15 = pick_close(symbol, interval, market_type, t15)
                if p15:
                    updates["price_after_15m"] = p15
                    ret15 = pnl_pct(decision, entry, p15)
                    updates["outcome_15m"] = to_outcome(ret15, decision)

            if not r.get("price_after_1h"):
                p1 = pick_close(symbol, interval, market_type, t1h)
                if p1:
                    updates["price_after_1h"] = p1
                    ret1h = pnl_pct(decision, entry, p1)
                    updates["outcome_1h"] = to_outcome(ret1h, decision)

            if not r.get("price_after_4h"):
                p4 = pick_close(symbol, interval, market_type, t4h)
                if p4:
                    updates["price_after_4h"] = p4
                    ret4h = pnl_pct(decision, entry, p4)
                    updates["outcome_4h"] = to_outcome(ret4h, decision)

            if updates:
                db.update_paper_evaluation(rec_id, updates)
                updated += 1

        except Exception:
            continue

    if not run_reco:
        try:
            db.log(f"[PAPER] eval only. updated={updated}", level="INFO")
        except Exception:
            pass
        return

    # -----------------------------------------
    # 2) Generate tuning recommendations (PENDING)
    # -----------------------------------------
    try:
        generate_recommendations(db, lookback_hours=24, max_recs=6, force=False)
    except Exception:
        pass

    try:
        db.log(f"[PAPER] Daily run complete. evaluated_updates={updated}", level="INFO")
    except Exception:
        pass


class PaperEvaluator:
    """Background evaluator that fills price_after_* and outcome_* in analysis_results."""

    def __init__(self, db, poll_seconds: int = 60):
        self.db = db
        self.poll_seconds = max(10, int(poll_seconds))
        self.running = False
        self.client = None
        try:
            self.client = Client(cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET, testnet=cfg.USE_TESTNET)
        except Exception:
            self.client = None

    def start(self):
        if self.running:
            return
        self.running = True
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            try:
                self._evaluate_batch()
            except Exception:
                pass
            time.sleep(self.poll_seconds)

    def _evaluate_batch(self, batch_size: int = 200):
        if not self.client:
            return
        rows = self.db.get_pending_paper_evaluations(limit=batch_size)
        if not rows:
            return

        for r in rows:
            try:
                rid = r["id"]
                symbol = r["symbol"]
                interval = r.get("interval") or "5m"
                market_type = r.get("market_type") or "futures"
                entry_price = float(r.get("entry_price") or 0.0)
                decision = (r.get("decision") or "NEUTRAL").upper()
                t0 = int(r.get("signal_time_ms") or 0)
                if entry_price <= 0 or t0 <= 0:
                    self.db.mark_paper_invalid(rid, note="invalid entry/timestamp")
                    continue

                updates = {}
                for label, delta in HORIZONS_MS.items():
                    col_price = f"price_after_{label}"
                    col_out = f"outcome_{label}"

                    if r.get(col_price) not in (None, 0, 0.0, "0"):
                        continue

                    target_ms = t0 + delta
                    price_t = self._close_at(symbol, interval, target_ms, market_type)
                    if price_t is None or price_t <= 0:
                        continue

                    updates[col_price] = float(price_t)

                    if decision in ("NEUTRAL", "HOLD", "SKIP", ""):
                        out = "SKIP"
                    else:
                        ret = (price_t - entry_price) / entry_price
                        if decision == "SHORT":
                            ret = -ret
                        out = "WIN" if ret > 0 else ("LOSS" if ret < 0 else "FLAT")

                    updates[col_out] = out

                if updates:
                    self.db.update_paper_evaluation(rid, updates)

            except Exception:
                continue

    def _close_at(self, symbol: str, interval: str, target_ms: int, market_type: str) -> Optional[float]:
        """Fetch close price of the candle ending at/just before target_ms."""
        try:
            step = INTERVAL_MS.get(interval, 5 * 60 * 1000)
            start_ms = target_ms - (step * 3)
            if market_type == "spot":
                k = self.client.get_klines(symbol=symbol, interval=interval, startTime=int(start_ms), endTime=int(target_ms), limit=3)
            else:
                k = self.client.futures_klines(symbol=symbol, interval=interval, startTime=int(start_ms), endTime=int(target_ms), limit=3)
            if not k:
                return None
            return float(k[-1][4])
        except Exception:
            return None
