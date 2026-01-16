"""fetcher.py

Market data fetch utilities (Spot + Futures).

Project rules:
- All time is UTC.
- Must be resilient (timeouts, reconnects, rate limiting).
- Keep signatures used across the codebase:
    - fetch_klines(symbol, interval, limit=500, market_type='futures')
    - fetch_klines_window(symbol, interval, end_ms, limit=500, market_type='futures')
    - fetch_klines_range(symbol, interval, start_ms, end_ms, limit=1500, market_type='futures')
    - fetch_futures_raw(), fetch_spot_raw(), get_smart_watchlist(...)

Notes:
- This module will create a Binance Client even without API keys (public endpoints still work).
- Kline DataFrames are indexed by UTC timestamps.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout

# ==========================================================
# BINANCE ENDPOINTS (Futures) - resilient + testnet-safe
# Official Futures Testnet REST base: https://demo-fapi.binance.com
# Official Futures Testnet WS base:   wss://fstream.binancefuture.com
# (Binance Open Platform: USDⓈ-M Futures General Info)
# ==========================================================

def _futures_rest_base(use_testnet: bool) -> str:
    # Zero-ENV: resolve from cfg (DB-backed)
    base = str(getattr(cfg, 'BINANCE_FUTURES_REST_BASE', '') or '').strip()
    if base:
        return base.rstrip('/')
    return 'https://demo-fapi.binance.com' if use_testnet else 'https://fapi.binance.com'

def _configure_futures_endpoints(c: Client, use_testnet: bool) -> None:
    """Make python-binance futures endpoints explicit (avoids testnet URL confusion)."""
    rest_base = _futures_rest_base(use_testnet)

    # python-binance uses these attributes in different versions; set whatever exists.
    # FUTURES_URL should include the '/fapi' prefix.
    candidates = {
        "FUTURES_URL": f"{rest_base}/fapi",
        "FUTURES_TESTNET_URL": f"{rest_base}/fapi",
        "FUTURES_DATA_URL": f"{rest_base}/futures/data",
        "FUTURES_TESTNET_DATA_URL": f"{rest_base}/futures/data",
    }
    for attr, url in candidates.items():
        if hasattr(c, attr):
            try:
                setattr(c, attr, url)
            except Exception:
                pass

# ==========================================================
# PATH & CONFIG
# ==========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    from config import cfg
except ImportError as e:
    raise ImportError("[CRITICAL] config.py not found or failed to import") from e


# ==========================================================
# BINANCE CLIENT (lazy / resilient)
# ==========================================================
_client: Optional[Client] = None
_client_init_error: Optional[str] = None


def _init_client() -> Optional[Client]:
    """Initialize Binance Client (public-only if keys missing)."""
    global _client_init_error

    try:
        api_key = getattr(cfg, "BINANCE_API_KEY", None)
        api_secret = getattr(cfg, "BINANCE_API_SECRET", None)
        use_testnet = bool(getattr(cfg, "USE_TESTNET", False))

        # Always create a client; public endpoints work without keys.
        # NOTE: python-binance's `testnet=` flag historically applies to Spot in some versions.
        # We explicitly configure USDⓈ-M Futures endpoints instead (safer).
        if api_key and api_secret:
            c = Client(api_key, api_secret, requests_params={"timeout": 10})
        else:
            c = Client(requests_params={"timeout": 10})

        # Make Futures endpoints explicit (mainnet or testnet).
        _configure_futures_endpoints(c, use_testnet)

        _client_init_error = None
        return c
    except Exception as ex:
        _client_init_error = str(ex)
        return None


def _get_client() -> Optional[Client]:
    global _client
    if _client is None:
        _client = _init_client()
        if _client is None:
            # Keep silent-ish; upstream will handle None.
            print(f"[CRITICAL] Fetcher Client Init Failed: {_client_init_error}")
    return _client


# ==========================================================
# CONSTANTS
# ==========================================================
INTERVAL_MAP = {
    "1m": Client.KLINE_INTERVAL_1MINUTE,
    "5m": Client.KLINE_INTERVAL_5MINUTE,
    "15m": Client.KLINE_INTERVAL_15MINUTE,
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "4h": Client.KLINE_INTERVAL_4HOUR,
}

ALWAYS_WATCH = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "PEPEUSDT",
    "SHIBUSDT",
    "WIFUSDT",
    "BONKUSDT",
]

# Simple global rate limiter
_last_api_call = 0.0
RATE_LIMIT_DELAY = 0.2  # seconds


# ==========================================================
# INTERNAL HELPERS
# ==========================================================
def _rate_limit() -> None:
    global _last_api_call
    now = time.time()
    elapsed = now - _last_api_call
    if elapsed < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - elapsed)
    _last_api_call = time.time()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _invalid_symbol_error(code: Any) -> bool:
    # Common Binance symbol errors:
    # -1121 Invalid symbol
    # -1122 Invalid parameter
    # -1130 Invalid parameter (varies)
    try:
        return int(code) in (-1121, -1122, -1130)
    except Exception:
        return False


def _klines_to_df(k: List[List[Any]]) -> Optional[pd.DataFrame]:
    """Convert Binance kline list to normalized OHLCV DataFrame (UTC index)."""
    if not k:
        return None

    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "qav",  # quote asset volume
        "trades",
        "tbba",  # taker buy base asset volume
        "tbqa",  # taker buy quote asset volume
        "ignore",
    ]

    try:
        df = pd.DataFrame(k, columns=cols)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df.set_index("open_time", inplace=True)

        # Keep useful columns; drop noise fields.
        for col in ("ignore", "close_time"):
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        numeric_cols = ["open", "high", "low", "close", "volume", "qav", "trades", "tbba", "tbqa"]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df.dropna(inplace=True)
        return df
    except Exception:
        return None


def _call_with_retries(fn, *, max_retries: int = 3, sleep_base: float = 1.0):
    attempt = 0
    while attempt < max_retries:
        try:
            _rate_limit()
            return fn()
        except BinanceAPIException as e:
            if _invalid_symbol_error(getattr(e, "code", None)):
                return None
            time.sleep(sleep_base)
        except (ReadTimeout, RequestsConnectionError):
            time.sleep(sleep_base * 2)
        except Exception:
            time.sleep(sleep_base)
        attempt += 1
    return None


# ==========================================================
# WATCHLIST
# ==========================================================
def get_smart_watchlist(tickers: List[Dict[str, Any]], limit_vol: int, limit_gainers: int) -> List[str]:
    """Build a watchlist: ALWAYS_WATCH + top volume + top gainers."""
    try:
        valid = [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")]

        for t in valid:
            t["quoteVolume"] = _safe_float(t.get("quoteVolume", 0))
            t["priceChangePercent"] = _safe_float(t.get("priceChangePercent", 0))

        # Top volume
        valid.sort(key=lambda x: x["quoteVolume"], reverse=True)
        top_vol = [x["symbol"] for x in valid[: max(0, int(limit_vol))]]

        # Top gainers
        valid.sort(key=lambda x: x["priceChangePercent"], reverse=True)
        top_gainers = [x["symbol"] for x in valid[: max(0, int(limit_gainers))]]

        combined = set(ALWAYS_WATCH + top_vol + top_gainers)
        return list(combined)
    except Exception as e:
        print(f"[ERROR] Watchlist build failed: {e}")
        return ALWAYS_WATCH.copy()


# ==========================================================
# MARKET LISTS
# ==========================================================
def fetch_futures_raw() -> List[str]:
    c = _get_client()
    if not c:
        return []
    try:
        tickers = _call_with_retries(lambda: c.futures_ticker())
        if not tickers:
            return []
        return get_smart_watchlist(tickers, getattr(cfg, "WATCHLIST_VOL_LIMIT", 150), getattr(cfg, "WATCHLIST_GAINERS_LIMIT", 50))
    except Exception as e:
        print(f"[ERROR] fetch_futures_raw: {e}")
        return []


def fetch_spot_raw() -> List[str]:
    c = _get_client()
    if not c:
        return []
    try:
        tickers = _call_with_retries(lambda: c.get_ticker())
        if not tickers:
            return []
        return get_smart_watchlist(tickers, getattr(cfg, "WATCHLIST_VOL_LIMIT", 150), getattr(cfg, "WATCHLIST_GAINERS_LIMIT", 50))
    except Exception as e:
        print(f"[ERROR] fetch_spot_raw: {e}")
        return []


# ==========================================================
# KLINES
# ==========================================================
def fetch_klines(symbol: str, interval: str, limit: int = 500, market_type: str = "futures") -> Optional[pd.DataFrame]:
    """Fetch last `limit` klines for symbol/interval.

    market_type: 'futures' (default) or 'spot'
    """
    c = _get_client()
    if not c:
        return None
    if interval not in INTERVAL_MAP:
        return None

    kv = INTERVAL_MAP[interval]

    # Enforce exchange limits (spot max 1000; futures up to 1500)
    try:
        limit_i = int(limit)
    except Exception:
        limit_i = 500
    if market_type == "spot" and limit_i > 1000:
        limit_i = 1000
    if market_type != "spot" and limit_i > 1500:
        limit_i = 1500

    def _do():
        if market_type == "spot":
            return c.get_klines(symbol=symbol, interval=kv, limit=limit_i)
        return c.futures_klines(symbol=symbol, interval=kv, limit=limit_i)

    k = _call_with_retries(_do)
    return _klines_to_df(k) if k else None


# ==========================================================
# HISTORICAL WINDOW HELPERS (UTC milliseconds)
# ==========================================================
def fetch_klines_window(
    symbol: str,
    interval: str,
    end_ms: int,
    limit: int = 500,
    market_type: str = "futures",
) -> Optional[pd.DataFrame]:
    """Fetch a klines window ending at `end_ms` (UTC milliseconds)."""
    c = _get_client()
    if not c:
        return None
    if interval not in INTERVAL_MAP:
        return None

    kv = INTERVAL_MAP[interval]

    try:
        limit_i = int(limit)
    except Exception:
        limit_i = 500
    if market_type == "spot" and limit_i > 1000:
        limit_i = 1000
    if market_type != "spot" and limit_i > 1500:
        limit_i = 1500

    def _do():
        if market_type == "spot":
            return c.get_klines(symbol=symbol, interval=kv, endTime=int(end_ms), limit=limit_i)
        return c.futures_klines(symbol=symbol, interval=kv, endTime=int(end_ms), limit=limit_i)

    k = _call_with_retries(_do)
    return _klines_to_df(k) if k else None


def fetch_klines_range(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
    market_type: str = "futures",
) -> Optional[pd.DataFrame]:
    """Fetch klines between start_ms and end_ms (UTC milliseconds)."""
    c = _get_client()
    if not c:
        return None
    if interval not in INTERVAL_MAP:
        return None

    kv = INTERVAL_MAP[interval]

    try:
        limit_i = int(limit)
    except Exception:
        limit_i = 1500
    if market_type == "spot" and limit_i > 1000:
        limit_i = 1000
    if market_type != "spot" and limit_i > 1500:
        limit_i = 1500

    def _do():
        if market_type == "spot":
            return c.get_klines(symbol=symbol, interval=kv, startTime=int(start_ms), endTime=int(end_ms), limit=limit_i)
        return c.futures_klines(symbol=symbol, interval=kv, startTime=int(start_ms), endTime=int(end_ms), limit=limit_i)

    k = _call_with_retries(_do)
    return _klines_to_df(k) if k else None
