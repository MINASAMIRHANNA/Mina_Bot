import asyncio
import logging
import random
import inspect
from typing import Optional, Set
from binance import AsyncClient, BinanceSocketManager
from config import cfg

from datetime import datetime, timezone

# ==========================================================
# WS STATUS (for Project Doctor)
# ==========================================================
_WS_STATE = {
    "started_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "sockets": {},  # id -> {connected, streams, last_connect_utc, last_msg_utc, reconnects, last_error}
    "errors_total": 0,
    "last_update_utc": None,
}

def get_ws_status():
    """Return a snapshot of the Futures WS status (safe to call from other threads)."""
    # shallow copy only (values are simple)
    try:
        s = dict(_WS_STATE)
        s["sockets"] = dict(_WS_STATE.get("sockets") or {})
        return s
    except Exception:
        return {"error": "unavailable"}

# ==========================================================
# Futures WS endpoint - testnet-safe
# Official Futures Testnet WS base: wss://fstream.binancefuture.com
# ==========================================================

def _futures_ws_base(use_testnet: bool) -> str:
    v = str(getattr(cfg, "BINANCE_FUTURES_WS_BASE", "") or "")
    if v.strip():
        return v.strip().rstrip("/")
    return "wss://fstream.binancefuture.com" if use_testnet else "wss://fstream.binance.com"

def _configure_async_futures_endpoints(client: AsyncClient, use_testnet: bool) -> None:
    # REST base (used by some socket managers internally)
    rest_base = str(getattr(cfg, "BINANCE_FUTURES_REST_BASE", "") or "")
    if not rest_base.strip():
        rest_base = "https://demo-fapi.binance.com" if use_testnet else "https://fapi.binance.com"
    rest_base = rest_base.strip().rstrip("/")

    ws_base = _futures_ws_base(use_testnet)

    candidates = {
        "FUTURES_URL": f"{rest_base}/fapi",
        "FUTURES_TESTNET_URL": f"{rest_base}/fapi",
        "FUTURES_WEBSOCKET_URL": ws_base,
        "FUTURES_TESTNET_WEBSOCKET_URL": ws_base,
    }
    for attr, url in candidates.items():
        if hasattr(client, attr):
            try:
                setattr(client, attr, url)
            except Exception:
                pass

# Configure Logging
logger = logging.getLogger("ws_logger")

# Shared semaphore to bound background callback tasks across all futures sockets
_CB_SEM: Optional[asyncio.Semaphore] = None
_TASKS: Set[asyncio.Task] = set()

def _get_cb_concurrency() -> int:
    # Zero-ENV: prefer DB setting WS_CALLBACK_CONCURRENCY, else fall back to cfg.MAX_CONCURRENT_TASKS
    try:
        v = int(getattr(cfg, "WS_CALLBACK_CONCURRENCY", 0) or 0)
        if v > 0:
            return max(1, min(64, v))
    except Exception:
        pass
    try:
        v = int(getattr(cfg, "MAX_CONCURRENT_TASKS", 20) or 20)
        return max(1, min(32, v))
    except Exception:
        return 10

def _ensure_semaphore() -> asyncio.Semaphore:
    global _CB_SEM
    if _CB_SEM is None:
        _CB_SEM = asyncio.Semaphore(_get_cb_concurrency())
    return _CB_SEM

async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x

async def _run_callback(callback, symbol: str, interval: str, kline: dict, market_type: str) -> None:
    """Run the user callback without blocking the WS receive loop.

    Important: if callback is a normal (sync) function, run it in a thread. This prevents
    BinanceWebsocketQueueOverflow / ping timeouts when message rate is high.
    """
    sem = _ensure_semaphore()
    await sem.acquire()
    try:
        if inspect.iscoroutinefunction(callback):
            await callback(symbol, interval, kline, market_type)
        else:
            # Run sync callback in a worker thread
            result = await asyncio.to_thread(callback, symbol, interval, kline, market_type)
            # If the sync callback returned an awaitable, await it on the loop
            if inspect.isawaitable(result):
                await result
    except Exception as e:
        print(f"[FuturesWS] Callback error for {symbol} {interval}: {e}")
    finally:
        try:
            sem.release()
        except Exception:
            pass



# Maximum streams per connection (Binance URL limit safety)
CHUNK_SIZE = cfg.WS_CHUNK_SIZE 

async def maintain_socket_connection(id, streams, callback, market_type="futures"):
    """
    Maintains a single WebSocket connection for a chunk of streams.
    Restarts automatically if it disconnects.

    Key fixes:
    - Keep recv loop fast (process only closed candles)
    - Run heavy callback work in background tasks with bounded concurrency
      to prevent BinanceWebsocketQueueOverflow / ping timeouts.
    """
    backoff = 1.0
    max_backoff = 60.0

    while True:
        client = None
        try:
            # Create Client based on config
            client = await AsyncClient.create(
                cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET
            )
            _configure_async_futures_endpoints(client, bool(getattr(cfg, 'USE_TESTNET', False)))
            bsm = BinanceSocketManager(client)

            # Prefer futures endpoint when available
            _sock = getattr(bsm, "futures_multiplex_socket", None)
            if callable(_sock):
                _cm = bsm.futures_multiplex_socket(streams)
            else:
                print("[WARN] BinanceSocketManager has no futures_multiplex_socket; using multiplex_socket")
                _cm = bsm.multiplex_socket(streams)

            async with _cm as stream:
                print(f"🔌 [Socket #{id}] Connected to {market_type.upper()} ({len(streams)} streams)...")
                try:
                    # update WS state
                    st = _WS_STATE.get("sockets", {}).get(str(id), {})
                    st.update({
                        "connected": True,
                        "streams": int(len(streams)),
                        "last_connect_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        "last_error": "",
                    })
                    st["reconnects"] = int(st.get("reconnects") or 0)
                    _WS_STATE["sockets"][str(id)] = st
                    _WS_STATE["last_update_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                except Exception:
                    pass

                backoff = 1.0  # reset on successful connect

                while True:
                    res = await stream.recv()
                    try:
                        st = _WS_STATE.get("sockets", {}).get(str(id), {})
                        st["last_msg_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                        _WS_STATE["sockets"][str(id)] = st
                        _WS_STATE["last_update_utc"] = st["last_msg_utc"]
                    except Exception:
                        pass


                    # Binance multiplex format: {"stream": "...", "data": {...}}
                    if not res or "data" not in res:
                        continue

                    data = res["data"]
                    if data.get("e") != "kline":
                        continue

                    k = data.get("k") or {}
                    # only on candle close
                    if not k.get("x"):
                        continue

                    symbol = data.get("s")
                    interval = k.get("i")
                    if not symbol or not interval:
                        continue

                    # bounded background callback
                    t = asyncio.create_task(_run_callback(callback, symbol, interval, k, market_type))
                    _TASKS.add(t)
                    t.add_done_callback(lambda _t: _TASKS.discard(_t))

        except Exception as e:
            # Common errors: queue overflow, ping timeout, connection closed
            print(f"⚠️ [Socket #{id}] Connection Lost: {e}")
            try:
                _WS_STATE["errors_total"] = int(_WS_STATE.get("errors_total") or 0) + 1
                st = _WS_STATE.get("sockets", {}).get(str(id), {})
                st["connected"] = False
                st["last_error"] = str(e)
                st["reconnects"] = int(st.get("reconnects") or 0) + 1
                _WS_STATE["sockets"][str(id)] = st
                _WS_STATE["last_update_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            except Exception:
                pass

            # exponential backoff + jitter
            sleep_s = min(max_backoff, backoff) + random.uniform(0.0, 1.0)
            print(f"🔄 [Socket #{id}] Reconnecting in {sleep_s:.1f}s...")
            await asyncio.sleep(sleep_s)
            backoff = min(max_backoff, backoff * 2.0)

        finally:
            if client:
                try:
                    await client.close_connection()
                except Exception:
                    pass


async def start_futures_sockets(symbols, timeframes, callback):
    """
    Splits subscription into chunks to avoid HTTP 414 errors.
    """
    # 1. Build all desired streams
    all_streams = []
    for s in symbols:
        for tf in timeframes:
            # Stream name format: symbol@kline_interval
            all_streams.append(f"{s.lower()}@kline_{tf}")
    
    # 2. Split into chunks
    chunks = [all_streams[i:i + CHUNK_SIZE] for i in range(0, len(all_streams), CHUNK_SIZE)]
    print(f"🚀 Starting {len(chunks)} parallel FUTURES connections for {len(all_streams)} streams...")
    
    # 3. Launch tasks
    tasks = []
    for i, chunk in enumerate(chunks):
        # نحدد هنا أن النوع هو futures
        tasks.append(maintain_socket_connection(i+1, chunk, callback, "futures"))
    
    await asyncio.gather(*tasks)