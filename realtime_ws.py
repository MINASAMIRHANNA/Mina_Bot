"""
realtime_ws.py (fixed)

Spot realtime WebSocket consumer for Binance kline streams.

Fixes:
- Prevent BinanceWebsocketQueueOverflow by keeping recv loop fast:
  * optionally process only closed candles (k['x'] == True)
  * run heavy callback work in background tasks with bounded concurrency
"""

from __future__ import annotations

import asyncio
import random
from typing import Awaitable, Callable, Iterable, List, Optional, Union

from binance import AsyncClient, BinanceSocketManager

from config import cfg

CallbackT = Callable[[str, str, dict], Union[None, Awaitable[None]]]


def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    if chunk_size <= 0:
        chunk_size = 50
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


async def _maybe_await(fn_result):
    if asyncio.iscoroutine(fn_result):
        await fn_result


def _get_ws_only_closed() -> bool:
    v = getattr(cfg, "WS_ONLY_CLOSED_CANDLES", True)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("0", "false", "no", "off"):
            return False
        if s in ("1", "true", "yes", "on"):
            return True
    return bool(v)


def _get_cb_concurrency() -> int:
    v = getattr(cfg, "WS_CALLBACK_CONCURRENCY", 200)
    try:
        v = int(v)
    except Exception:
        v = 200
    return max(10, v)


# Shared semaphore to bound background callback tasks across all sockets
_CB_SEM: Optional[asyncio.Semaphore] = None
_TASKS: set[asyncio.Task] = set()


def _ensure_semaphore() -> asyncio.Semaphore:
    global _CB_SEM
    if _CB_SEM is None:
        _CB_SEM = asyncio.Semaphore(_get_cb_concurrency())
    return _CB_SEM


async def _run_callback(callback: CallbackT, symbol: str, interval: str, kline: dict) -> None:
    sem = _ensure_semaphore()
    await sem.acquire()
    try:
        await _maybe_await(callback(symbol, interval, kline))
    except Exception as e:
        print(f"[SpotWS] Callback error for {symbol} {interval}: {e}")
    finally:
        try:
            sem.release()
        except Exception:
            pass


async def _dispatch_kline(callback: CallbackT, interval: str, msg: dict) -> None:
    """
    Binance multiplex format:
      {"stream":"btcusdt@kline_1m","data":{...,"k":{...}}}
    """
    if not msg or not isinstance(msg, dict):
        return

    data = msg.get("data")
    if not isinstance(data, dict):
        return

    if data.get("e") != "kline":
        return

    k = data.get("k")
    if not isinstance(k, dict):
        return

    # If enabled, process only candle close updates
    if _get_ws_only_closed() and not bool(k.get("x")):
        return

    symbol = k.get("s") or data.get("s")
    if not symbol:
        stream = msg.get("stream", "")
        if isinstance(stream, str) and "@kline_" in stream:
            symbol = stream.split("@", 1)[0].upper()

    if not symbol:
        return

    # Run callback in background to keep recv loop fast
    max_pending = int(getattr(cfg, "WS_MAX_PENDING_TASKS", 2000) or 2000)
    if max_pending > 0 and len(_TASKS) >= max_pending:
        return
    t = asyncio.create_task(_run_callback(callback, str(symbol).upper(), interval, k))
    _TASKS.add(t)
    t.add_done_callback(lambda x: _TASKS.discard(x))


async def maintain_socket_connection(index: int, streams: List[str], interval: str, callback: CallbackT) -> None:
    """Maintains a connection for a chunk of Spot streams. Restarts automatically if it disconnects."""
    backoff = 1.0
    max_backoff = 60.0

    while True:
        client: Optional[AsyncClient] = None
        bm: Optional[BinanceSocketManager] = None

        try:
            client = await AsyncClient.create(
                api_key=getattr(cfg, "BINANCE_API_KEY", None),
                api_secret=getattr(cfg, "BINANCE_API_SECRET", None),
                testnet=getattr(cfg, "USE_TESTNET", False),
            )

            bm = BinanceSocketManager(client)

            ms = bm.multiplex_socket(streams)
            print(f"🟢 [SpotWS-{index}] Connected ({len(streams)} streams)")

            backoff = 1.0

            async with ms as socket:
                while True:
                    msg = await socket.recv()
                    # Keep this fast
                    await _dispatch_kline(callback, interval, msg)

        except asyncio.CancelledError:
            print(f"🟡 [SpotWS-{index}] Cancelled")
            raise
        except Exception as e:
            print(f"🔴 [SpotWS-{index}] Disconnected/Error: {e}")
            sleep_s = min(max_backoff, backoff) + random.uniform(0, 0.5)
            print(f"⏳ [SpotWS-{index}] Reconnecting in {sleep_s:.1f}s...")
            await asyncio.sleep(sleep_s)
            backoff = min(max_backoff, backoff * 2)
        finally:
            try:
                if client is not None:
                    await client.close_connection()
            except Exception:
                pass


async def start_kline_socket(symbols: Iterable[str], interval: str, callback: CallbackT) -> None:
    """
    Start Spot kline multiplex sockets for a list of symbols.

    Args:
      symbols: iterable like ["BTCUSDT", "ETHUSDT", ...]
      interval: e.g. "1m", "5m"
      callback: callable called as callback(symbol, interval, kline_dict)
    """
    symbols_list = [str(s).upper() for s in symbols if s]
    if not symbols_list:
        print("⚠️ [SpotWS] No symbols provided.")
        return

    chunk_size = int(getattr(cfg, "WS_CHUNK_SIZE", 50) or 50)

    all_streams = [f"{s.lower()}@kline_{interval}" for s in symbols_list]
    chunks = _chunk_list(all_streams, chunk_size)

    print(f"🚀 Starting {len(chunks)} parallel Spot connections for {len(all_streams)} streams...")

    tasks = [
        asyncio.create_task(maintain_socket_connection(i, chunk, interval, callback))
        for i, chunk in enumerate(chunks)
    ]

    # Keep running
    await asyncio.gather(*tasks)
