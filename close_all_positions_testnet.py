"""close_all_positions_testnet.py

Safely closes ALL open Binance Futures positions.

- Uses DB-backed config (config.cfg) for API keys + endpoints.
- Works for TESTNET or LIVE depending on cfg.USE_TESTNET and cfg.BINANCE_FUTURES_REST_BASE.
- Supports BOTH mode and Hedge Mode (positionSide).

Run:
  python close_all_positions_testnet.py

WARNING:
- This script sends MARKET reduceOnly orders.
- Use with care.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from binance.client import Client

from config import cfg


class _NoPingClient(Client):
    """Disable spot ping() during init to avoid Spot-Testnet 502 killing futures calls."""

    def ping(self):
        try:
            return {}
        except Exception:
            return {}


def _configure_futures_endpoints(c: Client) -> None:
    rest_base = (getattr(cfg, "BINANCE_FUTURES_REST_BASE", "") or "").strip().rstrip("/")
    if not rest_base:
        rest_base = "https://demo-fapi.binance.com" if bool(getattr(cfg, "USE_TESTNET", True)) else "https://fapi.binance.com"

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


def quantize_qty(symbol: str, exchange_info: dict, qty: float) -> float:
    """Quantize qty to LOT_SIZE stepSize for a given symbol."""
    try:
        for s in exchange_info.get("symbols", []):
            if s.get("symbol") != symbol:
                continue
            for f in s.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    step = Decimal(str(f.get("stepSize", "0.0")))
                    if step == 0:
                        return float(qty)
                    q = (Decimal(str(qty)) / step).to_integral_value(rounding=ROUND_DOWN) * step
                    return float(q)
    except Exception:
        pass
    return float(qty)


def main() -> int:
    api_key = (getattr(cfg, "BINANCE_API_KEY", None) or "").strip()
    api_secret = (getattr(cfg, "BINANCE_API_SECRET", None) or "").strip()
    if not api_key or not api_secret:
        print("❌ Missing BINANCE_API_KEY / BINANCE_API_SECRET in DB settings (Dashboard Env Secrets).")
        return 2

    use_testnet = bool(getattr(cfg, "USE_TESTNET", True))
    print(f"[CLOSE_ALL] USE_TESTNET={use_testnet}")
    print(f"[CLOSE_ALL] FUTURES_REST_BASE={getattr(cfg, 'BINANCE_FUTURES_REST_BASE', None)}")

    # For futures we do not rely on Client(testnet=...) because that's mainly for spot.
    client = _NoPingClient(api_key, api_secret, testnet=False)
    _configure_futures_endpoints(client)

    info = client.futures_exchange_info()
    positions = client.futures_position_information()

    closed = 0
    for p in positions:
        try:
            amt = float(p.get("positionAmt", 0))
        except Exception:
            amt = 0.0

        if amt == 0.0:
            continue

        symbol = p.get("symbol")
        if not symbol:
            continue

        position_side = p.get("positionSide", "BOTH")  # Hedge mode support

        qty = abs(float(amt))
        qty = quantize_qty(symbol, info, qty)
        if qty <= 0:
            continue

        # Close direction
        side = "SELL" if amt > 0 else "BUY"  # BOTH mode
        if position_side == "LONG":
            side = "SELL"
        elif position_side == "SHORT":
            side = "BUY"

        try:
            kwargs = dict(symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly=True)
            if position_side != "BOTH":
                kwargs["positionSide"] = position_side
            client.futures_create_order(**kwargs)
            print(f"✅ Closed {symbol} qty={qty} side={side} positionSide={position_side}")
            closed += 1
        except Exception as e:
            print(f"❌ Failed closing {symbol}: {e}")

    print(f"Done. Closed orders sent: {closed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
