import os
from decimal import Decimal, ROUND_DOWN
from binance.client import Client
from config import cfg

def quantize_qty(symbol_info, qty: float) -> float:
    for s in symbol_info["symbols"]:
        if s["symbol"] == symbol:
            for f in s["filters"]:
                if f.get("filterType") == "LOT_SIZE":
                    step = Decimal(str(f["stepSize"]))
                    q = (Decimal(str(qty)) / step).to_integral_value(rounding=ROUND_DOWN) * step
                    return float(q)
    return qty

client = Client(cfg.API_KEY, cfg.API_SECRET, testnet=cfg.USE_TESTNET)
if cfg.USE_TESTNET:
    client.FUTURES_URL = "https://demo-fapi.binance.com"

info = client.futures_exchange_info()
positions = client.futures_position_information()

closed = 0
for p in positions:
    amt = float(p.get("positionAmt", 0))
    if amt == 0:
        continue

    symbol = p["symbol"]
    position_side = p.get("positionSide", "BOTH")  # Hedge mode support

    qty = abs(amt)
    qty = quantize_qty(info, qty)
    if qty == 0:
        continue

    # Close direction
    side = "SELL" if amt > 0 else "BUY"   # for BOTH mode
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
