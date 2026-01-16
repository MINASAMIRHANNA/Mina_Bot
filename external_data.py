import requests
import pandas as pd

def get_market_sentiment():
    """Fetches Fear & Greed Index from Alternative.me"""
    try:
        # Increased timeout to 10 seconds for reliability
        response = requests.get("https://api.alternative.me/fng/", timeout=10)
        data = response.json()
        value = int(data['data'][0]['value'])
        return value
    except Exception as e:
        print(f"[WARN] Sentiment fetch failed: {e}. Defaulting to 50.")
        return 50

def get_bitcoin_dominance():
    """Fetches BTC Dominance from Binance Futures (BTCDOMUSDT)"""
    try:
        # BTCDOMUSDT is a real tradeable index on Binance Futures
        url = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCDOMUSDT"
        response = requests.get(url, timeout=5)
        data = response.json()
        return float(data['price'])
    except Exception as e:
        # Fallback to 50 if API fails
        return 50.0

def check_news_circuit_breaker():
    """
    [NEW] Checks for Extreme Market Fear (Proxy for Bad News).
    Returns: True if trading should be HALTED (Danger), False if Safe.
    """
    try:
        sentiment = get_market_sentiment()
        # If Fear < 15, it's usually a crash or bad news event.
        if sentiment < 9:
            return True, f"EXTREME FEAR ({sentiment}/100)"
        return False, "NORMAL"
    except:
        return False, "ERROR"
