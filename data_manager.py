import time
import threading
from binance.client import Client
from config import cfg


class _NoPingClient(Client):
    """Disable spot ping() during init to avoid Spot-Testnet 502 killing futures modules."""
    def ping(self):
        try:
            return {}
        except Exception:
            return {}

def _futures_rest_base(use_testnet: bool) -> str:
    """Resolve futures REST base from cfg (DB-backed)."""
    base = str(getattr(cfg, "BINANCE_FUTURES_REST_BASE", "") or "").strip()
    if base:
        return base.rstrip("/")
    return "https://demo-fapi.binance.com" if use_testnet else "https://fapi.binance.com"

def _configure_futures_endpoints(c: Client, use_testnet: bool) -> None:
    rest_base = _futures_rest_base(use_testnet)
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


class DataManager:
    """Background fetcher for derivatives metrics (Funding + Open Interest).

    Notes:
    - Funding rate is read from futures_mark_price(lastFundingRate) and returned as a fraction (e.g. 0.0001).
    - OI change is returned as a fraction vs a moving baseline (e.g. 0.02 == +2%).
    - All operations are best-effort; failures should not stop the bot.
    """

    def __init__(self):
        self.client = None
        self.data_cache = {}
        self.running = False
        self.lock = threading.Lock()
        self.thread = None

        # error throttle
        self._last_err_ts = 0.0
        self._last_err_msg = ""

    def start(self, symbols):
        if self.running:
            return
        try:
            self.client = _NoPingClient(cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET, testnet=False)
            _configure_futures_endpoints(self.client, bool(getattr(cfg, "USE_TESTNET", True)))
            self.running = True
            self.thread = threading.Thread(target=self._loop_fetch, args=(list(symbols or []),), daemon=True)
            self.thread.start()
            print("🧠 DataManager ACTIVE (Funding + OI)")
        except Exception as e:
            print(f"[WARN] DataManager init failed (Bot will run without OI data): {e}")
            self.running = False

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)

    def _log_err(self, msg: str):
        """Optional error logging controlled by cfg (quiet by default)."""
        try:
            enabled = bool(getattr(cfg, "OI_LOG_ERRORS", False))
            if not enabled:
                return
            now = time.time()
            throttle = float(getattr(cfg, "OI_ERROR_THROTTLE_SEC", 60.0) or 60.0)
            if (now - self._last_err_ts) < throttle and msg == self._last_err_msg:
                return
            self._last_err_ts = now
            self._last_err_msg = msg
            print(f"[WARN] DataManager(OI): {msg}")
        except Exception:
            pass

    def _loop_fetch(self, symbols):
        SYMBOL_DELAY = float(getattr(cfg, "OI_SYMBOL_DELAY_SEC", 1.0) or 1.0)
        CYCLE_DELAY = float(getattr(cfg, "OI_CYCLE_DELAY_SEC", 30.0) or 30.0)
        BASELINE_WINDOW = float(getattr(cfg, "OI_BASELINE_WINDOW_SEC", 300.0) or 300.0)

        while self.running:
            cycle_start = time.time()
            for symbol in symbols:
                if not self.running:
                    break
                try:
                    mark = self.client.futures_mark_price(symbol=symbol)
                    funding = float(mark.get("lastFundingRate", 0) or 0.0)

                    oi_data = self.client.futures_open_interest(symbol=symbol)
                    current_oi = float(oi_data.get("openInterest", 0) or 0.0)

                    now = time.time()
                    with self.lock:
                        if symbol not in self.data_cache:
                            self.data_cache[symbol] = {
                                "funding": funding,
                                "oi_curr": current_oi,
                                "oi_prev": current_oi,
                                "last_update": now,
                            }
                        else:
                            d = self.data_cache[symbol]
                            if (now - float(d.get("last_update") or 0.0)) >= BASELINE_WINDOW:
                                d["oi_prev"] = float(d.get("oi_curr") or current_oi)
                                d["last_update"] = now
                            d["funding"] = funding
                            d["oi_curr"] = current_oi
                except Exception as e:
                    self._log_err(f"{symbol}: {e}")
                time.sleep(max(0.0, SYMBOL_DELAY))

            elapsed = time.time() - cycle_start
            if elapsed < CYCLE_DELAY:
                time.sleep(CYCLE_DELAY - elapsed)

    def get_derivatives_data(self, symbol):
        """Return (funding_rate, oi_change_fraction)."""
        with self.lock:
            data = self.data_cache.get(symbol)
        if not data:
            return 0.0, 0.0

        try:
            funding = float(data.get("funding") or 0.0)
            oi_prev = float(data.get("oi_prev") or 0.0)
            oi_curr = float(data.get("oi_curr") or 0.0)
        except Exception:
            return 0.0, 0.0

        if oi_prev > 0:
            oi_change = (oi_curr - oi_prev) / oi_prev
        else:
            oi_change = 0.0
        return funding, oi_change
