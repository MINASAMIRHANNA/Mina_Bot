import pandas as pd
from binance.client import Client


class _NoPingClient(Client):
    """Disable spot ping() during init to avoid Spot-Testnet 502 killing AuditLab."""
    def ping(self):
        try:
            return {}
        except Exception:
            return {}

from database import DatabaseManager
from indicators import atr, adx, rsi
from config import cfg

# ==========================================================
# AuditLab
# - Must NEVER block dashboard startup (no network calls at import)
# - Use Futures endpoints explicitly (demo-fapi for testnet)
# ==========================================================

_DB = None

def _get_db() -> DatabaseManager:
    global _DB
    if _DB is None:
        _DB = DatabaseManager()
    return _DB

def _futures_rest_base(use_testnet: bool) -> str:
    env = str(getattr(cfg, "BINANCE_FUTURES_REST_BASE", "") or "")
    if env.strip():
        return env.strip().rstrip("/")
    return "https://demo-fapi.binance.com" if use_testnet else "https://fapi.binance.com"

def _configure_futures_endpoints(c: Client, use_testnet: bool) -> None:
    """Make python-binance futures endpoints explicit (avoids testnet URL confusion)."""
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

class AuditLab:
    def __init__(self):
        # لا ننشئ Client هنا لتجنب كسر تشغيل الداشبورد عند مشاكل الشبكة.
        self._client: Client | None = None

    def _get_client(self) -> Client:
        if self._client is not None:
            return self._client

        use_testnet = bool(getattr(cfg, "USE_TESTNET", False))
        api_key = getattr(cfg, "BINANCE_API_KEY", None)
        api_secret = getattr(cfg, "BINANCE_API_SECRET", None)

        # إنشاء كلاينت باينانس مستقل لضمان عدم تداخل الطلبات مع البوت الرئيسي
        # ملاحظة: python-binance يقوم بعمل ping للـ Spot أثناء init.
        # نحن لا نستخدم Spot في AuditLab، لكن هذا ping قد يفشل أحيانًا على Spot testnet.
        # لذلك لا نستخدم testnet= هنا، ونثبت Futures endpoints صراحةً.
        if api_key and api_secret:
            c = _NoPingClient(api_key, api_secret, requests_params={"timeout": 10})
        else:
            c = _NoPingClient(requests_params={"timeout": 10})

        _configure_futures_endpoints(c, use_testnet)
        self._client = c
        return c

    def run_historical_audit(self, symbol, interval, start_str=None, end_str=None):
        """
        يجلب البيانات مباشرة من باينانس (Futures) ويحللها دون الاعتماد على fetcher.py لزيادة الاستقرار.
        """
        try:
            # 1) تحويل الوقت المختار من الواجهة إلى Timestamp (ملي ثانية)
            start_ts = int(pd.to_datetime(start_str).timestamp() * 1000) if start_str else None
            end_ts = int(pd.to_datetime(end_str).timestamp() * 1000) if end_str else None

            # 2) طلب البيانات التاريخية (Futures) بحد أقصى 1000 شمعة
            client = self._get_client()
            klines = client.futures_klines(
                symbol=symbol.upper(),
                interval=interval,
                startTime=start_ts,
                endTime=end_ts,
                limit=1000
            )

            if not klines:
                return []

            # 3) تحويل البيانات لـ DataFrame
            cols = [
                "open_time","open","high","low","close","volume",
                "close_time","qav","trades","tbba","tbqa","ignore"
            ]
            df = pd.DataFrame(klines, columns=cols)
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
            df.set_index("timestamp", inplace=True)

            for col in ["open","high","low","close","volume"]:
                df[col] = df[col].astype(float)

            # 4) حساب المؤشرات الفنية للتدقيق
            df["atr_val"] = atr(df, 14)
            df["adx_val"] = adx(df, 14)
            df["rsi_val"] = rsi(df, 14)

            # Gate thresholds من إعدادات البوت
            min_atr = float(_get_db().get_setting("min_atr_percent", 0.003) or 0.003)
            min_adx = float(_get_db().get_setting("min_adx", 15) or 15)
            max_rsi = float(_get_db().get_setting("max_rsi", 75) or 75)

            audit_results = []
            real_trades = _get_db().get_all_closed_trades() or []

            # 5) تحليل كل شمعة ومطابقتها مع سجل البوت الحقيقي
            for index, row in df.iterrows():
                if pd.isna(row["atr_val"]):
                    continue

                atr_pct = (row["atr_val"] / row["close"]) if row["close"] > 0 else 0
                status = "OPEN"
                if atr_pct < min_atr or row["adx_val"] < min_adx or row["rsi_val"] > max_rsi:
                    status = "CLOSED"

                ts_str = index.strftime("%Y-%m-%d %H:%M")
                match = "NONE"

                for t in real_trades:
                    # مطابقة تقريبية بناءً على اسم العملة والوقت (أول 16 حرف)
                    try:
                        if t["symbol"] == symbol.upper() and str(t.get("closed_at",""))[:16] == ts_str.replace(" ", "T"):
                            match = t.get("signal", "NONE")
                    except Exception:
                        pass

                audit_results.append({
                    "time": ts_str,
                    "price": round(row["close"], 6),
                    "atr_pct": round(atr_pct * 100, 3),
                    "adx": round(float(row["adx_val"]), 1),
                    "rsi": round(float(row["rsi_val"]), 1),
                    "gate": status,
                    "real_action": match
                })

            return audit_results[::-1]
        except Exception as e:
            print(f"❌ [AuditLab Core Error]: {e}")
            return []
