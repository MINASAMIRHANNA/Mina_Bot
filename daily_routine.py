import subprocess
import sys
import os
import requests
import time
from datetime import datetime, timezone, timedelta


# Setup Paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)

try:
    from config import cfg
except ImportError:
    print("❌ Config missing.")
    sys.exit(1)


def _utc_now():
    return datetime.now(timezone.utc)


def _fmt_utc(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def send_telegram(msg: str):
    try:
        if cfg.TELEGRAM_BOT_TOKEN and cfg.TELEGRAM_CHAT_ID:
            requests.post(
                f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cfg.TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=8,
            )
    except Exception:
        pass


def run_script(script_name: str) -> str:
    """Runs a script and returns its output (best-effort, with timeout)."""
    path = os.path.join(CURRENT_DIR, script_name)
    if not os.path.exists(path):
        return f"❌ Script not found: {script_name}"

    print(f"\n>>> Running: {script_name}...")

    timeout_sec = int(getattr(cfg, "DAILY_SCRIPT_TIMEOUT_SEC", 1800))  # 30 min default
    try:
        res = subprocess.run(
            [sys.executable, path],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        if res.stdout:
            print(res.stdout)
        if res.stderr:
            print(f"ERR: {res.stderr}")
        return (res.stdout or "") + ("\n" + res.stderr if res.stderr else "")
    except subprocess.TimeoutExpired:
        return f"⚠️ Timeout running {script_name} after {timeout_sec}s"
    except Exception as e:
        print(f"❌ Failed: {e}")
        return str(e)


def _db_housekeeping(report: list):
    """
    Snapshot retention / light DB housekeeping.
    Uses new DB API when available; falls back to legacy equity_history pruning.
    """
    try:
        from database import DatabaseManager

        db = DatabaseManager()
        retention_days = int(getattr(cfg, "SNAPSHOT_RETENTION_DAYS", 30))

        pruned = None
        if hasattr(db, "prune_equity_history"):
            try:
                pruned = db.prune_equity_history(retention_days=retention_days)
            except Exception:
                pruned = None
        else:
            # Legacy fallback (best effort)
            try:
                cutoff_dt = _utc_now() - timedelta(days=retention_days)
                cutoff = cutoff_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                with db.lock:
                    db.cursor.execute("DELETE FROM equity_history WHERE timestamp < ?", (cutoff,))
                    pruned = db.cursor.rowcount
                    db.conn.commit()
            except Exception:
                pruned = None

        if pruned is None:
            report.append(f"🧹 DB housekeeping: OK (retention {retention_days}d)")
        else:
            report.append(f"🧹 Snapshots pruned: {pruned} rows (retention {retention_days}d)")

    except Exception as e:
        report.append(f"⚠️ DB housekeeping failed: {e}")


def daily_job():
    start = _utc_now()
    print(f"=== ☀️ DAILY MAINTENANCE (UTC { _fmt_utc(start) }) ===")
    send_telegram("☀️ **Daily Routine Started (UTC)**\n_Optimizing AI & System..._")

    report = []

    # 0) DB housekeeping (snapshots retention etc.)
    _db_housekeeping(report)

    # 1. Harvest New Data
    out_harvest = run_script("auto_trainer.py")
    if "Added" in out_harvest:
        for line in out_harvest.split('\n'):
            if "Added" in line:
                report.append(f"🌱 {line.strip()}")
                break
    else:
        report.append("🌱 Data Harvested (No significant changes)")

    # 2. Retrain AI
    out_train = run_script("train_all_balanced.py")
    if "Saved" in out_train or "TRAINING COMPLETE" in out_train:
        report.append("🧠 AI Models Retrained")
    else:
        report.append("⚠️ AI Training Issue (Check logs)")

    
    # 2.b) Closed-loop learning (from live executed trades)
    # Safe: will auto-skip if not enough closed samples yet.
    try:
        out_cl = run_script("closed_loop_trainer.py")
        if "CLOSED-LOOP TRAINING: DONE" in out_cl:
            report.append("🧠 Closed-loop Training: DONE")
        elif "CLOSED-LOOP TRAINING: SKIP" in out_cl:
            report.append("🧠 Closed-loop Training: SKIP (not enough samples)")
        else:
            report.append("🧠 Closed-loop Training: (check output)")
    except Exception:
        report.append("🧠 Closed-loop Training: ERROR")
# 3. System Diagnostic (Using the NEW deep diagnostic)
    # Note: We check for the string printed by deep_diagnostic.py
    out_diag = run_script("deep_diagnostic.py")
    if "SYSTEM HEALTHY" in out_diag:
        report.append("✅ System Diagnostic: PASS")
    else:
        report.append("❌ System Diagnostic: FAIL (Check logs)")

    # 4. Financial Health Check
    out_health = run_script("bot_health.py")
    found_money = False
    for line in out_health.split('\n'):
        if "$" in line:
            # Clean up color codes if present
            clean_line = line.replace('\033[92m', '').replace('\033[0m', '').strip()
            report.append(f"💰 `{clean_line}`")
            found_money = True

    if not found_money:
        report.append("💰 Financials: (No Data/API Error)")

    # 5. Hot-Reload Main Bot (send command to bot to reload models/config)
    try:
        from database import DatabaseManager
        db = DatabaseManager()
        db.add_command("RELOAD_MODELS")
        report.append("🔄 Reload Command Sent to Bot")
    except Exception:
        report.append("⚠️ Failed to send Reload Command")

    # 6. Summary
    duration = str((_utc_now() - start)).split('.')[0]
    msg = f"✅ **Maintenance Complete (UTC)** ({duration})\n" + "\n".join(report)
    send_telegram(msg)
    print("\n=== ✅ DONE ===")


if __name__ == "__main__":
    daily_job()
