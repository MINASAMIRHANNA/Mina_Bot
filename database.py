import sqlite3
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from contextlib import nullcontext
from typing import Optional, Tuple, List, Dict, Any


class DatabaseManager:
    def __init__(self, db_file=None):
        # تحديد مسار قاعدة البيانات (robust)
        # Priority:
        # 1) explicit db_file arg
        # 2) config.cfg.DB_FILE (if available)
        # 3) env var DB_FILE
        # 4) local bot_data.db next to this file
        if db_file is None:
            # Try config.cfg first (avoids hard-coding paths in code)
            try:
                from config import cfg  # local import to avoid circular deps
                if getattr(cfg, "DB_FILE", None):
                    db_file = cfg.DB_FILE
            except Exception:
                db_file = None
        if db_file is None:
            # Sprint 2: DB pinning (avoid duplicate bot_data.db in different CWDs)
            try:
                from db_path import get_db_path
                db_file = get_db_path()
            except Exception:
                db_file = None

        if db_file is None:
            # Try common locations (supports running from /source or /dashboard)
            base_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = [
                os.path.join(base_dir, "bot_data.db"),
                os.path.join(os.path.dirname(base_dir), "bot_data.db"),
                os.path.join(os.path.dirname(os.path.dirname(base_dir)), "bot_data.db"),
                os.path.join(os.getcwd(), "bot_data.db"),
            ]
            for c in candidates:
                try:
                    if c and os.path.exists(c):
                        db_file = c
                        break
                except Exception:
                    continue
            if db_file is None:
                # default location (will be created)
                db_file = candidates[1]

        # Normalize path early (helps with relative CWD issues)
        try:
            db_file = os.path.expanduser(str(db_file))
        except Exception:
            pass
        try:
            db_file = os.path.abspath(str(db_file))
        except Exception:
            pass

        # If the target file exists but is NOT a SQLite DB, rename it safely and continue.
        # This prevents confusing runtime errors like: "file is not a database".
        try:
            if db_file and os.path.exists(db_file) and os.path.isfile(db_file):
                # A valid SQLite header starts with: b"SQLite format 3\x00"
                with open(db_file, "rb") as f:
                    head = f.read(16)
                if head and (not head.startswith(b"SQLite format 3")):
                    bad_name = f"{db_file}.bad.{int(time.time())}"
                    try:
                        os.rename(db_file, bad_name)
                        print(f"⚠️ [DB] {db_file} is not a SQLite DB. Renamed to: {bad_name}")
                    except Exception:
                        print(f"⚠️ [DB] {db_file} is not a SQLite DB (unable to rename). Will create a fresh DB.")
        except Exception:
            pass

        self.db_file = db_file

        # check_same_thread=False ضروري لعمل الـ Threads
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Improve concurrency & reduce lock issues
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
            self.conn.execute("PRAGMA busy_timeout=5000;")
        except Exception:
            pass

        self.cursor = self.conn.cursor()
        # RLock safer (prevents accidental deadlocks if functions call each other)
        self.lock = threading.RLock()

        self.create_tables()
        self._seed_defaults()
        self._ensure_schema()
        # Sprint 9: keep run mode + legacy keys consistent
        try:
            self.sync_legacy_mode_keys(self.get_setting("mode", "TEST"), source="db.init")
        except Exception:
            pass
        print(f"[DB] Connected to: {self.db_file}")

    # ==========================================================
    # INTERNAL UTC HELPERS
    # ==========================================================
    def _utc_iso(self):
        return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    def _ms_now(self):
        return int(time.time() * 1000)

    # ==========================================================
    # TABLES SETUP
    # ==========================================================
    def create_tables(self):
        try:
            with self.lock:
                # ---------------- TRADES ----------------
                # NOTE: Table is "IF NOT EXISTS" — for existing DB, schema updates happen in _ensure_schema()
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        signal TEXT,
                        entry_price REAL,
                        quantity REAL,
                        stop_loss REAL,
                        take_profits TEXT,
                        status TEXT DEFAULT 'OPEN',
                        timestamp TEXT,
                        timestamp_ms INTEGER,
                        close_price REAL,
                        close_reason TEXT,
                        closed_at TEXT,
                        closed_at_ms INTEGER,
                        pnl REAL DEFAULT 0,
                        confidence REAL DEFAULT 0,
                        explain TEXT,

                        market_type TEXT,
                        strategy_tag TEXT,
                        exit_profile TEXT,
                        leverage INTEGER,
                        tech_score REAL,
                        deriv_score REAL,
                        vol_spike INTEGER,
                        ai_vote TEXT,
                        ai_confidence REAL,
                        funding REAL,
                        oi_change REAL
                    )
                """)
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol_status ON trades(symbol, status)")

                # ---------------- TRADE EVENTS (Sprint 8: FSM / Protection history) ----------------
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trade_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id INTEGER,
                        event_type TEXT,
                        event_ts TEXT,
                        event_ts_ms INTEGER,
                        payload_json TEXT,
                        source TEXT
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_trade_time ON trade_events(trade_id, event_ts_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_type_time ON trade_events(event_type, event_ts_ms)")
                except Exception:
                    pass


                # ---------------- SETTINGS ----------------
                # NOTE: Older DBs may have only (key,value). Schema additions are handled in _ensure_schema().
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS settings (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TEXT,
                        version INTEGER DEFAULT 0,
                        source TEXT
                    )
                """)

                # ---------------- THRESHOLD ADJUSTMENTS ----------------
                # Per (mode, exit_profile) dynamic adjustment added to min_conf
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS threshold_adjustments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mode TEXT NOT NULL,
                        exit_profile TEXT NOT NULL,
                        adjustment REAL NOT NULL DEFAULT 0.0,
                        updated_at TEXT,
                        updated_at_ms INTEGER,
                        source TEXT,
                        UNIQUE(mode, exit_profile)
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_threshold_adj_mode_profile ON threshold_adjustments(mode, exit_profile)")
                except Exception:
                    pass

                # ---------------- SETTINGS AUDIT ----------------
                self.cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS settings_audit (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        key TEXT,
                        old_value TEXT,
                        new_value TEXT,
                        source TEXT,
                        created_at TEXT,
                        created_at_ms INTEGER
                    )
""")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_settings_audit_key_time ON settings_audit(key, created_at)")

                # ---------------- COMMANDS ----------------
                self.cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS commands (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        cmd TEXT,
                        params TEXT,
                        status TEXT DEFAULT 'PENDING',
                        created_at TEXT,
                        created_at_ms INTEGER,
                        source TEXT,
                        claimed_by TEXT,
                        claimed_at TEXT,
                        claimed_at_ms INTEGER,
                        done_at TEXT,
                        done_at_ms INTEGER,
                        last_error TEXT,
                        ack_meta TEXT
                    )
""")

                # ---------------- LOGS ----------------
                self.cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        level TEXT,
                        msg TEXT,
                        created_at TEXT,
                        created_at_ms INTEGER,
                        source TEXT
                    )
""")

                # ---------------- EQUITY HISTORY ----------------
                self.cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS equity_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        timestamp_ms INTEGER,
                        date_utc TEXT,
                        total_balance REAL,
                        unrealized_pnl REAL,
                        source TEXT,
                        meta TEXT
                    )
""")

                # ---------------- TRADE STATE (FSM) ----------------
                self.cursor.execute("""
                                        CREATE TABLE IF NOT EXISTS trade_state (
                        symbol TEXT PRIMARY KEY,
                        state TEXT,
                        strategy TEXT,
                        signal TEXT,
                        entry_price REAL,
                        stop_loss REAL,
                        take_profits TEXT,
                        confidence REAL,
                        reason TEXT,
                        state_since TEXT,
                        state_since_ms INTEGER,
                        last_update TEXT,
                        last_update_ms INTEGER
                    )
""")

                

                # ---------------- DECISION TRACES (Explainability / Doctor) ----------------
                # Lightweight, append-only trace of bot decisions (including NO_TRADE) for troubleshooting.
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS decision_traces (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT,
                        created_at_ms INTEGER,
                        symbol TEXT,
                        interval TEXT,
                        market_type TEXT,
                        strategy TEXT,
                        gate TEXT,
                        exit_profile TEXT,
                        decision TEXT,
                        conf_pct REAL,
                        min_conf REAL,
                        final_score REAL,
                        rule_signal TEXT,
                        ai_vote TEXT,
                        ai_confidence REAL,
                        pump_score REAL,
                        rsi REAL,
                        adx REAL,
                        vol_spike INTEGER,
                        funding REAL,
                        oi_change REAL,
                        trace TEXT
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_decision_traces_time ON decision_traces(created_at_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_decision_traces_symbol_time ON decision_traces(symbol, created_at_ms)")
                except Exception:
                    pass

                # ---------------- PERF METRICS (Latency / Performance) ----------------
                # Lightweight performance events to monitor analysis latency per symbol.
                # Stored as append-only rows; pruning is done periodically.
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS perf_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT,
                        created_at_ms INTEGER,
                        symbol TEXT,
                        interval TEXT,
                        market_type TEXT,
                        stage TEXT,
                        duration_ms INTEGER,
                        ok INTEGER,
                        meta TEXT
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_perf_metrics_time ON perf_metrics(created_at_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_perf_metrics_symbol_time ON perf_metrics(symbol, created_at_ms)")
                except Exception:
                    pass
                # ---------------- LEARNING ----------------
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS learning_stats (
                        date TEXT,
                        strategy TEXT,
                        exit_profile TEXT,
                        trades INTEGER,
                        wins INTEGER,
                        losses INTEGER,
                        pnl REAL,
                        avg_confidence REAL,
                        PRIMARY KEY (date, strategy, exit_profile)
                    )
                """)

                # ---------------- PAPER: analysis_results ----------------
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS analysis_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id INTEGER,
                        symbol TEXT,
                        signal_time TEXT,
                        signal_time_ms INTEGER,

                        market_type TEXT,
                        interval TEXT,
                        strategy_tag TEXT,

                        decision TEXT,
                        entry_price REAL,

                        price_after_15m REAL,
                        price_after_1h REAL,
                        price_after_4h REAL,

                        outcome_15m TEXT,
                        outcome_1h TEXT,
                        outcome_4h TEXT,

                        missed_opportunity INTEGER DEFAULT 0,
                        ai_accuracy_score REAL,

                        final_score REAL,
                        pump_score INTEGER,
                        confidence REAL,
                        tech_score REAL,
                        deriv_score REAL,
                        vol_spike INTEGER,
                        ai_vote TEXT,
                        ai_confidence REAL,
                        funding REAL,
                        oi_change REAL,
                        notes TEXT
                    )
                """)

                # ---------------- REJECTIONS ----------------
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rejections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        reason TEXT,
                        value REAL,
                        threshold REAL,
                        timestamp TEXT
                    )
                """)


                # ---------------- SIGNAL INBOX (Dashboard approvals) ----------------
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS signal_inbox (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        received_at TEXT,
                        received_at_ms INTEGER,
                        source TEXT DEFAULT 'bot',
                        status TEXT DEFAULT 'RECEIVED',
                        symbol TEXT,
                        timeframe TEXT,
                        strategy TEXT,
                        side TEXT,
                        confidence REAL,
                        score REAL,
                        payload TEXT,
                        approved_at_ms INTEGER,
                        rejected_at_ms INTEGER,
                        executed_at_ms INTEGER,
                        note TEXT
                    )
                """)
                
                # ---------------- AI SAMPLES (Closed-loop learning) ----------------
                # Stores feature snapshots at decision/execution time, then labels them on trade close.
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ai_samples (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT,
                        created_at_ms INTEGER,
                        symbol TEXT,
                        market_type TEXT,
                        algo_mode TEXT,
                        timeframe TEXT,
                        strategy TEXT,
                        side TEXT,
                        ai_vote TEXT,
                        ai_confidence REAL,
                        rule_score REAL,
                        confidence REAL,
                        features TEXT,
                        explain TEXT,
                        inbox_id INTEGER,
                        trade_id INTEGER,
                        status TEXT DEFAULT 'PENDING',
                        entry_price REAL,
                        exit_price REAL,
                        pnl REAL,
                        return_pct REAL,
                        close_reason TEXT,
                        closed_at_ms INTEGER
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ai_samples_status_time ON ai_samples(status, created_at_ms)")
                except Exception:
                    pass
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_ai_samples_trade ON ai_samples(trade_id)")
                except Exception:
                    pass
                self.conn.commit()

        except Exception as e:
            print(f"❌ DB Create Tables Error: {e}")

    # ==========================================================
    # SCHEMA MIGRATION (SAFE ADD-ONLY)
    # ==========================================================
    def _table_columns(self, table: str):
        try:
            self.cursor.execute(f"PRAGMA table_info({table})")
            return {row[1] for row in self.cursor.fetchall()}
        except Exception:
            return set()

    def _add_column(self, table: str, col_def: str):
        """col_def example: 'signal_time_ms INTEGER'"""
        try:
            col_name = col_def.strip().split()[0]
            if col_name in self._table_columns(table):
                return
            self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
            self.conn.commit()
        except Exception:
            pass

    def _ensure_schema(self):
        """Add missing columns/tables used by UTC + paper tournament + recommendations."""
        try:
            with self.lock:
                # ---- settings additions (Sprint 8+: ops toggles + dashboard audit) ----
                for col in [
                    "updated_at TEXT",
                    "version INTEGER DEFAULT 0",
                    "source TEXT",
                ]:
                    self._add_column("settings", col)

                # ---- analysis_results additions ----
                for col in [
                    "signal_time_ms INTEGER",
                    "market_type TEXT",
                    "interval TEXT",
                    "strategy_tag TEXT",
                    "final_score REAL",
                    "pump_score INTEGER",
                    "confidence REAL",
                    "tech_score REAL",
                    "deriv_score REAL",
                    "vol_spike INTEGER",
                    "ai_vote TEXT",
                    "ai_confidence REAL",
                    "funding REAL",
                    "oi_change REAL",
                    "outcome_4h TEXT",
                    "notes TEXT",
                ]:
                    self._add_column("analysis_results", col)

                # Create unique index to avoid duplicates
                try:
                    self.cursor.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_analysis_symbol_strategy_time "
                        "ON analysis_results(symbol, strategy_tag, signal_time_ms)"
                    )
                except Exception:
                    pass

                # ---- trades additions ----
                for col in [
                    "market_type TEXT",
                    "strategy_tag TEXT",
                    "timestamp_ms INTEGER",
                    "exit_profile TEXT",
                    "leverage INTEGER",
                    "tech_score REAL",
                    "deriv_score REAL",
                    "vol_spike INTEGER",
                    "ai_vote TEXT",
                    "ai_confidence REAL",
                    "funding REAL",
                    "oi_change REAL",
                    "closed_at_ms INTEGER",
                    "confidence REAL",
                    "signal_key TEXT",
                    "candle_close_ms INTEGER",
                    "timeframe TEXT",
                    "inbox_id INTEGER",]:
                    self._add_column("trades", col)


                # ---- Sprint 8: trade FSM + protection fields (safe add-only) ----
                for col in [
                    "fsm_state TEXT",
                    "fsm_state_since_ms INTEGER",
                    "fsm_state_updated_ms INTEGER",
                    "protection_status TEXT",
                    "protection_last_check_ms INTEGER",
                    "protection_details TEXT",
                    "protection_error TEXT",
                ]:
                    self._add_column("trades", col)

                # ---- Sprint 8: command ACK metadata ----
                for col in ["ack_meta TEXT"]:
                    self._add_column("commands", col)

                # ---- Sprint 8: trade_events history (idempotent) ----
                try:
                    self.cursor.execute("""
                        CREATE TABLE IF NOT EXISTS trade_events (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            trade_id INTEGER,
                            event_type TEXT,
                            event_ts TEXT,
                            event_ts_ms INTEGER,
                            payload_json TEXT,
                            source TEXT
                        )
                    """)
                except Exception:
                    pass
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_trade_time ON trade_events(trade_id, event_ts_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_type_time ON trade_events(event_type, event_ts_ms)")
                except Exception:
                    pass

                
                # ---- equity_history additions (UTC snapshots) ----
                for col in [
                    "timestamp_ms INTEGER",
                    "date_utc TEXT",
                    "source TEXT",
                    "meta TEXT",
                ]:
                    self._add_column("equity_history", col)

                # ---- optional ms/source columns for better dashboards (non-breaking) ----
                for col in ["created_at_ms INTEGER", "source TEXT"]:
                    self._add_column("logs", col)
                # Sprint 2: robust command queue fields
                for col in ["status TEXT DEFAULT 'PENDING'", "created_at TEXT", "created_at_ms INTEGER", "source TEXT",
                            "claimed_by TEXT", "claimed_at TEXT", "claimed_at_ms INTEGER",
                            "done_at TEXT", "done_at_ms INTEGER", "last_error TEXT"]:
                    self._add_column("commands", col)
                for col in ["created_at_ms INTEGER"]:
                    self._add_column("settings_audit", col)
                for col in ["state_since_ms INTEGER", "last_update_ms INTEGER"]:
                    self._add_column("trade_state", col)
                # ---- signal_inbox idempotency additions (non-breaking) ----
                for col in ["dedupe_key TEXT", "candle_close_ms INTEGER"]:
                    self._add_column("signal_inbox", col)


                # ---- signal_inbox compatibility: provide created_at columns (aliases for received_at) ----
                for col in ["created_at TEXT", "created_at_ms INTEGER"]:
                    self._add_column("signal_inbox", col)


                # ---- helpful indexes ----
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status, created_at_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_commands_cmd_status ON commands(cmd, status)")
                except Exception:
                    pass


                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_equity_time ON equity_history(timestamp_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_equity_date ON equity_history(date_utc)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_open_time ON trades(status, timestamp_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_closed_time ON trades(status, closed_at_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_signal_key ON trades(signal_key)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_inbox_dedupe_key ON signal_inbox(dedupe_key)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_time ON analysis_results(signal_time_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_symbol_time ON analysis_results(symbol, signal_time_ms)")
                except Exception:
                    pass

                # ---- threshold_adjustments table ----
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS threshold_adjustments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mode TEXT NOT NULL,
                        exit_profile TEXT NOT NULL,
                        adjustment REAL NOT NULL DEFAULT 0.0,
                        updated_at TEXT,
                        updated_at_ms INTEGER,
                        source TEXT,
                        UNIQUE(mode, exit_profile)
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_threshold_adj_mode_profile ON threshold_adjustments(mode, exit_profile)")
                except Exception:
                    pass

# ---- recommendations table ----
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS recommendations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date_utc TEXT,
                        created_at_ms INTEGER,
                        key TEXT,
                        current_value TEXT,
                        suggested_value TEXT,
                        category TEXT,
                        reason TEXT,
                        score REAL DEFAULT 0,
                        status TEXT DEFAULT 'PENDING',
                        approved_at_ms INTEGER
                    )
                """)
                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_recs_date ON recommendations(date_utc)")
                except Exception:
                    pass

                
                # ---- pump hunter additions ----
                try:
                    self.cursor.execute(
                        """CREATE TABLE IF NOT EXISTS pump_candidates (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            created_at TEXT,
                            created_at_ms INTEGER,
                            symbol TEXT,
                            market_type TEXT,
                            interval TEXT,
                            price REAL,
                            pump_score INTEGER,
                            ai_vote TEXT,
                            ai_confidence REAL,
                            funding REAL,
                            oi_change REAL,
                            status TEXT DEFAULT 'PENDING',
                            note TEXT,
                            payload_json TEXT
                        )"""
                    )
                except Exception:
                    pass

                try:
                    self.cursor.execute(
                        """CREATE TABLE IF NOT EXISTS pump_hunter_state (
                            id INTEGER PRIMARY KEY CHECK (id = 1),
                            last_heartbeat_ms INTEGER,
                            last_scan_ms INTEGER,
                            last_scan_count INTEGER,
                            last_error TEXT
                        )"""
                    )
                    # Ensure singleton row exists
                    self.cursor.execute(
                        """INSERT OR IGNORE INTO pump_hunter_state (id, last_heartbeat_ms, last_scan_ms, last_scan_count, last_error)
                            VALUES (1, 0, 0, 0, '')"""
                    )
                except Exception:
                    pass

                try:
                    self.cursor.execute(
                        """CREATE TABLE IF NOT EXISTS pump_hunter_oi (
                            symbol TEXT PRIMARY KEY,
                            last_oi REAL,
                            last_ms INTEGER
                        )"""
                    )
                except Exception:
                    pass

                try:
                    self.cursor.execute(
                        """CREATE TABLE IF NOT EXISTS pump_labels (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            created_at_ms INTEGER,
                            symbol TEXT,
                            timestamp_ms INTEGER,
                            label TEXT,
                            note TEXT
                        )"""
                    )
                except Exception:
                    pass

                try:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pump_candidates_ms ON pump_candidates(created_at_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pump_candidates_sym ON pump_candidates(symbol)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pump_candidates_status ON pump_candidates(status)")
                except Exception:
                    pass

                self.conn.commit()

        except Exception as e:
            print(f"⚠️ DB Schema ensure failed: {e}")

    # ==========================================================
    # DEFAULT SETTINGS
    # ==========================================================
    def _seed_defaults(self):
        defaults = {
            "threshold_adjustment": "0.0",
            "mode": "TEST",
            "scalp_size_usd": "100",
            "swing_size_usd": "500",
            "max_trade_usd": "2000",
            "strategy_mode": "CONSERVATIVE",
            "blacklist": "",
            "max_concurrent_trades": "10",
            "gate_atr_min_pct": "0.003",
            "gate_adx_min": "10",
            "rsi_max_buy": "70",
            "pump_score_min": "75",
            "min_conf_scalp": "55",
            "min_conf_swing": "65",
            "sl_scalp_mult": "0.8",
            "tp_scalp_mult": "1.2",
            "sl_swing_mult": "1.5",
            "tp_swing_mult": "2.0",
            "ind_rsi_len": "14",
            "ind_adx_len": "14",
            "ind_ma_fast": "50",
            "ind_ma_slow": "200",

            "enable_learning": "TRUE",
            "webhook_url": "",
            "webhook_enable_signal": "FALSE",
            "webhook_enable_trade": "FALSE",

            # Backward compatibility only. Keep FALSE for Zero-ENV behavior.
            "allow_env_overrides": "FALSE",

            "paper_tournament_enabled": "TRUE",
            "paper_eval_enabled": "TRUE",
            "paper_leaderboard_days": "7",

            "fut_pump_enabled": "FALSE",
            "fut_pump_score_min": "75",
            "fut_pump_oi_min": "0.015",
            "fut_pump_funding_max": "-0.0005",

            # Daily paper (UTC)
            "paper_daily_enabled": "TRUE",
            "paper_daily_time_utc": "00:05",
            "paper_daily_last_run_utc": "",

            # Doctor / Explainability
            "decision_trace_enabled": "TRUE",
            "decision_trace_every_sec": "15",
            "protection_audit_enabled": "TRUE",
            "protection_audit_every_sec": "30",

            "__settings_version__": "0",
            "paper_eval_interval_min": "15",
        
            "inbox_dedupe_sec": "60",
            "cooldown_after_trade_sec": "900",
            "cooldown_after_sl_sec": "1800",
            "symbol_lock_ttl_sec": "30",
            "block_if_open_position": "1",}

        for k, v in defaults.items():
            if self.get_setting(k) is None:
                self.set_setting(k, v, source="seed", bump_version=False, audit=False)

    # ==========================================================
    # SETTINGS
    # ==========================================================
    def get_setting(self, key, default=None):
        try:
            with self.lock:
                self.cursor.execute("SELECT value FROM settings WHERE key=?", (key,))
                row = self.cursor.fetchone()
                return row["value"] if row else default
        except Exception:
            return default

    # ==========================================================
    # DYNAMIC THRESHOLD ADJUSTMENT (Learning/Calibration)
    # ==========================================================
    def get_threshold_adjustment(self, mode: str, exit_profile: str = "") -> float:
        """Return dynamic adjustment added to min_conf.

        The bot uses: min_conf += db.get_threshold_adjustment(mode, exit_profile)

        Lookup order:
          1) threshold_adjustments table (mode + exit_profile)
          2) settings keys:
             - threshold_adjustment_{mode}_{exit_profile}
             - threshold_adjustment_{mode}
             - threshold_adjustment
        Fallback: 0.0
        """
        m = (mode or "").strip().lower()
        p = (exit_profile or "").strip().lower()
        if not m:
            return 0.0

        # 1) table lookup
        try:
            with self.lock:
                self.cursor.execute(
                    "SELECT adjustment FROM threshold_adjustments WHERE mode=? AND exit_profile=? LIMIT 1",
                    (m, p),
                )
                row = self.cursor.fetchone()
            if row and row["adjustment"] is not None:
                return float(row["adjustment"])
        except Exception:
            pass

        # 2) settings fallback
        try:
            keys = []
            if p:
                keys.append(f"threshold_adjustment_{m}_{p}")
            keys.append(f"threshold_adjustment_{m}")
            keys.append("threshold_adjustment")

            for k in keys:
                v = self.get_setting(k)
                if v is None or str(v).strip() == "":
                    continue
                try:
                    return float(v)
                except Exception:
                    continue
        except Exception:
            pass

        return 0.0

    def set_threshold_adjustment(self, mode: str, exit_profile: str, adjustment: float, source: str = "system") -> None:
        """Upsert threshold adjustment for (mode, exit_profile)."""
        m = (mode or "").strip().lower()
        p = (exit_profile or "").strip().lower()
        if not m:
            return

        try:
            adj = float(adjustment)
        except Exception:
            adj = 0.0

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(microsecond=0)
        now_iso = now.isoformat().replace("+00:00", "Z")
        now_ms = int(now.timestamp() * 1000)

        with self.lock:
            # Ensure table exists (safe no-op if already)
            try:
                self.cursor.execute(
                    """CREATE TABLE IF NOT EXISTS threshold_adjustments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mode TEXT NOT NULL,
                        exit_profile TEXT NOT NULL,
                        adjustment REAL NOT NULL DEFAULT 0.0,
                        updated_at TEXT,
                        updated_at_ms INTEGER,
                        source TEXT,
                        UNIQUE(mode, exit_profile)
                    )"""
                )
            except Exception:
                pass

            try:
                # SQLite UPSERT (3.24+)
                self.cursor.execute(
                    """INSERT INTO threshold_adjustments(mode, exit_profile, adjustment, updated_at, updated_at_ms, source)
                       VALUES(?,?,?,?,?,?)
                       ON CONFLICT(mode, exit_profile)
                       DO UPDATE SET adjustment=excluded.adjustment,
                                     updated_at=excluded.updated_at,
                                     updated_at_ms=excluded.updated_at_ms,
                                     source=excluded.source
                    """,
                    (m, p, adj, now_iso, now_ms, source),
                )
            except Exception:
                # Fallback for older SQLite
                self.cursor.execute(
                    "DELETE FROM threshold_adjustments WHERE mode=? AND exit_profile=?",
                    (m, p),
                )
                self.cursor.execute(
                    """INSERT INTO threshold_adjustments(mode, exit_profile, adjustment, updated_at, updated_at_ms, source)
                       VALUES(?,?,?,?,?,?)""",
                    (m, p, adj, now_iso, now_ms, source),
                )

            self.conn.commit()
    def get_all_settings(self):
        """Return all settings as dict."""
        try:
            with self.lock:
                self.cursor.execute("SELECT key, value FROM settings")
                rows = self.cursor.fetchall()
            return {r["key"]: r["value"] for r in rows} if rows else {}
        except Exception:
            return {}

    def get_settings_version(self) -> int:
        """Read version directly from DB."""
        try:
            with self.lock:
                self.cursor.execute("SELECT value FROM settings WHERE key='__settings_version__'")
                row = self.cursor.fetchone()
            if not row:
                return 0
            return int(float(row["value"]))
        except Exception:
            return 0

    def bump_settings_version(self, source="system") -> int:
        """Increment __settings_version__ safely and audit it.

        - Avoids INSERT OR REPLACE when richer settings schema exists (so we don't drop metadata).
        - Writes created_at_ms to settings_audit if that column exists.
        """
        try:
            now_iso = self._utc_iso()
            now_ms = self._ms_now()
            with self.lock:
                # Read old
                self.cursor.execute("SELECT value FROM settings WHERE key='__settings_version__'")
                row = self.cursor.fetchone()
                old_v = int(float(row["value"])) if row and row.get("value") is not None else (int(float(row[0])) if row and row[0] is not None else 0)
                new_v = old_v + 1

                cols = self._table_columns('settings')
                if {'updated_at','version','source'}.intersection(cols):
                    # Prefer an UPSERT that preserves extra columns
                    try:
                        self.cursor.execute(
                            """INSERT INTO settings(key,value,updated_at,source)
                               VALUES(?,?,?,?)
                               ON CONFLICT(key) DO UPDATE SET
                                 value=excluded.value,
                                 updated_at=excluded.updated_at,
                                 source=excluded.source
                            """,
                            ('__settings_version__', str(new_v), now_iso, str(source))
                        )
                    except Exception:
                        self.cursor.execute("UPDATE settings SET value=? WHERE key=?", (str(new_v), '__settings_version__'))
                else:
                    self.cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ('__settings_version__', str(new_v)))

                # Audit
                cols_a = self._table_columns('settings_audit')
                if 'created_at_ms' in cols_a:
                    self.cursor.execute(
                        "INSERT INTO settings_audit (key, old_value, new_value, source, created_at, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                        ('__settings_version__', str(old_v), str(new_v), str(source), now_iso, int(now_ms))
                    )
                else:
                    self.cursor.execute(
                        "INSERT INTO settings_audit (key, old_value, new_value, source, created_at) VALUES (?, ?, ?, ?, ?)",
                        ('__settings_version__', str(old_v), str(new_v), str(source), now_iso)
                    )

                self.conn.commit()
            return new_v
        except Exception:
            return self.get_settings_version()



    # ==========================================================
    # RUN MODE (Sprint 9) - single source of truth
    # ==========================================================
    @staticmethod
    def _normalize_run_mode(mode) -> str:
        m = str(mode or '').strip().upper()
        if m in ('PAPER', 'PAPER_TRADING', 'SIM', 'SIMULATED'):
            return 'PAPER'
        if m in ('TEST', 'TESTNET', 'DEMO'):
            return 'TEST'
        if m in ('LIVE', 'REAL', 'PROD', 'PRODUCTION'):
            return 'LIVE'
        return 'TEST'

    @classmethod
    def _mode_to_legacy(cls, mode: str) -> dict:
        m = cls._normalize_run_mode(mode)
        return {
            'RUN_MODE': m,
            'MODE': m,
            # Legacy boolean flags (strings so they work with existing UI/logic)
            'USE_TESTNET': 'TRUE' if m in ('TEST','PAPER') else 'FALSE',
            'PAPER_TRADING': 'TRUE' if m == 'PAPER' else 'FALSE',
            'ENABLE_LIVE_TRADING': 'TRUE' if m in ('TEST','LIVE') else 'FALSE',
        }

    def get_run_mode(self) -> str:
        try:
            return self._normalize_run_mode(self.get_setting('mode', 'TEST'))
        except Exception:
            return 'TEST'

    def sync_legacy_mode_keys(self, mode: str, source: str = 'mode_sync') -> None:
        """Keep legacy keys in settings synced with canonical `mode`.

        This is safe to call repeatedly; it only writes when values differ.
        It DOES NOT bump the settings version (to avoid noisy reload loops).
        """
        m = self._normalize_run_mode(mode)
        legacy = self._mode_to_legacy(m)
        # Ensure canonical mode exists and is normalized
        try:
            cur = str(self.get_setting('mode', '') or '').strip().upper()
        except Exception:
            cur = ''
        if cur != m:
            # set canonical mode (no recursion)
            try:
                self.set_setting('mode', m, source=source, bump_version=False, audit=False, sync_mode=False)
            except Exception:
                pass

        for k, v in legacy.items():
            try:
                existing = self.get_setting(k, None)
            except Exception:
                existing = None
            if (existing is None) or (str(existing) != str(v)):
                try:
                    self.set_setting(k, v, source=source, bump_version=False, audit=False, sync_mode=False)
                except Exception:
                    pass

    def set_setting(self, key, value, source="system", bump_version=True, audit=True, sync_mode=True):
        """Set a setting with optional audit + version bump."""
        try:
            now = self._utc_iso()
            new_val = str(value)

            with self.lock:
                cols = self._table_columns("settings")

                # Read old value (works for both schemas)
                try:
                    self.cursor.execute("SELECT value FROM settings WHERE key=?", (key,))
                    row = self.cursor.fetchone()
                    old_val = row["value"] if row else None
                except Exception:
                    old_val = None

                # Prefer richer schema if available; fallback to legacy (key,value)
                if "updated_at" in cols or "version" in cols or "source" in cols:
                    # Ensure add-only migrations are applied
                    try:
                        if "updated_at" not in cols:
                            self._add_column("settings", "updated_at TEXT")
                        if "version" not in cols:
                            self._add_column("settings", "version INTEGER DEFAULT 0")
                        if "source" not in cols:
                            self._add_column("settings", "source TEXT")
                    except Exception:
                        pass

                    cols2 = self._table_columns("settings")
                    if "version" in cols2:
                        # Upsert with version increment (per-setting)
                        self.cursor.execute(
                            """
                            INSERT INTO settings(key,value,updated_at,version,source)
                            VALUES(?,?,?,?,?)
                            ON CONFLICT(key) DO UPDATE SET
                                value=excluded.value,
                                updated_at=excluded.updated_at,
                                version=COALESCE(settings.version,0)+1,
                                source=excluded.source
                            """,
                            (key, new_val, now, 0, source),
                        )
                    else:
                        # No version column (rare): update value + metadata only
                        if "updated_at" in cols2 and "source" in cols2:
                            self.cursor.execute(
                                """
                                INSERT INTO settings(key,value,updated_at,source)
                                VALUES(?,?,?,?)
                                ON CONFLICT(key) DO UPDATE SET
                                    value=excluded.value,
                                    updated_at=excluded.updated_at,
                                    source=excluded.source
                                """,
                                (key, new_val, now, source),
                            )
                        else:
                            self.cursor.execute(
                                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                                (key, new_val),
                            )
                else:
                    # Legacy schema
                    self.cursor.execute(
                        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                        (key, new_val)
                    )

                if audit and (old_val != new_val):
                    self.cursor.execute(
                        "INSERT INTO settings_audit (key, old_value, new_value, source, created_at) VALUES (?, ?, ?, ?, ?)",
                        (key, old_val, new_val, source, now)
                    )

                self.conn.commit()

            # Sprint 9: when mode changes, sync legacy keys to prevent conflicts
            if sync_mode and (str(key).strip().upper() in ("MODE", "RUN_MODE", "MODE_SETTING") or str(key).strip().lower() == "mode") :
                try:
                    self.sync_legacy_mode_keys(new_val, source=source)
                except Exception:
                    pass
            if bump_version and key != "__settings_version__":
                self.bump_settings_version(source=source)

            return True

        except Exception as e:
            print(f"❌ DB Set Setting Error: {e}")
            return False


    # ==========================================================
    # TRADE STATE (FSM)
    # ==========================================================
    def get_trade_state(self, symbol):
        try:
            with self.lock:
                self.cursor.execute("SELECT * FROM trade_state WHERE symbol=?", (symbol,))
                row = self.cursor.fetchone()
                if not row:
                    return None

                d = dict(row)
                if d.get("take_profits"):
                    try:
                        d["take_profits"] = json.loads(d["take_profits"])
                    except Exception:
                        d["take_profits"] = []
                return d
        except Exception:
            return None

    def set_trade_state(
        self, symbol, state, strategy=None, signal=None,
        entry_price=None, stop_loss=None,
        take_profits=None, confidence=None, reason=None
    ):
        now = self._utc_iso()
        ms = self._ms_now()
        try:
            with self.lock:
                try:
                    # Newer schema (with *_ms columns)
                    self.cursor.execute("""
                        INSERT INTO trade_state
                        (symbol, state, strategy, signal, entry_price, stop_loss,
                         take_profits, confidence, reason, state_since, state_since_ms, last_update, last_update_ms)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            state=excluded.state,
                            strategy=excluded.strategy,
                            signal=excluded.signal,
                            entry_price=excluded.entry_price,
                            stop_loss=excluded.stop_loss,
                            take_profits=excluded.take_profits,
                            confidence=excluded.confidence,
                            reason=excluded.reason,
                            last_update=excluded.last_update,
                            last_update_ms=excluded.last_update_ms
                    """, (
                        symbol, state, strategy, signal,
                        float(entry_price) if entry_price is not None else None,
                        float(stop_loss) if stop_loss is not None else None,
                        json.dumps(take_profits) if isinstance(take_profits, (list, dict)) else (take_profits if take_profits is not None else None),
                        float(confidence) if confidence is not None else None,
                        reason,
                        now, ms,
                        now, ms
                    ))
                except Exception:
                    # Backward-compatible schema
                    self.cursor.execute("""
                        INSERT INTO trade_state
                        (symbol, state, strategy, signal, entry_price, stop_loss,
                         take_profits, confidence, reason, state_since, last_update)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            state=excluded.state,
                            strategy=excluded.strategy,
                            signal=excluded.signal,
                            entry_price=excluded.entry_price,
                            stop_loss=excluded.stop_loss,
                            take_profits=excluded.take_profits,
                            confidence=excluded.confidence,
                            reason=excluded.reason,
                            last_update=excluded.last_update
                    """, (
                        symbol, state, strategy, signal,
                        float(entry_price) if entry_price is not None else None,
                        float(stop_loss) if stop_loss is not None else None,
                        json.dumps(take_profits) if isinstance(take_profits, (list, dict)) else (take_profits if take_profits is not None else None),
                        float(confidence) if confidence is not None else None,
                        reason,
                        now, now
                    ))
                self.conn.commit()
        except Exception:
            pass

    def clear_trade_state(self, symbol):
        try:
            with self.lock:
                self.cursor.execute("DELETE FROM trade_state WHERE symbol=?", (symbol,))
                self.conn.commit()
        except Exception:
            pass

    # ==========================================================
    
    # ==========================================================
    # TRADE EVENTS / FSM (Sprint 8)
    # ==========================================================
    def add_trade_event(self, trade_id: int, event_type: str, payload: dict = None, source: str = "system", already_locked: bool = False, commit: bool = True) -> None:
        """Append an immutable trade event (best-effort).
        If already_locked=True, caller holds self.lock and will commit.
        """
        if not trade_id or not event_type:
            return
        ctx = nullcontext() if already_locked else self.lock
        try:
            with ctx:
                # Ensure table exists (best-effort)
                try:
                    self.cursor.execute("""
                        CREATE TABLE IF NOT EXISTS trade_events (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            trade_id INTEGER,
                            event_type TEXT,
                            event_ts TEXT,
                            event_ts_ms INTEGER,
                            payload_json TEXT,
                            source TEXT
                        )
                    """)
                except Exception:
                    pass

                ts = self._utc_iso()
                ts_ms = self._ms_now()
                pj = json.dumps(payload or {}, ensure_ascii=False)
                self.cursor.execute(
                    "INSERT INTO trade_events(trade_id, event_type, event_ts, event_ts_ms, payload_json, source) VALUES (?,?,?,?,?,?)",
                    (int(trade_id), str(event_type), ts, int(ts_ms), pj, str(source or "system")),
                )
                if commit and not already_locked:
                    self.conn.commit()
        except Exception:
            return

    def set_trade_fsm_state(self, trade_id: int, new_state: str, meta: dict = None, source: str = "system", already_locked: bool = False, commit: bool = True) -> None:
        """Update trade FSM state (best-effort, safe add-only).
        Writes fields if present and logs a trade_event.
        """
        if not trade_id or not new_state:
            return
        ctx = nullcontext() if already_locked else self.lock
        try:
            with ctx:
                cols = self._table_columns('trades')
                now_ms = self._ms_now()

                # Read current state (best-effort)
                old_state = None
                try:
                    if 'fsm_state' in cols:
                        r = self.cursor.execute('SELECT fsm_state FROM trades WHERE id=?', (int(trade_id),)).fetchone()
                        if r:
                            old_state = r[0]
                except Exception:
                    old_state = None

                updates = {}
                if 'fsm_state' in cols:
                    updates['fsm_state'] = str(new_state)
                if 'fsm_state_updated_ms' in cols:
                    updates['fsm_state_updated_ms'] = int(now_ms)
                if 'fsm_state_since_ms' in cols:
                    # reset since_ms when state changes; keep if same
                    try:
                        if old_state != new_state:
                            updates['fsm_state_since_ms'] = int(now_ms)
                    except Exception:
                        updates['fsm_state_since_ms'] = int(now_ms)

                if updates:
                    set_clause = ', '.join([f"{k}=?" for k in updates.keys()])
                    vals = list(updates.values()) + [int(trade_id)]
                    self.cursor.execute(f"UPDATE trades SET {set_clause} WHERE id=?", vals)

                # Event log
                try:
                    self.add_trade_event(
                        trade_id=int(trade_id),
                        event_type='FSM_STATE',
                        payload={"from": old_state, "to": str(new_state), "meta": meta or {}},
                        source=source,
                        already_locked=True,
                        commit=False,
                    )
                except Exception:
                    pass

                if commit and not already_locked:
                    self.conn.commit()
        except Exception:
            return


    # ----------------------------------------------------------
    # Sprint 8: Trade protection status (SL/TP/Trailing verifier)
    # ----------------------------------------------------------
    def update_trade_protection(
        self,
        trade_id: int,
        status: str,
        details: dict = None,
        error: str = None,
        source: str = "system",
        emit_event: bool = True,
        already_locked: bool = False,
        commit: bool = True,
    ) -> None:
        """Update protection fields on trades (safe add-only columns).

        This is a lightweight, best-effort helper used by execution_monitor.
        It will NOT raise (to avoid breaking runtime loops).
        """
        try:
            if trade_id is None:
                return
            def _do_update():
                now_ms = self._ms_now()
                cols = self._table_columns('trades')

                # Detect previous status for event suppression
                prev_status = None
                try:
                    if 'protection_status' in cols:
                        r = self.cursor.execute(
                            "SELECT protection_status FROM trades WHERE id=?",
                            (int(trade_id),)
                        ).fetchone()
                        if r is not None:
                            prev_status = r[0]
                except Exception:
                    prev_status = None

                updates = {}
                if 'protection_status' in cols:
                    updates['protection_status'] = (status or '').strip().upper() if status is not None else None
                if 'protection_last_check_ms' in cols:
                    updates['protection_last_check_ms'] = int(now_ms)
                if 'protection_details' in cols:
                    try:
                        updates['protection_details'] = json.dumps(details or {}, ensure_ascii=False)
                    except Exception:
                        updates['protection_details'] = json.dumps({"note": "details serialization failed"})
                if 'protection_error' in cols:
                    updates['protection_error'] = (str(error)[:800] if error else None)

                if updates:
                    set_clause = ', '.join([f"{k}=?" for k in updates.keys()])
                    vals = list(updates.values()) + [int(trade_id)]
                    self.cursor.execute(f"UPDATE trades SET {set_clause} WHERE id=?", vals)

                # Log event only when status changes OR status indicates a problem
                if emit_event and hasattr(self, 'add_trade_event'):
                    try:
                        new_status = (status or '').strip().upper()
                        changed = (prev_status is None) or (str(prev_status).strip().upper() != new_status)
                        problematic = new_status.startswith('MISSING') or new_status in (
                            'ERROR', 'SKIPPED_NO_KEYS', 'SKIPPED_NO_AUTH'
                        )
                        if changed or problematic:
                            self.add_trade_event(
                                trade_id=int(trade_id),
                                event_type='PROTECTION_CHECK',
                                payload={
                                    'status': new_status,
                                    'error': (str(error)[:400] if error else None),
                                    'details': (details or {}),
                                },
                                source=source,
                                already_locked=True,
                                commit=False,
                            )
                    except Exception:
                        pass

            if already_locked:
                _do_update()
            else:
                with self.lock:
                    _do_update()
                if commit:
                    try:
                        self.conn.commit()
                    except Exception:
                        pass
        except Exception:
            return

    # TRADES MANAGEMENT
    # ==========================================================
    def add_trade(self, payload):
        """
        Insert an OPEN trade.
        Works with old payloads and new payloads (market_type/strategy_tag/etc).
        """
        try:
            explain = payload.get("explain") or {}
            market_type = payload.get("market_type")
            strategy_tag = payload.get("strategy_tag")
            exit_profile = payload.get("exit_profile")
            leverage = payload.get("leverage")

            tech_score = payload.get("tech_score") if payload.get("tech_score") is not None else explain.get("tech_score")
            deriv_score = payload.get("deriv_score") if payload.get("deriv_score") is not None else explain.get("deriv_score")
            vol_spike = payload.get("vol_spike") if payload.get("vol_spike") is not None else explain.get("vol_spike")
            ai_vote = payload.get("ai_vote") if payload.get("ai_vote") is not None else explain.get("ai_vote")
            ai_conf = payload.get("ai_confidence") if payload.get("ai_confidence") is not None else explain.get("ai_confidence")
            funding = payload.get("funding") if payload.get("funding") is not None else explain.get("funding")
            oi_change = payload.get("oi_change") if payload.get("oi_change") is not None else explain.get("oi_change")

            ts = payload.get("timestamp") or self._utc_iso()
            ts_ms = payload.get("timestamp_ms")
            if not ts_ms:
                try:
                    s = str(ts).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dt = dt.astimezone(timezone.utc)
                    ts_ms = int(dt.timestamp() * 1000)
                except Exception:
                    ts_ms = self._ms_now()

            with self.lock:
                self.cursor.execute("""
                    INSERT INTO trades
                    (symbol, signal, entry_price, quantity, stop_loss,
                     take_profits, status, timestamp, timestamp_ms,
                     explain, confidence,
                     market_type, strategy_tag, exit_profile, leverage,
                     tech_score, deriv_score, vol_spike,
                     ai_vote, ai_confidence, funding, oi_change)
                    VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    payload.get("symbol"),
                    payload.get("signal"),
                    float(payload.get("entry_price") or 0.0),
                    float(payload.get("quantity") or 0.0),
                    float(payload.get("stop_loss") or 0.0),
                    json.dumps(payload.get("take_profits", [])),
                    ts,
                    int(ts_ms),
                    json.dumps(explain),
                    float(payload.get("confidence") or 0.0),
                    market_type,
                    strategy_tag,
                    exit_profile,
                    int(leverage) if leverage is not None else None,
                    float(tech_score) if tech_score is not None else None,
                    float(deriv_score) if deriv_score is not None else None,
                    int(vol_spike) if vol_spike is not None else None,
                    ai_vote,
                    float(ai_conf) if ai_conf is not None else None,
                    float(funding) if funding is not None else None,
                    float(oi_change) if oi_change is not None else None
                ))
                trade_id = int(self.cursor.lastrowid)

                # Sprint 8: trade history + FSM (best-effort, add-only)
                try:
                    self.add_trade_event(
                        trade_id=trade_id,
                        event_type="TRADE_CREATED",
                        payload={
                            "symbol": payload.get("symbol"),
                            "signal": payload.get("signal"),
                            "entry_price": payload.get("entry_price"),
                            "qty": payload.get("quantity"),
                            "market_type": market_type,
                            "strategy_tag": strategy_tag,
                            "timeframe": payload.get("timeframe"),
                            "candle_close_ms": payload.get("candle_close_ms"),
                            "signal_key": payload.get("signal_key"),
                        },
                        source="db.add_trade",
                        already_locked=True,
                        commit=False,
                    )
                    self.set_trade_fsm_state(
                        trade_id=trade_id,
                        new_state="OPEN",
                        meta={"note": "trade inserted"},
                        source="db.add_trade",
                        already_locked=True,
                        commit=False,
                    )
                except Exception:
                    pass

                # Sprint 5: idempotency metadata (safe add-only columns)
                try:
                    cols = self._table_columns('trades')
                    updates = {}
                    if 'signal_key' in cols and payload.get('signal_key'):
                        updates['signal_key'] = str(payload.get('signal_key'))
                    if 'candle_close_ms' in cols and payload.get('candle_close_ms') is not None:
                        updates['candle_close_ms'] = int(payload.get('candle_close_ms'))
                    if 'timeframe' in cols and payload.get('timeframe'):
                        updates['timeframe'] = str(payload.get('timeframe'))
                    if 'inbox_id' in cols and payload.get('inbox_id') is not None:
                        updates['inbox_id'] = int(payload.get('inbox_id'))
                    if updates:
                        set_clause = ', '.join([f"{k}=?" for k in updates.keys()])
                        vals = list(updates.values()) + [trade_id]
                        self.cursor.execute(f"UPDATE trades SET {set_clause} WHERE id=?", vals)
                except Exception:
                    pass

                self.conn.commit()
                return trade_id

        except Exception as e:
            print(f"❌ DB Add Trade Error: {e}")

    def update_stop_loss(self, symbol, new_sl):
        try:
            with self.lock:
                self.cursor.execute(
                    "UPDATE trades SET stop_loss = ? WHERE symbol = ? AND status='OPEN'",
                    (float(new_sl), symbol)
                )
                self.conn.commit()
        except Exception:
            pass

    def close_trade(self, trade_id_or_symbol, reason, close_price, exit_explain=None, explain=None):
        """
        Close an OPEN trade by symbol OR trade_id (backwards compatible).
        - old usage: close_trade(symbol, reason, close_price, exit_explain)
        - new usage: close_trade(trade_id, reason="...", close_price=..., explain={...})
        """
        if exit_explain is None and explain is not None:
            exit_explain = explain

        try:
            with self.lock:
                row = None

                # Try as trade_id
                try:
                    if isinstance(trade_id_or_symbol, int):
                        trade_id = int(trade_id_or_symbol)
                    elif isinstance(trade_id_or_symbol, str) and trade_id_or_symbol.isdigit():
                        trade_id = int(trade_id_or_symbol)
                    else:
                        trade_id = None
                except Exception:
                    trade_id = None

                if trade_id is not None:
                    self.cursor.execute(
                        "SELECT id, symbol, entry_price, quantity, signal, explain FROM trades WHERE id=? AND status='OPEN'",
                        (trade_id,)
                    )
                    row = self.cursor.fetchone()
                else:
                    symbol = str(trade_id_or_symbol)
                    self.cursor.execute(
                        "SELECT id, symbol, entry_price, quantity, signal, explain FROM trades WHERE symbol=? AND status='OPEN'",
                        (symbol,)
                    )
                    row = self.cursor.fetchone()

                if not row:
                    return

                trade_id = int(row["id"])

                # Sprint 8: FSM transition (best-effort)
                try:
                    self.set_trade_fsm_state(
                        trade_id=trade_id,
                        new_state="CLOSING",
                        meta={"reason": reason},
                        source="db.close_trade",
                        already_locked=True,
                        commit=False,
                    )
                except Exception:
                    pass
                entry = float(row["entry_price"] or 0.0)
                qty = float(row["quantity"] or 0.0)
                sig = str(row["signal"] or "")

                base_explain = json.loads(row["explain"]) if row["explain"] else {}

                pnl = 0.0
                cp = float(close_price) if close_price is not None else 0.0
                if entry > 0 and qty > 0:
                    sig_u = sig.upper()
                    is_long = ("LONG" in sig_u) or ("BUY" in sig_u)
                    is_short = ("SHORT" in sig_u) or ("SELL" in sig_u)
                    # If ambiguous (both), prefer explicit LONG/SHORT rules
                    if "LONG" in sig_u:
                        pnl = (cp - entry) * qty
                    elif "SHORT" in sig_u:
                        pnl = (entry - cp) * qty
                    elif is_long and not is_short:
                        pnl = (cp - entry) * qty
                    elif is_short and not is_long:
                        pnl = (entry - cp) * qty


                base_explain["exit"] = {
                    "reason": reason,
                    "price": cp,
                    "time": self._utc_iso(),
                    **(exit_explain or {})
                }

                self.cursor.execute("""
                    UPDATE trades
                    SET status='CLOSED',
                        close_reason=?,
                        close_price=?,
                        closed_at=?,
                        closed_at_ms=?,
                        pnl=?,
                        explain=?
                    WHERE id=? AND status='OPEN'
                """, (
                    reason,
                    cp,
                    self._utc_iso(),
                    self._ms_now(),
                    float(pnl),
                    json.dumps(base_explain),
                    trade_id
                ))

                # Sprint 8: trade close event + state (best-effort)
                try:
                    self.add_trade_event(
                        trade_id=trade_id,
                        event_type="TRADE_CLOSED",
                        payload={
                            "reason": reason,
                            "close_price": cp,
                            "pnl": pnl,
                        },
                        source="db.close_trade",
                        already_locked=True,
                        commit=False,
                    )
                    self.set_trade_fsm_state(
                        trade_id=trade_id,
                        new_state="CLOSED",
                        meta={"reason": reason, "pnl": pnl},
                        source="db.close_trade",
                        already_locked=True,
                        commit=False,
                    )
                except Exception:
                    pass

                self.conn.commit()

                # Closed-loop: label sample linked to this trade (best-effort)
                try:
                    self.close_ai_sample_for_trade(trade_id, cp, pnl, reason)
                except Exception:
                    pass

        except Exception as e:
            print(f"❌ DB Close Trade Error: {e}")

    def get_active_trade(self, symbol):
        try:
            with self.lock:
                self.cursor.execute("SELECT * FROM trades WHERE symbol=? AND status='OPEN'", (symbol,))
                row = self.cursor.fetchone()
                return self._row_to_trade(row) if row else None
        except Exception:
            return None

    def get_all_active_trades(self):
        try:
            with self.lock:
                self.cursor.execute("SELECT * FROM trades WHERE status='OPEN'")
                return [self._row_to_trade(x) for x in self.cursor.fetchall()]
        except Exception:
            return []



    def get_last_trade_for_symbol(self, symbol: str):
        """Return the most recent trade row (OPEN or CLOSED) for a symbol."""
        try:
            sym = str(symbol or "").upper().strip()
            if not sym:
                return None
            with self.lock:
                cur = self.conn.cursor()
                cur.execute("SELECT * FROM trades WHERE symbol=? ORDER BY timestamp_ms DESC LIMIT 1", (sym,))
                row = cur.fetchone()
                return self._row_to_trade(row) if row else None
        except Exception:
            return None

    def get_trade_by_signal_key(self, signal_key: str):
        """Return the most recent trade row for a signal_key (OPEN or CLOSED).

        Used for idempotency to prevent duplicate executions across restarts.
        """
        try:
            sk = str(signal_key or '').strip()
            if not sk:
                return None
            with self.lock:
                cur = self.conn.cursor()
                cur.execute("SELECT * FROM trades WHERE signal_key=? ORDER BY timestamp_ms DESC LIMIT 1", (sk,))
                row = cur.fetchone()
                return self._row_to_trade(row) if row else None
        except Exception:
            return None

    def has_trade_signal_key(self, signal_key: str, *, status: Optional[str] = None) -> bool:
        """True if a trade exists for this signal_key.

        If status is provided (e.g., 'OPEN'), filter by status.
        """
        try:
            sk = str(signal_key or '').strip()
            if not sk:
                return False
            with self.lock:
                cur = self.conn.cursor()
                if status:
                    cur.execute("SELECT 1 FROM trades WHERE signal_key=? AND status=? LIMIT 1", (sk, str(status)))
                else:
                    cur.execute("SELECT 1 FROM trades WHERE signal_key=? LIMIT 1", (sk,))
                return cur.fetchone() is not None
        except Exception:
            return False


    def acquire_symbol_lock(self, symbol: str, ttl_sec: Optional[int] = None):
        """Cross-process best-effort lock using settings table (atomic via BEGIN IMMEDIATE)."""
        try:
            sym = str(symbol or "").upper().strip()
            if not sym:
                return False
            if ttl_sec is None:
                try:
                    ttl_sec = int(float(self.get_setting("symbol_lock_ttl_sec") or 30))
                except Exception:
                    ttl_sec = 30
            ttl_ms = max(1, int(ttl_sec)) * 1000
            key = f"__lock_symbol__::{sym}"
            now_ms = int(time.time() * 1000)

            # Use a dedicated cursor and transaction to reduce races across processes.
            with self.lock:
                cur = self.conn.cursor()
                try:
                    cur.execute("BEGIN IMMEDIATE")
                except Exception:
                    # fallback if already in transaction
                    pass

                cur.execute("SELECT value FROM settings WHERE key=? LIMIT 1", (key,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    try:
                        prev_ms = int(float(row[0]))
                        if (now_ms - prev_ms) < ttl_ms:
                            try:
                                self.conn.rollback()
                            except Exception:
                                pass
                            return False
                    except Exception:
                        pass

                cur.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, str(now_ms)))
                self.conn.commit()
            return True
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False

    def release_symbol_lock(self, symbol: str):
        """Release lock early (optional)."""
        try:
            sym = str(symbol or "").upper().strip()
            if not sym:
                return False
            key = f"__lock_symbol__::{sym}"
            with self.lock:
                cur = self.conn.cursor()
                cur.execute("DELETE FROM settings WHERE key=?", (key,))
                self.conn.commit()
            return True
        except Exception:
            return False

    def get_open_trades(self):
        """Backward-compatible alias for open/active trades."""
        return self.get_all_active_trades()

    def get_daily_pnl(self, day_yyyy_mm_dd: str) -> float:
        """Compute realized PnL for closed trades on a given UTC day.

        day_yyyy_mm_dd: e.g. "2026-01-02"
        """
        day = str(day_yyyy_mm_dd or "").strip()
        if not day:
            return 0.0
        try:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT signal, entry_price, close_price, quantity FROM trades "
                "WHERE status='CLOSED' AND closed_at LIKE ? AND close_price IS NOT NULL",
                (day + "%",),
            )
            rows = cur.fetchall() or []
        except Exception:
            return 0.0

        pnl = 0.0
        for sig, entry, close, qty in rows:
            try:
                sig_s = str(sig or "").upper()
                entry_f = float(entry or 0.0)
                close_f = float(close or 0.0)
                qty_f = float(qty or 0.0)
                if entry_f <= 0 or close_f <= 0 or qty_f <= 0:
                    continue
                # LONG/BUY profit when close > entry; SHORT profit when close < entry
                if "SHORT" in sig_s or "SELL" in sig_s:
                    pnl += (entry_f - close_f) * qty_f
                else:
                    pnl += (close_f - entry_f) * qty_f
            except Exception:
                continue
        return float(pnl)

    def get_recent_losses(self, limit: int = 3):
        """Return last N closed trades that are losses (most recent first)."""
        try:
            lim = int(limit or 0)
        except Exception:
            lim = 0
        if lim <= 0:
            return []

        try:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT id, symbol, signal, entry_price, close_price, quantity, closed_at, closed_at_ms "
                "FROM trades WHERE status='CLOSED' AND close_price IS NOT NULL "
                "ORDER BY closed_at_ms DESC LIMIT ?",
                (lim * 5,),
            )
            rows = cur.fetchall() or []
        except Exception:
            return []

        losses = []
        for rid, sym, sig, entry, close, qty, closed_at, closed_ms in rows:
            try:
                sig_s = str(sig or "").upper()
                entry_f = float(entry or 0.0)
                close_f = float(close or 0.0)
                qty_f = float(qty or 0.0)
                if entry_f <= 0 or close_f <= 0 or qty_f <= 0:
                    continue
                if "SHORT" in sig_s or "SELL" in sig_s:
                    pnl = (entry_f - close_f) * qty_f
                else:
                    pnl = (close_f - entry_f) * qty_f
                if pnl < 0:
                    losses.append(
                        {"id": rid, "symbol": sym, "signal": sig, "pnl": float(pnl), "closed_at": closed_at, "closed_at_ms": closed_ms}
                    )
                if len(losses) >= lim:
                    break
            except Exception:
                continue
        return losses
    def get_all_closed_trades(self):
        try:
            with self.lock:
                self.cursor.execute("SELECT * FROM trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 200")
                return [self._row_to_trade(x) for x in self.cursor.fetchall()]
        except Exception:
            return []


    # ==========================================================
    # DASHBOARD INBOX (signal_inbox) SUPPORT
    # ==========================================================
    def add_signal_inbox(self, *, symbol: str, timeframe: str = "", strategy: str = "", side: str = "",
                         confidence: float = 0.0, score: float = 0.0, payload=None,
                         status: str = "RECEIVED", note=None, source: str = "bot",
                         dedupe_key: str = "", candle_close_ms: Optional[int] = None):
        """Insert a signal into signal_inbox so Dashboard can show/approve it."""
        try:
            sym = str(symbol or "").upper().strip()
            if not sym:
                return None
            now = datetime.now(timezone.utc)
            now_ms = int(now.timestamp() * 1000)
            # Dedupe: avoid inserting repeated PENDING_APPROVAL rows for the same symbol+side within a short window.
            try:
                dedupe_sec = int(float(self.get_setting("inbox_dedupe_sec") or 60))
            except Exception:
                dedupe_sec = 60
            dedupe_ms = max(0, dedupe_sec) * 1000

            st = str(status or "").upper().strip()
            sd = str(side or "").upper().strip()
            dk = str(dedupe_key or '').strip()
            if not dk and isinstance(payload, dict) and payload.get('dedupe_key'):
                dk = str(payload.get('dedupe_key') or '').strip()
            # Strong dedupe across restarts (preferred): same dedupe_key => update existing pending/received/approved row
            if dk:
                try:
                    cols_inbox = self._table_columns('signal_inbox')
                    if 'dedupe_key' in cols_inbox:
                        with self.lock:
                            cur = self.conn.cursor()
                            cur.execute(
                                "SELECT id, status FROM signal_inbox WHERE dedupe_key=? ORDER BY received_at_ms DESC LIMIT 1",
                                (dk,),
                            )
                            row = cur.fetchone()
                            if row and row[0]:
                                iid = int(row[0])
                                prev_st = str(row[1] or '').upper().strip()
                                if prev_st in {'PENDING_APPROVAL','RECEIVED','APPROVED'}:
                                    cur.execute(
                                        "UPDATE signal_inbox SET received_at=?, received_at_ms=?, source=?, status=?, symbol=?, timeframe=?, strategy=?, side=?, confidence=?, score=?, payload=?, note=?, dedupe_key=?, candle_close_ms=? WHERE id=?",
                                        (
                                            now.isoformat().replace('+00:00','Z'),
                                            int(now_ms),
                                            str(source or 'bot'),
                                            st,
                                            sym,
                                            str(timeframe or ''),
                                            str(strategy or ''),
                                            sd,
                                            float(confidence or 0.0),
                                            float(score or 0.0),
                                            json.dumps(payload or {}, ensure_ascii=False),
                                            (str(note) if note is not None else None),
                                            dk,
                                            int(candle_close_ms) if candle_close_ms is not None else None,
                                            iid,
                                        ),
                                    )
                                    self.conn.commit()
                                    # Compat: mirror received_at into created_at columns if present
                                    try:
                                        cols_inbox2 = self._table_columns('signal_inbox')
                                        if 'created_at' in cols_inbox2:
                                            cur.execute("UPDATE signal_inbox SET created_at=COALESCE(created_at, received_at) WHERE id=?", (iid,))
                                        if 'created_at_ms' in cols_inbox2:
                                            cur.execute("UPDATE signal_inbox SET created_at_ms=COALESCE(created_at_ms, received_at_ms) WHERE id=?", (iid,))
                                        self.conn.commit()
                                    except Exception:
                                        pass

                                    return iid
                except Exception:
                    pass

            if dedupe_ms > 0 and st in {"PENDING_APPROVAL", "RECEIVED"} and sym and sd:
                try:
                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute(
                            "SELECT id FROM signal_inbox WHERE symbol=? AND side=? AND status=? AND received_at_ms>=? ORDER BY received_at_ms DESC LIMIT 1",
                            (sym, sd, st, int(now_ms - dedupe_ms)),
                        )
                        row = cur.fetchone()
                        if row and row[0]:
                            iid = int(row[0])
                            cur.execute(
                                "UPDATE signal_inbox SET received_at=?, received_at_ms=?, source=?, status=?, symbol=?, timeframe=?, strategy=?, side=?, confidence=?, score=?, payload=?, note=? WHERE id=?",
                                (
                                    now.isoformat().replace("+00:00", "Z"),
                                    int(now_ms),
                                    str(source or "bot"),
                                    st,
                                    sym,
                                    str(timeframe or ""),
                                    str(strategy or ""),
                                    sd,
                                    float(confidence or 0.0),
                                    float(score or 0.0),
                                    json.dumps(payload or {}, ensure_ascii=False),
                                    (str(note) if note is not None else None),
                                    iid,
                                ),
                            )
                            # Compat: mirror received_at into created_at columns if present
                            try:
                                cols_inbox2 = self._table_columns('signal_inbox')
                                if 'created_at' in cols_inbox2:
                                    cur.execute("UPDATE signal_inbox SET created_at=COALESCE(created_at, received_at) WHERE id=?", (iid,))
                                if 'created_at_ms' in cols_inbox2:
                                    cur.execute("UPDATE signal_inbox SET created_at_ms=COALESCE(created_at_ms, received_at_ms) WHERE id=?", (iid,))
                                self.conn.commit()
                            except Exception:
                                pass

                            self.conn.commit()
                            return iid
                except Exception:
                    pass

            with self.lock:
                cur = self.conn.cursor()
                cur.execute(
                    """
                    INSERT INTO signal_inbox
                        (received_at, received_at_ms, source, status, symbol, timeframe, strategy, side,
                         confidence, score, payload, note)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now.isoformat(),
                        now_ms,
                        str(source or "bot"),
                        str(status or "RECEIVED"),
                        sym,
                        str(timeframe or ""),
                        str(strategy or ""),
                        str(side or ""),
                        float(confidence or 0.0),
                        float(score or 0.0),
                        json.dumps(payload or {}, ensure_ascii=False),
                        str(note) if note is not None else None,
                    ),
                )
                inbox_id = int(cur.lastrowid)

                # Sprint 5: set dedupe_key/candle_close_ms if columns exist (non-breaking)
                try:
                    cols_inbox = self._table_columns('signal_inbox')
                    updates = {}
                    if 'dedupe_key' in cols_inbox and dk:
                        updates['dedupe_key'] = dk
                    if 'candle_close_ms' in cols_inbox and candle_close_ms is not None:
                        updates['candle_close_ms'] = int(candle_close_ms)
                    # Backward compatible aliases for tooling (project_doctor expects created_at)
                    if 'created_at' in cols_inbox:
                        updates['created_at'] = now.isoformat().replace('+00:00','Z')
                    if 'created_at_ms' in cols_inbox:
                        updates['created_at_ms'] = int(now_ms)
                    if updates:
                        set_clause = ', '.join([f"{k}=?" for k in updates.keys()])
                        vals = list(updates.values()) + [inbox_id]
                        cur.execute(f"UPDATE signal_inbox SET {set_clause} WHERE id=?", vals)
                        self.conn.commit()
                except Exception:
                    pass
                self.conn.commit()

                return inbox_id
        except Exception:
            return None

    def get_signal_inbox(self, inbox_id: int):
        """Fetch one signal_inbox row, parsing payload JSON."""
        try:
            iid = int(inbox_id)
        except Exception:
            return None
        try:
            with self.lock:
                cur = self.conn.cursor()
                cur.execute("SELECT * FROM signal_inbox WHERE id=?", (iid,))
                row = cur.fetchone()
                if not row:
                    return None
                d = dict(row)
                try:
                    d["payload"] = json.loads(d.get("payload") or "{}")
                except Exception:
                    d["payload"] = {}
                return d
        except Exception:
            return None

    def mark_signal_inbox_executed(self, inbox_id: int, note=None) -> bool:
        """Mark a signal_inbox row as executed."""
        try:
            iid = int(inbox_id)
        except Exception:
            return False
        try:
            now_ms = int(time.time() * 1000)
            with self.lock:
                cur = self.conn.cursor()
                if note is None:
                    cur.execute(
                        "UPDATE signal_inbox SET status='EXECUTED', executed_at_ms=? WHERE id=?",
                        (now_ms, iid),
                    )
                else:
                    cur.execute(
                        "UPDATE signal_inbox SET status='EXECUTED', executed_at_ms=?, note=? WHERE id=?",
                        (now_ms, str(note), iid),
                    )
                self.conn.commit()
            return True
        except Exception:
            return False

    def _row_to_trade(self, row):
        if not row:
            return None
        d = dict(row)
        d["realized_pnl"] = d.get("pnl", 0.0)
        try:
            d["take_profits"] = json.loads(d["take_profits"]) if d.get("take_profits") else []
        except Exception:
            d["take_profits"] = []
        try:
            d["explain"] = json.loads(d["explain"]) if d.get("explain") else {}
        except Exception:
            d["explain"] = {}

        # Sprint 8: protection verifier details (optional JSON)
        try:
            d["protection_details"] = json.loads(d["protection_details"]) if d.get("protection_details") else {}
        except Exception:
            # keep raw string if it isn't JSON
            d["protection_details"] = d.get("protection_details")
        return d

    
    # ==========================================================
    # CLOSED-LOOP LEARNING (ai_samples)
    # ==========================================================
    def add_ai_sample(self, sample=None, **kwargs):
        """Insert a feature snapshot for closed-loop learning.

        Accepts either:
          - add_ai_sample({dict})
          - add_ai_sample(symbol=..., features={...}, ...)
        """
        try:
            d = sample if isinstance(sample, dict) else kwargs
            sym = str(d.get("symbol") or "").upper().strip()
            if not sym:
                return None

            now = datetime.now(timezone.utc)
            now_ms = int(now.timestamp() * 1000)

            features = d.get("features") or {}
            explain = d.get("explain") or d.get("payload") or {}

            with self.lock:
                cur = self.conn.cursor()
                cur.execute(
                    """
                    INSERT INTO ai_samples
                        (created_at, created_at_ms, symbol, market_type, algo_mode, timeframe, strategy, side,
                         ai_vote, ai_confidence, rule_score, confidence,
                         features, explain, inbox_id, trade_id, status, entry_price)
                    VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?,
                         ?, ?, ?, ?,
                         ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        now_ms,
                        sym,
                        str(d.get("market_type") or ""),
                        str(d.get("algo_mode") or d.get("mode") or ""),
                        str(d.get("timeframe") or d.get("tf") or ""),
                        str(d.get("strategy") or ""),
                        str(d.get("side") or ""),
                        str(d.get("ai_vote") or ""),
                        float(d.get("ai_confidence") or 0.0),
                        float(d.get("rule_score") or d.get("score") or 0.0),
                        float(d.get("confidence") or 0.0),
                        json.dumps(features or {}, ensure_ascii=False),
                        json.dumps(explain or {}, ensure_ascii=False),
                        int(d.get("inbox_id")) if d.get("inbox_id") is not None else None,
                        int(d.get("trade_id")) if d.get("trade_id") is not None else None,
                        str(d.get("status") or "PENDING"),
                        float(d.get("entry_price")) if d.get("entry_price") is not None else None,
                    ),
                )
                self.conn.commit()
                return int(cur.lastrowid)
        except Exception:
            return None

    def link_ai_sample_to_trade(self, inbox_id: int, trade_id: int, entry_price: float = None):
        """Attach trade_id to the latest ai_sample created for this inbox_id."""
        try:
            iid = int(inbox_id)
            tid = int(trade_id)
        except Exception:
            return False
        try:
            with self.lock:
                cur = self.conn.cursor()
                cur.execute(
                    "SELECT id FROM ai_samples WHERE inbox_id=? ORDER BY id DESC LIMIT 1",
                    (iid,),
                )
                row = cur.fetchone()
                if not row:
                    return False
                sid = int(row[0])
                cur.execute(
                    """
                    UPDATE ai_samples
                       SET trade_id=?,
                           entry_price=COALESCE(entry_price, ?),
                           status='EXECUTED'
                     WHERE id=?
                    """,
                    (tid, float(entry_price) if entry_price is not None else None, sid),
                )
                self.conn.commit()
                return True
        except Exception:
            return False

    def close_ai_sample_for_trade(self, trade_id: int, close_price: float, pnl: float, close_reason: str = ""):
        """Mark the ai_sample linked to trade_id as CLOSED and store outcome."""
        try:
            tid = int(trade_id)
        except Exception:
            return False
        try:
            cp = float(close_price) if close_price is not None else 0.0
            pnlv = float(pnl) if pnl is not None else 0.0
            with self.lock:
                cur = self.conn.cursor()
                cur.execute("SELECT id, entry_price FROM ai_samples WHERE trade_id=? ORDER BY id DESC LIMIT 1", (tid,))
                row = cur.fetchone()
                if not row:
                    return False
                sid = int(row[0])
                ep = float(row[1] or 0.0)
                ret = None
                if ep and ep > 0:
                    ret = pnlv / ep
                cur.execute(
                    """
                    UPDATE ai_samples
                       SET status='CLOSED',
                           exit_price=?,
                           pnl=?,
                           return_pct=?,
                           close_reason=?,
                           closed_at_ms=?
                     WHERE id=?
                    """,
                    (cp, pnlv, ret, str(close_reason or ""), int(time.time() * 1000), sid),
                )
                self.conn.commit()
                return True
        except Exception:
            return False


# ==========================================================
    # COMMANDS
    # ==========================================================
    def add_command(self, cmd, params=None, source="system"):
        try:
            now = self._utc_iso()
            ms = self._ms_now()
            with self.lock:
                p_str = json.dumps(params) if params else "{}"
                try:
                    self.cursor.execute(
                        "INSERT INTO commands (cmd, params, status, created_at, created_at_ms, source) VALUES (?, ?, 'PENDING', ?, ?, ?)",
                        (cmd, p_str, now, ms, str(source))
                    )
                except Exception:
                    # Backward-compatible schema
                    self.cursor.execute(
                        "INSERT INTO commands (cmd, params, created_at) VALUES (?, ?, ?)",
                        (cmd, p_str, now)
                    )
                self.conn.commit()
        except Exception:
            pass



    def _commands_has_status(self) -> bool:
        try:
            with self.lock:
                row = self.cursor.execute("PRAGMA table_info(commands)").fetchall()
                cols = {r[1] for r in row} if row else set()
                return "status" in cols
        except Exception:
            return False

    def reset_stale_inprogress_commands(self, ttl_sec: int = 300) -> int:
        """Reset stuck IN_PROGRESS commands back to PENDING.

        Helps if bot crashes mid-command. Safe: only affects commands older than ttl_sec.
        Returns number of rows updated.
        """
        try:
            ttl = int(ttl_sec)
        except Exception:
            ttl = 300
        if ttl <= 0:
            ttl = 300

        try:
            with self.lock:
                if not self._commands_has_status():
                    return 0
                now_ms = self._ms_now()
                cutoff = now_ms - (ttl * 1000)
                cur = self.conn.cursor()
                cur.execute(
                    "UPDATE commands SET status='PENDING' WHERE status='IN_PROGRESS' AND claimed_at_ms IS NOT NULL AND claimed_at_ms < ?",
                    (cutoff,)
                )
                n = cur.rowcount or 0
                self.conn.commit()
                return int(n)
        except Exception:
            return 0

    def claim_pending_commands(self, limit: int = 50, worker: str = "worker", only_cmds=None):
        """Atomically claim pending commands (exactly-once best effort).

        If only_cmds is provided, ONLY those commands are claimed (ownership isolation).
        Returns a list of rows like: (id, cmd, params)
        """
        try:
            lim = int(limit)
        except Exception:
            lim = 50
        if lim <= 0:
            lim = 50

        # Normalize filter list
        cmd_list = []
        for c in (only_cmds or []):
            s = str(c or "").strip().upper()
            if s:
                cmd_list.append(s)

        try:
            with self.lock:
                if not self._commands_has_status():
                    pending = self.get_pending_commands()[:lim]
                    if not cmd_list:
                        return pending
                    out = []
                    for r in pending:
                        try:
                            if str(r[1] or "").strip().upper() in cmd_list:
                                out.append(r)
                        except Exception:
                            continue
                    return out[:lim]

                # Clean stale in-progress (best effort)
                try:
                    self.reset_stale_inprogress_commands(ttl_sec=300)
                except Exception:
                    pass

                now = self._utc_iso()
                ms = self._ms_now()

                # Use an IMMEDIATE transaction to reduce cross-process race conditions.
                try:
                    self.conn.execute("BEGIN IMMEDIATE")
                except Exception:
                    pass

                cur = self.conn.cursor()
                if cmd_list:
                    placeholders = ",".join(["?"] * len(cmd_list))
                    rows = cur.execute(
                        f"SELECT id, cmd, params FROM commands WHERE status='PENDING' AND UPPER(cmd) IN ({placeholders}) ORDER BY id ASC LIMIT ?",
                        tuple(cmd_list) + (lim,),
                    ).fetchall()
                else:
                    rows = cur.execute(
                        "SELECT id, cmd, params FROM commands WHERE status='PENDING' ORDER BY id ASC LIMIT ?",
                        (lim,)
                    ).fetchall()

                claimed = []
                for r in rows or []:
                    try:
                        cid = r[0]
                        upd = cur.execute(
                            "UPDATE commands SET status='IN_PROGRESS', claimed_by=?, claimed_at=?, claimed_at_ms=? WHERE id=? AND status='PENDING'",
                            (str(worker), now, int(ms), int(cid))
                        )
                        if (upd.rowcount or 0) == 1:
                            claimed.append((r[0], r[1], r[2]))
                    except Exception:
                        continue

                self.conn.commit()
                return claimed
        except Exception:
            return []

    def get_pending_commands(self):
        """Legacy-safe: return pending command rows.

        If commands table has a 'status' column, filter status='PENDING'.
        Otherwise, return all rows (best-effort backward compatibility).
        """
        try:
            with self.lock:
                if self._commands_has_status():
                    self.cursor.execute("SELECT id, cmd, params FROM commands WHERE status='PENDING' ORDER BY id ASC")
                else:
                    # Very old schema: no status column
                    self.cursor.execute("SELECT id, cmd, params FROM commands ORDER BY id ASC")
                return self.cursor.fetchall()
        except Exception:
            return []

    def mark_command_done(self, cmd_id, ok: bool = True, error: str = ""):
        """Mark a command terminal state (DONE/FAILED) with timestamps.

        Backward compatible: if commands table lacks status/done columns, best-effort update.
        """
        try:
            cid = int(cmd_id)
        except Exception:
            return

        try:
            now = self._utc_iso()
            ms = self._ms_now()
            status = 'DONE' if bool(ok) else 'FAILED'
            err = str(error or '')
            with self.lock:
                if self._commands_has_status():
                    try:
                        self.cursor.execute(
                            "UPDATE commands SET status=?, done_at=?, done_at_ms=?, last_error=? WHERE id=?",
                            (status, now, int(ms), err, int(cid))
                        )
                    except Exception:
                        # minimal fallback
                        self.cursor.execute("UPDATE commands SET status=? WHERE id=?", (status, int(cid)))
                else:
                    # legacy schema
                    try:
                        self.cursor.execute("UPDATE commands SET status=? WHERE id=?", (status, int(cid)))
                    except Exception:
                        pass
                self.conn.commit()
        except Exception:
            pass

    # ==========================================================
    # LOGS & STATS
    # ==========================================================

    def get_recent_learning(self, days: int = 7):
        """Return recent learning_stats rows aggregated for last N days.

        Output rows are dict-like objects with keys:
          strategy, exit_profile, trades, wins, losses, pnl, avg_confidence
        """
        try:
            d = int(days)
        except Exception:
            d = 7
        if d <= 0:
            d = 7

        # Dates stored as 'YYYY-MM-DD' in UTC
        cutoff = (datetime.utcnow().date() - timedelta(days=d)).isoformat()

        try:
            with self.lock:
                self.cursor.execute(
                    """
                    SELECT strategy,
                           exit_profile,
                           SUM(trades) as trades,
                           SUM(wins) as wins,
                           SUM(losses) as losses,
                           SUM(pnl) as pnl,
                           AVG(avg_confidence) as avg_confidence
                    FROM learning_stats
                    WHERE date >= ?
                    GROUP BY strategy, exit_profile
                    ORDER BY trades DESC
                    """,
                    (cutoff,),
                )
                rows = self.cursor.fetchall() or []
            # Normalize to list[dict]
            out = []
            for r in rows:
                try:
                    out.append({
                        "strategy": r["strategy"] if isinstance(r, sqlite3.Row) else r[0],
                        "exit_profile": r["exit_profile"] if isinstance(r, sqlite3.Row) else r[1],
                        "trades": int((r["trades"] if isinstance(r, sqlite3.Row) else r[2]) or 0),
                        "wins": int((r["wins"] if isinstance(r, sqlite3.Row) else r[3]) or 0),
                        "losses": int((r["losses"] if isinstance(r, sqlite3.Row) else r[4]) or 0),
                        "pnl": float((r["pnl"] if isinstance(r, sqlite3.Row) else r[5]) or 0.0),
                        "avg_confidence": float((r["avg_confidence"] if isinstance(r, sqlite3.Row) else r[6]) or 0.0),
                    })
                except Exception:
                    continue
            return out
        except Exception:
            return []

    def get_closed_trades(self):
        """Backward-compatible alias."""
        try:
            return self.get_all_closed_trades()
        except Exception:
            return []

    def store_threshold_adjustments(self, adjustments):
        """Store threshold adjustments dict {(strategy, profile): int} in DB."""
        try:
            if not adjustments:
                return
            for k, v in adjustments.items():
                try:
                    if isinstance(k, (list, tuple)) and len(k) == 2:
                        strategy, profile = k
                    else:
                        continue
                    self.set_threshold_adjustment(str(strategy), str(profile), int(v))
                except Exception:
                    continue
        except Exception:
            pass

    def log(self, msg, level="INFO", source="system"):
        try:
            now = self._utc_iso()
            ms = self._ms_now()
            with self.lock:
                try:
                    self.cursor.execute(
                        "INSERT INTO logs (level, msg, created_at, created_at_ms, source) VALUES (?, ?, ?, ?, ?)",
                        (level, str(msg), now, ms, str(source))
                    )
                except Exception:
                    # Backward-compatible schema
                    self.cursor.execute(
                        "INSERT INTO logs (level, msg, created_at) VALUES (?, ?, ?)",
                        (level, str(msg), now)
                    )
                self.conn.commit()
        except Exception:
            pass

    def get_logs(self, limit=50):
        try:
            with self.lock:
                self.cursor.execute("SELECT created_at as time, level, msg FROM logs ORDER BY id DESC LIMIT ?", (limit,))
                return [dict(r) for r in self.cursor.fetchall()]
        except Exception:
            return []

    def get_stats(self):
        try:
            with self.lock:
                self.cursor.execute("""
                    SELECT count(*),
                           sum(case when pnl > 0 then 1 else 0 end),
                           sum(pnl)
                    FROM trades
                    WHERE status='CLOSED'
                """)
                row = self.cursor.fetchone()
                total = row[0] or 0
                wins = row[1] or 0
                pnl = row[2] or 0.0
                win_rate = round((wins / total * 100), 1) if total > 0 else 0
                return {"trades": total, "win_rate": win_rate, "pnl": round(pnl, 2)}
        except Exception:
            return {"trades": 0, "win_rate": 0, "pnl": 0}

    def get_equity_curve(self, limit=200):
        try:
            with self.lock:
                self.cursor.execute("SELECT closed_at, pnl FROM trades WHERE status='CLOSED' ORDER BY closed_at ASC")
                rows = self.cursor.fetchall()
                equity = []
                current = 0.0
                for r in rows:
                    current += float(r["pnl"] or 0.0)
                    equity.append({"timestamp": r["closed_at"], "total_balance": round(current, 2)})
                return equity[-limit:]
        except Exception:
            return []

    # ==========================================================
    # DASHBOARD REPORTS
    # ==========================================================
    def get_detailed_report(self):
        try:
            with self.lock:
                self.cursor.execute("""
                    SELECT substr(closed_at, 1, 10) as day, 
                           SUM(pnl) as pnl, COUNT(*) as count 
                    FROM trades 
                    WHERE status='CLOSED' 
                    GROUP BY day ORDER BY day DESC LIMIT 14
                """)
                daily_stats = [dict(r) for r in self.cursor.fetchall()]

                self.cursor.execute("""
                    SELECT symbol, COUNT(*) as count,
                           SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                           SUM(pnl) as total_pnl
                    FROM trades WHERE status='CLOSED'
                    GROUP BY symbol ORDER BY total_pnl DESC
                """)
                coin_stats = [dict(r) for r in self.cursor.fetchall()]

                self.cursor.execute("""
                    SELECT
                        CASE WHEN UPPER(signal) LIKE '%LONG%' OR UPPER(signal) LIKE '%BUY%' THEN 'LONG' WHEN UPPER(signal) LIKE '%SHORT%' OR UPPER(signal) LIKE '%SELL%' THEN 'SHORT' ELSE 'UNKNOWN' END as direction,
                        SUM(pnl) as total_pnl
                    FROM trades WHERE status='CLOSED'
                    GROUP BY direction
                """)
                dir_stats = [dict(r) for r in self.cursor.fetchall()]

                self.cursor.execute("""
                    SELECT COUNT(*) as total_trades,
                           SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as total_wins,
                           SUM(pnl) as gross_pnl
                    FROM trades WHERE status='CLOSED'
                """)
                totals = dict(self.cursor.fetchone())

                return {
                    "daily": daily_stats[::-1],
                    "coins": coin_stats,
                    "directions": dir_stats,
                    "totals": totals
                }
        except Exception as e:
            print(f"Report Error: {e}")
            return {"daily": [], "coins": [], "directions": [], "totals": {}}

    # ==========================================================
    # REJECTIONS
    # ==========================================================
    def log_rejection(self, symbol, reason, value, threshold):
        try:
            with self.lock:
                self.cursor.execute("""
                    INSERT INTO rejections (symbol, reason, value, threshold, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, reason, value, threshold, self._utc_iso()))
                self.conn.commit()
        except Exception:
            pass

    def get_rejection_stats(self):
        """جلب إحصائيات أسباب الرفض"""
        try:
            with self.lock:
                self.cursor.execute("""
                    SELECT reason, COUNT(*) as count
                    FROM rejections
                    GROUP BY reason
                    ORDER BY count DESC
                """)
                return [dict(r) for r in self.cursor.fetchall()]
        except Exception:
            return []

    # ==========================================================
    # ANALYTICS FUNCTIONS (Dashboard)
    # ==========================================================
    def get_golden_hours(self):
        """Best trading hours (UTC) using closed_at_ms when available."""
        try:
            import datetime as _dt
            with self.lock:
                self.cursor.execute("""
                    SELECT closed_at_ms, closed_at, pnl
                    FROM trades
                    WHERE status='CLOSED'
                    ORDER BY closed_at DESC
                    LIMIT 5000
                """)
                rows = [dict(r) for r in self.cursor.fetchall()]

            buckets = {}
            for r in rows:
                hour = None

                ms = r.get("closed_at_ms")
                if ms:
                    try:
                        dt = _dt.datetime.utcfromtimestamp(int(ms) / 1000)
                        hour = dt.strftime("%H")
                    except Exception:
                        hour = None

                if hour is None and r.get("closed_at"):
                    try:
                        s = str(r["closed_at"]).replace("Z", "+00:00")
                        dt = _dt.datetime.fromisoformat(s)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=_dt.timezone.utc)
                        dt = dt.astimezone(_dt.timezone.utc)
                        hour = dt.strftime("%H")
                    except Exception:
                        hour = None

                if hour is None:
                    continue

                b = buckets.setdefault(hour, {"hour": hour, "total_pnl": 0.0, "trades": 0})
                b["total_pnl"] += float(r.get("pnl") or 0.0)
                b["trades"] += 1

            out = list(buckets.values())
            out.sort(key=lambda x: x["hour"])
            return out
        except Exception:
            return []

    def get_ai_calibration(self):
        """Normalize confidence to percent (some models output 0..1)."""
        try:
            with self.lock:
                self.cursor.execute(
                    "SELECT confidence, pnl FROM trades WHERE status='CLOSED' ORDER BY id DESC LIMIT 5000"
                )
                rows = [dict(r) for r in self.cursor.fetchall()]

            buckets = {
                "High (80-100%)": {"bucket": "High (80-100%)", "total_trades": 0, "wins": 0},
                "Mid (60-79%)": {"bucket": "Mid (60-79%)", "total_trades": 0, "wins": 0},
                "Low (0-59%)": {"bucket": "Low (0-59%)", "total_trades": 0, "wins": 0},
            }

            for r in rows:
                conf = float(r.get("confidence") or 0.0)
                if conf <= 1.0:
                    conf *= 100.0

                if conf >= 80:
                    b = buckets["High (80-100%)"]
                elif conf >= 60:
                    b = buckets["Mid (60-79%)"]
                else:
                    b = buckets["Low (0-59%)"]

                b["total_trades"] += 1
                if float(r.get("pnl") or 0.0) > 0:
                    b["wins"] += 1

            out = [v for v in buckets.values() if v["total_trades"] > 0]
            out.sort(key=lambda x: x["total_trades"], reverse=True)
            return out
        except Exception:
            return []

    # ==========================================================
    # PAPER TOURNAMENT (VIRTUAL SIGNALS)
    # ==========================================================
    def add_paper_signals(self, signals: list) -> int:
        if not signals:
            return 0
        inserted = 0
        with self.lock:
            for s in signals:
                try:
                    ms = int(s.get("signal_time_ms") or 0)
                    try:
                        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        dt = None

                    self.cursor.execute("""
                        INSERT OR IGNORE INTO analysis_results(
                            trade_id, symbol, signal_time, signal_time_ms,
                            market_type, interval, strategy_tag,
                            decision, entry_price,
                            final_score, pump_score, confidence,
                            tech_score, deriv_score, vol_spike,
                            ai_vote, ai_confidence, funding, oi_change
                        ) VALUES (
                            NULL, ?, ?, ?,
                            ?, ?, ?,
                            ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?, ?
                        )
                    """, (
                        s.get("symbol"),
                        dt,
                        ms,
                        s.get("market_type"),
                        s.get("interval"),
                        s.get("strategy_tag"),
                        s.get("decision"),
                        float(s.get("entry_price") or 0.0),
                        float(s.get("final_score") or 0.0),
                        int(s.get("pump_score") or 0),
                        float(s.get("confidence") or 0.0),
                        float(s.get("tech_score") or 0.0),
                        float(s.get("deriv_score") or 0.0),
                        int(s.get("vol_spike") or 0),
                        s.get("ai_vote"),
                        float(s.get("ai_confidence") or 0.0),
                        float(s.get("funding") or 0.0),
                        float(s.get("oi_change") or 0.0),
                    ))
                    if self.cursor.rowcount:
                        inserted += 1
                except Exception:
                    continue
            self.conn.commit()
        return inserted

    def get_pending_paper_evaluations(self, limit: int = 200):
        now_ms = self._ms_now()
        min_age = 15 * 60 * 1000
        with self.lock:
            self.cursor.execute("""
                SELECT *
                FROM analysis_results
                WHERE signal_time_ms IS NOT NULL
                  AND signal_time_ms > 0
                  AND signal_time_ms <= ?
                  AND (
                        price_after_15m IS NULL OR price_after_15m=0 OR
                        price_after_1h  IS NULL OR price_after_1h=0 OR
                        price_after_4h  IS NULL OR price_after_4h=0
                  )
                ORDER BY signal_time_ms ASC
                LIMIT ?
            """, (now_ms - min_age, int(limit)))
            return self.cursor.fetchall()

    def update_paper_evaluation(self, rec_id: int, updates: dict):
        if not updates:
            return
        cols, vals = [], []
        for k, v in updates.items():
            cols.append(f"{k}=?")
            vals.append(v)
        vals.append(int(rec_id))
        q = "UPDATE analysis_results SET " + ", ".join(cols) + " WHERE id=?"
        with self.lock:
            try:
                self.cursor.execute(q, tuple(vals))
                self.conn.commit()
            except Exception:
                pass

    def mark_paper_invalid(self, rec_id: int, note: str = "invalid"):
        with self.lock:
            try:
                self.cursor.execute("UPDATE analysis_results SET notes=? WHERE id=?", (note, int(rec_id)))
                self.conn.commit()
            except Exception:
                pass

    def get_paper_leaderboard(self, days: int = 7, horizon: str = "1h", market_type: str = "futures"):
        """Return aggregated stats per strategy_tag for the given horizon."""
        horizon = horizon if horizon in ("15m", "1h", "4h") else "1h"
        col_price = f"price_after_{horizon}"
        now = datetime.now(timezone.utc)
        start_ms = int((now - timedelta(days=int(days))).timestamp() * 1000)

        with self.lock:
            self.cursor.execute(f"""
                SELECT strategy_tag, decision, entry_price, {col_price} as px
                FROM analysis_results
                WHERE market_type=?
                  AND strategy_tag IS NOT NULL
                  AND signal_time_ms >= ?
                  AND entry_price IS NOT NULL AND entry_price > 0
                  AND {col_price} IS NOT NULL AND {col_price} > 0
            """, (market_type, start_ms))
            rows = self.cursor.fetchall()

        agg = {}
        for r in rows:
            tag = r["strategy_tag"] or "unknown"
            dec = (r["decision"] or "NEUTRAL").upper()
            if dec == "NEUTRAL":
                continue

            entry = float(r["entry_price"] or 0)
            px = float(r["px"] or 0)
            if entry <= 0 or px <= 0:
                continue

            ret = (px - entry) / entry
            if dec == "SHORT":
                ret = -ret

            a = agg.setdefault(tag, {"strategy_tag": tag, "trades": 0, "wins": 0, "avg_return": 0.0})
            a["trades"] += 1
            if ret > 0:
                a["wins"] += 1
            a["avg_return"] += (ret - a["avg_return"]) / a["trades"]

        out = []
        for tag, a in agg.items():
            trades = a["trades"]
            win_rate = (a["wins"] / trades * 100.0) if trades else 0.0
            out.append({
                "strategy_tag": tag,
                "trades": trades,
                "win_rate": round(win_rate, 1),
                "avg_return": round(a["avg_return"] * 100.0, 3)
            })
        out.sort(key=lambda x: (x["avg_return"], x["win_rate"], x["trades"]), reverse=True)
        return out

    # ==========================================================
    # RECOMMENDATIONS (APPROVE)
    # ==========================================================
    def upsert_recommendations(self, date_utc: str, recs: list):
        now_ms = self._ms_now()
        if recs is None:
            recs = []
        with self.lock:
            try:
                self.cursor.execute("DELETE FROM recommendations WHERE date_utc=? AND status='PENDING'", (date_utc,))
            except Exception:
                pass

            for r in recs:
                try:
                    self.cursor.execute("""
                        INSERT INTO recommendations(
                            date_utc, created_at_ms, key, current_value, suggested_value,
                            category, reason, score, status
                        ) VALUES (?,?,?,?,?,?,?,?, 'PENDING')
                    """, (
                        date_utc, now_ms,
                        r.get("key"),
                        str(r.get("current_value")),
                        str(r.get("suggested_value")),
                        r.get("category", "tuning"),
                        r.get("reason", ""),
                        float(r.get("score") or 0.0),
                    ))
                except Exception:
                    continue

            self.conn.commit()

    def list_recommendations(self, date_utc: str):
        with self.lock:
            try:
                self.cursor.execute(
                    "SELECT * FROM recommendations WHERE date_utc=? ORDER BY score DESC, id DESC",
                    (date_utc,)
                )
                return self.cursor.fetchall()
            except Exception:
                return []

    def approve_recommendation(self, rec_id: int):
        now_ms = self._ms_now()
        with self.lock:
            try:
                self.cursor.execute("SELECT * FROM recommendations WHERE id=?", (int(rec_id),))
                rec = self.cursor.fetchone()
                if not rec:
                    return False, "not found"
                if rec["status"] == "APPROVED":
                    return True, "already approved"

                key = rec["key"]
                val = rec["suggested_value"]

                if key:
                    self.set_setting(key, val)

                self.cursor.execute(
                    "UPDATE recommendations SET status='APPROVED', approved_at_ms=? WHERE id=?",
                    (now_ms, int(rec_id))
                )
                self.conn.commit()
                return True, "approved"
            except Exception as e:
                return False, str(e)

    def reject_recommendation(self, rec_id: int):
        with self.lock:
            try:
                self.cursor.execute("UPDATE recommendations SET status='REJECTED' WHERE id=?", (int(rec_id),))
                self.conn.commit()
                return True
            except Exception:
                return False


    # ==========================================================
    # SNAPSHOTS / EQUITY HISTORY (UTC)
    # ==========================================================
    def add_equity_snapshot(self, total_balance: float, unrealized_pnl: float = 0.0, source: str = "system", meta: dict = None):
        """Append an equity snapshot (UTC).

        This powers the project's Snapshot feature (auditability + time-series analytics).

        Columns are added non-destructively via _ensure_schema(), so this works on older DBs too.
        """
        try:
            now = self._utc_iso()
            ms = self._ms_now()
            date_utc = now[:10]
            meta_str = json.dumps(meta or {}, ensure_ascii=False)
            with self.lock:
                try:
                    self.cursor.execute(
                        """INSERT INTO equity_history
                           (timestamp, timestamp_ms, date_utc, total_balance, unrealized_pnl, source, meta)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (now, ms, date_utc, float(total_balance), float(unrealized_pnl), str(source), meta_str)
                    )
                except Exception:
                    # Backward-compatible schema
                    self.cursor.execute(
                        "INSERT INTO equity_history (timestamp, total_balance, unrealized_pnl) VALUES (?, ?, ?)",
                        (now, float(total_balance), float(unrealized_pnl))
                    )
                self.conn.commit()
                return True
        except Exception:
            return False

    def get_equity_history(self, limit: int = 500, since_ms: int = None):
        """Return equity snapshots ordered ASC by time."""
        try:
            with self.lock:
                # Prefer timestamp_ms if present
                cols = self._table_columns("equity_history")
                if "timestamp_ms" in cols:
                    if since_ms is not None:
                        self.cursor.execute(
                            """SELECT timestamp, timestamp_ms, total_balance, unrealized_pnl, source, meta
                               FROM equity_history
                               WHERE timestamp_ms >= ?
                               ORDER BY timestamp_ms ASC
                               LIMIT ?""",
                            (int(since_ms), int(limit))
                        )
                    else:
                        self.cursor.execute(
                            """SELECT timestamp, timestamp_ms, total_balance, unrealized_pnl, source, meta
                               FROM equity_history
                               ORDER BY timestamp_ms DESC
                               LIMIT ?""",
                            (int(limit),)
                        )
                        rows = list(self.cursor.fetchall())
                        rows.reverse()
                        return [dict(r) for r in rows]

                    return [dict(r) for r in self.cursor.fetchall()]
                else:
                    # Legacy
                    if since_ms is not None:
                        return []
                    self.cursor.execute(
                        """SELECT timestamp, total_balance, unrealized_pnl
                           FROM equity_history
                           ORDER BY id DESC
                           LIMIT ?""",
                        (int(limit),)
                    )
                    rows = list(self.cursor.fetchall())
                    rows.reverse()
                    return [dict(r) for r in rows]
        except Exception:
            return []

    def prune_equity_history(self, retention_days: int = 30) -> int:
        """Delete old equity snapshots; returns number of deleted rows."""
        try:
            retention_days = int(retention_days)
            if retention_days <= 0:
                return 0
            cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp() * 1000)
            with self.lock:
                cols = self._table_columns("equity_history")
                if "timestamp_ms" in cols:
                    self.cursor.execute("DELETE FROM equity_history WHERE timestamp_ms < ?", (cutoff_ms,))
                    deleted = self.cursor.rowcount or 0
                else:
                    deleted = 0
                self.conn.commit()
                return deleted
        except Exception:
            return 0


    # ==========================================================
    # PUMP HUNTER
    # ==========================================================
    def update_pump_hunter_state(self, heartbeat_ms: Optional[int] = None, scan_ms: Optional[int] = None,
                                 scan_count: Optional[int] = None, last_error: Optional[str] = None) -> None:
        """Update Pump Hunter heartbeat/scan status (singleton row id=1)."""
        try:
            with self.lock:
                cols = self._table_columns("pump_hunter_state")
                if not cols:
                    return
                hb = int(heartbeat_ms) if heartbeat_ms is not None else None
                sm = int(scan_ms) if scan_ms is not None else None
                sc = int(scan_count) if scan_count is not None else None
                le = str(last_error) if last_error is not None else None

                # Build dynamic update
                fields = []
                params = []
                if hb is not None and "last_heartbeat_ms" in cols:
                    fields.append("last_heartbeat_ms = ?")
                    params.append(hb)
                if sm is not None and "last_scan_ms" in cols:
                    fields.append("last_scan_ms = ?")
                    params.append(sm)
                if sc is not None and "last_scan_count" in cols:
                    fields.append("last_scan_count = ?")
                    params.append(sc)
                if le is not None and "last_error" in cols:
                    fields.append("last_error = ?")
                    params.append(le)

                if not fields:
                    return
                params.append(1)
                self.cursor.execute(f"UPDATE pump_hunter_state SET {', '.join(fields)} WHERE id = ?", tuple(params))
                self.conn.commit()
        except Exception:
            return

    def get_pump_hunter_state(self) -> dict:
        try:
            with self.lock:
                self.cursor.execute("SELECT * FROM pump_hunter_state WHERE id = 1")
                row = self.cursor.fetchone()
                return dict(row) if row else {"id": 1, "last_heartbeat_ms": 0, "last_scan_ms": 0, "last_scan_count": 0, "last_error": ""}
        except Exception:
            return {"id": 1, "last_heartbeat_ms": 0, "last_scan_ms": 0, "last_scan_count": 0, "last_error": ""}

    def upsert_pump_hunter_oi(self, symbol: str, oi: float, ts_ms: int) -> Tuple[Optional[float], Optional[float]]:
        """Store last OI for symbol; returns (prev_oi, oi_change_pct)."""
        try:
            symbol = str(symbol).upper().strip()
            oi = float(oi)
            ts_ms = int(ts_ms)
            prev = None
            with self.lock:
                self.cursor.execute("SELECT last_oi FROM pump_hunter_oi WHERE symbol = ?", (symbol,))
                r = self.cursor.fetchone()
                if r:
                    prev = float(r["last_oi"] or 0.0)
                self.cursor.execute(
                    """INSERT INTO pump_hunter_oi(symbol, last_oi, last_ms) VALUES(?, ?, ?)
                           ON CONFLICT(symbol) DO UPDATE SET last_oi=excluded.last_oi, last_ms=excluded.last_ms""",
                    (symbol, oi, ts_ms)
                )
                self.conn.commit()
            if prev and prev > 0:
                return prev, (oi - prev) / prev
            return prev, None
        except Exception:
            return None, None

    def add_pump_candidate(self, payload: dict, status: str = "PENDING") -> int:
        """Insert a pump candidate row and return its id."""
        try:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            symbol = str(payload.get("symbol") or "").upper().strip()
            market_type = str(payload.get("market_type") or payload.get("market") or "futures").lower().strip()
            interval = str(payload.get("interval") or payload.get("timeframe") or payload.get("tf") or "5m")
            price = float(payload.get("entry_price") or payload.get("price") or 0.0)
            pump_score = int(float(payload.get("pump_score") or 0))
            ai_vote = str(payload.get("ai_vote") or payload.get("side") or payload.get("signal") or "").upper()
            ai_conf = float(payload.get("ai_confidence") or payload.get("confidence") or 0.0)

            st = str(status or "PENDING").upper()
            if st not in ("PENDING", "APPROVED", "REJECTED", "EXECUTED"):
                st = "PENDING"

            # Simple dedup: don't insert same symbol/market/interval within 5 minutes if still pending
            dedup_ms = int(now_ms - (5 * 60 * 1000))
            with self.lock:
                try:
                    self.cursor.execute(
                        """SELECT id FROM pump_candidates
                             WHERE symbol=? AND market_type=? AND interval=? AND status='PENDING' AND created_at_ms >= ?
                             ORDER BY id DESC LIMIT 1""",
                        (symbol, market_type, interval, dedup_ms)
                    )
                    ex = self.cursor.fetchone()
                    if ex:
                        return int(ex["id"])
                except Exception:
                    pass

                self.cursor.execute(
                    """INSERT INTO pump_candidates
                       (created_at, created_at_ms, symbol, market_type, interval, price, pump_score,
                        ai_vote, ai_confidence, funding, oi_change, status, note, payload_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        created_at, now_ms, symbol, market_type, interval, price, pump_score,
                        ai_vote, ai_conf,
                        float(payload.get("funding") or 0.0) if payload.get("funding") is not None else None,
                        float(payload.get("oi_change") or 0.0) if payload.get("oi_change") is not None else None,
                        st, str(payload.get("note") or ""), json.dumps(payload, ensure_ascii=False)
                    )
                )
                cid = int(self.cursor.lastrowid or 0)
                self.conn.commit()
                return cid
        except Exception:
            return 0

    def list_pump_candidates(self, limit: int = 50, status: Optional[str] = 'PENDING') -> List[Dict[str, Any]]:
        try:
            limit = max(1, min(int(limit), 500))
            st = str(status).upper() if status else None
            with self.lock:
                if st and st in ("PENDING", "APPROVED", "REJECTED", "EXECUTED"):
                    self.cursor.execute(
                        """SELECT * FROM pump_candidates WHERE status=? ORDER BY created_at_ms DESC LIMIT ?""",
                        (st, limit)
                    )
                else:
                    self.cursor.execute(
                        """SELECT * FROM pump_candidates ORDER BY created_at_ms DESC LIMIT ?""",
                        (limit,)
                    )
                rows = self.cursor.fetchall() or []
            out = []
            for r in rows:
                d = dict(r)
                try:
                    d["payload"] = json.loads(d.get("payload_json") or "{}")
                except Exception:
                    d["payload"] = {}
                out.append(d)
            return out
        except Exception:
            return []

    def get_pump_candidate(self, cid: int) -> Optional[Dict[str, Any]]:
        try:
            cid = int(cid)
            with self.lock:
                self.cursor.execute("SELECT * FROM pump_candidates WHERE id=?", (cid,))
                r = self.cursor.fetchone()
            if not r:
                return None
            d = dict(r)
            try:
                d["payload"] = json.loads(d.get("payload_json") or "{}")
            except Exception:
                d["payload"] = {}
            return d
        except Exception:
            return None

    def set_pump_candidate_status(self, cid: int, status: str, note: str = "") -> bool:
        try:
            cid = int(cid)
            st = str(status or "").upper()
            if st not in ("PENDING", "APPROVED", "REJECTED", "EXECUTED"):
                st = "PENDING"
            with self.lock:
                self.cursor.execute("UPDATE pump_candidates SET status=?, note=? WHERE id=?", (st, str(note or ""), cid))
                self.conn.commit()
            return True
        except Exception:
            return False

    def add_pump_label(self, symbol: str, timestamp_ms: int, label: str = "PUMP", note: str = "") -> int:
        """Store a human label for training (used by optional trainers)."""
        try:
            symbol = str(symbol).upper().strip()
            timestamp_ms = int(timestamp_ms)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            with self.lock:
                self.cursor.execute(
                    """INSERT INTO pump_labels(created_at_ms, symbol, timestamp_ms, label, note)
                         VALUES (?, ?, ?, ?, ?)""",
                    (now_ms, symbol, timestamp_ms, str(label or "PUMP"), str(note or ""))
                )
                rid = int(self.cursor.lastrowid or 0)
                self.conn.commit()
                return rid
        except Exception:
            return 0


# === SAFE PATCH EXTENSIONS (KEEP ORIGINAL CODE INTACT ABOVE) ===
# الهدف: تحسينات آمنة بدون حذف/كسر أي كود موجود.
# - اكتشاف تلف قاعدة SQLite (malformed schema) وعزل الملف تلقائياً ثم إنشاء DB جديدة
# - إنشاء Indexes إضافية لـ signal_inbox لتحسين أداء الداشبورد
# - Normalization بسيط لـ side عند تخزين signal_inbox (فقط لو فارغ/غريب)
# - Helpers اختيارية (لا تؤثر على السلوك الحالي إن لم تُستخدم)

def _safe__is_corruption_error(exc: Exception) -> bool:
    try:
        msg = str(exc).lower()
    except Exception:
        return False
    needles = [
        "malformed database schema",
        "database disk image is malformed",
        "file is not a database",
        "not a database",
        "database schema is locked",
    ]
    return any(n in msg for n in needles)

def _safe__now_utc_stamp() -> str:
    try:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    except Exception:
        return str(int(time.time()))

def _safe__quarantine_db(self, where: str, exc=None) -> bool:
    """
    Move corrupted DB to *.corrupt_TIMESTAMP and reopen a fresh DB.
    This runs ONLY when corruption is detected.
    """
    try:
        db_path = getattr(self, "db_file", None)
        if not db_path:
            return False

        # Close existing connection
        try:
            if getattr(self, "conn", None):
                self.conn.close()
        except Exception:
            pass

        ts = _safe__now_utc_stamp()
        corrupt_path = f"{db_path}.corrupt_{ts}"

        # Move main db + WAL/SHM if exist
        try:
            if os.path.exists(db_path):
                os.replace(db_path, corrupt_path)
        except Exception:
            pass
        for suf in ("-wal", "-shm"):
            try:
                p = db_path + suf
                if os.path.exists(p):
                    os.replace(p, corrupt_path + suf)
            except Exception:
                pass

        # Reopen fresh DB at original path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
            self.conn.execute("PRAGMA busy_timeout=5000;")
        except Exception:
            pass
        self.cursor = self.conn.cursor()

        # Recreate schema (use original implementations to avoid recursion)
        try:
            if hasattr(DatabaseManager, "_orig_create_tables"):
                DatabaseManager._orig_create_tables(self)
            else:
                DatabaseManager.create_tables(self)
        except Exception:
            # even if schema creation fails, do not crash app
            pass

        try:
            self._seed_defaults()
        except Exception:
            pass
        try:
            self._ensure_schema()
        except Exception:
            pass

        try:
            print(f"[DB] ⚠️ Corrupt DB detected at {where}; moved to: {corrupt_path} (new DB created at {db_path})")
            if exc is not None:
                print(f"[DB] Corruption reason: {exc}")
        except Exception:
            pass
        return True
    except Exception:
        return False

# Apply monkey-patches once (do not override user's code; just wrap it)
try:
    if not getattr(DatabaseManager, "_SAFE_PATCH_V1", False):
        DatabaseManager._SAFE_PATCH_V1 = True

        # Keep originals
        if not hasattr(DatabaseManager, "_orig_create_tables"):
            DatabaseManager._orig_create_tables = DatabaseManager.create_tables
        if not hasattr(DatabaseManager, "_orig_add_signal_inbox"):
            if hasattr(DatabaseManager, "add_signal_inbox"):
                DatabaseManager._orig_add_signal_inbox = DatabaseManager.add_signal_inbox

        # ---- Wrap create_tables: quick_check before schema creation + add indexes after ----
        def _safe_create_tables(self, *args, **kwargs):
            # quick_check (fast) to detect malformed schema early
            try:
                cur = self.conn.cursor()
                cur.execute("PRAGMA quick_check;")
                row = cur.fetchone()
                if row and str(row[0]).lower() != "ok":
                    raise sqlite3.DatabaseError(f"quick_check: {row[0]}")
            except Exception as e:
                if _safe__is_corruption_error(e) or "quick_check" in str(e).lower():
                    _safe__quarantine_db(self, "quick_check", e)

            # run original
            try:
                out = DatabaseManager._orig_create_tables(self, *args, **kwargs)
            except Exception as e:
                if _safe__is_corruption_error(e):
                    _safe__quarantine_db(self, "create_tables", e)
                    out = DatabaseManager._orig_create_tables(self, *args, **kwargs)
                else:
                    raise

            # ensure indexes for signal_inbox (safe even if table doesn't exist yet)
            try:
                with self.lock:
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_status ON signal_inbox(status)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_received ON signal_inbox(received_at_ms)")
                    self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_inbox_symbol ON signal_inbox(symbol)")
                    self.conn.commit()
            except Exception:
                pass

            return out

        DatabaseManager.create_tables = _safe_create_tables

        # ---- Wrap add_signal_inbox: normalize side فقط لو فاضي/غريب ----
        if hasattr(DatabaseManager, "_orig_add_signal_inbox"):
            def _safe_add_signal_inbox(self, *args, **kwargs):
                try:
                    side = kwargs.get("side", "")
                    payload = kwargs.get("payload", None)

                    side_s = str(side or "").strip().upper()
                    # best-effort: if side missing, try payload
                    if not side_s and isinstance(payload, dict):
                        for key in ("side", "decision", "direction", "signal", "action", "order_side"):
                            v = payload.get(key)
                            if v:
                                side_s = str(v).strip().upper()
                                break

                    # keep UI semantics: store LONG/SHORT in DB (leave WAIT/empty as-is)
                    if side_s in ("LONG", "BUY"):
                        kwargs["side"] = "LONG"
                    elif side_s in ("SHORT", "SELL"):
                        kwargs["side"] = "SHORT"
                    else:
                        # keep original value
                        kwargs["side"] = kwargs.get("side", "")

                except Exception:
                    pass

                return DatabaseManager._orig_add_signal_inbox(self, *args, **kwargs)

            DatabaseManager.add_signal_inbox = _safe_add_signal_inbox

        # ---- Optional helper: get pending inbox signals ----
        if not hasattr(DatabaseManager, "get_pending_signal_inbox"):
            def get_pending_signal_inbox(self, limit: int = 200):
                try:
                    lim = int(limit) if limit else 200
                    lim = max(1, min(lim, 2000))
                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute(
                            "SELECT * FROM signal_inbox WHERE status=? ORDER BY received_at_ms DESC LIMIT ?",
                            ("PENDING_APPROVAL", lim),
                        )
                        rows = cur.fetchall() or []
                    out = []
                    for r in rows:
                        rr = dict(r)
                        try:
                            rr["payload"] = json.loads(rr.get("payload") or "{}")
                        except Exception:
                            rr["payload"] = {}
                        out.append(rr)
                    return out
                except Exception:
                    return []

            DatabaseManager.get_pending_signal_inbox = get_pending_signal_inbox


        # ---- Optional helper: decision traces (Strategy Decision Trace) ----
        if not hasattr(DatabaseManager, "add_decision_trace"):
            def add_decision_trace(self, trace: dict) -> bool:
                """Insert a decision trace row (best-effort)."""
                try:
                    if not isinstance(trace, dict):
                        return False
                    now = self._utc_iso()
                    ms = self._ms_now()
                    symbol = str(trace.get("symbol") or "").upper().strip()
                    interval = str(trace.get("interval") or "")
                    market_type = str(trace.get("market_type") or "")
                    strategy = str(trace.get("strategy") or "")
                    gate = str(trace.get("gate") or "")
                    exit_profile = str(trace.get("exit_profile") or "")
                    decision = str(trace.get("decision") or "")

                    def _f(x):
                        try:
                            return float(x)
                        except Exception:
                            return None

                    row = (
                        now,
                        ms,
                        symbol,
                        interval,
                        market_type,
                        strategy,
                        gate,
                        exit_profile,
                        decision,
                        _f(trace.get("conf_pct")),
                        _f(trace.get("min_conf")),
                        _f(trace.get("final_score")),
                        str(trace.get("rule_signal") or ""),
                        str(trace.get("ai_vote") or ""),
                        _f(trace.get("ai_confidence")),
                        _f(trace.get("pump_score")),
                        _f(trace.get("rsi")),
                        _f(trace.get("adx")),
                        int(trace.get("vol_spike") or 0) if trace.get("vol_spike") is not None else None,
                        _f(trace.get("funding")),
                        _f(trace.get("oi_change")),
                        json.dumps(trace.get("trace") or trace, ensure_ascii=False),
                    )

                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute(
                            """INSERT INTO decision_traces (
                                   created_at, created_at_ms, symbol, interval, market_type,
                                   strategy, gate, exit_profile, decision,
                                   conf_pct, min_conf, final_score,
                                   rule_signal, ai_vote, ai_confidence,
                                   pump_score, rsi, adx, vol_spike, funding, oi_change,
                                   trace
                               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            row,
                        )
                        self.conn.commit()
                    return True
                except Exception:
                    return False

            DatabaseManager.add_decision_trace = add_decision_trace

        if not hasattr(DatabaseManager, "get_recent_decision_traces"):
            def get_recent_decision_traces(self, limit: int = 50):
                try:
                    lim = int(limit) if limit else 50
                    lim = max(1, min(lim, 500))
                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute(
                            "SELECT * FROM decision_traces ORDER BY created_at_ms DESC LIMIT ?",
                            (lim,),
                        )
                        rows = cur.fetchall() or []
                    out = []
                    for r in rows:
                        d = dict(r)
                        try:
                            d["trace"] = json.loads(d.get("trace") or "{}")
                        except Exception:
                            pass
                        out.append(d)
                    return out
                except Exception:
                    return []

            DatabaseManager.get_recent_decision_traces = get_recent_decision_traces

        if not hasattr(DatabaseManager, "prune_decision_traces"):
            def prune_decision_traces(self, max_rows: int = 20000):
                """Keep DB from growing without bounds."""
                try:
                    mr = int(max_rows) if max_rows else 20000
                    mr = max(1000, min(mr, 200000))
                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute("SELECT COUNT(1) AS c FROM decision_traces")
                        c = int(cur.fetchone()[0] or 0)
                        if c <= mr:
                            return 0
                        # delete oldest rows
                        to_del = c - mr
                        cur.execute(
                            "DELETE FROM decision_traces WHERE id IN (SELECT id FROM decision_traces ORDER BY created_at_ms ASC LIMIT ?)",
                            (to_del,),
                        )
                        self.conn.commit()
                    return to_del
                except Exception:
                    return 0

            DatabaseManager.prune_decision_traces = prune_decision_traces

        # ---- Optional helper: perf metrics (Latency per symbol) ----
        if not hasattr(DatabaseManager, "add_perf_metric"):
            def add_perf_metric(self, event: dict) -> bool:
                """Insert a perf metric row (best-effort)."""
                try:
                    if not isinstance(event, dict):
                        return False
                    now = self._utc_iso()
                    ms = self._ms_now()
                    symbol = str(event.get("symbol") or "").upper().strip()
                    interval = str(event.get("interval") or "")
                    market_type = str(event.get("market_type") or "")
                    stage = str(event.get("stage") or "")

                    def _i(x):
                        try:
                            return int(float(x))
                        except Exception:
                            return None

                    duration_ms = _i(event.get("duration_ms"))
                    okv = event.get("ok")
                    ok_int = 1 if (okv is True or str(okv).strip() in {"1","true","True","YES","yes"}) else 0
                    meta = event.get("meta")
                    if isinstance(meta, (dict, list)):
                        meta = json.dumps(meta, ensure_ascii=False)
                    meta = str(meta) if meta is not None else ""

                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute(
                            """INSERT INTO perf_metrics (
                                   created_at, created_at_ms, symbol, interval, market_type,
                                   stage, duration_ms, ok, meta
                               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (now, ms, symbol, interval, market_type, stage, duration_ms, ok_int, meta),
                        )
                        self.conn.commit()
                    return True
                except Exception:
                    return False

            DatabaseManager.add_perf_metric = add_perf_metric

        if not hasattr(DatabaseManager, "get_recent_perf_metrics"):
            def get_recent_perf_metrics(self, window_sec: int = 600, limit: int = 2000):
                """Fetch recent perf metrics (best-effort)."""
                try:
                    win = int(window_sec) if window_sec else 600
                    win = max(10, min(win, 24 * 3600))
                    lim = int(limit) if limit else 2000
                    lim = max(10, min(lim, 10000))
                    cutoff_ms = self._ms_now() - (win * 1000)
                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute(
                            "SELECT * FROM perf_metrics WHERE created_at_ms >= ? ORDER BY created_at_ms DESC LIMIT ?",
                            (cutoff_ms, lim),
                        )
                        rows = cur.fetchall() or []
                    out = []
                    for r in rows:
                        d = dict(r)
                        # best-effort parse meta JSON
                        try:
                            mv = d.get("meta")
                            if mv:
                                d["meta"] = json.loads(mv)
                        except Exception:
                            pass
                        out.append(d)
                    return out
                except Exception:
                    return []

            DatabaseManager.get_recent_perf_metrics = get_recent_perf_metrics

        if not hasattr(DatabaseManager, "prune_perf_metrics"):
            def prune_perf_metrics(self, max_rows: int = 200000):
                """Keep perf_metrics from growing without bounds."""
                try:
                    mr = int(max_rows) if max_rows else 200000
                    mr = max(10000, min(mr, 2_000_000))
                    with self.lock:
                        cur = self.conn.cursor()
                        cur.execute("SELECT COUNT(1) AS c FROM perf_metrics")
                        c = int(cur.fetchone()[0] or 0)
                        if c <= mr:
                            return 0
                        to_del = c - mr
                        cur.execute(
                            "DELETE FROM perf_metrics WHERE id IN (SELECT id FROM perf_metrics ORDER BY created_at_ms ASC LIMIT ?)",
                            (to_del,),
                        )
                        self.conn.commit()
                    return to_del
                except Exception:
                    return 0

            DatabaseManager.prune_perf_metrics = prune_perf_metrics

except Exception:
    # Never break import بسبب patch section
    pass
