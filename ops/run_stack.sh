#!/usr/bin/env bash
set -euo pipefail

# Run: Dashboard -> Monitor -> Bot

source "$(dirname "$0")/_lib.sh"

DASH_LOG="$LOGS_DIR/dashboard.log"
MON_LOG="$LOGS_DIR/monitor.log"
BOT_LOG="$LOGS_DIR/bot.log"

# Dashboard
start_proc "dashboard" "$DASH_LOG" uvicorn dashboard.app:app --host 0.0.0.0 --port 8000

# Monitor
start_proc "monitor" "$MON_LOG" python execution_monitor.py

# Bot
start_proc "bot" "$BOT_LOG" python main.py

echo ""
echo "[RUNBOOK] Stack is up."
echo "[RUNBOOK] - Status: bash ops/status_stack.sh"
echo "[RUNBOOK] - Stop:   bash ops/stop_stack.sh"
echo "[RUNBOOK] - Logs:   tail -f logs/runbook/*.log"
