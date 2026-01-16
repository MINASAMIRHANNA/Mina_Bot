#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "[LOOP] Execution monitor loop started. (Ctrl+C to stop)"
while true; do
  echo "[LOOP] Starting execution_monitor.py..."
  python execution_monitor.py || true
  echo "[LOOP] Monitor stopped. Restarting in 2 seconds..."
  sleep 2
done
