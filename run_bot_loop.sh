#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "[LOOP] Bot loop started. (Ctrl+C to stop)"
while true; do
  echo "[LOOP] Starting bot (main.py)..."
  python main.py || true
  echo "[LOOP] Bot stopped. Restarting in 2 seconds..."
  sleep 2
done
