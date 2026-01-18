#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/_lib.sh"

# Stop in reverse order
stop_proc "bot"
stop_proc "monitor"
stop_proc "dashboard"

echo "[RUNBOOK] Stack stopped."
