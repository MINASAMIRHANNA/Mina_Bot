#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/_lib.sh"

TS="$(date +%Y%m%d_%H%M%S)"
OUT="$LOGS_DIR/diag_${TS}.txt"

echo "[RUNBOOK] Writing diag snapshot to: $OUT"
python "$ROOT_DIR/tools/runtime_diag.py" > "$OUT" 2>&1 || true

echo "[RUNBOOK] Done."
