#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/_lib.sh"

bash "$ROOT_DIR/ops/stop_stack.sh" || true
sleep 1
bash "$ROOT_DIR/ops/run_stack.sh"
