#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/_lib.sh"

status_proc "dashboard"
status_proc "monitor"
status_proc "bot"
