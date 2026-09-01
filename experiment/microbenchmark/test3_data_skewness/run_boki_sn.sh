#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Requested baseline.  ROUNDS may be reduced for a smoke run without changing
# the closed-loop client count or Zipf setting.
CLIENTS=32
ZIPF=0.9
ROUNDS="${ROUNDS:-100}"
SEED="${SEED:-20260901}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-300}"

cd "$SCRIPT_DIR"
SYSTEM_MODE=BOKI_SN python3 run.py \
  --workflow c4 --clients "$CLIENTS" --zipf "$ZIPF" --rounds "$ROUNDS" \
  --seed "$SEED" --request-timeout "$REQUEST_TIMEOUT"
