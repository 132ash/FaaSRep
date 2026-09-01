#!/usr/bin/env bash
# Start the two Boki-style-SN control-plane services on the storage/SUT host.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HOST=${1:?usage: scripts/start_boki_sn.sh <bind-host>}
export SYSTEM_MODE=BOKI_SN
cd "$ROOT_DIR"

python3 "$ROOT_DIR/src/commit_manager/proxy.py" "$HOST" 9000 >"$ROOT_DIR/logging/boki_lock_manager.log" 2>&1 &
python3 "$ROOT_DIR/src/shadow_service/proxy.py" "$HOST" 9100 >"$ROOT_DIR/logging/boki_shadow_service.log" 2>&1 &
echo "Boki-style-SN lock manager: $HOST:9000; shadow service: $HOST:9100"
