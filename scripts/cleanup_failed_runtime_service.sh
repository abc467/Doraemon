#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${SERVICE_RESULT:-unknown}" == "success" ]]; then
  exit 0
fi

echo "[WARN] systemd service result=${SERVICE_RESULT:-unknown}; checking for failed-start remnants"
STOP_MASTER=1 "${SCRIPT_DIR}/stop_all_backend.sh" || true
