#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SOURCE_ENV="${REPO_ROOT}/config/runtime.a26022.env"
TARGET_ENV="/etc/doraemon/runtime.env"

if [[ ! -f "${SOURCE_ENV}" ]]; then
  echo "[ERROR] missing ${SOURCE_ENV}" >&2
  exit 1
fi

sudo install -m 0644 "${SOURCE_ENV}" "${TARGET_ENV}"
sudo systemctl daemon-reload
sudo systemctl disable --now agv_speed_odom_bridge.service >/dev/null 2>&1 || true
sudo systemctl enable doraemon-runtime.service >/dev/null

echo "[OK] installed ${TARGET_ENV}"
echo "[OK] disabled legacy agv_speed_odom_bridge.service"
echo "[OK] enabled doraemon-runtime.service"
echo "[INFO] daily tuning now reads ${SOURCE_ENV} directly at runtime startup"
echo "[INFO] rerun this installer only for first deployment or to refresh the /etc fallback"
echo "[INFO] restart runtime with:"
echo "  sudo systemctl restart doraemon-runtime.service"
