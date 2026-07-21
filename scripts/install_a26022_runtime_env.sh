#!/usr/bin/env bash

set -euo pipefail

echo "[ERROR] install_a26022_runtime_env.sh is retired for commercial deployments" >&2
echo "[ERROR] it must not replace /etc/doraemon/runtime.env or enable/start the robot service" >&2
echo "[INFO] install only from the frozen release with:" >&2
echo "[INFO]   DORAEMON_ENABLE_SERVICE=0 ./scripts/install_doraemon_runtime_service.sh" >&2
echo "[INFO] edit the per-vehicle configuration with: sudoedit /etc/doraemon/runtime.env" >&2
exit 2
