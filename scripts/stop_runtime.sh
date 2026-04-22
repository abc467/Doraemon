#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TMUX_SESSION="${TMUX_SESSION:-doraemon_task_ready}"
FRONTEND_TMUX_SESSION="${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}"

usage() {
  cat <<'EOF'
Usage: stop_runtime.sh

Stop only the runtime side of Doraemon and preserve frontend/backend service session.
EOF
}

for arg in "$@"; do
  case "${arg}" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: ${arg}" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/runtime_common.sh"

main() {
  runtime_common_init
  runtime_graceful_stop_runtime
  runtime_kill_runtime_tmux_sessions
  runtime_kill_runtime_nodes
  runtime_kill_runtime_processes

  echo "[OK] runtime stopped"
  echo "[INFO] tmux session cleared: ${TMUX_SESSION}"
  echo "[INFO] frontend service session preserved: ${FRONTEND_TMUX_SESSION}"
  echo "[INFO] ROS master target: ${ROS_MASTER_URI}"
}

main
