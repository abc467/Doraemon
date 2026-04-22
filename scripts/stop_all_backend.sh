#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMUX_SESSION="${TMUX_SESSION:-doraemon_task_ready}"
FRONTEND_TMUX_SESSION="${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}"
STOP_MASTER="${STOP_MASTER:-1}"

usage() {
  cat <<'EOF'
Usage: stop_all_backend.sh

Stop runtime session, frontend/backend service session, and optionally ROS master.

Environment:
  STOP_MASTER=1  stop roscore/rosmaster when no Doraemon tmux sessions remain
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
  runtime_stop_frontend_services
  runtime_stop_ros_master_if_idle

  echo "[OK] all Doraemon backend services stopped"
  echo "[INFO] runtime session cleared: ${TMUX_SESSION}"
  echo "[INFO] frontend service session cleared: ${FRONTEND_TMUX_SESSION}"
  echo "[INFO] ROS master stop requested: ${STOP_MASTER}"
}

main
