#!/usr/bin/env bash

set -euo pipefail

RECOVERY_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
export DORAEMON_RUNTIME_CONFIG_FILE="${DORAEMON_RUNTIME_CONFIG_FILE:-/etc/doraemon/runtime.env}"
# shellcheck disable=SC1091
source "${RECOVERY_SCRIPT_DIR}/start_runtime.sh"
# shellcheck disable=SC1091
source "${RECOVERY_SCRIPT_DIR}/runtime_common.sh"
runtime_common_init

LOCK_FILE="/run/user/$(id -u)/doraemon-orbbec-recovery.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  runtime_log_status "[ERROR] another Orbbec recovery is already active"
  exit 75
fi

if ! tmux has-session -t "${TMUX_SESSION}" 2>/dev/null; then
  runtime_log_status "[ERROR] runtime tmux session is missing: ${TMUX_SESSION}"
  exit 1
fi

camera_nodes=(/gemini_cf/camera /gemini_nj/camera /gemini_front/camera)
camera_windows=(depth_left depth_right depth_front)
camera_pids=()

cleanup_failed_recovery() {
  local exit_code="$?"
  trap - ERR
  runtime_log_status "[ERROR] Orbbec recovery failed; removing partial camera chain"
  for node in "${camera_nodes[@]}"; do
    timeout 2 rosnode kill "${node}" >/dev/null 2>&1 || true
  done
  for window in "${camera_windows[@]}"; do
    tmux kill-window -t "${TMUX_SESSION}:${window}" 2>/dev/null || true
  done
  exit "${exit_code}"
}
trap cleanup_failed_recovery ERR

for node in "${camera_nodes[@]}"; do
  pid="$(timeout 2 rosnode info "${node}" 2>/dev/null | awk '/Pid:/{print $2; exit}' || true)"
  [[ "${pid}" =~ ^[1-9][0-9]*$ ]] && camera_pids+=("${pid}")
  timeout 2 rosnode kill "${node}" >/dev/null 2>&1 || true
done

for window in "${camera_windows[@]}"; do
  tmux kill-window -t "${TMUX_SESSION}:${window}" 2>/dev/null || true
done

deadline=$((SECONDS + 10))
while (( SECONDS < deadline )); do
  alive=false
  for pid in "${camera_pids[@]}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      alive=true
      break
    fi
  done
  [[ "${alive}" == "false" ]] && break
  sleep 0.2
done

for pid in "${camera_pids[@]}"; do
  if kill -0 "${pid}" 2>/dev/null; then
    cmdline="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
    case "${cmdline}" in
      *orbbec_camera*|*camera_node*)
        runtime_log_status "[WARN] force-stopping wedged Orbbec pid=${pid}"
        kill -KILL "${pid}" 2>/dev/null || true
        ;;
    esac
  fi
done

runtime_cleanup_ros_nodes
sleep 1
start_depth_cameras_sequentially
trap - ERR
if [[ "${DORAEMON_ORBBEC_RECOVERY_CONTEXT:-runtime}" == "startup" ]]; then
  runtime_log_status "[OK] Orbbec chain startup recovery completed under startup ownership"
else
  runtime_log_status "[OK] Orbbec chain recovery completed; task remains paused"
fi
