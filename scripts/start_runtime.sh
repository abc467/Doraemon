#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LOG_DIR="${LOG_DIR:-${REPO_ROOT}/log/startup}"
STATUS_LOG="${STATUS_LOG:-${LOG_DIR}/startup_status.log}"
RESTART_LOCALIZATION_OUT="${RESTART_LOCALIZATION_OUT:-${LOG_DIR}/restart_localization.out}"
TMUX_SESSION="${TMUX_SESSION:-doraemon_task_ready}"
FRONTEND_TMUX_SESSION="${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}"
ROBOT_ID="${ROBOT_ID:-local_robot}"
PLAN_DB_PATH="${PLAN_DB_PATH:-/data/coverage/planning.db}"
OPS_DB_PATH="${OPS_DB_PATH:-/data/coverage/operations.db}"
MAPS_ROOT="${MAPS_ROOT:-/data/maps}"
EXTERNAL_MAPS_ROOT="${EXTERNAL_MAPS_ROOT:-/data/maps/imports}"
MAP_TOPIC="${MAP_TOPIC:-/map}"
START_ROSBRIDGE="${START_ROSBRIDGE:-true}"
ROSBRIDGE_ADDRESS="${ROSBRIDGE_ADDRESS:-0.0.0.0}"
ROSBRIDGE_PORT="${ROSBRIDGE_PORT:-9090}"
START_MAP_ASSET_SERVICE="${START_MAP_ASSET_SERVICE:-true}"
ENABLE_SITE_EDITOR_SERVICE="${ENABLE_SITE_EDITOR_SERVICE:-true}"
ENABLE_RECT_ZONE_PLANNER="${ENABLE_RECT_ZONE_PLANNER:-false}"
FRONTEND_DIR="${FRONTEND_DIR:-}"
START_FRONTEND_DEV="${START_FRONTEND_DEV:-0}"
FRONTEND_URL="${FRONTEND_URL:-http://127.0.0.1:5173/}"
ATTACH="${ATTACH:-0}"
CONTRACT_WAIT_TIMEOUT="${CONTRACT_WAIT_TIMEOUT:-30}"
CONTRACT_WAIT_INTERVAL="${CONTRACT_WAIT_INTERVAL:-1}"
READINESS_WAIT_TIMEOUT="${READINESS_WAIT_TIMEOUT:-90}"
ALLOW_NO_ACTIVE_MAP_STARTUP="${ALLOW_NO_ACTIVE_MAP_STARTUP:-0}"
FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-false}"
RUNTIME_START_DEPTH_CAMERAS="${RUNTIME_START_DEPTH_CAMERAS:-true}"
RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING="${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING:-true}"
RUNTIME_ENABLE_DEPTH_UP_CAM="${RUNTIME_ENABLE_DEPTH_UP_CAM:-false}"
TASK_AUTO_CHARGE_ENABLE="${TASK_AUTO_CHARGE_ENABLE:-false}"
EXECUTOR_AUTO_CHARGE_ENABLE="${EXECUTOR_AUTO_CHARGE_ENABLE:-false}"
RUN_BACKEND_RUNTIME_SMOKE="${RUN_BACKEND_RUNTIME_SMOKE:-1}"
BACKEND_RUNTIME_SMOKE_TASK_ID="${BACKEND_RUNTIME_SMOKE_TASK_ID:-0}"
BACKEND_RUNTIME_SMOKE_ACTIONS="${BACKEND_RUNTIME_SMOKE_ACTIONS:-}"
BACKEND_RUNTIME_SMOKE_EXTRA_ARGS="${BACKEND_RUNTIME_SMOKE_EXTRA_ARGS:-}"
RUN_REVISION_DB_HEALTH_CHECK="${RUN_REVISION_DB_HEALTH_CHECK:-0}"
REVISION_DB_HEALTH_STRICT="${REVISION_DB_HEALTH_STRICT:-0}"
RUN_BACKEND_PRODUCTION_ACCEPTANCE="${RUN_BACKEND_PRODUCTION_ACCEPTANCE:-0}"
BACKEND_PRODUCTION_ACCEPTANCE_PROFILE="${BACKEND_PRODUCTION_ACCEPTANCE_PROFILE:-}"
BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS="${BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS:-0}"
BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS="${BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS:-}"

mkdir -p "${LOG_DIR}"
: > "${STATUS_LOG}"

usage() {
  cat <<'EOF'
Usage: start_runtime.sh [--attach]

Official full-runtime bringup entry for Doraemon.

Environment highlights:
  START_FRONTEND_DEV=1
  FRONTEND_DIR=/path/to/frontend
  FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH=false
  CONTRACT_WAIT_TIMEOUT=30
  READINESS_WAIT_TIMEOUT=90
  ALLOW_NO_ACTIVE_MAP_STARTUP=0
  RUN_BACKEND_RUNTIME_SMOKE=1
  BACKEND_RUNTIME_SMOKE_ACTIONS=
  RUN_REVISION_DB_HEALTH_CHECK=0
  REVISION_DB_HEALTH_STRICT=0
  RUN_BACKEND_PRODUCTION_ACCEPTANCE=0
  BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=
  BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS=0
EOF
}

parse_args() {
  for arg in "$@"; do
    case "${arg}" in
      --attach)
        ATTACH=1
        ;;
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
}

append_shell_words() {
  local -n target_ref="$1"
  local raw_words="${2:-}"
  if [[ -z "${raw_words}" ]]; then
    return 0
  fi

  # shellcheck disable=SC2206
  local extra_args=( ${raw_words} )
  target_ref+=("${extra_args[@]}")
}

build_backend_runtime_smoke_cmd() {
  local -n cmd_ref="$1"

  cmd_ref=(
    rosrun
    coverage_planner
    run_backend_runtime_smoke.py
    --task-id
    "${BACKEND_RUNTIME_SMOKE_TASK_ID}"
    --text
  )

  if [[ -n "${BACKEND_RUNTIME_SMOKE_ACTIONS}" ]]; then
    cmd_ref+=(
      --actions
      "${BACKEND_RUNTIME_SMOKE_ACTIONS}"
    )
  fi

  append_shell_words cmd_ref "${BACKEND_RUNTIME_SMOKE_EXTRA_ARGS}"
}

build_backend_production_acceptance_cmd() {
  local -n cmd_ref="$1"

  cmd_ref=(
    rosrun
    coverage_planner
    run_backend_production_acceptance.py
    --profile
    "${BACKEND_PRODUCTION_ACCEPTANCE_PROFILE}"
    --plan-db-path
    "${PLAN_DB_PATH}"
    --ops-db-path
    "${OPS_DB_PATH}"
    --robot-id
    "${ROBOT_ID}"
    --text
  )

  if [[ "${BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS}" == "1" ]]; then
    cmd_ref+=(--allow-write-actions)
  fi

  append_shell_words cmd_ref "${BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS}"
}

start_runtime_session() {
  runtime_log_status "start tmux session ${TMUX_SESSION}"
  tmux new-session -d -s "${TMUX_SESSION}" -n base \
    "bash -lc 'source \"${DORAEMON_ROS_SETUP}\"; source \"${DORAEMON_WORKSPACE_SETUP}\"; export ROS_MASTER_URI=${ROS_MASTER_URI}; unset ROS_IP ROS_HOSTNAME; exec roslaunch cleanrobot cleanrobot_base.launch start_depth_cameras:=${RUNTIME_START_DEPTH_CAMERAS}'"

  runtime_tmux_window "${TMUX_SESSION}" imu_bias "exec roslaunch wheel_speed_odom_bridge imu_bias_correction.launch"
  runtime_tmux_window "${TMUX_SESSION}" odom "exec roslaunch wheel_speed_odom_bridge wheel_speed_odom.launch"
  runtime_tmux_window "${TMUX_SESSION}" hardware "exec roslaunch robot_hw_bridge hardware_bridges.launch"
  runtime_tmux_window "${TMUX_SESSION}" nav "exec roslaunch cleanrobot mbf_nav.launch start_map_asset_service:=false enable_depth_obstacle_tracking:=${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING} enable_depth_up_cam:=${RUNTIME_ENABLE_DEPTH_UP_CAM}"
  runtime_tmux_window "${TMUX_SESSION}" task "exec roslaunch coverage_task_manager task_system.launch task_auto_charge_enable:=${TASK_AUTO_CHARGE_ENABLE} executor_auto_charge_enable:=${EXECUTOR_AUTO_CHARGE_ENABLE}"

  tmux new-window -t "${TMUX_SESSION}" -n status \
    "bash -lc 'clear; echo \"Doraemon startup status\"; echo; exec tail -n 200 -f \"${STATUS_LOG}\"'"

  tmux set-option -t "${TMUX_SESSION}" remain-on-exit on >/dev/null
}

clear_previous_runtime() {
  runtime_log_status "clear previous runtime"
  runtime_kill_runtime_tmux_sessions
  runtime_kill_runtime_processes
}

clear_residual_state() {
  runtime_log_status "clear residual task and dock state"
  runtime_stop_task_execution_if_available
  rosservice call /dock_supply/cancel '{}' >/dev/null 2>&1 || true
}

restart_localization_to_active_map() {
  local active_map="${1:-}"
  local active_revision="${2:-}"

  if [[ -z "${active_map}" ]]; then
    active_map="$(runtime_get_active_map_name)"
  fi
  if [[ -z "${active_revision}" ]]; then
    active_revision="$(runtime_get_active_map_revision_id)"
  fi

  if [[ -z "${active_map}" ]]; then
    echo "[ERROR] no active map found in ${PLAN_DB_PATH}" >&2
    return 2
  fi

  runtime_log_status "active map: ${active_map} active_revision=${active_revision:-missing}"
  if ! rosservice type /cartographer/runtime/app/restart_localization >/dev/null 2>&1; then
    echo "[ERROR] /cartographer/runtime/app/restart_localization unavailable" >&2
    return 1
  fi

  rosservice call /cartographer/runtime/app/restart_localization "robot_id: '${ROBOT_ID}'
map_name: '${active_map}'
map_revision_id: '${active_revision}'" | tee "${RESTART_LOCALIZATION_OUT}"
}

handle_degraded_startup_without_active_map() {
  runtime_log_status "[WARN] no active map found; skip restart_localization and readiness gate"
  runtime_log_status "[WARN] system is service-ready only; task readiness stays unavailable until a map is activated and localization is completed"
  runtime_log_status "[OK] runtime started in degraded boot mode"
  runtime_log_status "tmux session: ${TMUX_SESSION}"
  runtime_log_status "frontend service session: ${FRONTEND_TMUX_SESSION}"
  runtime_log_status "ROS master: ${ROS_MASTER_URI}"
  runtime_log_status "frontend backend bridge: /clean_robot_server/app/map_server"
  runtime_log_status "SLAM status service: /clean_robot_server/app/get_slam_status"
  runtime_log_status "odometry status service: /clean_robot_server/app/get_odometry_status"
  runtime_log_status "task start service: /coverage_task_manager/app/exe_task_server"
}

run_backend_runtime_smoke_if_enabled() {
  if [[ "${RUN_BACKEND_RUNTIME_SMOKE}" != "1" ]]; then
    return 0
  fi

  runtime_log_status "运行 backend runtime smoke"

  local cmd=()
  build_backend_runtime_smoke_cmd cmd

  (
    source "${DORAEMON_ROS_SETUP}"
    source "${DORAEMON_WORKSPACE_SETUP}"
    "${cmd[@]}"
  )
}

run_revision_db_health_if_enabled() {
  if [[ "${RUN_REVISION_DB_HEALTH_CHECK}" != "1" ]]; then
    return 0
  fi

  runtime_log_status "检查 revision db health"

  local cmd=(
    rosrun
    coverage_planner
    check_revision_db_health.py
    --plan-db-path
    "${PLAN_DB_PATH}"
    --ops-db-path
    "${OPS_DB_PATH}"
    --robot-id
    "${ROBOT_ID}"
    --text
  )

  if [[ "${REVISION_DB_HEALTH_STRICT}" == "1" ]]; then
    cmd+=(--strict)
  fi

  (
    source "${DORAEMON_ROS_SETUP}"
    source "${DORAEMON_WORKSPACE_SETUP}"
    "${cmd[@]}"
  )
}

run_backend_production_acceptance_if_enabled() {
  if [[ "${RUN_BACKEND_PRODUCTION_ACCEPTANCE}" != "1" ]]; then
    return 0
  fi

  if [[ -z "${BACKEND_PRODUCTION_ACCEPTANCE_PROFILE}" ]]; then
    echo "[ERROR] BACKEND_PRODUCTION_ACCEPTANCE_PROFILE is required when RUN_BACKEND_PRODUCTION_ACCEPTANCE=1" >&2
    return 1
  fi

  runtime_log_status "运行 backend production acceptance profile=${BACKEND_PRODUCTION_ACCEPTANCE_PROFILE}"

  local cmd=()
  build_backend_production_acceptance_cmd cmd

  (
    source "${DORAEMON_ROS_SETUP}"
    source "${DORAEMON_WORKSPACE_SETUP}"
    "${cmd[@]}"
  )
}

run_post_ready_acceptance_if_enabled() {
  if [[ "${RUN_BACKEND_PRODUCTION_ACCEPTANCE}" == "1" ]]; then
    runtime_log_status "[INFO] backend production acceptance 已包含 revision db health + runtime smoke，跳过单独后置检查"
    run_backend_production_acceptance_if_enabled
    return 0
  fi

  run_revision_db_health_if_enabled
  run_backend_runtime_smoke_if_enabled
}

main() {
  runtime_common_init

  runtime_log_status "startup begin"
  runtime_log_status "workspace layout: ${DORAEMON_WORKSPACE_LAYOUT:-unknown}"
  runtime_ensure_frontend_service_session
  runtime_log_status "wait frontend roscore"
  runtime_wait_for_master 20

  clear_previous_runtime
  start_runtime_session
  runtime_cleanup_ros_nodes

  runtime_log_status "等待激光 / IMU / 里程计"
  runtime_wait_for_topic /scan 30
  runtime_wait_for_topic /imu 30
  runtime_wait_for_topic /imu_corrected 30
  runtime_wait_for_topic /odom 40

  runtime_log_status "检查双奥比中光深度相机（仅提示，不阻塞启动）"
  runtime_warn_if_optional_topic_missing /gemini_cf/depth/image_raw "左奥比中光深度图像" 8 3
  runtime_warn_if_optional_topic_missing /gemini_nj/depth/image_raw "右奥比中光深度图像" 8 3
  runtime_warn_if_optional_topic_missing /gemini_cf/depth/points "左奥比中光点云" 8 3
  runtime_warn_if_optional_topic_missing /gemini_nj/depth/points "右奥比中光点云" 8 3
  runtime_log_status "[INFO] 若相机硬件暂未上电，可先继续使用系统，其余链路不会被阻塞"

  runtime_log_status "等待核心服务"
  runtime_wait_for_service /clean_robot_server/app/map_server 30
  runtime_wait_for_service /database_server/app/profile_catalog_service 30
  runtime_wait_for_service /database_server/site/coverage_preview_service 30
  runtime_wait_for_service /database_server/site/coverage_commit_service 30
  runtime_wait_for_service /database_server/app/clean_task_service 30
  runtime_wait_for_service /coverage_task_manager/app/exe_task_server 30
  runtime_wait_for_service /coverage_task_manager/app/get_system_readiness 30
  runtime_wait_for_service /clean_robot_server/app/get_odometry_status 30
  runtime_wait_for_service /clean_robot_server/app/get_slam_status 30
  runtime_wait_for_service /clean_robot_server/app/submit_slam_command 30
  runtime_wait_for_service /clean_robot_server/app/get_slam_job 30
  runtime_wait_for_service /cartographer/runtime/app/restart_localization 30
  runtime_log_status "检查后端 contracts（canonical app/site 主链）"
  runtime_run_contract_check "${CONTRACT_WAIT_TIMEOUT}" "${CONTRACT_WAIT_INTERVAL}"

  clear_residual_state
  sleep 2

  runtime_log_status "重定位到当前活动地图"
  local active_map
  local active_revision
  local relocalize_rc=0
  active_map="$(runtime_get_active_map_name)"
  active_revision="$(runtime_get_active_map_revision_id)"
  restart_localization_to_active_map "${active_map}" "${active_revision}" || relocalize_rc=$?
  if (( relocalize_rc != 0 )); then
    if [[ "${ALLOW_NO_ACTIVE_MAP_STARTUP}" == "1" && "${relocalize_rc}" == "2" ]]; then
      handle_degraded_startup_without_active_map
      return 0
    fi
    return "${relocalize_rc}"
  fi

  runtime_log_status "检查定位状态（仅提示，不阻塞启动）"
  runtime_warn_if_optional_topic_missing /tracked_pose "定位位姿 /tracked_pose" 10 3
  runtime_warn_if_localization_not_ready "${active_map}" "${active_revision}"
  runtime_warn_if_odometry_not_ready

  runtime_log_status "检查深度避障链（仅提示，不阻塞启动）"
  runtime_warn_if_optional_node_missing /left/gs_node "左侧深度避障节点" 8
  runtime_warn_if_optional_node_missing /right/gs_node "右侧深度避障节点" 8
  runtime_warn_if_optional_topic_missing /left/obstacle_2d "左侧深度障碍输出" 8 3
  runtime_warn_if_optional_topic_missing /right/obstacle_2d "右侧深度障碍输出" 8 3
  runtime_log_status "[INFO] 若深度避障链未就绪，系统仍可启动；是否启用深度避障请以后续现场状态为准"

  runtime_log_status "等待任务系统 readiness"
  runtime_wait_for_readiness "${READINESS_WAIT_TIMEOUT}"

  run_post_ready_acceptance_if_enabled

  runtime_log_status "[OK] runtime ready"
  runtime_log_status "tmux session: ${TMUX_SESSION}"
  runtime_log_status "frontend service session: ${FRONTEND_TMUX_SESSION}"
  runtime_log_status "ROS master: ${ROS_MASTER_URI}"
  runtime_log_status "frontend backend bridge: /clean_robot_server/app/map_server"
  runtime_log_status "SLAM status service: /clean_robot_server/app/get_slam_status"
  runtime_log_status "odometry status service: /clean_robot_server/app/get_odometry_status"
  runtime_log_status "task start service: /coverage_task_manager/app/exe_task_server"
  if [[ "${START_FRONTEND_DEV}" == "1" && -d "${FRONTEND_DIR}" ]] && command -v pnpm >/dev/null 2>&1; then
    runtime_log_status "frontend: ${FRONTEND_URL}"
  fi
  runtime_log_status "backend rosbridge: ws://localhost:9090"
  runtime_log_status "you can now click Start in the frontend"

  if [[ "${ATTACH}" == "1" ]]; then
    tmux select-window -t "${TMUX_SESSION}:status" >/dev/null
    exec tmux attach -t "${TMUX_SESSION}"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  parse_args "$@"

  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/runtime_common.sh"

  main
fi
