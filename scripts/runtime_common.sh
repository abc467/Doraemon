#!/usr/bin/env bash

_RUNTIME_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${_RUNTIME_COMMON_DIR}/source_slam_runtime_env.sh"

runtime_common_init() {
  local common_dir
  local resolved_workspace_setup=""
  local resolved_workspace_layout=""
  common_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  export DORAEMON_SCRIPT_DIR="${common_dir}"
  export DORAEMON_REPO_ROOT="${DORAEMON_REPO_ROOT:-$(cd "${common_dir}/.." && pwd)}"
  export DORAEMON_ROS_SETUP="${DORAEMON_ROS_SETUP:-/opt/ros/noetic/setup.bash}"
  if [[ -n "${DORAEMON_WORKSPACE_SETUP:-}" ]]; then
    resolved_workspace_setup="${DORAEMON_WORKSPACE_SETUP}"
  else
    resolved_workspace_setup="$(resolve_workspace_setup "${DORAEMON_REPO_ROOT}")" || return 1
  fi
  export DORAEMON_WORKSPACE_SETUP="${resolved_workspace_setup}"
  resolved_workspace_layout="$(workspace_layout_from_setup "${resolved_workspace_setup}")"
  export DORAEMON_WORKSPACE_LAYOUT="${resolved_workspace_layout}"
  export ROS_MASTER_URI="${ROS_MASTER_URI:-http://localhost:11311}"
  unset ROS_IP
  unset ROS_HOSTNAME

  if [[ ! -f "${DORAEMON_ROS_SETUP}" ]]; then
    echo "[ERROR] ROS setup not found: ${DORAEMON_ROS_SETUP}" >&2
    return 1
  fi

  if [[ ! -f "${DORAEMON_WORKSPACE_SETUP}" ]]; then
    echo "[ERROR] Workspace setup not found: ${DORAEMON_WORKSPACE_SETUP}" >&2
    return 1
  fi

  set +u
  # shellcheck disable=SC1090
  source "${DORAEMON_ROS_SETUP}"
  # shellcheck disable=SC1090
  source "${DORAEMON_WORKSPACE_SETUP}"
  set -u
}

runtime_log_status() {
  local msg="$*"
  local ts
  ts="$(date '+%F %T')"
  if [[ -n "${STATUS_LOG:-}" ]]; then
    mkdir -p "$(dirname "${STATUS_LOG}")"
    echo "[${ts}] ${msg}" | tee -a "${STATUS_LOG}"
  else
    echo "[${ts}] ${msg}"
  fi
}

runtime_ros_master_available() {
  rosnode list >/dev/null 2>&1
}

runtime_wait_for_master() {
  local timeout_sec="${1:-20}"
  local start_ts
  start_ts="$(date +%s)"
  until runtime_ros_master_available; do
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      echo "[ERROR] roscore not ready after ${timeout_sec}s" >&2
      return 1
    fi
    sleep 1
  done
}

runtime_wait_for_service() {
  local service_name="$1"
  local timeout_sec="${2:-30}"
  local start_ts
  start_ts="$(date +%s)"
  until rosservice type "${service_name}" >/dev/null 2>&1; do
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      echo "[ERROR] service not ready: ${service_name}" >&2
      return 1
    fi
    sleep 1
  done
}

runtime_wait_for_topic() {
  local topic_name="$1"
  local timeout_sec="${2:-20}"
  local sample_timeout_sec="${3:-5}"
  local start_ts
  start_ts="$(date +%s)"
  until timeout "${sample_timeout_sec}" rostopic echo -n 1 "${topic_name}" >/dev/null 2>&1; do
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      echo "[ERROR] topic not ready: ${topic_name}" >&2
      return 1
    fi
    sleep 1
  done
}

runtime_topic_ready_once() {
  local topic_name="$1"
  local timeout_sec="${2:-8}"
  local sample_timeout_sec="${3:-3}"
  local start_ts
  start_ts="$(date +%s)"
  until timeout "${sample_timeout_sec}" rostopic echo -n 1 "${topic_name}" >/dev/null 2>&1; do
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      return 1
    fi
    sleep 1
  done
}

runtime_node_ready_once() {
  local node_name="$1"
  local timeout_sec="${2:-8}"
  local start_ts
  start_ts="$(date +%s)"
  until rosnode info "${node_name}" >/dev/null 2>&1; do
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      return 1
    fi
    sleep 1
  done
}

runtime_service_available() {
  local service_name="$1"
  rosservice type "${service_name}" >/dev/null 2>&1
}

runtime_get_rosparam_value() {
  local param_name="$1"
  rosparam get "${param_name}" 2>/dev/null | tr -d '\r'
}

runtime_trim_yaml_scalar() {
  local value="${1:-}"

  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"

  if [[ ${#value} -ge 2 && "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
    value="${value:1:${#value}-2}"
  elif [[ ${#value} -ge 2 && "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
    value="${value:1:${#value}-2}"
  fi

  printf '%s' "${value}"
}

runtime_extract_yaml_scalar() {
  local yaml_text="${1:-}"
  local field_name="$2"
  local raw_value

  raw_value="$(
    awk -F': ' -v key="${field_name}" '
      $1 ~ ("^[[:space:]]*" key "$") {
        print $2
        exit
      }
    ' <<<"${yaml_text}"
  )"

  runtime_trim_yaml_scalar "${raw_value}"
}

RUNTIME_LOCALIZATION_SOURCE=""
RUNTIME_LOCALIZATION_STATE=""
RUNTIME_LOCALIZATION_VALID=""
RUNTIME_LOCALIZATION_ACTIVE_MAP=""
RUNTIME_LOCALIZATION_ACTIVE_REVISION=""
RUNTIME_LOCALIZATION_RUNTIME_MAP=""
RUNTIME_LOCALIZATION_RUNTIME_REVISION=""

runtime_reset_localization_snapshot() {
  RUNTIME_LOCALIZATION_SOURCE=""
  RUNTIME_LOCALIZATION_STATE=""
  RUNTIME_LOCALIZATION_VALID=""
  RUNTIME_LOCALIZATION_ACTIVE_MAP=""
  RUNTIME_LOCALIZATION_ACTIVE_REVISION=""
  RUNTIME_LOCALIZATION_RUNTIME_MAP=""
  RUNTIME_LOCALIZATION_RUNTIME_REVISION=""
}

runtime_load_localization_snapshot() {
  local request
  local status_out=""

  runtime_reset_localization_snapshot

  request="$(printf "robot_id: '%s'\nrefresh_map_identity: false" "${ROBOT_ID:-local_robot}")"
  status_out="$(rosservice call /clean_robot_server/app/get_slam_status "${request}" 2>/dev/null || true)"
  if [[ -n "${status_out}" ]]; then
    RUNTIME_LOCALIZATION_SOURCE="app_query"
  fi

  if [[ -n "${status_out}" ]]; then
    RUNTIME_LOCALIZATION_STATE="$(runtime_extract_yaml_scalar "${status_out}" localization_state)"
    RUNTIME_LOCALIZATION_VALID="$(runtime_extract_yaml_scalar "${status_out}" localization_valid)"
    RUNTIME_LOCALIZATION_ACTIVE_MAP="$(runtime_extract_yaml_scalar "${status_out}" active_map_name)"
    RUNTIME_LOCALIZATION_ACTIVE_REVISION="$(runtime_extract_yaml_scalar "${status_out}" active_map_revision_id)"
    RUNTIME_LOCALIZATION_RUNTIME_MAP="$(runtime_extract_yaml_scalar "${status_out}" runtime_map_name)"
    RUNTIME_LOCALIZATION_RUNTIME_REVISION="$(runtime_extract_yaml_scalar "${status_out}" runtime_map_revision_id)"
    return 0
  fi

  RUNTIME_LOCALIZATION_SOURCE="rosparam_fallback"
  RUNTIME_LOCALIZATION_STATE="$(runtime_trim_yaml_scalar "$(runtime_get_rosparam_value /cartographer/runtime/localization_state || true)")"
  RUNTIME_LOCALIZATION_VALID="$(runtime_trim_yaml_scalar "$(runtime_get_rosparam_value /cartographer/runtime/localization_valid || true)")"
  RUNTIME_LOCALIZATION_RUNTIME_MAP="$(runtime_trim_yaml_scalar "$(runtime_get_rosparam_value /cartographer/runtime/map_name || true)")"
  RUNTIME_LOCALIZATION_RUNTIME_REVISION="$(runtime_trim_yaml_scalar "$(runtime_get_rosparam_value /cartographer/runtime/current_map_revision_id || true)")"
}

runtime_build_readiness_request() {
  printf "task_id: %s\nrefresh_map_identity: %s" \
    "${READINESS_TASK_ID:-0}" \
    "${READINESS_REFRESH_MAP_IDENTITY:-false}"
}

runtime_warn_if_optional_topic_missing() {
  local topic_name="$1"
  local label="$2"
  local timeout_sec="${3:-8}"
  local sample_timeout_sec="${4:-3}"
  if runtime_topic_ready_once "${topic_name}" "${timeout_sec}" "${sample_timeout_sec}"; then
    runtime_log_status "[OK] ${label}: ${topic_name}"
  else
    runtime_log_status "[WARN] ${label}未就绪（本次不阻塞启动）: ${topic_name}"
  fi
}

runtime_warn_if_optional_node_missing() {
  local node_name="$1"
  local label="$2"
  local timeout_sec="${3:-8}"
  if runtime_node_ready_once "${node_name}" "${timeout_sec}"; then
    runtime_log_status "[OK] ${label}: ${node_name}"
  else
    runtime_log_status "[WARN] ${label}未就绪（本次不阻塞启动）: ${node_name}"
  fi
}

runtime_warn_if_localization_not_ready() {
  local expected_map="$1"
  local expected_revision="${2:-}"
  local state
  local valid
  local active_map
  local active_revision
  local runtime_map
  local runtime_revision
  local source
  local localization_ready="false"
  local restart_summary

  runtime_load_localization_snapshot
  state="${RUNTIME_LOCALIZATION_STATE}"
  valid="${RUNTIME_LOCALIZATION_VALID}"
  active_map="${RUNTIME_LOCALIZATION_ACTIVE_MAP}"
  active_revision="${RUNTIME_LOCALIZATION_ACTIVE_REVISION}"
  runtime_map="${RUNTIME_LOCALIZATION_RUNTIME_MAP}"
  runtime_revision="${RUNTIME_LOCALIZATION_RUNTIME_REVISION}"
  source="${RUNTIME_LOCALIZATION_SOURCE:-unknown}"

  if [[ "${state}" == "localized" && "${valid,,}" == "true" ]]; then
    if [[ -n "${expected_revision}" && -n "${runtime_revision}" && "${runtime_revision}" == "${expected_revision}" ]]; then
      localization_ready="true"
    elif [[ -n "${expected_revision}" && -z "${runtime_revision}" && -n "${expected_map}" && "${runtime_map}" == "${expected_map}" ]]; then
      localization_ready="true"
    elif [[ -z "${expected_revision}" && -n "${expected_map}" && "${runtime_map}" == "${expected_map}" ]]; then
      localization_ready="true"
    elif [[ -n "${active_revision}" && -n "${runtime_revision}" && "${runtime_revision}" == "${active_revision}" ]]; then
      localization_ready="true"
    fi
  fi

  if [[ "${localization_ready}" == "true" ]]; then
    runtime_log_status "[OK] 定位状态正常: source=${source} state=${state} active_map=${active_map:-missing} active_revision=${active_revision:-missing} runtime_map=${runtime_map:-missing} runtime_revision=${runtime_revision:-missing}"
    return 0
  fi

  runtime_log_status "[WARN] 定位未完全就绪（本次不阻塞启动）: source=${source} state=${state:-missing} valid=${valid:-missing} active_map=${active_map:-missing} active_revision=${active_revision:-missing} runtime_map=${runtime_map:-missing} runtime_revision=${runtime_revision:-missing} expected_map=${expected_map:-missing} expected_revision=${expected_revision:-missing}"
  runtime_log_status "[WARN] 如需立即开始任务，建议先在前端确认地图与定位状态是否正确"

  if [[ -f "${RESTART_LOCALIZATION_OUT:-}" ]]; then
    restart_summary="$(grep -E '^(success:|message:|map_name:|map_revision_id:|localization_state:|active_map_revision_id:|runtime_map_revision_id:)' "${RESTART_LOCALIZATION_OUT}" | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
    if [[ -n "${restart_summary}" ]]; then
      runtime_log_status "[WARN] 本次重定位返回: ${restart_summary}"
    fi
  fi
}

runtime_warn_if_odometry_not_ready() {
  local status_out
  local connected
  local odom_valid
  local error_code
  local message

  status_out="$(rosservice call /clean_robot_server/app/get_odometry_status "robot_id: '${ROBOT_ID:-local_robot}'" 2>/dev/null || true)"
  if [[ -z "${status_out}" ]]; then
    runtime_log_status "[WARN] 里程计健康服务暂不可用: /clean_robot_server/app/get_odometry_status"
    return 0
  fi

  connected="$(awk -F': ' '/connected:/{print $2; exit}' <<<"${status_out}")"
  odom_valid="$(awk -F': ' '/odom_valid:/{print $2; exit}' <<<"${status_out}")"
  error_code="$(awk -F': ' '/error_code:/{print $2; exit}' <<<"${status_out}")"
  message="$(awk -F': ' '/message:/{print $2; exit}' <<<"${status_out}")"

  if [[ "${odom_valid,,}" == "true" ]]; then
    runtime_log_status "[OK] 里程计健康正常: connected=${connected:-missing} odom_valid=${odom_valid}"
    return 0
  fi

  runtime_log_status "[WARN] 里程计健康未就绪: connected=${connected:-missing} odom_valid=${odom_valid:-missing} code=${error_code:-missing} message=${message:-missing}"
}

runtime_wait_for_readiness() {
  local timeout_sec="${1:-60}"
  local start_ts
  local last_response=""
  local request
  start_ts="$(date +%s)"
  request="$(runtime_build_readiness_request)"
  while true; do
    last_response="$(rosservice call /coverage_task_manager/app/get_system_readiness "${request}" 2>/dev/null || true)"
    if grep -q 'can_start_task: True' <<<"${last_response}"; then
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      echo "[ERROR] task system not ready after ${timeout_sec}s" >&2
      if [[ -n "${last_response}" ]]; then
        echo "[ERROR] latest readiness response:" >&2
        echo "${last_response}" >&2
      fi
      return 1
    fi
    sleep 2
  done
}

runtime_query_active_map_identity_field() {
  local field_name="$1"
  python3 - "${field_name}" <<'PY'
import os
import sqlite3
import sys

db_path = os.environ.get("PLAN_DB_PATH", "/data/coverage/planning.db")
robot_id = os.environ.get("ROBOT_ID", "local_robot")
field_name = str(sys.argv[1] or "").strip()

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

row = None

try:
    row = conn.execute(
        """
        SELECT mr.map_name AS map_name, rar.active_revision_id AS active_revision_id
        FROM robot_active_map_revision rar
        JOIN map_revisions mr ON mr.revision_id=rar.active_revision_id
        WHERE rar.robot_id=?
        ORDER BY rar.updated_ts DESC
        LIMIT 1
        """,
        (robot_id,),
    ).fetchone()
except sqlite3.OperationalError:
    row = None

if row is None:
    try:
        row = conn.execute(
            """
            SELECT map_name AS map_name, '' AS active_revision_id
            FROM robot_active_map
            WHERE robot_id=?
            ORDER BY updated_ts DESC
            LIMIT 1
            """,
            (robot_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None

if row is not None and field_name in row.keys():
    value = str(row[field_name] or "").strip()
    if value:
        print(value)

conn.close()
PY
}

runtime_get_active_map_name() {
  runtime_query_active_map_identity_field "map_name"
}

runtime_get_active_map_revision_id() {
  runtime_query_active_map_identity_field "active_revision_id"
}

runtime_frontend_service_session_healthy() {
  local required_services=(
    "/clean_robot_server/app/map_server"
    "/database_server/app/profile_catalog_service"
    "/clean_robot_server/app/get_slam_status"
    "/clean_robot_server/app/get_slam_job"
  )
  local service_name

  if [[ "${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-false}" == "true" ]]; then
    required_services+=("/clean_robot_server/app/get_odometry_status")
  fi

  for service_name in "${required_services[@]}"; do
    runtime_service_available "${service_name}" || return 1
  done

  if [[ "${START_ROSBRIDGE:-true}" == "true" ]]; then
    runtime_service_available "/rosapi/topics" || return 1
  fi
}

runtime_site_gateway_health_ok() {
  local health_url="${SITE_GATEWAY_HEALTH_URL:-http://127.0.0.1:4173/api/health}"
  local output
  if ! command -v curl >/dev/null 2>&1; then
    return 1
  fi
  output="$(curl -fsS -m "${SITE_GATEWAY_HEALTH_TIMEOUT:-5}" "${health_url}" 2>/dev/null || true)"
  [[ "${output}" == *'"status":"ok"'* && "${output}" == *'"isConnected":true'* ]]
}

runtime_restart_site_gateway_if_disconnected() {
  local service_name="${SITE_GATEWAY_SERVICE:-clean-robot-site-gateway.service}"
  local health_url="${SITE_GATEWAY_HEALTH_URL:-http://127.0.0.1:4173/api/health}"
  local pid
  local attempt

  if [[ "${RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE:-true}" != "true" ]]; then
    return 0
  fi

  if runtime_site_gateway_health_ok; then
    runtime_log_status "[OK] site gateway connected: ${health_url}"
    return 0
  fi

  if ! command -v systemctl >/dev/null 2>&1 || ! systemctl is-active --quiet "${service_name}" 2>/dev/null; then
    runtime_log_status "[WARN] site gateway health not connected and service is not active: ${service_name}"
    return 0
  fi

  pid="$(systemctl show -p MainPID --value "${service_name}" 2>/dev/null || true)"
  if [[ -z "${pid}" || "${pid}" == "0" ]]; then
    runtime_log_status "[WARN] site gateway health not connected but MainPID is unavailable: ${service_name}"
    return 0
  fi

  runtime_log_status "[WARN] site gateway not connected to rosbridge; restarting ${service_name} via SIGTERM pid=${pid}"
  kill -TERM "${pid}" >/dev/null 2>&1 || {
    runtime_log_status "[WARN] failed to terminate site gateway pid=${pid}; run: sudo systemctl restart ${service_name}"
    return 0
  }

  for attempt in $(seq 1 15); do
    sleep 1
    if runtime_site_gateway_health_ok; then
      runtime_log_status "[OK] site gateway reconnected: ${health_url}"
      return 0
    fi
  done

  runtime_log_status "[WARN] site gateway did not reconnect yet; check: systemctl status ${service_name}"
}

runtime_tmux_window() {
  local session_name="$1"
  local window_name="$2"
  shift 2
  local cmd="$*"
  tmux new-window -t "${session_name}" -n "${window_name}" \
    "bash -lc 'source \"${DORAEMON_ROS_SETUP}\"; source \"${DORAEMON_WORKSPACE_SETUP}\"; export ROS_MASTER_URI=${ROS_MASTER_URI}; unset ROS_IP ROS_HOSTNAME; ${cmd}'"
}

runtime_ensure_frontend_service_session() {
  local backend_cmd
  local enable_odometry_health

  enable_odometry_health="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-false}"
  backend_cmd="export PLAN_DB_PATH='${PLAN_DB_PATH:-/data/coverage/planning.db}'; export OPS_DB_PATH='${OPS_DB_PATH:-/data/coverage/operations.db}'; export ROBOT_ID='${ROBOT_ID:-local_robot}'; export MAPS_ROOT='${MAPS_ROOT:-/data/maps}'; export EXTERNAL_MAPS_ROOT='${EXTERNAL_MAPS_ROOT:-/data/maps/imports}'; export MAP_TOPIC='${MAP_TOPIC:-/map}'; export START_ROSBRIDGE='${START_ROSBRIDGE:-true}'; export ROSBRIDGE_ADDRESS='${ROSBRIDGE_ADDRESS:-0.0.0.0}'; export ROSBRIDGE_PORT='${ROSBRIDGE_PORT:-9090}'; export START_MAP_ASSET_SERVICE='${START_MAP_ASSET_SERVICE:-true}'; export ENABLE_SITE_EDITOR_SERVICE='${ENABLE_SITE_EDITOR_SERVICE:-true}'; export ENABLE_RECT_ZONE_PLANNER='${ENABLE_RECT_ZONE_PLANNER:-false}'; export FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH='${enable_odometry_health}'; exec \"${DORAEMON_SCRIPT_DIR}/start_frontend_backend.sh\""

  runtime_log_status "ensure frontend service session ${FRONTEND_TMUX_SESSION}"
  if tmux has-session -t "${FRONTEND_TMUX_SESSION}" 2>/dev/null; then
    if runtime_ros_master_available && runtime_frontend_service_session_healthy; then
      runtime_log_status "frontend service session already running with app query contracts"
      return 0
    fi
    runtime_log_status "frontend service session exists but app query contracts are incomplete, recreate it"
    tmux kill-session -t "${FRONTEND_TMUX_SESSION}" || true
  fi

  if runtime_ros_master_available; then
    tmux new-session -d -s "${FRONTEND_TMUX_SESSION}" -n roscore \
      "bash -lc 'source \"${DORAEMON_ROS_SETUP}\"; source \"${DORAEMON_WORKSPACE_SETUP}\"; export ROS_MASTER_URI=${ROS_MASTER_URI}; unset ROS_IP ROS_HOSTNAME; echo \"roscore already running\"; exec sleep infinity'"
  else
    tmux new-session -d -s "${FRONTEND_TMUX_SESSION}" -n roscore \
      "bash -lc 'source \"${DORAEMON_ROS_SETUP}\"; source \"${DORAEMON_WORKSPACE_SETUP}\"; export ROS_MASTER_URI=${ROS_MASTER_URI}; unset ROS_IP ROS_HOSTNAME; exec roscore'"
    sleep 2
  fi

  runtime_tmux_window "${FRONTEND_TMUX_SESSION}" backend "${backend_cmd}"

  if [[ "${START_FRONTEND_DEV:-0}" == "1" && -d "${FRONTEND_DIR:-}" ]] && command -v pnpm >/dev/null 2>&1; then
    runtime_tmux_window "${FRONTEND_TMUX_SESSION}" frontend \
      "source ~/.bashrc >/dev/null 2>&1 || true; cd \"${FRONTEND_DIR}\"; exec pnpm run dev"
  fi

  tmux set-option -t "${FRONTEND_TMUX_SESSION}" remain-on-exit on >/dev/null
}

runtime_kill_tmux_session_if_exists() {
  local session_name="$1"
  tmux has-session -t "${session_name}" 2>/dev/null && tmux kill-session -t "${session_name}" || true
}

runtime_cleanup_ros_nodes() {
  if runtime_ros_master_available; then
    printf 'y\n' | rosnode cleanup >/dev/null 2>&1 || true
  fi
}

runtime_stop_task_execution_if_available() {
  if ! runtime_ros_master_available; then
    return 0
  fi

  if rosservice type /coverage_task_manager/app/exe_task_server >/dev/null 2>&1; then
    rosservice call /coverage_task_manager/app/exe_task_server "{command: 3, task_id: 0}" >/dev/null 2>&1 || true
    return 0
  fi

  echo "[INFO] /coverage_task_manager/app/exe_task_server unavailable, skip task stop"
  return 0
}

runtime_graceful_stop_runtime() {
  if ! runtime_ros_master_available; then
    echo "[INFO] ROS master not reachable, skip graceful stop"
    return 0
  fi

  echo "[INFO] send task stop"
  runtime_stop_task_execution_if_available

  echo "[INFO] cancel dock supply"
  rosservice call /dock_supply/cancel '{}' >/dev/null 2>&1 || true

  sleep 2
}

runtime_kill_runtime_tmux_sessions() {
  runtime_kill_tmux_session_if_exists "${TMUX_SESSION:-doraemon_task_ready}"
  runtime_kill_tmux_session_if_exists "my_session"
  runtime_kill_tmux_session_if_exists "orbbec_dual_depth"
  runtime_kill_tmux_session_if_exists "orbbec_dual_rgbd"
}

runtime_kill_runtime_nodes() {
  if ! runtime_ros_master_available; then
    return 0
  fi

  local nodes=(
    "/coverage_task_manager"
    "/coverage_executor"
    "/task_api_service"
    "/schedule_api_service"
    "/localization_lifecycle_manager"
    "/odometry_health"
    "/mcore_tcp_bridge"
    "/station_tcp_bridge"
    "/dock_supply_manager"
    "/dock_tracker"
    "/docking_controller"
    "/move_base_flex"
    "/map_constraints"
    "/cartographer_node"
    "/cartographer_occupancy_grid_node"
    "/runtime_flag_server"
    "/wheel_speed_odom"
    "/wheel_speed_odom_ekf"
    "/wheel_speed_odom_ekf_aggressive"
    "/wheel_speed_odom_ekf_balanced"
    "/wheel_speed_odom_ekf_conservative"
    "/ekf_covariance_override"
    "/vanjee_lidar_sdk_node"
    "/ahrs_driver"
    "/robot_state_publisher"
  )

  local node
  for node in "${nodes[@]}"; do
    rosnode info "${node}" >/dev/null 2>&1 || continue
    echo "[INFO] rosnode kill ${node}"
    rosnode kill "${node}" >/dev/null 2>&1 || true
  done

  sleep 2
  runtime_cleanup_ros_nodes
}

runtime_kill_runtime_processes() {
  local patterns=(
    "cleanrobot_base.launch"
    "realsense2_camera"
    "rs_camera.launch"
    "orbbec_camera_node"
    "wheel_speed_odom_bridge wheel_speed_odom.launch"
    "wheel_speed_odom_node"
    "ekf_covariance_override_node"
    "wheel_speed_odom_ekf_"
    "mcore_tcp_bridge.py"
    "station_tcp_bridge.py"
    "dock_supply_manager.py"
    "my_docking_controller.launch"
    "hardware_bridges.launch"
    "mbf_costmap_nav"
    "task_system.launch"
    "task_manager.launch"
    "task_manager_node.py"
    "task_api_service_node.py"
    "schedule_api_service_node.py"
    "executor_node.py"
    "localization_lifecycle_manager_node.py"
    "cartographer_node"
    "cartographer_occupancy_grid_node"
    "runtime_flag_server_node"
  )

  local pattern
  for pattern in "${patterns[@]}"; do
    pkill -f -- "${pattern}" >/dev/null 2>&1 || true
  done

  sleep 2
  runtime_cleanup_ros_nodes
}

runtime_stop_frontend_services() {
  runtime_kill_tmux_session_if_exists "${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}"

  if runtime_ros_master_available; then
    local nodes=(
      "/map_asset_service"
      "/coverage_planner_server"
      "/rect_zone_planner"
      "/site_editor_service"
      "/profile_catalog_service"
      "/slam_runtime_manager"
      "/slam_api_service"
      "/rosbridge_websocket"
      "/rosapi"
    )

    local node
    for node in "${nodes[@]}"; do
      rosnode info "${node}" >/dev/null 2>&1 || continue
      echo "[INFO] rosnode kill ${node}"
      rosnode kill "${node}" >/dev/null 2>&1 || true
    done
  fi

  local patterns=(
    "frontend_editor_backend.launch"
    "planner_server.launch"
    "map_asset_service_node.py"
    "planner_server_node.py"
    "rect_zone_planner_node.py"
    "site_editor_service_node.py"
    "profile_catalog_service_node.py"
    "slam_runtime_manager_node.py"
    "slam_api_service_node.py"
    "rosbridge_websocket"
    "rosapi_node"
  )

  local pattern
  for pattern in "${patterns[@]}"; do
    pkill -f -- "${pattern}" >/dev/null 2>&1 || true
  done

  sleep 2
  runtime_cleanup_ros_nodes
}

runtime_stop_ros_master_if_idle() {
  local stop_master
  stop_master="${STOP_MASTER:-1}"

  if [[ "${stop_master}" != "1" ]]; then
    return 0
  fi

  if tmux has-session -t "${TMUX_SESSION:-doraemon_task_ready}" 2>/dev/null; then
    return 0
  fi
  if tmux has-session -t "${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}" 2>/dev/null; then
    return 0
  fi

  pkill -f -- "roscore" >/dev/null 2>&1 || true
  pkill -f -- "rosmaster" >/dev/null 2>&1 || true
  pkill -f -- "rosout" >/dev/null 2>&1 || true
}

runtime_run_contract_check() {
  local timeout_sec="${1:-30}"
  local interval_sec="${2:-1}"
  local cmd=(
    rosrun
    coverage_planner
    check_ros_contracts.py
    --strict
    --text
    --wait-timeout
    "${timeout_sec}"
    --wait-interval
    "${interval_sec}"
  )
  "${cmd[@]}"
}
