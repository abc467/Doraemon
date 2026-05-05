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
REQUIRE_TASK_READINESS_ON_STARTUP="${REQUIRE_TASK_READINESS_ON_STARTUP:-0}"
ALLOW_NO_ACTIVE_MAP_STARTUP="${ALLOW_NO_ACTIVE_MAP_STARTUP:-auto}"
STARTUP_RELOCALIZE_ENABLE="${STARTUP_RELOCALIZE_ENABLE:-true}"
FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-false}"
CHASSIS_DRIVER="${CHASSIS_DRIVER:-${DORAEMON_CHASSIS_DRIVER:-legacy_mcore}}"
case "${CHASSIS_DRIVER}" in
  legacy|legacy_mcore|mcore)
    CHASSIS_DRIVER="legacy_mcore"
    ;;
  wheeltec|wheeltec_senior_diff|senior_diff)
    CHASSIS_DRIVER="wheeltec_senior_diff"
    ;;
esac
DEFAULT_RUNTIME_ROBOT_DESCRIPTION_PATH="${REPO_ROOT}/src/cleanrobot_description/urdf/clean_robot.urdf"
DEFAULT_RUNTIME_START_AHRS="true"
DEFAULT_RUNTIME_START_JOINT_STATE_PUBLISHER="false"
DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK="false"
DEFAULT_RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z="0.0"
DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK="false"
DEFAULT_RUNTIME_START_DEPTH_CAMERAS="true"
DEFAULT_RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING="true"
DEFAULT_START_WHEELTEC_BASE="false"
DEFAULT_START_WHEEL_ODOM="true"
DEFAULT_START_MCORE_BRIDGE="true"
DEFAULT_MCORE_ENABLE_CMD_VEL="true"
DEFAULT_START_STATION_BRIDGE="true"
DEFAULT_START_DOCK_SUPPLY_MANAGER="true"
DEFAULT_START_DOCKING_STACK="true"
DEFAULT_WAIT_FOR_CORRECTED_IMU_TOPIC="/imu_corrected"
DEFAULT_ODOMETRY_HEALTH_IMU_TOPIC="/imu_corrected"
DEFAULT_ODOMETRY_HEALTH_EKF_NODE_NAME="/wheel_speed_odom_ekf"
DEFAULT_WHEELTEC_SERIAL_DEVICE="/dev/wheeltec_controller"
WHEELTEC_BY_ID_DEVICE="/dev/serial/by-id/usb-WCH.CN_USB_Single_Serial_0002-if00"

if [[ "${CHASSIS_DRIVER}" == "wheeltec_senior_diff" ]]; then
  DEFAULT_RUNTIME_ROBOT_DESCRIPTION_PATH="${REPO_ROOT}/src/turn_on_wheeltec_robot/urdf/senior_diff_robot.urdf"
  DEFAULT_RUNTIME_START_AHRS="false"
  DEFAULT_RUNTIME_START_JOINT_STATE_PUBLISHER="true"
  DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK="true"
  DEFAULT_RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z="0.0374"
  DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK="true"
  DEFAULT_RUNTIME_START_DEPTH_CAMERAS="false"
  DEFAULT_RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING="false"
  DEFAULT_START_WHEELTEC_BASE="true"
  DEFAULT_START_WHEEL_ODOM="false"
  DEFAULT_START_MCORE_BRIDGE="false"
  DEFAULT_MCORE_ENABLE_CMD_VEL="false"
  DEFAULT_START_STATION_BRIDGE="false"
  DEFAULT_START_DOCK_SUPPLY_MANAGER="false"
  DEFAULT_START_DOCKING_STACK="false"
  DEFAULT_WAIT_FOR_CORRECTED_IMU_TOPIC=""
  DEFAULT_ODOMETRY_HEALTH_IMU_TOPIC="/imu"
  DEFAULT_ODOMETRY_HEALTH_EKF_NODE_NAME="/wheeltec_robot"
  if [[ ! -e "${DEFAULT_WHEELTEC_SERIAL_DEVICE}" && -e "${WHEELTEC_BY_ID_DEVICE}" ]]; then
    DEFAULT_WHEELTEC_SERIAL_DEVICE="${WHEELTEC_BY_ID_DEVICE}"
  fi
fi
RUNTIME_START_ROBOT_STATE_PUBLISHER="${RUNTIME_START_ROBOT_STATE_PUBLISHER:-true}"
RUNTIME_START_JOINT_STATE_PUBLISHER="${RUNTIME_START_JOINT_STATE_PUBLISHER:-${DEFAULT_RUNTIME_START_JOINT_STATE_PUBLISHER}}"
RUNTIME_ROBOT_DESCRIPTION_PATH="${RUNTIME_ROBOT_DESCRIPTION_PATH:-${DEFAULT_RUNTIME_ROBOT_DESCRIPTION_PATH}}"
RUNTIME_START_LIDAR="${RUNTIME_START_LIDAR:-true}"
RUNTIME_START_AHRS="${RUNTIME_START_AHRS:-${DEFAULT_RUNTIME_START_AHRS}}"
RUNTIME_START_DEPTH_CAMERAS="${RUNTIME_START_DEPTH_CAMERAS:-${DEFAULT_RUNTIME_START_DEPTH_CAMERAS}}"
RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING="${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING:-${DEFAULT_RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING}}"
RUNTIME_ENABLE_DEPTH_UP_CAM="${RUNTIME_ENABLE_DEPTH_UP_CAM:-false}"
RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK="${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK:-${DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK}}"
RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z="${RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z:-${DEFAULT_RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z}}"
RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK="${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK:-${DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK}}"
START_WHEELTEC_BASE="${START_WHEELTEC_BASE:-${DEFAULT_START_WHEELTEC_BASE}}"
WHEELTEC_SERIAL_DEVICE="${WHEELTEC_SERIAL_DEVICE:-${DEFAULT_WHEELTEC_SERIAL_DEVICE}}"
WHEELTEC_SERIAL_BAUDRATE="${WHEELTEC_SERIAL_BAUDRATE:-115200}"
WHEELTEC_CAR_MODE="${WHEELTEC_CAR_MODE:-senior_diff}"
WHEELTEC_CMD_VEL_TOPIC="${WHEELTEC_CMD_VEL_TOPIC:-/cmd_vel}"
WHEELTEC_ODOM_FRAME_ID="${WHEELTEC_ODOM_FRAME_ID:-odom}"
WHEELTEC_ROBOT_FRAME_ID="${WHEELTEC_ROBOT_FRAME_ID:-base_footprint}"
WHEELTEC_GYRO_FRAME_ID="${WHEELTEC_GYRO_FRAME_ID:-gyro_link}"
WHEELTEC_PUBLISH_ODOM_TF="${WHEELTEC_PUBLISH_ODOM_TF:-true}"
WHEELTEC_ODOM_X_SCALE="${WHEELTEC_ODOM_X_SCALE:-1.0}"
WHEELTEC_ODOM_Y_SCALE="${WHEELTEC_ODOM_Y_SCALE:-1.0}"
WHEELTEC_ODOM_Z_SCALE_POSITIVE="${WHEELTEC_ODOM_Z_SCALE_POSITIVE:-1.0}"
WHEELTEC_ODOM_Z_SCALE_NEGATIVE="${WHEELTEC_ODOM_Z_SCALE_NEGATIVE:-1.0}"
START_WHEEL_ODOM="${START_WHEEL_ODOM:-${DEFAULT_START_WHEEL_ODOM}}"
START_IMU_BIAS_CORRECTION="${START_IMU_BIAS_CORRECTION:-${START_WHEEL_ODOM}}"
ODOM_SERIAL_DEVICE="${ODOM_SERIAL_DEVICE:-/dev/odom}"
ODOM_SERIAL_BAUDRATE="${ODOM_SERIAL_BAUDRATE:-115200}"
ODOM_WHEEL_SEPARATION="${ODOM_WHEEL_SEPARATION:-0.7307}"
ODOM_WHEEL_DIAMETER="${ODOM_WHEEL_DIAMETER:-0.165}"
ODOM_GEAR_RATIO="${ODOM_GEAR_RATIO:-9.0}"
ODOM_ENCODER_PPR="${ODOM_ENCODER_PPR:-10000.0}"
ODOM_LEFT_WHEEL_SCALE="${ODOM_LEFT_WHEEL_SCALE:-0.9995}"
ODOM_RIGHT_WHEEL_SCALE="${ODOM_RIGHT_WHEEL_SCALE:-1.0010}"
ODOM_ANGULAR_VELOCITY_SIGN="${ODOM_ANGULAR_VELOCITY_SIGN:--1.0}"
START_MCORE_BRIDGE="${START_MCORE_BRIDGE:-${DEFAULT_START_MCORE_BRIDGE}}"
MCORE_SERVER_IP="${MCORE_SERVER_IP:-192.168.16.10}"
MCORE_SERVER_PORT="${MCORE_SERVER_PORT:-5001}"
MCORE_ENABLE_CMD_VEL="${MCORE_ENABLE_CMD_VEL:-${DEFAULT_MCORE_ENABLE_CMD_VEL}}"
MCORE_CMD_VEL_TOPIC="${MCORE_CMD_VEL_TOPIC:-/cmd_vel}"
MCORE_LINEAR_VELOCITY_SCALE="${MCORE_LINEAR_VELOCITY_SCALE:-1.0}"
MCORE_ANGULAR_VELOCITY_SCALE="${MCORE_ANGULAR_VELOCITY_SCALE:-1.0}"
MCORE_LINEAR_VELOCITY_SIGN="${MCORE_LINEAR_VELOCITY_SIGN:-1.0}"
MCORE_ANGULAR_VELOCITY_SIGN="${MCORE_ANGULAR_VELOCITY_SIGN:-1.0}"
MCORE_MAX_ABS_LINEAR_VELOCITY="${MCORE_MAX_ABS_LINEAR_VELOCITY:-0.0}"
MCORE_MAX_ABS_ANGULAR_VELOCITY="${MCORE_MAX_ABS_ANGULAR_VELOCITY:-0.0}"
START_STATION_BRIDGE="${START_STATION_BRIDGE:-${DEFAULT_START_STATION_BRIDGE}}"
START_DOCK_SUPPLY_MANAGER="${START_DOCK_SUPPLY_MANAGER:-${DEFAULT_START_DOCK_SUPPLY_MANAGER}}"
START_DOCKING_STACK="${START_DOCKING_STACK:-${DEFAULT_START_DOCKING_STACK}}"
STATION_SERVER_IP="${STATION_SERVER_IP:-10.2.0.200}"
STATION_SERVER_PORT="${STATION_SERVER_PORT:-5007}"
WAIT_FOR_SCAN_TOPIC="${WAIT_FOR_SCAN_TOPIC:-/scan}"
WAIT_FOR_IMU_TOPIC="${WAIT_FOR_IMU_TOPIC:-/imu}"
WAIT_FOR_CORRECTED_IMU_TOPIC="${WAIT_FOR_CORRECTED_IMU_TOPIC:-${DEFAULT_WAIT_FOR_CORRECTED_IMU_TOPIC}}"
WAIT_FOR_ODOM_TOPIC="${WAIT_FOR_ODOM_TOPIC:-/odom}"
ODOMETRY_HEALTH_IMU_TOPIC="${ODOMETRY_HEALTH_IMU_TOPIC:-${DEFAULT_ODOMETRY_HEALTH_IMU_TOPIC}}"
ODOMETRY_HEALTH_EKF_NODE_NAME="${ODOMETRY_HEALTH_EKF_NODE_NAME:-${DEFAULT_ODOMETRY_HEALTH_EKF_NODE_NAME}}"
REQUIRE_MCORE_BRIDGE_FOR_READINESS="${REQUIRE_MCORE_BRIDGE_FOR_READINESS:-false}"
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
  REQUIRE_TASK_READINESS_ON_STARTUP=0
  ALLOW_NO_ACTIVE_MAP_STARTUP=auto
  STARTUP_RELOCALIZE_ENABLE=true
  RUN_BACKEND_RUNTIME_SMOKE=1
  BACKEND_RUNTIME_SMOKE_ACTIONS=
  RUN_REVISION_DB_HEALTH_CHECK=0
  REVISION_DB_HEALTH_STRICT=0
  RUN_BACKEND_PRODUCTION_ACCEPTANCE=0
  BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=
  BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS=0
  START_WHEEL_ODOM=true
  ODOM_SERIAL_DEVICE=/dev/odom
  ODOM_WHEEL_SEPARATION=0.7307
  START_MCORE_BRIDGE=true
  MCORE_ENABLE_CMD_VEL=true
  MCORE_SERVER_IP=192.168.16.10
  CHASSIS_DRIVER=wheeltec_senior_diff
  WHEELTEC_SERIAL_DEVICE=/dev/wheeltec_controller
  WHEELTEC_CAR_MODE=senior_diff
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

allow_no_active_map_startup_enabled() {
  local value
  value="$(printf '%s' "${ALLOW_NO_ACTIVE_MAP_STARTUP:-auto}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|auto)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

startup_relocalize_enabled() {
  local value
  value="$(printf '%s' "${STARTUP_RELOCALIZE_ENABLE:-true}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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
    "bash -lc 'source \"${DORAEMON_ROS_SETUP}\"; source \"${DORAEMON_WORKSPACE_SETUP}\"; export ROS_MASTER_URI=${ROS_MASTER_URI}; unset ROS_IP ROS_HOSTNAME; exec roslaunch cleanrobot cleanrobot_base.launch start_robot_state_publisher:=${RUNTIME_START_ROBOT_STATE_PUBLISHER} start_joint_state_publisher:=${RUNTIME_START_JOINT_STATE_PUBLISHER} robot_description_path:=${RUNTIME_ROBOT_DESCRIPTION_PATH} start_lidar:=${RUNTIME_START_LIDAR} start_ahrs:=${RUNTIME_START_AHRS} start_depth_cameras:=${RUNTIME_START_DEPTH_CAMERAS} publish_base_footprint_to_base_link:=${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK} base_footprint_to_base_link_z:=${RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z} publish_base_footprint_to_gyro_link:=${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK}'"

  if [[ "${START_WHEELTEC_BASE}" == "true" ]]; then
    runtime_tmux_window "${TMUX_SESSION}" wheeltec "exec roslaunch robot_hw_bridge wheeltec_senior_diff_base.launch serial_device:=${WHEELTEC_SERIAL_DEVICE} serial_baudrate:=${WHEELTEC_SERIAL_BAUDRATE} car_mode:=${WHEELTEC_CAR_MODE} cmd_vel_topic:=${WHEELTEC_CMD_VEL_TOPIC} odom_frame_id:=${WHEELTEC_ODOM_FRAME_ID} robot_frame_id:=${WHEELTEC_ROBOT_FRAME_ID} gyro_frame_id:=${WHEELTEC_GYRO_FRAME_ID} publish_odom_tf:=${WHEELTEC_PUBLISH_ODOM_TF} odom_x_scale:=${WHEELTEC_ODOM_X_SCALE} odom_y_scale:=${WHEELTEC_ODOM_Y_SCALE} odom_z_scale_positive:=${WHEELTEC_ODOM_Z_SCALE_POSITIVE} odom_z_scale_negative:=${WHEELTEC_ODOM_Z_SCALE_NEGATIVE}"
  else
    runtime_log_status "[INFO] skip wheeltec chassis base: START_WHEELTEC_BASE=${START_WHEELTEC_BASE}"
  fi

  if [[ "${START_IMU_BIAS_CORRECTION}" == "true" ]]; then
    runtime_tmux_window "${TMUX_SESSION}" imu_bias "exec roslaunch wheel_speed_odom_bridge imu_bias_correction.launch"
  else
    runtime_log_status "[INFO] skip imu bias correction: START_IMU_BIAS_CORRECTION=${START_IMU_BIAS_CORRECTION}"
  fi

  if [[ "${START_WHEEL_ODOM}" == "true" ]]; then
    runtime_tmux_window "${TMUX_SESSION}" odom "exec roslaunch wheel_speed_odom_bridge wheel_speed_odom.launch serial_device:=${ODOM_SERIAL_DEVICE} serial_baudrate:=${ODOM_SERIAL_BAUDRATE} wheel_separation:=${ODOM_WHEEL_SEPARATION} wheel_diameter:=${ODOM_WHEEL_DIAMETER} gear_ratio:=${ODOM_GEAR_RATIO} encoder_pulses_per_motor_revolution:=${ODOM_ENCODER_PPR} left_wheel_scale:=${ODOM_LEFT_WHEEL_SCALE} right_wheel_scale:=${ODOM_RIGHT_WHEEL_SCALE} angular_velocity_sign:=${ODOM_ANGULAR_VELOCITY_SIGN}"
  else
    runtime_log_status "[INFO] skip wheel odom bridge: START_WHEEL_ODOM=${START_WHEEL_ODOM}; expecting another node to provide /odom"
  fi

  if [[ "${START_MCORE_BRIDGE}" == "true" || "${START_STATION_BRIDGE}" == "true" || "${START_DOCK_SUPPLY_MANAGER}" == "true" || "${START_DOCKING_STACK}" == "true" ]]; then
    runtime_tmux_window "${TMUX_SESSION}" hardware "exec roslaunch robot_hw_bridge hardware_bridges.launch enable_mcore_bridge:=${START_MCORE_BRIDGE} enable_station_bridge:=${START_STATION_BRIDGE} enable_dock_supply_manager:=${START_DOCK_SUPPLY_MANAGER} enable_docking_stack:=${START_DOCKING_STACK} mcore_server_ip:=${MCORE_SERVER_IP} mcore_server_port:=${MCORE_SERVER_PORT} mcore_enable_cmd_vel:=${MCORE_ENABLE_CMD_VEL} mcore_cmd_vel_topic:=${MCORE_CMD_VEL_TOPIC} mcore_linear_velocity_scale:=${MCORE_LINEAR_VELOCITY_SCALE} mcore_angular_velocity_scale:=${MCORE_ANGULAR_VELOCITY_SCALE} mcore_linear_velocity_sign:=${MCORE_LINEAR_VELOCITY_SIGN} mcore_angular_velocity_sign:=${MCORE_ANGULAR_VELOCITY_SIGN} mcore_max_abs_linear_velocity:=${MCORE_MAX_ABS_LINEAR_VELOCITY} mcore_max_abs_angular_velocity:=${MCORE_MAX_ABS_ANGULAR_VELOCITY} station_server_ip:=${STATION_SERVER_IP} station_server_port:=${STATION_SERVER_PORT}"
  else
    runtime_log_status "[INFO] skip legacy hardware bridges: all hardware bridge switches are false"
  fi
  runtime_tmux_window "${TMUX_SESSION}" nav "exec roslaunch cleanrobot mbf_nav.launch start_map_asset_service:=false enable_depth_obstacle_tracking:=${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING} enable_depth_up_cam:=${RUNTIME_ENABLE_DEPTH_UP_CAM} plan_db_path:=${PLAN_DB_PATH} ops_db_path:=${OPS_DB_PATH} maps_root:=${MAPS_ROOT} external_maps_root:=${EXTERNAL_MAPS_ROOT} robot_id:=${ROBOT_ID}"
  runtime_tmux_window "${TMUX_SESSION}" task "exec roslaunch coverage_task_manager task_system.launch task_auto_charge_enable:=${TASK_AUTO_CHARGE_ENABLE} executor_auto_charge_enable:=${EXECUTOR_AUTO_CHARGE_ENABLE} odometry_health_imu_topic:=${ODOMETRY_HEALTH_IMU_TOPIC} odometry_health_ekf_node_name:=${ODOMETRY_HEALTH_EKF_NODE_NAME} require_mcore_bridge_for_readiness:=${REQUIRE_MCORE_BRIDGE_FOR_READINESS} plan_db_path:=${PLAN_DB_PATH} ops_db_path:=${OPS_DB_PATH} maps_root:=${MAPS_ROOT} robot_id:=${ROBOT_ID}"

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
  runtime_restart_site_gateway_if_disconnected
}

handle_service_ready_without_startup_relocalization() {
  local active_map="${1:-}"
  local active_revision="${2:-}"

  runtime_log_status "[WARN] startup active-map relocalization disabled: STARTUP_RELOCALIZE_ENABLE=${STARTUP_RELOCALIZE_ENABLE}"
  runtime_log_status "[WARN] skip old active map relocalization: active_map=${active_map:-missing} active_revision=${active_revision:-missing}"
  runtime_log_status "[WARN] system is service-ready only; create/activate a map and localize before starting coverage tasks"
  runtime_log_status "[OK] runtime started in service-ready mode"
  runtime_log_status "tmux session: ${TMUX_SESSION}"
  runtime_log_status "frontend service session: ${FRONTEND_TMUX_SESSION}"
  runtime_log_status "ROS master: ${ROS_MASTER_URI}"
  runtime_log_status "frontend backend bridge: /clean_robot_server/app/map_server"
  runtime_log_status "SLAM status service: /clean_robot_server/app/get_slam_status"
  runtime_log_status "odometry status service: /clean_robot_server/app/get_odometry_status"
  runtime_log_status "task start service: /coverage_task_manager/app/exe_task_server"
  runtime_restart_site_gateway_if_disconnected
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
  runtime_log_status "chassis driver: ${CHASSIS_DRIVER}"
  runtime_log_status "workspace layout: ${DORAEMON_WORKSPACE_LAYOUT:-unknown}"
  runtime_ensure_frontend_service_session
  runtime_log_status "wait frontend roscore"
  runtime_wait_for_master 20

  clear_previous_runtime
  start_runtime_session
  runtime_cleanup_ros_nodes

  runtime_log_status "等待激光 / IMU / 里程计"
  runtime_wait_for_topic "${WAIT_FOR_SCAN_TOPIC}" 30
  runtime_wait_for_topic "${WAIT_FOR_IMU_TOPIC}" 30
  if [[ -n "${WAIT_FOR_CORRECTED_IMU_TOPIC}" ]]; then
    runtime_wait_for_topic "${WAIT_FOR_CORRECTED_IMU_TOPIC}" 30
  else
    runtime_log_status "[INFO] skip corrected IMU wait: WAIT_FOR_CORRECTED_IMU_TOPIC is empty"
  fi
  runtime_wait_for_topic "${WAIT_FOR_ODOM_TOPIC}" 40

  if [[ "${RUNTIME_START_DEPTH_CAMERAS}" == "true" ]]; then
    runtime_log_status "检查双奥比中光深度相机（仅提示，不阻塞启动）"
    runtime_warn_if_optional_topic_missing /gemini_cf/depth/image_raw "左奥比中光深度图像" 8 3
    runtime_warn_if_optional_topic_missing /gemini_nj/depth/image_raw "右奥比中光深度图像" 8 3
    runtime_warn_if_optional_topic_missing /gemini_cf/depth/points "左奥比中光点云" 8 3
    runtime_warn_if_optional_topic_missing /gemini_nj/depth/points "右奥比中光点云" 8 3
    runtime_log_status "[INFO] 若相机硬件暂未上电，可先继续使用系统，其余链路不会被阻塞"
  else
    runtime_log_status "[INFO] skip depth camera checks: RUNTIME_START_DEPTH_CAMERAS=${RUNTIME_START_DEPTH_CAMERAS}"
  fi

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

  local active_map
  local active_revision
  local relocalize_rc=0
  active_map="$(runtime_get_active_map_name)"
  active_revision="$(runtime_get_active_map_revision_id)"

  if ! startup_relocalize_enabled; then
    handle_service_ready_without_startup_relocalization "${active_map}" "${active_revision}"
    return 0
  fi

  runtime_log_status "重定位到当前活动地图"
  restart_localization_to_active_map "${active_map}" "${active_revision}" || relocalize_rc=$?
  if (( relocalize_rc != 0 )); then
    if allow_no_active_map_startup_enabled && [[ "${relocalize_rc}" == "2" ]]; then
      handle_degraded_startup_without_active_map
      return 0
    fi
    return "${relocalize_rc}"
  fi

  runtime_log_status "检查定位状态（仅提示，不阻塞启动）"
  runtime_warn_if_optional_topic_missing /tracked_pose "定位位姿 /tracked_pose" 10 3
  runtime_warn_if_localization_not_ready "${active_map}" "${active_revision}"
  runtime_warn_if_odometry_not_ready

  if [[ "${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING}" == "true" ]]; then
    runtime_log_status "检查深度避障链（仅提示，不阻塞启动）"
    runtime_warn_if_optional_node_missing /left/gs_node "左侧深度避障节点" 8
    runtime_warn_if_optional_node_missing /right/gs_node "右侧深度避障节点" 8
    runtime_warn_if_optional_topic_missing /left/obstacle_2d "左侧深度障碍输出" 8 3
    runtime_warn_if_optional_topic_missing /right/obstacle_2d "右侧深度障碍输出" 8 3
    runtime_log_status "[INFO] 若深度避障链未就绪，系统仍可启动；是否启用深度避障请以后续现场状态为准"
  else
    runtime_log_status "[INFO] skip depth obstacle checks: RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING=${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING}"
  fi

  runtime_log_status "等待任务系统 readiness"
  local readiness_rc=0
  runtime_wait_for_readiness "${READINESS_WAIT_TIMEOUT}" || readiness_rc=$?
  if (( readiness_rc != 0 )); then
    if [[ "${REQUIRE_TASK_READINESS_ON_STARTUP}" == "1" ]]; then
      return "${readiness_rc}"
    fi
    runtime_log_status "[WARN] 任务 readiness 暂未满足（本次不阻塞启动）"
    runtime_log_status "[WARN] system is service-ready only; task readiness stays unavailable until map/localization/safety gates are satisfied"
    runtime_log_status "[OK] runtime started in service-ready mode"
    runtime_log_status "tmux session: ${TMUX_SESSION}"
    runtime_log_status "frontend service session: ${FRONTEND_TMUX_SESSION}"
    runtime_log_status "ROS master: ${ROS_MASTER_URI}"
    runtime_log_status "frontend backend bridge: /clean_robot_server/app/map_server"
    runtime_log_status "SLAM status service: /clean_robot_server/app/get_slam_status"
    runtime_log_status "odometry status service: /clean_robot_server/app/get_odometry_status"
    runtime_log_status "task start service: /coverage_task_manager/app/exe_task_server"
    runtime_restart_site_gateway_if_disconnected
    return 0
  fi

  run_post_ready_acceptance_if_enabled

  runtime_log_status "[OK] runtime ready"
  runtime_log_status "tmux session: ${TMUX_SESSION}"
  runtime_log_status "frontend service session: ${FRONTEND_TMUX_SESSION}"
  runtime_log_status "ROS master: ${ROS_MASTER_URI}"
  runtime_log_status "frontend backend bridge: /clean_robot_server/app/map_server"
  runtime_log_status "SLAM status service: /clean_robot_server/app/get_slam_status"
  runtime_log_status "odometry status service: /clean_robot_server/app/get_odometry_status"
  runtime_log_status "task start service: /coverage_task_manager/app/exe_task_server"
  runtime_restart_site_gateway_if_disconnected
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

true
