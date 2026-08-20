#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PLAN_DB_PATH="${PLAN_DB_PATH:-/data/coverage/planning.db}"
OPS_DB_PATH="${OPS_DB_PATH:-/data/coverage/operations.db}"
ROBOT_ID="${ROBOT_ID:-local_robot}"
MAPS_ROOT="${MAPS_ROOT:-/data/maps}"
EXTERNAL_MAPS_ROOT="${EXTERNAL_MAPS_ROOT:-/data/maps/imports}"
MAP_TOPIC="${MAP_TOPIC:-/map}"
START_ROSBRIDGE="${START_ROSBRIDGE:-true}"
ROSBRIDGE_ADDRESS="${ROSBRIDGE_ADDRESS:-127.0.0.1}"
ROSBRIDGE_PORT="${ROSBRIDGE_PORT:-9090}"
START_MAP_ASSET_SERVICE="${START_MAP_ASSET_SERVICE:-true}"
ENABLE_SITE_EDITOR_SERVICE="${ENABLE_SITE_EDITOR_SERVICE:-true}"
ENABLE_RECT_ZONE_PLANNER="${ENABLE_RECT_ZONE_PLANNER:-false}"
ENABLE_MANUAL_DRIVE_SERVICE="${ENABLE_MANUAL_DRIVE_SERVICE:-false}"
DORAEMON_NO_ACTION_ACCEPTANCE="${DORAEMON_NO_ACTION_ACCEPTANCE:-true}"
DORAEMON_ACTION_TEST_APPROVED="${DORAEMON_ACTION_TEST_APPROVED:-false}"
FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-true}"
FRONTEND_BACKEND_ODOMETRY_HEALTH_IMU_TOPIC="${FRONTEND_BACKEND_ODOMETRY_HEALTH_IMU_TOPIC:-/imu_corrected}"
FRONTEND_BACKEND_ODOMETRY_HEALTH_EKF_NODE_NAME="${FRONTEND_BACKEND_ODOMETRY_HEALTH_EKF_NODE_NAME:-/wheel_speed_odom_ekf}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE:-${MANUAL_DRIVE_REQUIRE_ROLE:-false}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE:-${MANUAL_DRIVE_REQUIRE_SLAM_STATE:-false}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE:-${MANUAL_DRIVE_REQUIRE_TASK_STATE:-false}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE:-${MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE:-false}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS:-${MANUAL_DRIVE_REQUIRE_COMBINED_STATUS:-true}}"
FRONTEND_BACKEND_MANUAL_DRIVE_PUBLISH_HZ="${FRONTEND_BACKEND_MANUAL_DRIVE_PUBLISH_HZ:-${MANUAL_DRIVE_PUBLISH_HZ:-20.0}}"

if [[ "${ROSBRIDGE_ADDRESS}" != "127.0.0.1" || "${ROSBRIDGE_PORT}" != "9090" ]]; then
  echo "[ERROR] frontend backend rosbridge is fixed at 127.0.0.1:9090" >&2
  exit 1
fi

usage() {
  cat <<'EOF'
Usage: start_frontend_backend.sh

Official frontend/backend-only bringup entry for Doraemon.
By default this mode enables odometry_health and disables manual drive.
EOF
}

normalize_boolean_variable() {
  local variable_name="$1"
  local normalized
  normalized="$(printf '%s' "${!variable_name:-}" | tr '[:upper:]' '[:lower:]')"
  case "${normalized}" in
    1|true|yes|on)
      printf -v "${variable_name}" '%s' true
      ;;
    0|false|no|off)
      printf -v "${variable_name}" '%s' false
      ;;
    *)
      echo "[ERROR] ${variable_name} must be an explicit boolean" >&2
      return 1
      ;;
  esac
}

validate_manual_drive_mode() {
  local variable_name
  for variable_name in \
    ENABLE_MANUAL_DRIVE_SERVICE \
    DORAEMON_NO_ACTION_ACCEPTANCE \
    DORAEMON_ACTION_TEST_APPROVED \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS; do
    normalize_boolean_variable "${variable_name}"
  done

  if [[ "${DORAEMON_NO_ACTION_ACCEPTANCE}" == "true" ]]; then
    if [[ "${DORAEMON_ACTION_TEST_APPROVED}" != "false" ]]; then
      echo "[ERROR] no-action frontend backend requires DORAEMON_ACTION_TEST_APPROVED=false" >&2
      return 1
    fi
    ENABLE_MANUAL_DRIVE_SERVICE=false
    return 0
  fi

  if [[ "${DORAEMON_ACTION_TEST_APPROVED}" != "true" ]]; then
    echo "[ERROR] action-capable frontend backend requires DORAEMON_ACTION_TEST_APPROVED=true" >&2
    return 1
  fi
  [[ "${ENABLE_MANUAL_DRIVE_SERVICE}" == "true" ]] || return 0

  for variable_name in \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE \
    FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE; do
    if [[ "${!variable_name}" != "false" ]]; then
      echo "[ERROR] remote-control manual drive requires ${variable_name}=false" >&2
      return 1
    fi
  done
  if [[ "${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS}" != "true" ]]; then
    echo "[ERROR] manual drive requires live platform/E-stop status" >&2
    return 1
  fi
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

validate_manual_drive_mode

source "${SCRIPT_DIR}/source_slam_runtime_env.sh"
source_slam_runtime_env "${REPO_ROOT}"

exec roslaunch coverage_planner frontend_editor_backend.launch \
  plan_db_path:="${PLAN_DB_PATH}" \
  ops_db_path:="${OPS_DB_PATH}" \
  robot_id:="${ROBOT_ID}" \
  maps_root:="${MAPS_ROOT}" \
  external_maps_root:="${EXTERNAL_MAPS_ROOT}" \
  map_topic:="${MAP_TOPIC}" \
  start_rosbridge:="${START_ROSBRIDGE}" \
  rosbridge_address:="${ROSBRIDGE_ADDRESS}" \
  rosbridge_port:="${ROSBRIDGE_PORT}" \
  start_map_asset_service:="${START_MAP_ASSET_SERVICE}" \
  enable_site_editor_service:="${ENABLE_SITE_EDITOR_SERVICE}" \
  enable_rect_zone_planner:="${ENABLE_RECT_ZONE_PLANNER}" \
  enable_manual_drive_service:="${ENABLE_MANUAL_DRIVE_SERVICE}" \
  manual_drive_no_action_acceptance:="${DORAEMON_NO_ACTION_ACCEPTANCE}" \
  manual_drive_action_test_approved:="${DORAEMON_ACTION_TEST_APPROVED}" \
  manual_drive_require_role:="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE}" \
  manual_drive_require_slam_state:="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE}" \
  manual_drive_require_task_state:="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE}" \
  manual_drive_require_odometry_state:="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE}" \
  manual_drive_require_combined_status:="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS}" \
  manual_drive_publish_hz:="${FRONTEND_BACKEND_MANUAL_DRIVE_PUBLISH_HZ}" \
  enable_odometry_health:="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH}" \
  odometry_health_imu_topic:="${FRONTEND_BACKEND_ODOMETRY_HEALTH_IMU_TOPIC}" \
  odometry_health_ekf_node_name:="${FRONTEND_BACKEND_ODOMETRY_HEALTH_EKF_NODE_NAME}"
