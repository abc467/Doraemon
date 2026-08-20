#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_vehicle_identity.sh"

DORAEMON_PRODUCTION_ENTRY="false"
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  DORAEMON_PRODUCTION_ENTRY="true"
  if [[ "${DORAEMON_RUNTIME_CONFIG_FILE:-/etc/doraemon/runtime.env}" != "/etc/doraemon/runtime.env" ]]; then
    echo "[ERROR] commercial runtime config path is fixed at /etc/doraemon/runtime.env" >&2
    exit 1
  fi
  DORAEMON_RUNTIME_CONFIG_FILE="/etc/doraemon/runtime.env"
  if [[ "$(stat -c '%U %a' "${DORAEMON_RUNTIME_CONFIG_FILE}" 2>/dev/null || true)" != "root 640" ]]; then
    echo "[ERROR] /etc/doraemon/runtime.env must be root-owned mode 0640" >&2
    exit 1
  fi
  commercial_validate_runtime_env_file "${DORAEMON_RUNTIME_CONFIG_FILE}" || exit 1
else
  DORAEMON_RUNTIME_CONFIG_FILE="${DORAEMON_RUNTIME_CONFIG_FILE:-${REPO_ROOT}/config/runtime.a26022.env}"
fi
readonly SCRIPT_DIR REPO_ROOT DORAEMON_PRODUCTION_ENTRY
DORAEMON_RUNTIME_CONFIG_LOADED="false"
if [[ -f "${DORAEMON_RUNTIME_CONFIG_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${DORAEMON_RUNTIME_CONFIG_FILE}"
  set +a
  DORAEMON_RUNTIME_CONFIG_LOADED="true"
fi
if [[ "${DORAEMON_PRODUCTION_ENTRY}" == "true" ]]; then
  commercial_pin_storage_paths
  export DORAEMON_REPO_ROOT="${REPO_ROOT}"
  export DORAEMON_RUNTIME_CONFIG_FILE="/etc/doraemon/runtime.env"
  export DORAEMON_ROS_SETUP="/opt/ros/noetic/setup.bash"
  export ROS_HOME="/var/lib/doraemon/ros"
  unset LD_PRELOAD LD_AUDIT LD_ORIGIN_PATH LIBRARY_PATH
fi

LOG_DIR="${LOG_DIR:-/var/log/doraemon/startup}"
STATUS_LOG="${STATUS_LOG:-${LOG_DIR}/startup_status.log}"
RESTART_LOCALIZATION_OUT="${RESTART_LOCALIZATION_OUT:-${LOG_DIR}/restart_localization.out}"
TMUX_SESSION="${TMUX_SESSION:-doraemon_task_ready}"
FRONTEND_TMUX_SESSION="${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}"
ROBOT_ID="${ROBOT_ID:-}"
PLAN_DB_PATH="${PLAN_DB_PATH:-/data/coverage/planning.db}"
OPS_DB_PATH="${OPS_DB_PATH:-/data/coverage/operations.db}"
MAPS_ROOT="${MAPS_ROOT:-/data/maps}"
EXTERNAL_MAPS_ROOT="${EXTERNAL_MAPS_ROOT:-/data/maps/imports}"
MAP_TOPIC="${MAP_TOPIC:-/map}"
START_ROSBRIDGE="${START_ROSBRIDGE:-true}"
ROSBRIDGE_ADDRESS="${ROSBRIDGE_ADDRESS:-127.0.0.1}"
ROSBRIDGE_PORT="${ROSBRIDGE_PORT:-9090}"
START_MAP_ASSET_SERVICE="${START_MAP_ASSET_SERVICE:-true}"
ENABLE_SITE_EDITOR_SERVICE="${ENABLE_SITE_EDITOR_SERVICE:-true}"
ENABLE_RECT_ZONE_PLANNER="${ENABLE_RECT_ZONE_PLANNER:-false}"
FRONTEND_DIR="${FRONTEND_DIR:-}"
START_FRONTEND_DEV="${START_FRONTEND_DEV:-0}"
RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE="${RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE:-false}"
DORAEMON_NO_ACTION_ACCEPTANCE="${DORAEMON_NO_ACTION_ACCEPTANCE:-true}"
DORAEMON_ACTION_TEST_APPROVED="${DORAEMON_ACTION_TEST_APPROVED:-false}"
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
  mcore_tcp|tcp_mcore|new_mcore)
    CHASSIS_DRIVER="mcore_tcp"
    ;;
  mcore_serial|serial_mcore)
    CHASSIS_DRIVER="mcore_serial"
    ;;
  legacy|legacy_mcore|mcore)
    CHASSIS_DRIVER="legacy_mcore"
    ;;
  wheeltec|wheeltec_senior_diff|senior_diff)
    CHASSIS_DRIVER="wheeltec_senior_diff"
    ;;
esac
DEFAULT_RUNTIME_ROBOT_DESCRIPTION_PATH="${REPO_ROOT}/src/cleanrobot_description/urdf/a26022_clean_robot.urdf"
DEFAULT_RUNTIME_START_AHRS="false"
DEFAULT_RUNTIME_START_JOINT_STATE_PUBLISHER="false"
DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK="false"
DEFAULT_RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z="0.0"
DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK="false"
# A26022 mcore robots carry two front/side Orbbec depth cameras. Start the
# camera drivers and the depth obstacle chain by default; chassis profiles that
# do not have cameras override these below.
DEFAULT_RUNTIME_START_DEPTH_CAMERAS="true"
DEFAULT_RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING="true"
DEFAULT_START_WHEELTEC_BASE="false"
DEFAULT_START_WHEEL_ODOM="true"
DEFAULT_START_MCORE_BRIDGE="false"
DEFAULT_START_MCORE_VELOCITY_SENDER="true"
DEFAULT_MCORE_ENABLE_CMD_VEL="false"
DEFAULT_START_STATION_BRIDGE="true"
DEFAULT_START_DOCK_SUPPLY_MANAGER="true"
DEFAULT_START_DOCKING_STACK="true"
DEFAULT_WAIT_FOR_CORRECTED_IMU_TOPIC=""
DEFAULT_ODOMETRY_HEALTH_IMU_TOPIC="/imu"
DEFAULT_ODOMETRY_HEALTH_EKF_NODE_NAME="/wheel_speed_odom_ekf"
DEFAULT_WHEELTEC_SERIAL_DEVICE="/dev/wheeltec_controller"
WHEELTEC_BY_ID_DEVICE="/dev/serial/by-id/usb-WCH.CN_USB_Single_Serial_0002-if00"
DEFAULT_ODOM_SERIAL_DEVICE="/dev/wheel_odom"
ODOM_BY_ID_DEVICE="/dev/serial/by-id/usb-1a86_USB2.0-Serial-if00-port0"
if [[ ! -e "${DEFAULT_ODOM_SERIAL_DEVICE}" && -e "${ODOM_BY_ID_DEVICE}" ]]; then
  DEFAULT_ODOM_SERIAL_DEVICE="${ODOM_BY_ID_DEVICE}"
fi

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
RUNTIME_LIDAR_NTP_IP="${RUNTIME_LIDAR_NTP_IP:-192.168.127.88}"
RUNTIME_LIDAR_NTP_PORT="${RUNTIME_LIDAR_NTP_PORT:-5678}"
RUNTIME_LIDAR_NTP_ENABLE="${RUNTIME_LIDAR_NTP_ENABLE:--1}"
RUNTIME_START_IMU="${RUNTIME_START_IMU:-true}"
RUNTIME_IMU_PORT="${RUNTIME_IMU_PORT:-/dev/imu}"
RUNTIME_IMU_BAUD="${RUNTIME_IMU_BAUD:-115200}"
RUNTIME_IMU_SLAVE_ADDRESS="${RUNTIME_IMU_SLAVE_ADDRESS:-1}"
RUNTIME_IMU_PUBLISH_RAW="${RUNTIME_IMU_PUBLISH_RAW:-true}"
RUNTIME_IMU_PUBLISH_DURING_CALIBRATION="${RUNTIME_IMU_PUBLISH_DURING_CALIBRATION:-true}"
RUNTIME_START_AHRS="${RUNTIME_START_AHRS:-${DEFAULT_RUNTIME_START_AHRS}}"
RUNTIME_START_DEPTH_CAMERAS="${RUNTIME_START_DEPTH_CAMERAS:-${DEFAULT_RUNTIME_START_DEPTH_CAMERAS}}"
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER="${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER:-}"
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER="${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER:-}"
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER="${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER:-}"
RUNTIME_ORBBEC_CAMERA1_USB_PORT="${RUNTIME_ORBBEC_CAMERA1_USB_PORT:-}"
RUNTIME_ORBBEC_CAMERA2_USB_PORT="${RUNTIME_ORBBEC_CAMERA2_USB_PORT:-}"
RUNTIME_ORBBEC_CAMERA3_USB_PORT="${RUNTIME_ORBBEC_CAMERA3_USB_PORT:-}"
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS="${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS:-true}"
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES="${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES:-true}"
RUNTIME_DEPTH_CAMERA_READY_SAMPLES="${RUNTIME_DEPTH_CAMERA_READY_SAMPLES:-3}"
RUNTIME_DEPTH_CAMERA_READY_TIMEOUT="${RUNTIME_DEPTH_CAMERA_READY_TIMEOUT:-30}"
RUNTIME_DEPTH_CAMERA_INTER_START_DELAY="${RUNTIME_DEPTH_CAMERA_INTER_START_DELAY:-2}"
RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE="${RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE:-true}"
RUNTIME_DEPTH_CAMERA_STALE_TIMEOUT="${RUNTIME_DEPTH_CAMERA_STALE_TIMEOUT:-3.0}"
RUNTIME_DEPTH_CAMERA_RECOVERY_COOLDOWN="${RUNTIME_DEPTH_CAMERA_RECOVERY_COOLDOWN:-60.0}"
# Internal transaction state, intentionally not configurable: a boot may
# restart the whole camera chain at most once, regardless of whether the first
# failure happens during sequential launch or during the later hard gate.
DEPTH_CAMERA_STARTUP_RECOVERY_USED=0
RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING="${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING:-${DEFAULT_RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING}}"
RUNTIME_ENABLE_DEPTH_LEFT_CAM="${RUNTIME_ENABLE_DEPTH_LEFT_CAM:-true}"
RUNTIME_ENABLE_DEPTH_RIGHT_CAM="${RUNTIME_ENABLE_DEPTH_RIGHT_CAM:-true}"
RUNTIME_ENABLE_DEPTH_UP_CAM="${RUNTIME_ENABLE_DEPTH_UP_CAM:-false}"
RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK="${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK:-${DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK}}"
RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z="${RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z:-${DEFAULT_RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z}}"
RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK="${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK:-${DEFAULT_RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK}}"
RUNTIME_BASE_EXTRA_ARGS="${RUNTIME_BASE_EXTRA_ARGS:-}"
START_WHEELTEC_BASE="${START_WHEELTEC_BASE:-${DEFAULT_START_WHEELTEC_BASE}}"
WHEELTEC_SERIAL_DEVICE="${WHEELTEC_SERIAL_DEVICE:-${DEFAULT_WHEELTEC_SERIAL_DEVICE}}"
WHEELTEC_BASE_EXTRA_ARGS="${WHEELTEC_BASE_EXTRA_ARGS:-}"
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
WHEEL_ODOM_EXTRA_ARGS="${WHEEL_ODOM_EXTRA_ARGS:-}"
START_IMU_BIAS_CORRECTION="${START_IMU_BIAS_CORRECTION:-false}"
ODOM_SERIAL_DEVICE="${ODOM_SERIAL_DEVICE:-${DEFAULT_ODOM_SERIAL_DEVICE}}"
ODOM_SERIAL_BAUDRATE="${ODOM_SERIAL_BAUDRATE:-115200}"
ODOM_PROTOCOL_MODE="${ODOM_PROTOCOL_MODE:-framed_434c}"
ODOM_USE_DEVICE_TIMESTAMP="${ODOM_USE_DEVICE_TIMESTAMP:-false}"
ODOM_PUBLISH_RAW_ODOM_TF="${ODOM_PUBLISH_RAW_ODOM_TF:-false}"
ODOM_FRAME_ID="${ODOM_FRAME_ID:-odom}"
ODOM_CHILD_FRAME_ID="${ODOM_CHILD_FRAME_ID:-base_footprint}"
ODOM_WHEEL_SEPARATION="${ODOM_WHEEL_SEPARATION:-0.46}"
ODOM_WHEEL_DIAMETER="${ODOM_WHEEL_DIAMETER:-0.165}"
ODOM_GEAR_RATIO="${ODOM_GEAR_RATIO:-9.0}"
ODOM_ENCODER_PPR="${ODOM_ENCODER_PPR:-10000.0}"
ODOM_LEFT_ENCODER_SIGN="${ODOM_LEFT_ENCODER_SIGN:-1.0}"
ODOM_RIGHT_ENCODER_SIGN="${ODOM_RIGHT_ENCODER_SIGN:--1.0}"
ODOM_LEFT_WHEEL_SCALE="${ODOM_LEFT_WHEEL_SCALE:-1.0}"
ODOM_RIGHT_WHEEL_SCALE="${ODOM_RIGHT_WHEEL_SCALE:-1.0}"
ODOM_ANGULAR_VELOCITY_SIGN="${ODOM_ANGULAR_VELOCITY_SIGN:--1.0}"
START_MCORE_BRIDGE="${START_MCORE_BRIDGE:-${DEFAULT_START_MCORE_BRIDGE}}"
START_MCORE_VELOCITY_SENDER="${START_MCORE_VELOCITY_SENDER:-${DEFAULT_START_MCORE_VELOCITY_SENDER}}"
MCORE_TRANSPORT="${MCORE_TRANSPORT:-tcp}"
MCORE_SERIAL_DEVICE="${MCORE_SERIAL_DEVICE:-/dev/mcore}"
MCORE_VELOCITY_EXTRA_ARGS="${MCORE_VELOCITY_EXTRA_ARGS:-}"
MCORE_SERIAL_BAUDRATE="${MCORE_SERIAL_BAUDRATE:-115200}"
MCORE_SERVER_IP="${MCORE_SERVER_IP:-192.168.127.10}"
MCORE_SERVER_PORT="${MCORE_SERVER_PORT:-5001}"
MCORE_TCP_HOST="${MCORE_TCP_HOST:-${MCORE_SERVER_IP}}"
MCORE_TCP_PORT="${MCORE_TCP_PORT:-8080}"
MCORE_ENABLE_CMD_VEL="${MCORE_ENABLE_CMD_VEL:-${DEFAULT_MCORE_ENABLE_CMD_VEL}}"
MCORE_CMD_VEL_TOPIC="${MCORE_CMD_VEL_TOPIC:-/cmd_vel}"
MCORE_LINEAR_VELOCITY_SCALE="${MCORE_LINEAR_VELOCITY_SCALE:-1000.0}"
MCORE_ANGULAR_VELOCITY_SCALE="${MCORE_ANGULAR_VELOCITY_SCALE:-1000.0}"
MCORE_LINEAR_VELOCITY_SIGN="${MCORE_LINEAR_VELOCITY_SIGN:-1.0}"
MCORE_ANGULAR_VELOCITY_SIGN="${MCORE_ANGULAR_VELOCITY_SIGN:-1.0}"
MCORE_MAX_ABS_LINEAR_VELOCITY="${MCORE_MAX_ABS_LINEAR_VELOCITY:-0.0}"
MCORE_MAX_ABS_ANGULAR_VELOCITY="${MCORE_MAX_ABS_ANGULAR_VELOCITY:-0.0}"
MCORE_ENABLE_TX_LOG="${MCORE_ENABLE_TX_LOG:-false}"
MCORE_ENABLE_RX_LOG="${MCORE_ENABLE_RX_LOG:-true}"
START_STATION_BRIDGE="${START_STATION_BRIDGE:-${DEFAULT_START_STATION_BRIDGE}}"
START_DOCK_SUPPLY_MANAGER="${START_DOCK_SUPPLY_MANAGER:-${DEFAULT_START_DOCK_SUPPLY_MANAGER}}"
START_DOCKING_STACK="${START_DOCKING_STACK:-${DEFAULT_START_DOCKING_STACK}}"
HARDWARE_BRIDGES_EXTRA_ARGS="${HARDWARE_BRIDGES_EXTRA_ARGS:-}"
ENABLE_MANUAL_DRIVE_SERVICE="${ENABLE_MANUAL_DRIVE_SERVICE:-false}"
MANUAL_DRIVE_REQUIRE_ROLE="${MANUAL_DRIVE_REQUIRE_ROLE:-false}"
MANUAL_DRIVE_REQUIRE_SLAM_STATE="${MANUAL_DRIVE_REQUIRE_SLAM_STATE:-false}"
MANUAL_DRIVE_REQUIRE_TASK_STATE="${MANUAL_DRIVE_REQUIRE_TASK_STATE:-false}"
MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE="${MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE:-false}"
MANUAL_DRIVE_REQUIRE_COMBINED_STATUS="${MANUAL_DRIVE_REQUIRE_COMBINED_STATUS:-true}"
MANUAL_DRIVE_PUBLISH_HZ="${MANUAL_DRIVE_PUBLISH_HZ:-20.0}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE:-${MANUAL_DRIVE_REQUIRE_ROLE}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE:-${MANUAL_DRIVE_REQUIRE_SLAM_STATE}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE:-${MANUAL_DRIVE_REQUIRE_TASK_STATE}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE:-${MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE}}"
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS="${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS:-${MANUAL_DRIVE_REQUIRE_COMBINED_STATUS}}"
FRONTEND_BACKEND_MANUAL_DRIVE_PUBLISH_HZ="${FRONTEND_BACKEND_MANUAL_DRIVE_PUBLISH_HZ:-${MANUAL_DRIVE_PUBLISH_HZ}}"
STATION_SERVER_IP="${STATION_SERVER_IP:-192.168.127.12}"
STATION_SERVER_PORT="${STATION_SERVER_PORT:-5007}"
WAIT_FOR_SCAN_TOPIC="${WAIT_FOR_SCAN_TOPIC:-/scan}"
WAIT_FOR_IMU_TOPIC="${WAIT_FOR_IMU_TOPIC:-/imu}"
WAIT_FOR_CORRECTED_IMU_TOPIC="${WAIT_FOR_CORRECTED_IMU_TOPIC:-${DEFAULT_WAIT_FOR_CORRECTED_IMU_TOPIC}}"
WAIT_FOR_ODOM_TOPIC="${WAIT_FOR_ODOM_TOPIC:-/odom}"
ODOMETRY_HEALTH_IMU_TOPIC="${ODOMETRY_HEALTH_IMU_TOPIC:-${DEFAULT_ODOMETRY_HEALTH_IMU_TOPIC}}"
ODOMETRY_HEALTH_EKF_NODE_NAME="${ODOMETRY_HEALTH_EKF_NODE_NAME:-${DEFAULT_ODOMETRY_HEALTH_EKF_NODE_NAME}}"
REQUIRE_MCORE_BRIDGE_FOR_READINESS="${REQUIRE_MCORE_BRIDGE_FOR_READINESS:-false}"
TASK_AUTO_CHARGE_ENABLE="${TASK_AUTO_CHARGE_ENABLE:-false}"
# The executor's legacy absolute-dock path has no vehicle-bound calibration
# gate. Commercial runtime never exposes that bypass; task_manager owns all
# automatic return-to-dock dispatches.
EXECUTOR_AUTO_CHARGE_ENABLE=false
ACTUATOR_DEBUG_REQUIRE_SAFETY_STATUS="${ACTUATOR_DEBUG_REQUIRE_SAFETY_STATUS:-true}"
DEFAULT_RETURN_TO_DOCK_ON_FINISH="${DEFAULT_RETURN_TO_DOCK_ON_FINISH:-false}"
DOCK_TARGET_DIST="${DOCK_TARGET_DIST:-0.780}"
DOCK_XY_TOLERANCE="${DOCK_XY_TOLERANCE:-0.005}"
DOCK_YAW_TOLERANCE="${DOCK_YAW_TOLERANCE:-0.04}"
DOCK_POSE_SCORE_THRESH="${DOCK_POSE_SCORE_THRESH:-0.00012}"
DOCK_CALIBRATION_STORAGE_PATH="${DOCK_CALIBRATION_STORAGE_PATH:-/data/coverage/dock_calibration.yaml}"
MECHANICAL_CONNECT_ENABLE="${MECHANICAL_CONNECT_ENABLE:-false}"
SKIP_PRECISE_DOCKING_IF_STATION_IN_PLACE="${SKIP_PRECISE_DOCKING_IF_STATION_IN_PLACE:-false}"
DIRECT_CHARGE_AFTER_PRECISE_DOCKING="${DIRECT_CHARGE_AFTER_PRECISE_DOCKING:-true}"
CHARGE_VOLTAGE_CONFIRM_ENABLE="${CHARGE_VOLTAGE_CONFIRM_ENABLE:-false}"
DOCK_SUPPLY_ENABLE_DRAIN="${DOCK_SUPPLY_ENABLE_DRAIN:-true}"
DOCK_SUPPLY_ENABLE_REFILL="${DOCK_SUPPLY_ENABLE_REFILL:-true}"
DOCK_SUPPLY_DRAIN_TIMEOUT_S="${DOCK_SUPPLY_DRAIN_TIMEOUT_S:-600.0}"
DOCK_SUPPLY_TARGET_CLEAN_LEVEL="${DOCK_SUPPLY_TARGET_CLEAN_LEVEL:-74}"
DOCK_SUPPLY_REFILL_TIMEOUT_S="${DOCK_SUPPLY_REFILL_TIMEOUT_S:-600.0}"
DOCK_SUPPLY_REFILL_SETTLE_S="${DOCK_SUPPLY_REFILL_SETTLE_S:-20.0}"
DOCK_SUPPLY_COMBINED_STATUS_WAIT_S="${DOCK_SUPPLY_COMBINED_STATUS_WAIT_S:-5.0}"
DOCK_SUPPLY_COMBINED_STATUS_STALE_TIMEOUT_S="${DOCK_SUPPLY_COMBINED_STATUS_STALE_TIMEOUT_S:-3.0}"
AUTO_CHARGE_TARGET_SOC="${AUTO_CHARGE_TARGET_SOC:-${TARGET_SOC:-0.95}}"
AUTO_CHARGE_LOW_SOC="${AUTO_CHARGE_LOW_SOC:-${LOW_SOC:-0.15}}"
AUTO_CHARGE_RESUME_SOC="${AUTO_CHARGE_RESUME_SOC:-${RESUME_SOC:-0.95}}"
AUTO_CHARGE_REARM_SOC="${AUTO_CHARGE_REARM_SOC:-${REARM_SOC:-0.95}}"
AUTO_CHARGE_MONITOR_ENABLE="${AUTO_CHARGE_MONITOR_ENABLE:-false}"
AUTO_CHARGE_MONITOR_RESET_ON_START="${AUTO_CHARGE_MONITOR_RESET_ON_START:-false}"
AUTO_CHARGE_MONITOR_COUNT_AUTO_ONLY="${AUTO_CHARGE_MONITOR_COUNT_AUTO_ONLY:-true}"
AUTO_CHARGE_MONITOR_RECOVERY_ENABLE="${AUTO_CHARGE_MONITOR_RECOVERY_ENABLE:-false}"
# Legacy contact_jog publishes /cmd_vel outside TaskManager's calibration gate.
AUTO_CHARGE_MONITOR_RECOVERY_STRATEGY=redock
AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S="${AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S:-180.0}"
AUTO_CHARGE_MONITOR_RECOVERY_MIN_SOC_DELTA="${AUTO_CHARGE_MONITOR_RECOVERY_MIN_SOC_DELTA:-0.001}"
AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS="${AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS:-2}"
AUTO_CHARGE_MONITOR_RECOVERY_BACK_DISTANCE_M="${AUTO_CHARGE_MONITOR_RECOVERY_BACK_DISTANCE_M:-0.20}"
AUTO_CHARGE_MONITOR_RECOVERY_FORWARD_DISTANCE_M="${AUTO_CHARGE_MONITOR_RECOVERY_FORWARD_DISTANCE_M:-0.205}"
AUTO_CHARGE_MONITOR_RECOVERY_SPEED_MPS="${AUTO_CHARGE_MONITOR_RECOVERY_SPEED_MPS:-0.03}"
AUTO_CHARGE_MONITOR_RECOVERY_TOGGLE_CHARGE="${AUTO_CHARGE_MONITOR_RECOVERY_TOGGLE_CHARGE:-false}"
AUTO_CHARGE_MONITOR_RECOVERY_REDOCK_SERVICE="${AUTO_CHARGE_MONITOR_RECOVERY_REDOCK_SERVICE:-/coverage_task_manager/auto_charge_redock}"
AUTO_CHARGE_MONITOR_RECOVERY_EXHAUSTED_SERVICE="${AUTO_CHARGE_MONITOR_RECOVERY_EXHAUSTED_SERVICE:-/coverage_task_manager/auto_charge_recovery_exhausted}"
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
  DORAEMON_NO_ACTION_ACCEPTANCE=true
  DORAEMON_ACTION_TEST_APPROVED=false
  ENABLE_MANUAL_DRIVE_SERVICE=false
  START_WHEEL_ODOM=true
  ODOM_SERIAL_DEVICE=/dev/wheel_odom
  ODOM_FRAME_ID=odom
  ODOM_CHILD_FRAME_ID=base_footprint
  START_MCORE_VELOCITY_SENDER=true
  MCORE_TRANSPORT=tcp
  MCORE_TCP_HOST=192.168.127.10
  MCORE_TCP_PORT=8080
  CHASSIS_DRIVER=wheeltec_senior_diff
  WHEELTEC_SERIAL_DEVICE=/dev/wheeltec_controller
  RUNTIME_BASE_EXTRA_ARGS=
  WHEEL_ODOM_EXTRA_ARGS=
  MCORE_VELOCITY_EXTRA_ARGS=
  WHEELTEC_BASE_EXTRA_ARGS=
  HARDWARE_BRIDGES_EXTRA_ARGS=
  STATION_SERVER_IP=192.168.127.12
  STATION_SERVER_PORT=5007
  DOCK_TARGET_DIST=0.780
  DOCK_XY_TOLERANCE=0.005
  DOCK_YAW_TOLERANCE=0.04
  DOCK_POSE_SCORE_THRESH=0.00012
  DOCK_CALIBRATION_STORAGE_PATH=/data/coverage/dock_calibration.yaml
  DOCK_SUPPLY_ENABLE_DRAIN=true
  DOCK_SUPPLY_ENABLE_REFILL=true
  DOCK_SUPPLY_DRAIN_TIMEOUT_S=600.0
  DOCK_SUPPLY_TARGET_CLEAN_LEVEL=74
  DOCK_SUPPLY_REFILL_TIMEOUT_S=600.0
  DOCK_SUPPLY_REFILL_SETTLE_S=20.0
  AUTO_CHARGE_TARGET_SOC=0.95
  AUTO_CHARGE_LOW_SOC=0.15
  AUTO_CHARGE_RESUME_SOC=0.95
  AUTO_CHARGE_REARM_SOC=0.95
  AUTO_CHARGE_MONITOR_RECOVERY_STRATEGY=redock
  AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S=180.0
  AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS=2
  AUTO_CHARGE_MONITOR_RECOVERY_BACK_DISTANCE_M=0.20
  AUTO_CHARGE_MONITOR_RECOVERY_FORWARD_DISTANCE_M=0.205
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
  local parsed_words=( ${raw_words} )
  target_ref+=("${parsed_words[@]}")
}

join_shell_words() {
  local -n words_ref="$1"
  local joined=""
  printf -v joined '%q ' "${words_ref[@]}"
  printf '%s' "${joined% }"
}

normalize_boolean_variable() {
  local variable_name="$1"
  local raw_value="${!variable_name:-}"
  local normalized
  normalized="$(printf '%s' "${raw_value}" | tr '[:upper:]' '[:lower:]')"
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

runtime_is_positive_finite_number() {
  local raw_value="${1:-}"
  LC_ALL=C awk -v raw="${raw_value}" '
    BEGIN {
      if (raw !~ /^\+?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$/) {
        exit 1
      }
      number = raw + 0
      rendered = sprintf("%.17g", number)
      if (rendered ~ /[Ii][Nn][Ff]|[Nn][Aa][Nn]/) {
        exit 1
      }
      exit !(number > 0)
    }
  '
}

runtime_is_unit_sign() {
  local raw_value="${1:-}"
  LC_ALL=C awk -v raw="${raw_value}" '
    BEGIN {
      if (raw !~ /^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$/) {
        exit 1
      }
      number = raw + 0
      rendered = sprintf("%.17g", number)
      if (rendered ~ /[Ii][Nn][Ff]|[Nn][Aa][Nn]/) {
        exit 1
      }
      exit !(number == -1 || number == 1)
    }
  '
}

runtime_is_positive_integer() {
  [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

runtime_value_is_placeholder() {
  commercial_value_is_placeholder "${1:-}"
}

validate_commercial_vehicle_identity() {
  if runtime_value_is_placeholder "${ROBOT_ID}" || [[ "${ROBOT_ID}" == "local_robot" ]]; then
    echo "[ERROR] ROBOT_ID must be the explicit vehicle asset identifier" >&2
    return 1
  fi
  if runtime_value_is_placeholder "${DORAEMON_A_BOX_IFACE:-}"; then
    echo "[ERROR] DORAEMON_A_BOX_IFACE must be the explicit internal wired interface" >&2
    return 1
  fi
  if [[ "${ROSBRIDGE_ADDRESS}" != "127.0.0.1" ]]; then
    echo "[ERROR] commercial rosbridge must bind only to 127.0.0.1" >&2
    return 1
  fi
  if [[ "${ROSBRIDGE_PORT}" != "9090" || "${START_ROSBRIDGE}" != "true" ]]; then
    echo "[ERROR] commercial rosbridge must be enabled on fixed loopback port 9090" >&2
    return 1
  fi
  if [[ "${START_FRONTEND_DEV}" != "0" || -n "${FRONTEND_DIR}" ]]; then
    echo "[ERROR] mutable frontend development mode is forbidden in the commercial runtime" >&2
    return 1
  fi

  normalize_boolean_variable RUNTIME_START_DEPTH_CAMERAS
  normalize_boolean_variable RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS
  normalize_boolean_variable DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES
  normalize_boolean_variable RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE
  normalize_boolean_variable RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING
  normalize_boolean_variable RUNTIME_ENABLE_DEPTH_LEFT_CAM
  normalize_boolean_variable RUNTIME_ENABLE_DEPTH_RIGHT_CAM
  normalize_boolean_variable RUNTIME_ENABLE_DEPTH_UP_CAM
  normalize_boolean_variable DORAEMON_NO_ACTION_ACCEPTANCE
  normalize_boolean_variable DORAEMON_ACTION_TEST_APPROVED
  normalize_boolean_variable ENABLE_MANUAL_DRIVE_SERVICE
  normalize_boolean_variable START_MCORE_BRIDGE
  normalize_boolean_variable START_MCORE_VELOCITY_SENDER
  normalize_boolean_variable MCORE_ENABLE_CMD_VEL
  normalize_boolean_variable FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE
  normalize_boolean_variable FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE
  normalize_boolean_variable FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE
  normalize_boolean_variable FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE
  normalize_boolean_variable FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS
  local camera_sequence_number_name
  for camera_sequence_number_name in \
    RUNTIME_DEPTH_CAMERA_READY_SAMPLES \
    RUNTIME_DEPTH_CAMERA_READY_TIMEOUT \
    RUNTIME_DEPTH_CAMERA_INTER_START_DELAY; do
    if ! runtime_is_positive_integer "${!camera_sequence_number_name:-}"; then
      echo "[ERROR] ${camera_sequence_number_name} must be a positive integer" >&2
      return 1
    fi
  done
  local camera_watchdog_number_name
  for camera_watchdog_number_name in \
    RUNTIME_DEPTH_CAMERA_STALE_TIMEOUT \
    RUNTIME_DEPTH_CAMERA_RECOVERY_COOLDOWN; do
    if ! runtime_is_positive_finite_number "${!camera_watchdog_number_name:-}"; then
      echo "[ERROR] ${camera_watchdog_number_name} must be finite and > 0" >&2
      return 1
    fi
  done
  if [[ "${DORAEMON_NO_ACTION_ACCEPTANCE}" == "true" ]]; then
    if [[ "${DORAEMON_ACTION_TEST_APPROVED}" != "false" ]]; then
      echo "[ERROR] no-action acceptance requires DORAEMON_ACTION_TEST_APPROVED=false" >&2
      return 1
    fi
  elif [[ "${DORAEMON_ACTION_TEST_APPROVED}" != "true" ]]; then
    echo "[ERROR] action-capable runtime requires DORAEMON_ACTION_TEST_APPROVED=true" >&2
    return 1
  else
    local velocity_limit_name velocity_scale_name velocity_sign_name
    for velocity_limit_name in \
      MCORE_MAX_ABS_LINEAR_VELOCITY \
      MCORE_MAX_ABS_ANGULAR_VELOCITY; do
      if ! runtime_is_positive_finite_number "${!velocity_limit_name:-}"; then
        echo "[ERROR] action-capable runtime requires ${velocity_limit_name} to be finite and > 0" >&2
        return 1
      fi
    done

    for velocity_scale_name in \
      MCORE_LINEAR_VELOCITY_SCALE \
      MCORE_ANGULAR_VELOCITY_SCALE; do
      if ! runtime_is_positive_finite_number "${!velocity_scale_name:-}"; then
        echo "[ERROR] action-capable runtime requires ${velocity_scale_name} to be finite and > 0" >&2
        return 1
      fi
    done

    for velocity_sign_name in \
      MCORE_LINEAR_VELOCITY_SIGN \
      MCORE_ANGULAR_VELOCITY_SIGN; do
      if ! runtime_is_unit_sign "${!velocity_sign_name:-}"; then
        echo "[ERROR] action-capable runtime requires ${velocity_sign_name} to be exactly -1 or +1" >&2
        return 1
      fi
    done

    if [[ "${CHASSIS_DRIVER}" == "mcore_tcp" ]] &&
       [[ "${START_MCORE_VELOCITY_SENDER}" != "true" ||
          "${START_MCORE_BRIDGE}" != "false" ||
          "${MCORE_ENABLE_CMD_VEL}" != "false" ]]; then
      echo "[ERROR] mcore_tcp action mode requires exactly one motion transport: " \
           "START_MCORE_VELOCITY_SENDER=true, START_MCORE_BRIDGE=false, " \
           "MCORE_ENABLE_CMD_VEL=false" >&2
      return 1
    fi

    if [[ "${ENABLE_MANUAL_DRIVE_SERVICE}" == "true" ]]; then
      local manual_drive_gate_name
      for manual_drive_gate_name in \
        FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE \
        FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE \
        FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE \
        FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE; do
        if [[ "${!manual_drive_gate_name}" != "false" ]]; then
          echo "[ERROR] remote-control manual drive requires ${manual_drive_gate_name}=false" >&2
          return 1
        fi
      done
      if [[ "${FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS}" != "true" ]]; then
        echo "[ERROR] manual drive requires live platform/E-stop status" >&2
        return 1
      fi
    fi
  fi

  if [[ "${RUNTIME_START_DEPTH_CAMERAS}" != "true" ]]; then
    echo "[ERROR] this commercial vehicle requires all three Orbbec cameras" >&2
    return 1
  fi
  if ! commercial_validate_required_orbbec_identities \
    "${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER}" \
    "${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER}" \
    "${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER}" \
    "${RUNTIME_ORBBEC_CAMERA1_USB_PORT}" \
    "${RUNTIME_ORBBEC_CAMERA2_USB_PORT}" \
    "${RUNTIME_ORBBEC_CAMERA3_USB_PORT}"; then
    echo "[ERROR] Orbbec serials/topologies must be explicit, valid, and unique" >&2
    return 1
  fi
  if [[ "${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS}" != "true" ||
        "${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES}" != "true" ||
        "${RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE}" != "true" ||
        "${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING}" != "true" ||
        "${RUNTIME_ENABLE_DEPTH_LEFT_CAM}" != "true" ||
        "${RUNTIME_ENABLE_DEPTH_RIGHT_CAM}" != "true" ||
        "${RUNTIME_ENABLE_DEPTH_UP_CAM}" != "true" ]]; then
    echo "[ERROR] commercial Orbbec runtime requires all topic, identity, watchdog, and obstacle gates" >&2
    return 1
  fi
  local usbfs_memory_mb=""
  usbfs_memory_mb="$(cat /sys/module/usbcore/parameters/usbfs_memory_mb 2>/dev/null || true)"
  if [[ ! "${usbfs_memory_mb}" =~ ^[0-9]+$ ]] || (( usbfs_memory_mb < 128 )); then
    echo "[ERROR] commercial Orbbec runtime requires usbfs_memory_mb>=128; actual=${usbfs_memory_mb:-missing}" >&2
    return 1
  fi

  local extra_args=()
  append_shell_words extra_args "${RUNTIME_BASE_EXTRA_ARGS}"
  local extra_arg
  for extra_arg in "${extra_args[@]}"; do
    case "${extra_arg}" in
      start_depth_cameras:=*|camera[123]_bind_by_usb_port:=*|camera[123]_serial_number:=*|camera[123]_usb_port:=*)
        echo "[ERROR] RUNTIME_BASE_EXTRA_ARGS may not override protected camera argument: ${extra_arg%%:=*}" >&2
        return 1
        ;;
    esac
  done

  extra_args=()
  append_shell_words extra_args "${WHEEL_ODOM_EXTRA_ARGS}"
  for extra_arg in "${extra_args[@]}"; do
    case "${extra_arg}" in
      serial_device:=*|serial_baudrate:=*|protocol_mode:=*|use_device_timestamp:=*|publish_raw_odom_tf:=*|frame_id:=*|child_frame_id:=*|wheel_separation:=*|wheel_diameter:=*|gear_ratio:=*|encoder_pulses_per_motor_revolution:=*|left_encoder_sign:=*|right_encoder_sign:=*|left_wheel_scale:=*|right_wheel_scale:=*|angular_velocity_sign:=*)
        echo "[ERROR] WHEEL_ODOM_EXTRA_ARGS may not override protected vehicle argument: ${extra_arg%%:=*}" >&2
        return 1
        ;;
    esac
  done

  extra_args=()
  append_shell_words extra_args "${MCORE_VELOCITY_EXTRA_ARGS}"
  for extra_arg in "${extra_args[@]}"; do
    case "${extra_arg}" in
      transport:=*|serial_device:=*|serial_baudrate:=*|tcp_host:=*|tcp_port:=*|cmd_vel_topic:=*|linear_velocity_scale:=*|angular_velocity_scale:=*|linear_velocity_sign:=*|angular_velocity_sign:=*|max_abs_linear_velocity:=*|max_abs_angular_velocity:=*|enable_tx_log:=*|enable_rx_log:=*)
        echo "[ERROR] MCORE_VELOCITY_EXTRA_ARGS may not override protected vehicle argument: ${extra_arg%%:=*}" >&2
        return 1
        ;;
    esac
  done

  if [[ "${DORAEMON_NO_ACTION_ACCEPTANCE}" == "true" ]]; then
    extra_args=()
    append_shell_words extra_args "${HARDWARE_BRIDGES_EXTRA_ARGS}"
    for extra_arg in "${extra_args[@]}"; do
      case "${extra_arg}" in
        enable_mcore_bridge:=*|mcore_enable_cmd_vel:=*|enable_station_bridge:=*|enable_dock_supply_manager:=*|enable_docking_stack:=*|mechanical_connect_enable:=*|direct_charge_after_precise_docking:=*|dock_supply_enable_drain:=*|dock_supply_enable_refill:=*|charge_voltage_confirm_enable:=*)
          echo "[ERROR] HARDWARE_BRIDGES_EXTRA_ARGS may not override protected no-action argument: ${extra_arg%%:=*}" >&2
          return 1
          ;;
      esac
    done

    if [[ -n "${BACKEND_RUNTIME_SMOKE_ACTIONS}" ]]; then
      echo "[ERROR] BACKEND_RUNTIME_SMOKE_ACTIONS must be empty in no-action mode" >&2
      return 1
    fi
    if [[ -n "${BACKEND_RUNTIME_SMOKE_EXTRA_ARGS}" ]]; then
      echo "[ERROR] BACKEND_RUNTIME_SMOKE_EXTRA_ARGS must be empty in no-action mode" >&2
      return 1
    fi
    if [[ "${RUN_BACKEND_PRODUCTION_ACCEPTANCE}" == "1" && \
          "${BACKEND_PRODUCTION_ACCEPTANCE_PROFILE}" != "read_only_gate" ]]; then
      echo "[ERROR] no-action production acceptance only permits profile=read_only_gate" >&2
      return 1
    fi
    case "${BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS,,}" in
      ""|0|false|no|off)
        ;;
      *)
        echo "[ERROR] BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS must be disabled in no-action mode" >&2
        return 1
        ;;
    esac
    if [[ -n "${BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS}" ]]; then
      echo "[ERROR] BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS must be empty in no-action mode" >&2
      return 1
    fi
  fi
}

apply_no_action_acceptance_overrides() {
  [[ "${DORAEMON_NO_ACTION_ACCEPTANCE}" == "true" ]] || return 0
  TASK_AUTO_CHARGE_ENABLE=false
  EXECUTOR_AUTO_CHARGE_ENABLE=false
  AUTO_CHARGE_MONITOR_ENABLE=false
  AUTO_CHARGE_MONITOR_RECOVERY_ENABLE=false
  DEFAULT_RETURN_TO_DOCK_ON_FINISH=false
  MCORE_ENABLE_CMD_VEL=false
  START_MCORE_BRIDGE=false
  START_MCORE_VELOCITY_SENDER=false
  START_WHEELTEC_BASE=false
  START_STATION_BRIDGE=false
  START_DOCK_SUPPLY_MANAGER=false
  START_DOCKING_STACK=false
  MECHANICAL_CONNECT_ENABLE=false
  DIRECT_CHARGE_AFTER_PRECISE_DOCKING=false
  DOCK_SUPPLY_ENABLE_DRAIN=false
  DOCK_SUPPLY_ENABLE_REFILL=false
  CHARGE_VOLTAGE_CONFIRM_ENABLE=false
  RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE=false
  ENABLE_MANUAL_DRIVE_SERVICE=false
}

assert_no_action_runtime_isolated() {
  [[ "${DORAEMON_NO_ACTION_ACCEPTANCE}" == "true" ]] || return 0

  local active_nodes=""
  local active_services=""
  local forbidden=""
  local auto_charge=""
  local active_topics=""
  local cmd_vel_info=""
  local publisher=""
  local subscriber=""
  active_nodes="$(runtime_run_ros_cli rosnode list 2>/dev/null)" || {
    runtime_log_status "[ERROR] unable to verify no-action ROS node isolation"
    return 1
  }
  for forbidden in \
    /mcore_velocity_sender \
    /mcore_tcp_bridge \
    /station_tcp_bridge \
    /dock_supply_manager \
    /dock_tracker \
    /docking_controller \
    /auto_charge_monitor \
    /manual_drive_service \
    /wheeltec_robot; do
    if grep -Fxq -- "${forbidden}" <<<"${active_nodes}"; then
      runtime_log_status "[ERROR] forbidden action transport is active in no-action mode: ${forbidden}"
      return 1
    fi
  done

  active_services="$(runtime_run_ros_cli rosservice list 2>/dev/null)" || {
    runtime_log_status "[ERROR] unable to verify no-action ROS service isolation"
    return 1
  }
  for forbidden in \
    /dock_supply/start \
    /dock_supply/exit \
    /dock_supply/recovery_retreat \
    /clean_robot_server/app/manual_drive_command \
    /clean_robot_server/app/get_manual_drive_status; do
    if grep -Fxq -- "${forbidden}" <<<"${active_services}"; then
      runtime_log_status "[ERROR] forbidden action service is active in no-action mode: ${forbidden}"
      return 1
    fi
  done

  auto_charge="$(runtime_get_rosparam_value /coverage_task_manager/auto_charge_enable || true)"
  if [[ "${auto_charge,,}" != "false" ]]; then
    runtime_log_status "[ERROR] auto-charge parameter must be false in no-action mode: ${auto_charge:-missing}"
    return 1
  fi

  active_topics="$(runtime_run_ros_cli rostopic list 2>/dev/null)" || {
    runtime_log_status "[ERROR] unable to verify no-action ROS topic isolation"
    return 1
  }
  if grep -Fxq -- /cmd_vel <<<"${active_topics}"; then
    cmd_vel_info="$(runtime_run_ros_cli rostopic info /cmd_vel 2>/dev/null)" || {
      runtime_log_status "[ERROR] unable to inspect /cmd_vel publishers in no-action mode"
      return 1
    }
    while IFS= read -r publisher; do
      [[ -n "${publisher}" ]] || continue
      case "${publisher}" in
        /coverage_executor|/move_base_flex)
          ;;
        *)
          runtime_log_status "[ERROR] unapproved /cmd_vel publisher is active in no-action mode: ${publisher}"
          return 1
          ;;
      esac
    done < <(
      awk '
        /^Publishers:$/ { in_publishers=1; next }
        /^Subscribers:$/ { in_publishers=0 }
        in_publishers && /^[[:space:]]*\*[[:space:]]+\// {
          line=$0
          sub(/^[[:space:]]*\*[[:space:]]+/, "", line)
          sub(/[[:space:]].*$/, "", line)
          print line
        }
      ' <<<"${cmd_vel_info}"
    )
    while IFS= read -r subscriber; do
      [[ -n "${subscriber}" ]] || continue
      runtime_log_status "[ERROR] /cmd_vel has an active subscriber in no-action mode: ${subscriber}"
      return 1
    done < <(
      awk '
        /^Subscribers:$/ { in_subscribers=1; next }
        in_subscribers && /^[[:space:]]*\*[[:space:]]+\// {
          line=$0
          sub(/^[[:space:]]*\*[[:space:]]+/, "", line)
          sub(/[[:space:]].*$/, "", line)
          print line
        }
      ' <<<"${cmd_vel_info}"
    )
  fi
  runtime_log_status "[OK] no-action transport and manual-drive isolation verified"
}

validate_external_runtime_log_paths() {
  local commercial_log_root="/var/log/doraemon"
  local actual_log_root=""
  local name
  local value
  local resolved
  if [[ -L "${commercial_log_root}" || ( -e "${commercial_log_root}" && ! -d "${commercial_log_root}" ) ]]; then
    echo "[ERROR] /var/log/doraemon must be a real non-symlink directory" >&2
    return 1
  fi
  if [[ -e "${commercial_log_root}" ]] && mountpoint -q "${commercial_log_root}"; then
    echo "[ERROR] /var/log/doraemon must not be an unreviewed bind/mount point" >&2
    return 1
  fi
  actual_log_root="$(realpath -m "${commercial_log_root}")"
  if [[ "${actual_log_root}" != "${commercial_log_root}" ]]; then
    echo "[ERROR] /var/log/doraemon resolves outside its fixed external path: ${actual_log_root}" >&2
    return 1
  fi
  for name in LOG_DIR STATUS_LOG RESTART_LOCALIZATION_OUT; do
    value="${!name}"
    resolved="$(realpath -m "${value}")"
    case "${resolved}" in
      "${commercial_log_root}"|"${commercial_log_root}"/*)
        ;;
      *)
        echo "[ERROR] ${name} must stay under /var/log/doraemon: ${resolved}" >&2
        return 1
        ;;
    esac
    case "${resolved}" in
      /opt/doraemon/releases|/opt/doraemon/releases/*)
        echo "[ERROR] ${name} resolves into an immutable release: ${resolved}" >&2
        return 1
        ;;
    esac
  done
}

runtime_require_orbbec_serial() {
  local camera_name="$1"
  local expected_serial="$2"
  local service_name="/${camera_name}/get_serial"
  local response=""
  local actual_serial=""
  local success=""

  runtime_wait_for_service "${service_name}" 20
  response="$(runtime_run_ros_cli rosservice call "${service_name}" '{}')" || {
    runtime_log_status "[ERROR] unable to query ${service_name}"
    return 1
  }
  actual_serial="$(runtime_extract_yaml_scalar "${response}" data)"
  success="$(runtime_extract_yaml_scalar "${response}" success | tr '[:upper:]' '[:lower:]')"
  if [[ "${success}" != "true" || "${actual_serial}" != "${expected_serial}" ]]; then
    runtime_log_status "[ERROR] ${camera_name} serial mismatch expected=${expected_serial} actual=${actual_serial:-missing}"
    return 1
  fi
  runtime_log_status "[OK] ${camera_name} serial=${actual_serial}"
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

log_effective_runtime_parameters() {
  local dock_threshold
  dock_threshold="$(awk -v target="${DOCK_TARGET_DIST}" -v tol="${DOCK_XY_TOLERANCE}" 'BEGIN { printf "%.3f", target + tol }' 2>/dev/null || printf 'unknown')"

  runtime_log_status "station bridge: ${STATION_SERVER_IP}:${STATION_SERVER_PORT}"
  runtime_log_status "no-action acceptance: ${DORAEMON_NO_ACTION_ACCEPTANCE} action_test_approved=${DORAEMON_ACTION_TEST_APPROVED} manual_drive=${ENABLE_MANUAL_DRIVE_SERVICE} mcore_sender=${START_MCORE_VELOCITY_SENDER} cmd_vel=${MCORE_ENABLE_CMD_VEL} station_bridge=${START_STATION_BRIDGE} dock_supply=${START_DOCK_SUPPLY_MANAGER} docking_stack=${START_DOCKING_STACK} task_auto_charge=${TASK_AUTO_CHARGE_ENABLE} monitor=${AUTO_CHARGE_MONITOR_ENABLE} gateway_auto_start=${RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE}"
  runtime_log_status "Orbbec: start=${RUNTIME_START_DEPTH_CAMERAS} require_topics=${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS} require_identities=${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES} watchdog=${RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE}(stale=${RUNTIME_DEPTH_CAMERA_STALE_TIMEOUT}s cooldown=${RUNTIME_DEPTH_CAMERA_RECOVERY_COOLDOWN}s) obstacle_tracking=${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING} obstacle_sources(left=${RUNTIME_ENABLE_DEPTH_LEFT_CAM} right=${RUNTIME_ENABLE_DEPTH_RIGHT_CAM} front=${RUNTIME_ENABLE_DEPTH_UP_CAM}) left=${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER}@${RUNTIME_ORBBEC_CAMERA1_USB_PORT} right=${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER}@${RUNTIME_ORBBEC_CAMERA2_USB_PORT} front=${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER}@${RUNTIME_ORBBEC_CAMERA3_USB_PORT}"
  runtime_log_status "dock tuning: target=${DOCK_TARGET_DIST} xy_tolerance=${DOCK_XY_TOLERANCE} yaw_tolerance=${DOCK_YAW_TOLERANCE} threshold=${dock_threshold} score_thresh=${DOCK_POSE_SCORE_THRESH}"
  runtime_log_status "auto charge: task=${TASK_AUTO_CHARGE_ENABLE} executor=${EXECUTOR_AUTO_CHARGE_ENABLE} low=${AUTO_CHARGE_LOW_SOC} resume=${AUTO_CHARGE_RESUME_SOC} rearm=${AUTO_CHARGE_REARM_SOC} target=${AUTO_CHARGE_TARGET_SOC}"
  runtime_log_status "dock supply: post_charge_drain=${DOCK_SUPPLY_ENABLE_DRAIN} refill=${DOCK_SUPPLY_ENABLE_REFILL} drain_timeout_s=${DOCK_SUPPLY_DRAIN_TIMEOUT_S} clean_stop_above=${DOCK_SUPPLY_TARGET_CLEAN_LEVEL}% refill_timeout_s=${DOCK_SUPPLY_REFILL_TIMEOUT_S} refill_settle_s=${DOCK_SUPPLY_REFILL_SETTLE_S} combined_status_wait_s=${DOCK_SUPPLY_COMBINED_STATUS_WAIT_S} combined_status_stale_s=${DOCK_SUPPLY_COMBINED_STATUS_STALE_TIMEOUT_S}"
  if [[ "${AUTO_CHARGE_MONITOR_RECOVERY_STRATEGY}" == "redock" ]]; then
    runtime_log_status "charge recovery monitor: enable=${AUTO_CHARGE_MONITOR_ENABLE} recovery=${AUTO_CHARGE_MONITOR_RECOVERY_ENABLE} strategy=redock timeout_s=${AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S} retreat=/dock_supply/recovery_retreat attempts=${AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS}"
  else
    runtime_log_status "charge recovery monitor: enable=${AUTO_CHARGE_MONITOR_ENABLE} recovery=${AUTO_CHARGE_MONITOR_RECOVERY_ENABLE} strategy=${AUTO_CHARGE_MONITOR_RECOVERY_STRATEGY} timeout_s=${AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S} back_m=${AUTO_CHARGE_MONITOR_RECOVERY_BACK_DISTANCE_M} forward_m=${AUTO_CHARGE_MONITOR_RECOVERY_FORWARD_DISTANCE_M} attempts=${AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS}"
  fi
}

build_backend_runtime_smoke_cmd() {
  local -n cmd_ref="$1"

  cmd_ref=(
    rosrun
    coverage_planner
    run_backend_runtime_smoke.py
    --robot-id
    "${ROBOT_ID}"
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

runtime_wait_for_consecutive_topic_samples() {
  local topic_name="$1"
  local camera_label="$2"
  local message_filter="$3"

  runtime_log_status "等待${camera_label}连续${RUNTIME_DEPTH_CAMERA_READY_SAMPLES}帧有效数据: ${topic_name}"
  if ! timeout "${RUNTIME_DEPTH_CAMERA_READY_TIMEOUT}" \
      rostopic echo -n "${RUNTIME_DEPTH_CAMERA_READY_SAMPLES}" \
      --filter "${message_filter}" "${topic_name}" \
      >/dev/null 2>&1; then
    runtime_log_status "[ERROR] ${camera_label}未在${RUNTIME_DEPTH_CAMERA_READY_TIMEOUT}s内输出连续有效数据，停止后续相机启动"
    return 1
  fi
  runtime_log_status "[OK] ${camera_label}连续有效数据已就绪: ${topic_name}"
}

verify_depth_camera_chain() {
  if [[ "${RUNTIME_START_DEPTH_CAMERAS}" != "true" ]]; then
    runtime_log_status "[INFO] skip Orbbec chain verification: RUNTIME_START_DEPTH_CAMERAS=${RUNTIME_START_DEPTH_CAMERAS}"
    return 0
  fi
  if [[ "${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS}" != "true" ]]; then
    runtime_log_status "[WARN] 跳过深度相机话题就绪检查；相机缺失或异常不阻塞整机启动"
    return 0
  fi

  runtime_log_status "检查三台奥比中光深度相机（商业启动硬门）"
  runtime_wait_for_consecutive_topic_samples \
    /gemini_cf/depth/image_raw "左相机深度图" \
    'm.width > 0 and m.height > 0 and len(m.data) > 0 and any(m.data)' || return 1
  runtime_wait_for_consecutive_topic_samples \
    /gemini_nj/depth/image_raw "右相机深度图" \
    'm.width > 0 and m.height > 0 and len(m.data) > 0 and any(m.data)' || return 1
  runtime_wait_for_consecutive_topic_samples \
    /gemini_front/depth/image_raw "前相机深度图" \
    'm.width > 0 and m.height > 0 and len(m.data) > 0 and any(m.data)' || return 1
  runtime_wait_for_consecutive_topic_samples \
    /gemini_cf/depth/points "左相机点云" \
    'm.width > 0 and m.height > 0 and m.point_step > 0 and len(m.data) > 0' || return 1
  runtime_wait_for_consecutive_topic_samples \
    /gemini_nj/depth/points "右相机点云" \
    'm.width > 0 and m.height > 0 and m.point_step > 0 and len(m.data) > 0' || return 1
  runtime_wait_for_consecutive_topic_samples \
    /gemini_front/depth/points "前相机点云" \
    'm.width > 0 and m.height > 0 and m.point_step > 0 and len(m.data) > 0' || return 1

  if [[ "${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES}" == "true" ]]; then
    runtime_require_orbbec_serial gemini_cf "${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER}" || return 1
    runtime_require_orbbec_serial gemini_nj "${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER}" || return 1
    runtime_require_orbbec_serial gemini_front "${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER}" || return 1
  else
    runtime_log_status "[SKIP] 深度相机身份复核已关闭"
  fi
  runtime_log_status "[OK] 三台深度相机连续图像、点云与身份均就绪"
}

ensure_depth_camera_chain_ready() {
  if verify_depth_camera_chain; then
    return 0
  fi

  # Startup owns the camera chain until this gate passes.  The runtime
  # watchdog is deliberately not running yet, so there can be no competing
  # kill/relaunch operation while this one bounded recovery is in progress.
  runtime_log_status "[WARN] Orbbec商业启动硬门未通过；主启动流程将串行重启完整相机链一次"
  if ! recover_depth_camera_chain_once_during_startup; then
    return 1
  fi
  if ! verify_depth_camera_chain; then
    runtime_log_status "[ERROR] Orbbec相机链重启后仍未通过商业启动硬门"
    return 1
  fi
  runtime_log_status "[OK] Orbbec启动期相机链已经一次有界恢复后就绪"
}

recover_depth_camera_chain_once_during_startup() {
  if (( DEPTH_CAMERA_STARTUP_RECOVERY_USED != 0 )); then
    runtime_log_status "[ERROR] Orbbec启动期唯一一次相机链恢复已使用，拒绝再次重启"
    return 1
  fi
  DEPTH_CAMERA_STARTUP_RECOVERY_USED=1
  if ! DORAEMON_ORBBEC_RECOVERY_CONTEXT=startup \
      "${REPO_ROOT}/scripts/restart_orbbec_camera_chain.sh"; then
    runtime_log_status "[ERROR] Orbbec启动期唯一一次相机链恢复失败"
    return 1
  fi
}

start_depth_camera_window() {
  local window_name="$1"
  local camera_label="$2"
  local camera_name="$3"
  local serial_number="$4"
  local usb_port="$5"
  local connection_delay="$6"
  local point_topic="$7"
  local enable_color="$8"
  local depth_format="$9"
  local enable_soft_filter="${10}"
  local depth_topic="/${camera_name}/depth/image_raw"

  local camera_cmd_words=(
    exec
    roslaunch
    cleanrobot
    orbbec_single_depth_ground.launch
    camera_name:="${camera_name}"
    bind_by_usb_port:=false
    serial_number:="${serial_number}"
    usb_port:="${usb_port}"
    device_num:=3
    connection_delay:="${connection_delay}"
    enable_color:="${enable_color}"
    depth_format:="${depth_format}"
    enable_soft_filter:="${enable_soft_filter}"
  )

  runtime_log_status "启动${camera_label}: name=${camera_name} serial=${serial_number}"
  runtime_tmux_window "${TMUX_SESSION}" "${window_name}" "$(join_shell_words camera_cmd_words)" || return 1
  runtime_wait_for_consecutive_topic_samples \
    "${depth_topic}" "${camera_label}深度图" \
    'm.width > 0 and m.height > 0 and len(m.data) > 0 and any(m.data)' || return 1
  runtime_wait_for_consecutive_topic_samples \
    "${point_topic}" "${camera_label}点云" \
    'm.width > 0 and m.height > 0 and m.point_step > 0 and len(m.data) > 0' || return 1
  if [[ "${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES}" == "true" ]]; then
    runtime_require_orbbec_serial "${camera_name}" "${serial_number}" || return 1
  fi
}

start_depth_cameras_sequentially() {
  if [[ "${RUNTIME_START_DEPTH_CAMERAS}" != "true" ]]; then
    runtime_log_status "[INFO] skip sequential Orbbec startup: RUNTIME_START_DEPTH_CAMERAS=${RUNTIME_START_DEPTH_CAMERAS}"
    return 0
  fi

  runtime_log_status "顺序启动三台Orbbec相机: 左 -> 右 -> 前"
  # The two legacy Gemini Max side cameras can keep publishing timestamped
  # depth frames while the SDK soft filter turns most or all samples into zero,
  # even on independent motherboard USB3 ports. Keep their SDK filter disabled;
  # downstream ground/noise filtering remains active. Raw depth and point-cloud
  # hard gates still reject empty or all-zero camera output.
  start_depth_camera_window depth_left 左相机 gemini_cf \
    "${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER}" "${RUNTIME_ORBBEC_CAMERA1_USB_PORT}" \
    800 /gemini_cf/depth/points false Y11 false || return 1
  runtime_log_status "左相机稳定等待${RUNTIME_DEPTH_CAMERA_INTER_START_DELAY}s后启动右相机"
  sleep "${RUNTIME_DEPTH_CAMERA_INTER_START_DELAY}"

  start_depth_camera_window depth_right 右相机 gemini_nj \
    "${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER}" "${RUNTIME_ORBBEC_CAMERA2_USB_PORT}" \
    1800 /gemini_nj/depth/points false Y11 false || return 1
  runtime_log_status "右相机稳定等待${RUNTIME_DEPTH_CAMERA_INTER_START_DELAY}s后启动前相机"
  sleep "${RUNTIME_DEPTH_CAMERA_INTER_START_DELAY}"

  start_depth_camera_window depth_front 前相机 gemini_front \
    "${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER}" "${RUNTIME_ORBBEC_CAMERA3_USB_PORT}" \
    2800 /gemini_front/depth/points true Y16 true || return 1
  runtime_log_status "[OK] 三台Orbbec相机已按左、右、前顺序启动"
}

start_depth_cameras_with_startup_recovery() {
  if start_depth_cameras_sequentially; then
    return 0
  fi
  runtime_log_status "[WARN] Orbbec初始顺序启动失败；尝试启动期唯一一次完整链恢复"
  recover_depth_camera_chain_once_during_startup
}

start_depth_camera_watchdog() {
  if [[ "${RUNTIME_START_DEPTH_CAMERAS}" != "true" ||
        "${RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE}" != "true" ]]; then
    runtime_log_status "[INFO] skip Orbbec watchdog"
    return 0
  fi
  local watchdog_cmd_words=(
    exec rosrun cleanrobot orbbec_camera_watchdog_node.py
    _stale_timeout:="${RUNTIME_DEPTH_CAMERA_STALE_TIMEOUT}"
    _recovery_cooldown:="${RUNTIME_DEPTH_CAMERA_RECOVERY_COOLDOWN}"
    _recovery_script:="${REPO_ROOT}/scripts/restart_orbbec_camera_chain.sh"
  )
  runtime_tmux_window "${TMUX_SESSION}" depth_watchdog "$(join_shell_words watchdog_cmd_words)"
  runtime_log_status "[OK] Orbbec运行监控已启动"
}

finalize_depth_camera_supervision() {
  # This function is called only from successful startup exits, after every
  # other blocking gate for that exit has passed.  Until this point startup is
  # the sole owner of camera launch/recovery; afterwards the watchdog is the
  # sole owner.  The watchdog's own startup grace covers the few milliseconds
  # before the top-level startup transaction commits.
  ensure_depth_camera_chain_ready
  start_depth_camera_watchdog
}

start_runtime_session() {
  runtime_log_status "start tmux session ${TMUX_SESSION}"

  local base_cmd_words=(
    exec
    roslaunch
    cleanrobot
    cleanrobot_base.launch
  )
  if [[ "${RUNTIME_START_ROBOT_STATE_PUBLISHER}" != "true" ]]; then
    base_cmd_words+=(start_robot_state_publisher:="${RUNTIME_START_ROBOT_STATE_PUBLISHER}")
  fi
  if [[ "${RUNTIME_START_JOINT_STATE_PUBLISHER}" != "false" ]]; then
    base_cmd_words+=(start_joint_state_publisher:="${RUNTIME_START_JOINT_STATE_PUBLISHER}")
  fi
  if [[ "${RUNTIME_ROBOT_DESCRIPTION_PATH}" != "${REPO_ROOT}/src/cleanrobot_description/urdf/a26022_clean_robot.urdf" ]]; then
    base_cmd_words+=(robot_description_path:="${RUNTIME_ROBOT_DESCRIPTION_PATH}")
  fi
  if [[ "${RUNTIME_START_LIDAR}" != "true" ]]; then
    base_cmd_words+=(start_lidar:="${RUNTIME_START_LIDAR}")
  fi
  base_cmd_words+=(
    lidar_ntp_ip:="${RUNTIME_LIDAR_NTP_IP}"
    lidar_ntp_port:="${RUNTIME_LIDAR_NTP_PORT}"
    lidar_ntp_enable:="${RUNTIME_LIDAR_NTP_ENABLE}"
  )
  if [[ "${RUNTIME_START_IMU}" != "true" ]]; then
    base_cmd_words+=(start_imu:="${RUNTIME_START_IMU}")
  fi
  base_cmd_words+=(
    imu_port:="${RUNTIME_IMU_PORT}"
    imu_baud:="${RUNTIME_IMU_BAUD}"
    imu_slave_address:="${RUNTIME_IMU_SLAVE_ADDRESS}"
  )
  if [[ "${RUNTIME_START_AHRS}" != "false" ]]; then
    base_cmd_words+=(start_ahrs:="${RUNTIME_START_AHRS}")
  fi
  if [[ "${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK}" != "false" ]]; then
    base_cmd_words+=(
      publish_base_footprint_to_base_link:="${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_BASE_LINK}"
      base_footprint_to_base_link_z:="${RUNTIME_BASE_FOOTPRINT_TO_BASE_LINK_Z}"
    )
  fi
  if [[ "${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK}" != "false" ]]; then
    base_cmd_words+=(publish_base_footprint_to_gyro_link:="${RUNTIME_PUBLISH_BASE_FOOTPRINT_TO_GYRO_LINK}")
  fi
  append_shell_words base_cmd_words "${RUNTIME_BASE_EXTRA_ARGS}"
  # Append protected commercial identity arguments last. The validator rejects
  # duplicates in RUNTIME_BASE_EXTRA_ARGS; this final ordering is defense in depth.
  base_cmd_words+=(
    # Production camera drivers are launched one-by-one below. Keeping them
    # out of the base roslaunch prevents concurrent selectDevice() calls.
    start_depth_cameras:=false
    camera1_bind_by_usb_port:=false
    camera2_bind_by_usb_port:=false
    camera3_bind_by_usb_port:=false
    camera1_serial_number:="${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER}"
    camera2_serial_number:="${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER}"
    camera3_serial_number:="${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER}"
    camera1_usb_port:="${RUNTIME_ORBBEC_CAMERA1_USB_PORT}"
    camera2_usb_port:="${RUNTIME_ORBBEC_CAMERA2_USB_PORT}"
    camera3_usb_port:="${RUNTIME_ORBBEC_CAMERA3_USB_PORT}"
  )

  local base_cmd
  base_cmd="$(join_shell_words base_cmd_words)"

  tmux new-session -d -s "${TMUX_SESSION}" -n base \
    "bash -lc 'source \"${DORAEMON_ROS_SETUP}\"; source \"${DORAEMON_WORKSPACE_SETUP}\"; export ROS_MASTER_URI=${ROS_MASTER_URI}; export ROS_IP=127.0.0.1; unset ROS_HOSTNAME; ${base_cmd}'"

  start_depth_cameras_with_startup_recovery

  if [[ "${START_WHEELTEC_BASE}" == "true" ]]; then
    local wheeltec_cmd_words=(
      exec
      roslaunch
      robot_hw_bridge
      wheeltec_senior_diff_base.launch
      serial_device:="${WHEELTEC_SERIAL_DEVICE}"
    )
    append_shell_words wheeltec_cmd_words "${WHEELTEC_BASE_EXTRA_ARGS}"
    runtime_tmux_window "${TMUX_SESSION}" wheeltec "$(join_shell_words wheeltec_cmd_words)"
  else
    runtime_log_status "[INFO] skip wheeltec chassis base: START_WHEELTEC_BASE=${START_WHEELTEC_BASE}"
  fi

  if [[ "${START_IMU_BIAS_CORRECTION}" == "true" ]]; then
    runtime_tmux_window "${TMUX_SESSION}" imu_bias "exec roslaunch wheel_speed_odom_bridge imu_bias_correction.launch"
  else
    runtime_log_status "[INFO] skip imu bias correction: START_IMU_BIAS_CORRECTION=${START_IMU_BIAS_CORRECTION}"
  fi

  if [[ "${START_WHEEL_ODOM}" == "true" ]]; then
    local odom_cmd_words=(
      exec
      roslaunch
      wheel_speed_odom_bridge
      wheel_speed_odom.launch
    )
    append_shell_words odom_cmd_words "${WHEEL_ODOM_EXTRA_ARGS}"
    # Per-vehicle odometry parameters are protected by the startup validator
    # and appended last as an additional defense against accidental overrides.
    odom_cmd_words+=(
      serial_device:="${ODOM_SERIAL_DEVICE}"
      serial_baudrate:="${ODOM_SERIAL_BAUDRATE}"
      protocol_mode:="${ODOM_PROTOCOL_MODE}"
      use_device_timestamp:="${ODOM_USE_DEVICE_TIMESTAMP}"
      publish_raw_odom_tf:="${ODOM_PUBLISH_RAW_ODOM_TF}"
      frame_id:="${ODOM_FRAME_ID}"
      child_frame_id:="${ODOM_CHILD_FRAME_ID}"
      wheel_separation:="${ODOM_WHEEL_SEPARATION}"
      wheel_diameter:="${ODOM_WHEEL_DIAMETER}"
      gear_ratio:="${ODOM_GEAR_RATIO}"
      encoder_pulses_per_motor_revolution:="${ODOM_ENCODER_PPR}"
      left_encoder_sign:="${ODOM_LEFT_ENCODER_SIGN}"
      right_encoder_sign:="${ODOM_RIGHT_ENCODER_SIGN}"
      left_wheel_scale:="${ODOM_LEFT_WHEEL_SCALE}"
      right_wheel_scale:="${ODOM_RIGHT_WHEEL_SCALE}"
      angular_velocity_sign:="${ODOM_ANGULAR_VELOCITY_SIGN}"
    )
    runtime_tmux_window "${TMUX_SESSION}" odom "$(join_shell_words odom_cmd_words)"
  else
    runtime_log_status "[INFO] skip wheel odom bridge: START_WHEEL_ODOM=${START_WHEEL_ODOM}; expecting another node to provide /odom"
  fi

  if [[ "${START_MCORE_VELOCITY_SENDER}" == "true" ]]; then
    local mcore_velocity_cmd_words=(
      exec
      roslaunch
      mcore_chassis_bridge
      mcore_velocity_sender.launch
    )
    append_shell_words mcore_velocity_cmd_words "${MCORE_VELOCITY_EXTRA_ARGS}"
    # The action transport must use the reviewed vehicle values. Append these
    # protected arguments last in addition to rejecting duplicate extra args.
    mcore_velocity_cmd_words+=(
      transport:="${MCORE_TRANSPORT}"
      serial_device:="${MCORE_SERIAL_DEVICE}"
      serial_baudrate:="${MCORE_SERIAL_BAUDRATE}"
      tcp_host:="${MCORE_TCP_HOST}"
      tcp_port:="${MCORE_TCP_PORT}"
      cmd_vel_topic:="${MCORE_CMD_VEL_TOPIC}"
      linear_velocity_scale:="${MCORE_LINEAR_VELOCITY_SCALE}"
      angular_velocity_scale:="${MCORE_ANGULAR_VELOCITY_SCALE}"
      linear_velocity_sign:="${MCORE_LINEAR_VELOCITY_SIGN}"
      angular_velocity_sign:="${MCORE_ANGULAR_VELOCITY_SIGN}"
      max_abs_linear_velocity:="${MCORE_MAX_ABS_LINEAR_VELOCITY}"
      max_abs_angular_velocity:="${MCORE_MAX_ABS_ANGULAR_VELOCITY}"
      enable_tx_log:="${MCORE_ENABLE_TX_LOG}"
      enable_rx_log:="${MCORE_ENABLE_RX_LOG}"
    )
    runtime_tmux_window "${TMUX_SESSION}" mcore_velocity "$(join_shell_words mcore_velocity_cmd_words)"
  else
    runtime_log_status "[INFO] skip M-core velocity sender: START_MCORE_VELOCITY_SENDER=${START_MCORE_VELOCITY_SENDER}"
  fi

  if [[ "${START_MCORE_BRIDGE}" == "true" || "${START_STATION_BRIDGE}" == "true" || "${START_DOCK_SUPPLY_MANAGER}" == "true" || "${START_DOCKING_STACK}" == "true" ]]; then
    local hardware_cmd_words=(
      exec
      roslaunch
      robot_hw_bridge
      hardware_bridges.launch
      enable_mcore_bridge:="${START_MCORE_BRIDGE}"
      mcore_enable_cmd_vel:="${MCORE_ENABLE_CMD_VEL}"
      enable_station_bridge:="${START_STATION_BRIDGE}"
      enable_dock_supply_manager:="${START_DOCK_SUPPLY_MANAGER}"
      enable_docking_stack:="${START_DOCKING_STACK}"
      station_server_ip:="${STATION_SERVER_IP}"
      station_server_port:="${STATION_SERVER_PORT}"
      dock_target_dist:="${DOCK_TARGET_DIST}"
      dock_xy_tolerance:="${DOCK_XY_TOLERANCE}"
      dock_yaw_tolerance:="${DOCK_YAW_TOLERANCE}"
      dock_pose_score_thresh:="${DOCK_POSE_SCORE_THRESH}"
      mechanical_connect_enable:="${MECHANICAL_CONNECT_ENABLE}"
      skip_precise_docking_if_station_in_place:="${SKIP_PRECISE_DOCKING_IF_STATION_IN_PLACE}"
      direct_charge_after_precise_docking:="${DIRECT_CHARGE_AFTER_PRECISE_DOCKING}"
      dock_supply_enable_drain:="${DOCK_SUPPLY_ENABLE_DRAIN}"
      dock_supply_enable_refill:="${DOCK_SUPPLY_ENABLE_REFILL}"
      dock_supply_drain_timeout_s:="${DOCK_SUPPLY_DRAIN_TIMEOUT_S}"
      dock_supply_target_clean_level:="${DOCK_SUPPLY_TARGET_CLEAN_LEVEL}"
      dock_supply_refill_timeout_s:="${DOCK_SUPPLY_REFILL_TIMEOUT_S}"
      dock_supply_refill_settle_s:="${DOCK_SUPPLY_REFILL_SETTLE_S}"
      dock_supply_combined_status_wait_s:="${DOCK_SUPPLY_COMBINED_STATUS_WAIT_S}"
      dock_supply_combined_status_stale_timeout_s:="${DOCK_SUPPLY_COMBINED_STATUS_STALE_TIMEOUT_S}"
      target_soc:="${AUTO_CHARGE_TARGET_SOC}"
      charge_voltage_confirm_enable:="${CHARGE_VOLTAGE_CONFIRM_ENABLE}"
    )
    append_shell_words hardware_cmd_words "${HARDWARE_BRIDGES_EXTRA_ARGS}"
    if [[ "${DORAEMON_NO_ACTION_ACCEPTANCE}" == "true" ]]; then
      hardware_cmd_words+=(
        enable_mcore_bridge:=false
        mcore_enable_cmd_vel:=false
        enable_station_bridge:=false
        enable_dock_supply_manager:=false
        enable_docking_stack:=false
        mechanical_connect_enable:=false
        direct_charge_after_precise_docking:=false
        dock_supply_enable_drain:=false
        dock_supply_enable_refill:=false
        charge_voltage_confirm_enable:=false
      )
    fi
    runtime_tmux_window "${TMUX_SESSION}" hardware "$(join_shell_words hardware_cmd_words)"
  else
    runtime_log_status "[INFO] skip legacy hardware bridges: all hardware bridge switches are false"
  fi
  runtime_tmux_window "${TMUX_SESSION}" nav "exec roslaunch cleanrobot mbf_nav.launch start_map_asset_service:=false enable_depth_obstacle_tracking:=${RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING} enable_depth_left_cam:=${RUNTIME_ENABLE_DEPTH_LEFT_CAM} enable_depth_right_cam:=${RUNTIME_ENABLE_DEPTH_RIGHT_CAM} enable_depth_up_cam:=${RUNTIME_ENABLE_DEPTH_UP_CAM} plan_db_path:=${PLAN_DB_PATH} ops_db_path:=${OPS_DB_PATH} maps_root:=${MAPS_ROOT} external_maps_root:=${EXTERNAL_MAPS_ROOT} robot_id:=${ROBOT_ID}"
  local task_cmd_words=(
    exec
    roslaunch
    coverage_task_manager
    task_system.launch
    task_auto_charge_enable:="${TASK_AUTO_CHARGE_ENABLE}"
    executor_auto_charge_enable:="${EXECUTOR_AUTO_CHARGE_ENABLE}"
    default_return_to_dock_on_finish:="${DEFAULT_RETURN_TO_DOCK_ON_FINISH}"
    low_soc:="${AUTO_CHARGE_LOW_SOC}"
    resume_soc:="${AUTO_CHARGE_RESUME_SOC}"
    rearm_soc:="${AUTO_CHARGE_REARM_SOC}"
    dock_calibration_storage_path:="${DOCK_CALIBRATION_STORAGE_PATH}"
    dock_calibration_score_threshold:="${DOCK_POSE_SCORE_THRESH}"
    dock_calibration_target_dist:="${DOCK_TARGET_DIST}"
    dock_calibration_xy_tolerance:="${DOCK_XY_TOLERANCE}"
    auto_charge_monitor_enable:="${AUTO_CHARGE_MONITOR_ENABLE}"
    auto_charge_monitor_reset_on_start:="${AUTO_CHARGE_MONITOR_RESET_ON_START}"
    auto_charge_monitor_count_auto_only:="${AUTO_CHARGE_MONITOR_COUNT_AUTO_ONLY}"
    auto_charge_monitor_recovery_enable:="${AUTO_CHARGE_MONITOR_RECOVERY_ENABLE}"
    auto_charge_monitor_recovery_strategy:="${AUTO_CHARGE_MONITOR_RECOVERY_STRATEGY}"
    auto_charge_monitor_recovery_timeout_s:="${AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S}"
    auto_charge_monitor_recovery_min_soc_delta:="${AUTO_CHARGE_MONITOR_RECOVERY_MIN_SOC_DELTA}"
    auto_charge_monitor_recovery_max_attempts:="${AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS}"
    auto_charge_monitor_recovery_back_distance_m:="${AUTO_CHARGE_MONITOR_RECOVERY_BACK_DISTANCE_M}"
    auto_charge_monitor_recovery_forward_distance_m:="${AUTO_CHARGE_MONITOR_RECOVERY_FORWARD_DISTANCE_M}"
    auto_charge_monitor_recovery_speed_mps:="${AUTO_CHARGE_MONITOR_RECOVERY_SPEED_MPS}"
    auto_charge_monitor_recovery_toggle_charge:="${AUTO_CHARGE_MONITOR_RECOVERY_TOGGLE_CHARGE}"
    auto_charge_monitor_recovery_redock_service:="${AUTO_CHARGE_MONITOR_RECOVERY_REDOCK_SERVICE}"
    auto_charge_monitor_recovery_exhausted_service:="${AUTO_CHARGE_MONITOR_RECOVERY_EXHAUSTED_SERVICE}"
    odometry_health_imu_topic:="${ODOMETRY_HEALTH_IMU_TOPIC}"
    odometry_health_ekf_node_name:="${ODOMETRY_HEALTH_EKF_NODE_NAME}"
    require_mcore_bridge_for_readiness:="${REQUIRE_MCORE_BRIDGE_FOR_READINESS}"
    actuator_debug_require_safety_status:="${ACTUATOR_DEBUG_REQUIRE_SAFETY_STATUS}"
    plan_db_path:="${PLAN_DB_PATH}"
    ops_db_path:="${OPS_DB_PATH}"
    maps_root:="${MAPS_ROOT}"
    robot_id:="${ROBOT_ID}"
  )
  runtime_tmux_window "${TMUX_SESSION}" task "$(join_shell_words task_cmd_words)"

  tmux new-window -t "${TMUX_SESSION}" -n status \
    "bash -lc 'clear; echo \"Doraemon startup status\"; echo; exec tail -n 200 -f \"${STATUS_LOG}\"'"

  tmux set-option -t "${TMUX_SESSION}" remain-on-exit on >/dev/null
}

clear_previous_runtime() {
  runtime_log_status "clear previous runtime"
  runtime_graceful_stop_runtime
  runtime_kill_runtime_nodes
  runtime_kill_runtime_processes
  runtime_kill_runtime_tmux_sessions
}

clear_residual_state() {
  runtime_log_status "clear residual task and dock state"
  runtime_stop_task_execution_if_available
  runtime_run_ros_cli rosservice call /dock_supply/cancel '{}' >/dev/null 2>&1 || true
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
  finalize_depth_camera_supervision
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

  finalize_depth_camera_supervision
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
  validate_commercial_vehicle_identity
  apply_no_action_acceptance_overrides
  validate_external_runtime_log_paths
  mkdir -p "${LOG_DIR}"
  : > "${STATUS_LOG}"
  runtime_common_init

  runtime_log_status "startup begin"
  runtime_log_status "chassis driver: ${CHASSIS_DRIVER}"
  runtime_log_status "workspace layout: ${DORAEMON_WORKSPACE_LAYOUT:-unknown}"
  runtime_log_status "runtime config: ${DORAEMON_RUNTIME_CONFIG_FILE} loaded=${DORAEMON_RUNTIME_CONFIG_LOADED}"
  log_effective_runtime_parameters
  STARTUP_TRANSACTION_STARTED=1
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
  assert_no_action_runtime_isolated

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
    if [[ "${RUNTIME_ENABLE_DEPTH_LEFT_CAM}" == "true" ]]; then
      runtime_warn_if_optional_node_missing /left/gs_node "左侧深度避障节点" 8
      runtime_warn_if_optional_topic_missing /left/obstacle_2d "左侧深度障碍输出" 8 3
    fi
    if [[ "${RUNTIME_ENABLE_DEPTH_RIGHT_CAM}" == "true" ]]; then
      runtime_warn_if_optional_node_missing /right/gs_node "右侧深度避障节点" 8
      runtime_warn_if_optional_topic_missing /right/obstacle_2d "右侧深度障碍输出" 8 3
    fi
    if [[ "${RUNTIME_ENABLE_DEPTH_UP_CAM}" == "true" ]]; then
      runtime_warn_if_optional_node_missing /up/gs_node "前向深度避障节点" 8
      runtime_warn_if_optional_topic_missing /up/obstacle_2d "前向深度障碍输出" 8 3
    fi
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
    finalize_depth_camera_supervision
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
  finalize_depth_camera_supervision

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

  STARTUP_COMMITTED=0
  STARTUP_TRANSACTION_STARTED=0
  cleanup_failed_startup() {
    local exit_code="$?"
    trap - EXIT INT TERM
    if (( exit_code != 0 && STARTUP_COMMITTED == 0 && STARTUP_TRANSACTION_STARTED == 1 )); then
      set +e
      runtime_log_status "[ERROR] startup failed; cleaning ROS nodes before tmux sessions"
      STOP_MASTER=1 "${SCRIPT_DIR}/stop_all_backend.sh"
    fi
    exit "${exit_code}"
  }
  trap cleanup_failed_startup EXIT
  trap 'exit 143' TERM
  trap 'exit 130' INT

  main
  STARTUP_COMMITTED=1
  trap - EXIT INT TERM
fi

true
