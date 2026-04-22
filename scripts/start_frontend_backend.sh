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
ROSBRIDGE_ADDRESS="${ROSBRIDGE_ADDRESS:-0.0.0.0}"
ROSBRIDGE_PORT="${ROSBRIDGE_PORT:-9090}"
START_MAP_ASSET_SERVICE="${START_MAP_ASSET_SERVICE:-true}"
ENABLE_SITE_EDITOR_SERVICE="${ENABLE_SITE_EDITOR_SERVICE:-true}"
ENABLE_RECT_ZONE_PLANNER="${ENABLE_RECT_ZONE_PLANNER:-false}"
FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-true}"

usage() {
  cat <<'EOF'
Usage: start_frontend_backend.sh

Official frontend/backend-only bringup entry for Doraemon.
By default this mode enables odometry_health inside frontend_editor_backend.launch.
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
  enable_odometry_health:="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH}"
