#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/cartographer_runtime_common.sh"

RVIZ_CONFIG="${RVIZ_CONFIG:-${DEFAULT_LOC_RVIZ_CONFIG}}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-30}"
SKIP_RVIZ="${SKIP_RVIZ:-0}"

usage() {
    echo "Usage: $0 <pbstream file>" >&2
    echo "Examples:" >&2
    echo "  $0 0330-1.pbstream" >&2
    echo "  $0 map/0330-1.pbstream" >&2
}

main() {
    local map_arg="${1:-}"
    local response=""

    if [[ -z "${map_arg}" ]]; then
        usage
        exit 1
    fi

    setup_runtime_env
    resolve_map_filename "${map_arg}"
    ensure_cartographer_runtime "${WAIT_TIMEOUT_SEC}"
    wait_for_service /set_param "${WAIT_TIMEOUT_SEC}"
    wait_for_service /get_param "${WAIT_TIMEOUT_SEC}"

    echo "[INFO] Loading pure_location_odom config..."
    response="$(call_visual_command "load_config" '{"config_entry":"pure_location_odom"}')"
    print_info_response "ROS server response for load_config:" "${response}"

    echo "[INFO] Loading map state: ${RESOLVED_MAP_FILENAME}"
    response="$(call_visual_command "load_state" "{\"filename\":\"${RESOLVED_MAP_FILENAME}\",\"frozen\":true}")"
    print_info_response "ROS server response for load_state:" "${response}"

    ensure_wheel_speed_odom "${WAIT_TIMEOUT_SEC}"

    echo "[INFO] Adding trajectory..."
    response="$(call_visual_command "add_trajectory" '')"
    print_info_response "ROS server response for add_trajectory:" "${response}"

    echo "[INFO] Triggering global relocation..."
    response="$(call_visual_command "try_global_relocate" '')"
    print_info_response "ROS server response for try_global_relocate:" "${response}"

    echo "[INFO] Setting global_relocated=true..."
    response="$(call_set_param "global_relocated" "true")"
    print_info_response "ROS server response for /set_param global_relocated=true:" "${response}"

    if [[ "${SKIP_RVIZ}" == "1" || "${SKIP_RVIZ,,}" == "true" ]]; then
        echo "[INFO] SKIP_RVIZ=${SKIP_RVIZ}, skip launching RViz."
    else
        echo "[INFO] Launching RViz with ${RVIZ_CONFIG}"
        launch_rviz "${RVIZ_CONFIG}"
    fi
}

main "$@"
