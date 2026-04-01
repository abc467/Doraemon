#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/cartographer_runtime_common.sh"

RVIZ_CONFIG="${RVIZ_CONFIG:-${DEFAULT_SLAM_RVIZ_CONFIG}}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-30}"

main() {
    local response=""

    setup_runtime_env
    ensure_cartographer_runtime "${WAIT_TIMEOUT_SEC}"
    wait_for_service /set_param "${WAIT_TIMEOUT_SEC}"

    echo "[INFO] Loading slam config..."
    response="$(call_visual_command "load_config" '{"config_entry":"slam"}')"
    print_info_response "ROS server response for load_config:" "${response}"

    echo "[INFO] Adding trajectory..."
    response="$(call_visual_command "add_trajectory" '')"
    print_info_response "ROS server response for add_trajectory:" "${response}"

    echo "[INFO] Launching RViz with ${RVIZ_CONFIG}"
    launch_rviz "${RVIZ_CONFIG}"
}

main "$@"
