#!/bin/bash

_SOURCE_SLAM_RUNTIME_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

default_ros_machine_ip() {
    local ips ip
    ips="$(hostname -I 2>/dev/null)"

    for ip in $ips; do
        case "$ip" in
            10.*)
                echo "$ip"
                return 0
                ;;
        esac
    done

    for ip in $ips; do
        case "$ip" in
            192.168.*)
                echo "$ip"
                return 0
                ;;
        esac
    done

    echo "$ips" | awk '{print $1}'
}

apply_ros_network_env() {
    local machine_ip="${ROS_MACHINE_IP:-}"

    if [[ -z "${machine_ip}" ]]; then
        machine_ip="$(default_ros_machine_ip)"
    fi

    if [[ -n "${machine_ip}" ]]; then
        export ROS_MACHINE_IP="${machine_ip}"
        export ROS_IP="${ROS_IP:-$machine_ip}"
        export ROS_HOSTNAME="${ROS_HOSTNAME:-$machine_ip}"
        export ROS_MASTER_URI="${ROS_MASTER_URI:-http://$machine_ip:11311}"
    fi
}

is_slam_config_root() {
    local candidate="${1:-}"
    [[ -n "${candidate}" ]] || return 1
    [[ -f "${candidate}/slam/config.lua" ]] || return 1
    [[ -f "${candidate}/pure_location_odom/config.lua" ]] || return 1
    [[ -f "${candidate}/relocalization/global_relocation.sml" ]] || return 1
}

resolve_slam_config_root() {
    local repo_root="${1:-${_SOURCE_SLAM_RUNTIME_ENV_DIR}/..}"
    local deployment_root="/data/config/slam/cartographer"
    local canonical_root="${repo_root}/src/cleanrobot/config/slam/cartographer"

    if is_slam_config_root "${deployment_root}"; then
        echo "${deployment_root}"
        return 0
    fi

    if is_slam_config_root "${canonical_root}"; then
        echo "${canonical_root}"
        return 0
    fi

    echo "[ERROR] No valid SLAM config root found." >&2
    echo "[ERROR] Checked deployment override: ${deployment_root}" >&2
    echo "[ERROR] Checked canonical source root: ${canonical_root}" >&2
    return 1
}

resolve_workspace_setup() {
    local repo_root="${1:-${_SOURCE_SLAM_RUNTIME_ENV_DIR}/..}"
    repo_root="$(cd "${repo_root}" && pwd)"

    if [[ -f "${repo_root}/devel/setup.bash" ]]; then
        echo "${repo_root}/devel/setup.bash"
        return 0
    fi

    if [[ -f "${repo_root}/install/setup.bash" ]]; then
        echo "${repo_root}/install/setup.bash"
        return 0
    fi

    echo "[ERROR] Could not find devel/setup.bash (preferred) or install/setup.bash (fallback) under ${repo_root}." >&2
    return 1
}

workspace_layout_from_setup() {
    local workspace_setup="${1:-}"

    case "${workspace_setup}" in
        */install/setup.bash)
            echo "install"
            ;;
        */devel/setup.bash)
            echo "devel"
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

resolve_workspace_lib_root() {
    local repo_root="${1:-${_SOURCE_SLAM_RUNTIME_ENV_DIR}/..}"
    local workspace_setup=""

    repo_root="$(cd "${repo_root}" && pwd)"
    workspace_setup="$(resolve_workspace_setup "${repo_root}")" || return 1

    case "${workspace_setup}" in
        "${repo_root}/install/setup.bash")
            echo "${repo_root}/install/lib"
            ;;
        "${repo_root}/devel/setup.bash")
            echo "${repo_root}/devel/lib"
            ;;
        *)
            echo "[ERROR] Unsupported workspace setup path: ${workspace_setup}" >&2
            return 1
            ;;
    esac
}

source_slam_runtime_env() {
    local repo_root="${1:-${_SOURCE_SLAM_RUNTIME_ENV_DIR}/..}"
    local workspace_setup=""
    local workspace_lib_root=""

    repo_root="$(cd "${repo_root}" && pwd)"

    set +u
    source /opt/ros/noetic/setup.bash

    workspace_setup="$(resolve_workspace_setup "${repo_root}")" || return 1
    workspace_lib_root="$(resolve_workspace_lib_root "${repo_root}")" || return 1
    source "${workspace_setup}"

    apply_ros_network_env
    set -u

    export SLAM_ROOT="${repo_root}"
    if [[ -z "${SLAM_CONFIG_ROOT:-}" ]]; then
        local resolved_config_root=""
        resolved_config_root="$(resolve_slam_config_root "${repo_root}")" || return 1
        export SLAM_CONFIG_ROOT="${resolved_config_root}"
    fi
    export SLAM_MAP_ROOT="${SLAM_MAP_ROOT:-${MAPS_ROOT:-/data/maps}}"
    export WORKSPACE_SETUP="${workspace_setup}"
    export WORKSPACE_LIB_ROOT="${workspace_lib_root}"
    export WORKSPACE_LAYOUT="$(workspace_layout_from_setup "${workspace_setup}")"
    export ROS_MASTER_URI="${ROS_MASTER_URI:-http://localhost:11311}"
}
