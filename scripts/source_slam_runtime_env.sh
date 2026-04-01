#!/bin/bash

_SOURCE_SLAM_RUNTIME_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source_slam_runtime_env() {
    local repo_root="${1:-${_SOURCE_SLAM_RUNTIME_ENV_DIR}/..}"
    local workspace_setup=""

    repo_root="$(cd "${repo_root}" && pwd)"

    set +u
    source /opt/ros/noetic/setup.bash

    if [[ -f "${repo_root}/install/setup.bash" ]]; then
        workspace_setup="${repo_root}/install/setup.bash"
    elif [[ -f "${repo_root}/devel/setup.bash" ]]; then
        workspace_setup="${repo_root}/devel/setup.bash"
    else
        echo "[ERROR] Could not find install/setup.bash or devel/setup.bash under ${repo_root}." >&2
        return 1
    fi

    source "${workspace_setup}"

    if [[ -f "${repo_root}/scripts/ros_network_env.sh" ]]; then
        source "${repo_root}/scripts/ros_network_env.sh"
    fi
    set -u

    export SLAM_ROOT="${repo_root}"
    export WORKSPACE_SETUP="${workspace_setup}"
    export ROS_MASTER_URI="${ROS_MASTER_URI:-http://localhost:11311}"
}
