#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/source_slam_runtime_env.sh"

NODE_WAIT_TIMEOUT_SEC="${NODE_WAIT_TIMEOUT_SEC:-5}"

node_is_running() {
    local node_name="$1"
    rosnode ping -c 1 "${node_name}" >/dev/null 2>&1
}

wait_for_node_exit() {
    local node_name="$1"
    local timeout_sec="${2:-5}"
    local start_ts

    start_ts="$(date +%s)"
    while node_is_running "${node_name}"; do
        if (( "$(date +%s)" - start_ts >= timeout_sec )); then
            echo "[WARN] ${node_name} 在 ${timeout_sec}s 内未退出。"
            return 1
        fi
        sleep 0.2
    done

    return 0
}

kill_node_via_ros() {
    local node_name="$1"

    if ! node_is_running "${node_name}"; then
        echo "[INFO] ${node_name} 未在 ROS 中注册。"
        return 0
    fi

    echo "[INFO] 正在通过 rosnode kill 关闭 ${node_name}..."
    rosnode kill "${node_name}" >/dev/null 2>&1 || true

    if wait_for_node_exit "${node_name}" "${NODE_WAIT_TIMEOUT_SEC}"; then
        echo "[INFO] ${node_name} 已关闭。"
        return 0
    fi

    return 1
}

process_exists() {
    local pattern="$1"
    pgrep -f -- "${pattern}" >/dev/null 2>&1
}

kill_process_pattern() {
    local pattern="$1"
    local label="$2"
    local timeout_sec="${3:-5}"
    local start_ts

    if ! process_exists "${pattern}"; then
        return 0
    fi

    echo "[INFO] 检测到残留进程，正在发送 SIGTERM 给 ${label}..."
    pkill -TERM -f -- "${pattern}" >/dev/null 2>&1 || true

    start_ts="$(date +%s)"
    while process_exists "${pattern}"; do
        if (( "$(date +%s)" - start_ts >= timeout_sec )); then
            echo "[WARN] ${label} 未在 ${timeout_sec}s 内退出，发送 SIGKILL。"
            pkill -KILL -f -- "${pattern}" >/dev/null 2>&1 || true
            break
        fi
        sleep 0.2
    done
}

main() {
    local ros_available=0

    source_slam_runtime_env "${REPO_ROOT}"

    if rosnode list >/dev/null 2>&1; then
        ros_available=1
    else
        echo "[WARN] 当前无法连接 ROS Master，将直接按进程名清理。"
    fi

    if (( ros_available == 1 )); then
        kill_node_via_ros "/cartographer_occupancy_grid_node" || true
        kill_node_via_ros "/cartographer_node" || true
    fi

    kill_process_pattern "(^|/)cartographer_occupancy_grid_node($| )" "cartographer_occupancy_grid_node"
    kill_process_pattern "(^|/)cartographer_node($| )" "cartographer_node"

    echo "[INFO] cartographer 节点关闭流程已完成。"
}

main "$@"
