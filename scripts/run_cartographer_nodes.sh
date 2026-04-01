#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

set +u
source /opt/ros/noetic/setup.bash
set -u

if [[ -f "${REPO_ROOT}/install/setup.bash" ]]; then
    WORKSPACE_SETUP="${REPO_ROOT}/install/setup.bash"
    BIN_ROOT="${REPO_ROOT}/install/lib"
elif [[ -f "${REPO_ROOT}/devel/setup.bash" ]]; then
    WORKSPACE_SETUP="${REPO_ROOT}/devel/setup.bash"
    BIN_ROOT="${REPO_ROOT}/devel/lib"
else
    echo "[ERROR] 未找到 install/setup.bash 或 devel/setup.bash，请先编译工程。" >&2
    exit 1
fi

set +u
source "${WORKSPACE_SETUP}"
set -u

export SLAM_ROOT="${REPO_ROOT}"
export ROS_MASTER_URI="${ROS_MASTER_URI:-http://localhost:11311}"

LOG_DIR="${LOG_DIR:-${REPO_ROOT}/log}"
PARAM_SPACE_NODE="${BIN_ROOT}/param_space/param_space_node"
CARTOGRAPHER_NODE="${BIN_ROOT}/cartographer_ros/cartographer_node"
OCCUPANCY_GRID_NODE="${BIN_ROOT}/cartographer_ros/cartographer_occupancy_grid_node"
INCLUDE_FROZEN_SUBMAPS="${INCLUDE_FROZEN_SUBMAPS:-true}"
INCLUDE_UNFROZEN_SUBMAPS="${INCLUDE_UNFROZEN_SUBMAPS:-false}"

PARAM_SPACE_PID=""
CARTOGRAPHER_PID=""
OCCUPANCY_GRID_PID=""
ROSCORE_PID=""
STARTED_PARAM_SPACE=0
STARTED_ROSCORE=0
EXITING=0

mkdir -p "${LOG_DIR}"

check_pid_running() {
    local pid="${1:-}"
    [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

check_process() {
    local pattern="$1"
    pgrep -f -- "${pattern}" >/dev/null 2>&1
}

kill_process() {
    local pattern="$1"
    if check_process "${pattern}"; then
        pkill -f -- "${pattern}" >/dev/null 2>&1 || true
    fi
}

wait_for_roscore() {
    local timeout_sec="${1:-30}"
    local start_ts
    start_ts="$(date +%s)"

    until rosnode list >/dev/null 2>&1; do
        if (( "$(date +%s)" - start_ts >= timeout_sec )); then
            echo "[ERROR] roscore 在 ${timeout_sec}s 内未就绪，请检查 ${LOG_DIR}/roscore.log" >&2
            return 1
        fi
        sleep 1
    done
}

wait_for_service() {
    local service_name="$1"
    local timeout_sec="${2:-15}"
    local start_ts
    start_ts="$(date +%s)"

    until rosservice type "${service_name}" >/dev/null 2>&1; do
        if (( "$(date +%s)" - start_ts >= timeout_sec )); then
            echo "[ERROR] 服务 ${service_name} 在 ${timeout_sec}s 内未就绪。" >&2
            return 1
        fi
        sleep 1
    done
}

ensure_binary_exists() {
    local binary="$1"
    if [[ ! -x "${binary}" ]]; then
        echo "[ERROR] 可执行文件不存在或不可执行: ${binary}" >&2
        exit 1
    fi
}

ensure_no_conflicts() {
    local conflicts=0
    local patterns=(
        "(^|/)cartographer_node($| )"
        "(^|/)cartographer_occupancy_grid_node($| )"
    )

    local pattern
    for pattern in "${patterns[@]}"; do
        if check_process "${pattern}"; then
            echo "[ERROR] 检测到已有进程正在运行: ${pattern}" >&2
            conflicts=1
        fi
    done

    if (( conflicts != 0 )); then
        echo "[ERROR] 请先关闭已有 cartographer 相关进程，再运行本脚本。" >&2
        return 1
    fi
}

start_roscore_if_needed() {
    if rosnode list >/dev/null 2>&1; then
        echo "[INFO] roscore 已在运行，直接复用。"
        return 0
    fi

    if check_process "${PARAM_SPACE_NODE}"; then
        echo "[INFO] 检测到旧的 param_space_node 进程，启动 roscore 前先清理..."
        kill_process "${PARAM_SPACE_NODE}"
    fi

    echo "[INFO] roscore 未运行，正在启动..."
    roscore >"${LOG_DIR}/roscore.log" 2>&1 &
    ROSCORE_PID=$!
    STARTED_ROSCORE=1
    wait_for_roscore
    echo "[INFO] roscore 已启动，PID=${ROSCORE_PID}，日志=${LOG_DIR}/roscore.log"
}

print_log_excerpt() {
    local label="$1"
    local logfile="$2"

    if [[ -f "${logfile}" ]]; then
        echo "[INFO] ${label} 最近日志:" >&2
        tail -n 20 "${logfile}" >&2 || true
    fi
}

start_param_space_if_needed() {
    local param_space_log="${LOG_DIR}/param_space_node.log"

    ensure_binary_exists "${PARAM_SPACE_NODE}"

    if rosservice type /set_param >/dev/null 2>&1 &&
       rosservice type /get_param >/dev/null 2>&1; then
        echo "[INFO] /set_param 和 /get_param 已就绪，直接复用现有 param_space 服务。"
        return 0
    fi

    if check_process "${PARAM_SPACE_NODE}"; then
        echo "[INFO] 检测到已有 param_space_node 进程，等待服务注册..."
    else
        "${PARAM_SPACE_NODE}" >"${param_space_log}" 2>&1 &
        PARAM_SPACE_PID=$!
        STARTED_PARAM_SPACE=1
        echo "[INFO] param_space_node 已启动，PID=${PARAM_SPACE_PID}，日志=${param_space_log}"
    fi

    wait_for_service /set_param 15 || {
        print_log_excerpt "param_space_node" "${param_space_log}"
        return 1
    }
    wait_for_service /get_param 15 || {
        print_log_excerpt "param_space_node" "${param_space_log}"
        return 1
    }
}

start_cartographer_nodes() {
    local cartographer_log="${LOG_DIR}/cartographer_node.log"
    local occupancy_log="${LOG_DIR}/cartographer_occupancy_grid_node.log"

    ensure_binary_exists "${PARAM_SPACE_NODE}"
    ensure_binary_exists "${CARTOGRAPHER_NODE}"
    ensure_binary_exists "${OCCUPANCY_GRID_NODE}"

    "${CARTOGRAPHER_NODE}" >"${cartographer_log}" 2>&1 &
    CARTOGRAPHER_PID=$!
    echo "[INFO] cartographer_node 已启动，PID=${CARTOGRAPHER_PID}，日志=${cartographer_log}"

    "${OCCUPANCY_GRID_NODE}" \
        --include_frozen_submaps="${INCLUDE_FROZEN_SUBMAPS}" \
        --include_unfrozen_submaps="${INCLUDE_UNFROZEN_SUBMAPS}" \
        >"${occupancy_log}" 2>&1 &
    OCCUPANCY_GRID_PID=$!
    echo "[INFO] cartographer_occupancy_grid_node 已启动，PID=${OCCUPANCY_GRID_PID}，日志=${occupancy_log}"

    sleep 1

    if ! check_pid_running "${CARTOGRAPHER_PID}"; then
        echo "[ERROR] cartographer_node 启动后立即退出。" >&2
        print_log_excerpt "cartographer_node" "${cartographer_log}"
        return 1
    fi

    if ! check_pid_running "${OCCUPANCY_GRID_PID}"; then
        echo "[ERROR] cartographer_occupancy_grid_node 启动后立即退出。" >&2
        print_log_excerpt "cartographer_occupancy_grid_node" "${occupancy_log}"
        return 1
    fi
}

stop_pid() {
    local pid="${1:-}"
    local label="$2"

    if ! check_pid_running "${pid}"; then
        return 0
    fi

    echo "[INFO] 正在关闭 ${label} (PID=${pid})..."
    kill "${pid}" >/dev/null 2>&1 || true

    local deadline=$((SECONDS + 5))
    while check_pid_running "${pid}"; do
        if (( SECONDS >= deadline )); then
            echo "[WARN] ${label} 未在 5s 内退出，发送 SIGKILL。"
            kill -9 "${pid}" >/dev/null 2>&1 || true
            break
        fi
        sleep 0.2
    done

    wait "${pid}" 2>/dev/null || true
}

cleanup() {
    if (( EXITING )); then
        return
    fi
    EXITING=1

    echo
    echo "[INFO] 正在停止 cartographer 相关进程..."
    stop_pid "${OCCUPANCY_GRID_PID}" "cartographer_occupancy_grid_node"
    stop_pid "${CARTOGRAPHER_PID}" "cartographer_node"
    if (( STARTED_PARAM_SPACE == 1 )); then
        stop_pid "${PARAM_SPACE_PID}" "param_space_node"
    fi

    if (( STARTED_ROSCORE == 1 )); then
        stop_pid "${ROSCORE_PID}" "roscore"
    fi

    echo "[INFO] 清理完成。"
}

monitor_nodes() {
    while true; do
        if ! rosservice type /set_param >/dev/null 2>&1 ||
           ! rosservice type /get_param >/dev/null 2>&1; then
            echo "[ERROR] param_space 服务不可用，请检查 ${LOG_DIR}/param_space_node.log" >&2
            print_log_excerpt "param_space_node" "${LOG_DIR}/param_space_node.log"
            return 1
        fi

        if ! check_pid_running "${CARTOGRAPHER_PID}"; then
            echo "[ERROR] cartographer_node 已退出，请检查 ${LOG_DIR}/cartographer_node.log" >&2
            print_log_excerpt "cartographer_node" "${LOG_DIR}/cartographer_node.log"
            return 1
        fi

        if ! check_pid_running "${OCCUPANCY_GRID_PID}"; then
            echo "[ERROR] cartographer_occupancy_grid_node 已退出，请检查 ${LOG_DIR}/cartographer_occupancy_grid_node.log" >&2
            print_log_excerpt "cartographer_occupancy_grid_node" "${LOG_DIR}/cartographer_occupancy_grid_node.log"
            return 1
        fi

        sleep 1
    done
}

trap 'exit 130' INT
trap 'exit 143' TERM
trap cleanup EXIT

main() {
    cd "${REPO_ROOT}"

    echo "[INFO] REPO_ROOT=${REPO_ROOT}"
    echo "[INFO] WORKSPACE_SETUP=${WORKSPACE_SETUP}"
    echo "[INFO] BIN_ROOT=${BIN_ROOT}"
    echo "[INFO] SLAM_ROOT=${SLAM_ROOT}"

    ensure_no_conflicts
    start_roscore_if_needed
    start_param_space_if_needed
    start_cartographer_nodes

    echo "[INFO] param_space_node、cartographer_node 和 cartographer_occupancy_grid_node 已运行，按 Ctrl+C 可关闭。"
    monitor_nodes
}

main "$@"
