#!/bin/bash

_CARTO_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${_CARTO_COMMON_DIR}/source_slam_runtime_env.sh"

REPO_ROOT="$(cd "${_CARTO_COMMON_DIR}/.." && pwd)"
MAP_DIR="${REPO_ROOT}/map"
LOG_DIR="${LOG_DIR:-${REPO_ROOT}/log}"
DEFAULT_RVIZ_CONFIG="${REPO_ROOT}/src/cartographer_ros/cartographer_ros/configuration_files/demo_2d.rviz"
DEFAULT_LOC_RVIZ_CONFIG="${REPO_ROOT}/rviz/loc.rviz"
DEFAULT_SLAM_RVIZ_CONFIG="${REPO_ROOT}/rviz/slam_live_submaps.rviz"

RESOLVED_MAP_FILENAME=""
RESOLVED_MAP_PATH=""
RUNTIME_LAUNCH_PID=""
RUNTIME_LOG=""
ODOM_LAUNCH_PID=""
ODOM_LOG=""

setup_runtime_env() {
    source_slam_runtime_env "${REPO_ROOT}"
}

report_launch_failure() {
    local label="${1:-process}"
    local logfile="${2:-}"

    if [[ -n "${logfile}" && -f "${logfile}" ]]; then
        echo "[ERROR] ${label} failed. Recent log output:" >&2
        tail -n 40 "${logfile}" >&2 || true
        echo "[ERROR] Full log: ${logfile}" >&2
    else
        echo "[ERROR] ${label} failed." >&2
    fi
}

wait_for_service() {
    local service_name="$1"
    local timeout_sec="${2:-30}"
    local watched_pid="${3:-}"
    local watched_label="${4:-process}"
    local watched_log="${5:-}"
    local start_ts

    start_ts="$(date +%s)"
    until rosservice type "${service_name}" >/dev/null 2>&1; do
        if [[ -n "${watched_pid}" ]] && ! kill -0 "${watched_pid}" >/dev/null 2>&1; then
            report_launch_failure "${watched_label}" "${watched_log}"
            return 1
        fi

        if (( "$(date +%s)" - start_ts >= timeout_sec )); then
            echo "[ERROR] Service ${service_name} was not ready within ${timeout_sec}s." >&2
            report_launch_failure "${watched_label}" "${watched_log}"
            return 1
        fi
        sleep 1
    done
}

node_is_running() {
    local node_name="$1"
    rosnode ping -c 1 "${node_name}" >/dev/null 2>&1
}

wait_for_ros_node() {
    local node_name="$1"
    local timeout_sec="${2:-30}"
    local watched_pid="${3:-}"
    local watched_label="${4:-process}"
    local watched_log="${5:-}"
    local start_ts

    start_ts="$(date +%s)"
    until node_is_running "${node_name}"; do
        if [[ -n "${watched_pid}" ]] && ! kill -0 "${watched_pid}" >/dev/null 2>&1; then
            report_launch_failure "${watched_label}" "${watched_log}"
            return 1
        fi

        if (( "$(date +%s)" - start_ts >= timeout_sec )); then
            echo "[ERROR] Node ${node_name} was not ready within ${timeout_sec}s." >&2
            report_launch_failure "${watched_label}" "${watched_log}"
            return 1
        fi
        sleep 1
    done
}

call_visual_command() {
    local command="$1"
    local payload_json="$2"
    local request=""
    local output=""
    local code=""
    local msg=""

    printf -v request "command: '%s'\npayload_json: '%s'" "${command}" "${payload_json}"
    output="$(rosservice call /visual_command "${request}" 2>&1)" || {
        echo "[ERROR] /visual_command ${command} call failed:" >&2
        echo "${output}" >&2
        return 1
    }

    code="$(printf '%s\n' "${output}" | awk '/^code:/{print $2; exit}')"
    msg="$(printf '%s\n' "${output}" | awk -F'msg: ' '/^msg:/{print $2; exit}')"
    if [[ "${code}" != "0" ]]; then
        echo "[ERROR] /visual_command ${command} returned code=${code:-unknown} msg=${msg:-<empty>}." >&2
        echo "${output}" >&2
        return 1
    fi

    printf '%s\n' "${output}"
}

print_info_response() {
    local label="$1"
    local response="${2:-}"

    echo "[INFO] ${label}"
    if [[ -z "${response}" ]]; then
        echo "[INFO]   <empty response>"
        return 0
    fi

    while IFS= read -r line; do
        echo "[INFO]   ${line}"
    done <<< "${response}"
}

call_set_param() {
    local key="$1"
    local value="$2"
    local request=""
    local output=""
    local opcode=""
    local msg=""

    printf -v request "{keys: ['%s'], vals: ['%s']}" "${key}" "${value}"
    output="$(rosservice call /set_param "${request}" 2>&1)" || {
        echo "[ERROR] /set_param ${key} call failed:" >&2
        echo "${output}" >&2
        return 1
    }

    opcode="$(printf '%s\n' "${output}" | awk '/^opcode:/{print $2; exit}')"
    msg="$(printf '%s\n' "${output}" | awk -F'msg: ' '/^msg:/{print $2; exit}')"
    if [[ -n "${opcode}" && "${opcode}" != "0" ]]; then
        echo "[ERROR] /set_param ${key} returned opcode=${opcode} msg=${msg:-<empty>}." >&2
        echo "${output}" >&2
        return 1
    fi

    printf '%s\n' "${output}"
}

launch_rviz() {
    local rviz_config="${1:-${DEFAULT_RVIZ_CONFIG}}"

    if [[ ! -f "${rviz_config}" ]]; then
        echo "[ERROR] RViz config does not exist: ${rviz_config}" >&2
        return 1
    fi

    if ! command -v rviz >/dev/null 2>&1; then
        echo "[ERROR] rviz is not available in PATH." >&2
        return 1
    fi

    rviz -d "${rviz_config}" >/dev/null 2>&1 &
}

resolve_map_filename() {
    local map_arg="$1"
    local candidate=""
    local candidate_path=""
    local candidate_real=""
    local map_dir_real=""

    if [[ -z "${map_arg}" ]]; then
        echo "[ERROR] Map filename is required." >&2
        return 1
    fi

    if [[ -f "${map_arg}" ]]; then
        candidate="${map_arg}"
    elif [[ -f "${REPO_ROOT}/${map_arg}" ]]; then
        candidate="${REPO_ROOT}/${map_arg}"
    elif [[ -f "${MAP_DIR}/${map_arg}" ]]; then
        candidate="${MAP_DIR}/${map_arg}"
    else
        echo "[ERROR] Map file not found: ${map_arg}" >&2
        echo "[ERROR] Looked under ${REPO_ROOT} and ${MAP_DIR}." >&2
        return 1
    fi

    candidate_path="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "${candidate}")"
    candidate_real="$(readlink -f "${candidate_path}")"

    if [[ ! -r "${candidate_real}" ]]; then
        echo "[ERROR] Map file is not readable: ${candidate_real}" >&2
        return 1
    fi
    if [[ ! -s "${candidate_real}" ]]; then
        echo "[ERROR] Map file is empty: ${candidate_real}" >&2
        return 1
    fi

    map_dir_real="$(readlink -f "${MAP_DIR}")"
    case "${candidate_path}" in
        "${map_dir_real}"/*)
            ;;
        *)
            echo "[ERROR] Map file must be located under ${MAP_DIR} because /visual_command loads from ${MAP_DIR}." >&2
            return 1
            ;;
    esac

    RESOLVED_MAP_PATH="${candidate_path}"
    RESOLVED_MAP_FILENAME="$(basename "${candidate_path}")"
}

ensure_cartographer_runtime() {
    local timeout_sec="${1:-30}"

    if rosservice type /visual_command >/dev/null 2>&1; then
        return 0
    fi

    mkdir -p "${LOG_DIR}"

    if pgrep -f -- "${REPO_ROOT}/scripts/run_cartographer_nodes.sh" >/dev/null 2>&1; then
        echo "[INFO] Waiting for existing cartographer runtime to become ready..."
    else
        RUNTIME_LOG="${LOG_DIR}/run_cartographer_nodes.$(date +%F_%H-%M-%S).log"
        echo "[INFO] Starting cartographer runtime via ${REPO_ROOT}/scripts/run_cartographer_nodes.sh"
        "${REPO_ROOT}/scripts/run_cartographer_nodes.sh" >"${RUNTIME_LOG}" 2>&1 </dev/null &
        RUNTIME_LAUNCH_PID=$!
        echo "[INFO] cartographer runtime log: ${RUNTIME_LOG}"
    fi

    wait_for_service /visual_command "${timeout_sec}" "${RUNTIME_LAUNCH_PID}" "cartographer runtime" "${RUNTIME_LOG}"
}

launch_wheel_speed_odom() {
    mkdir -p "${LOG_DIR}"
    ODOM_LOG="${LOG_DIR}/wheel_speed_odom.$(date +%F_%H-%M-%S).log"

    (
        source "${REPO_ROOT}/scripts/source_slam_runtime_env.sh"
        source_slam_runtime_env "${REPO_ROOT}"
        cd "${REPO_ROOT}"
        exec roslaunch wheel_speed_odom_bridge wheel_speed_odom.launch
    ) >"${ODOM_LOG}" 2>&1 </dev/null &

    ODOM_LAUNCH_PID=$!
    echo "[INFO] wheel_speed_odom launch log: ${ODOM_LOG}"
}

stop_wheel_speed_odom() {
    local stopped=0

    if pgrep -f -- "roslaunch wheel_speed_odom_bridge wheel_speed_odom.launch" >/dev/null 2>&1; then
        pkill -f -- "roslaunch wheel_speed_odom_bridge wheel_speed_odom.launch" >/dev/null 2>&1 || true
        stopped=1
    fi

    for node_name in \
        /wheel_speed_odom \
        /ekf_covariance_override \
        /wheel_speed_odom_ekf_conservative \
        /wheel_speed_odom_ekf_balanced \
        /wheel_speed_odom_ekf_aggressive
    do
        if node_is_running "${node_name}"; then
            rosnode kill "${node_name}" >/dev/null 2>&1 || true
            stopped=1
        fi
    done

    if (( stopped == 1 )); then
        sleep 1
    fi
}

ensure_wheel_speed_odom() {
    local timeout_sec="${1:-30}"

    if node_is_running /wheel_speed_odom; then
        echo "[INFO] Existing /wheel_speed_odom detected, restarting it for a clean state..."
        stop_wheel_speed_odom
    else
        echo "[INFO] /wheel_speed_odom is not running, starting it now..."
    fi

    launch_wheel_speed_odom
    wait_for_ros_node /wheel_speed_odom "${timeout_sec}" "${ODOM_LAUNCH_PID}" "wheel_speed_odom launch" "${ODOM_LOG}"
}
