#!/usr/bin/env bash

# Pure, side-effect-free validation shared by the systemd preflight and the
# direct runtime entrypoint. Hardware presence and SDK enumeration remain in
# wait_robot_boot_ready.sh.

commercial_value_is_placeholder() {
  local value="${1:-}"
  [[ -z "${value}" || "${value}" == *REPLACE* ]]
}

commercial_pin_storage_paths() {
  # These locations are part of the commercial on-vehicle storage contract.
  # Set them after runtime.env has been sourced so neither the service manager
  # environment nor a caller-provided environment can redirect production I/O.
  export PLAN_DB_PATH="/data/coverage/planning.db"
  export OPS_DB_PATH="/data/coverage/operations.db"
  export MAPS_ROOT="/data/maps"
  export EXTERNAL_MAPS_ROOT="/data/maps/imports"
  export DOCK_CALIBRATION_STORAGE_PATH="/data/coverage/dock_calibration.yaml"
}

commercial_validate_required_orbbec_identities() {
  local serials=("$1" "$2" "$3")
  local paths=("$4" "$5" "$6")
  local value

  for value in "${serials[@]}" "${paths[@]}"; do
    commercial_value_is_placeholder "${value}" && return 1
    [[ "${value}" != *[[:space:]]* ]] || return 1
  done

  for value in "${paths[@]}"; do
    [[ "${value}" =~ ^[0-9]+-[0-9]+([.][0-9]+)*$ ]] || return 1
  done

  [[
    "${serials[0]}" != "${serials[1]}" &&
    "${serials[0]}" != "${serials[2]}" &&
    "${serials[1]}" != "${serials[2]}" &&
    "${paths[0]}" != "${paths[1]}" &&
    "${paths[0]}" != "${paths[2]}" &&
    "${paths[1]}" != "${paths[2]}"
  ]]
}

commercial_runtime_env_key_is_reserved() {
  case "${1:-}" in
    SCRIPT_DIR|REPO_ROOT|DORAEMON_PRODUCTION_ENTRY|\
    DORAEMON_REPO_ROOT|DORAEMON_RUNTIME_CONFIG_FILE|DORAEMON_WORKSPACE_SETUP|DORAEMON_ROS_SETUP|\
    DORAEMON_DEPS_ROOT|ABSEIL_ROOT|ORTOOLS_ROOT|FIELDS2COVER_ROOT|FLIRT_ROOT|absl_DIR|\
    HOME|ROS_HOME|ROS_LOG_DIR|LOG_DIR|STATUS_LOG|RESTART_LOCALIZATION_OUT|\
    SLAM_ROOT|SLAM_CONFIG_ROOT|WORKSPACE_SETUP|WORKSPACE_LIB_ROOT|WORKSPACE_LAYOUT|\
    ROS_MASTER_URI|ROS_IP|ROS_HOSTNAME|\
    START_FRONTEND_DEV|FRONTEND_DIR|FRONTEND_URL|ATTACH|\
    ALLOW_NO_ACTIVE_MAP_STARTUP|RUN_BACKEND_RUNTIME_SMOKE|BACKEND_RUNTIME_SMOKE_TASK_ID|\
    BACKEND_RUNTIME_SMOKE_ACTIONS|BACKEND_RUNTIME_SMOKE_EXTRA_ARGS|\
    RUN_REVISION_DB_HEALTH_CHECK|REVISION_DB_HEALTH_STRICT|\
    RUN_BACKEND_PRODUCTION_ACCEPTANCE|BACKEND_PRODUCTION_ACCEPTANCE_PROFILE|\
    BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS|BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS|\
    LD_LIBRARY_PATH|LD_PRELOAD|LD_AUDIT|LD_ORIGIN_PATH|LIBRARY_PATH|\
    PYTHONPATH|CMAKE_PREFIX_PATH|PKG_CONFIG_PATH|\
    PATH|SHELL|BASH_ENV|ENV|SHELLOPTS|BASHOPTS|CDPATH|GLOBIGNORE|IFS|PS4|\
    TMPDIR|TMP|TEMP|TMUX_TMPDIR)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

commercial_validate_runtime_env_file() {
  local runtime_env_file="$1"
  local line=""
  local key=""
  local value=""
  local -A seen_keys=()

  if [[ ! -f "${runtime_env_file}" || -L "${runtime_env_file}" ]]; then
    echo "[ERROR] runtime environment must be a regular non-symlink file: ${runtime_env_file}" >&2
    return 1
  fi

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#${line%%[![:space:]]*}}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    if [[ "${line}" =~ ^([A-Za-z_][A-Za-z0-9_]*)=([-A-Za-z0-9_./:@,+]*)$ ]]; then
      key="${BASH_REMATCH[1]}"
      value="${BASH_REMATCH[2]}"
      if [[ -n "${seen_keys[${key}]+x}" ]]; then
        echo "[ERROR] runtime environment contains a duplicate key: ${key}" >&2
        return 1
      fi
      seen_keys[${key}]=1
      if commercial_runtime_env_key_is_reserved "${key}"; then
        echo "[ERROR] runtime environment may not override reserved key: ${key}" >&2
        return 1
      fi
      case "${key}" in
        PLAN_DB_PATH)
          [[ "${value}" == "/data/coverage/planning.db" ]] || {
            echo "[ERROR] PLAN_DB_PATH is fixed at /data/coverage/planning.db" >&2
            return 1
          }
          ;;
        OPS_DB_PATH)
          [[ "${value}" == "/data/coverage/operations.db" ]] || {
            echo "[ERROR] OPS_DB_PATH is fixed at /data/coverage/operations.db" >&2
            return 1
          }
          ;;
        MAPS_ROOT)
          [[ "${value}" == "/data/maps" ]] || {
            echo "[ERROR] MAPS_ROOT is fixed at /data/maps" >&2
            return 1
          }
          ;;
        EXTERNAL_MAPS_ROOT)
          [[ "${value}" == "/data/maps/imports" ]] || {
            echo "[ERROR] EXTERNAL_MAPS_ROOT is fixed at /data/maps/imports" >&2
            return 1
          }
          ;;
        DOCK_CALIBRATION_STORAGE_PATH)
          [[ "${value}" == "/data/coverage/dock_calibration.yaml" ]] || {
            echo "[ERROR] DOCK_CALIBRATION_STORAGE_PATH is fixed at /data/coverage/dock_calibration.yaml" >&2
            return 1
          }
          ;;
      esac
    else
      echo "[ERROR] runtime environment contains an unsafe or non-assignment line" >&2
      return 1
    fi
  done <"${runtime_env_file}"
}

commercial_validate_dependencies_env_file() {
  local deps_env_file="$1"
  local line=""
  local key=""
  local value=""
  local -A seen_keys=()
  local expected_key=""
  local -A expected_values=(
    [DORAEMON_DEPS_ROOT]="/opt/doraemon/deps"
    [DORAEMON_CMAKE_ROOT]="/opt/doraemon/deps/cmake-3.20.6"
    [DORAEMON_CMAKE_BIN]="/opt/doraemon/deps/cmake-3.20.6/bin/cmake"
    [DORAEMON_CTEST_BIN]="/opt/doraemon/deps/cmake-3.20.6/bin/ctest"
    [DORAEMON_GCC_VERSION]="10.5.0"
    [CC]="/usr/bin/gcc-10"
    [CXX]="/usr/bin/g++-10"
    [ABSEIL_ROOT]="/opt/doraemon/deps/abseil-20211102.0"
    [ORTOOLS_ROOT]="/opt/doraemon/deps/ortools-9.9"
    [FIELDS2COVER_ROOT]="/opt/doraemon/deps/fields2cover-2.0.0"
    [FLIRT_ROOT]="/opt/doraemon/deps/flirt-doraemon-20260319"
    [absl_DIR]="/opt/doraemon/deps/abseil-20211102.0/lib/cmake/absl"
    [CMAKE_PREFIX_PATH]="/opt/doraemon/deps/abseil-20211102.0:/opt/doraemon/deps/ortools-9.9:/opt/doraemon/deps/fields2cover-2.0.0:/opt/doraemon/deps/flirt-doraemon-20260319"
    [PYTHONPATH]="/opt/doraemon/deps/fields2cover-2.0.0/lib/python3.8/site-packages"
    [PATH]="/opt/doraemon/deps/cmake-3.20.6/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  )
  if [[ ! -f "${deps_env_file}" || -L "${deps_env_file}" ]]; then
    echo "[ERROR] dependency environment must be a regular non-symlink file: ${deps_env_file}" >&2
    return 1
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#${line%%[![:space:]]*}}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    if [[ ! "${line}" =~ ^([A-Za-z_][A-Za-z0-9_]*)=([-A-Za-z0-9_./:@,+]*)$ ]]; then
      echo "[ERROR] dependency environment contains an unsafe or non-assignment line" >&2
      return 1
    fi
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"
    if [[ -n "${seen_keys[${key}]+x}" ]]; then
      echo "[ERROR] dependency environment contains a duplicate key: ${key}" >&2
      return 1
    fi
    seen_keys[${key}]=1
    if [[ -z "${expected_values[${key}]+x}" ]]; then
      echo "[ERROR] dependency environment contains an unexpected key: ${key}" >&2
      return 1
    fi
    if [[ "${value}" != "${expected_values[${key}]}" ]]; then
      echo "[ERROR] dependency environment value is not the pinned commercial baseline: ${key}" >&2
      return 1
    fi
  done <"${deps_env_file}"
  for expected_key in "${!expected_values[@]}"; do
    if [[ -z "${seen_keys[${expected_key}]+x}" ]]; then
      echo "[ERROR] dependency environment is missing required key: ${expected_key}" >&2
      return 1
    fi
  done
}
