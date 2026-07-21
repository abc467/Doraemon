#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERSIONS_FILE="${REPO_ROOT}/deploy/manifests/x86_ubuntu20_versions.env"
FAILURES=0
WARNINGS=0
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_vehicle_identity.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_filesystem_security.sh"
# shellcheck disable=SC1090
source "${VERSIONS_FILE}"

ok() {
  echo "[OK] $*"
}

fail() {
  echo "[FAIL] $*" >&2
  FAILURES=$((FAILURES + 1))
}

warn() {
  echo "[WARN] $*" >&2
  WARNINGS=$((WARNINGS + 1))
}

check_file() {
  if [[ -f "$1" ]]; then
    ok "$1"
  else
    fail "missing $1"
  fi
}

check_root_immutable_tree() {
  local path="$1"
  local label="$2"
  local actual=""
  local offender=""
  local scan_status=0
  if [[ ! -d "${path}" || -L "${path}" ]]; then
    fail "${label} root must be a real non-symlink directory: ${path}"
    return
  fi
  actual="$(stat -c '%U:%G %a' "${path}" 2>/dev/null || true)"
  if [[ "${actual}" != "root:root 755" ]]; then
    fail "${label} root must be root:root 755 (actual=${actual:-missing})"
    return
  fi
  offender="$(find "${path}" -xdev \( -type f -o -type d -o -type l \) \
    \( ! -user root -o ! -group root \) -print -quit 2>&1)"
  scan_status=$?
  if [[ "${scan_status}" -ne 0 ]]; then
    fail "${label} ownership audit failed: ${offender}"
    return
  fi
  [[ -z "${offender}" ]] && ok "${label} is entirely root-owned" || \
    fail "${label} contains non-root-owned path: ${offender}"
  offender="$(find "${path}" -xdev \( -type f -o -type d \) -perm /022 \
    -print -quit 2>&1)"
  scan_status=$?
  if [[ "${scan_status}" -ne 0 ]]; then
    fail "${label} permissions audit failed: ${offender}"
    return
  fi
  [[ -z "${offender}" ]] && ok "${label} has no group/other-writable paths" || \
    fail "${label} contains group/other-writable path: ${offender}"
  if offender="$(commercial_find_mount_below "${path}" 0)"; then
    fail "${label} contains a nested mount hidden from the immutable-tree scan: ${offender}"
  else
    ok "${label} contains no nested mounts"
  fi
  if offender="$(commercial_find_symlink_outside_tree "${path}")"; then
    fail "${label} contains a dangling or escaping symlink: ${offender}"
  else
    ok "${label} symlinks remain inside the immutable tree"
  fi
}

check_external_mutable_directory() {
  local path="$1"
  local expected_owner_group="$2"
  local expected_mode="$3"
  local result_label="$4"
  local actual=""
  local resolved=""
  if [[ ! -d "${path}" || -L "${path}" ]]; then
    fail "missing or symlinked path for ${result_label}: ${path}"
    return
  fi
  if mountpoint -q "${path}"; then
    fail "${result_label} uses an unreviewed bind/mount point: ${path}"
    return
  fi
  resolved="$(realpath -m "${path}")"
  case "${resolved}" in
    /opt/doraemon/releases|/opt/doraemon/releases/*)
      fail "${result_label} resolves into the immutable release tree: ${resolved}"
      return
      ;;
  esac
  actual="$(stat -c '%U:%G %a' "${path}" 2>/dev/null || true)"
  [[ "${actual}" == "${expected_owner_group} ${expected_mode}" ]] && \
    ok "${result_label}" || \
    fail "${result_label}: expected ${expected_owner_group} ${expected_mode} (actual=${actual:-missing})"
}

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

valid_boolean() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on|0|false|no|off)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

placeholder_or_empty() {
  local value="${1:-}"
  [[ -z "${value}" || "${value}" == *REPLACE* ]]
}

check_configuration_files() {
  local actual=""
  if [[ -L /etc/doraemon || ! -d /etc/doraemon ]]; then
    fail "/etc/doraemon must be a real non-symlink directory"
  else
    actual="$(stat -c '%U:%G %a' /etc/doraemon 2>/dev/null || true)"
    [[ "${actual}" == "root:root 755" ]] && ok "/etc/doraemon ownership and mode" || \
      fail "/etc/doraemon must be root:root 755 (actual=${actual:-missing})"
  fi

  if [[ -L /etc/doraemon/deps.env || ! -f /etc/doraemon/deps.env ]]; then
    fail "/etc/doraemon/deps.env must be a regular non-symlink file"
  else
    actual="$(stat -c '%U:%G %a' /etc/doraemon/deps.env 2>/dev/null || true)"
    [[ "${actual}" == "root:root 644" ]] && ok "dependency environment ownership and mode" || \
      fail "/etc/doraemon/deps.env must be root:root 644 (actual=${actual:-missing})"
    if commercial_validate_dependencies_env_file /etc/doraemon/deps.env; then
      ok "dependency environment contains only fixed dependency paths"
    else
      fail "dependency environment contains unsafe or unexpected values"
    fi
    if cmp -s /etc/doraemon/deps.env "${REPO_ROOT}/config/deps.x86_ubuntu20.env"; then
      ok "dependency environment matches release template byte-for-byte"
    else
      fail "dependency environment must exactly match config/deps.x86_ubuntu20.env"
    fi
  fi

  if [[ -L /etc/profile.d/doraemon-deps.sh || ! -f /etc/profile.d/doraemon-deps.sh || \
        "$(stat -c '%U:%G %a' /etc/profile.d/doraemon-deps.sh 2>/dev/null || true)" != "root:root 644" ]]; then
    fail "/etc/profile.d/doraemon-deps.sh must be a root:root 0644 regular file"
  elif cmp -s /etc/profile.d/doraemon-deps.sh \
      "${REPO_ROOT}/config/doraemon-deps.profile.sh"; then
    ok "interactive dependency profile matches release template"
  else
    fail "interactive dependency profile must exactly match config/doraemon-deps.profile.sh"
  fi

  if [[ -L /etc/ld.so.conf.d/doraemon-deps.conf || \
        ! -f /etc/ld.so.conf.d/doraemon-deps.conf || \
        "$(stat -c '%U:%G %a' /etc/ld.so.conf.d/doraemon-deps.conf 2>/dev/null || true)" != \
          "root:root 644" ]]; then
    fail "/etc/ld.so.conf.d/doraemon-deps.conf must be a root:root 0644 regular file"
  elif cmp -s /etc/ld.so.conf.d/doraemon-deps.conf \
      "${REPO_ROOT}/config/doraemon-deps.ld.so.conf"; then
    ok "dynamic-loader dependency paths match release template"
  else
    fail "dynamic-loader paths must exactly match config/doraemon-deps.ld.so.conf"
  fi

  if [[ -L /etc/doraemon/runtime.env || ! -f /etc/doraemon/runtime.env ]]; then
    fail "/etc/doraemon/runtime.env must be a regular non-symlink file"
  else
    actual="$(stat -c '%U:%G %a' /etc/doraemon/runtime.env 2>/dev/null || true)"
    if [[ "${actual}" == "root:a 640" ]]; then
      ok "runtime environment ownership and mode"
    else
      fail "/etc/doraemon/runtime.env must be root:a 640 (actual=${actual:-missing})"
    fi
    if commercial_validate_runtime_env_file /etc/doraemon/runtime.env; then
      ok "runtime environment contains no reserved execution overrides"
    else
      fail "runtime environment contains reserved or executable overrides"
    fi
  fi
}

check_static_runtime_contract() {
  local unit_source="${REPO_ROOT}/deploy/systemd/doraemon-runtime.service"
  local start_source="${REPO_ROOT}/scripts/start_runtime.sh"
  local frontend_backend_source="${REPO_ROOT}/scripts/start_frontend_backend.sh"
  local orbbec_config="${REPO_ROOT}/src/orbbec-ros-sdk/config/OrbbecSDKConfig_v1.0.xml"
  local orbbec_driver="${REPO_ROOT}/src/orbbec-ros-sdk/src/ob_camera_node_driver.cpp"
  local orbbec_list_devices="${REPO_ROOT}/src/orbbec-ros-sdk/src/list_devices_node.cpp"
  local orbbec_pair_gate="${REPO_ROOT}/scripts/verify_orbbec_sdk_pairs.py"
  local boot_preflight="${REPO_ROOT}/scripts/wait_robot_boot_ready.sh"
  local orbbec_storage="${REPO_ROOT}/src/orbbec-ros-sdk/include/orbbec_camera/storage.h"
  local required_unit_line=""
  local unit_line_count=""
  local environment_files=""
  local fatal_handler=""
  for required_unit_line in \
    "Type=oneshot" \
    "User=a" \
    "Group=a" \
    "ExecStartPre=/opt/doraemon/current/scripts/wait_robot_boot_ready.sh" \
    "ExecStart=/opt/doraemon/current/scripts/start_runtime.sh" \
    "ExecStop=/opt/doraemon/current/scripts/stop_all_backend.sh" \
    "ExecStopPost=/opt/doraemon/current/scripts/cleanup_failed_runtime_service.sh" \
    "RemainAfterExit=yes" \
    "Restart=no" \
    "KillMode=mixed" \
    "UMask=0027" \
    "NoNewPrivileges=true"; do
    unit_line_count="$(grep -Fxc -- "${required_unit_line}" "${unit_source}" || true)"
    [[ "${unit_line_count}" == "1" ]] && \
      ok "runtime unit exact contract ${required_unit_line%%=*}" || \
      fail "runtime unit must contain exactly one: ${required_unit_line}"
  done
  environment_files="$(awk '
    /^\[Service\]$/ { in_service = 1; next }
    in_service && /^\[/ { in_service = 0 }
    in_service && /^EnvironmentFile=/ { print }
  ' "${unit_source}")"
  if [[ "${environment_files}" == $'EnvironmentFile=-/etc/doraemon/runtime.env\nEnvironmentFile=-/etc/doraemon/deps.env' ]]; then
    ok "runtime unit EnvironmentFile order"
  else
    fail "runtime unit EnvironmentFiles must be runtime.env then deps.env"
  fi
  grep -q '^Restart=no$' "${unit_source}" && ok "runtime unit is fail-closed" || fail "runtime unit Restart=no"
  grep -q '^KillMode=mixed$' "${unit_source}" && ok "runtime unit KillMode=mixed" || fail "runtime unit KillMode=mixed"
  grep -q '^ExecStopPost=.*cleanup_failed_runtime_service.sh$' "${unit_source}" && \
    ok "runtime unit failed-start cleanup" || fail "runtime unit ExecStopPost cleanup"
  grep -Fq 'LOG_DIR="${LOG_DIR:-/var/log/doraemon/startup}"' "${start_source}" && \
    ok "startup log default is external" || fail "startup log default must be external"
  grep -Fq 'mutable frontend development mode is forbidden in the commercial runtime' \
    "${start_source}" && ok "commercial runtime forbids mutable frontend dev mode" || \
    fail "commercial runtime must reject START_FRONTEND_DEV/FRONTEND_DIR"
  grep -Fq 'frontend backend rosbridge is fixed at 127.0.0.1:9090' \
    "${frontend_backend_source}" && ok "standalone frontend backend is loopback-only" || \
    fail "standalone frontend backend must reject non-loopback rosbridge"
  grep -Fq '<OutputDir>/var/log/doraemon/orbbec</OutputDir>' "${orbbec_config}" && \
    ok "Orbbec SDK log directory is external" || fail "Orbbec SDK OutputDir must be external"
  grep -Fq '<FileLogLevel>5</FileLogLevel>' "${orbbec_config}" && \
    ok "Orbbec SDK file logging is disabled" || fail "Orbbec SDK FileLogLevel must be OFF"
  fatal_handler="$(sed -n '/^void fatalSignalHandler/,/^}/p' "${orbbec_driver}")"
  if grep -Fq '::write(STDERR_FILENO' <<<"${fatal_handler}" && \
      grep -Fq '_exit(128 + signum)' <<<"${fatal_handler}" && \
      ! grep -Eq 'backward::|ros::|ofstream|ioctl|USBDEVFS_RESET|std::exit|(^|[^_[:alnum:]])exit\(' \
        <<<"${fatal_handler}" && \
      ! grep -Eq 'sigaction\((SIGINT|SIGTERM)' "${orbbec_driver}" && \
      awk '
        /installFatalSignalHandlers\(\);/ { installed = NR }
        /make_shared<ob::Context>/ { constructed = NR }
        END { exit !(installed && constructed && installed < constructed) }
      ' "${orbbec_driver}"; then
    ok "Orbbec fatal-signal path is async-signal-safe"
  else
    fail "Orbbec fatal-signal path must use only fixed write(2) and _exit"
  fi
  grep -Fq 'kOrbbecCaptureDirectory[] = "/var/lib/doraemon/orbbec-captures"' "${orbbec_storage}" && \
    ok "Orbbec capture directory is external" || fail "Orbbec captures must use external storage"
  if awk '
      /disableOrbbecSdkFileLogging/ { configured = NR }
      /make_shared<ob::Context>/ { constructed = NR }
      END { exit !(configured && constructed && configured < constructed) }
    ' "${orbbec_list_devices}"; then
    ok "Orbbec enumeration configures logging before Context"
  else
    fail "Orbbec enumeration must configure logging before Context"
  fi
  if grep -Fq 'DORAEMON_ORBBEC_DEVICE_V1|' "${orbbec_list_devices}" && \
      grep -Fq 'return kSdkErrorExitCode' "${orbbec_list_devices}" && \
      grep -Fq 'return kStandardErrorExitCode' "${orbbec_list_devices}" && \
      grep -Fq 'return kUnknownErrorExitCode' "${orbbec_list_devices}" && \
      ! grep -Fq 'ROS_INFO_STREAM("serial:' "${orbbec_list_devices}"; then
    ok "Orbbec enumeration uses an atomic versioned machine protocol"
  else
    fail "Orbbec enumeration must use machine records and fail nonzero on exceptions"
  fi
  if [[ -f "${orbbec_pair_gate}" && ! -L "${orbbec_pair_gate}" ]] && \
      grep -Fq 'REQUIRED_CONSECUTIVE_SNAPSHOTS = 2' "${orbbec_pair_gate}" && \
      grep -Fq 'python3 "${SCRIPT_DIR}/verify_orbbec_sdk_pairs.py"' "${boot_preflight}" && \
      grep -Fq 'orbbec_remaining_sec=$((TIMEOUT_SEC - $(elapsed_sec)))' \
        "${boot_preflight}" && \
      ! grep -Fq 'DORAEMON_ORBBEC_LIST_DEVICES_BINARY' "${boot_preflight}"; then
    ok "Orbbec boot gate requires two exact snapshots within the shared timeout"
  else
    fail "Orbbec boot gate stability and fixed-binary contract"
  fi
}

check_installed_vehicle_identity() {
  local runtime_env_stat=""
  runtime_env_stat="$(stat -c '%U %a' /etc/doraemon/runtime.env 2>/dev/null || true)"
  if [[ "${runtime_env_stat}" != "root 640" ]]; then
    fail "/etc/doraemon/runtime.env must be root-owned mode 0640 (actual=${runtime_env_stat:-missing})"
    return
  fi
  if ! commercial_validate_runtime_env_file /etc/doraemon/runtime.env; then
    fail "runtime environment contains reserved or executable overrides"
    return
  fi
  set +u
  set -a
  # shellcheck disable=SC1091
  source /etc/doraemon/runtime.env
  set +a
  set -u

  if placeholder_or_empty "${ROBOT_ID:-}" || [[ "${ROBOT_ID:-}" == "local_robot" ]]; then
    fail "ROBOT_ID must be the explicit vehicle asset identifier"
  else
    ok "vehicle ROBOT_ID is explicit"
  fi
  if placeholder_or_empty "${DORAEMON_A_BOX_IFACE:-}"; then
    fail "DORAEMON_A_BOX_IFACE must be the explicit internal wired interface"
  else
    ok "internal wired interface is explicit"
  fi

  if commercial_validate_required_orbbec_identities \
      "${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER:-}" \
      "${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER:-}" \
      "${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER:-}" \
      "${RUNTIME_ORBBEC_CAMERA1_USB_PORT:-}" \
      "${RUNTIME_ORBBEC_CAMERA2_USB_PORT:-}" \
      "${RUNTIME_ORBBEC_CAMERA3_USB_PORT:-}"; then
    ok "vehicle camera serials and USB topologies are explicit, unique, and syntactically valid"
  else
    fail "vehicle camera identities must contain three unique serials and three valid unique USB topologies"
  fi

  valid_boolean "${RUNTIME_START_DEPTH_CAMERAS:-}" && \
    truthy "${RUNTIME_START_DEPTH_CAMERAS:-}" && \
    ok "three-camera commercial baseline enabled" || fail "RUNTIME_START_DEPTH_CAMERAS=true"
  truthy "${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS:-}" && \
    ok "depth camera topic gate enabled" || fail "RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true"
  truthy "${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES:-}" && \
    ok "depth camera identity gate enabled" || fail "DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true"
  valid_boolean "${DORAEMON_NO_ACTION_ACCEPTANCE:-}" && \
    truthy "${DORAEMON_NO_ACTION_ACCEPTANCE:-}" && \
    ok "no-action acceptance mode enabled" || fail "DORAEMON_NO_ACTION_ACCEPTANCE=true during phases H/K"
  valid_boolean "${DORAEMON_ACTION_TEST_APPROVED:-}" && \
    ! truthy "${DORAEMON_ACTION_TEST_APPROVED:-}" && \
    ok "action-test approval remains disabled" || fail "DORAEMON_ACTION_TEST_APPROVED=false during phases H/K"
  [[ "${ROSBRIDGE_ADDRESS:-}" == "127.0.0.1" ]] && \
    ok "rosbridge configured for loopback only" || fail "ROSBRIDGE_ADDRESS must equal 127.0.0.1"
}

check_installed_runtime_contract() {
  local service="doraemon-runtime.service"
  local unit_path="/etc/systemd/system/${service}"
  local actual=""
  local expected_root=""
  local expected_workspace_setup=""
  local service_user=""
  local service_group=""
  local offender=""
  local scan_status=0
  local execution_property=""
  local property_name=""
  local expected_path=""
  local expected_environment=""
  local protected_variable=""
  local mutable_directory=""
  local mutable_label=""
  expected_root="$(realpath "${REPO_ROOT}")"

  if [[ -f "${expected_root}/install/setup.bash" ]]; then
    expected_workspace_setup="${expected_root}/install/setup.bash"
  else
    expected_workspace_setup="${expected_root}/devel/setup.bash"
  fi

  if [[ -L "${unit_path}" || ! -f "${unit_path}" || \
        "$(stat -c '%U:%G %a' "${unit_path}" 2>/dev/null || true)" != "root:root 644" ]]; then
    fail "installed runtime unit must be a root:root 0644 regular file: ${unit_path}"
  else
    ok "installed runtime unit ownership and mode"
  fi
  actual="$(systemctl show -p FragmentPath --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "${unit_path}" ]] && ok "installed runtime unit fragment path" || \
    fail "installed runtime unit fragment path must be ${unit_path} (actual=${actual:-missing})"
  actual="$(systemctl show -p DropInPaths --value "${service}" 2>/dev/null || true)"
  [[ -z "${actual}" ]] && ok "installed runtime has no unreviewed drop-ins" || \
    fail "installed runtime must not have systemd drop-ins: ${actual}"

  actual="$(systemctl show -p Type --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "oneshot" ]] && ok "installed runtime Type=oneshot" || fail "installed runtime Type=oneshot (actual=${actual:-missing})"
  actual="$(systemctl show -p Restart --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "no" ]] && ok "installed runtime Restart=no" || fail "installed runtime Restart=no (actual=${actual:-missing})"
  actual="$(systemctl show -p KillMode --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "mixed" ]] && ok "installed runtime KillMode=mixed" || fail "installed runtime KillMode=mixed (actual=${actual:-missing})"
  actual="$(systemctl show -p RemainAfterExit --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "yes" ]] && ok "installed runtime RemainAfterExit=yes" || fail "installed runtime RemainAfterExit=yes (actual=${actual:-missing})"
  actual="$(systemctl show -p UMask --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "0027" ]] && ok "installed runtime UMask=0027" || fail "installed runtime UMask=0027 (actual=${actual:-missing})"
  actual="$(systemctl show -p NoNewPrivileges --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "yes" ]] && ok "installed runtime NoNewPrivileges=yes" || fail "installed runtime NoNewPrivileges=yes (actual=${actual:-missing})"

  service_user="$(systemctl show -p User --value "${service}" 2>/dev/null || true)"
  service_group="$(systemctl show -p Group --value "${service}" 2>/dev/null || true)"
  [[ "${service_user}" == "a" ]] && ok "installed runtime User=a" || fail "installed runtime User must be a (actual=${service_user:-missing})"
  [[ "${service_group}" == "a" ]] && ok "installed runtime Group=a" || fail "installed runtime Group must be a (actual=${service_group:-missing})"

  actual="$(systemctl show -p WorkingDirectory --value "${service}" 2>/dev/null || true)"
  if [[ -n "${actual}" && "$(realpath "${actual}" 2>/dev/null || true)" == "${expected_root}" ]]; then
    ok "installed runtime points to current release"
  else
    fail "installed runtime WorkingDirectory points outside current release: ${actual:-missing}"
  fi
  actual="$(systemctl show -p EnvironmentFiles --value "${service}" 2>/dev/null || true)"
  if [[ "${actual}" == $'/etc/doraemon/runtime.env (ignore_errors=yes)\n/etc/doraemon/deps.env (ignore_errors=yes)' ]]; then
    ok "installed runtime EnvironmentFile order"
  else
    fail "installed runtime EnvironmentFiles must be runtime.env then deps.env (actual=${actual:-missing})"
  fi
  for execution_property in \
    "ExecStartPre:${expected_root}/scripts/wait_robot_boot_ready.sh" \
    "ExecStart:${expected_root}/scripts/start_runtime.sh" \
    "ExecStop:${expected_root}/scripts/stop_all_backend.sh" \
    "ExecStopPost:${expected_root}/scripts/cleanup_failed_runtime_service.sh"; do
    property_name="${execution_property%%:*}"
    expected_path="${execution_property#*:}"
    actual="$(systemctl show -p "${property_name}" --value "${service}" 2>/dev/null || true)"
    if [[ "${actual}" == *"path=${expected_path} ; argv[]=${expected_path} ; ignore_errors=no"* ]]; then
      ok "installed runtime ${property_name} path"
    else
      fail "installed runtime ${property_name} must use ${expected_path} without arguments"
    fi
  done
  actual="$(systemctl show -p NRestarts --value "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "0" ]] && ok "runtime automatic restart count is zero" || fail "runtime NRestarts must be zero (actual=${actual:-missing})"

  actual="$(systemctl show -p Environment --value "${service}" 2>/dev/null || true)"
  for expected_environment in \
    "DORAEMON_REPO_ROOT=${expected_root}" \
    "DORAEMON_RUNTIME_CONFIG_FILE=/etc/doraemon/runtime.env" \
    "DORAEMON_WORKSPACE_SETUP=${expected_workspace_setup}" \
    "DORAEMON_ROS_SETUP=/opt/ros/noetic/setup.bash" \
    "ROS_HOME=/var/lib/doraemon/ros" \
    "ROS_MASTER_URI=http://127.0.0.1:11311" \
    "LOG_DIR=/var/log/doraemon/startup"; do
    [[ " ${actual} " == *" ${expected_environment} "* ]] && \
      ok "installed protected environment ${expected_environment%%=*}" || \
      fail "installed unit missing protected environment: ${expected_environment}"
  done
  actual="$(systemctl show -p UnsetEnvironment --value "${service}" 2>/dev/null || true)"
  for protected_variable in ROS_IP ROS_HOSTNAME LD_LIBRARY_PATH LD_PRELOAD LD_AUDIT \
      LD_ORIGIN_PATH LIBRARY_PATH BASH_ENV ENV CDPATH GLOBIGNORE IFS PS4 TMPDIR TMP \
      TEMP TMUX_TMPDIR; do
    [[ " ${actual} " == *" ${protected_variable} "* ]] && \
      ok "installed runtime unsets ${protected_variable}" || \
      fail "installed runtime must unset ${protected_variable}"
  done

  actual="$(systemctl is-enabled "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "disabled" ]] && ok "runtime service disabled" || fail "runtime service must remain disabled (actual=${actual:-missing})"
  actual="$(systemctl is-active "${service}" 2>/dev/null || true)"
  [[ "${actual}" == "inactive" ]] && ok "runtime service inactive" || fail "runtime service must be inactive (actual=${actual:-missing})"

  for mutable_directory in \
    /var/lib/doraemon \
    /var/lib/doraemon/ros \
    /var/lib/doraemon/orbbec-captures \
    /var/log/doraemon \
    /var/log/doraemon/startup \
    /var/log/doraemon/slam-runtime \
    /var/log/doraemon/orbbec \
    /data/coverage \
    /data/maps \
    /data/maps/imports; do
    case "${mutable_directory}" in
      /var/log/doraemon/slam-runtime)
        mutable_label="SLAM runtime log directory ownership and mode"
        ;;
      /var/log/doraemon/orbbec)
        mutable_label="external Orbbec log directory ownership and mode"
        ;;
      *)
        mutable_label="external runtime directory ownership and mode"
        ;;
    esac
    check_external_mutable_directory "${mutable_directory}" \
      "a:a" 750 "${mutable_label}"
  done
  if commercial_validate_slam_config_override_tree /data/config/slam/cartographer 0; then
    ok "reviewed read-only SLAM configuration override directory"
  else
    fail "SLAM configuration override directory is not root-managed and read-only"
  fi
  for expected_path in /opt /opt/doraemon /opt/doraemon/releases; do
    actual="$(stat -c '%U:%G %a' "${expected_path}" 2>/dev/null || true)"
    if [[ -d "${expected_path}" && ! -L "${expected_path}" && \
          "${actual}" == "root:root 755" ]]; then
      ok "immutable release ancestor ${expected_path}"
    else
      fail "release ancestor must be a real root:root 755 directory: ${expected_path} (actual=${actual:-missing})"
    fi
  done
  if [[ -L /opt/doraemon/current && \
        "$(stat -c '%U:%G' /opt/doraemon/current 2>/dev/null || true)" == "root:root" && \
        "$(realpath /opt/doraemon/current 2>/dev/null || true)" == "${expected_root}" ]]; then
    ok "current release link is root-owned"
  else
    fail "/opt/doraemon/current must be a root-owned symlink to ${expected_root}"
  fi
  offender="$(find "${expected_root}" -xdev \( -type f -o -type d -o -type l \) \
    \( ! -user root -o ! -group root \) -print -quit 2>&1)"
  scan_status=$?
  if [[ "${scan_status}" -ne 0 ]]; then
    fail "installed release ownership audit failed: ${offender}"
  elif [[ -n "${offender}" ]]; then
    fail "installed release contains non-root-owned paths"
  else
    ok "installed release is entirely root-owned"
  fi
  offender="$(find "${expected_root}" -xdev \( -type f -o -type d \) -perm /022 \
    -print -quit 2>&1)"
  scan_status=$?
  if [[ "${scan_status}" -ne 0 ]]; then
    fail "installed release permissions audit failed: ${offender}"
  elif [[ -n "${offender}" ]]; then
    fail "installed release contains group/other-writable paths"
  else
    ok "installed release is group/other read-only"
  fi
  if offender="$(commercial_find_mount_below "${expected_root}" 0)"; then
    fail "installed release contains a nested mount: ${offender}"
  else
    ok "installed release contains no nested mounts"
  fi
  if offender="$(commercial_find_unsafe_release_symlink "${expected_root}")"; then
    fail "installed release contains a dangling or unapproved escaping symlink: ${offender}"
  else
    ok "installed release symlink targets are contained or explicitly approved"
  fi
  offender="$(commercial_find_forbidden_release_artifact "${expected_root}")"
  [[ -z "${offender}" ]] && \
    ok "release contains no logs, bags, test bags, databases, captures, or export logs" || \
    fail "release contains forbidden generated/runtime artifact: ${offender}"
  if commercial_verify_release_git_identity "${expected_root}" \
      "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
      "${DORAEMON_BACKEND_GIT_URL}"; then
    ok "backend release is a clean shallow clone of exact deployment tag"
  else
    fail "backend release Git identity is not the fixed commercial tag"
  fi
  check_installed_vehicle_identity
}

[[ "$(uname -m)" == "x86_64" ]] && ok "architecture x86_64" || fail "architecture must be x86_64"
check_static_runtime_contract
check_file /opt/ros/noetic/setup.bash
check_file /etc/doraemon/deps.env
check_file /etc/doraemon/runtime.env
check_configuration_files
check_root_immutable_tree /opt/doraemon/deps "dependency tree"

if env -u DORAEMON_DEPS_ROOT -u DORAEMON_DEPS_BUILD_ROOT -u DORAEMON_APT_ROOT \
    -u DORAEMON_ROS1_KEYRING -u DORAEMON_DEPENDENCY_TEST_MODE \
    "${REPO_ROOT}/scripts/install_x86_ubuntu20_dependencies.sh" \
    --verify-toolchain-only; then
  ok "pinned CMake and GCC/G++ toolchain"
else
  fail "pinned CMake and GCC/G++ toolchain verification"
fi
if env -u DORAEMON_DEPS_ROOT -u DORAEMON_DEPS_BUILD_ROOT -u DORAEMON_APT_ROOT \
    -u DORAEMON_ROS1_KEYRING -u DORAEMON_DEPENDENCY_TEST_MODE \
    "${REPO_ROOT}/scripts/install_x86_ubuntu20_dependencies.sh" \
    --verify-shapely-only; then
  ok "pinned Ubuntu Shapely package and isolated geometry runtime"
else
  fail "pinned Ubuntu Shapely package or geometry runtime verification"
fi
if env -u DORAEMON_DEPS_ROOT -u DORAEMON_DEPS_BUILD_ROOT -u DORAEMON_APT_ROOT \
    -u DORAEMON_ROS1_KEYRING -u DORAEMON_DEPENDENCY_TEST_MODE \
    "${REPO_ROOT}/scripts/install_x86_ubuntu20_dependencies.sh" \
    --verify-ros1-apt-only; then
  ok "exact USTC ROS1 source and official signing key"
else
  fail "USTC ROS1 source or official signing-key verification"
fi

if [[ -f /etc/doraemon/deps.env && ! -L /etc/doraemon/deps.env && \
      "$(stat -c '%U:%G %a' /etc/doraemon/deps.env 2>/dev/null || true)" == "root:root 644" ]] && \
    commercial_validate_dependencies_env_file /etc/doraemon/deps.env; then
  set -a
  # shellcheck disable=SC1091
  source /etc/doraemon/deps.env
  set +a

  [[ -f "${ORTOOLS_ROOT:-}/lib/libortools.so" ]] &&
    ok "OR-Tools ${ORTOOLS_ROOT}" ||
    fail "OR-Tools shared library"
  [[ -f "${FIELDS2COVER_ROOT:-}/lib/libFields2Cover.so" ]] &&
    ok "Fields2Cover ${FIELDS2COVER_ROOT}" ||
    fail "Fields2Cover shared library"
  [[ -f "${FLIRT_ROOT:-}/lib/libflirtlib_feature.so" ]] &&
    ok "FLIRT ${FLIRT_ROOT}" ||
    fail "FLIRT shared library"

  python3 -c "import fields2cover" >/dev/null 2>&1 &&
    ok "Fields2Cover Python import" ||
    fail "Fields2Cover Python import"

  FIELDS2COVER_EXTENSIONS=()
  FIELDS2COVER_MODULES=()
  if [[ -d "${FIELDS2COVER_ROOT:-}" && -d "${ORTOOLS_ROOT:-}" ]]; then
    mapfile -t FIELDS2COVER_EXTENSIONS < <(
      find "${FIELDS2COVER_ROOT}" -xdev -type f -name _fields2cover_python.so -print
    )
    mapfile -t FIELDS2COVER_MODULES < <(
      find "${FIELDS2COVER_ROOT}" -xdev -type f -name fields2cover.py -print
    )
  fi
  if [[ "${#FIELDS2COVER_EXTENSIONS[@]}" -ne 1 ||
        "${#FIELDS2COVER_MODULES[@]}" -ne 1 ]]; then
    fail "Fields2Cover Python install must contain exactly one extension and module"
  elif [[ ! -x "${REPO_ROOT}/scripts/harden_fields2cover_python_install.sh" ]]; then
    fail "missing executable Fields2Cover Python install verifier"
  elif bash "${REPO_ROOT}/scripts/harden_fields2cover_python_install.sh" \
      --verify-only \
      --system-loader \
      --require-root-owner \
      "${FIELDS2COVER_EXTENSIONS[0]}" \
      "${FIELDS2COVER_MODULES[0]}" \
      "${FIELDS2COVER_ROOT}" \
      "${ORTOOLS_ROOT}" \
      "/var/tmp/doraemon-deps-build"; then
    ok "Fields2Cover Python RPATH, loader, ownership, and permissions"
  else
    fail "Fields2Cover Python install is not independent of the build cache"
  fi
fi

WORKSPACE_SETUP=""
if [[ -f "${REPO_ROOT}/install/setup.bash" ]]; then
  WORKSPACE_SETUP="${REPO_ROOT}/install/setup.bash"
elif [[ -f "${REPO_ROOT}/devel/setup.bash" ]]; then
  WORKSPACE_SETUP="${REPO_ROOT}/devel/setup.bash"
fi

if [[ -n "${WORKSPACE_SETUP}" ]]; then
  ok "Doraemon workspace setup ${WORKSPACE_SETUP}"
else
  fail "Doraemon workspace is not built"
fi

if commercial_validate_workspace_build_provenance \
    "${REPO_ROOT}" \
    "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
    /opt/doraemon/deps/cmake-3.20.6/bin/cmake \
    /usr/bin/gcc-10 \
    /usr/bin/g++-10 \
    "${DORAEMON_BACKEND_GIT_URL}"; then
  ok "workspace was freshly built on this machine with the pinned toolchain"
else
  fail "workspace build provenance or CMake cache audit"
fi

if python3 "${REPO_ROOT}/scripts/verify_rosbridge_loopback_patch.py"; then
  ok "rosbridge loopback source overlay"
else
  fail "rosbridge loopback source overlay"
fi

if [[ -n "${WORKSPACE_SETUP}" ]]; then
  set +u
  # shellcheck disable=SC1091
  source /opt/ros/noetic/setup.bash
  # shellcheck disable=SC1090
  source "${WORKSPACE_SETUP}"
  set -u

  if ROSBRIDGE_OVERLAY="$(rospack find rosbridge_server 2>/dev/null)"; then
    if [[ "$(realpath "${ROSBRIDGE_OVERLAY}")" == "$(realpath "${REPO_ROOT}/src/rosbridge_server")" ]]; then
      ok "rosbridge_server resolves to release overlay ${ROSBRIDGE_OVERLAY}"
    else
      fail "rosbridge_server resolves outside release overlay: ${ROSBRIDGE_OVERLAY}"
    fi
  else
    fail "rosbridge_server cannot be resolved"
  fi
fi

if commercial_validate_runtime_env_file /etc/doraemon/runtime.env; then
  set +u
  set -a
  # shellcheck disable=SC1091
  source /etc/doraemon/runtime.env
  set +a
  set -u
  if DORAEMON_REPO_ROOT="${REPO_ROOT}" \
      DORAEMON_RUNTIME_CONFIG_FILE=/etc/doraemon/runtime.env \
      DORAEMON_BOOT_WAIT_TIMEOUT=30 \
      "${REPO_ROOT}/scripts/wait_robot_boot_ready.sh"; then
    ok "local hardware identities, udev permissions, network, and Orbbec SDK pairs"
  else
    fail "local hardware/udev/network commercial preflight"
  fi
  if [[ -n "${STATION_SERVER_IP:-}" && -n "${STATION_SERVER_PORT:-}" ]] && \
      timeout 3 bash -c 'exec 3<>"/dev/tcp/$1/$2"' bash \
        "${STATION_SERVER_IP}" "${STATION_SERVER_PORT}" >/dev/null 2>&1; then
    ok "charging-station endpoint ${STATION_SERVER_IP}:${STATION_SERVER_PORT}"
  else
    warn "charging-station endpoint is not reachable; close this only during stage L"
  fi
else
  fail "runtime environment could not be loaded for hardware/network checks"
fi

CURRENT_RELEASE="$(realpath /opt/doraemon/current 2>/dev/null || true)"
if [[ -n "${CURRENT_RELEASE}" && "$(realpath "${REPO_ROOT}")" == "${CURRENT_RELEASE}" ]]; then
  check_installed_runtime_contract
else
  warn "candidate workspace is not /opt/doraemon/current; installed unit/config checks deferred"
fi

echo "summary: failures=${FAILURES} warnings=${WARNINGS}"
[[ "${FAILURES}" -eq 0 ]]
