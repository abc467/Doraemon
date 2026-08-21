#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DEFAULT_REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_vehicle_identity.sh"

TIMEOUT_SEC="${DORAEMON_BOOT_WAIT_TIMEOUT:-120}"
ROBOT_ID="${ROBOT_ID:-}"
ROSBRIDGE_ADDRESS="${ROSBRIDGE_ADDRESS:-127.0.0.1}"
CHASSIS_DRIVER="${DORAEMON_CHASSIS_DRIVER:-${CHASSIS_DRIVER:-legacy_mcore}}"
case "${CHASSIS_DRIVER}" in
  mcore_tcp|tcp_mcore|new_mcore)
    CHASSIS_DRIVER="mcore_tcp"
    ;;
  mcore_serial|serial_mcore)
    CHASSIS_DRIVER="mcore_serial"
    ;;
  legacy|legacy_mcore|mcore)
    CHASSIS_DRIVER="legacy_mcore"
    ;;
  wheeltec|wheeltec_senior_diff|senior_diff)
    CHASSIS_DRIVER="wheeltec_senior_diff"
    ;;
esac
DEFAULT_CHASSIS_DEVICE=""
DEFAULT_REQUIRE_IMU_DEVICE="true"
DEFAULT_REQUIRE_ODOM_DEVICE="true"
DEFAULT_REQUIRE_CHASSIS_DEVICE="false"
DEFAULT_REQUIRE_MBOX_PING="true"
if [[ "${CHASSIS_DRIVER}" == "mcore_serial" ]]; then
  DEFAULT_CHASSIS_DEVICE="${DORAEMON_MCORE_SERIAL_DEVICE:-/dev/mcore}"
  DEFAULT_REQUIRE_CHASSIS_DEVICE="true"
  DEFAULT_REQUIRE_MBOX_PING="false"
fi
if [[ "${CHASSIS_DRIVER}" == "mcore_tcp" ]]; then
  DEFAULT_REQUIRE_CHASSIS_DEVICE="false"
  DEFAULT_REQUIRE_MBOX_PING="true"
fi
if [[ "${CHASSIS_DRIVER}" == "wheeltec_senior_diff" ]]; then
  DEFAULT_CHASSIS_DEVICE="${DORAEMON_WHEELTEC_BY_ID_DEVICE:-/dev/serial/by-id/usb-WCH.CN_USB_Single_Serial_0002-if00}"
  DEFAULT_REQUIRE_IMU_DEVICE="false"
  DEFAULT_REQUIRE_ODOM_DEVICE="false"
  DEFAULT_REQUIRE_CHASSIS_DEVICE="true"
  DEFAULT_REQUIRE_MBOX_PING="false"
fi
A_BOX_IP="${DORAEMON_A_BOX_IP:-192.168.127.11}"
A_BOX_IFACE="${DORAEMON_A_BOX_IFACE:-}"
MBOX_IP="${DORAEMON_MBOX_IP:-192.168.127.10}"
LIDAR_IP="${DORAEMON_LIDAR_IP:-192.168.127.23}"
IMU_DEVICE="${DORAEMON_IMU_DEVICE:-/dev/imu}"
ODOM_DEVICE="${DORAEMON_ODOM_DEVICE:-/dev/wheel_odom}"
CHASSIS_DEVICE="${DORAEMON_CHASSIS_DEVICE:-${WHEELTEC_SERIAL_DEVICE:-${DEFAULT_CHASSIS_DEVICE}}}"
REQUIRE_IMU_DEVICE="${DORAEMON_REQUIRE_IMU_DEVICE:-${DEFAULT_REQUIRE_IMU_DEVICE}}"
REQUIRE_ODOM_DEVICE="${DORAEMON_REQUIRE_ODOM_DEVICE:-${DEFAULT_REQUIRE_ODOM_DEVICE}}"
REQUIRE_CHASSIS_DEVICE="${DORAEMON_REQUIRE_CHASSIS_DEVICE:-${DEFAULT_REQUIRE_CHASSIS_DEVICE}}"
REQUIRE_MBOX_PING="${DORAEMON_REQUIRE_MBOX_PING:-${DEFAULT_REQUIRE_MBOX_PING}}"
REQUIRE_LIDAR_PING="${DORAEMON_REQUIRE_LIDAR_PING:-true}"
START_DEPTH_CAMERAS="${RUNTIME_START_DEPTH_CAMERAS:-false}"
NO_ACTION_ACCEPTANCE="${DORAEMON_NO_ACTION_ACCEPTANCE:-true}"
ACTION_TEST_APPROVED="${DORAEMON_ACTION_TEST_APPROVED:-false}"
REQUIRE_DEPTH_CAMERA_TOPICS="${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS:-true}"
REQUIRE_DEPTH_CAMERA_IDENTITIES="${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES:-true}"
ORBBEC_CAMERA1_SERIAL_NUMBER="${RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER:-}"
ORBBEC_CAMERA2_SERIAL_NUMBER="${RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER:-}"
ORBBEC_CAMERA3_SERIAL_NUMBER="${RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER:-}"
ORBBEC_CAMERA1_USB_PORT="${RUNTIME_ORBBEC_CAMERA1_USB_PORT:-}"
ORBBEC_CAMERA2_USB_PORT="${RUNTIME_ORBBEC_CAMERA2_USB_PORT:-}"
ORBBEC_CAMERA3_USB_PORT="${RUNTIME_ORBBEC_CAMERA3_USB_PORT:-}"
COMMERCIAL_ORBBEC_VENDOR_ID="2bc5"
COMMERCIAL_ORBBEC_MIN_USB_SPEED="5000"
ORBBEC_VENDOR_ID="${DORAEMON_ORBBEC_VENDOR_ID:-${COMMERCIAL_ORBBEC_VENDOR_ID}}"
ORBBEC_MIN_USB_SPEED="${DORAEMON_ORBBEC_MIN_USB_SPEED:-${COMMERCIAL_ORBBEC_MIN_USB_SPEED}}"
REPO_ROOT="${DORAEMON_REPO_ROOT:-${DEFAULT_REPO_ROOT}}"
if [[ "$(realpath -m "${REPO_ROOT}")" != "${DEFAULT_REPO_ROOT}" ]]; then
  echo "[ERROR] DORAEMON_REPO_ROOT must match the executing release: ${DEFAULT_REPO_ROOT}" >&2
  exit 1
fi
REPO_ROOT="${DEFAULT_REPO_ROOT}"
if [[ -n "${DORAEMON_RUNTIME_CONFIG_FILE:-}" ]]; then
  if [[ "${DORAEMON_RUNTIME_CONFIG_FILE}" != "/etc/doraemon/runtime.env" ||
        "$(stat -c '%U %a' /etc/doraemon/runtime.env 2>/dev/null || true)" != "root 640" ]]; then
    echo "[ERROR] commercial runtime config must be /etc/doraemon/runtime.env, root-owned mode 0640" >&2
    exit 1
  fi
  commercial_validate_runtime_env_file /etc/doraemon/runtime.env || exit 1
fi

log() {
  echo "[$(date '+%F %T')] $*"
}

elapsed_sec() {
  local now
  now="$(date +%s)"
  echo $((now - START_TS))
}

has_time_left() {
  (( "$(elapsed_sec)" < TIMEOUT_SEC ))
}

wait_for() {
  local label="$1"
  shift

  while true; do
    if "$@" >/dev/null 2>&1; then
      log "[OK] ${label}"
      return 0
    fi

    if ! has_time_left; then
      log "[ERROR] timeout waiting for ${label} after ${TIMEOUT_SEC}s"
      return 1
    fi

    sleep 1
  done
}

has_device() {
  [[ -e "$1" ]]
}

has_usb_serial() {
  local expected_serial="$1"
  local serial_file
  for serial_file in /sys/bus/usb/devices/*/serial; do
    [[ -r "${serial_file}" ]] || continue
    [[ "$(<"${serial_file}")" == "${expected_serial}" ]] && return 0
  done
  return 1
}

is_placeholder() {
  commercial_value_is_placeholder "${1:-}"
}

validate_unique_camera_identities() {
  commercial_validate_required_orbbec_identities \
    "${ORBBEC_CAMERA1_SERIAL_NUMBER}" \
    "${ORBBEC_CAMERA2_SERIAL_NUMBER}" \
    "${ORBBEC_CAMERA3_SERIAL_NUMBER}" \
    "${ORBBEC_CAMERA1_USB_PORT}" \
    "${ORBBEC_CAMERA2_USB_PORT}" \
    "${ORBBEC_CAMERA3_USB_PORT}"
}

orbbec_list_devices_binary() {
  for candidate in \
    "${REPO_ROOT}/install/lib/orbbec_camera/list_devices_node" \
    "${REPO_ROOT}/devel/lib/orbbec_camera/list_devices_node"; do
    [[ -x "${candidate}" ]] && printf '%s' "${candidate}" && return 0
  done
  return 1
}

orbbec_workspace_setup() {
  for candidate in "${REPO_ROOT}/install/setup.bash" "${REPO_ROOT}/devel/setup.bash"; do
    [[ -f "${candidate}" ]] && printf '%s' "${candidate}" && return 0
  done
  return 1
}

has_workspace_setup() {
  [[ -f "${REPO_ROOT}/install/setup.bash" || -f "${REPO_ROOT}/devel/setup.bash" ]]
}

has_a_box_ip() {
  if [[ -n "${A_BOX_IFACE}" ]]; then
    ip -4 addr show dev "${A_BOX_IFACE}" | grep -q "inet ${A_BOX_IP}/"
    return
  fi
  ip -4 addr show | grep -q "inet ${A_BOX_IP}/"
}

can_ping() {
  ping -c 1 -W 1 "$1"
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

validate_orbbec_commercial_baseline() {
  local normalized_vendor="${ORBBEC_VENDOR_ID,,}"

  if [[ "${normalized_vendor}" != "${COMMERCIAL_ORBBEC_VENDOR_ID}" ]]; then
    log "[ERROR] DORAEMON_ORBBEC_VENDOR_ID must remain ${COMMERCIAL_ORBBEC_VENDOR_ID} for the commercial baseline"
    return 1
  fi
  ORBBEC_VENDOR_ID="${normalized_vendor}"

  if [[ ! "${ORBBEC_MIN_USB_SPEED}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    log "[ERROR] DORAEMON_ORBBEC_MIN_USB_SPEED must be numeric and at least ${COMMERCIAL_ORBBEC_MIN_USB_SPEED}"
    return 1
  fi
  if ! awk -v requested="${ORBBEC_MIN_USB_SPEED}" \
      -v baseline="${COMMERCIAL_ORBBEC_MIN_USB_SPEED}" \
      'BEGIN { exit !(requested >= baseline) }'; then
    log "[ERROR] DORAEMON_ORBBEC_MIN_USB_SPEED must not be below ${COMMERCIAL_ORBBEC_MIN_USB_SPEED}"
    return 1
  fi
}

validate_installed_commercial_udev_rules() {
  local require_orbbec_rule="${1:-true}"
  local serial_rule="/etc/udev/rules.d/99-doraemon-a26022-serial.rules"
  local orbbec_rule="/etc/udev/rules.d/99-obsensor-ros1-libusb.rules"
  local expected_orbbec_rule="${REPO_ROOT}/src/orbbec-ros-sdk/scripts/99-obsensor-ros1-libusb.rules"

  if [[ ! -f "${serial_rule}" || -L "${serial_rule}" || \
        "$(stat -c '%U:%G %a' "${serial_rule}" 2>/dev/null || true)" != "root:root 644" ]]; then
    log "[ERROR] installed udev rule must be a root:root 0644 regular file: ${serial_rule}"
    return 1
  fi
  if grep -q 'REPLACE_' "${serial_rule}"; then
    log "[ERROR] serial udev rule still contains a placeholder"
    return 1
  fi

  if truthy "${require_orbbec_rule}"; then
    if [[ ! -f "${orbbec_rule}" || -L "${orbbec_rule}" || \
          "$(stat -c '%U:%G %a' "${orbbec_rule}" 2>/dev/null || true)" != "root:root 644" ]]; then
      log "[ERROR] installed udev rule must be a root:root 0644 regular file: ${orbbec_rule}"
      return 1
    fi
    if [[ ! -f "${expected_orbbec_rule}" || -L "${expected_orbbec_rule}" ]] || \
        ! cmp -s "${expected_orbbec_rule}" "${orbbec_rule}"; then
      log "[ERROR] installed Orbbec udev rule does not match this release"
      return 1
    fi
    log "[OK] installed serial and Orbbec udev rules are root-managed"
  else
    log "[OK] installed serial udev rule is root-managed"
    log "[SKIP] Orbbec udev rule audit disabled by non-blocking camera mode"
  fi
}

validate_serial_alias_permissions() {
  local alias_path="$1"
  local alias_name="$2"
  local serial_rule="/etc/udev/rules.d/99-doraemon-a26022-serial.rules"
  local target=""
  local id_path=""

  if [[ ! -L "${alias_path}" ]]; then
    log "[ERROR] required serial alias is not a symlink: ${alias_path}"
    return 1
  fi
  target="$(realpath -e -- "${alias_path}" 2>/dev/null)" || {
    log "[ERROR] required serial alias is dangling: ${alias_path}"
    return 1
  }
  if [[ ! -c "${target}" || "${target}" != /dev/ttyUSB* || \
        "$(stat -c '%U:%G %a' "${target}" 2>/dev/null || true)" != "root:dialout 660" ]]; then
    log "[ERROR] ${alias_path} must resolve to a root:dialout 0660 ttyUSB device"
    return 1
  fi
  id_path="$(udevadm info --query=property --name="${target}" 2>/dev/null | \
    sed -n 's/^ID_PATH=//p' | head -n1)"
  if [[ -z "${id_path}" ]] || ! awk -v alias_name="${alias_name}" -v id_path="${id_path}" '
      index($0, "ENV{ID_PATH}==\"" id_path "\"") &&
      index($0, "SYMLINK+=\"" alias_name "\"") { found = 1 }
      END { exit !found }
    ' "${serial_rule}"; then
    log "[ERROR] ${alias_path} does not match its locally measured ID_PATH rule"
    return 1
  fi
  log "[OK] ${alias_path} -> ${target} is root:dialout 0660 and matches ID_PATH=${id_path}"
}

validate_orbbec_usb_node_permissions() {
  local vendor_file=""
  local device_root=""
  local busnum=""
  local devnum=""
  local usb_node=""
  local node_count=0

  for vendor_file in /sys/bus/usb/devices/*/idVendor; do
    [[ -r "${vendor_file}" ]] || continue
    [[ "$(<"${vendor_file}")" == "${ORBBEC_VENDOR_ID}" ]] || continue
    device_root="${vendor_file%/idVendor}"
    [[ -r "${device_root}/busnum" && -r "${device_root}/devnum" ]] || {
      log "[ERROR] Orbbec sysfs node lacks busnum/devnum: ${device_root}"
      return 1
    }
    busnum="$(<"${device_root}/busnum")"
    devnum="$(<"${device_root}/devnum")"
    printf -v usb_node '/dev/bus/usb/%03d/%03d' "${busnum}" "${devnum}"
    if [[ ! -c "${usb_node}" || -L "${usb_node}" || \
          "$(stat -c '%U:%G %a' "${usb_node}" 2>/dev/null || true)" != "root:video 660" ]]; then
      log "[ERROR] Orbbec USB node must be root:video 0660: ${usb_node}"
      return 1
    fi
    node_count=$((node_count + 1))
  done
  if (( node_count < 3 )); then
    log "[ERROR] expected at least three local Orbbec USB device nodes, found ${node_count}"
    return 1
  fi
  log "[OK] ${node_count} Orbbec USB device nodes are root:video 0660"
}

if [[ ! "${TIMEOUT_SEC}" =~ ^[1-9][0-9]*$ ]] || (( TIMEOUT_SEC > 600 )); then
  log "[ERROR] DORAEMON_BOOT_WAIT_TIMEOUT must be an integer from 1 through 600"
  exit 1
fi

START_TS="$(date +%s)"

log "waiting for Doraemon robot boot dependencies"
log "repo=${REPO_ROOT} chassis=${CHASSIS_DRIVER} chassis_device=${CHASSIS_DEVICE:-none} a_box=${A_BOX_IP} a_box_iface=${A_BOX_IFACE:-auto} mbox=${MBOX_IP} lidar=${LIDAR_IP} imu=${IMU_DEVICE} odom=${ODOM_DEVICE} timeout=${TIMEOUT_SEC}s"

if is_placeholder "${ROBOT_ID}" || [[ "${ROBOT_ID}" == "local_robot" ]]; then
  log "[ERROR] ROBOT_ID must be the explicit vehicle asset identifier"
  exit 1
fi
if is_placeholder "${A_BOX_IFACE}"; then
  log "[ERROR] DORAEMON_A_BOX_IFACE must be the explicit internal wired interface"
  exit 1
fi
if [[ "${ROSBRIDGE_ADDRESS}" != "127.0.0.1" ]]; then
  log "[ERROR] commercial rosbridge must bind only to 127.0.0.1"
  exit 1
fi
for boolean_name in START_DEPTH_CAMERAS REQUIRE_DEPTH_CAMERA_TOPICS REQUIRE_DEPTH_CAMERA_IDENTITIES NO_ACTION_ACCEPTANCE ACTION_TEST_APPROVED; do
  if ! valid_boolean "${!boolean_name}"; then
    log "[ERROR] ${boolean_name} must be an explicit boolean"
    exit 1
  fi
done
if truthy "${NO_ACTION_ACCEPTANCE}"; then
  if truthy "${ACTION_TEST_APPROVED}"; then
    log "[ERROR] no-action acceptance requires DORAEMON_ACTION_TEST_APPROVED=false"
    exit 1
  fi
  log "[OK] no-action acceptance mode is enabled"
else
  if ! truthy "${ACTION_TEST_APPROVED}"; then
    log "[ERROR] action-capable runtime requires DORAEMON_ACTION_TEST_APPROVED=true"
    exit 1
  fi
  log "[WARN] explicitly approved action-test mode is enabled"
fi
udevadm settle --timeout=10 || true

if truthy "${START_DEPTH_CAMERAS}" && truthy "${REQUIRE_DEPTH_CAMERA_IDENTITIES}"; then
  validate_orbbec_commercial_baseline
  validate_installed_commercial_udev_rules true
  validate_orbbec_usb_node_permissions
else
  validate_installed_commercial_udev_rules false
  log "[WARN] Orbbec device, USB3 topology, permissions, and SDK checks are disabled; camera faults will not block robot startup"
fi
validate_serial_alias_permissions "${IMU_DEVICE}" imu
validate_serial_alias_permissions "${ODOM_DEVICE}" wheel_odom

wait_for "workspace setup" has_workspace_setup
if truthy "${REQUIRE_IMU_DEVICE}"; then
  wait_for "${IMU_DEVICE}" has_device "${IMU_DEVICE}"
else
  log "[SKIP] IMU device check disabled"
fi
if truthy "${REQUIRE_ODOM_DEVICE}"; then
  wait_for "${ODOM_DEVICE}" has_device "${ODOM_DEVICE}"
else
  log "[SKIP] odom device check disabled"
fi
if truthy "${REQUIRE_CHASSIS_DEVICE}"; then
  wait_for "${CHASSIS_DEVICE}" has_device "${CHASSIS_DEVICE}"
else
  log "[SKIP] chassis device check disabled"
fi
wait_for "A-box IP ${A_BOX_IP}" has_a_box_ip
if truthy "${REQUIRE_MBOX_PING}"; then
  wait_for "M-box ${MBOX_IP}" can_ping "${MBOX_IP}"
else
  log "[SKIP] M-box ping check disabled"
fi
if truthy "${REQUIRE_LIDAR_PING}"; then
  wait_for "LiDAR ${LIDAR_IP}" can_ping "${LIDAR_IP}"
else
  log "[SKIP] LiDAR ping check disabled"
fi
if truthy "${START_DEPTH_CAMERAS}" && truthy "${REQUIRE_DEPTH_CAMERA_IDENTITIES}"; then
  if ! validate_unique_camera_identities; then
    log "[ERROR] Orbbec serials/topologies must be explicit, valid, and unique"
    exit 1
  fi
  wait_for "Orbbec left serial ${ORBBEC_CAMERA1_SERIAL_NUMBER}" has_usb_serial "${ORBBEC_CAMERA1_SERIAL_NUMBER}"
  wait_for "Orbbec right serial ${ORBBEC_CAMERA2_SERIAL_NUMBER}" has_usb_serial "${ORBBEC_CAMERA2_SERIAL_NUMBER}"
  wait_for "Orbbec front serial ${ORBBEC_CAMERA3_SERIAL_NUMBER}" has_usb_serial "${ORBBEC_CAMERA3_SERIAL_NUMBER}"
  orbbec_binary="$(orbbec_list_devices_binary)" || {
    log "[ERROR] fixed Orbbec SDK enumerator is unavailable in this release"
    exit 1
  }
  workspace_setup="$(orbbec_workspace_setup)" || {
    log "[ERROR] workspace setup disappeared before Orbbec SDK enumeration"
    exit 1
  }
  set +u
  # shellcheck disable=SC1091
  source /opt/ros/noetic/setup.bash
  # shellcheck disable=SC1090
  source "${workspace_setup}"
  set -u
  orbbec_remaining_sec=$((TIMEOUT_SEC - $(elapsed_sec)))
  if (( orbbec_remaining_sec <= 0 )); then
    log "[ERROR] global boot wait budget was exhausted before Orbbec SDK enumeration"
    exit 1
  fi
  if ! python3 "${SCRIPT_DIR}/verify_orbbec_sdk_pairs.py" \
      --binary "${orbbec_binary}" \
      --timeout-seconds "${orbbec_remaining_sec}" \
      --allow-topology-remap \
      --vendor-id "${ORBBEC_VENDOR_ID}" \
      --min-usb-speed "${ORBBEC_MIN_USB_SPEED}" \
      --expected "${ORBBEC_CAMERA1_SERIAL_NUMBER}|${ORBBEC_CAMERA1_USB_PORT}" \
      --expected "${ORBBEC_CAMERA2_SERIAL_NUMBER}|${ORBBEC_CAMERA2_USB_PORT}" \
      --expected "${ORBBEC_CAMERA3_SERIAL_NUMBER}|${ORBBEC_CAMERA3_USB_PORT}"; then
    log "[ERROR] Orbbec SDK serial identity/USB3 stability gate failed"
    exit 1
  fi
else
  log "[SKIP] Orbbec serial/USB3/SDK readiness gate disabled; continuing without camera startup dependency"
fi

log "[OK] Doraemon robot boot dependencies are ready"
