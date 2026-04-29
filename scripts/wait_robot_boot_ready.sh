#!/usr/bin/env bash

set -euo pipefail

TIMEOUT_SEC="${DORAEMON_BOOT_WAIT_TIMEOUT:-120}"
CHASSIS_DRIVER="${DORAEMON_CHASSIS_DRIVER:-${CHASSIS_DRIVER:-legacy_mcore}}"
case "${CHASSIS_DRIVER}" in
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
if [[ "${CHASSIS_DRIVER}" == "wheeltec_senior_diff" ]]; then
  DEFAULT_CHASSIS_DEVICE="${DORAEMON_WHEELTEC_BY_ID_DEVICE:-/dev/serial/by-id/usb-WCH.CN_USB_Single_Serial_0002-if00}"
  DEFAULT_REQUIRE_IMU_DEVICE="false"
  DEFAULT_REQUIRE_ODOM_DEVICE="false"
  DEFAULT_REQUIRE_CHASSIS_DEVICE="true"
  DEFAULT_REQUIRE_MBOX_PING="false"
fi
A_BOX_IP="${DORAEMON_A_BOX_IP:-192.168.16.11}"
MBOX_IP="${DORAEMON_MBOX_IP:-192.168.16.10}"
LIDAR_IP="${DORAEMON_LIDAR_IP:-192.168.16.23}"
IMU_DEVICE="${DORAEMON_IMU_DEVICE:-/dev/imu}"
ODOM_DEVICE="${DORAEMON_ODOM_DEVICE:-/dev/odom}"
CHASSIS_DEVICE="${DORAEMON_CHASSIS_DEVICE:-${WHEELTEC_SERIAL_DEVICE:-${DEFAULT_CHASSIS_DEVICE}}}"
REQUIRE_IMU_DEVICE="${DORAEMON_REQUIRE_IMU_DEVICE:-${DEFAULT_REQUIRE_IMU_DEVICE}}"
REQUIRE_ODOM_DEVICE="${DORAEMON_REQUIRE_ODOM_DEVICE:-${DEFAULT_REQUIRE_ODOM_DEVICE}}"
REQUIRE_CHASSIS_DEVICE="${DORAEMON_REQUIRE_CHASSIS_DEVICE:-${DEFAULT_REQUIRE_CHASSIS_DEVICE}}"
REQUIRE_MBOX_PING="${DORAEMON_REQUIRE_MBOX_PING:-${DEFAULT_REQUIRE_MBOX_PING}}"
REQUIRE_LIDAR_PING="${DORAEMON_REQUIRE_LIDAR_PING:-true}"
REPO_ROOT="${DORAEMON_REPO_ROOT:-/home/linaro/Doraemon}"

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

has_workspace_setup() {
  [[ -f "${REPO_ROOT}/install/setup.bash" || -f "${REPO_ROOT}/devel/setup.bash" ]]
}

has_eth0_ip() {
  ip -4 addr show dev eth0 | grep -q "inet ${A_BOX_IP}/"
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

START_TS="$(date +%s)"

log "waiting for Doraemon robot boot dependencies"
log "repo=${REPO_ROOT} chassis=${CHASSIS_DRIVER} chassis_device=${CHASSIS_DEVICE:-none} a_box=${A_BOX_IP} mbox=${MBOX_IP} lidar=${LIDAR_IP} imu=${IMU_DEVICE} odom=${ODOM_DEVICE} timeout=${TIMEOUT_SEC}s"

udevadm settle --timeout=10 || true

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
wait_for "eth0 ${A_BOX_IP}" has_eth0_ip
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

log "[OK] Doraemon robot boot dependencies are ready"
