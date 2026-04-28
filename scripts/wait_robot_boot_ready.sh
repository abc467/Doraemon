#!/usr/bin/env bash

set -euo pipefail

TIMEOUT_SEC="${DORAEMON_BOOT_WAIT_TIMEOUT:-120}"
A_BOX_IP="${DORAEMON_A_BOX_IP:-192.168.16.11}"
MBOX_IP="${DORAEMON_MBOX_IP:-192.168.16.10}"
LIDAR_IP="${DORAEMON_LIDAR_IP:-192.168.16.23}"
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

START_TS="$(date +%s)"

log "waiting for Doraemon robot boot dependencies"
log "repo=${REPO_ROOT} a_box=${A_BOX_IP} mbox=${MBOX_IP} lidar=${LIDAR_IP} timeout=${TIMEOUT_SEC}s"

udevadm settle --timeout=10 || true

wait_for "workspace setup" has_workspace_setup
wait_for "/dev/imu" has_device /dev/imu
wait_for "/dev/odom" has_device /dev/odom
wait_for "eth0 ${A_BOX_IP}" has_eth0_ip
wait_for "M-box ${MBOX_IP}" can_ping "${MBOX_IP}"
wait_for "LiDAR ${LIDAR_IP}" can_ping "${LIDAR_IP}"

log "[OK] Doraemon robot boot dependencies are ready"
