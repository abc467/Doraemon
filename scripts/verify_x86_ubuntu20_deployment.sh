#!/usr/bin/env bash

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FAILURES=0
WARNINGS=0

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

[[ "$(uname -m)" == "x86_64" ]] && ok "architecture x86_64" || fail "architecture must be x86_64"
check_file /opt/ros/noetic/setup.bash
check_file /etc/doraemon/deps.env
check_file /etc/doraemon/runtime.env

if [[ -f /etc/doraemon/deps.env ]]; then
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

for device in /dev/imu /dev/wheel_odom; do
  [[ -e "${device}" ]] && ok "device ${device}" || warn "device ${device} is not present"
done

for address in 192.168.127.10 192.168.127.23 192.168.127.12; do
  ping -c 1 -W 1 "${address}" >/dev/null 2>&1 &&
    ok "network ${address}" ||
    warn "cannot reach ${address}"
done

if systemctl is-enabled doraemon-runtime.service >/dev/null 2>&1; then
  ok "doraemon-runtime.service enabled"
else
  warn "doraemon-runtime.service is not enabled"
fi

echo "summary: failures=${FAILURES} warnings=${WARNINGS}"
[[ "${FAILURES}" -eq 0 ]]
