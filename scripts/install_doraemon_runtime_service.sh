#!/usr/bin/env bash

set -euo pipefail

if [[ "${EUID}" -eq 0 ]]; then
  echo "[ERROR] run this script as the deployment user; it invokes sudo itself" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVICE_USER="${DORAEMON_SERVICE_USER:-${USER}}"
SERVICE_GROUP="${DORAEMON_SERVICE_GROUP:-$(id -gn "${SERVICE_USER}")}"
SERVICE_NAME="${DORAEMON_SERVICE_NAME:-doraemon-runtime.service}"
ENABLE_SERVICE="${DORAEMON_ENABLE_SERVICE:-0}"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
RUNTIME_ENV="/etc/doraemon/runtime.env"
DEPS_ENV="/etc/doraemon/deps.env"

if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  echo "[ERROR] service user does not exist: ${SERVICE_USER}" >&2
  exit 1
fi
SERVICE_HOME="$(getent passwd "${SERVICE_USER}" | cut -d: -f6)"
if [[ -z "${SERVICE_HOME}" || ! -d "${SERVICE_HOME}" ]]; then
  echo "[ERROR] service user home does not exist: ${SERVICE_HOME:-unknown}" >&2
  exit 1
fi

if [[ -f "${REPO_ROOT}/install/setup.bash" ]]; then
  WORKSPACE_SETUP="${REPO_ROOT}/install/setup.bash"
elif [[ -f "${REPO_ROOT}/devel/setup.bash" ]]; then
  WORKSPACE_SETUP="${REPO_ROOT}/devel/setup.bash"
else
  echo "[ERROR] workspace setup is missing; build the backend first" >&2
  exit 1
fi

if [[ ! -f "${DEPS_ENV}" ]]; then
  echo "[ERROR] missing ${DEPS_ENV}; install native dependencies first" >&2
  exit 1
fi

sudo -v
sudo install -d -m 0755 /etc/doraemon
sudo install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" -m 0750 \
  /var/lib/doraemon \
  /var/lib/doraemon/ros \
  /var/log/doraemon \
  /var/log/doraemon/startup \
  /data/coverage \
  /data/maps \
  /data/maps/imports \
  /data/config/slam/cartographer

if [[ ! -f "${RUNTIME_ENV}" ]]; then
  sudo install -m 0640 -o root -g "${SERVICE_GROUP}" \
    "${REPO_ROOT}/config/runtime.a26022.env" "${RUNTIME_ENV}"
  echo "[OK] installed initial ${RUNTIME_ENV}"
else
  echo "[KEEP] existing ${RUNTIME_ENV}"
fi

sudo usermod -aG dialout,plugdev,video "${SERVICE_USER}"

unit_file="$(mktemp)"
trap 'rm -f "${unit_file}"' EXIT
cat >"${unit_file}" <<EOF
[Unit]
Description=Doraemon Robot Runtime
After=local-fs.target network-online.target systemd-udev-settle.service
Wants=network-online.target systemd-udev-settle.service

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${REPO_ROOT}
Environment=HOME=${SERVICE_HOME}
Environment=TERM=xterm-256color
Environment=DORAEMON_REPO_ROOT=${REPO_ROOT}
Environment=DORAEMON_RUNTIME_CONFIG_FILE=${RUNTIME_ENV}
Environment=DORAEMON_WORKSPACE_SETUP=${WORKSPACE_SETUP}
Environment=ROS_HOME=/var/lib/doraemon/ros
Environment=LOG_DIR=/var/log/doraemon/startup
Environment=ALLOW_NO_ACTIVE_MAP_STARTUP=1
Environment=RUN_BACKEND_RUNTIME_SMOKE=0
Environment=DORAEMON_BOOT_WAIT_TIMEOUT=120
EnvironmentFile=-${DEPS_ENV}
EnvironmentFile=-${RUNTIME_ENV}
ExecStartPre=${REPO_ROOT}/scripts/wait_robot_boot_ready.sh
ExecStart=${REPO_ROOT}/scripts/start_runtime.sh
ExecStop=${REPO_ROOT}/scripts/stop_all_backend.sh
RemainAfterExit=yes
TimeoutStartSec=240
TimeoutStopSec=90
Restart=on-failure
RestartSec=15
UMask=0027
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

sudo install -m 0644 "${unit_file}" "${UNIT_PATH}"
sudo systemctl daemon-reload

if [[ "${ENABLE_SERVICE}" == "1" ]]; then
  sudo systemctl enable "${SERVICE_NAME}"
  echo "[OK] installed and enabled ${SERVICE_NAME}"
else
  echo "[OK] installed ${SERVICE_NAME}; automatic startup state was not changed"
fi
echo "[INFO] service was not started"
echo "[INFO] review ${RUNTIME_ENV}, udev aliases, network addresses, and E-stop before starting"
echo "[INFO] start with: sudo systemctl start ${SERVICE_NAME}"
echo "[INFO] after acceptance, enable with: sudo systemctl enable ${SERVICE_NAME}"
