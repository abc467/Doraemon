#!/usr/bin/env bash

set -euo pipefail

if [[ "${EUID}" -eq 0 ]]; then
  echo "[ERROR] run this script as the deployment user; it invokes sudo itself" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVICE_USER="${DORAEMON_SERVICE_USER:-${USER}}"
SERVICE_GROUP="${DORAEMON_SERVICE_GROUP:-$(id -gn "${SERVICE_USER}")}"
SERVICE_NAME="${DORAEMON_SERVICE_NAME:-doraemon-runtime.service}"
ENABLE_SERVICE="${DORAEMON_ENABLE_SERVICE:-0}"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
ORBBEC_USB_SERVICE_NAME="doraemon-orbbec-usb-preflight.service"
ORBBEC_USB_UNIT_PATH="/etc/systemd/system/${ORBBEC_USB_SERVICE_NAME}"
RUNTIME_ENV="/etc/doraemon/runtime.env"
DEPS_ENV="/etc/doraemon/deps.env"
VERSIONS_FILE="${REPO_ROOT}/deploy/manifests/x86_ubuntu20_versions.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_vehicle_identity.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_filesystem_security.sh"
# shellcheck disable=SC1090
source "${VERSIONS_FILE}"

if [[ "${SERVICE_USER}" != "a" || "${SERVICE_GROUP}" != "a" ]]; then
  echo "[ERROR] this commercial vehicle profile requires service user/group a:a" >&2
  exit 1
fi
if [[ "${SERVICE_NAME}" != "doraemon-runtime.service" ]]; then
  echo "[ERROR] commercial runtime service name is fixed at doraemon-runtime.service" >&2
  exit 1
fi
if [[ "${ENABLE_SERVICE}" != "0" ]]; then
  echo "[ERROR] installer may not enable the runtime; enable it explicitly only after final acceptance" >&2
  exit 1
fi

if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  echo "[ERROR] service user does not exist: ${SERVICE_USER}" >&2
  exit 1
fi
if [[ "$(id -u "${SERVICE_USER}")" -eq 0 ]]; then
  echo "[ERROR] robot runtime service user must never be root" >&2
  exit 1
fi
SERVICE_HOME="$(getent passwd "${SERVICE_USER}" | cut -d: -f6)"
if [[ -z "${SERVICE_HOME}" || ! -d "${SERVICE_HOME}" ]]; then
  echo "[ERROR] service user home does not exist: ${SERVICE_HOME:-unknown}" >&2
  exit 1
fi

case "${REPO_ROOT}" in
  /opt/doraemon/releases/*)
    ;;
  *)
    echo "[ERROR] commercial runtime must be installed from /opt/doraemon/releases/<tag>: ${REPO_ROOT}" >&2
    exit 1
    ;;
esac
RELEASES_ROOT="$(dirname "${REPO_ROOT}")"
for immutable_ancestor in /opt /opt/doraemon "${RELEASES_ROOT}"; do
  if [[ ! -d "${immutable_ancestor}" || -L "${immutable_ancestor}" || \
        "$(stat -c '%U:%G %a' "${immutable_ancestor}" 2>/dev/null || true)" != \
          "root:root 755" ]]; then
    echo "[ERROR] release ancestor must be a real root:root mode 0755 directory: ${immutable_ancestor}" >&2
    exit 1
  fi
done
if ! commercial_verify_release_git_identity "${REPO_ROOT}" \
    "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
    "${DORAEMON_BACKEND_GIT_URL}"; then
  exit 1
fi
if NESTED_RELEASE_MOUNT="$(commercial_find_mount_below "${REPO_ROOT}" 0)"; then
  echo "[ERROR] release contains a nested mount: ${NESTED_RELEASE_MOUNT}" >&2
  exit 1
fi
if UNSAFE_RELEASE_SYMLINK="$(commercial_find_unsafe_release_symlink "${REPO_ROOT}")"; then
  echo "[ERROR] release contains a dangling or unapproved escaping symlink: ${UNSAFE_RELEASE_SYMLINK}" >&2
  exit 1
fi
FORBIDDEN_RELEASE_ARTIFACT="$(commercial_find_forbidden_release_artifact "${REPO_ROOT}")"
if [[ -n "${FORBIDDEN_RELEASE_ARTIFACT}" ]]; then
  echo "[ERROR] release contains forbidden generated/runtime data: ${FORBIDDEN_RELEASE_ARTIFACT}" >&2
  exit 1
fi
if ! NON_ROOT_RELEASE_ENTRY="$(find "${REPO_ROOT}" -xdev \
    \( -type f -o -type d -o -type l \) \
    \( ! -user root -o ! -group root \) -print -quit 2>&1)"; then
  echo "[ERROR] failed to audit release ownership: ${NON_ROOT_RELEASE_ENTRY}" >&2
  exit 1
fi
if [[ -n "${NON_ROOT_RELEASE_ENTRY}" ]]; then
  echo "[ERROR] release is not frozen root:root: ${NON_ROOT_RELEASE_ENTRY}" >&2
  exit 1
fi
if ! WRITABLE_RELEASE_ENTRY="$(find "${REPO_ROOT}" -xdev \
    \( -type f -o -type d \) -perm /022 -print -quit 2>&1)"; then
  echo "[ERROR] failed to audit release permissions: ${WRITABLE_RELEASE_ENTRY}" >&2
  exit 1
fi
if [[ -n "${WRITABLE_RELEASE_ENTRY}" ]]; then
  echo "[ERROR] release contains a group/other-writable path: ${WRITABLE_RELEASE_ENTRY}" >&2
  exit 1
fi

if systemctl cat "${SERVICE_NAME}" >/dev/null 2>&1; then
  PREINSTALL_ACTIVE_STATE="$(systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true)"
  PREINSTALL_ENABLED_STATE="$(systemctl is-enabled "${SERVICE_NAME}" 2>/dev/null || true)"
  if [[ "${PREINSTALL_ACTIVE_STATE}" != "inactive" || \
        "${PREINSTALL_ENABLED_STATE}" != "disabled" ]]; then
    echo "[ERROR] existing ${SERVICE_NAME} must be exactly inactive and disabled before installation" >&2
    echo "[ERROR] actual active=${PREINSTALL_ACTIVE_STATE:-missing} enabled=${PREINSTALL_ENABLED_STATE:-missing}" >&2
    echo "[ERROR] investigate and clear any failed state explicitly; the installer will not hide it" >&2
    exit 1
  fi
fi

if [[ -f "${REPO_ROOT}/install/setup.bash" ]]; then
  WORKSPACE_SETUP="${REPO_ROOT}/install/setup.bash"
elif [[ -f "${REPO_ROOT}/devel/setup.bash" ]]; then
  WORKSPACE_SETUP="${REPO_ROOT}/devel/setup.bash"
else
  echo "[ERROR] workspace setup is missing; build the backend first" >&2
  exit 1
fi
commercial_validate_workspace_build_provenance \
  "${REPO_ROOT}" \
  "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
  /opt/doraemon/deps/cmake-3.20.6/bin/cmake \
  /usr/bin/gcc-10 \
  /usr/bin/g++-10 \
  "${DORAEMON_BACKEND_GIT_URL}"

if [[ ! -f "${DEPS_ENV}" || -L "${DEPS_ENV}" || \
      "$(stat -c '%U:%G %a' "${DEPS_ENV}" 2>/dev/null || true)" != "root:root 644" ]]; then
  echo "[ERROR] missing ${DEPS_ENV}; install native dependencies first" >&2
  exit 1
fi
commercial_validate_dependencies_env_file "${DEPS_ENV}"
if [[ -L /etc/ld.so.conf.d/doraemon-deps.conf || \
      ! -f /etc/ld.so.conf.d/doraemon-deps.conf || \
      "$(stat -c '%U:%G %a' /etc/ld.so.conf.d/doraemon-deps.conf 2>/dev/null || true)" != \
        "root:root 644" ]] || \
    ! cmp -s /etc/ld.so.conf.d/doraemon-deps.conf \
      "${REPO_ROOT}/config/doraemon-deps.ld.so.conf"; then
  echo "[ERROR] dynamic-loader dependency configuration does not match the release template" >&2
  exit 1
fi
if [[ ! -d /opt/doraemon/deps || -L /opt/doraemon/deps || \
      "$(stat -c '%U:%G %a' /opt/doraemon/deps 2>/dev/null || true)" != "root:root 755" ]]; then
  echo "[ERROR] dependency root must be a real root:root 0755 directory" >&2
  exit 1
fi
if NESTED_DEPS_MOUNT="$(commercial_find_mount_below /opt/doraemon/deps 0)"; then
  echo "[ERROR] dependency tree contains a nested mount: ${NESTED_DEPS_MOUNT}" >&2
  exit 1
fi
if UNSAFE_DEPS_SYMLINK="$(commercial_find_symlink_outside_tree /opt/doraemon/deps)"; then
  echo "[ERROR] dependency tree contains a dangling or escaping symlink: ${UNSAFE_DEPS_SYMLINK}" >&2
  exit 1
fi
if ! NON_ROOT_DEPS_ENTRY="$(find /opt/doraemon/deps -xdev \
    \( -type f -o -type d -o -type l \) \
    \( ! -user root -o ! -group root \) -print -quit 2>&1)"; then
  echo "[ERROR] failed to audit dependency tree ownership: ${NON_ROOT_DEPS_ENTRY}" >&2
  exit 1
fi
if [[ -n "${NON_ROOT_DEPS_ENTRY}" ]]; then
  echo "[ERROR] dependency tree contains a non-root-owned path: ${NON_ROOT_DEPS_ENTRY}" >&2
  exit 1
fi
if ! WRITABLE_DEPS_ENTRY="$(find /opt/doraemon/deps -xdev \
    \( -type f -o -type d \) -perm /022 -print -quit 2>&1)"; then
  echo "[ERROR] failed to audit dependency tree permissions: ${WRITABLE_DEPS_ENTRY}" >&2
  exit 1
fi
if [[ -n "${WRITABLE_DEPS_ENTRY}" ]]; then
  echo "[ERROR] dependency tree contains a group/other-writable path: ${WRITABLE_DEPS_ENTRY}" >&2
  exit 1
fi
if [[ -L /etc/doraemon || ( -e /etc/doraemon && ! -d /etc/doraemon ) ]]; then
  echo "[ERROR] /etc/doraemon must be a real non-symlink directory" >&2
  exit 1
fi

assert_external_mutable_directory_safe() {
  local path="$1"
  local resolved=""
  if [[ -L "${path}" || ( -e "${path}" && ! -d "${path}" ) ]]; then
    echo "[ERROR] mutable runtime path must be a real directory, not a symlink: ${path}" >&2
    return 1
  fi
  if [[ -e "${path}" ]] && mountpoint -q "${path}"; then
    echo "[ERROR] mutable runtime path must not be a bind/mount point without a reviewed storage profile: ${path}" >&2
    return 1
  fi
  resolved="$(realpath -m "${path}")"
  case "${resolved}" in
    /opt/doraemon/releases|/opt/doraemon/releases/*)
      echo "[ERROR] mutable runtime path resolves into the immutable release tree: ${path}" >&2
      return 1
      ;;
  esac
  case "${path}" in
    /data/*)
      case "${resolved}" in
        /data/*) ;;
        *)
          echo "[ERROR] data path resolves outside /data: ${path} -> ${resolved}" >&2
          return 1
          ;;
      esac
      ;;
  esac
}

sudo -v
if [[ ! -e /data ]]; then
  sudo install -d -o root -g root -m 0755 /data
fi
if [[ -L /data || ! -d /data || "$(realpath -m /data)" != "/data" ]]; then
  echo "[ERROR] /data must be a real directory (a reviewed dedicated /data mount is allowed)" >&2
  exit 1
fi
for mutable_path in \
  /var/lib/doraemon \
  /var/lib/doraemon/orbbec-captures \
  /var/lib/doraemon/ros \
  /var/log/doraemon \
  /var/log/doraemon/orbbec \
  /var/log/doraemon/startup \
  /var/log/doraemon/slam-runtime \
  /data/coverage \
  /data/maps \
  /data/maps/imports; do
  assert_external_mutable_directory_safe "${mutable_path}"
done

sudo install -d -m 0755 /etc/doraemon
if [[ "$(stat -c '%U:%G %a' /etc/doraemon 2>/dev/null || true)" != "root:root 755" ]]; then
  echo "[ERROR] /etc/doraemon must be root:root mode 0755" >&2
  exit 1
fi
sudo install -d -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" -m 0750 \
  /var/lib/doraemon \
  /var/lib/doraemon/orbbec-captures \
  /var/lib/doraemon/ros \
  /var/log/doraemon \
  /var/log/doraemon/orbbec \
  /var/log/doraemon/startup \
  /var/log/doraemon/slam-runtime \
  /data/coverage \
  /data/maps \
  /data/maps/imports

for config_path in \
  /data/config \
  /data/config/slam \
  /data/config/slam/cartographer; do
  if [[ -L "${config_path}" || ( -e "${config_path}" && ! -d "${config_path}" ) ]]; then
    echo "[ERROR] SLAM config path must be a real directory, not a symlink: ${config_path}" >&2
    exit 1
  fi
  case "$(realpath -m -- "${config_path}")" in
    /data/config|/data/config/*) ;;
    *)
      echo "[ERROR] SLAM config path resolves outside /data/config: ${config_path}" >&2
      exit 1
      ;;
  esac
done

# Create only missing path components. Never run install -d through a path
# before proving that an existing component is a real directory.
for config_parent in /data/config /data/config/slam; do
  if [[ ! -e "${config_parent}" ]]; then
    sudo install -d -o root -g root -m 0755 "${config_parent}"
  fi
  if [[ ! -d "${config_parent}" || -L "${config_parent}" || \
        "$(stat -c '%U:%G %a' "${config_parent}" 2>/dev/null || true)" != \
          "root:root 755" ]]; then
    echo "[ERROR] SLAM config parent must be a real root:root 0755 directory: ${config_parent}" >&2
    exit 1
  fi
done
if [[ ! -e /data/config/slam/cartographer ]]; then
  sudo install -d -o root -g "${SERVICE_GROUP}" -m 0750 \
    /data/config/slam/cartographer
fi
commercial_validate_slam_config_override_tree /data/config/slam/cartographer 0

for mutable_path in \
  /var/lib/doraemon \
  /var/lib/doraemon/orbbec-captures \
  /var/lib/doraemon/ros \
  /var/log/doraemon \
  /var/log/doraemon/orbbec \
  /var/log/doraemon/startup \
  /var/log/doraemon/slam-runtime \
  /data/coverage \
  /data/maps \
  /data/maps/imports; do
  assert_external_mutable_directory_safe "${mutable_path}"
  if [[ "$(stat -c '%U:%G %a' "${mutable_path}" 2>/dev/null || true)" != \
        "${SERVICE_USER}:${SERVICE_GROUP} 750" ]]; then
    echo "[ERROR] mutable runtime directory has unexpected ownership/mode: ${mutable_path}" >&2
    exit 1
  fi
done

if [[ ! -f "${RUNTIME_ENV}" ]]; then
  sudo install -m 0640 -o root -g "${SERVICE_GROUP}" \
    "${REPO_ROOT}/config/runtime.a26022.env" "${RUNTIME_ENV}"
  echo "[OK] installed initial ${RUNTIME_ENV}"
else
  echo "[KEEP] existing ${RUNTIME_ENV}"
fi
if [[ -L "${RUNTIME_ENV}" || \
      "$(stat -c '%U:%G %a' "${RUNTIME_ENV}" 2>/dev/null || true)" != "root:${SERVICE_GROUP} 640" ]]; then
  echo "[ERROR] ${RUNTIME_ENV} must be a root-owned non-symlink file, group ${SERVICE_GROUP}, mode 0640" >&2
  exit 1
fi
commercial_validate_runtime_env_file "${RUNTIME_ENV}"

sudo usermod -aG dialout,plugdev,video "${SERVICE_USER}"

unit_file="$(mktemp)"
trap 'rm -f "${unit_file}"' EXIT
cat >"${unit_file}" <<EOF
[Unit]
Description=Doraemon Robot Runtime
After=local-fs.target network-online.target systemd-udev-settle.service ${ORBBEC_USB_SERVICE_NAME}
Wants=network-online.target systemd-udev-settle.service
Requires=${ORBBEC_USB_SERVICE_NAME}

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_GROUP}
WorkingDirectory=${REPO_ROOT}
EnvironmentFile=-${RUNTIME_ENV}
EnvironmentFile=-${DEPS_ENV}
Environment=HOME=${SERVICE_HOME}
Environment=TERM=xterm-256color
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
Environment=DORAEMON_REPO_ROOT=${REPO_ROOT}
Environment=DORAEMON_RUNTIME_CONFIG_FILE=${RUNTIME_ENV}
Environment=DORAEMON_WORKSPACE_SETUP=${WORKSPACE_SETUP}
Environment=DORAEMON_ROS_SETUP=/opt/ros/noetic/setup.bash
Environment=ROS_HOME=/var/lib/doraemon/ros
Environment=ROS_MASTER_URI=http://127.0.0.1:11311
Environment=LOG_DIR=/var/log/doraemon/startup
Environment=ALLOW_NO_ACTIVE_MAP_STARTUP=1
Environment=RUN_BACKEND_RUNTIME_SMOKE=0
Environment=RUN_REVISION_DB_HEALTH_CHECK=0
Environment=RUN_BACKEND_PRODUCTION_ACCEPTANCE=0
Environment=DORAEMON_BOOT_WAIT_TIMEOUT=120
UnsetEnvironment=ROS_IP ROS_HOSTNAME LD_LIBRARY_PATH LD_PRELOAD LD_AUDIT LD_ORIGIN_PATH LIBRARY_PATH BASH_ENV ENV CDPATH GLOBIGNORE IFS PS4 TMPDIR TMP TEMP TMUX_TMPDIR
ExecStartPre=${REPO_ROOT}/scripts/wait_robot_boot_ready.sh
ExecStart=${REPO_ROOT}/scripts/start_runtime.sh
ExecStop=${REPO_ROOT}/scripts/stop_all_backend.sh
ExecStopPost=${REPO_ROOT}/scripts/cleanup_failed_runtime_service.sh
RemainAfterExit=yes
TimeoutStartSec=240
TimeoutStopSec=90
Restart=no
KillMode=mixed
UMask=0027
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

sudo install -m 0644 "${unit_file}" "${UNIT_PATH}"
sudo install -m 0644 \
  "${REPO_ROOT}/deploy/systemd/${ORBBEC_USB_SERVICE_NAME}" \
  "${ORBBEC_USB_UNIT_PATH}"
sudo systemctl daemon-reload

sudo systemctl disable "${SERVICE_NAME}"
sudo systemctl reset-failed "${SERVICE_NAME}" || true
if [[ "$(systemctl is-enabled "${SERVICE_NAME}" 2>/dev/null || true)" != "disabled" ]]; then
  echo "[ERROR] ${SERVICE_NAME} did not remain disabled after installation" >&2
  exit 1
fi
if [[ "$(systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true)" != "inactive" ]]; then
  echo "[ERROR] ${SERVICE_NAME} did not remain inactive after installation" >&2
  exit 1
fi
echo "[OK] installed ${SERVICE_NAME}; service is disabled and inactive"
echo "[INFO] service was not started"
echo "[INFO] review ${RUNTIME_ENV}, udev aliases, network addresses, and E-stop before starting"
echo "[INFO] start with: sudo systemctl start ${SERVICE_NAME}"
echo "[INFO] after acceptance, enable with: sudo systemctl enable ${SERVICE_NAME}"
