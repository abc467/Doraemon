#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
DEPS_ENV="/etc/doraemon/deps.env"
VERSIONS_FILE="${REPO_ROOT}/deploy/manifests/x86_ubuntu20_versions.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_vehicle_identity.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_filesystem_security.sh"
# shellcheck disable=SC1090
source "${VERSIONS_FILE}"
CPU_COUNT="$(nproc)"
DEFAULT_JOBS="${CPU_COUNT}"
if (( DEFAULT_JOBS > 4 )); then
  DEFAULT_JOBS=4
fi
JOBS="${DORAEMON_BUILD_JOBS:-${DEFAULT_JOBS}}"

if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "[ERROR] this build entry supports x86_64 only" >&2
  exit 1
fi

if [[ "${REPO_ROOT}" != "/opt/doraemon/releases/${DORAEMON_BACKEND_DEPLOYMENT_TAG}" ]]; then
  echo "[ERROR] commercial workspace must be built in the exact physical release directory" >&2
  exit 1
fi
if ! commercial_verify_release_git_identity "${REPO_ROOT}" \
    "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
    "${DORAEMON_BACKEND_GIT_URL}"; then
  exit 1
fi
BUILD_GIT_COMMIT="$(commercial_release_git_readonly "${REPO_ROOT}" head-commit)"
BUILD_SOURCE_ATTESTATION=remote
if ! commercial_verify_remote_deployment_tag \
    "${DORAEMON_BACKEND_GIT_URL}" \
    "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
    "${BUILD_GIT_COMMIT}"; then
  TRIAL_ROBOT_ID=""
  if [[ -f /etc/doraemon/runtime.env ]] && \
      commercial_validate_runtime_env_file /etc/doraemon/runtime.env; then
    TRIAL_ROBOT_ID="$(sed -n 's/^ROBOT_ID=//p' /etc/doraemon/runtime.env | head -n1)"
  fi
  commercial_validate_trial_release_exception \
    /etc/doraemon/trial-release-exception.env \
    "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
    "${BUILD_GIT_COMMIT}" \
    "${DORAEMON_BACKEND_GIT_URL}" \
    "${TRIAL_ROBOT_ID}" || exit 1
  BUILD_SOURCE_ATTESTATION=trial-exception
  echo "[WARN] using the approved, expiring per-vehicle unpublished-tag trial exception"
fi
for stale_path in \
  build devel install .catkin_tools .catkin_workspace log logs Log; do
  if [[ -e "${REPO_ROOT}/${stale_path}" || -L "${REPO_ROOT}/${stale_path}" ]]; then
    echo "[ERROR] fresh commercial build refuses pre-existing state: ${REPO_ROOT}/${stale_path}" >&2
    echo "[ERROR] create a new shallow clone from the exact tag; do not reuse or copy build artifacts" >&2
    exit 1
  fi
done

if [[ ! -f /opt/ros/noetic/setup.bash ]]; then
  echo "[ERROR] ROS Noetic is not installed" >&2
  exit 1
fi

if [[ -n "${DORAEMON_DEPS_ENV:-}" && "${DORAEMON_DEPS_ENV}" != "${DEPS_ENV}" ]]; then
  echo "[ERROR] commercial build dependency environment is fixed at ${DEPS_ENV}" >&2
  exit 1
fi
if [[ ! -f "${DEPS_ENV}" || -L "${DEPS_ENV}" || \
      "$(stat -c '%U:%G %a' "${DEPS_ENV}" 2>/dev/null || true)" != "root:root 644" ]]; then
  echo "[ERROR] dependency environment is missing: ${DEPS_ENV}" >&2
  echo "[INFO] run scripts/install_x86_ubuntu20_dependencies.sh first" >&2
  exit 1
fi
commercial_validate_dependencies_env_file "${DEPS_ENV}"

# Prevent a nominally successful build/test run from silently skipping the
# production geometry suite when Shapely is absent or shadowed by user/pip
# content. This mode is read-only and validates the exact focal package.
env -u DORAEMON_DEPS_ROOT -u DORAEMON_DEPS_BUILD_ROOT -u DORAEMON_APT_ROOT \
  -u DORAEMON_ROS1_KEYRING -u DORAEMON_DEPENDENCY_TEST_MODE \
  "${SCRIPT_DIR}/install_x86_ubuntu20_dependencies.sh" \
  --verify-shapely-only

set -a
# shellcheck disable=SC1090
source "${DEPS_ENV}"
set +a

if [[ "$(realpath -e -- "${DORAEMON_CMAKE_BIN}")" != \
      "$(realpath -e -- "$(command -v cmake)")" || \
      "$(realpath -e -- "${CC}")" != "$(realpath -e -- /usr/bin/gcc-10)" || \
      "$(realpath -e -- "${CXX}")" != "$(realpath -e -- /usr/bin/g++-10)" ]]; then
  echo "[ERROR] commercial build toolchain is not the pinned CMake/GCC/G++ baseline" >&2
  exit 1
fi

# Cartographer must use Ubuntu's Protobuf 3.6.1. The complete dependency
# environment also contains OR-Tools/Fields2Cover CMake prefixes, whose bundled
# Protobuf 25.3 is ABI-incompatible with this workspace. Keep those products
# available through their dedicated variables/PYTHONPATH, but expose only the
# two prefixes required by this catkin build.
: "${ABSEIL_ROOT:?ABSEIL_ROOT is missing from ${DEPS_ENV}}"
: "${FLIRT_ROOT:?FLIRT_ROOT is missing from ${DEPS_ENV}}"
: "${absl_DIR:?absl_DIR is missing from ${DEPS_ENV}}"
export CMAKE_PREFIX_PATH="${ABSEIL_ROOT}:${FLIRT_ROOT}"

set +u
# shellcheck disable=SC1091
source /opt/ros/noetic/setup.bash
set -u

cd "${REPO_ROOT}"

catkin init

catkin config \
  --extend /opt/ros/noetic \
  --cmake-args \
    -DCMAKE_BUILD_TYPE=Release \
    "-DCMAKE_C_COMPILER=${CC}" \
    "-DCMAKE_CXX_COMPILER=${CXX}" \
    "-Dabsl_DIR=${absl_DIR}" \
    "-DFLIRT_ROOT=${FLIRT_ROOT}"

catkin build --no-status -j"${JOBS}"

BUILD_PROVENANCE_MARKER="${REPO_ROOT}/build/.doraemon-commercial-build.env"
if [[ -e "${BUILD_PROVENANCE_MARKER}" || -L "${BUILD_PROVENANCE_MARKER}" ]]; then
  echo "[ERROR] build unexpectedly created the reserved provenance marker" >&2
  exit 1
fi
BUILD_CACHE_COUNT="$(find "${REPO_ROOT}/build" -xdev -type f -name CMakeCache.txt | wc -l)"
if [[ "${BUILD_CACHE_COUNT}" -eq 0 ]]; then
  echo "[ERROR] build produced no CMake cache provenance" >&2
  exit 1
fi
{
  printf 'DORAEMON_BUILD_PROVENANCE_VERSION=1\n'
  printf 'DORAEMON_BUILD_REPO_ROOT=%s\n' "${REPO_ROOT}"
  printf 'DORAEMON_BUILD_GIT_TAG=%s\n' "${DORAEMON_BACKEND_DEPLOYMENT_TAG}"
  printf 'DORAEMON_BUILD_GIT_COMMIT=%s\n' "${BUILD_GIT_COMMIT}"
  printf 'DORAEMON_BUILD_GIT_TREE=%s\n' \
    "$(commercial_release_git_readonly "${REPO_ROOT}" head-tree)"
  printf 'DORAEMON_BUILD_HOSTNAME=%s\n' "$(hostname)"
  printf 'DORAEMON_BUILD_MACHINE_ID_SHA256=%s\n' "$(sha256sum /etc/machine-id | awk '{print $1}')"
  printf 'DORAEMON_BUILD_CMAKE_BIN=%s\n' "$(realpath -e -- "${DORAEMON_CMAKE_BIN}")"
  printf 'DORAEMON_BUILD_CC=%s\n' "$(realpath -e -- "${CC}")"
  printf 'DORAEMON_BUILD_CXX=%s\n' "$(realpath -e -- "${CXX}")"
  printf 'DORAEMON_BUILD_CACHE_COUNT=%s\n' "${BUILD_CACHE_COUNT}"
  printf 'DORAEMON_BUILD_FINISHED_UTC=%s\n' "$(date -u +%Y%m%dT%H%M%SZ)"
  printf 'DORAEMON_BUILD_SOURCE_ATTESTATION=%s\n' "${BUILD_SOURCE_ATTESTATION}"
} >"${BUILD_PROVENANCE_MARKER}"
chmod 0644 "${BUILD_PROVENANCE_MARKER}"

commercial_validate_workspace_build_provenance \
  "${REPO_ROOT}" \
  "${DORAEMON_BACKEND_DEPLOYMENT_TAG}" \
  "${DORAEMON_CMAKE_BIN}" \
  "${CC}" \
  "${CXX}" \
  "${DORAEMON_BACKEND_GIT_URL}"

# shellcheck disable=SC1091
set +u
source "${REPO_ROOT}/devel/setup.bash"
set -u
python3 "${REPO_ROOT}/scripts/verify_rosbridge_loopback_patch.py"
python3 -c "import fields2cover; print('Fields2Cover runtime:', fields2cover.__file__)"
env -u DORAEMON_DEPS_ROOT -u DORAEMON_DEPS_BUILD_ROOT -u DORAEMON_APT_ROOT \
  -u DORAEMON_ROS1_KEYRING -u DORAEMON_DEPENDENCY_TEST_MODE \
  "${SCRIPT_DIR}/install_x86_ubuntu20_dependencies.sh" \
  --verify-shapely-only
rospack find coverage_planner
rospack find coverage_task_manager
rospack find robot_hw_bridge

ROSBRIDGE_OVERLAY="$(rospack find rosbridge_server)"
if [[ "$(realpath "${ROSBRIDGE_OVERLAY}")" != "$(realpath "${REPO_ROOT}/src/rosbridge_server")" ]]; then
  echo "[ERROR] rosbridge_server did not resolve to the release overlay: ${ROSBRIDGE_OVERLAY}" >&2
  exit 1
fi
echo "rosbridge_server overlay: ${ROSBRIDGE_OVERLAY}"

echo "[OK] Doraemon workspace build completed"
