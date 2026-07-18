#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEPS_ENV="${DORAEMON_DEPS_ENV:-/etc/doraemon/deps.env}"
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

if [[ ! -f /opt/ros/noetic/setup.bash ]]; then
  echo "[ERROR] ROS Noetic is not installed" >&2
  exit 1
fi

if [[ ! -f "${DEPS_ENV}" ]]; then
  echo "[ERROR] dependency environment is missing: ${DEPS_ENV}" >&2
  echo "[INFO] run scripts/install_x86_ubuntu20_dependencies.sh first" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${DEPS_ENV}"
set +a
set +u
# shellcheck disable=SC1091
source /opt/ros/noetic/setup.bash
set -u

cd "${REPO_ROOT}"

if [[ ! -f .catkin_tools/profiles/default/config.yaml ]]; then
  catkin init
fi

catkin config \
  --extend /opt/ros/noetic \
  --cmake-args \
    -DCMAKE_BUILD_TYPE=Release \
    "-Dabsl_DIR=${absl_DIR}" \
    "-DFLIRT_ROOT=${FLIRT_ROOT}"

catkin build --no-status -j"${JOBS}"

# shellcheck disable=SC1091
set +u
source "${REPO_ROOT}/devel/setup.bash"
set -u
python3 -c "import fields2cover; print('Fields2Cover runtime:', fields2cover.__file__)"
rospack find coverage_planner
rospack find coverage_task_manager
rospack find robot_hw_bridge

echo "[OK] Doraemon workspace build completed"
