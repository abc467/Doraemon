#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERSIONS_FILE="${REPO_ROOT}/deploy/manifests/x86_ubuntu20_versions.env"

if [[ ! -f "${VERSIONS_FILE}" ]]; then
  echo "[ERROR] missing dependency manifest: ${VERSIONS_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${VERSIONS_FILE}"

DEPS_ROOT="${DORAEMON_DEPS_ROOT:-/opt/doraemon/deps}"
BUILD_ROOT="${DORAEMON_DEPS_BUILD_ROOT:-/var/tmp/doraemon-deps-build}"
CPU_COUNT="$(nproc)"
DEFAULT_JOBS="${CPU_COUNT}"
if (( DEFAULT_JOBS > 4 )); then
  DEFAULT_JOBS=4
fi
JOBS="${DORAEMON_BUILD_JOBS:-${DEFAULT_JOBS}}"
SKIP_APT=0

usage() {
  cat <<'EOF'
Usage: install_x86_ubuntu20_dependencies.sh [--skip-apt]

Builds the pinned Doraemon native dependencies and installs them below
/opt/doraemon/deps. Run this script as the normal deployment user; it invokes
sudo only for package installation and writes under /opt and /etc.
EOF
}

for arg in "$@"; do
  case "${arg}" in
    --skip-apt)
      SKIP_APT=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: ${arg}" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${EUID}" -eq 0 ]]; then
  echo "[ERROR] run this script as the normal deployment user, not as root" >&2
  exit 1
fi

if [[ "$(uname -m)" != "${DORAEMON_TARGET_ARCH}" ]]; then
  echo "[ERROR] expected ${DORAEMON_TARGET_ARCH}, got $(uname -m)" >&2
  exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "${DORAEMON_TARGET_UBUNTU}" ]]; then
  echo "[ERROR] expected Ubuntu ${DORAEMON_TARGET_UBUNTU}, got ${PRETTY_NAME:-unknown}" >&2
  exit 1
fi

sudo -v

if [[ "${SKIP_APT}" -eq 0 ]]; then
  sudo install -d -m 0755 /usr/share/keyrings
  curl -fsSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key |
    sudo tee /usr/share/keyrings/ros-archive-keyring.gpg >/dev/null
  echo "deb [arch=amd64 signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros/ubuntu focal main" |
    sudo tee /etc/apt/sources.list.d/ros1.list >/dev/null

  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates curl git gnupg lsb-release \
    build-essential cmake ninja-build pkg-config \
    python3 python3-dev python3-pip python3-empy python3-numpy \
    python3-catkin-tools python3-vcstool python3-rosdep \
    libboost-all-dev libceres-dev libsuitesparse-dev libeigen3-dev \
    liblua5.2-dev libgoogle-glog-dev libgflags-dev \
    libprotobuf-dev protobuf-compiler libcairo2-dev libyaml-cpp-dev \
    libgdal-dev libgeos-dev libtinyxml2-dev libtbb-dev swig \
    libudev-dev libdw-dev libusb-1.0-0-dev patchelf tmux \
    ros-noetic-desktop ros-noetic-rosbridge-server \
    ros-noetic-pcl-ros ros-noetic-tf2-sensor-msgs \
    ros-noetic-costmap-2d ros-noetic-base-local-planner \
    ros-noetic-camera-info-manager ros-noetic-image-geometry \
    ros-noetic-image-transport ros-noetic-nodelet \
    ros-noetic-diagnostic-updater ros-noetic-move-base-msgs \
    ros-noetic-move-base-flex ros-noetic-robot-localization

  if [[ ! -f /etc/ros/rosdep/sources.list.d/20-default.list ]]; then
    sudo rosdep init
  fi
  rosdep update --rosdistro=noetic
  rosdep install \
    --from-paths "${REPO_ROOT}/src" \
    --ignore-src \
    --rosdistro=noetic \
    -y
fi

sudo install -d -m 0755 "${DEPS_ROOT}"
install -d -m 0755 "${BUILD_ROOT}/src" "${BUILD_ROOT}/build"

ABSEIL_PREFIX="${DEPS_ROOT}/abseil-${ABSEIL_VERSION}"
ORTOOLS_PREFIX="${DEPS_ROOT}/ortools-${ORTOOLS_VERSION}"
FIELDS2COVER_PREFIX="${DEPS_ROOT}/fields2cover-${FIELDS2COVER_VERSION}"
FLIRT_PREFIX="${DEPS_ROOT}/flirt-${FLIRT_VERSION}"

prepare_source() {
  local name="$1"
  local url="$2"
  local commit="$3"
  local source_dir="${BUILD_ROOT}/src/${name}"

  if [[ ! -d "${source_dir}/.git" ]]; then
    install -d -m 0755 "${source_dir}"
    git -C "${source_dir}" init -q
    git -C "${source_dir}" remote add origin "${url}"
  else
    git -C "${source_dir}" remote set-url origin "${url}"
  fi

  git -C "${source_dir}" fetch -q --depth 1 origin "${commit}"
  git -C "${source_dir}" checkout -q --detach FETCH_HEAD

  if [[ "$(git -C "${source_dir}" rev-parse HEAD)" != "${commit}" ]]; then
    echo "[ERROR] ${name} commit verification failed" >&2
    exit 1
  fi

  printf '%s\n' "${source_dir}"
}

build_abseil() {
  local source_dir
  local build_dir="${BUILD_ROOT}/build/abseil-${ABSEIL_VERSION}"
  source_dir="$(prepare_source abseil-cpp https://github.com/abseil/abseil-cpp.git "${ABSEIL_COMMIT}")"

  cmake -S "${source_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_INSTALL_PREFIX="${ABSEIL_PREFIX}" \
    -DABSL_BUILD_TESTING=OFF
  cmake --build "${build_dir}" --parallel "${JOBS}"
  sudo cmake --install "${build_dir}"
}

build_ortools() {
  local source_dir
  local build_dir="${BUILD_ROOT}/build/ortools-${ORTOOLS_VERSION}"
  source_dir="$(prepare_source or-tools https://github.com/google/or-tools.git "${ORTOOLS_COMMIT}")"

  cmake -S "${source_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${ORTOOLS_PREFIX}" \
    -DBUILD_DEPS=ON \
    -DBUILD_CXX=ON \
    -DBUILD_PYTHON=OFF \
    -DBUILD_SAMPLES=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_TESTING=OFF
  cmake --build "${build_dir}" --parallel "${JOBS}"
  sudo cmake --install "${build_dir}"
}

build_fields2cover() {
  local source_dir
  local build_dir="${BUILD_ROOT}/build/fields2cover-${FIELDS2COVER_VERSION}"
  source_dir="$(prepare_source Fields2Cover https://github.com/Fields2Cover/Fields2Cover.git "${FIELDS2COVER_COMMIT}")"

  cmake -S "${source_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${FIELDS2COVER_PREFIX}" \
    -DCMAKE_PREFIX_PATH="${ORTOOLS_PREFIX}" \
    -DBUILD_PYTHON=ON \
    -DBUILD_TUTORIALS=OFF \
    -DBUILD_DOC=OFF \
    -DBUILD_TESTING=OFF
  cmake --build "${build_dir}" --parallel "${JOBS}"
  sudo cmake --install "${build_dir}"
}

build_flirt() {
  local build_dir="${BUILD_ROOT}/build/flirt-${FLIRT_VERSION}"

  cmake -S "${REPO_ROOT}/third_party/flirt" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${FLIRT_PREFIX}"
  cmake --build "${build_dir}" --parallel "${JOBS}"
  sudo cmake --install "${build_dir}"
}

build_abseil
build_ortools
build_fields2cover
build_flirt

PYTHON_SITE_DIR="$(
  find "${FIELDS2COVER_PREFIX}" -type f -name fields2cover.py -printf '%h\n' |
    head -n 1
)"
if [[ -z "${PYTHON_SITE_DIR}" ]]; then
  echo "[ERROR] Fields2Cover Python module was not installed below ${FIELDS2COVER_PREFIX}" >&2
  exit 1
fi

deps_ld_conf="$(mktemp)"
deps_env="$(mktemp)"
deps_profile="$(mktemp)"
trap 'rm -f "${deps_ld_conf}" "${deps_env}" "${deps_profile}"' EXIT

cat >"${deps_ld_conf}" <<EOF
${ABSEIL_PREFIX}/lib
${ABSEIL_PREFIX}/lib64
${ORTOOLS_PREFIX}/lib
${ORTOOLS_PREFIX}/lib64
${FIELDS2COVER_PREFIX}/lib
${FIELDS2COVER_PREFIX}/lib64
${FLIRT_PREFIX}/lib
${FLIRT_PREFIX}/lib64
EOF
sudo install -m 0644 "${deps_ld_conf}" /etc/ld.so.conf.d/doraemon-deps.conf
sudo ldconfig

cat >"${deps_env}" <<EOF
DORAEMON_DEPS_ROOT=${DEPS_ROOT}
ABSEIL_ROOT=${ABSEIL_PREFIX}
ORTOOLS_ROOT=${ORTOOLS_PREFIX}
FIELDS2COVER_ROOT=${FIELDS2COVER_PREFIX}
FLIRT_ROOT=${FLIRT_PREFIX}
absl_DIR=${ABSEIL_PREFIX}/lib/cmake/absl
CMAKE_PREFIX_PATH=${ABSEIL_PREFIX}:${ORTOOLS_PREFIX}:${FIELDS2COVER_PREFIX}:${FLIRT_PREFIX}
PYTHONPATH=${PYTHON_SITE_DIR}
EOF
sudo install -d -m 0755 /etc/doraemon
sudo install -m 0644 "${deps_env}" /etc/doraemon/deps.env

cat >"${deps_profile}" <<EOF
export DORAEMON_DEPS_ROOT="${DEPS_ROOT}"
export ABSEIL_ROOT="${ABSEIL_PREFIX}"
export ORTOOLS_ROOT="${ORTOOLS_PREFIX}"
export FIELDS2COVER_ROOT="${FIELDS2COVER_PREFIX}"
export FLIRT_ROOT="${FLIRT_PREFIX}"
export absl_DIR="${ABSEIL_PREFIX}/lib/cmake/absl"
export CMAKE_PREFIX_PATH="${ABSEIL_PREFIX}:${ORTOOLS_PREFIX}:${FIELDS2COVER_PREFIX}:${FLIRT_PREFIX}\${CMAKE_PREFIX_PATH:+:\${CMAKE_PREFIX_PATH}}"
export PYTHONPATH="${PYTHON_SITE_DIR}\${PYTHONPATH:+:\${PYTHONPATH}}"
EOF
sudo install -m 0644 "${deps_profile}" /etc/profile.d/doraemon-deps.sh

env PYTHONPATH="${PYTHON_SITE_DIR}" python3 -c \
  "import fields2cover; print('Fields2Cover OK:', fields2cover.__file__)"

if ldd "${FIELDS2COVER_PREFIX}/lib/libFields2Cover.so" | grep -q "not found"; then
  echo "[ERROR] Fields2Cover has unresolved shared-library dependencies" >&2
  ldd "${FIELDS2COVER_PREFIX}/lib/libFields2Cover.so" >&2
  exit 1
fi

echo "[OK] Doraemon native dependencies installed under ${DEPS_ROOT}"
echo "[OK] runtime environment written to /etc/doraemon/deps.env"
echo "[INFO] build cache remains at ${BUILD_ROOT} and may be removed after acceptance"
