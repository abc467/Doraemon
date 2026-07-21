#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERSIONS_FILE="${REPO_ROOT}/deploy/manifests/x86_ubuntu20_versions.env"
DEPS_ENV_TEMPLATE="${REPO_ROOT}/config/deps.x86_ubuntu20.env"
DEPS_PROFILE_TEMPLATE="${REPO_ROOT}/config/doraemon-deps.profile.sh"
DEPS_LD_CONF_TEMPLATE="${REPO_ROOT}/config/doraemon-deps.ld.so.conf"

if [[ ! -f "${VERSIONS_FILE}" ]]; then
  echo "[ERROR] missing dependency manifest: ${VERSIONS_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${VERSIONS_FILE}"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_vehicle_identity.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/commercial_filesystem_security.sh"

DEPS_ROOT="${DORAEMON_DEPS_ROOT:-/opt/doraemon/deps}"
BUILD_ROOT="${DORAEMON_DEPS_BUILD_ROOT:-/var/tmp/doraemon-deps-build}"
CMAKE_PREFIX="${DEPS_ROOT}/cmake-${CMAKE_VERSION}"
CMAKE_BIN="${CMAKE_PREFIX}/bin/cmake"
CTEST_BIN="${CMAKE_PREFIX}/bin/ctest"
ABSEIL_PREFIX="${DEPS_ROOT}/abseil-${ABSEIL_VERSION}"
ORTOOLS_PREFIX="${DEPS_ROOT}/ortools-${ORTOOLS_VERSION}"
FIELDS2COVER_PREFIX="${DEPS_ROOT}/fields2cover-${FIELDS2COVER_VERSION}"
FLIRT_PREFIX="${DEPS_ROOT}/flirt-${FLIRT_VERSION}"
CMAKE_ARCHIVE_NAME="cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz"
CMAKE_ARCHIVE_ROOT="cmake-${CMAKE_VERSION}-linux-x86_64"
CC_BIN="/usr/bin/gcc-${GCC_TOOLCHAIN_MAJOR}"
CXX_BIN="/usr/bin/g++-${GCC_TOOLCHAIN_MAJOR}"
APT_ROOT="${DORAEMON_APT_ROOT:-/etc/apt}"
APT_SOURCES_DIR="${APT_ROOT}/sources.list.d"
ROS1_APT_LIST="${APT_SOURCES_DIR}/ros1.list"
ROS1_KEYRING="${DORAEMON_ROS1_KEYRING:-/usr/share/keyrings/ros-archive-keyring.gpg}"
ROS1_APT_LINE="deb [arch=amd64 signed-by=${ROS1_KEYRING}] ${ROS1_APT_REPOSITORY_URL} ${ROS1_APT_SUITE} ${ROS1_APT_COMPONENT}"
CPU_COUNT="$(nproc)"
DEFAULT_JOBS="${CPU_COUNT}"
if (( DEFAULT_JOBS > 4 )); then
  DEFAULT_JOBS=4
fi
JOBS="${DORAEMON_BUILD_JOBS:-${DEFAULT_JOBS}}"
SKIP_APT=0
VERIFY_CMAKE_ONLY=0
VERIFY_TOOLCHAIN_ONLY=0
VERIFY_SHAPELY_ONLY=0
VERIFY_ROS1_APT_ONLY=0
CONFIGURE_ROS1_APT_ONLY=0
VERIFY_DEPENDENCY_CACHE_DIR=""
CMAKE_DOWNLOAD_TMP=""
CMAKE_STAGE_DIR=""
TEMP_FILES=()

cleanup() {
  local path

  if [[ -n "${CMAKE_DOWNLOAD_TMP}" ]]; then
    rm -f -- "${CMAKE_DOWNLOAD_TMP}"
  fi
  for path in "${TEMP_FILES[@]}"; do
    rm -f -- "${path}"
  done
  if [[ -n "${CMAKE_STAGE_DIR}" && -d "${CMAKE_STAGE_DIR}" ]]; then
    case "${CMAKE_STAGE_DIR}" in
      "${DEPS_ROOT}/.cmake-${CMAKE_VERSION}.stage."*)
        if ! sudo -n rm -rf --one-file-system -- "${CMAKE_STAGE_DIR}"; then
          echo "[WARN] could not remove incomplete CMake staging directory: ${CMAKE_STAGE_DIR}" >&2
        fi
        ;;
      *)
        echo "[WARN] refusing to clean unexpected CMake staging path: ${CMAKE_STAGE_DIR}" >&2
        ;;
    esac
  fi
}

trap cleanup EXIT
trap 'exit 130' HUP INT TERM

usage() {
  cat <<'EOF'
Usage: install_x86_ubuntu20_dependencies.sh [--skip-apt] [--verify-cmake-only]
                                               [--verify-toolchain-only]
                                               [--verify-shapely-only]
                                               [--verify-ros1-apt-only]
                                               [--configure-ros1-apt-only]
                                               [--verify-dependency-cache-only=DIR]

Builds the pinned Doraemon native dependencies and installs them below
/opt/doraemon/deps. Run this script as the normal deployment user; it invokes
sudo only for package installation and writes under /opt and /etc.

  --verify-cmake-only  Read-only verification of the pinned CMake prefix.
  --verify-toolchain-only
                       Read-only verification of pinned CMake and GCC/G++.
  --verify-shapely-only
                       Read-only verification of the exact Ubuntu Shapely
                       package, import path, version, and geometry runtime.
  --verify-ros1-apt-only
                       Read-only verification of the exact USTC ROS1 source
                       and the pinned official ROS signing key.
  --configure-ros1-apt-only
                       Install/verify the pinned ROS key and migrate only ROS1
                       apt entries to the exact managed USTC source. ROS2 and
                       other apt entries are preserved; packages are not built.
  --verify-dependency-cache-only=DIR
                       Also verify DIR/CMakeCache.txt uses that toolchain.
EOF
}

for arg in "$@"; do
  case "${arg}" in
    --skip-apt)
      SKIP_APT=1
      ;;
    --verify-cmake-only)
      VERIFY_CMAKE_ONLY=1
      ;;
    --verify-toolchain-only)
      VERIFY_TOOLCHAIN_ONLY=1
      ;;
    --verify-shapely-only)
      VERIFY_SHAPELY_ONLY=1
      ;;
    --verify-ros1-apt-only)
      VERIFY_ROS1_APT_ONLY=1
      ;;
    --configure-ros1-apt-only)
      CONFIGURE_ROS1_APT_ONLY=1
      ;;
    --verify-dependency-cache-only=*)
      VERIFY_DEPENDENCY_CACHE_DIR="${arg#*=}"
      if [[ -z "${VERIFY_DEPENDENCY_CACHE_DIR}" ]]; then
        echo "[ERROR] --verify-dependency-cache-only requires a directory" >&2
        exit 1
      fi
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

VERIFY_MODE_COUNT=$((
  VERIFY_CMAKE_ONLY +
  VERIFY_TOOLCHAIN_ONLY +
  VERIFY_SHAPELY_ONLY +
  VERIFY_ROS1_APT_ONLY +
  CONFIGURE_ROS1_APT_ONLY
))
if [[ -n "${VERIFY_DEPENDENCY_CACHE_DIR}" ]]; then
  VERIFY_MODE_COUNT=$((VERIFY_MODE_COUNT + 1))
fi
if (( VERIFY_MODE_COUNT > 1 )); then
  echo "[ERROR] read-only verification options are mutually exclusive" >&2
  exit 1
fi

PATH_OVERRIDE_REQUESTED=0
[[ -v DORAEMON_DEPS_ROOT && "${DORAEMON_DEPS_ROOT}" != "/opt/doraemon/deps" ]] && \
  PATH_OVERRIDE_REQUESTED=1
[[ -v DORAEMON_DEPS_BUILD_ROOT && \
   "${DORAEMON_DEPS_BUILD_ROOT}" != "/var/tmp/doraemon-deps-build" ]] && \
  PATH_OVERRIDE_REQUESTED=1
[[ -v DORAEMON_APT_ROOT && "${DORAEMON_APT_ROOT}" != "/etc/apt" ]] && \
  PATH_OVERRIDE_REQUESTED=1
[[ -v DORAEMON_ROS1_KEYRING && \
   "${DORAEMON_ROS1_KEYRING}" != "/usr/share/keyrings/ros-archive-keyring.gpg" ]] && \
  PATH_OVERRIDE_REQUESTED=1
if [[ "${PATH_OVERRIDE_REQUESTED}" -eq 1 ]]; then
  if [[ "${DORAEMON_DEPENDENCY_TEST_MODE:-0}" != "1" || \
        "${VERIFY_MODE_COUNT}" -ne 1 || "${CONFIGURE_ROS1_APT_ONLY}" -eq 1 ]]; then
    echo "[ERROR] commercial install paths are fixed; overrides are allowed only in explicit read-only test mode" >&2
    exit 1
  fi
elif [[ "${DEPS_ROOT}" != "/opt/doraemon/deps" || \
        "${BUILD_ROOT}" != "/var/tmp/doraemon-deps-build" || \
        "${APT_ROOT}" != "/etc/apt" || \
        "${ROS1_KEYRING}" != "/usr/share/keyrings/ros-archive-keyring.gpg" ]]; then
  echo "[ERROR] commercial dependency installer resolved a non-standard installation path" >&2
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

verify_pinned_cmake() {
  local prefix="$1"
  local cmake_bin="${prefix}/bin/cmake"
  local ctest_bin="${prefix}/bin/ctest"
  local output
  local first_line
  local actual_sha256=""
  local offender=""
  local fixed_prefix="/opt/doraemon/deps/cmake-${CMAKE_VERSION}"
  local fixed_stage_prefix="/opt/doraemon/deps/.cmake-${CMAKE_VERSION}.stage."

  if [[ -L "${prefix}" || ! -x "${cmake_bin}" || -L "${cmake_bin}" ||
    ! -x "${ctest_bin}" || -L "${ctest_bin}" ]]; then
    echo "[ERROR] CMake toolchain is incomplete below ${prefix}" >&2
    return 1
  fi

  if ! output="$("${cmake_bin}" --version 2>&1)"; then
    echo "[ERROR] failed to execute ${cmake_bin} --version" >&2
    return 1
  fi
  first_line="${output%%$'\n'*}"
  if [[ "${first_line}" != "cmake version ${CMAKE_VERSION}" ]]; then
    echo "[ERROR] expected cmake version ${CMAKE_VERSION}, got: ${first_line}" >&2
    return 1
  fi

  if ! output="$("${ctest_bin}" --version 2>&1)"; then
    echo "[ERROR] failed to execute ${ctest_bin} --version" >&2
    return 1
  fi
  first_line="${output%%$'\n'*}"
  if [[ "${first_line}" != "ctest version ${CMAKE_VERSION}" ]]; then
    echo "[ERROR] expected ctest version ${CMAKE_VERSION}, got: ${first_line}" >&2
    return 1
  fi

  if [[ "${prefix}" == "${fixed_prefix}" || "${prefix}" == "${fixed_stage_prefix}"* ]]; then
    offender="$(find "${prefix}" -xdev \( -type f -o -type d -o -type l \) \
      \( ! -user root -o ! -group root \) -print -quit 2>&1)" || {
      echo "[ERROR] failed to audit pinned CMake ownership: ${offender}" >&2
      return 1
    }
    if [[ -n "${offender}" ]]; then
      echo "[ERROR] pinned CMake tree is not entirely root-owned: ${offender}" >&2
      return 1
    fi
    offender="$(find "${prefix}" -xdev \( -type f -o -type d \) -perm /022 \
      -print -quit 2>&1)" || {
      echo "[ERROR] failed to audit pinned CMake permissions: ${offender}" >&2
      return 1
    }
    if [[ -n "${offender}" ]]; then
      echo "[ERROR] pinned CMake tree contains a group/other-writable path: ${offender}" >&2
      return 1
    fi
    if offender="$(commercial_find_mount_below "${prefix}" 0)"; then
      echo "[ERROR] pinned CMake tree contains a nested mount: ${offender}" >&2
      return 1
    fi
    if ! actual_sha256="$(LC_ALL=C tar --sort=name --mtime=@0 --owner=0 --group=0 \
        --numeric-owner --format=gnu -cf - -C "${prefix}" . | sha256sum)"; then
      echo "[ERROR] failed to calculate pinned CMake installed-tree digest" >&2
      return 1
    fi
    actual_sha256="${actual_sha256%% *}"
    if [[ "${actual_sha256}" != "${CMAKE_INSTALLED_TREE_SHA256}" ]]; then
      echo "[ERROR] pinned CMake installed-tree SHA256 mismatch" >&2
      echo "[ERROR] expected ${CMAKE_INSTALLED_TREE_SHA256}, got ${actual_sha256}" >&2
      return 1
    fi
  elif [[ "${DORAEMON_DEPENDENCY_TEST_MODE:-0}" != "1" ]]; then
    echo "[ERROR] refusing to verify CMake outside the fixed commercial prefix: ${prefix}" >&2
    return 1
  fi

  echo "[OK] pinned CMake ${CMAKE_VERSION} verified at ${prefix}"
}

verify_pinned_compilers() {
  local tool
  local expected_name
  local full_version
  local major
  local resolved

  for tool in "${CC_BIN}" "${CXX_BIN}"; do
    if [[ ! -x "${tool}" ]] || ! resolved="$(realpath -e -- "${tool}" 2>/dev/null)" ||
      [[ ! -f "${resolved}" || ! -x "${resolved}" || "${resolved}" != /usr/bin/* ]]; then
      echo "[ERROR] pinned compiler is missing or resolves outside /usr/bin: ${tool}" >&2
      return 1
    fi
    expected_name="$(basename "${tool}")"
    if ! full_version="$("${tool}" -dumpfullversion -dumpversion 2>/dev/null)"; then
      echo "[ERROR] failed to query compiler version: ${tool}" >&2
      return 1
    fi
    major="${full_version%%.*}"
    if [[ "${major}" != "${GCC_TOOLCHAIN_MAJOR}" || "${full_version}" != "${GCC_TOOLCHAIN_VERSION}" ]]; then
      echo "[ERROR] expected ${expected_name} ${GCC_TOOLCHAIN_VERSION} (major ${GCC_TOOLCHAIN_MAJOR}), got ${full_version}" >&2
      return 1
    fi
  done

  echo "[OK] pinned GCC/G++ ${GCC_TOOLCHAIN_VERSION} verified"
}

cmake_cache_value() {
  local cache_file="$1"
  local key="$2"

  awk -v key="${key}" '
    index($0, key ":") == 1 {
      sub(/^[^=]*=/, "")
      print
      exit
    }
  ' "${cache_file}"
}

verify_dependency_cmake_cache() {
  local build_dir="$1"
  local cache_file="${build_dir}/CMakeCache.txt"
  local key
  local expected
  local cached
  local cached_real
  local expected_real

  if [[ -L "${build_dir}" || ( -e "${build_dir}" && ! -d "${build_dir}" ) || \
        "$(realpath -m -- "${build_dir}")" != "${build_dir}" ]]; then
    echo "[ERROR] dependency build path must be a canonical real directory or absent: ${build_dir}" >&2
    return 1
  fi
  case "${build_dir}" in
    "${BUILD_ROOT}/build"/*) ;;
    *)
      echo "[ERROR] dependency build path is outside the fixed build root: ${build_dir}" >&2
      return 1
      ;;
  esac
  if [[ -d "${build_dir}" ]]; then
    if cached="$(commercial_find_mount_below "${build_dir}" 0)"; then
      echo "[ERROR] dependency build path contains a nested mount: ${cached}" >&2
      return 1
    fi
    if cached="$(commercial_find_symlink_outside_tree "${build_dir}")"; then
      echo "[ERROR] dependency build path contains a dangling or escaping symlink: ${cached}" >&2
      return 1
    fi
  fi

  if [[ ! -e "${cache_file}" ]]; then
    return 0
  fi
  if [[ ! -f "${cache_file}" || -L "${cache_file}" ]]; then
    echo "[ERROR] unsafe CMake cache path: ${cache_file}" >&2
    return 1
  fi

  for key in CMAKE_COMMAND CMAKE_C_COMPILER CMAKE_CXX_COMPILER; do
    case "${key}" in
      CMAKE_COMMAND) expected="${CMAKE_BIN}" ;;
      CMAKE_C_COMPILER) expected="${CC_BIN}" ;;
      CMAKE_CXX_COMPILER) expected="${CXX_BIN}" ;;
    esac
    cached="$(cmake_cache_value "${cache_file}" "${key}")"
    if [[ -z "${cached}" ]]; then
      echo "[ERROR] ${cache_file} does not pin ${key}" >&2
      echo "[ERROR] clean this dependency build directory before retrying" >&2
      return 1
    fi
    if ! cached_real="$(realpath -e -- "${cached}" 2>/dev/null)" ||
      ! expected_real="$(realpath -e -- "${expected}" 2>/dev/null)" ||
      [[ "${cached_real}" != "${expected_real}" ]]; then
      echo "[ERROR] stale ${key} in ${cache_file}: ${cached}" >&2
      echo "[ERROR] expected ${expected}; clean this dependency build directory before retrying" >&2
      return 1
    fi
  done

  echo "[OK] dependency CMake cache uses the pinned toolchain: ${build_dir}"
}

preflight_dependency_install_paths() {
  local path=""
  local actual=""
  local offender=""

  for path in /opt /opt/doraemon; do
    actual="$(stat -c '%U:%G %a' "${path}" 2>/dev/null || true)"
    if [[ ! -d "${path}" || -L "${path}" || \
          "$(realpath -e -- "${path}" 2>/dev/null || true)" != "${path}" || \
          "${actual}" != "root:root 755" ]]; then
      echo "[ERROR] dependency ancestor must be a canonical root:root 0755 directory: ${path}" >&2
      return 1
    fi
  done

  if [[ ! -e "${DEPS_ROOT}" ]]; then
    sudo install -d -o root -g root -m 0755 "${DEPS_ROOT}"
  fi
  actual="$(stat -c '%U:%G %a' "${DEPS_ROOT}" 2>/dev/null || true)"
  if [[ ! -d "${DEPS_ROOT}" || -L "${DEPS_ROOT}" || \
        "$(realpath -e -- "${DEPS_ROOT}" 2>/dev/null || true)" != "${DEPS_ROOT}" || \
        "${actual}" != "root:root 755" ]]; then
    echo "[ERROR] dependency root must be a canonical root:root 0755 directory" >&2
    return 1
  fi
  if offender="$(commercial_find_mount_below "${DEPS_ROOT}" 0)"; then
    echo "[ERROR] dependency root contains a nested mount before installation: ${offender}" >&2
    return 1
  fi
  if offender="$(commercial_find_symlink_outside_tree "${DEPS_ROOT}")"; then
    echo "[ERROR] dependency root contains a dangling or escaping symlink before installation: ${offender}" >&2
    return 1
  fi
  offender="$(find "${DEPS_ROOT}" -xdev \( -type f -o -type d -o -type l \) \
    \( ! -user root -o ! -group root \) -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit dependency ownership before installation: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] existing dependency tree is not entirely root-owned: ${offender}" >&2
    return 1
  fi
  offender="$(find "${DEPS_ROOT}" -xdev \( -type f -o -type d \) -perm /022 \
    -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit dependency permissions before installation: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] existing dependency tree is group/other-writable: ${offender}" >&2
    return 1
  fi

  for path in \
    "${CMAKE_PREFIX}" \
    "${ABSEIL_PREFIX}" \
    "${ORTOOLS_PREFIX}" \
    "${FIELDS2COVER_PREFIX}" \
    "${FLIRT_PREFIX}"; do
    if [[ -e "${path}" || -L "${path}" ]]; then
      if [[ ! -d "${path}" || -L "${path}" || \
            "$(realpath -e -- "${path}" 2>/dev/null || true)" != "${path}" ]]; then
        echo "[ERROR] existing dependency prefix is not a canonical real directory: ${path}" >&2
        return 1
      fi
    fi
  done

  actual="$(stat -c '%U:%G %a' /var/tmp 2>/dev/null || true)"
  if [[ ! -d /var/tmp || -L /var/tmp || "$(realpath -e -- /var/tmp)" != /var/tmp || \
        "${actual}" != "root:root 1777" ]]; then
    echo "[ERROR] /var/tmp must be a canonical root:root 1777 directory" >&2
    return 1
  fi
  if [[ ! -e "${BUILD_ROOT}" ]]; then
    install -d -m 0755 "${BUILD_ROOT}"
  fi
  actual="$(stat -c '%U:%G %a' "${BUILD_ROOT}" 2>/dev/null || true)"
  if [[ ! -d "${BUILD_ROOT}" || -L "${BUILD_ROOT}" || \
        "$(realpath -e -- "${BUILD_ROOT}" 2>/dev/null || true)" != "${BUILD_ROOT}" || \
        "${actual}" != "$(id -un):$(id -gn) 755" ]]; then
    echo "[ERROR] dependency build root must be a canonical deployment-user 0755 directory" >&2
    return 1
  fi
  if offender="$(commercial_find_mount_below "${BUILD_ROOT}" 0)"; then
    echo "[ERROR] dependency build root contains a nested mount: ${offender}" >&2
    return 1
  fi
  if offender="$(commercial_find_symlink_outside_tree "${BUILD_ROOT}")"; then
    echo "[ERROR] dependency build root contains a dangling or escaping symlink: ${offender}" >&2
    return 1
  fi
  for path in "${BUILD_ROOT}/src" "${BUILD_ROOT}/build"; do
    if [[ -e "${path}" || -L "${path}" ]]; then
      if [[ ! -d "${path}" || -L "${path}" || \
            "$(realpath -e -- "${path}" 2>/dev/null || true)" != "${path}" ]]; then
        echo "[ERROR] dependency build subdirectory is unsafe: ${path}" >&2
        return 1
      fi
    else
      install -d -m 0755 "${path}"
    fi
  done
}

validate_cmake_manifest() {
  local expected_url="https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/${CMAKE_ARCHIVE_NAME}"

  if [[ "${DORAEMON_TARGET_ARCH}" != "x86_64" ]]; then
    echo "[ERROR] the pinned CMake binary archive supports x86_64 only" >&2
    return 1
  fi
  if [[ "${CMAKE_LINUX_X86_64_URL}" != "${expected_url}" ]]; then
    echo "[ERROR] unexpected CMake source URL: ${CMAKE_LINUX_X86_64_URL}" >&2
    return 1
  fi
  if [[ ! "${CMAKE_LINUX_X86_64_SHA256}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[ERROR] invalid CMake SHA256 in ${VERSIONS_FILE}" >&2
    return 1
  fi
  if [[ ! "${CMAKE_INSTALLED_TREE_SHA256:-}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[ERROR] invalid installed CMake tree SHA256 in ${VERSIONS_FILE}" >&2
    return 1
  fi
}

validate_compiler_manifest() {
  if [[ ! "${GCC_TOOLCHAIN_MAJOR}" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] invalid GCC toolchain major in ${VERSIONS_FILE}" >&2
    return 1
  fi
  if [[ ! "${GCC_TOOLCHAIN_VERSION}" =~ ^${GCC_TOOLCHAIN_MAJOR}\.[0-9]+\.[0-9]+$ ]]; then
    echo "[ERROR] invalid GCC toolchain version in ${VERSIONS_FILE}" >&2
    return 1
  fi
}

validate_shapely_manifest() {
  if [[ "${SHAPELY_APT_PACKAGE:-}" != "python3-shapely" ||
        "${SHAPELY_APT_VERSION:-}" != "1.7.0-1build1" ||
        "${SHAPELY_APT_ARCH:-}" != "amd64" ||
        "${SHAPELY_PYTHON_VERSION:-}" != "1.7.0" ]]; then
    echo "[ERROR] unexpected Ubuntu 20.04 Shapely identity in ${VERSIONS_FILE}" >&2
    return 1
  fi
  if [[ "${SHAPELY_APT_DEB_SHA256:-}" != \
        "230c303ce98fb8fdb4906ec5ce3c32b6a662af032c767682b4c3bdcd2dfb2686" ]]; then
    echo "[ERROR] unexpected Ubuntu 20.04 Shapely package digest in ${VERSIONS_FILE}" >&2
    return 1
  fi
}

verify_pinned_shapely_apt_metadata() {
  local metadata=""

  validate_shapely_manifest
  if ! command -v apt-cache >/dev/null 2>&1; then
    echo "[ERROR] apt-cache is required to verify the pinned Shapely package" >&2
    return 1
  fi
  if ! metadata="$(apt-cache show \
      "${SHAPELY_APT_PACKAGE}=${SHAPELY_APT_VERSION}" 2>/dev/null)" ||
      [[ -z "${metadata}" ]]; then
    echo "[ERROR] pinned Shapely package metadata is unavailable from APT" >&2
    return 1
  fi
  if ! awk \
      -v package="${SHAPELY_APT_PACKAGE}" \
      -v version="${SHAPELY_APT_VERSION}" \
      -v architecture="${SHAPELY_APT_ARCH}" \
      -v sha256="${SHAPELY_APT_DEB_SHA256}" '
    BEGIN { RS = ""; FS = "\n"; found = 0 }
    {
      delete value
      for (i = 1; i <= NF; ++i) {
        separator = index($i, ":")
        if (separator > 1) {
          key = substr($i, 1, separator - 1)
          value[key] = substr($i, separator + 2)
        }
      }
      if (value["Package"] == package && value["Version"] == version &&
          value["Architecture"] == architecture && value["SHA256"] == sha256) {
        found = 1
      }
    }
    END { exit found ? 0 : 1 }
  ' <<<"${metadata}"; then
    echo "[ERROR] APT metadata does not match the approved Shapely package digest" >&2
    return 1
  fi
  echo "[OK] pinned APT metadata for ${SHAPELY_APT_PACKAGE}=${SHAPELY_APT_VERSION} verified"
}

verify_pinned_shapely() {
  local package_record=""
  local expected_record=""
  local module_record=""
  local module_path=""
  local package_verify=""
  local offender=""
  local package_root="/usr/lib/python3/dist-packages/shapely"

  validate_shapely_manifest
  for command in dpkg dpkg-query; do
    if ! command -v "${command}" >/dev/null 2>&1; then
      echo "[ERROR] ${command} is required to verify the pinned Shapely package" >&2
      return 1
    fi
  done
  if ! package_record="$(dpkg-query -W \
      -f='${Status}|${Version}|${Architecture}\n' \
      "${SHAPELY_APT_PACKAGE}" 2>/dev/null)"; then
    echo "[ERROR] required package is not installed: ${SHAPELY_APT_PACKAGE}" >&2
    return 1
  fi
  expected_record="install ok installed|${SHAPELY_APT_VERSION}|${SHAPELY_APT_ARCH}"
  if [[ "${package_record}" != "${expected_record}" ]]; then
    echo "[ERROR] expected ${SHAPELY_APT_PACKAGE} ${SHAPELY_APT_VERSION} ${SHAPELY_APT_ARCH}" >&2
    echo "[ERROR] dpkg reports: ${package_record}" >&2
    return 1
  fi

  if [[ ! -d "${package_root}" || -L "${package_root}" ||
        "$(realpath -e -- "${package_root}" 2>/dev/null || true)" != "${package_root}" ||
        "$(stat -c '%U:%G %a' "${package_root}" 2>/dev/null || true)" != "root:root 755" ]]; then
    echo "[ERROR] Shapely package root is not a canonical root:root 0755 directory" >&2
    return 1
  fi
  offender="$(find "${package_root}" -xdev -type l -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit the Shapely package tree: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] Shapely package tree contains a symlink: ${offender}" >&2
    return 1
  fi
  offender="$(find "${package_root}" -xdev \( -type f -o -type d \) \
    \( ! -user root -o ! -group root -o -perm /022 \) -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit Shapely ownership and permissions: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] Shapely package tree is not root-owned and non-writable: ${offender}" >&2
    return 1
  fi

  if ! package_verify="$(dpkg --verify "${SHAPELY_APT_PACKAGE}" 2>&1)"; then
    echo "[ERROR] dpkg could not verify ${SHAPELY_APT_PACKAGE}: ${package_verify}" >&2
    return 1
  fi
  if [[ -n "${package_verify}" ]]; then
    echo "[ERROR] installed Shapely package files differ from dpkg metadata" >&2
    echo "${package_verify}" >&2
    return 1
  fi

  if ! module_record="$(env -u PYTHONHOME -u PYTHONPATH \
      PYTHONNOUSERSITE=1 /usr/bin/python3 -I - \
      "${SHAPELY_PYTHON_VERSION}" "${package_root}" <<'PY'
import os
import pathlib
import sys

expected_version = sys.argv[1]
expected_root = pathlib.Path(sys.argv[2]).resolve(strict=True)

import shapely
from shapely.geometry import Polygon
from shapely.ops import unary_union

module_path = pathlib.Path(shapely.__file__).resolve(strict=True)
try:
    module_path.relative_to(expected_root)
except ValueError:
    raise SystemExit("Shapely resolved outside the Ubuntu dist-packages root")
if shapely.__version__ != expected_version:
    raise SystemExit(
        "expected Shapely %s, got %s" % (expected_version, shapely.__version__)
    )

left = Polygon(((0, 0), (1, 0), (1, 1), (0, 1)))
right = Polygon(((1, 0), (2, 0), (2, 1), (1, 1)))
merged = unary_union((left, right))
if not merged.is_valid or abs(merged.area - 2.0) > 1e-12:
    raise SystemExit("Shapely/GEOS functional geometry check failed")

print("%s|%s" % (shapely.__version__, module_path))
PY
  )"; then
    echo "[ERROR] isolated Shapely import or geometry operation failed" >&2
    return 1
  fi
  module_path="${module_record#*|}"
  if [[ "${module_record%%|*}" != "${SHAPELY_PYTHON_VERSION}" ||
        "${module_path}" != "${package_root}/__init__.py" ]]; then
    echo "[ERROR] Shapely resolved to an unexpected version or import path: ${module_record}" >&2
    return 1
  fi
  if ! dpkg-query -L "${SHAPELY_APT_PACKAGE}" | \
      awk -v expected="${module_path}" '$0 == expected { found = 1 } END { exit found ? 0 : 1 }'; then
    echo "[ERROR] imported Shapely module is not owned by ${SHAPELY_APT_PACKAGE}" >&2
    return 1
  fi

  echo "[OK] pinned Shapely ${SHAPELY_PYTHON_VERSION} verified from ${module_path}"
}

validate_ros1_manifest() {
  if [[ "${ROS1_APT_REPOSITORY_URL}" != "https://mirrors.ustc.edu.cn/ros/ubuntu" ]]; then
    echo "[ERROR] unexpected ROS1 apt repository: ${ROS1_APT_REPOSITORY_URL}" >&2
    return 1
  fi
  if [[ "${ROS1_APT_SUITE}" != "${VERSION_CODENAME:-}" || "${ROS1_APT_COMPONENT}" != "main" ]]; then
    echo "[ERROR] ROS1 apt suite/component does not match this host" >&2
    return 1
  fi
  if [[ "${ROS1_APT_KEY_URL}" != "https://raw.githubusercontent.com/ros/rosdistro/master/ros.key" ]]; then
    echo "[ERROR] unexpected ROS1 signing-key URL: ${ROS1_APT_KEY_URL}" >&2
    return 1
  fi
  if [[ ! "${ROS1_APT_KEY_SHA256}" =~ ^[0-9a-f]{64}$ ||
    ! "${ROS1_APT_KEY_FINGERPRINT}" =~ ^[0-9A-F]{40}$ ]]; then
    echo "[ERROR] invalid ROS1 signing-key identity in ${VERSIONS_FILE}" >&2
    return 1
  fi
}

is_ros1_apt_line() {
  local line="$1"
  local deb_pattern='^[[:space:]]*deb(-src)?[[:space:]]'
  local ros1_pattern='https?://[^[:space:]]*/ros/ubuntu/?([[:space:]]|$)'

  [[ "${line}" =~ ${deb_pattern} && "${line}" =~ ${ros1_pattern} ]]
}

is_target_ros1_apt_line() {
  local line="$1"
  local deb_pattern='^[[:space:]]*deb[[:space:]]'
  local url_pattern='https://mirrors\.ustc\.edu\.cn/ros/ubuntu/?([[:space:]]|$)'
  local suite_pattern="[[:space:]]${ROS1_APT_SUITE}[[:space:]]+${ROS1_APT_COMPONENT}([[:space:]]|$)"

  [[ "${line}" =~ ${deb_pattern} && "${line}" =~ ${url_pattern} && "${line}" =~ ${suite_pattern} ]]
}

is_legacy_official_ros1_apt_line() {
  local line="$1"
  local official_pattern='https?://packages\.ros\.org/ros/ubuntu/?([[:space:]]|$)'

  is_ros1_apt_line "${line}" && [[ "${line}" =~ ${official_pattern} ]]
}

validate_ros1_list_file() {
  local list_file="$1"
  local managed="$2"
  local line

  if [[ -L "${list_file}" ]]; then
    if [[ "${managed}" -eq 1 ]]; then
      echo "[ERROR] refusing symlinked managed ROS1 apt list: ${list_file}" >&2
      return 1
    fi
    while IFS= read -r line || [[ -n "${line}" ]]; do
      if is_ros1_apt_line "${line}"; then
        echo "[ERROR] refusing ROS1 source in symlinked apt list: ${list_file}" >&2
        return 1
      fi
    done <"${list_file}"
    return 0
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ -z "${line//[[:space:]]/}" || "${line}" =~ ^[[:space:]]*# ]]; then
      continue
    fi
    if [[ "${managed}" -eq 1 ]]; then
      if is_target_ros1_apt_line "${line}" || is_legacy_official_ros1_apt_line "${line}"; then
        continue
      fi
      echo "[ERROR] managed ROS1 list contains unexpected content: ${list_file}" >&2
      return 1
    fi
    if is_ros1_apt_line "${line}" && ! is_target_ros1_apt_line "${line}"; then
      echo "[ERROR] conflicting unmanaged ROS1 apt source in ${list_file}: ${line}" >&2
      return 1
    fi
  done <"${list_file}"
}

preflight_ros1_apt_sources() {
  local list_file
  local line
  local -a list_files=()
  local -a deb822_files=()

  if [[ -f "${APT_ROOT}/sources.list" ]]; then
    while IFS= read -r line || [[ -n "${line}" ]]; do
      if is_ros1_apt_line "${line}"; then
        echo "[ERROR] ROS1 apt source must not be stored in ${APT_ROOT}/sources.list" >&2
        return 1
      fi
    done <"${APT_ROOT}/sources.list"
  fi

  shopt -s nullglob
  list_files=("${APT_SOURCES_DIR}"/*.list)
  deb822_files=("${APT_SOURCES_DIR}"/*.sources)
  shopt -u nullglob
  for list_file in "${list_files[@]}"; do
    if [[ "${list_file}" == "${ROS1_APT_LIST}" ]]; then
      validate_ros1_list_file "${list_file}" 1
    else
      validate_ros1_list_file "${list_file}" 0
    fi
  done
  for list_file in "${deb822_files[@]}"; do
    if grep -Eq '^[[:space:]]*URIs:.*https?://[^[:space:]]*/ros/ubuntu/?([[:space:]]|$)' "${list_file}"; then
      echo "[ERROR] conflicting ROS1 Deb822 source is not managed automatically: ${list_file}" >&2
      return 1
    fi
  done
}

configure_ros1_apt_source() {
  local enabled="$1"
  local list_file
  local line
  local output_file
  local found_target
  local ros1_count=0
  local -a list_files=()

  validate_ros1_manifest
  sudo install -d -m 0755 "${APT_SOURCES_DIR}"
  preflight_ros1_apt_sources

  shopt -s nullglob
  list_files=("${APT_SOURCES_DIR}"/*.list)
  shopt -u nullglob
  for list_file in "${list_files[@]}"; do
    if [[ "${list_file}" == "${ROS1_APT_LIST}" ]]; then
      continue
    fi
    found_target=0
    while IFS= read -r line || [[ -n "${line}" ]]; do
      if is_target_ros1_apt_line "${line}"; then
        found_target=1
      fi
    done <"${list_file}"
    if [[ "${found_target}" -eq 0 ]]; then
      continue
    fi

    output_file="$(mktemp)"
    TEMP_FILES+=("${output_file}")
    while IFS= read -r line || [[ -n "${line}" ]]; do
      if ! is_target_ros1_apt_line "${line}"; then
        printf '%s\n' "${line}" >>"${output_file}"
      fi
    done <"${list_file}"
    sudo install -o root -g root -m 0644 "${output_file}" "${list_file}"
    echo "[INFO] migrated duplicate USTC ROS1 entry out of ${list_file}; non-ROS1 lines were preserved"
  done

  output_file="$(mktemp)"
  TEMP_FILES+=("${output_file}")
  printf '%s\n' '# Managed by Doraemon commercial deployment; do not add ROS2 here.' >"${output_file}"
  if [[ "${enabled}" -eq 1 ]]; then
    printf '%s\n' "${ROS1_APT_LINE}" >>"${output_file}"
  fi
  sudo install -o root -g root -m 0644 "${output_file}" "${ROS1_APT_LIST}"

  shopt -s nullglob
  list_files=("${APT_SOURCES_DIR}"/*.list)
  shopt -u nullglob
  for list_file in "${list_files[@]}"; do
    while IFS= read -r line || [[ -n "${line}" ]]; do
      if is_ros1_apt_line "${line}"; then
        ((ros1_count += 1))
        if [[ "${list_file}" != "${ROS1_APT_LIST}" || "${line}" != "${ROS1_APT_LINE}" ]]; then
          echo "[ERROR] ROS1 apt source uniqueness verification failed: ${list_file}" >&2
          return 1
        fi
      fi
    done <"${list_file}"
  done
  if [[ "${ros1_count}" -ne "${enabled}" ]]; then
    echo "[ERROR] expected ${enabled} active ROS1 apt source, found ${ros1_count}" >&2
    return 1
  fi
  if [[ "${enabled}" -eq 1 ]]; then
    echo "[OK] unique ROS1 apt source configured: ${ROS1_APT_LINE}"
  else
    echo "[INFO] ROS1 apt source temporarily disabled for bootstrap prerequisites"
  fi
}

verify_ros1_key_file() {
  local key_file="$1"
  local actual_sha256
  local fingerprint

  if [[ ! -f "${key_file}" || -L "${key_file}" ]]; then
    return 1
  fi
  actual_sha256="$(sha256sum "${key_file}")"
  actual_sha256="${actual_sha256%% *}"
  if [[ "${actual_sha256}" != "${ROS1_APT_KEY_SHA256}" ]]; then
    return 1
  fi
  fingerprint="$(gpg --batch --no-options --no-default-keyring --keyring /dev/null \
    --show-keys --with-colons "${key_file}" 2>/dev/null |
    awk -F: '$1 == "fpr" { print $10; exit }')"
  [[ "${fingerprint}" == "${ROS1_APT_KEY_FINGERPRINT}" ]]
}

install_official_ros1_key() {
  local key_tmp

  validate_ros1_manifest
  if verify_ros1_key_file "${ROS1_KEYRING}"; then
    echo "[INFO] reusing verified official ROS1 signing key: ${ROS1_KEYRING}"
    return 0
  fi
  if [[ -L "${ROS1_KEYRING}" ]]; then
    echo "[ERROR] refusing to replace symlinked ROS1 keyring: ${ROS1_KEYRING}" >&2
    return 1
  fi

  key_tmp="$(mktemp)"
  TEMP_FILES+=("${key_tmp}")
  if ! curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
    --retry 3 --output "${key_tmp}" "${ROS1_APT_KEY_URL}"; then
    echo "[ERROR] failed to download the official ROS1 signing key" >&2
    return 1
  fi
  if ! verify_ros1_key_file "${key_tmp}"; then
    echo "[ERROR] official ROS1 signing-key SHA256 or fingerprint verification failed" >&2
    return 1
  fi
  sudo install -d -m 0755 "$(dirname "${ROS1_KEYRING}")"
  sudo install -o root -g root -m 0644 "${key_tmp}" "${ROS1_KEYRING}"
  if ! verify_ros1_key_file "${ROS1_KEYRING}"; then
    echo "[ERROR] installed ROS1 signing-key verification failed" >&2
    return 1
  fi
  echo "[OK] official ROS1 signing key installed and verified"
}

verify_ros1_apt_state() {
  local list_file
  local line
  local owner_group
  local mode
  local ros1_count=0
  local -a list_files=()

  validate_ros1_manifest
  for command in gpg sha256sum; do
    if ! command -v "${command}" >/dev/null 2>&1; then
      echo "[ERROR] required ROS1 verification command is unavailable: ${command}" >&2
      return 1
    fi
  done
  preflight_ros1_apt_sources

  if [[ ! -f "${ROS1_APT_LIST}" || -L "${ROS1_APT_LIST}" ]]; then
    echo "[ERROR] managed ROS1 apt list is missing or unsafe: ${ROS1_APT_LIST}" >&2
    return 1
  fi
  owner_group="$(stat -c '%U:%G' "${ROS1_APT_LIST}")"
  mode="$(stat -c '%a' "${ROS1_APT_LIST}")"
  if [[ "${owner_group}" != "root:root" || "${mode}" != "644" ]]; then
    echo "[ERROR] ROS1 apt list must be root:root 0644: ${ROS1_APT_LIST}" >&2
    return 1
  fi

  shopt -s nullglob
  list_files=("${APT_SOURCES_DIR}"/*.list)
  shopt -u nullglob
  for list_file in "${list_files[@]}"; do
    while IFS= read -r line || [[ -n "${line}" ]]; do
      if is_ros1_apt_line "${line}"; then
        ((ros1_count += 1))
        if [[ "${list_file}" != "${ROS1_APT_LIST}" || "${line}" != "${ROS1_APT_LINE}" ]]; then
          echo "[ERROR] ROS1 apt source is not the exact managed USTC entry: ${list_file}" >&2
          return 1
        fi
      fi
    done <"${list_file}"
  done
  if [[ "${ros1_count}" -ne 1 ]]; then
    echo "[ERROR] expected exactly one managed ROS1 apt source, found ${ros1_count}" >&2
    return 1
  fi

  if [[ ! -f "${ROS1_KEYRING}" || -L "${ROS1_KEYRING}" ]]; then
    echo "[ERROR] official ROS1 keyring is missing or unsafe: ${ROS1_KEYRING}" >&2
    return 1
  fi
  owner_group="$(stat -c '%U:%G' "${ROS1_KEYRING}")"
  mode="$(stat -c '%a' "${ROS1_KEYRING}")"
  if [[ "${owner_group}" != "root:root" || "${mode}" != "644" ]]; then
    echo "[ERROR] ROS1 keyring must be root:root 0644: ${ROS1_KEYRING}" >&2
    return 1
  fi
  if ! verify_ros1_key_file "${ROS1_KEYRING}"; then
    echo "[ERROR] installed ROS1 key SHA256/fingerprint verification failed" >&2
    return 1
  fi
  echo "[OK] exact unique USTC ROS1 source and official signing key verified"
}

install_pinned_cmake() {
  local toolchain_root="${BUILD_ROOT}/toolchains"
  local archive="${toolchain_root}/${CMAKE_ARCHIVE_NAME}"
  local members_file
  local actual_sha256

  validate_cmake_manifest

  if [[ -e "${CMAKE_PREFIX}" ]]; then
    if [[ ! -d "${CMAKE_PREFIX}" ]]; then
      echo "[ERROR] CMake prefix exists but is not a directory: ${CMAKE_PREFIX}" >&2
      return 1
    fi
    if ! verify_pinned_cmake "${CMAKE_PREFIX}"; then
      echo "[ERROR] refusing to overwrite the existing CMake prefix" >&2
      return 1
    fi
    echo "[INFO] reusing the verified CMake prefix"
    return 0
  fi

  for command in curl sha256sum tar; do
    if ! command -v "${command}" >/dev/null 2>&1; then
      echo "[ERROR] required CMake bootstrap command is unavailable: ${command}" >&2
      return 1
    fi
  done

  install -d -m 0755 "${toolchain_root}"
  if [[ ! -f "${archive}" ]]; then
    CMAKE_DOWNLOAD_TMP="${archive}.part.$$"
    rm -f -- "${CMAKE_DOWNLOAD_TMP}"
    echo "[INFO] downloading pinned CMake from ${CMAKE_LINUX_X86_64_URL}"
    if ! curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
      --retry 3 --output "${CMAKE_DOWNLOAD_TMP}" "${CMAKE_LINUX_X86_64_URL}"; then
      echo "[ERROR] failed to download pinned CMake" >&2
      rm -f -- "${CMAKE_DOWNLOAD_TMP}"
      CMAKE_DOWNLOAD_TMP=""
      return 1
    fi
    mv -- "${CMAKE_DOWNLOAD_TMP}" "${archive}"
    CMAKE_DOWNLOAD_TMP=""
    chmod 0644 "${archive}"
  fi

  actual_sha256="$(sha256sum "${archive}")"
  actual_sha256="${actual_sha256%% *}"
  if [[ "${actual_sha256}" != "${CMAKE_LINUX_X86_64_SHA256}" ]]; then
    echo "[ERROR] CMake archive SHA256 verification failed" >&2
    echo "[ERROR] expected ${CMAKE_LINUX_X86_64_SHA256}, got ${actual_sha256}" >&2
    return 1
  fi

  members_file="$(mktemp "${toolchain_root}/.${CMAKE_ARCHIVE_NAME}.members.XXXXXX")"
  TEMP_FILES+=("${members_file}")
  if ! tar -tzf "${archive}" >"${members_file}"; then
    echo "[ERROR] failed to read the verified CMake archive" >&2
    return 1
  fi
  if ! awk -v root="${CMAKE_ARCHIVE_ROOT}" '
    $0 != root && $0 != root "/" && index($0, root "/") != 1 { invalid = 1 }
    END { exit invalid ? 1 : 0 }
  ' "${members_file}"; then
    echo "[ERROR] CMake archive contains an unexpected top-level path" >&2
    return 1
  fi
  for required_member in bin/cmake bin/ctest; do
    if ! grep -Fxq "${CMAKE_ARCHIVE_ROOT}/${required_member}" "${members_file}"; then
      echo "[ERROR] CMake archive is missing ${required_member}" >&2
      return 1
    fi
  done

  CMAKE_STAGE_DIR="${DEPS_ROOT}/.cmake-${CMAKE_VERSION}.stage.$$"
  if [[ -e "${CMAKE_STAGE_DIR}" ]]; then
    echo "[ERROR] CMake staging path already exists: ${CMAKE_STAGE_DIR}" >&2
    return 1
  fi
  sudo install -d -m 0755 "${CMAKE_STAGE_DIR}"
  if ! sudo tar -xzf "${archive}" --strip-components=1 --no-same-owner \
    -C "${CMAKE_STAGE_DIR}"; then
    echo "[ERROR] failed to extract the verified CMake archive" >&2
    return 1
  fi
  sudo chown -R root:root "${CMAKE_STAGE_DIR}"
  verify_pinned_cmake "${CMAKE_STAGE_DIR}"

  if [[ -e "${CMAKE_PREFIX}" ]]; then
    echo "[ERROR] CMake prefix appeared during installation: ${CMAKE_PREFIX}" >&2
    return 1
  fi
  sudo mv -- "${CMAKE_STAGE_DIR}" "${CMAKE_PREFIX}"
  CMAKE_STAGE_DIR=""
  verify_pinned_cmake "${CMAKE_PREFIX}"
  echo "[OK] installed pinned CMake from ${CMAKE_LINUX_X86_64_URL}"
}

validate_shapely_manifest
if [[ "${VERIFY_SHAPELY_ONLY}" -eq 1 ]]; then
  verify_pinned_shapely
  exit 0
fi
validate_cmake_manifest
if [[ "${VERIFY_CMAKE_ONLY}" -eq 1 ]]; then
  verify_pinned_cmake "${CMAKE_PREFIX}"
  exit 0
fi
validate_compiler_manifest
if [[ "${VERIFY_TOOLCHAIN_ONLY}" -eq 1 ]]; then
  verify_pinned_cmake "${CMAKE_PREFIX}"
  verify_pinned_compilers
  exit 0
fi
if [[ -n "${VERIFY_DEPENDENCY_CACHE_DIR}" ]]; then
  verify_pinned_cmake "${CMAKE_PREFIX}"
  verify_pinned_compilers
  verify_dependency_cmake_cache "${VERIFY_DEPENDENCY_CACHE_DIR}"
  exit 0
fi
validate_ros1_manifest
if [[ "${VERIFY_ROS1_APT_ONLY}" -eq 1 ]]; then
  verify_ros1_apt_state
  exit 0
fi

if [[ "${EUID}" -eq 0 ]]; then
  echo "[ERROR] run this script as the normal deployment user, not as root" >&2
  exit 1
fi

sudo -v

if [[ "${CONFIGURE_ROS1_APT_ONLY}" -eq 1 ]]; then
  install_official_ros1_key
  configure_ros1_apt_source 1
  verify_ros1_apt_state
  exit 0
fi

preflight_dependency_install_paths

if [[ "${SKIP_APT}" -eq 0 ]]; then
  # A pristine image may not have the HTTPS/key tools yet. Remove only known,
  # equivalent ROS1 entries before that bootstrap apt update; ROS2 line
  # contents in the same file are preserved unchanged.
  if ! command -v curl >/dev/null 2>&1 || ! command -v gpg >/dev/null 2>&1 ||
    [[ ! -r /etc/ssl/certs/ca-certificates.crt ]]; then
    configure_ros1_apt_source 0
    sudo apt-get update
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
      ca-certificates curl gnupg
  fi

  for command in curl gpg sha256sum; do
    if ! command -v "${command}" >/dev/null 2>&1; then
      echo "[ERROR] required ROS1 apt bootstrap command is unavailable: ${command}" >&2
      exit 1
    fi
  done
  install_official_ros1_key
  configure_ros1_apt_source 1
  verify_ros1_apt_state

  sudo apt-get update
  verify_pinned_shapely_apt_metadata
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates curl git gnupg lsb-release \
    build-essential "gcc-${GCC_TOOLCHAIN_MAJOR}" "g++-${GCC_TOOLCHAIN_MAJOR}" \
    ninja-build pkg-config \
    python3 python3-dev python3-pip python3-empy python3-numpy \
    "${SHAPELY_APT_PACKAGE}=${SHAPELY_APT_VERSION}" \
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
else
  # --skip-apt skips mutation, not commercial baseline verification.
  verify_ros1_apt_state
fi

# Geometry validation is a production safety dependency, not an optional test
# extra. Verify both normal installs and --skip-apt reuse before any native
# dependency build is allowed to continue.
verify_pinned_shapely

install_pinned_cmake
verify_pinned_compilers
export PATH="${CMAKE_PREFIX}/bin:${PATH}"
export CC="${CC_BIN}"
export CXX="${CXX_BIN}"
hash -r
verify_pinned_cmake "${CMAKE_PREFIX}"

FIELDS2COVER_PYTHON_EXTENSION=""
FIELDS2COVER_PYTHON_MODULE=""

prepare_source() {
  local name="$1"
  local url="$2"
  local commit="$3"
  local source_dir="${BUILD_ROOT}/src/${name}"
  local status_output
  local remote_urls
  local worktree_root

  if [[ -L "${source_dir}" || ( -e "${source_dir}" && ! -d "${source_dir}" ) ]]; then
    echo "[ERROR] unsafe dependency source path: ${source_dir}" >&2
    exit 1
  fi

  if [[ ! -d "${source_dir}/.git" ]]; then
    if [[ -d "${source_dir}" ]] &&
      [[ -n "$(find "${source_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
      echo "[ERROR] refusing to initialize non-empty dependency source directory: ${source_dir}" >&2
      exit 1
    fi
    install -d -m 0755 "${source_dir}"
    git -C "${source_dir}" init -q
    git -C "${source_dir}" remote add origin "${url}"
  else
    if ! worktree_root="$(git -C "${source_dir}" rev-parse --show-toplevel 2>/dev/null)" ||
      [[ "$(realpath -e -- "${worktree_root}")" != "$(realpath -e -- "${source_dir}")" ]]; then
      echo "[ERROR] dependency source is not a standalone Git worktree: ${source_dir}" >&2
      exit 1
    fi
    status_output="$(git -C "${source_dir}" status --porcelain --untracked-files=all)"
    if [[ -n "${status_output}" ]]; then
      echo "[ERROR] dependency source is dirty before checkout: ${source_dir}" >&2
      echo "${status_output}" >&2
      exit 1
    fi
    git -C "${source_dir}" remote set-url origin "${url}"
  fi

  remote_urls="$(git -C "${source_dir}" remote get-url --all origin)"
  if [[ "${remote_urls}" != "${url}" ]]; then
    echo "[ERROR] ${name} origin verification failed: ${remote_urls}" >&2
    exit 1
  fi
  git -C "${source_dir}" fetch -q --depth 1 origin "${commit}"
  git -C "${source_dir}" checkout -q --detach FETCH_HEAD

  if [[ "$(git -C "${source_dir}" rev-parse HEAD)" != "${commit}" ]]; then
    echo "[ERROR] ${name} commit verification failed" >&2
    exit 1
  fi
  remote_urls="$(git -C "${source_dir}" remote get-url --all origin)"
  if [[ "${remote_urls}" != "${url}" ]]; then
    echo "[ERROR] ${name} origin changed during checkout: ${remote_urls}" >&2
    exit 1
  fi
  status_output="$(git -C "${source_dir}" status --porcelain --untracked-files=all)"
  if [[ -n "${status_output}" ]]; then
    echo "[ERROR] dependency source is dirty after checkout: ${source_dir}" >&2
    echo "${status_output}" >&2
    exit 1
  fi

  printf '%s\n' "${source_dir}"
}

build_abseil() {
  local source_dir
  local build_dir="${BUILD_ROOT}/build/abseil-${ABSEIL_VERSION}"
  source_dir="$(prepare_source abseil-cpp https://github.com/abseil/abseil-cpp.git "${ABSEIL_COMMIT}")"

  verify_dependency_cmake_cache "${build_dir}"
  "${CMAKE_BIN}" -S "${source_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="${CC_BIN}" \
    -DCMAKE_CXX_COMPILER="${CXX_BIN}" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_INSTALL_PREFIX="${ABSEIL_PREFIX}" \
    -DABSL_BUILD_TESTING=OFF
  "${CMAKE_BIN}" --build "${build_dir}" --parallel "${JOBS}"
  sudo "${CMAKE_BIN}" --install "${build_dir}"
}

build_ortools() {
  local source_dir
  local build_dir="${BUILD_ROOT}/build/ortools-${ORTOOLS_VERSION}"
  source_dir="$(prepare_source or-tools https://github.com/google/or-tools.git "${ORTOOLS_COMMIT}")"

  verify_dependency_cmake_cache "${build_dir}"
  "${CMAKE_BIN}" -S "${source_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="${CC_BIN}" \
    -DCMAKE_CXX_COMPILER="${CXX_BIN}" \
    -DCMAKE_INSTALL_PREFIX="${ORTOOLS_PREFIX}" \
    -DBUILD_DEPS=ON \
    -DBUILD_CXX=ON \
    -DBUILD_PYTHON=OFF \
    -DBUILD_SAMPLES=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_TESTING=OFF
  "${CMAKE_BIN}" --build "${build_dir}" --parallel "${JOBS}"
  sudo "${CMAKE_BIN}" --install "${build_dir}"
}

build_fields2cover() {
  local source_dir
  local build_dir="${BUILD_ROOT}/build/fields2cover-${FIELDS2COVER_VERSION}"
  local -a python_extensions=()
  local -a python_modules=()
  local candidate
  source_dir="$(prepare_source Fields2Cover https://github.com/Fields2Cover/Fields2Cover.git "${FIELDS2COVER_COMMIT}")"

  verify_dependency_cmake_cache "${build_dir}"
  "${CMAKE_BIN}" -S "${source_dir}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="${CC_BIN}" \
    -DCMAKE_CXX_COMPILER="${CXX_BIN}" \
    -DCMAKE_INSTALL_PREFIX="${FIELDS2COVER_PREFIX}" \
    -DCMAKE_PREFIX_PATH="${ORTOOLS_PREFIX}" \
    -DBUILD_PYTHON=ON \
    -DBUILD_TUTORIALS=OFF \
    -DBUILD_DOC=OFF \
    -DBUILD_TESTING=OFF
  "${CMAKE_BIN}" --build "${build_dir}" --parallel "${JOBS}"
  sudo "${CMAKE_BIN}" --install "${build_dir}"

  while IFS= read -r -d '' candidate; do
    python_extensions+=("${candidate}")
  done < <(find "${FIELDS2COVER_PREFIX}" -xdev -type f -name _fields2cover_python.so -print0)
  while IFS= read -r -d '' candidate; do
    python_modules+=("${candidate}")
  done < <(find "${FIELDS2COVER_PREFIX}" -xdev -type f -name fields2cover.py -print0)

  if [[ "${#python_extensions[@]}" -ne 1 ]]; then
    echo "[ERROR] expected exactly one installed _fields2cover_python.so below ${FIELDS2COVER_PREFIX}, found ${#python_extensions[@]}" >&2
    exit 1
  fi
  if [[ "${#python_modules[@]}" -ne 1 ]]; then
    echo "[ERROR] expected exactly one installed fields2cover.py below ${FIELDS2COVER_PREFIX}, found ${#python_modules[@]}" >&2
    exit 1
  fi

  FIELDS2COVER_PYTHON_EXTENSION="${python_extensions[0]}"
  FIELDS2COVER_PYTHON_MODULE="${python_modules[0]}"

  # Fields2Cover's setup.py copies the SWIG extension directly from its build
  # tree, bypassing CMake's normal install-RPATH rewrite. Harden the copied
  # module before the dependency build cache can ever be removed.
  sudo chown -R root:root "${FIELDS2COVER_PREFIX}"
  sudo bash "${SCRIPT_DIR}/harden_fields2cover_python_install.sh" \
    --require-root-owner \
    "${FIELDS2COVER_PYTHON_EXTENSION}" \
    "${FIELDS2COVER_PYTHON_MODULE}" \
    "${FIELDS2COVER_PREFIX}" \
    "${ORTOOLS_PREFIX}" \
    "${BUILD_ROOT}"
}

build_flirt() {
  local build_dir="${BUILD_ROOT}/build/flirt-${FLIRT_VERSION}"

  verify_dependency_cmake_cache "${build_dir}"
  "${CMAKE_BIN}" -S "${REPO_ROOT}/third_party/flirt" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER="${CC_BIN}" \
    -DCMAKE_CXX_COMPILER="${CXX_BIN}" \
    -DCMAKE_INSTALL_PREFIX="${FLIRT_PREFIX}"
  "${CMAKE_BIN}" --build "${build_dir}" --parallel "${JOBS}"
  sudo "${CMAKE_BIN}" --install "${build_dir}"
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
TEMP_FILES+=("${deps_ld_conf}" "${deps_env}" "${deps_profile}")

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
if [[ ! -f "${DEPS_LD_CONF_TEMPLATE}" || -L "${DEPS_LD_CONF_TEMPLATE}" ]] || \
    ! cmp -s "${deps_ld_conf}" \
      <(grep -v '^[[:space:]]*#' "${DEPS_LD_CONF_TEMPLATE}" | sed '/^[[:space:]]*$/d'); then
  echo "[ERROR] generated loader configuration differs from the fixed commercial template" >&2
  exit 1
fi
if [[ -L /etc/ld.so.conf.d || ! -d /etc/ld.so.conf.d || \
      -L /etc/ld.so.conf.d/doraemon-deps.conf ]]; then
  echo "[ERROR] unsafe dynamic-loader configuration path" >&2
  exit 1
fi
sudo install -o root -g root -m 0644 \
  "${DEPS_LD_CONF_TEMPLATE}" /etc/ld.so.conf.d/doraemon-deps.conf
if [[ "$(stat -c '%U:%G %a' /etc/ld.so.conf.d/doraemon-deps.conf 2>/dev/null || true)" != \
      "root:root 644" ]] || \
    ! cmp -s /etc/ld.so.conf.d/doraemon-deps.conf "${DEPS_LD_CONF_TEMPLATE}"; then
  echo "[ERROR] installed dynamic-loader configuration is not the fixed template" >&2
  exit 1
fi
sudo ldconfig

if [[ -z "${FIELDS2COVER_PYTHON_EXTENSION}" || -z "${FIELDS2COVER_PYTHON_MODULE}" ]]; then
  echo "[ERROR] Fields2Cover Python installation paths were not recorded" >&2
  exit 1
fi
bash "${SCRIPT_DIR}/harden_fields2cover_python_install.sh" \
  --verify-only \
  --system-loader \
  --require-root-owner \
  "${FIELDS2COVER_PYTHON_EXTENSION}" \
  "${FIELDS2COVER_PYTHON_MODULE}" \
  "${FIELDS2COVER_PREFIX}" \
  "${ORTOOLS_PREFIX}" \
  "${BUILD_ROOT}"

cat >"${deps_env}" <<EOF
DORAEMON_DEPS_ROOT=${DEPS_ROOT}
DORAEMON_CMAKE_ROOT=${CMAKE_PREFIX}
DORAEMON_CMAKE_BIN=${CMAKE_BIN}
DORAEMON_CTEST_BIN=${CTEST_BIN}
DORAEMON_GCC_VERSION=${GCC_TOOLCHAIN_VERSION}
CC=${CC_BIN}
CXX=${CXX_BIN}
ABSEIL_ROOT=${ABSEIL_PREFIX}
ORTOOLS_ROOT=${ORTOOLS_PREFIX}
FIELDS2COVER_ROOT=${FIELDS2COVER_PREFIX}
FLIRT_ROOT=${FLIRT_PREFIX}
absl_DIR=${ABSEIL_PREFIX}/lib/cmake/absl
CMAKE_PREFIX_PATH=${ABSEIL_PREFIX}:${ORTOOLS_PREFIX}:${FIELDS2COVER_PREFIX}:${FLIRT_PREFIX}
PYTHONPATH=${PYTHON_SITE_DIR}
PATH=${CMAKE_PREFIX}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
EOF
if [[ ! -f "${DEPS_ENV_TEMPLATE}" || -L "${DEPS_ENV_TEMPLATE}" ]]; then
  echo "[ERROR] missing fixed dependency environment template: ${DEPS_ENV_TEMPLATE}" >&2
  exit 1
fi
commercial_validate_dependencies_env_file "${DEPS_ENV_TEMPLATE}"
if ! cmp -s "${deps_env}" <(grep -v '^[[:space:]]*#' "${DEPS_ENV_TEMPLATE}" | sed '/^[[:space:]]*$/d'); then
  echo "[ERROR] generated dependency environment differs from the fixed commercial template" >&2
  exit 1
fi
sudo install -d -m 0755 /etc/doraemon
sudo install -o root -g root -m 0644 "${DEPS_ENV_TEMPLATE}" /etc/doraemon/deps.env

cat >"${deps_profile}" <<EOF
export DORAEMON_DEPS_ROOT="${DEPS_ROOT}"
export DORAEMON_CMAKE_ROOT="${CMAKE_PREFIX}"
export DORAEMON_CMAKE_BIN="${CMAKE_BIN}"
export DORAEMON_CTEST_BIN="${CTEST_BIN}"
export DORAEMON_GCC_VERSION="${GCC_TOOLCHAIN_VERSION}"
export CC="${CC_BIN}"
export CXX="${CXX_BIN}"
export ABSEIL_ROOT="${ABSEIL_PREFIX}"
export ORTOOLS_ROOT="${ORTOOLS_PREFIX}"
export FIELDS2COVER_ROOT="${FIELDS2COVER_PREFIX}"
export FLIRT_ROOT="${FLIRT_PREFIX}"
export absl_DIR="${ABSEIL_PREFIX}/lib/cmake/absl"
export CMAKE_PREFIX_PATH="${ABSEIL_PREFIX}:${ORTOOLS_PREFIX}:${FIELDS2COVER_PREFIX}:${FLIRT_PREFIX}\${CMAKE_PREFIX_PATH:+:\${CMAKE_PREFIX_PATH}}"
export PYTHONPATH="${PYTHON_SITE_DIR}\${PYTHONPATH:+:\${PYTHONPATH}}"
export PATH="${CMAKE_PREFIX}/bin:\${PATH}"
EOF
if [[ ! -f "${DEPS_PROFILE_TEMPLATE}" || -L "${DEPS_PROFILE_TEMPLATE}" ]] || \
    ! cmp -s "${deps_profile}" <(grep -v '^[[:space:]]*#' "${DEPS_PROFILE_TEMPLATE}" | sed '/^[[:space:]]*$/d'); then
  echo "[ERROR] generated dependency profile differs from the fixed commercial template" >&2
  exit 1
fi
sudo install -o root -g root -m 0644 \
  "${DEPS_PROFILE_TEMPLATE}" /etc/profile.d/doraemon-deps.sh

env PYTHONPATH="${PYTHON_SITE_DIR}" python3 -c \
  "import fields2cover; print('Fields2Cover OK:', fields2cover.__file__)"
verify_pinned_shapely

if ldd "${FIELDS2COVER_PREFIX}/lib/libFields2Cover.so" | grep -q "not found"; then
  echo "[ERROR] Fields2Cover has unresolved shared-library dependencies" >&2
  ldd "${FIELDS2COVER_PREFIX}/lib/libFields2Cover.so" >&2
  exit 1
fi

if offender="$(commercial_find_mount_below "${DEPS_ROOT}" 0)"; then
  echo "[ERROR] dependency tree contains a nested mount: ${offender}" >&2
  exit 1
fi
if offender="$(commercial_find_symlink_outside_tree "${DEPS_ROOT}")"; then
  echo "[ERROR] dependency tree contains a dangling or escaping symlink: ${offender}" >&2
  exit 1
fi
sudo chown -hR root:root "${DEPS_ROOT}"
sudo chmod -R go-w "${DEPS_ROOT}"
sudo chown root:root "${DEPS_ROOT}"
sudo chmod 0755 "${DEPS_ROOT}"

verify_pinned_cmake "${CMAKE_PREFIX}"
verify_pinned_compilers
echo "[OK] Doraemon native dependencies installed under ${DEPS_ROOT}"
echo "[OK] runtime environment written to /etc/doraemon/deps.env"
echo "[INFO] build cache remains at ${BUILD_ROOT} and may be removed after acceptance"
