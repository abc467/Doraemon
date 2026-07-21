#!/usr/bin/env bash

# Side-effect-free filesystem checks shared by the commercial installer and
# deployment verifier. Callers decide whether a failure is fatal or reported.

commercial_find_mount_below() {
  local path="$1"
  local include_root="${2:-0}"
  local root=""
  local target=""
  local resolved_target=""

  root="$(realpath -e -- "${path}" 2>/dev/null)" || return 1
  while IFS= read -r target; do
    [[ -n "${target}" ]] || continue
    resolved_target="$(realpath -m -- "${target}" 2>/dev/null)" || continue
    if [[ "${include_root}" == "1" && "${resolved_target}" == "${root}" ]]; then
      printf '%s\n' "${target}"
      return 0
    fi
    case "${resolved_target}" in
      "${root}"/*)
        printf '%s\n' "${target}"
        return 0
        ;;
    esac
  done < <(findmnt -rn -o TARGET 2>/dev/null)
  return 1
}

commercial_find_unsafe_release_symlink() {
  local root="$1"
  local canonical_root=""
  local link=""
  local target=""

  canonical_root="$(realpath -e -- "${root}" 2>/dev/null)" || return 1
  while IFS= read -r -d '' link; do
    target="$(realpath -e -- "${link}" 2>/dev/null)" || {
      printf '%s\n' "${link}"
      return 0
    }
    case "${target}" in
      "${canonical_root}"|"${canonical_root}"/*|\
      /opt/ros/noetic/share/catkin/cmake/toplevel.cmake|\
      /usr/include/google/protobuf)
        ;;
      *)
        printf '%s\n' "${link}"
        return 0
        ;;
    esac
  done < <(find "${canonical_root}" -xdev -type l -print0)
  return 1
}

commercial_find_symlink_outside_tree() {
  local root="$1"
  local canonical_root=""
  local link=""
  local target=""

  canonical_root="$(realpath -e -- "${root}" 2>/dev/null)" || return 1
  while IFS= read -r -d '' link; do
    target="$(realpath -e -- "${link}" 2>/dev/null)" || {
      printf '%s\n' "${link}"
      return 0
    }
    case "${target}" in
      "${canonical_root}"|"${canonical_root}"/*) ;;
      *)
        printf '%s\n' "${link}"
        return 0
        ;;
    esac
  done < <(find "${canonical_root}" -xdev -type l -print0)
  return 1
}

commercial_find_forbidden_release_artifact() {
  local root="$1"
  find "${root}" -xdev \
    -path "${root}/.git" -prune -o \
    \( \
      -type d \( \
        -iname log -o -iname logs -o -iname test_bag -o \
        -iname image -o -iname point_cloud \
      \) -o \
      -type f \( \
        -iname '*.bag' -o -iname '*.bag.*' -o -iname export.log -o \
        -iname '*.db' -o -iname '*.sqlite*' \
      \) \
    \) -print -quit
}

commercial_verify_release_git_identity() {
  local root="$1"
  local expected_tag="$2"
  local expected_origin="${3:-}"
  local actual=""
  local status_output=""

  if [[ "$(basename -- "${root}")" != "${expected_tag}" ]]; then
    echo "[ERROR] release directory name must equal deployment tag ${expected_tag}: ${root}" >&2
    return 1
  fi
  if [[ ! -d "${root}/.git" || -L "${root}/.git" || \
        ! -f "${root}/.git/shallow" || -L "${root}/.git/shallow" ]]; then
    echo "[ERROR] backend release must be a shallow Git clone with real .git metadata" >&2
    return 1
  fi
  if ! actual="$(git -c safe.directory="${root}" -C "${root}" \
      rev-parse --is-shallow-repository 2>/dev/null)" || [[ "${actual}" != "true" ]]; then
    echo "[ERROR] backend release repository is not shallow" >&2
    return 1
  fi
  if ! actual="$(git -c safe.directory="${root}" -C "${root}" \
      describe --tags --exact-match 2>/dev/null)" || [[ "${actual}" != "${expected_tag}" ]]; then
    echo "[ERROR] backend release HEAD is not exact tag ${expected_tag}" >&2
    return 1
  fi
  if [[ "$(git -c safe.directory="${root}" -C "${root}" \
      cat-file -t "refs/tags/${expected_tag}" 2>/dev/null || true)" != "tag" ]]; then
    echo "[ERROR] deployment tag must be an annotated tag object: ${expected_tag}" >&2
    return 1
  fi
  if [[ -n "${expected_origin}" ]]; then
    if ! actual="$(git -c safe.directory="${root}" -C "${root}" \
        remote get-url --all origin 2>/dev/null)" || [[ "${actual}" != "${expected_origin}" ]]; then
      echo "[ERROR] backend origin must be exactly ${expected_origin} (actual=${actual:-missing})" >&2
      return 1
    fi
  fi
  if [[ "$(git -c safe.directory="${root}" -C "${root}" rev-parse HEAD 2>/dev/null)" != \
        "$(git -c safe.directory="${root}" -C "${root}" rev-list -n 1 "${expected_tag}" 2>/dev/null)" ]]; then
    echo "[ERROR] deployment tag ${expected_tag} does not resolve to release HEAD" >&2
    return 1
  fi
  if ! status_output="$(git -c safe.directory="${root}" -C "${root}" \
      status --porcelain=v1 --untracked-files=all 2>&1)"; then
    echo "[ERROR] failed to inspect backend release Git status: ${status_output}" >&2
    return 1
  fi
  if [[ -n "${status_output}" ]]; then
    echo "[ERROR] backend release tracked/source tree is dirty" >&2
    echo "${status_output}" >&2
    return 1
  fi
}

commercial_verify_remote_deployment_tag() {
  local git_url="$1"
  local expected_tag="$2"
  local expected_commit="$3"
  local refs=""
  local remote_commit=""
  local remote_status=0

  if [[ ! "${expected_tag}" =~ ^[-A-Za-z0-9._]+$ || \
        ! "${expected_commit}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "[ERROR] invalid remote deployment tag/commit identity" >&2
    return 1
  fi
  if refs="$(
      GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false SSH_ASKPASS=/bin/false \
        /usr/bin/timeout --signal=TERM --kill-after=5s 20s \
        /usr/bin/git ls-remote --exit-code --tags "${git_url}" \
          "refs/tags/${expected_tag}" "refs/tags/${expected_tag}^{}" \
          2>/dev/null
    )"; then
    :
  else
    remote_status="$?"
    if [[ "${remote_status}" -eq 124 || "${remote_status}" -eq 137 ]]; then
      echo "[ERROR] remote deployment tag check timed out after 20 seconds: ${git_url}" >&2
    else
      echo "[ERROR] deployment tag is not published or reachable at ${git_url}: ${expected_tag}" >&2
    fi
    return 1
  fi
  remote_commit="$(awk -v ref="refs/tags/${expected_tag}^{}" '$2 == ref {print $1}' <<<"${refs}")"
  if [[ "${remote_commit}" != "${expected_commit}" ]]; then
    echo "[ERROR] remote annotated tag does not resolve to expected commit" >&2
    return 1
  fi
}

commercial_validate_trial_release_exception() {
  local exception_file="$1"
  local expected_tag="$2"
  local expected_commit="$3"
  local expected_git_url="$4"
  local expected_robot_id="${5:-}"
  local line=""
  local key=""
  local value=""
  local now=""
  local -A values=()
  local -A allowed_keys=(
    [DORAEMON_TRIAL_EXCEPTION_VERSION]=1
    [DORAEMON_TRIAL_ROBOT_ID]=1
    [DORAEMON_TRIAL_HOSTNAME]=1
    [DORAEMON_TRIAL_BACKEND_TAG]=1
    [DORAEMON_TRIAL_BACKEND_COMMIT]=1
    [DORAEMON_TRIAL_CANONICAL_GIT_URL]=1
    [DORAEMON_TRIAL_ALLOW_UNPUBLISHED_TAG]=1
    [DORAEMON_TRIAL_EXPIRES_UTC]=1
    [DORAEMON_TRIAL_REASON]=1
  )

  if [[ ! -f "${exception_file}" || -L "${exception_file}" || \
        "$(stat -c '%U:%G %a' "${exception_file}" 2>/dev/null || true)" != "root:a 640" ]]; then
    echo "[ERROR] trial release exception must be a root:a 0640 regular file" >&2
    return 1
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -n "${line}" ]] || continue
    if [[ ! "${line}" =~ ^([A-Z][A-Z0-9_]*)=([-A-Za-z0-9_./:+]+)$ ]]; then
      echo "[ERROR] trial release exception contains an unsafe line" >&2
      return 1
    fi
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"
    if [[ -z "${allowed_keys[${key}]+x}" || -n "${values[${key}]+x}" ]]; then
      echo "[ERROR] trial release exception has an unexpected or duplicate key: ${key}" >&2
      return 1
    fi
    values[${key}]="${value}"
  done <"${exception_file}"
  for key in "${!allowed_keys[@]}"; do
    if [[ -z "${values[${key}]+x}" ]]; then
      echo "[ERROR] trial release exception is missing ${key}" >&2
      return 1
    fi
  done
  now="$(date -u +%Y%m%dT%H%M%SZ)"
  if [[ "${values[DORAEMON_TRIAL_EXCEPTION_VERSION]}" != "1" ||
        "${values[DORAEMON_TRIAL_HOSTNAME]}" != "$(hostname)" ||
        "${values[DORAEMON_TRIAL_BACKEND_TAG]}" != "${expected_tag}" ||
        "${values[DORAEMON_TRIAL_BACKEND_COMMIT]}" != "${expected_commit}" ||
        "${values[DORAEMON_TRIAL_CANONICAL_GIT_URL]}" != "${expected_git_url}" ||
        "${values[DORAEMON_TRIAL_ALLOW_UNPUBLISHED_TAG]}" != "true" ||
        ! "${values[DORAEMON_TRIAL_ROBOT_ID]}" =~ ^[-A-Za-z0-9._]+$ ||
        ! "${values[DORAEMON_TRIAL_EXPIRES_UTC]}" =~ ^[0-9]{8}T[0-9]{6}Z$ ||
        "${values[DORAEMON_TRIAL_EXPIRES_UTC]}" < "${now}" ||
        ( -n "${expected_robot_id}" && \
          "${values[DORAEMON_TRIAL_ROBOT_ID]}" != "${expected_robot_id}" ) ]]; then
    echo "[ERROR] trial release exception does not match this vehicle/tag or has expired" >&2
    return 1
  fi
}

commercial_cmake_cache_value() {
  local cache="$1"
  local key="$2"
  local -a values=()

  mapfile -t values < <(sed -n "s/^${key}:[^=]*=//p" "${cache}")
  if [[ "${#values[@]}" -ne 1 || -z "${values[0]}" ]]; then
    echo "[ERROR] CMake cache must contain exactly one ${key}: ${cache}" >&2
    return 1
  fi
  printf '%s\n' "${values[0]}"
}

commercial_validate_workspace_build_provenance() {
  local root="$1"
  local expected_tag="$2"
  local expected_cmake="$3"
  local expected_cc="$4"
  local expected_cxx="$5"
  local expected_origin="${6:-}"
  local marker=""
  local line=""
  local key=""
  local value=""
  local cache=""
  local cache_value=""
  local cache_home=""
  local foreign_content=""
  local actual_commit=""
  local actual_tree=""
  local actual_hostname=""
  local actual_machine_id_sha256=""
  local actual_cache_count=0
  local marker_stat=""
  local expected_robot_id=""
  local -a caches=()
  local -A marker_values=()
  local -A allowed_keys=(
    [DORAEMON_BUILD_PROVENANCE_VERSION]=1
    [DORAEMON_BUILD_REPO_ROOT]=1
    [DORAEMON_BUILD_GIT_TAG]=1
    [DORAEMON_BUILD_GIT_COMMIT]=1
    [DORAEMON_BUILD_GIT_TREE]=1
    [DORAEMON_BUILD_HOSTNAME]=1
    [DORAEMON_BUILD_MACHINE_ID_SHA256]=1
    [DORAEMON_BUILD_CMAKE_BIN]=1
    [DORAEMON_BUILD_CC]=1
    [DORAEMON_BUILD_CXX]=1
    [DORAEMON_BUILD_CACHE_COUNT]=1
    [DORAEMON_BUILD_FINISHED_UTC]=1
    [DORAEMON_BUILD_SOURCE_ATTESTATION]=1
  )

  root="$(realpath -e -- "${root}" 2>/dev/null)" || {
    echo "[ERROR] workspace root does not exist" >&2
    return 1
  }
  commercial_verify_release_git_identity "${root}" "${expected_tag}" \
    "${expected_origin}" || return 1
  expected_cmake="$(realpath -e -- "${expected_cmake}" 2>/dev/null)" || {
    echo "[ERROR] pinned CMake executable does not exist" >&2
    return 1
  }
  expected_cc="$(realpath -e -- "${expected_cc}" 2>/dev/null)" || {
    echo "[ERROR] pinned C compiler does not exist" >&2
    return 1
  }
  expected_cxx="$(realpath -e -- "${expected_cxx}" 2>/dev/null)" || {
    echo "[ERROR] pinned C++ compiler does not exist" >&2
    return 1
  }

  for required_directory in "${root}/build" "${root}/devel"; do
    if [[ ! -d "${required_directory}" || -L "${required_directory}" ]]; then
      echo "[ERROR] workspace build provenance requires a real directory: ${required_directory}" >&2
      return 1
    fi
    if foreign_content="$(commercial_find_mount_below "${required_directory}" 0)"; then
      echo "[ERROR] workspace build tree contains a nested mount: ${foreign_content}" >&2
      return 1
    fi
  done
  cache_value="$(realpath -e -- "${root}/devel/setup.bash" 2>/dev/null)" || {
    echo "[ERROR] workspace devel/setup.bash is missing or dangling" >&2
    return 1
  }
  if [[ ! -f "${cache_value}" ]]; then
    echo "[ERROR] workspace devel/setup.bash does not resolve to a regular file" >&2
    return 1
  fi
  case "${cache_value}" in
    "${root}/devel"/*) ;;
    *)
      echo "[ERROR] workspace devel/setup.bash resolves outside this release" >&2
      return 1
      ;;
  esac

  marker="${root}/build/.doraemon-commercial-build.env"
  marker_stat="$(stat -c '%U:%G %a' "${marker}" 2>/dev/null || true)"
  if [[ ! -f "${marker}" || -L "${marker}" || \
        ( "${marker_stat}" != "a:a 644" && "${marker_stat}" != "root:root 644" ) ]]; then
    echo "[ERROR] workspace build provenance marker must be a:a or root:root mode 0644" >&2
    return 1
  fi
  if [[ -n "$(find "${root}/build" -xdev -type l -name CMakeCache.txt -print -quit)" ]]; then
    echo "[ERROR] workspace contains a symlinked CMakeCache.txt" >&2
    return 1
  fi
  mapfile -d '' -t caches < <(
    find "${root}/build" -xdev -type f -name CMakeCache.txt -print0 | sort -z
  )
  actual_cache_count="${#caches[@]}"
  if [[ "${actual_cache_count}" -eq 0 ]]; then
    echo "[ERROR] workspace build contains no CMakeCache.txt files" >&2
    return 1
  fi

  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -n "${line}" ]] || continue
    if [[ ! "${line}" =~ ^([A-Z][A-Z0-9_]*)=([-A-Za-z0-9_./:+]+)$ ]]; then
      echo "[ERROR] workspace build provenance marker contains an unsafe line" >&2
      return 1
    fi
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"
    if [[ -z "${allowed_keys[${key}]+x}" || -n "${marker_values[${key}]+x}" ]]; then
      echo "[ERROR] workspace build provenance marker has an unexpected or duplicate key: ${key}" >&2
      return 1
    fi
    marker_values[${key}]="${value}"
  done <"${marker}"
  for key in "${!allowed_keys[@]}"; do
    if [[ -z "${marker_values[${key}]+x}" ]]; then
      echo "[ERROR] workspace build provenance marker is missing ${key}" >&2
      return 1
    fi
  done

  actual_commit="$(git -c safe.directory="${root}" -C "${root}" rev-parse HEAD 2>/dev/null)" || return 1
  actual_tree="$(git -c safe.directory="${root}" -C "${root}" rev-parse 'HEAD^{tree}' 2>/dev/null)" || return 1
  actual_hostname="$(hostname)"
  actual_machine_id_sha256="$(sha256sum /etc/machine-id | awk '{print $1}')"
  if [[ "${marker_values[DORAEMON_BUILD_PROVENANCE_VERSION]}" != "1" ||
        "${marker_values[DORAEMON_BUILD_REPO_ROOT]}" != "${root}" ||
        "${marker_values[DORAEMON_BUILD_GIT_TAG]}" != "${expected_tag}" ||
        "${marker_values[DORAEMON_BUILD_GIT_COMMIT]}" != "${actual_commit}" ||
        "${marker_values[DORAEMON_BUILD_GIT_TREE]}" != "${actual_tree}" ||
        "${marker_values[DORAEMON_BUILD_HOSTNAME]}" != "${actual_hostname}" ||
        "${marker_values[DORAEMON_BUILD_MACHINE_ID_SHA256]}" != "${actual_machine_id_sha256}" ||
        "${marker_values[DORAEMON_BUILD_CMAKE_BIN]}" != "${expected_cmake}" ||
        "${marker_values[DORAEMON_BUILD_CC]}" != "${expected_cc}" ||
        "${marker_values[DORAEMON_BUILD_CXX]}" != "${expected_cxx}" ||
        "${marker_values[DORAEMON_BUILD_CACHE_COUNT]}" != "${actual_cache_count}" ||
        ! "${marker_values[DORAEMON_BUILD_FINISHED_UTC]}" =~ ^[0-9]{8}T[0-9]{6}Z$ ||
        ( "${marker_values[DORAEMON_BUILD_SOURCE_ATTESTATION]}" != "remote" && \
          "${marker_values[DORAEMON_BUILD_SOURCE_ATTESTATION]}" != "trial-exception" ) ]]; then
    echo "[ERROR] workspace build provenance marker does not match this machine, tag, or toolchain" >&2
    return 1
  fi
  if [[ "${marker_values[DORAEMON_BUILD_SOURCE_ATTESTATION]}" == "trial-exception" ]]; then
    if [[ -f /etc/doraemon/runtime.env ]] && \
        commercial_validate_runtime_env_file /etc/doraemon/runtime.env; then
      expected_robot_id="$(sed -n 's/^ROBOT_ID=//p' /etc/doraemon/runtime.env | head -n1)"
    fi
    commercial_validate_trial_release_exception \
      /etc/doraemon/trial-release-exception.env \
      "${expected_tag}" "${actual_commit}" "${expected_origin}" \
      "${expected_robot_id}" || return 1
  fi

  for cache in "${caches[@]}"; do
    if [[ "$(realpath -e -- "${cache}" 2>/dev/null)" != "${cache}" ]]; then
      echo "[ERROR] CMake cache is not a canonical regular path: ${cache}" >&2
      return 1
    fi
    cache_value="$(commercial_cmake_cache_value "${cache}" CMAKE_COMMAND)" || return 1
    if [[ "$(realpath -e -- "${cache_value}" 2>/dev/null)" != "${expected_cmake}" ]]; then
      echo "[ERROR] CMake cache used an unpinned cmake command: ${cache}" >&2
      return 1
    fi
    cache_value="$(commercial_cmake_cache_value "${cache}" CMAKE_C_COMPILER)" || return 1
    if [[ "$(realpath -e -- "${cache_value}" 2>/dev/null)" != "${expected_cc}" ]]; then
      echo "[ERROR] CMake cache used an unpinned C compiler: ${cache}" >&2
      return 1
    fi
    cache_value="$(commercial_cmake_cache_value "${cache}" CMAKE_CXX_COMPILER)" || return 1
    if [[ "$(realpath -e -- "${cache_value}" 2>/dev/null)" != "${expected_cxx}" ]]; then
      echo "[ERROR] CMake cache used an unpinned C++ compiler: ${cache}" >&2
      return 1
    fi
    cache_home="$(commercial_cmake_cache_value "${cache}" CMAKE_HOME_DIRECTORY)" || return 1
    cache_home="$(realpath -e -- "${cache_home}" 2>/dev/null)" || {
      echo "[ERROR] CMake source directory no longer exists: ${cache}" >&2
      return 1
    }
    case "${cache_home}" in
      "${root}/src"/*) ;;
      "${root}/build/catkin_tools_prebuild")
        if [[ "${cache}" != "${root}/build/catkin_tools_prebuild/CMakeCache.txt" ]]; then
          echo "[ERROR] catkin prebuild source exception used by an unexpected cache: ${cache}" >&2
          return 1
        fi
        ;;
      *)
        echo "[ERROR] CMake cache source directory is outside this release: ${cache_home}" >&2
        return 1
        ;;
    esac
    if grep -Eq '/home/|/var/tmp/|/opt/doraemon/deps/(ortools-9\.9|fields2cover-2\.0\.0)' "${cache}"; then
      echo "[ERROR] CMake cache contains a workstation/build-cache or incompatible prefix: ${cache}" >&2
      return 1
    fi
    foreign_content="$(sed "s|${root}|<current-release>|g" "${cache}" | \
      grep -m1 -E '/opt/doraemon/releases/' || true)"
    if [[ -n "${foreign_content}" ]]; then
      echo "[ERROR] CMake cache references another release: ${cache}" >&2
      return 1
    fi
  done
}

commercial_validate_slam_config_override_tree() {
  local root="${1:-/data/config/slam/cartographer}"
  local required_layout="${2:-0}"
  local actual=""
  local offender=""

  if [[ "${required_layout}" != "0" && "${required_layout}" != "1" ]]; then
    echo "[ERROR] SLAM config layout policy must be 0 (empty allowed) or 1 (required)" >&2
    return 1
  fi

  if [[ ! -d "${root}" || -L "${root}" || "$(realpath -e -- "${root}" 2>/dev/null)" != "${root}" ]]; then
    echo "[ERROR] SLAM config override must be a real canonical directory: ${root}" >&2
    return 1
  fi
  actual="$(stat -c '%U:%G %a' "${root}" 2>/dev/null || true)"
  if [[ "${actual}" != "root:a 750" ]]; then
    echo "[ERROR] SLAM config override root must be root:a 0750 (actual=${actual:-missing})" >&2
    return 1
  fi
  if offender="$(commercial_find_mount_below "${root}" 1)"; then
    echo "[ERROR] SLAM config override must not contain a mount point: ${offender}" >&2
    return 1
  fi
  offender="$(find "${root}" -xdev -type l -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit SLAM config override symlinks: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] SLAM config override may not contain symlinks: ${offender}" >&2
    return 1
  fi
  offender="$(find "${root}" -xdev \
    \( -type f -o -type d \) \( ! -user root -o ! -group a \) \
    -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit SLAM config override ownership: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] SLAM config override must be entirely root:a: ${offender}" >&2
    return 1
  fi
  offender="$(find "${root}" -xdev \
    \( -type d ! -perm 0750 -o -type f ! -perm 0640 -o \
       ! -type d ! -type f ! -type l \) -print -quit 2>&1)" || {
    echo "[ERROR] failed to audit SLAM config override modes/types: ${offender}" >&2
    return 1
  }
  if [[ -n "${offender}" ]]; then
    echo "[ERROR] SLAM override directories must be 0750 and files 0640: ${offender}" >&2
    return 1
  fi

  # Policy 0 permits only a completely empty staging directory. The moment an
  # override contains any entry it becomes authoritative and must have the
  # complete reviewed layout; an incomplete tree must never silently fall back
  # to the release configuration.
  if [[ "${required_layout}" == "0" ]]; then
    offender="$(find "${root}" -xdev -mindepth 1 -print -quit 2>&1)" || {
      echo "[ERROR] failed to inspect SLAM config override contents: ${offender}" >&2
      return 1
    }
    if [[ -n "${offender}" ]]; then
      required_layout="1"
    fi
  fi
  if [[ "${required_layout}" == "1" ]]; then
    for required_file in \
      "${root}/slam/config.lua" \
      "${root}/pure_location_odom/config.lua" \
      "${root}/relocalization/global_relocation.sml"; do
      if [[ ! -f "${required_file}" || -L "${required_file}" ]]; then
        echo "[ERROR] incomplete reviewed SLAM config override: ${required_file}" >&2
        return 1
      fi
    done
  fi
}
