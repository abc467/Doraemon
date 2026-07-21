#!/usr/bin/env bash

set -euo pipefail

VERIFY_ONLY=0
SYSTEM_LOADER=0
REQUIRE_ROOT_OWNER=0

usage() {
  cat <<'EOF'
Usage: harden_fields2cover_python_install.sh [options] \
  EXTENSION PYTHON_MODULE FIELDS2COVER_PREFIX ORTOOLS_PREFIX BUILD_ROOT

Options:
  --verify-only         Do not mutate the installed files.
  --system-loader       Verify ldd resolution with inherited loader search and
                        injection variables removed.
  --require-root-owner  Require every regular file and directory in the prefix to
                        be owned by root:root.
EOF
}

while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --verify-only)
      VERIFY_ONLY=1
      ;;
    --system-loader)
      SYSTEM_LOADER=1
      ;;
    --require-root-owner)
      REQUIRE_ROOT_OWNER=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ "$#" -ne 5 ]]; then
  usage >&2
  exit 2
fi

EXTENSION="$(realpath -e "$1")"
PYTHON_MODULE="$(realpath -e "$2")"
FIELDS2COVER_PREFIX="$(realpath -e "$3")"
ORTOOLS_PREFIX="$(realpath -e "$4")"
BUILD_ROOT="$(realpath -m "$5")"
EXPECTED_RPATH="${FIELDS2COVER_PREFIX}/lib:${ORTOOLS_PREFIX}/lib"

case "${EXTENSION}" in
  "${FIELDS2COVER_PREFIX}"/*) ;;
  *)
    echo "[ERROR] Fields2Cover extension is outside its install prefix: ${EXTENSION}" >&2
    exit 1
    ;;
esac
case "${PYTHON_MODULE}" in
  "${FIELDS2COVER_PREFIX}"/*) ;;
  *)
    echo "[ERROR] Fields2Cover Python module is outside its install prefix: ${PYTHON_MODULE}" >&2
    exit 1
    ;;
esac

if ! command -v patchelf >/dev/null 2>&1; then
  echo "[ERROR] patchelf is required to harden the Fields2Cover Python extension" >&2
  exit 1
fi

if [[ "${VERIFY_ONLY}" -eq 0 ]]; then
  patchelf --set-rpath "${EXPECTED_RPATH}" "${EXTENSION}"
  chmod 0755 "${EXTENSION}"
  chmod 0644 "${PYTHON_MODULE}"
  find "${FIELDS2COVER_PREFIX}" -xdev \( -type f -o -type d \) -perm /022 \
    -exec chmod go-w -- {} +
fi

ACTUAL_RPATH="$(patchelf --print-rpath "${EXTENSION}")"
if [[ "${ACTUAL_RPATH}" != "${EXPECTED_RPATH}" ]]; then
  echo "[ERROR] unexpected Fields2Cover Python RPATH: ${ACTUAL_RPATH}" >&2
  echo "[ERROR] expected: ${EXPECTED_RPATH}" >&2
  exit 1
fi
if [[ "${ACTUAL_RPATH}" == *"${BUILD_ROOT}"* ||
      "${ACTUAL_RPATH}" == *"/var/tmp/"* ||
      "${ACTUAL_RPATH}" == *"_deps"* ]]; then
  echo "[ERROR] Fields2Cover Python RPATH still references a build/cache path: ${ACTUAL_RPATH}" >&2
  exit 1
fi

if [[ "$(stat -c '%a' "${EXTENSION}")" != "755" ]]; then
  echo "[ERROR] Fields2Cover Python extension mode is not 0755: ${EXTENSION}" >&2
  exit 1
fi
if [[ "$(stat -c '%a' "${PYTHON_MODULE}")" != "644" ]]; then
  echo "[ERROR] Fields2Cover Python module mode is not 0644: ${PYTHON_MODULE}" >&2
  exit 1
fi

WRITABLE_ENTRY="$({ find "${FIELDS2COVER_PREFIX}" -xdev \( -type f -o -type d \) -perm /022 -print -quit; } 2>/dev/null)"
if [[ -n "${WRITABLE_ENTRY}" ]]; then
  echo "[ERROR] Fields2Cover install contains a group/other-writable entry: ${WRITABLE_ENTRY}" >&2
  exit 1
fi

if [[ "${REQUIRE_ROOT_OWNER}" -eq 1 ]]; then
  NON_ROOT_ENTRY="$({ find "${FIELDS2COVER_PREFIX}" -xdev \( -type f -o -type d \) \
    \( ! -user root -o ! -group root \) -print -quit; } 2>/dev/null)"
  if [[ -n "${NON_ROOT_ENTRY}" ]]; then
    echo "[ERROR] Fields2Cover install contains a non-root-owned entry: ${NON_ROOT_ENTRY}" >&2
    exit 1
  fi
fi

if [[ "${SYSTEM_LOADER}" -eq 1 ]]; then
  if ! LDD_OUTPUT="$(
    env \
      -u LD_LIBRARY_PATH \
      -u LD_PRELOAD \
      -u LD_AUDIT \
      -u LD_ORIGIN_PATH \
      -u LIBRARY_PATH \
      LC_ALL=C \
      ldd -r "${EXTENSION}" 2>&1
  )"; then
    echo "[ERROR] ldd -r failed for Fields2Cover Python extension" >&2
    printf '%s\n' "${LDD_OUTPUT}" >&2
    exit 1
  fi
else
  VERIFY_LD_LIBRARY_PATH="${FIELDS2COVER_PREFIX}/lib:${ORTOOLS_PREFIX}/lib"
  if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
    VERIFY_LD_LIBRARY_PATH+="${VERIFY_LD_LIBRARY_PATH:+:}${LD_LIBRARY_PATH}"
  fi
  if ! LDD_OUTPUT="$(env LD_LIBRARY_PATH="${VERIFY_LD_LIBRARY_PATH}" LC_ALL=C ldd -r "${EXTENSION}" 2>&1)"; then
    echo "[ERROR] ldd -r failed for Fields2Cover Python extension" >&2
    printf '%s\n' "${LDD_OUTPUT}" >&2
    exit 1
  fi
fi

if grep -Eq '(^|[[:space:]])not found($|[[:space:]])|undefined symbol:' <<<"${LDD_OUTPUT}"; then
  echo "[ERROR] Fields2Cover Python extension has an unresolved dependency" >&2
  printf '%s\n' "${LDD_OUTPUT}" >&2
  exit 1
fi

verify_resolved_prefix() {
  local soname="$1"
  local expected_prefix="$2"
  local resolved

  resolved="$(awk -v soname="${soname}" '$1 == soname && $2 == "=>" {print $3; exit}' <<<"${LDD_OUTPUT}")"
  if [[ -z "${resolved}" || ! -e "${resolved}" ]]; then
    echo "[ERROR] ${soname} did not resolve to an existing library" >&2
    exit 1
  fi
  resolved="$(realpath -e "${resolved}")"
  case "${resolved}" in
    "${expected_prefix}"/*) ;;
    *)
      echo "[ERROR] ${soname} resolved outside ${expected_prefix}: ${resolved}" >&2
      exit 1
      ;;
  esac
}

verify_resolved_prefix libFields2Cover.so "${FIELDS2COVER_PREFIX}"
verify_resolved_prefix libsteering_functions.so "${FIELDS2COVER_PREFIX}"
verify_resolved_prefix libmatplot.so.1 "${FIELDS2COVER_PREFIX}"
verify_resolved_prefix libortools.so.9 "${ORTOOLS_PREFIX}"

echo "[OK] Fields2Cover Python install hardened: rpath=${ACTUAL_RPATH} loader=$([[ "${SYSTEM_LOADER}" -eq 1 ]] && echo system || echo explicit)"
