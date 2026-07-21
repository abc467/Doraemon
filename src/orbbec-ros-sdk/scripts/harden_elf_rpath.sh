#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: harden_elf_rpath.sh <dynamic-elf>" >&2
  exit 2
fi

ELF_PATH="$1"
PATCHELF_BIN="${PATCHELF:-patchelf}"

if [[ ! -f "${ELF_PATH}" || -L "${ELF_PATH}" ]]; then
  echo "[ERROR] expected a regular, non-symlink ELF: ${ELF_PATH}" >&2
  exit 1
fi
if ! command -v "${PATCHELF_BIN}" >/dev/null 2>&1; then
  echo "[ERROR] patchelf is required to harden Orbbec binaries" >&2
  exit 1
fi
if ! readelf -h "${ELF_PATH}" >/dev/null 2>&1; then
  echo "[ERROR] expected a dynamic ELF: ${ELF_PATH}" >&2
  exit 1
fi

original_rpath="$("${PATCHELF_BIN}" --print-rpath "${ELF_PATH}")"
sanitized_components=()

if [[ -n "${original_rpath}" ]]; then
  IFS=':' read -r -a rpath_components <<<"${original_rpath}"
  for component in "${rpath_components[@]}"; do
    # An empty component, or '.', makes the loader search the process working
    # directory. Drop it instead of preserving an unsafe implicit search path.
    if [[ -z "${component}" || "${component}" == "." ]]; then
      continue
    fi

    case "${component}" in
      '$ORIGIN'|'$ORIGIN/..')
        ;;
      /opt/ros/noetic/lib|/opt/doraemon/deps/*|/usr/lib|/usr/lib/*|/lib|/lib/*)
        ;;
      *)
        echo "[ERROR] unsafe Orbbec RPATH component in ${ELF_PATH}: ${component}" >&2
        exit 1
        ;;
    esac

    duplicate=0
    for existing in "${sanitized_components[@]}"; do
      if [[ "${existing}" == "${component}" ]]; then
        duplicate=1
        break
      fi
    done
    if [[ "${duplicate}" -eq 0 ]]; then
      sanitized_components+=("${component}")
    fi
  done
fi

sanitized_rpath=""
if (( ${#sanitized_components[@]} > 0 )); then
  printf -v sanitized_rpath '%s:' "${sanitized_components[@]}"
  sanitized_rpath="${sanitized_rpath%:}"
fi

if [[ "${sanitized_rpath}" != "${original_rpath}" ]]; then
  if [[ -n "${sanitized_rpath}" ]]; then
    "${PATCHELF_BIN}" --set-rpath "${sanitized_rpath}" "${ELF_PATH}"
  else
    "${PATCHELF_BIN}" --remove-rpath "${ELF_PATH}"
  fi
fi

verified_rpath="$("${PATCHELF_BIN}" --print-rpath "${ELF_PATH}")"
if [[ "${verified_rpath}" != "${sanitized_rpath}" ]]; then
  echo "[ERROR] Orbbec RPATH verification failed for ${ELF_PATH}" >&2
  exit 1
fi

if [[ -n "${verified_rpath}" ]]; then
  IFS=':' read -r -a verified_components <<<"${verified_rpath}"
  for component in "${verified_components[@]}"; do
    if [[ -z "${component}" || "${component}" == "." ]]; then
      echo "[ERROR] empty/current-directory RPATH remained in ${ELF_PATH}" >&2
      exit 1
    fi
  done
fi
