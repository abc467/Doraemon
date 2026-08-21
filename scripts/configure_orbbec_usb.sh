#!/usr/bin/env bash

set -euo pipefail

USBFS_MEMORY_PATH="/sys/module/usbcore/parameters/usbfs_memory_mb"
MIN_USBFS_MEMORY_MB="${DORAEMON_ORBBEC_USBFS_MEMORY_MB:-128}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "[ERROR] Orbbec USB preflight must run as root" >&2
  exit 1
fi
if [[ ! "${MIN_USBFS_MEMORY_MB}" =~ ^[1-9][0-9]*$ ]]; then
  echo "[ERROR] DORAEMON_ORBBEC_USBFS_MEMORY_MB must be a positive integer" >&2
  exit 1
fi
if [[ ! -r "${USBFS_MEMORY_PATH}" || ! -w "${USBFS_MEMORY_PATH}" ]]; then
  echo "[ERROR] usbfs memory parameter is unavailable: ${USBFS_MEMORY_PATH}" >&2
  exit 1
fi

current="$(<"${USBFS_MEMORY_PATH}")"
if [[ ! "${current}" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] invalid usbfs_memory_mb value: ${current}" >&2
  exit 1
fi
if (( current < MIN_USBFS_MEMORY_MB )); then
  printf '%s\n' "${MIN_USBFS_MEMORY_MB}" >"${USBFS_MEMORY_PATH}"
fi
current="$(<"${USBFS_MEMORY_PATH}")"
if [[ ! "${current}" =~ ^[0-9]+$ ]] || (( current < MIN_USBFS_MEMORY_MB )); then
  echo "[ERROR] failed to apply usbfs_memory_mb>=${MIN_USBFS_MEMORY_MB}: ${current}" >&2
  exit 1
fi
echo "[OK] usbfs_memory_mb=${current} (required >=${MIN_USBFS_MEMORY_MB})"
