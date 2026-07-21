#!/usr/bin/env bash

set -euo pipefail

echo "[ERROR] the vendor WheelTec udev helper is retired for commercial vehicles" >&2
echo "[ERROR] its fixed serial identity is not valid for this mainboard" >&2
echo "[INFO] physically identify each serial device on this board, then install a" >&2
echo "[INFO] reviewed root-owned rule derived from:" >&2
echo "[INFO]   deploy/udev/99-doraemon-a26022-serial.rules.example" >&2
exit 2

