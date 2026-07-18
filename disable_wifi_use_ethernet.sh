#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo bash "$0" "$@"
fi

service_path="/etc/systemd/system/disable-wifi-on-boot.service"

install -m 0644 /dev/stdin "${service_path}" <<'SERVICE'
[Unit]
Description=Disable Wi-Fi after boot
Wants=NetworkManager.service
After=NetworkManager.service

[Service]
Type=oneshot
ExecStart=/usr/bin/nmcli radio wifi off
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable --now disable-wifi-on-boot.service

while IFS=: read -r name type; do
  case "${type}" in
    802-11-wireless|wifi)
      nmcli connection modify "${name}" connection.autoconnect no || true
      ;;
    802-3-ethernet|ethernet)
      nmcli connection modify "${name}" connection.autoconnect yes || true
      ;;
  esac
done < <(nmcli -t -f NAME,TYPE connection show)

nmcli radio wifi off

echo "Wi-Fi has been disabled. Ethernet autoconnect is enabled."
nmcli general status
nmcli device status
