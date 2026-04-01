#!/bin/bash

# Shared ROS network environment for cross-machine access (e.g. Windows frontend via rosbridge).
# Usage:
#   source /home/sunnybaer/Doraemon/scripts/ros_network_env.sh

_ros_default_ip() {
  local ips ip
  ips="$(hostname -I 2>/dev/null)"
  for ip in $ips; do
    case "$ip" in
      10.*) echo "$ip"; return 0 ;;
    esac
  done
  for ip in $ips; do
    case "$ip" in
      192.168.*) echo "$ip"; return 0 ;;
    esac
  done
  echo "$ips" | awk '{print $1}'
}

if [ -z "${ROS_MACHINE_IP:-}" ]; then
  ROS_MACHINE_IP="$(_ros_default_ip)"
fi

if [ -n "${ROS_MACHINE_IP:-}" ]; then
  export ROS_IP="${ROS_IP:-$ROS_MACHINE_IP}"
  export ROS_HOSTNAME="${ROS_HOSTNAME:-$ROS_MACHINE_IP}"
  export ROS_MASTER_URI="${ROS_MASTER_URI:-http://$ROS_MACHINE_IP:11311}"
fi
