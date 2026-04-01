#!/bin/bash
set -euo pipefail

source /opt/ros/noetic/setup.bash
source /home/sunnybaer/Doraemon/devel/setup.bash
source /home/sunnybaer/Doraemon/scripts/ros_network_env.sh

exec roslaunch coverage_planner frontend_editor_backend.launch
