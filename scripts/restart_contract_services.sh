#!/bin/bash
set -euo pipefail

ROOT_DIR="/home/sunnybaer/Doraemon"
LOG_DIR="/home/sunnybaer/.ros/manual_restart"

PLAN_DB_PATH="${PLAN_DB_PATH:-/data/coverage/planning.db}"
OPS_DB_PATH="${OPS_DB_PATH:-/data/coverage/operations.db}"
ROBOT_ID="${ROBOT_ID:-local_robot}"
RUNTIME_NS="${RUNTIME_NS:-/cartographer/runtime}"
MAP_TOPIC="${MAP_TOPIC:-/map}"
TRACKED_POSE_TOPIC="${TRACKED_POSE_TOPIC:-/tracked_pose}"

mkdir -p "${LOG_DIR}"

source /opt/ros/noetic/setup.bash
source "${ROOT_DIR}/devel/setup.bash"
source "${ROOT_DIR}/scripts/ros_network_env.sh"

echo "[restart_contract_services] ROS_MASTER_URI=${ROS_MASTER_URI:-}"
echo "[restart_contract_services] ROS_IP=${ROS_IP:-}"
echo "[restart_contract_services] ROS_HOSTNAME=${ROS_HOSTNAME:-}"

for node_name in /task_api_service /schedule_api_service /localization_lifecycle_manager; do
  rosnode kill "${node_name}" >/dev/null 2>&1 || true
done
sleep 1

python3 - <<'PY'
import rosgraph
import rosnode

master = rosgraph.Master("/restart_contract_services")
rosnode.cleanup_master_blacklist(
    master,
    ["/task_api_service", "/schedule_api_service", "/localization_lifecycle_manager"],
)
PY

sleep 1

nohup rosrun coverage_task_manager task_api_service_node.py \
  __name:=task_api_service \
  _plan_db_path:="${PLAN_DB_PATH}" \
  _ops_db_path:="${OPS_DB_PATH}" \
  _robot_id:="${ROBOT_ID}" \
  _service_name:=/database_server/clean_task_service \
  _contract_param_ns:=/database_server/contracts/clean_task_service \
  _default_return_to_dock_on_finish:=false \
  >"${LOG_DIR}/task_api_service.out" 2>&1 &

nohup rosrun coverage_task_manager schedule_api_service_node.py \
  __name:=schedule_api_service \
  _plan_db_path:="${PLAN_DB_PATH}" \
  _ops_db_path:="${OPS_DB_PATH}" \
  _robot_id:="${ROBOT_ID}" \
  _service_name:=/database_server/clean_schedule_service \
  _contract_param_ns:=/database_server/contracts/clean_schedule_service \
  >"${LOG_DIR}/schedule_api_service.out" 2>&1 &

nohup rosrun coverage_planner localization_lifecycle_manager_node.py \
  __name:=localization_lifecycle_manager \
  _plan_db_path:="${PLAN_DB_PATH}" \
  _ops_db_path:="${OPS_DB_PATH}" \
  _robot_id:="${ROBOT_ID}" \
  _runtime_ns:="${RUNTIME_NS}" \
  _service_name:=/cartographer/runtime/restart_localization \
  _contract_param_ns:=/cartographer/runtime/contracts/restart_localization \
  _map_topic:="${MAP_TOPIC}" \
  _tracked_pose_topic:="${TRACKED_POSE_TOPIC}" \
  >"${LOG_DIR}/localization_lifecycle_manager.out" 2>&1 &

python3 "${ROOT_DIR}/src/coverage_planner/tools/check_ros_contracts.py" \
  --strict --text --wait-timeout 20 --wait-interval 1
