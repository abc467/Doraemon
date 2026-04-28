#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
FRONTEND_TMUX_SESSION="${FRONTEND_TMUX_SESSION:-doraemon_frontend_services}"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/source_slam_runtime_env.sh"
source_slam_runtime_env "${REPO_ROOT}"
ROSBRIDGE_WS_URL="${ROSBRIDGE_WS_URL:-ws://127.0.0.1:9090}"
FRONTEND_SERVICE_MODE="${FRONTEND_SERVICE_MODE:-runtime}"
FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH="${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH:-true}"
EXPECT_FRONTEND_READINESS_APP_QUERY="${EXPECT_FRONTEND_READINESS_APP_QUERY:-}"
EXPECT_FRONTEND_ODOMETRY_APP_QUERY="${EXPECT_FRONTEND_ODOMETRY_APP_QUERY:-}"
CHECK_SITE_GATEWAY="${CHECK_SITE_GATEWAY:-1}"
SITE_GATEWAY_HEALTH_URL="${SITE_GATEWAY_HEALTH_URL:-http://127.0.0.1:4173/api/health}"

export ROS_MASTER_URI="http://localhost:11311"
unset ROS_IP
unset ROS_HOSTNAME

if ! eval "$(
  python3 - <<'PY'
from coverage_planner.canonical_contract_types import (
    APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
    APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
    APP_GET_SLAM_JOB_SERVICE_TYPE,
    APP_GET_SLAM_STATUS_SERVICE_TYPE,
    APP_GET_SYSTEM_READINESS_SERVICE_TYPE,
)

exports = {
    "APP_GET_ODOMETRY_STATUS_SERVICE_TYPE": APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
    "APP_GET_PROFILE_CATALOG_SERVICE_TYPE": APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
    "APP_GET_SLAM_JOB_SERVICE_TYPE": APP_GET_SLAM_JOB_SERVICE_TYPE,
    "APP_GET_SLAM_STATUS_SERVICE_TYPE": APP_GET_SLAM_STATUS_SERVICE_TYPE,
    "APP_GET_SYSTEM_READINESS_SERVICE_TYPE": APP_GET_SYSTEM_READINESS_SERVICE_TYPE,
}

for key in sorted(exports):
    value = str(exports[key]).replace("'", "'\"'\"'")
    print("%s='%s'" % (key, value))
PY
)"; then
  echo "[FAIL] failed to load canonical contract type env" >&2
  exit 1
fi

check_ok() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "[OK] ${label}"
    return 0
  fi
  echo "[FAIL] ${label}"
  return 1
}

check_site_gateway_health() {
  local output
  if ! command -v curl >/dev/null 2>&1; then
    echo "[FAIL] site gateway health: curl unavailable"
    return 1
  fi
  if ! output="$(curl -fsS -m 5 "${SITE_GATEWAY_HEALTH_URL}" 2>&1)"; then
    echo "[FAIL] site gateway health: ${SITE_GATEWAY_HEALTH_URL} ${output}"
    return 1
  fi
  if [[ "${output}" == *'"status":"ok"'* && "${output}" == *'"isConnected":true'* ]]; then
    echo "[OK] site gateway health: ${SITE_GATEWAY_HEALTH_URL}"
    return 0
  fi
  echo "[FAIL] site gateway health: rosbridge not connected ${output}"
  return 1
}

check_service_type() {
  local service_name="$1"
  local expected_type="$2"
  local actual_type
  actual_type="$(rosservice type "${service_name}" 2>/dev/null || true)"
  if [[ "${actual_type}" == "${expected_type}" ]]; then
    echo "[OK] service ${service_name} type=${expected_type}"
    return 0
  fi
  echo "[FAIL] service ${service_name} type=${actual_type:-missing} expected=${expected_type}"
  return 1
}

check_rosbridge_contracts() {
  CHECK_ROSBRIDGE_WS_URL="${ROSBRIDGE_WS_URL}" \
  CHECK_ROSBRIDGE_SKIP_ODOMETRY="${1}" \
  CHECK_ROSBRIDGE_SKIP_READINESS="${2}" \
  python3 - <<'PY'
import json
import os
import sys
import time
import uuid

import websocket

from coverage_planner.canonical_contract_types import (
    APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
    APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
    APP_GET_SLAM_JOB_SERVICE_TYPE,
    APP_GET_SLAM_STATUS_REQUEST_TYPE,
    APP_GET_SLAM_STATUS_SERVICE_TYPE,
    APP_GET_SYSTEM_READINESS_SERVICE_TYPE,
)


def build_service_checks(*, include_odometry=True, include_readiness=True):
    checks = [
        {
            "kind": "service",
            "target": "/clean_robot_server/app/get_slam_status",
            "type": APP_GET_SLAM_STATUS_SERVICE_TYPE,
            "args": {"robot_id": "local_robot", "refresh_map_identity": False},
            "required_value_keys": ("success", "message", "state"),
            "required_nested_value_keys": {
                "state": (
                    "pending_map_name",
                    "active_map_revision_id",
                    "runtime_map_revision_id",
                    "pending_map_revision_id",
                    "pending_map_switch_status",
                    "can_verify_map_revision",
                    "can_activate_map_revision",
                ),
            },
        },
        {
            "kind": "service",
            "target": "/database_server/app/profile_catalog_service",
            "type": APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
            "args": {"profile_kind": "", "include_disabled": False, "map_name": ""},
            "required_value_keys": ("success", "message", "profiles"),
        },
        {
            "kind": "service",
            "target": "/clean_robot_server/app/get_slam_job",
            "type": APP_GET_SLAM_JOB_SERVICE_TYPE,
            "args": {"job_id": "", "robot_id": "local_robot"},
            "required_value_keys": ("found", "message", "error_code", "job"),
            "required_nested_value_keys": {
                "job": ("requested_map_revision_id", "resolved_map_revision_id"),
            },
        },
    ]
    if include_odometry:
        checks.append(
            {
                "kind": "service",
                "target": "/clean_robot_server/app/get_odometry_status",
                "type": APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
                "args": {"robot_id": "local_robot"},
                "required_value_keys": ("success", "message", "state"),
            }
        )
    if include_readiness:
        checks.append(
            {
                "kind": "service",
                "target": "/coverage_task_manager/app/get_system_readiness",
                "type": APP_GET_SYSTEM_READINESS_SERVICE_TYPE,
                "args": {"task_id": 0, "refresh_map_identity": False},
                "required_value_keys": ("success", "message", "readiness"),
                "required_nested_value_keys": {
                    "readiness": (
                        "task_map_revision_id",
                        "active_map_revision_id",
                        "runtime_map_revision_id",
                    ),
                },
            }
        )
    checks.append(
        {
            "kind": "service",
            "target": "/rosapi/service_request_details",
            "type": "",
            "args": {"type": APP_GET_SLAM_STATUS_SERVICE_TYPE},
            "required_value_keys": ("typedefs",),
            "expect_typedef_type": APP_GET_SLAM_STATUS_REQUEST_TYPE,
        }
    )
    return tuple(checks)


def build_topic_checks(*, include_odometry=True):
    checks = [
        {
            "kind": "topic",
            "target": "/clean_robot_server/slam_state",
            "required_msg_keys": (
                "robot_id",
                "current_mode",
                "runtime_map_name",
                "localization_state",
                "pending_map_name",
                "active_map_revision_id",
                "runtime_map_revision_id",
                "pending_map_revision_id",
                "pending_map_switch_status",
                "can_verify_map_revision",
                "can_activate_map_revision",
            ),
        },
    ]
    if include_odometry:
        checks.append(
            {
                "kind": "topic",
                "target": "/clean_robot_server/odometry_state",
                "required_msg_keys": (
                    "robot_id",
                    "odom_source",
                    "validation_mode",
                    "odom_stream_ready",
                    "connected",
                    "odom_valid",
                ),
            }
        )
    return tuple(checks)


def call_service(ws_url, service_name, service_type, args, timeout_s):
    ws = websocket.create_connection(ws_url, timeout=timeout_s)
    request_id = "call:%s:%s" % (service_name, uuid.uuid4().hex)
    payload = {
        "op": "call_service",
        "id": request_id,
        "service": service_name,
        "args": dict(args or {}),
    }
    if service_type:
        payload["type"] = str(service_type)
    ws.send(json.dumps(payload))
    deadline = time.time() + float(timeout_s)
    try:
        while time.time() < deadline:
            raw = ws.recv()
            message = json.loads(raw)
            if str(message.get("id") or "") == request_id:
                return message
        raise RuntimeError("timeout waiting for service response")
    finally:
        try:
            ws.close()
        except Exception:
            pass


def subscribe_once(ws_url, topic_name, timeout_s):
    ws = websocket.create_connection(ws_url, timeout=timeout_s)
    request_id = "sub:%s:%s" % (topic_name, uuid.uuid4().hex)
    payload = {
        "op": "subscribe",
        "id": request_id,
        "topic": topic_name,
        "throttle_rate": 0,
        "queue_length": 1,
    }
    ws.send(json.dumps(payload))
    deadline = time.time() + float(timeout_s)
    try:
        while time.time() < deadline:
            raw = ws.recv()
            message = json.loads(raw)
            if str(message.get("op") or "") != "publish":
                continue
            if str(message.get("topic") or "") != str(topic_name):
                continue
            return message
        raise RuntimeError("timeout waiting for topic publish")
    finally:
        try:
            ws.close()
        except Exception:
            pass


def check_service(ws_url, spec, timeout_s):
    result = {
        "kind": "service",
        "target": str(spec.get("target") or ""),
        "ok": False,
        "issues": [],
    }
    try:
        response = call_service(
            ws_url=ws_url,
            service_name=result["target"],
            service_type=str(spec.get("type") or ""),
            args=dict(spec.get("args") or {}),
            timeout_s=timeout_s,
        )
        values = dict(response.get("values") or {})
        if not bool(response.get("result", False)):
            result["issues"].append("rosbridge result=false")
        for key in tuple(spec.get("required_value_keys") or ()):
            if key not in values:
                result["issues"].append("missing response value key: %s" % str(key))
        for container_key, required_keys in dict(spec.get("required_nested_value_keys") or {}).items():
            nested_value = values.get(container_key)
            if not isinstance(nested_value, dict):
                result["issues"].append("missing nested response object: %s" % str(container_key))
                continue
            for key in tuple(required_keys or ()):
                if key not in nested_value:
                    result["issues"].append(
                        "missing nested response value key: %s.%s" % (str(container_key), str(key))
                    )
        expect_typedef_type = str(spec.get("expect_typedef_type") or "")
        if expect_typedef_type:
            typedefs = list(values.get("typedefs") or [])
            if not typedefs:
                result["issues"].append("typedefs empty")
            else:
                typedef_type = str(dict(typedefs[0] or {}).get("type") or "")
                if typedef_type != expect_typedef_type:
                    result["issues"].append(
                        "unexpected typedef type=%s expected=%s" % (typedef_type, expect_typedef_type)
                    )
        result["ok"] = not result["issues"]
    except Exception as exc:
        result["issues"].append(str(exc))
    return result


def check_topic(ws_url, spec, timeout_s):
    result = {
        "kind": "topic",
        "target": str(spec.get("target") or ""),
        "ok": False,
        "issues": [],
    }
    try:
        message = subscribe_once(ws_url=ws_url, topic_name=result["target"], timeout_s=timeout_s)
        msg = dict(message.get("msg") or {})
        for key in tuple(spec.get("required_msg_keys") or ()):
            if key not in msg:
                result["issues"].append("missing topic field: %s" % str(key))
        result["ok"] = not result["issues"]
    except Exception as exc:
        result["issues"].append(str(exc))
    return result


def print_report(ws_url, checks):
    print("ROSBridge contract checks: %s" % ws_url)
    summary_issues = []
    for item in checks:
        status = "OK" if bool(item.get("ok", False)) else "FAIL"
        print("- %s %s: %s" % (str(item.get("kind") or ""), str(item.get("target") or ""), status))
        for issue in list(item.get("issues") or []):
            print("  issue: %s" % str(issue))
            summary_issues.append("%s %s: %s" % (item["kind"], item["target"], issue))
    print("Summary: %s" % ("OK" if not summary_issues else "FAIL"))
    for issue in summary_issues:
        print("- %s" % issue)
    return 0 if not summary_issues else 1


def main():
    ws_url = os.environ.get("CHECK_ROSBRIDGE_WS_URL", "ws://127.0.0.1:9090")
    include_odometry = os.environ.get("CHECK_ROSBRIDGE_SKIP_ODOMETRY", "0") != "1"
    include_readiness = os.environ.get("CHECK_ROSBRIDGE_SKIP_READINESS", "0") != "1"
    checks = []
    for spec in build_service_checks(include_odometry=include_odometry, include_readiness=include_readiness):
        checks.append(check_service(ws_url, spec, timeout_s=5.0))
    for spec in build_topic_checks(include_odometry=include_odometry):
        checks.append(check_topic(ws_url, spec, timeout_s=6.0))
    return print_report(ws_url, checks)


sys.exit(main())
PY
}

resolve_frontend_expectations() {
  local default_readiness
  local default_odometry

  case "${FRONTEND_SERVICE_MODE}" in
    runtime)
      default_readiness=1
      default_odometry=1
      ;;
    frontend-only)
      default_readiness=0
      if [[ "${FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH}" == "true" ]]; then
        default_odometry=1
      else
        default_odometry=0
      fi
      ;;
    *)
      echo "[FAIL] unknown FRONTEND_SERVICE_MODE=${FRONTEND_SERVICE_MODE}" >&2
      return 1
      ;;
  esac

  EXPECTED_FRONTEND_READINESS_APP_QUERY="${EXPECT_FRONTEND_READINESS_APP_QUERY:-${default_readiness}}"
  EXPECTED_FRONTEND_ODOMETRY_APP_QUERY="${EXPECT_FRONTEND_ODOMETRY_APP_QUERY:-${default_odometry}}"
}

main() {
  local failed=0
  local rosbridge_skip_odometry=0
  local rosbridge_skip_readiness=0

  if ! resolve_frontend_expectations; then
    exit 1
  fi

  if tmux has-session -t "${FRONTEND_TMUX_SESSION}" 2>/dev/null; then
    echo "[OK] frontend tmux session: ${FRONTEND_TMUX_SESSION}"
  else
    echo "[FAIL] frontend tmux session: ${FRONTEND_TMUX_SESSION}"
    failed=1
  fi

  check_ok "ros master reachable" rosnode list || failed=1
  check_ok "service /clean_robot_server/app/map_server" rosservice type /clean_robot_server/app/map_server || failed=1
  check_service_type /database_server/app/profile_catalog_service "${APP_GET_PROFILE_CATALOG_SERVICE_TYPE}" || failed=1
  if [[ "${EXPECTED_FRONTEND_READINESS_APP_QUERY}" == "1" ]]; then
    check_service_type /coverage_task_manager/app/get_system_readiness "${APP_GET_SYSTEM_READINESS_SERVICE_TYPE}" || failed=1
  else
    rosbridge_skip_readiness=1
  fi
  if [[ "${EXPECTED_FRONTEND_ODOMETRY_APP_QUERY}" == "1" ]]; then
    check_service_type /clean_robot_server/app/get_odometry_status "${APP_GET_ODOMETRY_STATUS_SERVICE_TYPE}" || failed=1
  else
    rosbridge_skip_odometry=1
  fi
  check_service_type /clean_robot_server/app/get_slam_status "${APP_GET_SLAM_STATUS_SERVICE_TYPE}" || failed=1
  check_service_type /clean_robot_server/app/get_slam_job "${APP_GET_SLAM_JOB_SERVICE_TYPE}" || failed=1
  check_ok "service /rosapi/topics" rosservice type /rosapi/topics || failed=1
  check_ok "node /rosbridge_websocket" rosnode info /rosbridge_websocket || failed=1

  if output="$(check_rosbridge_contracts "${rosbridge_skip_odometry}" "${rosbridge_skip_readiness}" 2>&1)"; then
    echo "${output}"
    echo "[OK] rosbridge app query contracts: ${ROSBRIDGE_WS_URL}"
  else
    echo "${output}"
    echo "[FAIL] rosbridge app query contracts: ${ROSBRIDGE_WS_URL}"
    failed=1
  fi

  if [[ "${CHECK_SITE_GATEWAY}" == "1" ]]; then
    check_site_gateway_health || failed=1
  fi

  if [[ "${failed}" -eq 0 ]]; then
    echo "[OK] frontend services are online"
  else
    echo "[FAIL] frontend services are not fully ready"
    exit 1
  fi
}

main "$@"
