#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
import time

import rosgraph
import rosservice

from my_msg_srv.msg import CleanSchedule, CleanTask
from my_msg_srv.srv import (
    OperateSchedule,
    OperateScheduleRequest,
    OperateScheduleResponse,
    OperateTask,
    OperateTaskRequest,
    OperateTaskResponse,
    RestartLocalization,
    RestartLocalizationResponse,
)

from coverage_planner.ros_contract import build_contract_report


def _local_contracts():
    return {
        "clean_task_service": build_contract_report(
            service_name="/database_server/clean_task_service",
            contract_name="clean_task_service",
            service_cls=OperateTask,
            request_cls=OperateTaskRequest,
            response_cls=OperateTaskResponse,
            dependencies={"task": CleanTask},
            features=[
                "CleanTask.repeat_after_full_charge",
                "OperateTask.repeat_after_full_charge_state",
            ],
        ),
        "clean_schedule_service": build_contract_report(
            service_name="/database_server/clean_schedule_service",
            contract_name="clean_schedule_service",
            service_cls=OperateSchedule,
            request_cls=OperateScheduleRequest,
            response_cls=OperateScheduleResponse,
            dependencies={"schedule": CleanSchedule},
            features=["CleanSchedule.repeat_after_full_charge"],
        ),
        "restart_localization": build_contract_report(
            service_name="/cartographer/runtime/restart_localization",
            contract_name="restart_localization",
            service_cls=RestartLocalization,
            request_cls=RestartLocalization._request_class,
            response_cls=RestartLocalizationResponse,
            dependencies={},
            features=["restart_localization_service"],
        ),
    }


def _contract_targets(local_contracts):
    return {
        "/database_server/contracts/clean_task_service": local_contracts["clean_task_service"],
        "/database_server/contracts/clean_schedule_service": local_contracts["clean_schedule_service"],
        "/cartographer/runtime/contracts/restart_localization": local_contracts["restart_localization"],
    }


def _service_targets(local_contracts):
    return {
        "/database_server/clean_task_service": local_contracts["clean_task_service"],
        "/database_server/clean_schedule_service": local_contracts["clean_schedule_service"],
        "/cartographer/runtime/restart_localization": local_contracts["restart_localization"],
    }


def _compare_runtime_contract(local_contract, runtime_contract):
    expected_service = dict(local_contract.get("service") or {})
    runtime_service = dict((runtime_contract or {}).get("service") or {})
    issues = []
    if not runtime_service:
        issues.append("runtime contract missing service block")
    else:
        if str(runtime_service.get("md5") or "") != str(expected_service.get("md5") or ""):
            issues.append(
                "service md5 mismatch runtime=%s local=%s"
                % (str(runtime_service.get("md5") or ""), str(expected_service.get("md5") or ""))
            )
    for dep_key, expected_dep in dict(local_contract.get("dependencies") or {}).items():
        runtime_dep = dict((runtime_contract or {}).get("dependencies", {}).get(dep_key) or {})
        if not runtime_dep:
            issues.append("runtime dependency missing: %s" % dep_key)
            continue
        if str(runtime_dep.get("md5") or "") != str(expected_dep.get("md5") or ""):
            issues.append(
                "dependency %s md5 mismatch runtime=%s local=%s"
                % (
                    dep_key,
                    str(runtime_dep.get("md5") or ""),
                    str(expected_dep.get("md5") or ""),
                )
            )
    return {
        "ok": not issues,
        "issues": issues,
    }


def _fetch_runtime(master: rosgraph.Master, local_contracts):
    runtime = {"params": {}, "services": {}}
    for key, expected in _contract_targets(local_contracts).items():
        try:
            value = master.getParam(key)
            runtime["params"][key] = {
                "value": value,
                "comparison": _compare_runtime_contract(expected, value),
            }
        except Exception as exc:
            runtime["params"][key] = {"error": str(exc), "comparison": {"ok": False, "issues": [str(exc)]}}
    for service_name, expected in _service_targets(local_contracts).items():
        service_info = {
            "expected_type": str(expected.get("service", {}).get("type") or ""),
            "expected_md5": str(expected.get("service", {}).get("md5") or ""),
        }
        try:
            service_info["type"] = rosservice.get_service_type(service_name)
            service_info["node"] = rosservice.get_service_node(service_name)
            service_info["uri"] = rosservice.get_service_uri(service_name)
            headers = rosservice.get_service_headers(service_name, service_info["uri"])
            service_info["headers"] = headers
            runtime_md5 = str((headers or {}).get("md5sum") or "")
            runtime_type = str((headers or {}).get("type") or service_info.get("type") or "")
            issues = []
            if runtime_type != service_info["expected_type"]:
                issues.append(
                    "type mismatch runtime=%s local=%s" % (runtime_type, service_info["expected_type"])
                )
            if runtime_md5 != service_info["expected_md5"]:
                issues.append(
                    "md5 mismatch runtime=%s local=%s" % (runtime_md5, service_info["expected_md5"])
                )
            service_info["comparison"] = {"ok": not issues, "issues": issues}
        except Exception as exc:
            service_info["error"] = str(exc)
            service_info["comparison"] = {"ok": False, "issues": [str(exc)]}
        runtime["services"][service_name] = service_info
    summary_issues = []
    for key, value in runtime["params"].items():
        if not bool(((value or {}).get("comparison") or {}).get("ok", False)):
            summary_issues.append("param %s: %s" % (key, "; ".join(((value or {}).get("comparison") or {}).get("issues", []))))
    for key, value in runtime["services"].items():
        if not bool(((value or {}).get("comparison") or {}).get("ok", False)):
            summary_issues.append("service %s: %s" % (key, "; ".join(((value or {}).get("comparison") or {}).get("issues", []))))
    runtime["summary"] = {"ok": not summary_issues, "issues": summary_issues}
    return runtime


def _summary_ok(payload):
    runtime = dict(payload.get("runtime") or {})
    summary = dict(runtime.get("summary") or {})
    return bool(summary.get("ok", False))


def _print_text_summary(payload):
    local = dict(payload.get("local") or {})
    runtime = dict(payload.get("runtime") or {})
    summary = dict(runtime.get("summary") or {})
    print("Local contracts:")
    for key in sorted(local.keys()):
        item = dict(local.get(key) or {})
        service = dict(item.get("service") or {})
        print(
            "- %s: %s md5=%s"
            % (
                key,
                str(service.get("type") or "-"),
                str(service.get("md5") or "-"),
            )
        )
    print("Runtime checks:")
    if "error" in runtime:
        print("- runtime error: %s" % str(runtime.get("error") or "unknown error"))
    for service_name, info in sorted(dict(runtime.get("services") or {}).items()):
        cmp_info = dict((info or {}).get("comparison") or {})
        status = "OK" if bool(cmp_info.get("ok", False)) else "FAIL"
        node = str((info or {}).get("node") or "-")
        md5 = str(dict((info or {}).get("headers") or {}).get("md5sum") or (info or {}).get("expected_md5") or "-")
        print("- %s: %s node=%s md5=%s" % (service_name, status, node, md5))
        for issue in list(cmp_info.get("issues") or []):
            print("  issue: %s" % str(issue))
    for param_name, info in sorted(dict(runtime.get("params") or {}).items()):
        cmp_info = dict((info or {}).get("comparison") or {})
        status = "OK" if bool(cmp_info.get("ok", False)) else "FAIL"
        print("- %s: %s" % (param_name, status))
        for issue in list(cmp_info.get("issues") or []):
            print("  issue: %s" % str(issue))
    if summary:
        print("Overall: %s" % ("OK" if bool(summary.get("ok", False)) else "FAIL"))


def _collect_payload():
    payload = {"local": _local_contracts()}
    try:
        master = rosgraph.Master("/check_ros_contracts")
        master.getPid()
        payload["runtime"] = _fetch_runtime(master, payload["local"])
    except Exception as exc:
        payload["runtime"] = {"error": str(exc), "summary": {"ok": False, "issues": [str(exc)]}}
    return payload


def main():
    parser = argparse.ArgumentParser(description="Check local ROS service/message contracts against runtime")
    parser.add_argument("--strict", action="store_true", help="exit with code 1 when a mismatch or missing contract is found")
    parser.add_argument("--text", action="store_true", help="print a concise human-readable summary instead of JSON")
    parser.add_argument("--wait-timeout", type=float, default=0.0, help="keep checking until contracts are healthy or timeout seconds elapse")
    parser.add_argument("--wait-interval", type=float, default=2.0, help="poll interval seconds while waiting")
    args = parser.parse_args()

    payload = _collect_payload()
    deadline = time.time() + max(0.0, float(args.wait_timeout or 0.0))
    while (not _summary_ok(payload)) and float(args.wait_timeout or 0.0) > 0.0 and time.time() < deadline:
        time.sleep(max(0.2, float(args.wait_interval or 2.0)))
        payload = _collect_payload()

    if args.text:
        _print_text_summary(payload)
    else:
        json.dump(payload, sys.stdout, indent=2, sort_keys=True, ensure_ascii=False)
        sys.stdout.write("\n")
    if args.strict and (not _summary_ok(payload)):
        sys.exit(1)


if __name__ == "__main__":
    main()
