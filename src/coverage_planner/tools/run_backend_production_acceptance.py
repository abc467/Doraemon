#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from check_revision_db_health import build_report as build_db_health_report
from run_backend_runtime_smoke import build_report as build_runtime_smoke_report
from run_revision_workflow_acceptance import build_report as build_revision_acceptance_report


SUPPORTED_PROFILES = (
    "read_only_gate",
    "verify_revision_gate",
    "activate_revision_gate",
    "activate_revision_prepare_for_task_gate",
    "candidate_save_gate",
    "revision_cycle_gate",
    "revision_cycle_prepare_for_task_gate",
)

WRITE_PROFILES = {
    "verify_revision_gate",
    "activate_revision_gate",
    "activate_revision_prepare_for_task_gate",
    "candidate_save_gate",
    "revision_cycle_gate",
    "revision_cycle_prepare_for_task_gate",
}

PROFILE_TO_REVISION_PROFILE = {
    "verify_revision_gate": "verify_revision",
    "activate_revision_gate": "activate_revision",
    "activate_revision_prepare_for_task_gate": "activate_revision_prepare_for_task",
    "candidate_save_gate": "mapping_save_candidate",
    "revision_cycle_gate": "mapping_save_verify_activate",
    "revision_cycle_prepare_for_task_gate": "mapping_save_verify_activate_prepare_for_task",
}


def _stage(name: str, report: Optional[Dict[str, object]], *, skipped: bool = False, skip_reason: str = "") -> Dict[str, object]:
    summary = dict((report or {}).get("summary") or {})
    if skipped:
        return {
            "name": str(name or ""),
            "ok": False,
            "skipped": True,
            "skip_reason": str(skip_reason or ""),
            "revision_scope": {},
            "report": report or {},
        }
    return {
        "name": str(name or ""),
        "ok": bool(summary.get("ok", False)),
        "skipped": False,
        "skip_reason": "",
        "revision_scope": dict((report or {}).get("revision_scope") or {}),
        "report": report or {},
    }


def _summary_issues(report: Dict[str, object]) -> List[str]:
    summary = dict(report.get("summary") or {})
    issues = [str(item) for item in list(summary.get("issues") or []) if str(item)]
    if issues:
        return issues
    findings = list(report.get("findings") or [])
    return [
        "%s:%s" % (str(item.get("code") or ""), str(item.get("message") or ""))
        for item in findings
        if str(item.get("message") or "")
    ]


def _smoke_args(args) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=int(args.task_id),
        service_timeout=float(args.service_timeout),
        job_timeout=float(args.smoke_job_timeout if args.smoke_job_timeout is not None else args.job_timeout),
        poll_interval=float(args.poll_interval),
        robot_id=str(args.robot_id or "local_robot"),
        ops_db_path=str(args.ops_db_path or ""),
        map_name="",
        map_revision_id="",
        frame_id=str(args.frame_id or "map"),
        save_map_name="",
        description_prefix=str(args.description_prefix or "production_gate"),
        actions=[],
        set_active=False,
        has_initial_pose=False,
        initial_pose_x=0.0,
        initial_pose_y=0.0,
        initial_pose_yaw=0.0,
        include_unfinished_submaps=False,
        set_active_on_save=False,
        switch_to_localization_after_save=False,
        relocalize_after_switch=False,
        run_task_cycle=bool(getattr(args, "run_task_cycle", False)),
        task_timeout=float(getattr(args, "task_timeout", 300.0)),
        ignore_warning=list(args.ignore_warning or []),
        json=False,
        text=False,
    )


def _revision_args(args) -> SimpleNamespace:
    return SimpleNamespace(
        profile=str(PROFILE_TO_REVISION_PROFILE.get(args.profile) or ""),
        task_id=int(args.task_id),
        service_timeout=float(args.service_timeout),
        job_timeout=float(args.job_timeout),
        poll_interval=float(args.poll_interval),
        robot_id=str(args.robot_id or "local_robot"),
        map_name=str(args.map_name or ""),
        map_revision_id=str(args.map_revision_id or ""),
        save_map_name=str(args.save_map_name or ""),
        frame_id=str(args.frame_id or "map"),
        description_prefix=str(args.description_prefix or "production_gate"),
        allow_write_actions=True,
        ignore_warning=list(args.ignore_warning or []),
        json=False,
        text=False,
    )


def build_report(
    args,
    *,
    db_report_builder: Callable[..., Dict[str, object]] = build_db_health_report,
    smoke_report_builder: Callable[[object], Dict[str, object]] = build_runtime_smoke_report,
    revision_report_builder: Callable[[object], Dict[str, object]] = build_revision_acceptance_report,
) -> Dict[str, object]:
    stages: List[Dict[str, object]] = []

    db_report = db_report_builder(
        plan_db_path=str(args.plan_db_path or ""),
        ops_db_path=str(args.ops_db_path or ""),
        robot_id=str(args.robot_id or ""),
        strict=bool(args.db_strict),
    )
    stages.append(_stage("revision_db_health", db_report))

    smoke_report = smoke_report_builder(_smoke_args(args))
    stages.append(_stage("runtime_smoke", smoke_report))

    prechecks_ok = all(bool(stage.get("ok", False)) for stage in stages)
    if args.profile != "read_only_gate":
        if prechecks_ok or bool(args.continue_on_precheck_failure):
            revision_report = revision_report_builder(_revision_args(args))
            stages.append(_stage("revision_acceptance", revision_report))
        else:
            stages.append(
                _stage(
                    "revision_acceptance",
                    None,
                    skipped=True,
                    skip_reason="prechecks failed; revision acceptance not executed",
                )
            )

    issues: List[str] = []
    for stage in stages:
        if bool(stage.get("skipped", False)):
            issues.append("%s: %s" % (str(stage.get("name") or ""), str(stage.get("skip_reason") or "skipped")))
            continue
        if bool(stage.get("ok", False)):
            continue
        stage_report = dict(stage.get("report") or {})
        stage_issues = _summary_issues(stage_report)
        if stage_issues:
            issues.extend(["%s: %s" % (str(stage.get("name") or ""), text) for text in stage_issues])
        else:
            issues.append("%s: failed" % str(stage.get("name") or "stage"))

    return {
        "profile": str(args.profile or ""),
        "plan_db_path": str(args.plan_db_path or ""),
        "ops_db_path": str(args.ops_db_path or ""),
        "robot_id": str(args.robot_id or ""),
        "revision_scope": {
            str(stage.get("name") or ""): dict(stage.get("revision_scope") or {})
            for stage in stages
            if not bool(stage.get("skipped", False))
        },
        "stages": stages,
        "summary": {
            "ok": not issues,
            "issues": issues,
        },
    }


def _format_scope_summary(revision_scope: Dict[str, object]) -> str:
    revision_scope = dict(revision_scope or {})
    if "post" in revision_scope and not any(
        key in revision_scope for key in ("active", "runtime", "latest_head", "pending_target")
    ):
        return _format_scope_summary(dict(revision_scope.get("post") or {}))
    active = dict(revision_scope.get("active") or {})
    runtime = dict(revision_scope.get("runtime") or {})
    latest_head = dict(revision_scope.get("latest_head") or {})
    pending_target = dict(revision_scope.get("pending_target") or {})
    if not any((active, runtime, latest_head, pending_target)):
        return ""
    return "active=%s/%s runtime=%s/%s latest_head=%s/%s pending=%s/%s status=%s" % (
        str(active.get("map_name") or "") or "-",
        str(active.get("revision_id") or "") or "-",
        str(runtime.get("map_name") or "") or "-",
        str(runtime.get("revision_id") or "") or "-",
        str(latest_head.get("map_name") or "") or "-",
        str(latest_head.get("revision_id") or "") or "-",
        str(pending_target.get("map_name") or "") or "-",
        str(pending_target.get("revision_id") or "") or "-",
        str(pending_target.get("status") or "") or "-",
    )


def _print_stage_text(stage: Dict[str, object]) -> None:
    name = str(stage.get("name") or "")
    if bool(stage.get("skipped", False)):
        print("- stage %s: SKIP" % name)
        print("  reason: %s" % str(stage.get("skip_reason") or ""))
        return
    print("- stage %s: %s" % (name, "OK" if bool(stage.get("ok", False)) else "FAIL"))
    report = dict(stage.get("report") or {})
    summary = dict(report.get("summary") or {})
    if "error_count" in summary or "warning_count" in summary:
        print(
            "  summary: errors=%s warnings=%s"
            % (
                int(summary.get("error_count", 0) or 0),
                int(summary.get("warning_count", 0) or 0),
            )
        )
    scope_summary = _format_scope_summary(stage.get("revision_scope") or {})
    if scope_summary:
        print("  revision_scope: %s" % scope_summary)
    for issue in _summary_issues(report)[:8]:
        print("  issue: %s" % str(issue))


def _print_text(report: Dict[str, object]) -> None:
    print("Backend production acceptance")
    print("Profile: %s" % str(report.get("profile") or ""))
    print("Plan DB: %s" % str(report.get("plan_db_path") or ""))
    if str(report.get("ops_db_path") or ""):
        print("Ops DB: %s" % str(report.get("ops_db_path") or ""))
    if str(report.get("robot_id") or ""):
        print("Robot: %s" % str(report.get("robot_id") or ""))
    for stage in list(report.get("stages") or []):
        _print_stage_text(stage)
    summary = dict(report.get("summary") or {})
    print("Summary: %s" % ("OK" if bool(summary.get("ok", False)) else "FAIL"))
    for issue in list(summary.get("issues") or []):
        print("- %s" % str(issue))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run backend production acceptance in a fixed order: revision db health -> runtime smoke -> revision acceptance."
    )
    parser.add_argument("--profile", required=True, choices=SUPPORTED_PROFILES)
    parser.add_argument("--plan-db-path", required=True, help="planning.db path")
    parser.add_argument("--ops-db-path", default="", help="operations.db path")
    parser.add_argument("--task-id", type=int, default=0)
    parser.add_argument("--service-timeout", type=float, default=10.0)
    parser.add_argument("--job-timeout", type=float, default=120.0)
    parser.add_argument("--smoke-job-timeout", type=float, default=None)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--robot-id", default="local_robot")
    parser.add_argument("--run-task-cycle", action="store_true")
    parser.add_argument("--task-timeout", type=float, default=300.0)
    parser.add_argument("--map-name", default="")
    parser.add_argument("--map-revision-id", default="")
    parser.add_argument("--save-map-name", default="")
    parser.add_argument("--frame-id", default="map")
    parser.add_argument("--description-prefix", default="production_gate")
    parser.add_argument("--allow-write-actions", action="store_true")
    parser.add_argument("--continue-on-precheck-failure", action="store_true")
    parser.add_argument("--db-strict", action="store_true")
    parser.add_argument(
        "--ignore-warning",
        action="append",
        default=["station_status stale or missing"],
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--text", action="store_true")
    return parser


def validate_args(args) -> None:
    if bool(getattr(args, "run_task_cycle", False)):
        if int(args.task_id or 0) <= 0:
            raise ValueError("--task-id must be > 0 when --run-task-cycle is enabled")
        if not str(args.ops_db_path or "").strip():
            raise ValueError("--ops-db-path is required when --run-task-cycle is enabled")
    if args.profile in WRITE_PROFILES and not bool(args.allow_write_actions):
        raise ValueError("--allow-write-actions is required for profile %s" % str(args.profile))
    if args.profile in {
        "verify_revision_gate",
        "activate_revision_gate",
        "activate_revision_prepare_for_task_gate",
    }:
        if not (str(args.map_name or "").strip() or str(args.map_revision_id or "").strip()):
            raise ValueError("--map-name or --map-revision-id is required for profile %s" % str(args.profile))
    if args.profile in {
        "candidate_save_gate",
        "revision_cycle_gate",
        "revision_cycle_prepare_for_task_gate",
    } and not str(args.save_map_name or "").strip():
        raise ValueError("--save-map-name is required for profile %s" % str(args.profile))


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        validate_args(args)
        report = build_report(args)
    except Exception as exc:
        if args.json:
            json.dump({"summary": {"ok": False, "issues": [str(exc)]}}, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        else:
            print("Backend production acceptance")
            print("Summary: FAIL")
            print("- %s" % str(exc))
        return 1

    if args.json:
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    if args.text or not args.json:
        _print_text(report)
    return 0 if bool((report.get("summary") or {}).get("ok", False)) else 2


if __name__ == "__main__":
    raise SystemExit(main())
