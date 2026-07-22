#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import contextlib
import io
import os
import sys
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
TOOLS_DIR = os.path.join(PKG_DIR, "tools")

if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

import run_backend_production_acceptance as production


def _ok_report(*_args, **_kwargs):
    return {"summary": {"ok": True, "issues": []}, "revision_scope": {}}


class BackendProductionAcceptanceIntegrationTest(unittest.TestCase):
    def test_all_profiles_require_the_fixed_commercial_databases(self):
        for option, path in (
            ("--plan-db-path", "/tmp/planning.db"),
            ("--ops-db-path", "/tmp/operations.db"),
        ):
            with self.subTest(option=option):
                argv = [
                    "--profile",
                    "read_only_gate",
                    "--plan-db-path",
                    "/data/coverage/planning.db",
                    "--ops-db-path",
                    "/data/coverage/operations.db",
                    "--robot-id",
                    "CR-001",
                ]
                argv[argv.index(option) + 1] = path
                args = production.build_arg_parser().parse_args(argv)
                with self.assertRaisesRegex(ValueError, "is fixed at"):
                    production.validate_args(args)

    def _write_args(self):
        return production.build_arg_parser().parse_args(
            [
                "--profile",
                "activate_revision_gate",
                "--plan-db-path",
                "/data/coverage/planning.db",
                "--ops-db-path",
                "/data/coverage/operations.db",
                "--robot-id",
                "CR-001",
                "--map-name",
                "site_a",
                "--map-revision-id",
                "rev_site_a_01",
                "--allow-write-actions",
            ]
        )

    def test_mapping_profiles_are_hard_rejected_for_programmatic_callers(self):
        for profile in sorted(production.FORBIDDEN_MAPPING_PROFILES):
            with self.subTest(profile=profile):
                args = self._write_args()
                args.profile = profile
                with self.assertRaisesRegex(
                    ValueError,
                    "run_revision_workflow_acceptance.py checkpoint v2 pause/resume",
                ):
                    production.build_report(
                        args,
                        db_report_builder=_ok_report,
                        smoke_report_builder=_ok_report,
                        revision_report_builder=_ok_report,
                    )

    def test_continue_on_precheck_failure_cli_option_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                production.build_arg_parser().parse_args(
                    [
                        "--profile",
                        "read_only_gate",
                        "--plan-db-path",
                        "/data/coverage/planning.db",
                        "--ops-db-path",
                        "/data/coverage/operations.db",
                        "--robot-id",
                        "CR-001",
                        "--continue-on-precheck-failure",
                    ]
                )

    def test_task_cycle_is_rejected_before_any_precheck(self):
        args = self._write_args()
        args.run_task_cycle = True
        db_builder = mock.Mock(return_value=_ok_report())
        smoke_builder = mock.Mock(return_value=_ok_report())
        revision_builder = mock.Mock(return_value=_ok_report())

        with self.assertRaisesRegex(
            ValueError,
            "runtime prechecks must remain read-only",
        ):
            production.build_report(
                args,
                db_report_builder=db_builder,
                smoke_report_builder=smoke_builder,
                revision_report_builder=revision_builder,
            )

        db_builder.assert_not_called()
        smoke_builder.assert_not_called()
        revision_builder.assert_not_called()

    def test_prepare_profile_requires_positive_task_id_before_any_precheck(self):
        args = self._write_args()
        args.profile = "activate_revision_prepare_for_task_gate"
        args.task_id = 0
        db_builder = mock.Mock(return_value=_ok_report())
        smoke_builder = mock.Mock(return_value=_ok_report())
        revision_builder = mock.Mock(return_value=_ok_report())

        with self.assertRaisesRegex(ValueError, "--task-id must be > 0"):
            production.build_report(
                args,
                db_report_builder=db_builder,
                smoke_report_builder=smoke_builder,
                revision_report_builder=revision_builder,
            )

        db_builder.assert_not_called()
        smoke_builder.assert_not_called()
        revision_builder.assert_not_called()

    def test_write_profile_runtime_precheck_never_targets_a_task(self):
        args = self._write_args()
        args.profile = "activate_revision_prepare_for_task_gate"
        args.task_id = 23
        observed = {}

        def run_smoke(smoke_args):
            observed["smoke_task_id"] = smoke_args.task_id
            return _ok_report()

        def run_revision(revision_args):
            observed["revision_task_id"] = revision_args.task_id
            return _ok_report()

        report = production.build_report(
            args,
            db_report_builder=_ok_report,
            smoke_report_builder=run_smoke,
            revision_report_builder=run_revision,
        )

        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])
        self.assertEqual(observed["smoke_task_id"], 0)
        self.assertEqual(observed["revision_task_id"], 23)

    def test_read_only_profile_runtime_precheck_keeps_requested_task(self):
        args = self._write_args()
        args.profile = "read_only_gate"
        args.task_id = 23
        observed = {}

        def run_smoke(smoke_args):
            observed["smoke_task_id"] = smoke_args.task_id
            return _ok_report()

        report = production.build_report(
            args,
            db_report_builder=_ok_report,
            smoke_report_builder=run_smoke,
            revision_report_builder=mock.Mock(),
        )

        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])
        self.assertEqual(observed["smoke_task_id"], 23)

    def test_save_map_name_is_rejected_before_any_precheck(self):
        args = self._write_args()
        args.save_map_name = "site_a"
        db_builder = mock.Mock(return_value=_ok_report())

        with self.assertRaisesRegex(ValueError, "--save-map-name is prohibited"):
            production.build_report(
                args,
                db_report_builder=db_builder,
                smoke_report_builder=_ok_report,
                revision_report_builder=_ok_report,
            )

        db_builder.assert_not_called()

    def test_write_profile_never_runs_when_either_precheck_fails(self):
        failed = {"summary": {"ok": False, "issues": ["precheck failed"]}}
        for failed_stage in ("database", "runtime"):
            with self.subTest(failed_stage=failed_stage):
                args = self._write_args()
                # A stale programmatic caller cannot restore the deleted
                # fail-open CLI behavior.
                args.continue_on_precheck_failure = True
                revision_builder = mock.Mock(return_value=_ok_report())
                report = production.build_report(
                    args,
                    db_report_builder=(
                        (lambda **_kwargs: failed)
                        if failed_stage == "database"
                        else _ok_report
                    ),
                    smoke_report_builder=(
                        (lambda _args: failed)
                        if failed_stage == "runtime"
                        else _ok_report
                    ),
                    revision_report_builder=revision_builder,
                )

                revision_builder.assert_not_called()
                revision_stage = report["stages"][-1]
                self.assertEqual(revision_stage["name"], "revision_acceptance")
                self.assertTrue(revision_stage["skipped"])
                self.assertFalse(report["summary"]["ok"])

    def test_write_profile_runs_only_after_both_prechecks_pass(self):
        args = self._write_args()
        db_builder = mock.Mock(return_value=_ok_report())
        smoke_builder = mock.Mock(return_value=_ok_report())
        revision_builder = mock.Mock(return_value=_ok_report())
        report = production.build_report(
            args,
            db_report_builder=db_builder,
            smoke_report_builder=smoke_builder,
            revision_report_builder=revision_builder,
        )

        self.assertTrue(db_builder.call_args.kwargs["strict"])
        smoke_builder.assert_called_once()
        self.assertFalse(smoke_builder.call_args.args[0].run_task_cycle)
        revision_builder.assert_called_once()
        revision_args = revision_builder.call_args.args[0]
        self.assertEqual(revision_args.profile, "activate_revision")
        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])

    def test_revision_args_never_enable_mapping_checkpoint_bypass(self):
        args = self._write_args()
        observed = {}

        def run_mapping(revision_args):
            observed.update(
                pause_after_start_mapping=revision_args.pause_after_start_mapping,
                checkpoint_path=revision_args.checkpoint_path,
                resume_from_checkpoint=revision_args.resume_from_checkpoint,
            )
            return _ok_report()

        report = production.build_report(
            args,
            db_report_builder=_ok_report,
            smoke_report_builder=_ok_report,
            revision_report_builder=run_mapping,
        )

        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])
        self.assertEqual(
            observed,
            {
                "pause_after_start_mapping": False,
                "checkpoint_path": "",
                "resume_from_checkpoint": "",
            },
        )


if __name__ == "__main__":
    unittest.main()
