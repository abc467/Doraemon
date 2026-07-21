#!/usr/bin/env python3

import importlib.util
import os
import subprocess
import unittest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
GATE_PATH = os.path.join(REPO_ROOT, "scripts", "verify_orbbec_sdk_pairs.py")
SPEC = importlib.util.spec_from_file_location("verify_orbbec_sdk_pairs", GATE_PATH)
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)

EXPECTED = ("LEFT|4-2.4.2", "RIGHT|4-2.1.2", "FRONT|2-3")


def snapshot(*pairs):
    return "".join(
        "%s%s\n" % (GATE.MACHINE_PREFIX, pair)
        for pair in pairs
    )


FULL_A = snapshot(EXPECTED[0], EXPECTED[1], EXPECTED[2])
FULL_B = snapshot(EXPECTED[2], EXPECTED[0], EXPECTED[1])


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, duration):
        self.value += duration


class SequenceRunner:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def __call__(self, binary, timeout_seconds):
        self.calls.append((binary, timeout_seconds))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def attempt(stdout, returncode=0, stderr=""):
    return GATE.AttemptResult(returncode, stdout, stderr)


def run_gate(outcomes, timeout_seconds=10.0):
    clock = FakeClock()
    runner = SequenceRunner(outcomes)
    logs = []
    result = GATE.verify_stable_pairs(
        "/fixed/release/devel/lib/orbbec_camera/list_devices_node",
        EXPECTED,
        timeout_seconds,
        runner=runner,
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
        logger=logs.append,
    )
    return result, runner, logs


class OrbbecSdkPairGateTest(unittest.TestCase):
    def test_partial_then_two_exact_snapshots_succeeds(self):
        result, runner, _ = run_gate(
            [attempt(snapshot(EXPECTED[0], EXPECTED[1])), attempt(FULL_A), attempt(FULL_B)]
        )
        self.assertTrue(result.success)
        self.assertEqual(3, len(runner.calls))
        self.assertEqual(2, result.consecutive)

    def test_order_is_irrelevant_but_exact_set_is_required(self):
        result, runner, _ = run_gate([attempt(FULL_B), attempt(FULL_A)])
        self.assertTrue(result.success)
        self.assertEqual(2, len(runner.calls))

    def test_bad_snapshot_resets_consecutive_count(self):
        bad_snapshots = {
            "partial": snapshot(EXPECTED[0], EXPECTED[1]),
            "extra": snapshot(*EXPECTED, "EXTRA|9-9"),
            "duplicate": snapshot(EXPECTED[0], EXPECTED[1], EXPECTED[1]),
            "duplicate_serial": snapshot(EXPECTED[0], EXPECTED[1], "LEFT|9-9"),
            "duplicate_topology": snapshot(EXPECTED[0], EXPECTED[1], "OTHER|4-2.4.2"),
            "wrong_exact_set": snapshot(EXPECTED[0], EXPECTED[1], "OTHER|9-9"),
            "malformed": GATE.MACHINE_PREFIX + "LEFT|4-2|extra\n",
            "legacy": "[INFO] serial: LEFT\n[INFO] port id : 4-2.4.2\n",
        }
        for label, bad_output in bad_snapshots.items():
            with self.subTest(label=label):
                result, runner, _ = run_gate(
                    [attempt(FULL_A), attempt(bad_output), attempt(FULL_B), attempt(FULL_A)]
                )
                self.assertTrue(result.success)
                self.assertEqual(4, len(runner.calls))

    def test_nonzero_exit_with_full_output_resets_streak(self):
        result, runner, _ = run_gate(
            [attempt(FULL_A), attempt(FULL_B, returncode=2), attempt(FULL_A), attempt(FULL_B)]
        )
        self.assertTrue(result.success)
        self.assertEqual(4, len(runner.calls))

    def test_process_timeout_resets_streak(self):
        process_timeout = subprocess.TimeoutExpired("enumerator", 1, stderr="RAW_SECRET_TOKEN")
        result, runner, logs = run_gate(
            [attempt(FULL_A), process_timeout, attempt(FULL_B), attempt(FULL_A)]
        )
        self.assertTrue(result.success)
        self.assertEqual(4, len(runner.calls))
        self.assertNotIn("RAW_SECRET_TOKEN", "\n".join(logs))

    def test_deadline_fails_closed_and_caps_each_attempt(self):
        partial = attempt(snapshot(EXPECTED[0]))
        result, runner, logs = run_gate([partial, partial], timeout_seconds=2.0)
        self.assertFalse(result.success)
        self.assertEqual(2, len(runner.calls))
        self.assertTrue(all(0 < budget <= 2.0 for _, budget in runner.calls))
        self.assertIn("last_status=partial-output", logs[-1])

    def test_single_exact_snapshot_before_deadline_is_not_enough(self):
        result, runner, _ = run_gate([attempt(FULL_A)], timeout_seconds=1.0)
        self.assertFalse(result.success)
        self.assertEqual(1, len(runner.calls))
        self.assertEqual(1, result.consecutive)

    def test_snapshot_returned_after_global_deadline_is_rejected(self):
        clock = FakeClock()
        logs = []

        def slow_runner(_binary, _attempt_seconds):
            clock.value += 1.1
            return attempt(FULL_A)

        result = GATE.verify_stable_pairs(
            "/fixed/enumerator",
            EXPECTED,
            1.0,
            runner=slow_runner,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
            logger=logs.append,
        )
        self.assertFalse(result.success)
        self.assertEqual("global-deadline-exceeded", result.last_status)
        self.assertNotIn("exact snapshot", "\n".join(logs))

    def test_snapshot_rejects_unsafe_or_ambiguous_records(self):
        invalid_outputs = (
            "",
            snapshot(*EXPECTED, "EXTRA|9-9"),
            snapshot(EXPECTED[0], EXPECTED[1], EXPECTED[1]),
            snapshot(EXPECTED[0], EXPECTED[1], "LEFT|9-9"),
            snapshot(EXPECTED[0], EXPECTED[1], "OTHER|4-2.4.2"),
            GATE.MACHINE_PREFIX + "|4-2\n",
            GATE.MACHINE_PREFIX + "LEFT|\n",
            GATE.MACHINE_PREFIX + "LEFT|4-2|extra\n",
            GATE.MACHINE_PREFIX + "LEFT SPACE|4-2\n",
            GATE.MACHINE_PREFIX + "LEFT|4-2\x00\n",
            "noise before " + GATE.MACHINE_PREFIX + "LEFT|4-2\n",
            FULL_A + "\n" + FULL_B,
        )
        for output in invalid_outputs:
            with self.subTest(output=repr(output)):
                self.assertFalse(GATE.parse_machine_snapshot(output).valid)

    def test_raw_child_output_is_never_reported_on_nonzero_exit(self):
        secret = "RAW_SECRET_TOKEN"
        failed = attempt(FULL_A + secret, returncode=9, stderr=secret)
        result, _, logs = run_gate([failed, failed], timeout_seconds=2.0)
        self.assertFalse(result.success)
        rendered = "\n".join(logs)
        self.assertNotIn(secret, rendered)
        self.assertIn("last_status=enumerator-nonzero", rendered)
        self.assertIn("last_rc=9", rendered)

    def test_per_attempt_budget_uses_fifteen_second_cap(self):
        result, runner, _ = run_gate([attempt(FULL_A), attempt(FULL_B)], timeout_seconds=20.0)
        self.assertTrue(result.success)
        self.assertEqual(15.0, runner.calls[0][1])
        self.assertLessEqual(runner.calls[1][1], 15.0)

    def test_nonfinite_timeout_is_rejected_before_enumeration(self):
        for timeout_seconds in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(timeout_seconds=timeout_seconds):
                with self.assertRaises(ValueError):
                    GATE.verify_stable_pairs(
                        "/fixed/enumerator",
                        EXPECTED,
                        timeout_seconds,
                        logger=None,
                    )

    def test_producer_and_boot_script_enforce_versioned_fixed_contract(self):
        with open(
            os.path.join(REPO_ROOT, "src", "orbbec-ros-sdk", "src", "list_devices_node.cpp"),
            "r",
            encoding="utf-8",
        ) as handle:
            producer = handle.read()
        with open(
            os.path.join(REPO_ROOT, "scripts", "wait_robot_boot_ready.sh"),
            "r",
            encoding="utf-8",
        ) as handle:
            boot_gate = handle.read()

        self.assertIn("DORAEMON_ORBBEC_DEVICE_V1|", producer)
        self.assertNotIn('ROS_INFO_STREAM("serial:', producer)
        self.assertNotIn('ROS_INFO_STREAM("port id', producer)
        self.assertIn("return kSdkErrorExitCode", producer)
        self.assertIn("return kStandardErrorExitCode", producer)
        self.assertIn("return kUnknownErrorExitCode", producer)
        self.assertGreater(
            producer.index("std::cout << kMachineRecordPrefix"),
            producer.index("device_records.emplace_back"),
        )
        self.assertIn('python3 "${SCRIPT_DIR}/verify_orbbec_sdk_pairs.py"', boot_gate)
        self.assertIn("orbbec_remaining_sec=$((TIMEOUT_SEC - $(elapsed_sec)))", boot_gate)
        self.assertNotIn("DORAEMON_ORBBEC_LIST_DEVICES_BINARY", boot_gate)
        self.assertNotIn('"${orbbec_binary}" 2>&1', boot_gate)


if __name__ == "__main__":
    unittest.main()
