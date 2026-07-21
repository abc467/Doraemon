#!/usr/bin/env python3
"""Fail-closed stability gate for the commercial Orbbec SDK snapshot."""

import argparse
import math
import os
import re
import subprocess
import sys
import time
from typing import Callable, NamedTuple, Optional, Sequence, Tuple


MACHINE_PREFIX = "DORAEMON_ORBBEC_DEVICE_V1|"
MAX_ATTEMPT_SECONDS = 15.0
RETRY_INTERVAL_SECONDS = 1.0
REQUIRED_CONSECUTIVE_SNAPSHOTS = 2
MAX_TOTAL_TIMEOUT_SECONDS = 600.0
SERIAL_PATTERN = r"[A-Za-z0-9_./:@,+-]{1,128}"
TOPOLOGY_PATTERN = r"[0-9]+-[0-9]+(?:[.][0-9]+)*"
EXPECTED_PAIR_RE = re.compile(r"^(%s)\|(%s)$" % (SERIAL_PATTERN, TOPOLOGY_PATTERN))
MACHINE_RECORD_RE = re.compile(
    r"^%s(%s)\|(%s)$"
    % (re.escape(MACHINE_PREFIX), SERIAL_PATTERN, TOPOLOGY_PATTERN)
)


class AttemptResult(NamedTuple):
    returncode: int
    stdout: str
    stderr: str


class SnapshotResult(NamedTuple):
    valid: bool
    status: str
    pairs: Tuple[str, ...]


class GateResult(NamedTuple):
    success: bool
    attempts: int
    consecutive: int
    last_status: str
    last_returncode: int
    observed: Tuple[str, ...]
    missing: Tuple[str, ...]
    unexpected: Tuple[str, ...]


def validate_expected_pairs(expected_pairs: Sequence[str]) -> Tuple[str, ...]:
    if len(expected_pairs) != 3:
        raise ValueError("exactly three expected pairs are required")

    normalized = []
    serials = []
    topologies = []
    for pair in expected_pairs:
        match = EXPECTED_PAIR_RE.fullmatch(pair)
        if match is None or len(pair) > 257:
            raise ValueError("an expected pair is malformed")
        serial, topology = match.groups()
        normalized.append("%s|%s" % (serial, topology))
        serials.append(serial)
        topologies.append(topology)

    if len(set(normalized)) != 3:
        raise ValueError("expected pairs must be unique")
    if len(set(serials)) != 3 or len(set(topologies)) != 3:
        raise ValueError("expected serials and topologies must each be unique")
    return tuple(sorted(normalized))


def parse_machine_snapshot(output: str) -> SnapshotResult:
    if not isinstance(output, str) or not output:
        return SnapshotResult(False, "empty-output", ())

    lines = output.splitlines()
    if not lines:
        return SnapshotResult(False, "empty-output", ())

    pairs = []
    serials = []
    topologies = []
    for line in lines:
        if len(line) > 300:
            return SnapshotResult(False, "malformed-output", ())
        match = MACHINE_RECORD_RE.fullmatch(line)
        if match is None:
            return SnapshotResult(False, "malformed-output", ())
        serial, topology = match.groups()
        pair = "%s|%s" % (serial, topology)
        pairs.append(pair)
        serials.append(serial)
        topologies.append(topology)

    normalized = tuple(sorted(pairs))
    if len(pairs) < 3:
        return SnapshotResult(False, "partial-output", normalized)
    if len(pairs) > 3:
        return SnapshotResult(False, "extra-output", normalized)
    if len(set(pairs)) != len(pairs):
        return SnapshotResult(False, "duplicate-pair", normalized)
    if len(set(serials)) != len(serials):
        return SnapshotResult(False, "duplicate-serial", normalized)
    if len(set(topologies)) != len(topologies):
        return SnapshotResult(False, "duplicate-topology", normalized)
    return SnapshotResult(True, "exact-shape", normalized)


def run_enumerator(binary: str, attempt_seconds: float) -> AttemptResult:
    try:
        completed = subprocess.run(
            [binary],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=attempt_seconds,
            check=False,
        )
        return AttemptResult(completed.returncode, completed.stdout, completed.stderr)
    except subprocess.TimeoutExpired:
        # subprocess.run has already killed and reaped the enumerator. Discard
        # any partial output carried by the exception.
        return AttemptResult(124, "", "")
    except OSError:
        return AttemptResult(127, "", "")


def _format_pairs(pairs: Sequence[str]) -> str:
    return ",".join(pairs) if pairs else "<none>"


def verify_stable_pairs(
    binary: str,
    expected_pairs: Sequence[str],
    timeout_seconds: float,
    runner: Callable[[str, float], AttemptResult] = run_enumerator,
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    logger: Optional[Callable[[str], None]] = print,
) -> GateResult:
    expected = validate_expected_pairs(expected_pairs)
    if (
        not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
        or timeout_seconds > MAX_TOTAL_TIMEOUT_SECONDS
    ):
        raise ValueError("timeout must be within the commercial gate bounds")

    deadline = monotonic() + timeout_seconds
    attempts = 0
    consecutive = 0
    last_status = "deadline-exhausted"
    last_returncode = 0
    observed: Tuple[str, ...] = ()

    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        attempt_seconds = min(MAX_ATTEMPT_SECONDS, remaining)
        attempts += 1

        try:
            attempt = runner(binary, attempt_seconds)
            last_returncode = int(attempt.returncode)
        except subprocess.TimeoutExpired:
            attempt = AttemptResult(124, "", "")
            last_returncode = 124
        except OSError:
            attempt = AttemptResult(127, "", "")
            last_returncode = 127
        except subprocess.SubprocessError:
            attempt = AttemptResult(125, "", "")
            last_returncode = 125

        if monotonic() > deadline:
            consecutive = 0
            observed = ()
            last_status = "global-deadline-exceeded"
        elif last_returncode != 0:
            consecutive = 0
            observed = ()
            last_status = (
                "attempt-timeout"
                if last_returncode in (124, 137)
                else "enumerator-nonzero"
            )
        else:
            snapshot = parse_machine_snapshot(attempt.stdout)
            observed = snapshot.pairs
            if snapshot.valid and observed == expected:
                consecutive += 1
                last_status = "exact-match"
                if logger is not None:
                    logger(
                        "[INFO] Orbbec SDK exact snapshot attempt=%d consecutive=%d/%d observed=%s"
                        % (
                            attempts,
                            consecutive,
                            REQUIRED_CONSECUTIVE_SNAPSHOTS,
                            _format_pairs(observed),
                        )
                    )
                if consecutive >= REQUIRED_CONSECUTIVE_SNAPSHOTS:
                    if logger is not None:
                        logger(
                            "[OK] Orbbec SDK stable serial/topology pairs attempts=%d consecutive=%d observed=%s"
                            % (attempts, consecutive, _format_pairs(observed))
                        )
                    return GateResult(
                        True,
                        attempts,
                        consecutive,
                        last_status,
                        last_returncode,
                        observed,
                        (),
                        (),
                    )
            else:
                consecutive = 0
                last_status = (
                    "pair-set-mismatch" if snapshot.valid else snapshot.status
                )

        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        sleeper(min(RETRY_INTERVAL_SECONDS, remaining))

    missing = tuple(sorted(set(expected) - set(observed)))
    unexpected = tuple(sorted(set(observed) - set(expected)))
    if logger is not None:
        logger(
            "[ERROR] Orbbec SDK stable pair gate failed attempts=%d consecutive=%d/%d last_status=%s last_rc=%d expected=%s observed=%s missing=%s unexpected=%s"
            % (
                attempts,
                consecutive,
                REQUIRED_CONSECUTIVE_SNAPSHOTS,
                last_status,
                last_returncode,
                _format_pairs(expected),
                _format_pairs(observed),
                _format_pairs(missing),
                _format_pairs(unexpected),
            )
        )
    return GateResult(
        False,
        attempts,
        consecutive,
        last_status,
        last_returncode,
        observed,
        missing,
        unexpected,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Require two consecutive exact Orbbec SDK identity snapshots"
    )
    parser.add_argument("--binary", required=True)
    parser.add_argument("--timeout-seconds", required=True, type=float)
    parser.add_argument("--expected", action="append", required=True)
    args = parser.parse_args(argv)

    if not os.path.isabs(args.binary) or not os.path.isfile(args.binary):
        print("[ERROR] fixed Orbbec enumerator is unavailable", file=sys.stderr)
        return 1
    if not os.access(args.binary, os.X_OK):
        print("[ERROR] fixed Orbbec enumerator is not executable", file=sys.stderr)
        return 1

    try:
        result = verify_stable_pairs(
            args.binary,
            args.expected,
            args.timeout_seconds,
        )
    except ValueError as error:
        print("[ERROR] invalid Orbbec SDK pair-gate configuration: %s" % error)
        return 1
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
