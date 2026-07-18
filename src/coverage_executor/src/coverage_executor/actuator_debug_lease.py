# -*- coding: utf-8 -*-
"""Pure safety policy and wall-clock lease helpers for actuator debugging.

This module deliberately has no ROS imports so the safety decisions can be
unit-tested without a running ROS master.
"""

from dataclasses import dataclass
import math
from typing import Optional


@dataclass(frozen=True)
class ActuatorDebugSafetyLimits:
    max_linear_mps: float = 0.03
    max_angular_rps: float = 0.05
    odom_stale_s: float = 1.0
    telemetry_stale_s: float = 2.0
    safety_status_stale_s: float = 2.0
    require_authoritative_safety_status: bool = False


@dataclass(frozen=True)
class ActuatorDebugSafetySnapshot:
    executor_state: str
    run_thread_active: bool
    linear_speed_mps: float
    angular_speed_rps: float
    odom_age_s: float
    mcore_connected_seen: bool
    mcore_connected: bool
    telemetry_seen: bool
    telemetry_age_s: float
    telemetry_generation: int
    safety_status_seen: bool
    safety_status_age_s: float
    safety_status_generation: int
    emergency_stop_active: bool


def _finite_abs(value: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.inf
    if not math.isfinite(number):
        return math.inf
    return abs(number)


def runtime_safety_violation(
    snapshot: ActuatorDebugSafetySnapshot,
    limits: ActuatorDebugSafetyLimits,
) -> Optional[str]:
    """Return the first fail-closed runtime safety violation, if any."""
    if not bool(snapshot.mcore_connected_seen):
        return "M-core connection state unavailable"
    if not bool(snapshot.mcore_connected):
        return "M-core disconnected"
    if bool(snapshot.emergency_stop_active):
        return "physical emergency stop active"
    if _finite_abs(snapshot.odom_age_s) > float(limits.odom_stale_s):
        return "odometry unavailable or stale"
    linear_speed = _finite_abs(snapshot.linear_speed_mps)
    angular_speed = _finite_abs(snapshot.angular_speed_rps)
    if linear_speed > float(limits.max_linear_mps) or angular_speed > float(limits.max_angular_rps):
        return (
            "chassis moving: linear=%.3fm/s angular=%.3frad/s limits=(%.3f,%.3f)"
            % (
                linear_speed,
                angular_speed,
                float(limits.max_linear_mps),
                float(limits.max_angular_rps),
            )
        )
    if not bool(snapshot.telemetry_seen):
        return "M-core telemetry unavailable"
    if _finite_abs(snapshot.telemetry_age_s) > float(limits.telemetry_stale_s):
        return "M-core telemetry unavailable or stale"
    if not bool(snapshot.safety_status_seen) and bool(
        limits.require_authoritative_safety_status
    ):
        return "M-core safety status unavailable"
    if bool(snapshot.safety_status_seen) and _finite_abs(
        snapshot.safety_status_age_s
    ) > float(limits.safety_status_stale_s):
        return "M-core safety status unavailable or stale"
    return None


def entry_safety_violation(
    snapshot: ActuatorDebugSafetySnapshot,
    limits: ActuatorDebugSafetyLimits,
) -> Optional[str]:
    """Return why a new debug lease cannot start, or ``None`` when safe."""
    if str(snapshot.executor_state or "").strip().upper() != "IDLE":
        return "executor must be IDLE"
    if bool(snapshot.run_thread_active):
        return "executor run thread is active"
    return runtime_safety_violation(snapshot, limits)


class WallClockLease:
    """Small monotonic/wall-clock agnostic fixed-duration lease state."""

    def __init__(self, duration_s: float):
        self.duration_s = max(1.0, float(duration_s))
        self.active = False
        self.deadline_s = 0.0

    def enable_or_renew(self, now_s: float) -> bool:
        """Enable/renew and return ``True`` when this was a renewal."""
        renewed = bool(self.active)
        self.active = True
        self.deadline_s = float(now_s) + self.duration_s
        return renewed

    def disable(self):
        self.active = False
        self.deadline_s = 0.0

    def expired(self, now_s: float) -> bool:
        return bool(self.active and float(now_s) >= float(self.deadline_s))

    def remaining_s(self, now_s: float) -> float:
        if not self.active:
            return 0.0
        return max(0.0, float(self.deadline_s) - float(now_s))
