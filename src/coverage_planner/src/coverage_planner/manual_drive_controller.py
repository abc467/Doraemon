# -*- coding: utf-8 -*-

"""Safety and command helpers for frontend manual-drive controls."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple


SUPPORTED_DIFF_DIRECTIONS = ("forward", "backward", "turn_left", "turn_right")
IDLE_STATES = frozenset({"", "IDLE", "DONE", "FAILED", "CANCELED", "CANCELLED", "STOPPED", "ABORTED"})
PENDING_SWITCH_IDLE_STATES = frozenset({"", "DONE", "SUCCESS", "SUCCEEDED", "FAILED", "CANCELED", "CANCELLED"})


@dataclass
class ManualDriveConfig:
    enabled: bool = True
    cmd_vel_topic: str = "/cmd_vel"
    linear_mps_limit: float = 0.3
    angular_radps_limit: float = 0.5
    default_linear_mps: float = 0.12
    default_angular_radps: float = 0.35
    watchdog_timeout_ms: int = 1000
    min_duration_ms: int = 100
    publish_hz: float = 20.0
    require_role: bool = False
    allowed_roles: Tuple[str, ...] = ("operator", "service", "engineer", "admin")
    allowed_capabilities: Tuple[str, ...] = ("manual_drive", "manual-drive", "robot:manual_drive")
    require_slam_state: bool = False
    require_task_state: bool = False
    require_odometry_state: bool = False
    require_combined_status: bool = False
    slam_state_stale_timeout_s: float = 2.0
    task_state_stale_timeout_s: float = 2.0
    odometry_state_stale_timeout_s: float = 2.0
    combined_status_stale_timeout_s: float = 2.0
    supports_strafe: bool = False


def _as_bool(value) -> bool:
    return bool(value)


def _text(value) -> str:
    return str(value or "").strip()


def _upper(value) -> str:
    return _text(value).upper()


def _fresh(ts: float, *, now: float, timeout_s: float) -> bool:
    try:
        stamp = float(ts or 0.0)
        timeout = float(timeout_s)
    except Exception:
        return False
    return bool(stamp > 0.0 and (float(now) - stamp) <= max(0.0, timeout))


def _append_unique(items: List[str], text: str):
    value = _text(text)
    if value and value not in items:
        items.append(value)


def _finite_positive(value: float, default: float) -> float:
    try:
        normalized = abs(float(value))
    except Exception:
        normalized = 0.0
    if not math.isfinite(normalized) or normalized <= 0.0:
        return float(default)
    return normalized


def _clamp_abs(value: float, limit: float) -> float:
    try:
        normalized = float(value)
    except Exception:
        normalized = 0.0
    try:
        max_abs = abs(float(limit))
    except Exception:
        max_abs = 0.0
    if not math.isfinite(normalized):
        normalized = 0.0
    return max(-max_abs, min(max_abs, normalized))


class ManualDriveSafetyController:
    def __init__(self, config: ManualDriveConfig):
        self.config = config

    @property
    def supported_directions(self) -> Tuple[str, ...]:
        if bool(self.config.supports_strafe):
            return SUPPORTED_DIFF_DIRECTIONS + ("strafe_left", "strafe_right")
        return SUPPORTED_DIFF_DIRECTIONS

    def requested_duration_ms(self, value: int) -> int:
        try:
            requested = int(value or 0)
        except Exception:
            requested = 0
        if requested <= 0:
            requested = int(self.config.watchdog_timeout_ms)
        return max(
            int(self.config.min_duration_ms),
            min(int(self.config.watchdog_timeout_ms), requested),
        )

    def caller_allowed(self, *, caller_role: str = "", caller_capabilities: Optional[Iterable[str]] = None) -> bool:
        role = _text(caller_role).lower()
        caps = {_text(item).lower() for item in list(caller_capabilities or []) if _text(item)}
        allowed_roles = {_text(item).lower() for item in self.config.allowed_roles if _text(item)}
        allowed_caps = {_text(item).lower() for item in self.config.allowed_capabilities if _text(item)}
        if role and role in allowed_roles:
            return True
        if caps.intersection(allowed_caps):
            return True
        return not bool(self.config.require_role)

    def role_blockers(self, *, caller_role: str = "", caller_capabilities: Optional[Iterable[str]] = None) -> List[str]:
        if self.caller_allowed(caller_role=caller_role, caller_capabilities=caller_capabilities):
            return []
        role = _text(caller_role) or "missing"
        return ["manual drive is not permitted for caller role=%s" % role]

    def safety_blockers(
        self,
        *,
        now: float,
        slam_state=None,
        slam_state_ts: float = 0.0,
        task_state=None,
        task_state_ts: float = 0.0,
        odometry_state=None,
        odometry_state_ts: float = 0.0,
        combined_status=None,
        combined_status_ts: float = 0.0,
        caller_role: str = "",
        caller_capabilities: Optional[Iterable[str]] = None,
    ) -> List[str]:
        blockers: List[str] = []
        if not bool(self.config.enabled):
            blockers.append("manual drive is disabled")
        for item in self.role_blockers(caller_role=caller_role, caller_capabilities=caller_capabilities):
            _append_unique(blockers, item)
        if bool(self.config.require_task_state):
            self._append_task_blockers(blockers, task_state=task_state, task_state_ts=task_state_ts, now=now)
        if bool(self.config.require_slam_state):
            self._append_slam_blockers(blockers, slam_state=slam_state, slam_state_ts=slam_state_ts, now=now)
        if bool(self.config.require_odometry_state):
            self._append_odometry_blockers(
                blockers,
                odometry_state=odometry_state,
                odometry_state_ts=odometry_state_ts,
                now=now,
            )
        if bool(self.config.require_combined_status):
            self._append_platform_blockers(
                blockers,
                combined_status=combined_status,
                combined_status_ts=combined_status_ts,
                now=now,
            )
        return blockers

    def _append_task_blockers(self, blockers: List[str], *, task_state, task_state_ts: float, now: float):
        if task_state is None or not _fresh(
            task_state_ts,
            now=now,
            timeout_s=float(self.config.task_state_stale_timeout_s),
        ):
            _append_unique(blockers, "task state unavailable or stale")
            return
        if _as_bool(getattr(task_state, "interlock_active", False)):
            _append_unique(
                blockers,
                "task interlock active: %s" % (_text(getattr(task_state, "interlock_reason", "")) or "-"),
            )
        if _text(getattr(task_state, "active_job_id", "")) or _text(getattr(task_state, "run_id", "")):
            _append_unique(blockers, "cleaning task is active")
        mission_state = _upper(getattr(task_state, "mission_state", ""))
        public_state = _upper(getattr(task_state, "public_state", ""))
        executor_state = _upper(getattr(task_state, "executor_state", ""))
        if mission_state not in IDLE_STATES:
            _append_unique(blockers, "task mission_state is %s" % mission_state)
        if public_state not in IDLE_STATES:
            _append_unique(blockers, "task public_state is %s" % public_state)
        if executor_state not in IDLE_STATES:
            _append_unique(blockers, "task executor_state is %s" % executor_state)

    def _append_slam_blockers(self, blockers: List[str], *, slam_state, slam_state_ts: float, now: float):
        if slam_state is None or not _fresh(
            slam_state_ts,
            now=now,
            timeout_s=float(self.config.slam_state_stale_timeout_s),
        ):
            _append_unique(blockers, "slam state unavailable or stale")
            return
        current_mode = _text(getattr(slam_state, "current_mode", "")).lower()
        if current_mode == "mapping":
            _append_unique(blockers, "SLAM is in mapping mode")
        if _as_bool(getattr(slam_state, "busy", False)):
            _append_unique(blockers, "SLAM workflow is busy")
        if _text(getattr(slam_state, "active_job_id", "")):
            _append_unique(blockers, "SLAM job is running")
        if _as_bool(getattr(slam_state, "task_running", False)):
            _append_unique(blockers, "task runtime is busy according to SLAM state")
        pending_status = _upper(getattr(slam_state, "pending_map_switch_status", ""))
        if pending_status not in PENDING_SWITCH_IDLE_STATES:
            _append_unique(blockers, "map switch is pending: status=%s" % pending_status)
        localization_state = _text(getattr(slam_state, "localization_state", "")).lower()
        localization_valid = _as_bool(getattr(slam_state, "localization_valid", False))
        if current_mode != "mapping" and (localization_state != "localized" or not localization_valid):
            _append_unique(
                blockers,
                "localization not ready: state=%s valid=%s"
                % (localization_state or "-", str(bool(localization_valid)).lower()),
            )
        if not _as_bool(getattr(slam_state, "active_map_match", False)):
            _append_unique(blockers, "runtime map does not match active map")

    def _append_odometry_blockers(self, blockers: List[str], *, odometry_state, odometry_state_ts: float, now: float):
        if odometry_state is None or not _fresh(
            odometry_state_ts,
            now=now,
            timeout_s=float(self.config.odometry_state_stale_timeout_s),
        ):
            _append_unique(blockers, "odometry state unavailable or stale")
            return
        if not _as_bool(getattr(odometry_state, "odom_valid", False)):
            code = _text(getattr(odometry_state, "error_code", ""))
            message = _text(getattr(odometry_state, "message", ""))
            _append_unique(blockers, "odometry not ready: code=%s message=%s" % (code or "-", message or "-"))

    def _append_platform_blockers(self, blockers: List[str], *, combined_status, combined_status_ts: float, now: float):
        if combined_status is None or not _fresh(
            combined_status_ts,
            now=now,
            timeout_s=float(self.config.combined_status_stale_timeout_s),
        ):
            _append_unique(blockers, "platform status unavailable or stale")
            return
        status = list(getattr(combined_status, "status", []) or [])
        if len(status) >= 1 and bool(status[0]):
            _append_unique(blockers, "emergency stop 1 is active")
        if len(status) >= 2 and bool(status[1]):
            _append_unique(blockers, "emergency stop 2 is active")
        if not _as_bool(getattr(combined_status, "overall_ready", True)):
            _append_unique(blockers, "platform overall_ready is false")

    def velocity_for_request(
        self,
        *,
        direction: str,
        linear_mps: float = 0.0,
        angular_radps: float = 0.0,
    ) -> Tuple[float, float, float]:
        normalized_direction = _text(direction).lower()
        if normalized_direction not in self.supported_directions:
            raise ValueError("unsupported manual drive direction: %s" % (normalized_direction or "-"))
        linear = min(
            _finite_positive(linear_mps, float(self.config.default_linear_mps)),
            abs(float(self.config.linear_mps_limit)),
        )
        angular = min(
            _finite_positive(angular_radps, float(self.config.default_angular_radps)),
            abs(float(self.config.angular_radps_limit)),
        )
        if normalized_direction == "forward":
            return (_clamp_abs(linear, self.config.linear_mps_limit), 0.0, 0.0)
        if normalized_direction == "backward":
            return (_clamp_abs(-linear, self.config.linear_mps_limit), 0.0, 0.0)
        if normalized_direction == "turn_left":
            return (0.0, 0.0, _clamp_abs(angular, self.config.angular_radps_limit))
        if normalized_direction == "turn_right":
            return (0.0, 0.0, _clamp_abs(-angular, self.config.angular_radps_limit))
        if normalized_direction == "strafe_left":
            return (0.0, _clamp_abs(linear, self.config.linear_mps_limit), 0.0)
        if normalized_direction == "strafe_right":
            return (0.0, _clamp_abs(-linear, self.config.linear_mps_limit), 0.0)
        raise ValueError("unsupported manual drive direction: %s" % (normalized_direction or "-"))
