# -*- coding: utf-8 -*-

"""Shared helpers for human-assisted localization pose overrides."""

from __future__ import annotations

import time
from typing import Dict, Optional

import rospy


DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS = "/localization/manual_assist_pose"


def _normalize_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


def _float_value(raw: Dict[str, object], *keys: str) -> float:
    for key in keys:
        if key in raw:
            return float(raw.get(key) or 0.0)
    raise ValueError("missing %s" % "/".join(str(item or "") for item in keys if str(item or "").strip()))


def inspect_manual_assist_pose_override(
    param_ns: str = DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
    *,
    requested_map_name: str = "",
    requested_revision_id: str = "",
) -> Dict[str, object]:
    normalized_param_ns = str(param_ns or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS).strip()
    if not normalized_param_ns:
        normalized_param_ns = DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
    normalized_requested_map_name = _normalize_map_name(requested_map_name)
    normalized_requested_revision_id = str(requested_revision_id or "").strip()
    info = {
        "param_ns": normalized_param_ns,
        "requested_map_name": normalized_requested_map_name,
        "requested_revision_id": normalized_requested_revision_id,
        "scope_map_name": "",
        "scope_revision_id": "",
        "status": "missing",
        "message": "",
        "override": None,
    }
    try:
        raw = rospy.get_param(normalized_param_ns, {})
    except Exception:
        return info
    if not isinstance(raw, dict):
        info["status"] = "invalid_payload"
        info["message"] = "manual assist pose override payload is not a dict"
        return info
    if not bool(raw.get("enabled", False)):
        info["status"] = "disabled"
        return info

    scope_map_name = _normalize_map_name(raw.get("map_name", ""))
    scope_revision_id = str(raw.get("map_revision_id", "") or "").strip()
    info["scope_map_name"] = scope_map_name
    info["scope_revision_id"] = scope_revision_id

    if not (scope_map_name or scope_revision_id):
        info["status"] = "missing_scope"
        info["message"] = "manual assist pose override missing map scope"
        return info
    if normalized_requested_revision_id and scope_revision_id and scope_revision_id != normalized_requested_revision_id:
        info["status"] = "scope_mismatch"
        info["message"] = (
            "manual assist pose override revision mismatch: requested=%s override=%s"
            % (normalized_requested_revision_id, scope_revision_id)
        )
        return info
    if normalized_requested_map_name and scope_map_name and scope_map_name != normalized_requested_map_name:
        info["status"] = "scope_mismatch"
        info["message"] = (
            "manual assist pose override map mismatch: requested=%s override=%s"
            % (normalized_requested_map_name, scope_map_name)
        )
        return info

    try:
        initial_pose_x = _float_value(raw, "x", "initial_pose_x")
        initial_pose_y = _float_value(raw, "y", "initial_pose_y")
        initial_pose_yaw = _float_value(raw, "yaw", "initial_pose_yaw")
    except Exception as exc:
        info["status"] = "invalid_pose"
        info["message"] = "manual assist pose override invalid pose: %s" % str(exc)
        return info

    frame_id = str(raw.get("frame_id", "map") or "map").strip() or "map"
    override = {
        "param_ns": normalized_param_ns,
        "frame_id": frame_id,
        "map_name": scope_map_name,
        "map_revision_id": scope_revision_id,
        "initial_pose_x": float(initial_pose_x),
        "initial_pose_y": float(initial_pose_y),
        "initial_pose_yaw": float(initial_pose_yaw),
        "consume_once": bool(raw.get("consume_once", True)),
    }
    info["status"] = "ready"
    info["message"] = manual_assist_pose_summary(override)
    info["override"] = override
    return info


def load_manual_assist_pose_override(
    param_ns: str = DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
    *,
    requested_map_name: str = "",
    requested_revision_id: str = "",
) -> Optional[Dict[str, object]]:
    info = inspect_manual_assist_pose_override(
        param_ns,
        requested_map_name=requested_map_name,
        requested_revision_id=requested_revision_id,
    )
    override = info.get("override")
    if info.get("status") != "ready" or not isinstance(override, dict):
        return None
    return dict(override)


def _update_manual_assist_pose_override(
    override: Optional[Dict[str, object]],
    *,
    status: str = "",
    map_name: str = "",
    map_revision_id: str = "",
    message: str = "",
    localization_state: str = "",
    job_id: str = "",
    used: Optional[bool] = None,
    disable: bool = False,
) -> None:
    param_ns = str((override or {}).get("param_ns") or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS).strip()
    if not param_ns:
        param_ns = DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
    try:
        current = rospy.get_param(param_ns, {})
    except Exception:
        current = {}
    updated = dict(current) if isinstance(current, dict) else {}
    if bool(disable):
        updated["enabled"] = False
        updated["last_consumed_ts"] = float(time.time())
    updated["last_status_ts"] = float(time.time())
    updated["last_status"] = str(status or "").strip()
    updated["last_map_name"] = _normalize_map_name(map_name)
    updated["last_map_revision_id"] = str(map_revision_id or "").strip()
    updated["last_message"] = str(message or "").strip()
    updated["last_localization_state"] = str(localization_state or "").strip()
    updated["last_job_id"] = str(job_id or "").strip()
    if used is not None:
        updated["last_used"] = bool(used)
    try:
        rospy.set_param(param_ns, updated)
    except Exception:
        pass


def consume_manual_assist_pose_override(
    override: Optional[Dict[str, object]],
    *,
    status: str = "",
    map_name: str = "",
    map_revision_id: str = "",
    message: str = "",
    localization_state: str = "",
    job_id: str = "",
    used: Optional[bool] = None,
) -> None:
    _update_manual_assist_pose_override(
        override,
        status=status,
        map_name=map_name,
        map_revision_id=map_revision_id,
        message=message,
        localization_state=localization_state,
        job_id=job_id,
        used=used,
        disable=True,
    )


def record_manual_assist_pose_override_status(
    override: Optional[Dict[str, object]],
    *,
    status: str = "",
    map_name: str = "",
    map_revision_id: str = "",
    message: str = "",
    localization_state: str = "",
    job_id: str = "",
    used: Optional[bool] = None,
) -> None:
    _update_manual_assist_pose_override(
        override,
        status=status,
        map_name=map_name,
        map_revision_id=map_revision_id,
        message=message,
        localization_state=localization_state,
        job_id=job_id,
        used=used,
        disable=False,
    )


def manual_assist_pose_summary(override: Optional[Dict[str, object]]) -> str:
    payload = dict(override or {})
    if not payload:
        return "manual_assist_pose=none"
    return (
        "manual_assist_pose frame=%s scope_map=%s scope_revision=%s pose=(%.3f,%.3f,%.3f)"
        % (
            str(payload.get("frame_id") or "map"),
            str(payload.get("map_name") or "-"),
            str(payload.get("map_revision_id") or "-"),
            float(payload.get("initial_pose_x") or 0.0),
            float(payload.get("initial_pose_y") or 0.0),
            float(payload.get("initial_pose_yaw") or 0.0),
        )
    )
