# -*- coding: utf-8 -*-

"""Workflow execution helpers shared by the formal SLAM runtime manager."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from coverage_planner.slam_workflow_semantics import localization_is_ready


@dataclass(frozen=True)
class RuntimeLocalizationSnapshot:
    active_map_name: str = ""
    active_map_revision_id: str = ""
    current_mode: str = ""
    runtime_map_name: str = ""
    runtime_map_revision_id: str = ""
    localization_state: str = ""
    localization_valid: bool = False


@dataclass(frozen=True)
class LocalizationRequest:
    robot_id: str
    map_name: str
    operation: int
    map_revision_id: str = ""
    frame_id: str = "map"
    has_initial_pose: bool = False
    initial_pose_x: float = 0.0
    initial_pose_y: float = 0.0
    initial_pose_yaw: float = 0.0


def resolve_target_map_name(map_name: str, active_map_name: str = "") -> str:
    """Pick the explicit request map, otherwise reuse the currently selected active map."""
    explicit_name = str(map_name or "").strip()
    if explicit_name:
        return explicit_name
    return str(active_map_name or "").strip()


def should_restart_localization_after_save(
    *,
    switch_to_localization_after_save: bool = False,
    relocalize_after_switch: bool = False,
) -> bool:
    """Commercial contract: restart localization only when both follow-up flags are enabled."""
    return bool(switch_to_localization_after_save) and bool(relocalize_after_switch)


class WorkflowRuntimeExecutor:
    def __init__(
        self,
        *,
        response_factory: Callable[..., Any],
        get_runtime_snapshot: Callable[[str], RuntimeLocalizationSnapshot],
        restart_localization: Callable[..., Any],
        sync_task_ready_state: Optional[Callable[..., None]] = None,
    ):
        self._response_factory = response_factory
        self._get_runtime_snapshot = get_runtime_snapshot
        self._restart_localization = restart_localization
        self._sync_task_ready_state = sync_task_ready_state

    def prepare_for_task(self, request: LocalizationRequest):
        snapshot = self._get_runtime_snapshot(request.robot_id)
        target_name = resolve_target_map_name(request.map_name, snapshot.active_map_name)
        if not target_name:
            return self._response_factory(
                success=False,
                message="map_name or active map is required for prepare_for_task",
                error_code="active_map_required",
                operation=request.operation,
                map_name="",
                localization_state="not_localized",
                current_mode="localization",
            )

        if self._is_task_ready(
            snapshot=snapshot,
            target_name=target_name,
            target_revision_id=str(request.map_revision_id or "").strip(),
        ):
            resolved_revision_id = (
                str(request.map_revision_id or "").strip()
                or str(snapshot.runtime_map_revision_id or "").strip()
                or str(snapshot.active_map_revision_id or "").strip()
            )
            if callable(self._sync_task_ready_state):
                self._sync_task_ready_state(
                    robot_id=request.robot_id,
                    map_name=target_name,
                    map_revision_id=resolved_revision_id,
                    localization_state=str(snapshot.localization_state or "localized").strip() or "localized",
                    localization_valid=bool(snapshot.localization_valid),
                    current_mode="localization",
                )
            return self._response_factory(
                success=True,
                message="task already ready",
                error_code="",
                operation=request.operation,
                map_name=target_name,
                map_revision_id=resolved_revision_id,
                localization_state="localized",
                current_mode="localization",
            )

        return self._restart(request, target_name)

    def switch_map_and_localize(self, request: LocalizationRequest):
        return self._restart(request, str(request.map_name or "").strip())

    def relocalize(self, request: LocalizationRequest):
        snapshot = self._get_runtime_snapshot(request.robot_id)
        target_name = resolve_target_map_name(request.map_name, snapshot.active_map_name)
        return self._restart(request, target_name)

    def stop_mapping(self, *, robot_id: str, map_name: str, map_revision_id: str = "", operation: int):
        snapshot = self._get_runtime_snapshot(robot_id)
        target_name = resolve_target_map_name(map_name, snapshot.active_map_name)
        return self._restart_localization(
            robot_id=robot_id,
            map_name=target_name,
            map_revision_id=str(map_revision_id or "").strip(),
            operation=operation,
        )

    def _restart(self, request: LocalizationRequest, target_name: str):
        return self._restart_localization(
            robot_id=request.robot_id,
            map_name=target_name,
            map_revision_id=str(request.map_revision_id or "").strip(),
            operation=request.operation,
            frame_id=request.frame_id,
            has_initial_pose=bool(request.has_initial_pose),
            initial_pose_x=float(request.initial_pose_x),
            initial_pose_y=float(request.initial_pose_y),
            initial_pose_yaw=float(request.initial_pose_yaw),
        )

    @staticmethod
    def _is_task_ready(
        *,
        snapshot: RuntimeLocalizationSnapshot,
        target_name: str,
        target_revision_id: str = "",
    ) -> bool:
        target_revision_id = str(target_revision_id or "").strip()
        active_revision_id = str(snapshot.active_map_revision_id or "").strip()
        runtime_revision_id = str(snapshot.runtime_map_revision_id or "").strip()
        active_map_name = str(snapshot.active_map_name or "").strip()
        runtime_map_name = str(snapshot.runtime_map_name or "").strip()

        if target_revision_id:
            revision_ready = bool(
                active_revision_id
                and runtime_revision_id
                and active_revision_id == target_revision_id
                and runtime_revision_id == target_revision_id
            )
            name_ready = bool(
                (not active_map_name or active_map_name == target_name)
                and (not runtime_map_name or runtime_map_name == target_name)
            )
        elif active_revision_id or runtime_revision_id:
            revision_ready = bool(
                active_revision_id
                and runtime_revision_id
                and active_revision_id == runtime_revision_id
            )
            name_ready = bool(target_name and active_map_name == target_name and runtime_map_name == target_name)
        else:
            revision_ready = True
            name_ready = bool(target_name and active_map_name == target_name and runtime_map_name == target_name)

        return bool(
            snapshot.current_mode == "localization"
            and revision_ready
            and name_ready
            and localization_is_ready(snapshot.localization_state, snapshot.localization_valid)
        )
