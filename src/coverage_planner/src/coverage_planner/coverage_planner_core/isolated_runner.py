# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import signal
import subprocess
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from shapely.geometry import Polygon
    from shapely.ops import unary_union
    from shapely.validation import explain_validity

    _HAS_SHAPELY = True
except Exception:  # pragma: no cover - runtime packaging decides this
    Polygon = None
    unary_union = None
    explain_validity = None
    _HAS_SHAPELY = False

from .types import BlockDebug, BlockPlan, PlanResult, PlannerParams, RobotSpec


@dataclass
class IsolatedPlanOutcome:
    result: Optional[PlanResult]
    crashed: bool = False
    timeout: bool = False
    message: str = ""


def _validated_ring(value: Any, *, label: str) -> Tuple[Optional[List[Tuple[float, float]]], str]:
    if not isinstance(value, (list, tuple)):
        return None, "%s must be an array" % label
    ring: List[Tuple[float, float]] = []
    for index, point in enumerate(value):
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            return None, "%s[%d] must contain x/y" % (label, int(index))
        try:
            x = float(point[0])
            y = float(point[1])
        except (TypeError, ValueError, OverflowError):
            return None, "%s[%d] has non-numeric x/y" % (label, int(index))
        if not (math.isfinite(x) and math.isfinite(y)):
            return None, "%s[%d] has non-finite x/y" % (label, int(index))
        if not ring or ring[-1] != (x, y):
            ring.append((x, y))
    if len(ring) >= 2 and ring[0] == ring[-1]:
        ring = ring[:-1]
    if len(set(ring)) < 3:
        return None, "%s must contain at least 3 distinct points" % label
    return ring, ""


def _effective_regions_validation_error(regions: Any) -> str:
    """Validate the exact payload before any native Fields2Cover call."""
    if not _HAS_SHAPELY:
        return "Shapely is unavailable; cannot validate effective regions"
    if not isinstance(regions, (list, tuple)) or not regions:
        return "effective_regions must contain at least one Polygon"

    polygons = []
    for region_index, region in enumerate(regions):
        if not isinstance(region, dict):
            return "effective_regions[%d] must be an object" % int(region_index)
        outer, error = _validated_ring(
            region.get("outer"),
            label="effective_regions[%d].outer" % int(region_index),
        )
        if error:
            return error
        raw_holes = region.get("holes") or []
        if not isinstance(raw_holes, (list, tuple)):
            return "effective_regions[%d].holes must be an array" % int(region_index)
        holes: List[List[Tuple[float, float]]] = []
        for hole_index, raw_hole in enumerate(raw_holes):
            hole, error = _validated_ring(
                raw_hole,
                label="effective_regions[%d].holes[%d]" % (int(region_index), int(hole_index)),
            )
            if error:
                return error
            holes.append(hole or [])
        try:
            polygon = Polygon(outer or [], holes)
        except Exception as exc:
            return "effective_regions[%d] cannot be constructed: %s" % (int(region_index), str(exc))
        if polygon.is_empty or polygon.geom_type != "Polygon" or float(polygon.area) <= 1e-9:
            return "effective_regions[%d] is empty or degenerate" % int(region_index)
        if not polygon.is_valid:
            reason = str(explain_validity(polygon) if explain_validity is not None else "invalid geometry")
            return "effective_regions[%d] is invalid: %s" % (int(region_index), reason)
        polygons.append(polygon)

    try:
        combined = unary_union(polygons)
    except Exception as exc:
        return "effective_regions union failed validation: %s" % str(exc)
    if combined.is_empty or not combined.is_valid:
        reason = str(explain_validity(combined) if explain_validity is not None else "invalid geometry")
        return "effective_regions union is invalid: %s" % reason
    return ""


def _block_debug_to_dict(debug: Optional[BlockDebug]) -> Optional[Dict[str, Any]]:
    if debug is None:
        return None
    return {
        "swath_segs_xyz": debug.swath_segs_xyz,
        "snake_pts_xy": debug.snake_pts_xy,
        "edge_loop_xy": debug.edge_loop_xy,
        "conn1_xy": debug.conn1_xy,
        "conn2_xy": debug.conn2_xy,
        "keypts": debug.keypts,
        "entry_xy": debug.entry_xy,
        "exit_xy": debug.exit_xy,
    }


def _block_debug_from_dict(data: Optional[Dict[str, Any]]) -> Optional[BlockDebug]:
    if not data:
        return None
    return BlockDebug(
        swath_segs_xyz=data.get("swath_segs_xyz"),
        snake_pts_xy=data.get("snake_pts_xy"),
        edge_loop_xy=data.get("edge_loop_xy"),
        conn1_xy=data.get("conn1_xy"),
        conn2_xy=data.get("conn2_xy"),
        keypts=data.get("keypts"),
        entry_xy=data.get("entry_xy"),
        exit_xy=data.get("exit_xy"),
    )


def _block_to_dict(block: BlockPlan) -> Dict[str, Any]:
    return {
        "block_id": int(block.block_id),
        "path_xy": block.path_xy,
        "entry_xyyaw": block.entry_xyyaw,
        "exit_xyyaw": block.exit_xyyaw,
        "stats": block.stats,
        "debug": _block_debug_to_dict(block.debug),
    }


def _block_from_dict(data: Dict[str, Any]) -> BlockPlan:
    return BlockPlan(
        block_id=int(data.get("block_id", 0)),
        path_xy=[tuple(p) for p in (data.get("path_xy") or [])],
        entry_xyyaw=tuple(data.get("entry_xyyaw") or (0.0, 0.0, 0.0)),
        exit_xyyaw=tuple(data.get("exit_xyyaw") or (0.0, 0.0, 0.0)),
        stats=dict(data.get("stats") or {}),
        debug=_block_debug_from_dict(data.get("debug")),
    )


def plan_result_to_dict(result: PlanResult) -> Dict[str, Any]:
    return {
        "ok": bool(result.ok),
        "error_code": str(result.error_code or ""),
        "error_message": str(result.error_message or ""),
        "frame_id": str(result.frame_id or "map"),
        "blocks": [_block_to_dict(block) for block in (result.blocks or [])],
        "exec_order": [int(x) for x in (result.exec_order or [])],
        "total_length_m": float(result.total_length_m or 0.0),
    }


def plan_result_from_dict(data: Dict[str, Any]) -> PlanResult:
    return PlanResult(
        ok=bool(data.get("ok", False)),
        error_code=str(data.get("error_code") or ""),
        error_message=str(data.get("error_message") or ""),
        frame_id=str(data.get("frame_id") or "map"),
        blocks=[_block_from_dict(block) for block in (data.get("blocks") or [])],
        exec_order=[int(x) for x in (data.get("exec_order") or [])],
        total_length_m=float(data.get("total_length_m") or 0.0),
    )


def _signal_name(returncode: int) -> str:
    if returncode >= 0:
        return "exit_%d" % int(returncode)
    signum = abs(int(returncode))
    try:
        return signal.Signals(signum).name
    except Exception:
        return "signal_%d" % signum


def run_plan_coverage_isolated(
    *,
    frame_id: str,
    outer: List[Any],
    holes: List[Any],
    robot_spec: RobotSpec,
    params: PlannerParams,
    effective_regions: Optional[List[Dict[str, Any]]] = None,
    debug: bool = False,
    timeout_s: float = 45.0,
) -> IsolatedPlanOutcome:
    regions_to_validate: Sequence[Dict[str, Any]]
    if effective_regions is None:
        regions_to_validate = [{"outer": outer or [], "holes": holes or []}]
    else:
        regions_to_validate = effective_regions
    validation_error = _effective_regions_validation_error(regions_to_validate)
    if validation_error:
        message = "native planner rejected invalid effective geometry: %s" % validation_error
        return IsolatedPlanOutcome(
            result=PlanResult(
                ok=False,
                error_code="INVALID_EFFECTIVE_REGION",
                error_message=message,
                frame_id=str(frame_id or "map"),
                blocks=[],
                exec_order=[],
                total_length_m=0.0,
            ),
            message=message,
        )

    payload = {
        "frame_id": str(frame_id or "map"),
        "outer": outer or [],
        "holes": holes or [],
        "robot_spec": dict(robot_spec.__dict__),
        "params": dict(params.__dict__),
        "effective_regions": effective_regions,
        "debug": bool(debug),
    }
    cmd = [sys.executable, "-m", "coverage_planner.coverage_planner_core.isolated_runner", "--worker"]
    try:
        proc = subprocess.run(
            cmd,
            input=json.dumps(payload, ensure_ascii=False),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=max(1.0, float(timeout_s or 45.0)),
        )
    except subprocess.TimeoutExpired as exc:
        return IsolatedPlanOutcome(
            result=None,
            timeout=True,
            message="planner worker timed out after %.1fs" % float(timeout_s or 45.0),
        )
    except Exception as exc:
        return IsolatedPlanOutcome(result=None, crashed=True, message=str(exc))

    if proc.returncode != 0:
        detail = (proc.stderr or "").strip()
        if len(detail) > 1200:
            detail = detail[-1200:]
        message = "planner worker failed with %s" % _signal_name(proc.returncode)
        if detail:
            message = "%s: %s" % (message, detail)
        return IsolatedPlanOutcome(result=None, crashed=True, message=message)

    try:
        data = json.loads(proc.stdout or "{}")
    except Exception as exc:
        return IsolatedPlanOutcome(
            result=None,
            crashed=True,
            message="planner worker returned invalid json: %s" % str(exc),
        )
    if data.get("status") != "ok":
        return IsolatedPlanOutcome(
            result=None,
            crashed=True,
            message=str(data.get("message") or "planner worker failed"),
        )
    return IsolatedPlanOutcome(result=plan_result_from_dict(data.get("result") or {}))


def _worker_main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        from .planner import plan_coverage

        result = plan_coverage(
            frame_id=str(payload.get("frame_id") or "map"),
            outer=payload.get("outer") or [],
            holes=payload.get("holes") or [],
            robot_spec=RobotSpec(**dict(payload.get("robot_spec") or {})),
            params=PlannerParams(**dict(payload.get("params") or {})),
            effective_regions=payload.get("effective_regions"),
            debug=bool(payload.get("debug", False)),
        )
        sys.stdout.write(json.dumps({"status": "ok", "result": plan_result_to_dict(result)}, ensure_ascii=False))
        sys.stdout.flush()
        return 0
    except Exception as exc:
        sys.stdout.write(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False))
        sys.stdout.flush()
        return 2


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--worker":
        raise SystemExit(_worker_main())
    raise SystemExit("use --worker")
