# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
import json
import signal
import subprocess
import sys
from typing import Any, Dict, List, Optional

from .types import BlockDebug, BlockPlan, PlanResult, PlannerParams, RobotSpec


@dataclass
class IsolatedPlanOutcome:
    result: Optional[PlanResult]
    crashed: bool = False
    timeout: bool = False
    message: str = ""


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
