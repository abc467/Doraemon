#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import math
import traceback
import rospy
import actionlib

from coverage_msgs.msg import Polygon2D, ZoneGeometry
from coverage_msgs.msg import PlanCoverageAction, PlanCoverageFeedback, PlanCoverageResult

from coverage_planner.coverage_planner_core.types import RobotSpec, PlannerParams
from coverage_planner.coverage_planner_core.geom_norm import normalize_polygon
from coverage_planner.coverage_planner_core.planner import plan_coverage
from coverage_planner.constraints import (
    DEFAULT_VIRTUAL_WALL_BUFFER_M,
    compile_map_constraints,
    compile_zone_constraints,
)

from coverage_planner.plan_store.store import PlanStore
from coverage_planner.map_identity import (
    ensure_map_identity,
    get_runtime_map_identity,
    get_runtime_map_revision_id,
    get_runtime_map_scope,
)

# Viz is optional (don't break server if viz module is not ready)
try:
    from coverage_planner.coverage_planner_ros.ros_viz_publisher import RosVizPublisher
except Exception:
    RosVizPublisher = None


def polygon2d_to_list(poly: Polygon2D):
    return [(float(p.x), float(p.y)) for p in poly.points]


def parse_polygon_json(polygon_json: str):
    """
    JSON format:
      {"frame_id":"map",
       "outer":[[x,y],...],
       "holes":[[[x,y],...], ...],
       "prec":3}
    """
    obj = json.loads(polygon_json)
    outer = obj.get("outer", None)
    holes = obj.get("holes", []) or []
    frame_id = obj.get("frame_id", "map")
    prec = int(obj.get("prec", 3))
    if outer is None or len(outer) < 3:
        raise ValueError("polygon_json.outer must have >=3 points")
    return outer, holes, frame_id, prec


def _finite_float(value, default):
    try:
        v = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(v):
        return float(default)
    return float(v)


def _positive_float(value, default):
    v = _finite_float(value, default)
    if v <= 0.0:
        return float(default)
    return float(v)


def _positive_int(value, default):
    try:
        v = int(value)
    except Exception:
        return int(default)
    if v <= 0:
        return int(default)
    return int(v)


class CoveragePlannerActionServer:
    def __init__(self):
        # ---- params ----
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.default_frame_id = rospy.get_param("~frame_id", "map")
        self.topic_ns = rospy.get_param("~topic_ns", "/f2c").rstrip("/")

        # Map identity (map_id/map_md5) for plan metadata & later execution consistency checks
        self.auto_map_identity_enable = bool(rospy.get_param("~auto_map_identity_enable", True))
        self.map_topic = str(rospy.get_param("~map_topic", "/map"))
        self.map_identity_timeout_s = float(rospy.get_param("~map_identity_timeout_s", 2.0))
        self.planner_version = str(rospy.get_param("~planner_version", "coverage_planner_v1"))
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.default_virtual_wall_buffer_m = float(
            rospy.get_param("~default_virtual_wall_buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M)
        )

        if self.auto_map_identity_enable:
            mid, mmd5, ok = ensure_map_identity(map_topic=self.map_topic, timeout_s=self.map_identity_timeout_s)
            if ok:
                rospy.loginfo("[planner_server] map identity ready: map_id=%s map_md5=%s", mid, mmd5)
            else:
                rospy.logwarn("[planner_server] map identity not available yet (will still plan, but plan map fields may be empty)")

        self.store = PlanStore(self.plan_db_path)
        self.default_robot_spec = self.load_robot_spec()
        self.default_planner_params = self.load_planner_defaults()
        self.line_w = float(self.default_planner_params.line_w)

        self.viz = None
        if RosVizPublisher is not None:
            try:
                self.viz = RosVizPublisher(
                    topic_ns=self.topic_ns,
                    frame_id=self.default_frame_id,
                    line_w=float(self.default_planner_params.line_w),
                )
            except Exception as e:
                rospy.logwarn("[planner_server] viz init failed, continue without viz: %s", str(e))
                self.viz = None

        self.server = actionlib.SimpleActionServer(
            "~plan_coverage",
            PlanCoverageAction,
            execute_cb=self.execute_cb,
            auto_start=False
        )
        self.server.start()
        rospy.loginfo("[planner_server] ready: %s", rospy.get_name() + "/plan_coverage")
        rospy.loginfo("[planner_server] plan_db_path=%s topic_ns=%s frame_id=%s", self.plan_db_path, self.topic_ns, self.default_frame_id)
        rospy.loginfo("[planner_server] robot_defaults=%s", json.dumps(self.default_robot_spec.__dict__, ensure_ascii=False, sort_keys=True))
        rospy.loginfo("[planner_server] planner_defaults=%s", json.dumps(self.default_planner_params.__dict__, ensure_ascii=False, sort_keys=True))

    def _active_asset_runtime_scope_error(self, active_asset, *, map_id: str, map_md5: str) -> str:
        active_name = str((active_asset or {}).get("map_name") or "").strip()
        active_revision_id = str((active_asset or {}).get("revision_id") or "").strip()
        active_id = str((active_asset or {}).get("map_id") or "").strip()
        active_md5 = str((active_asset or {}).get("map_md5") or "").strip()
        runtime_revision_id = str(get_runtime_map_revision_id("/cartographer/runtime") or "").strip()
        if active_revision_id and runtime_revision_id and active_revision_id != runtime_revision_id:
            return (
                "current /map does not match selected map asset: selected=%s runtime_revision=%s asset_revision=%s"
                % (active_name or "-", runtime_revision_id, active_revision_id)
            )
        if (not runtime_revision_id) and active_md5 and map_md5 and active_md5 != map_md5:
            return (
                "current /map does not match selected map asset: selected=%s runtime_md5=%s asset_md5=%s"
                % (active_name or "-", map_md5, active_md5)
            )
        if (not runtime_revision_id) and active_id and map_id and active_id != map_id:
            return (
                "current /map does not match selected map asset: selected=%s runtime_id=%s asset_id=%s"
                % (active_name or "-", map_id, active_id)
            )
        return ""

    def _param_dict(self, name: str):
        raw = rospy.get_param("~" + str(name), {})
        return raw if isinstance(raw, dict) else {}

    def _private_param(self, key: str, default):
        return rospy.get_param("~" + str(key), default)

    def _cfg_value(self, cfg: dict, key: str, fallback):
        if key in cfg:
            return cfg[key]
        return fallback

    def load_robot_spec(self) -> RobotSpec:
        """
        Robot cfg from robot local config center / param server.
        This keeps planner goal clean and stable.
        """
        cfg = self._param_dict("robot")
        cov_width = _positive_float(
            self._cfg_value(cfg, "cov_width", self._private_param("robot/cov_width", 1.0)),
            1.0,
        )
        width = _positive_float(
            self._cfg_value(cfg, "width", self._private_param("robot/width", cov_width)),
            cov_width,
        )
        min_turning_radius = _positive_float(
            self._cfg_value(
                cfg,
                "min_turning_radius",
                self._private_param("robot/min_turning_radius", 0.8),
            ),
            0.8,
        )
        max_diff_curv = _finite_float(
            self._cfg_value(cfg, "max_diff_curv", self._private_param("robot/max_diff_curv", 0.2)),
            0.2,
        )
        return RobotSpec(
            cov_width=cov_width,
            width=width,
            min_turning_radius=min_turning_radius,
            max_diff_curv=max_diff_curv,
        )

    def load_planner_defaults(self) -> PlannerParams:
        cfg = self._param_dict("planner")
        return PlannerParams(
            split_angle_deg=_finite_float(
                self._cfg_value(cfg, "split_angle_deg", self._private_param("split_angle_deg", PlannerParams.split_angle_deg)),
                PlannerParams.split_angle_deg,
            ),
            turn_model=str(
                self._cfg_value(cfg, "turn_model", self._private_param("turn_model", PlannerParams.turn_model))
                or PlannerParams.turn_model
            ),
            viz_step_m=_positive_float(
                self._cfg_value(cfg, "viz_step_m", self._private_param("viz_step_m", PlannerParams.viz_step_m)),
                PlannerParams.viz_step_m,
            ),
            path_step_m=_positive_float(
                self._cfg_value(cfg, "path_step_m", self._private_param("path_step_m", PlannerParams.path_step_m)),
                PlannerParams.path_step_m,
            ),
            turn_step_m=_positive_float(
                self._cfg_value(cfg, "turn_step_m", self._private_param("turn_step_m", PlannerParams.turn_step_m)),
                PlannerParams.turn_step_m,
            ),
            line_w=_positive_float(
                self._cfg_value(
                    cfg,
                    "line_w",
                    self._private_param("line_w", PlannerParams.line_w),
                ),
                PlannerParams.line_w,
            ),
            mute_stderr=bool(
                self._cfg_value(cfg, "mute_stderr", self._private_param("mute_stderr", PlannerParams.mute_stderr))
            ),
            wall_margin_m=_finite_float(
                self._cfg_value(cfg, "wall_margin_m", self._private_param("wall_margin_m", PlannerParams.wall_margin_m)),
                PlannerParams.wall_margin_m,
            ),
            turn_margin_m=_finite_float(
                self._cfg_value(cfg, "turn_margin_m", self._private_param("turn_margin_m", PlannerParams.turn_margin_m)),
                PlannerParams.turn_margin_m,
            ),
            edge_corner_radius_m=_finite_float(
                self._cfg_value(
                    cfg,
                    "edge_corner_radius_m",
                    self._private_param("edge_corner_radius_m", PlannerParams.edge_corner_radius_m),
                ),
                PlannerParams.edge_corner_radius_m,
            ),
            edge_corner_pull=_finite_float(
                self._cfg_value(cfg, "edge_corner_pull", self._private_param("edge_corner_pull", PlannerParams.edge_corner_pull)),
                PlannerParams.edge_corner_pull,
            ),
            edge_corner_min_pts=_positive_int(
                self._cfg_value(
                    cfg,
                    "edge_corner_min_pts",
                    self._private_param("edge_corner_min_pts", PlannerParams.edge_corner_min_pts),
                ),
                PlannerParams.edge_corner_min_pts,
            ),
            pre_proj_min=_finite_float(
                self._cfg_value(cfg, "pre_proj_min", self._private_param("pre_proj_min", PlannerParams.pre_proj_min)),
                PlannerParams.pre_proj_min,
            ),
            pre_proj_max=_finite_float(
                self._cfg_value(cfg, "pre_proj_max", self._private_param("pre_proj_max", PlannerParams.pre_proj_max)),
                PlannerParams.pre_proj_max,
            ),
            pre_prefix_max=_finite_float(
                self._cfg_value(cfg, "pre_prefix_max", self._private_param("pre_prefix_max", PlannerParams.pre_prefix_max)),
                PlannerParams.pre_prefix_max,
            ),
            e_pre_min=_finite_float(
                self._cfg_value(cfg, "e_pre_min", self._private_param("e_pre_min", PlannerParams.e_pre_min)),
                PlannerParams.e_pre_min,
            ),
            e_pre_max=_finite_float(
                self._cfg_value(cfg, "e_pre_max", self._private_param("e_pre_max", PlannerParams.e_pre_max)),
                PlannerParams.e_pre_max,
            ),
        )

    def resolve_planner_params(self, goal) -> PlannerParams:
        defaults = self.default_planner_params

        def goal_float(value, default, *, positive=False):
            v = _finite_float(value, 0.0)
            if abs(v) <= 1e-9:
                return float(default)
            return _positive_float(v, default) if positive else float(v)

        def goal_int(value, default, *, positive=False):
            try:
                v = int(value)
            except Exception:
                v = 0
            if v == 0:
                return int(default)
            if positive and v <= 0:
                return int(default)
            return int(v)

        turn_model = str(goal.turn_model or "").strip() or str(defaults.turn_model)
        return PlannerParams(
            split_angle_deg=goal_float(goal.split_angle_deg, defaults.split_angle_deg),
            turn_model=turn_model,
            viz_step_m=goal_float(goal.viz_step_m, defaults.viz_step_m, positive=True),
            path_step_m=goal_float(goal.path_step_m, defaults.path_step_m, positive=True),
            turn_step_m=goal_float(goal.turn_step_m, defaults.turn_step_m, positive=True),
            line_w=goal_float(goal.line_w, defaults.line_w, positive=True),
            mute_stderr=bool(goal.mute_stderr or defaults.mute_stderr),
            wall_margin_m=goal_float(goal.wall_margin_m, defaults.wall_margin_m),
            turn_margin_m=goal_float(goal.turn_margin_m, defaults.turn_margin_m),
            edge_corner_radius_m=goal_float(goal.edge_corner_radius_m, defaults.edge_corner_radius_m),
            edge_corner_pull=goal_float(goal.edge_corner_pull, defaults.edge_corner_pull),
            edge_corner_min_pts=goal_int(goal.edge_corner_min_pts, defaults.edge_corner_min_pts, positive=True),
            pre_proj_min=goal_float(goal.pre_proj_min, defaults.pre_proj_min),
            pre_proj_max=goal_float(goal.pre_proj_max, defaults.pre_proj_max),
            pre_prefix_max=goal_float(goal.pre_prefix_max, defaults.pre_prefix_max),
            e_pre_min=goal_float(goal.e_pre_min, defaults.e_pre_min),
            e_pre_max=goal_float(goal.e_pre_max, defaults.e_pre_max),
        )

    def execute_cb(self, goal):
        fb = PlanCoverageFeedback()
        res = PlanCoverageResult()

        def publish(stage, prog, msg):
            fb.stage = stage
            fb.progress = float(prog)
            fb.message = msg
            self.server.publish_feedback(fb)

        def finish_ok(plan_id, blocks, total_len):
            res.ok = True
            res.plan_id = plan_id
            res.error_code = ""
            res.error_message = ""
            res.blocks = int(blocks)
            res.total_path_length_m = float(total_len)
            self.server.set_succeeded(res)

        def abort(code, msg):
            res.ok = False
            res.plan_id = ""
            res.error_code = code
            res.error_message = msg
            res.blocks = 0
            res.total_path_length_m = 0.0
            self.server.set_aborted(res)

        try:
            publish("VALIDATE", 0.05, "parse input")

            # ---- 1) Input geometry ----
            if goal.geom.outer.points:
                frame_id = goal.geom.frame_id or self.default_frame_id
                outer = polygon2d_to_list(goal.geom.outer)
                holes = [polygon2d_to_list(h) for h in goal.geom.holes]
                prec = 3
            elif goal.polygon_json:
                outer, holes, frame_id, prec = parse_polygon_json(goal.polygon_json)
            else:
                return abort("BAD_INPUT", "geom is empty and polygon_json is empty")

            # Normalize (close rings, fix orientation, rounding)
            outer_n, holes_n = normalize_polygon(outer, holes, prec=prec)
            # Core expects unclosed rings (like your YAML input), so drop last closing point
            outer_u = outer_n[:-1]
            holes_u = [h[:-1] for h in holes_n]

            publish("LOAD_CONSTRAINTS", 0.15, "load active map constraints")
            if self.auto_map_identity_enable:
                map_id, map_md5, ok = ensure_map_identity(
                    map_topic=self.map_topic,
                    timeout_s=self.map_identity_timeout_s,
                    set_global_params=True,
                    set_private_params=True,
                    refresh=True,
                )
            else:
                map_id, map_md5 = get_runtime_map_identity()
                ok = bool(map_id or map_md5)
            map_id = str(map_id or "").strip()
            map_md5 = str(map_md5 or "").strip()
            if not map_id:
                return abort("MAP_IDENTITY_MISSING", "planner requires map_id before constraint-aware planning")

            active_asset = None
            try:
                active_asset = self.store.get_active_map(robot_id=self.robot_id)
            except Exception:
                active_asset = None

            if active_asset is None:
                return abort("MAP_SCOPE_MISSING", "planner requires a selected current map before saving plans")
            if active_asset is not None:
                active_name = str(active_asset.get("map_name") or "").strip()
                active_revision_id = str(active_asset.get("revision_id") or "").strip()
                if active_name:
                    rospy.set_param("/map_name", active_name)
                scope_error = self._active_asset_runtime_scope_error(
                    active_asset,
                    map_id=map_id,
                    map_md5=map_md5,
                )
                if scope_error:
                    return abort(
                        "MAP_ASSET_RUNTIME_MISMATCH",
                        scope_error,
                    )

            raw_constraints = self.store.load_map_constraints(
                map_id=map_id,
                map_revision_id=active_revision_id if active_asset is not None else "",
                map_md5_hint=map_md5,
                create_if_missing=False,
            )
            compiled_map_constraints = compile_map_constraints(
                map_id=map_id,
                map_md5=map_md5 or str(raw_constraints.get("map_md5") or ""),
                constraint_version=str(raw_constraints.get("constraint_version") or ""),
                no_go_areas=raw_constraints.get("no_go_areas") or [],
                virtual_walls=raw_constraints.get("virtual_walls") or [],
                default_buffer_m=float(self.default_virtual_wall_buffer_m),
                prec=prec,
            )
            compiled_zone_constraints = compile_zone_constraints(
                zone_outer=outer_u,
                zone_holes=holes_u,
                map_constraints=compiled_map_constraints,
                prec=prec,
            )
            if not compiled_zone_constraints.effective_regions:
                return abort("NO_EFFECTIVE_REGION", "constraints leave no reachable cleaning area in zone")

            plan_profile_name = str(goal.profile_name or "").strip()

            # ---- 2) Planner params ----
            params = self.resolve_planner_params(goal)
            robot = self.default_robot_spec
            rospy.loginfo(
                "[planner_server] plan request zone=%s plan_profile_name=%s robot=%s planner=%s",
                str(goal.zone_id or ""),
                plan_profile_name,
                json.dumps(robot.__dict__, ensure_ascii=False, sort_keys=True),
                json.dumps(params.__dict__, ensure_ascii=False, sort_keys=True),
            )

            if self.server.is_preempt_requested():
                self.server.set_preempted()
                return

            publish("PLAN", 0.25, "run core planner")
            plan_res = plan_coverage(
                frame_id=frame_id,
                outer=outer_u,
                holes=holes_u,
                effective_regions=compiled_zone_constraints.effective_regions,
                robot_spec=robot,
                params=params,
                debug=bool(goal.debug_publish_markers),
                preempt_cb=self.server.is_preempt_requested
            )

            if self.server.is_preempt_requested():
                self.server.set_preempted()
                return

            if not getattr(plan_res, "ok", False):
                return abort(getattr(plan_res, "error_code", "PLAN_FAILED"),
                             getattr(plan_res, "error_message", "plan failed"))

            publish("STORE", 0.85, "store plan to sqlite")

            # ---- 3) Store plan ----
            plan_id = self.store.save_plan(
                zone_id=str(goal.zone_id),
                zone_version=int(goal.zone_version),
                frame_id=frame_id,
                plan_profile_name=plan_profile_name,
                params=params.__dict__,
                robot_spec=robot.__dict__,
                outer=outer_u,
                holes=compiled_zone_constraints.keepout_snapshot_rings,
                plan_result=plan_res,
                map_name=str((active_asset or {}).get("map_name") or ""),
                map_revision_id=str((active_asset or {}).get("revision_id") or ""),
                map_id=str(map_id or ""),
                map_md5=str(map_md5 or ""),
                constraint_version=str(compiled_zone_constraints.constraint_version or ""),
                planner_version=str(self.planner_version or ""),
            )

            # best-effort bind active plan (version-guard)
            try:
                self.store.set_active_plan(
                    str(goal.zone_id),
                    int(goal.zone_version),
                    plan_id,
                    plan_profile_name=plan_profile_name,
                    frame_id=frame_id,
                    outer=outer_u,
                    holes=compiled_zone_constraints.keepout_snapshot_rings,
                    map_name=str((active_asset or {}).get("map_name") or ""),
                    map_revision_id=str((active_asset or {}).get("revision_id") or ""),
                    map_id=str(map_id or ""),
                    map_md5=str(map_md5 or ""),
                )
            except Exception as e:
                rospy.logwarn("[planner_server] set_active_plan failed (ok to ignore if you don't use zones table yet): %s", str(e))

            # ---- 4) Debug viz ----
            if goal.debug_publish_markers and self.viz is not None:
                publish("VIZ", 0.93, "publish rviz markers")
                try:
                    self.viz.line_w = float(params.line_w)
                    self.viz.publish_plan_debug(plan_res, viz_step_m=params.viz_step_m)
                except Exception as e:
                    rospy.logwarn("[planner_server] viz publish failed: %s", str(e))

            publish("DONE", 1.0, "success")
            finish_ok(plan_id, len(plan_res.blocks or []), float(getattr(plan_res, "total_length_m", 0.0) or 0.0))

        except Exception as e:
            tb = traceback.format_exc()
            rospy.logerr("[planner_server] EXCEPTION: %s\n%s", str(e), tb)
            abort("EXCEPTION", f"{e}")


def main():
    rospy.init_node("coverage_planner_server")
    CoveragePlannerActionServer()
    rospy.spin()


if __name__ == "__main__":
    main()
