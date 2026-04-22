# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any

from coverage_planner.plan_store.store import PlanStore


@dataclass
class LoadedBlock:
    block_id: int
    entry_xyyaw: Tuple[float, float, float]
    exit_xyyaw: Tuple[float, float, float]
    path_xyyaw: List[Tuple[float, float, float]]
    length_m: float
    point_count: int


@dataclass
class LoadedPlan:
    plan_id: str
    zone_id: str
    zone_version: int
    frame_id: str
    map_name: str
    map_revision_id: str
    plan_profile_name: str
    constraint_version: str
    exec_order: List[int]
    blocks: List[LoadedBlock]
    total_length_m: float
    map_id: str = ""
    map_md5: str = ""
    planner_version: str = ""


class PlanLoader:
    """Load a persisted plan from SQLite.

    Notes:
      - The planner stores `plan_profile_name` in the `plans` table.
      - Commercial execution only supports explicit `plan_profile_name`.
        The loader resolves plans inside that profile scope and does not silently
        fall back to "latest plan for zone".
    """

    def __init__(self, db_path: str):
        self.store = PlanStore(db_path)

    def close(self):
        self.store.close()

    def get_zone_meta(
        self,
        zone_id: str,
        *,
        map_name: str = "",
        map_revision_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Expose zones table meta for executor-side consistency checks."""
        try:
            return self.store.get_zone_meta(
                str(zone_id),
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or ""),
            )
        except Exception:
            return None

    def get_active_constraint_version(self, map_id: str, map_revision_id: str = "") -> Optional[str]:
        try:
            return self.store.get_active_constraint_version(
                str(map_id),
                str(map_revision_id or ""),
            )
        except Exception:
            return None

    def load_for_zone(
        self,
        zone_id: str,
        zone_version: Optional[int] = None,
        plan_profile_name: Optional[str] = None,
        *,
        map_name: str = "",
        map_revision_id: str = "",
    ) -> LoadedPlan:
        zone_id = str(zone_id)
        map_name = str(map_name or "").strip()
        map_revision_id = str(map_revision_id or "").strip()

        plan_id: Optional[str] = None

        prof = (plan_profile_name or "").strip()
        if not prof:
            scope = f" map={map_name}" if map_name else ""
            raise RuntimeError(f"plan_profile_name is required for zone_id={zone_id}{scope}")

        if prof:
            plan_id = self.store.get_latest_plan_id_by_profile(
                zone_id,
                prof,
                zone_version=zone_version,
                map_name=map_name,
                map_revision_id=map_revision_id,
            )

        if not plan_id:
            plan_id = self.store.get_active_plan_id(
                zone_id,
                plan_profile_name=(prof or None),
                map_name=map_name,
                map_revision_id=map_revision_id,
            )

        if not plan_id:
            scope = f" map={map_name}" if map_name else ""
            raise RuntimeError(f"No plan found for zone_id={zone_id} profile={prof}{scope}")

        meta = self.store.load_plan_meta(plan_id)
        exec_order = meta.get("exec_order_json") or list(range(int(meta["blocks"])))
        frame_id = meta.get("frame_id") or "map"
        plan_profile_name_loaded = meta.get("plan_profile_name") or "cover_standard"

        blocks: List[LoadedBlock] = []
        # 按 block_id 全量读出，然后按 exec_order 排序
        block_map: Dict[int, LoadedBlock] = {}
        for bid in range(int(meta["blocks"])):
            b = self.store.load_block(plan_id, bid)
            path = b["path_xyyaw"]
            entry = (float(b["entry_x"]), float(b["entry_y"]), float(b["entry_yaw"]))
            exitp = (float(b["exit_x"]), float(b["exit_y"]), float(b["exit_yaw"]))
            lb = LoadedBlock(
                block_id=int(b["block_id"]),
                entry_xyyaw=entry,
                exit_xyyaw=exitp,
                path_xyyaw=path,
                length_m=float(b["length_m"]),
                point_count=int(b["point_count"]),
            )
            block_map[lb.block_id] = lb

        for bid in exec_order:
            bid = int(bid)
            if bid in block_map:
                blocks.append(block_map[bid])
            else:
                raise RuntimeError(f"exec_order references missing block_id={bid}")

        return LoadedPlan(
            plan_id=str(meta["plan_id"]),
            zone_id=str(meta["zone_id"]),
            zone_version=int(meta["zone_version"]),
            frame_id=str(frame_id),
            map_name=str(meta.get("map_name") or ""),
            map_revision_id=str(meta.get("map_revision_id") or ""),
            plan_profile_name=str(plan_profile_name_loaded),
            constraint_version=str(meta.get("constraint_version") or ""),
            exec_order=[int(x) for x in exec_order],
            blocks=blocks,
            total_length_m=float(meta["total_length_m"]),
            map_id=str(meta.get("map_id") or ""),
            map_md5=str(meta.get("map_md5") or ""),
            planner_version=str(meta.get("planner_version") or ""),
        )
