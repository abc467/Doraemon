#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os
import shutil
import time

from coverage_planner.map_io import compute_occupancy_grid_md5, origin_to_jsonable, yaml_pgm_to_occupancy, write_occupancy_to_yaml_pgm
from coverage_planner.plan_store.store import PlanStore


def _utc_version() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def main():
    ap = argparse.ArgumentParser(description="Import existing yaml/pgm maps into managed static map assets")
    ap.add_argument("--plan-db-path", default="/data/coverage/planning.db")
    ap.add_argument("--maps-root", default="/data/maps")
    ap.add_argument("--src-glob", default="")
    ap.add_argument("--robot-id", default="local_robot")
    ap.add_argument("--set-active", action="store_true")
    args = ap.parse_args()

    if not str(args.src_glob or "").strip():
        raise SystemExit("--src-glob is required for explicit offline import")

    store = PlanStore(args.plan_db_path)
    files = sorted(glob.glob(os.path.expanduser(args.src_glob)))
    if not files:
        raise SystemExit("no yaml files found for glob: %s" % args.src_glob)

    for idx, yaml_path in enumerate(files):
        map_name = os.path.splitext(os.path.basename(yaml_path))[0]
        map_version = _utc_version()
        occ = yaml_pgm_to_occupancy(yaml_path)
        map_md5 = compute_occupancy_grid_md5(occ)
        map_id = "map_%s" % str(map_md5)[:8]

        out_dir = os.path.join(os.path.expanduser(args.maps_root), map_name, map_version)
        os.makedirs(out_dir, exist_ok=True)
        pgm_path, new_yaml_path = write_occupancy_to_yaml_pgm(occ, out_dir, base_name="map")

        pbstream_src = os.path.splitext(yaml_path)[0] + ".pbstream"
        pbstream_dst = ""
        if os.path.exists(pbstream_src):
            pbstream_dst = os.path.join(out_dir, "map.pbstream")
            shutil.copy2(pbstream_src, pbstream_dst)

        store.register_map_asset_version(
            map_name=map_name,
            map_version=map_version,
            map_id=map_id,
            map_md5=map_md5,
            yaml_path=new_yaml_path,
            pgm_path=pgm_path,
            pbstream_path=pbstream_dst,
            frame_id=str(occ.header.frame_id or "map"),
            resolution=float(occ.info.resolution or 0.0),
            origin=origin_to_jsonable(occ),
            display_name=map_name,
            robot_id=args.robot_id,
            set_active=bool(args.set_active and idx == len(files) - 1),
        )
        store.backfill_legacy_map_scope(
            map_name=map_name,
            map_version=map_version,
            map_id=map_id,
            map_md5=map_md5,
        )
        print("imported", map_name, map_version, map_id, new_yaml_path)


if __name__ == "__main__":
    main()
