#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os

from coverage_planner.map_asset_import import map_id_from_md5
from coverage_planner.map_asset_import import normalize_import_verification_mode
from coverage_planner.map_asset_import import register_imported_map_asset
from coverage_planner.map_io import compute_occupancy_grid_md5, yaml_pgm_to_occupancy, write_occupancy_to_yaml_pgm
from coverage_planner.map_path_security import (
    canonical_directory_root,
    copy_regular_file_exclusive,
    ensure_secure_parent_directory,
    map_asset_target_paths,
    validate_existing_regular_file,
    validate_map_name,
)
from coverage_planner.plan_store.store import PlanStore


def _find_existing_asset(store: PlanStore, map_name: str, map_md5: str):
    if not str(map_md5 or "").strip():
        return None
    for asset in store.list_map_assets_by_name(map_name=map_name):
        if str(asset.get("map_md5") or "").strip() == str(map_md5 or "").strip():
            return asset
    return None


def main():
    ap = argparse.ArgumentParser(description="Import existing yaml/pgm maps into managed static map assets")
    ap.add_argument("--plan-db-path", default="/data/coverage/planning.db")
    ap.add_argument("--maps-root", default="/data/maps")
    ap.add_argument("--src-glob", default="")
    ap.add_argument("--robot-id", default="local_robot")
    ap.add_argument(
        "--verification-mode",
        default="candidate",
        help="candidate or offline_verified",
    )
    ap.add_argument("--set-active", action="store_true")
    args = ap.parse_args()

    if not str(args.src_glob or "").strip():
        raise SystemExit("--src-glob is required for explicit offline import")
    verification_mode = normalize_import_verification_mode(args.verification_mode)
    if bool(args.set_active) and verification_mode != "offline_verified":
        raise SystemExit("--set-active requires --verification-mode=offline_verified")

    maps_root = canonical_directory_root(args.maps_root, label="maps_root")
    store = PlanStore(args.plan_db_path)
    files = sorted(glob.glob(os.path.expanduser(args.src_glob)))
    if not files:
        raise SystemExit("no yaml files found for glob: %s" % args.src_glob)

    for idx, yaml_path in enumerate(files):
        yaml_path = os.path.abspath(os.path.expanduser(str(yaml_path or "").strip()))
        source_root = canonical_directory_root(os.path.dirname(yaml_path), label="map import source root")
        yaml_path = validate_existing_regular_file(
            source_root,
            yaml_path,
            suffix=".yaml",
            label="map import yaml",
        )
        map_name = validate_map_name(os.path.splitext(os.path.basename(yaml_path))[0])
        occ = yaml_pgm_to_occupancy(yaml_path, allowed_root=source_root)
        map_md5 = ""
        if verification_mode == "offline_verified":
            map_md5 = str(compute_occupancy_grid_md5(occ) or "").strip()
            if not map_md5:
                raise RuntimeError("failed to compute map_md5 for %s" % yaml_path)
        existing_asset = _find_existing_asset(store, map_name=map_name, map_md5=map_md5)
        if existing_asset is not None:
            print(
                "reused",
                existing_asset.get("map_name"),
                str(existing_asset.get("revision_id") or ""),
                str(existing_asset.get("map_id") or map_id_from_md5(map_md5) or ""),
            )
            continue

        revision_id = store.generate_map_revision_id(map_name)
        target_paths = map_asset_target_paths(maps_root, map_name, revision_id)
        for target_path in target_paths.values():
            if os.path.lexists(target_path):
                raise RuntimeError("target map asset already exists: %s" % target_path)
        ensure_secure_parent_directory(maps_root, target_paths["pbstream_path"])
        out_root = os.path.dirname(target_paths["pbstream_path"])
        pbstream_src = os.path.splitext(yaml_path)[0] + ".pbstream"
        pbstream_dst = ""
        pgm_path = ""
        new_yaml_path = ""
        try:
            pgm_path, new_yaml_path = write_occupancy_to_yaml_pgm(
                occ,
                out_root,
                base_name=map_name,
                allowed_root=maps_root,
            )
            if os.path.lexists(pbstream_src):
                pbstream_src = validate_existing_regular_file(
                    source_root,
                    pbstream_src,
                    suffix=".pbstream",
                    label="map import pbstream",
                )
                pbstream_dst = copy_regular_file_exclusive(
                    source_root,
                    pbstream_src,
                    maps_root,
                    target_paths["pbstream_path"],
                    suffix=".pbstream",
                )
        except Exception:
            for created_path in (pbstream_dst, new_yaml_path, pgm_path):
                if created_path and os.path.lexists(created_path):
                    try:
                        os.unlink(created_path)
                    except OSError:
                        pass
            raise

        asset, snapshot_md5 = register_imported_map_asset(
            store,
            map_name=map_name,
            occ=occ,
            yaml_path=new_yaml_path,
            pgm_path=pgm_path,
            pbstream_path=pbstream_dst,
            display_name=map_name,
            description="",
            enabled=True,
            robot_id=args.robot_id,
            verification_mode=verification_mode,
            set_active=bool(args.set_active and idx == len(files) - 1),
            revision_id=revision_id,
        )
        if str(asset.get("map_id") or "").strip() or str(asset.get("map_md5") or "").strip():
            store.backfill_unscoped_map_scope(
                map_name=map_name,
                map_id=str(asset.get("map_id") or ""),
                map_md5=str(asset.get("map_md5") or ""),
            )
        print(
            "imported",
            map_name,
            str(asset.get("revision_id") or ""),
            str(asset.get("verification_status") or ""),
            snapshot_md5,
            new_yaml_path,
        )


if __name__ == "__main__":
    main()
