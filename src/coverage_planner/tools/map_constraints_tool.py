#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import sys

from coverage_planner.constraints import DEFAULT_VIRTUAL_WALL_BUFFER_M
from coverage_planner.plan_store.store import PlanStore


def _load_json(path: str):
    with open(os.path.expanduser(path), "r", encoding="utf-8") as f:
        return json.load(f)


def _dump_json(obj):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")


def _cmd_import(args):
    payload = _load_json(args.input)
    map_id = str(args.map_id or payload.get("map_id") or "").strip()
    if not map_id:
        raise SystemExit("map_id is required (use --map-id or payload.map_id)")

    store = PlanStore(args.plan_db_path)
    try:
        version = store.replace_map_constraints(
            map_id=map_id,
            map_md5=str(args.map_md5 or payload.get("map_md5") or "").strip(),
            no_go_areas=list(payload.get("no_go_areas") or []),
            virtual_walls=[
                dict(wall, buffer_m=float(wall.get("buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M) or DEFAULT_VIRTUAL_WALL_BUFFER_M))
                for wall in (payload.get("virtual_walls") or [])
            ],
            constraint_version=str(payload.get("constraint_version") or "").strip(),
        )
        _dump_json(
            {
                "ok": True,
                "plan_db_path": str(args.plan_db_path),
                "map_id": map_id,
                "constraint_version": version,
            }
        )
    finally:
        store.close()


def _cmd_export(args):
    store = PlanStore(args.plan_db_path)
    try:
        data = store.load_map_constraints(
            map_id=str(args.map_id or "").strip(),
            constraint_version=str(args.constraint_version or "").strip() or None,
            create_if_missing=bool(args.create_if_missing),
        )
        _dump_json(data)
    finally:
        store.close()


def main():
    parser = argparse.ArgumentParser(description="Import/export map-level no-go areas and virtual walls")
    parser.add_argument(
        "--plan-db-path",
        default="/data/coverage/planning.db",
        help="planning.db path (default: /data/coverage/planning.db)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_import = sub.add_parser("import", help="import a new active constraint snapshot from JSON")
    p_import.add_argument("--input", required=True, help="JSON file path")
    p_import.add_argument("--map-id", default="", help="override payload map_id")
    p_import.add_argument("--map-md5", default="", help="override payload map_md5")
    p_import.set_defaults(func=_cmd_import)

    p_export = sub.add_parser("export", help="export current or specific constraint snapshot")
    p_export.add_argument("--map-id", required=True, help="map_id to export")
    p_export.add_argument("--constraint-version", default="", help="specific constraint_version to export")
    p_export.add_argument(
        "--create-if-missing",
        action="store_true",
        help="create an empty active constraint snapshot when the map has no active version yet",
    )
    p_export.set_defaults(func=_cmd_export)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
