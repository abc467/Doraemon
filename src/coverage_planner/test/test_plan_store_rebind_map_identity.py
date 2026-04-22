#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.plan_store.store import PlanStore


class PlanStoreRebindMapIdentityTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="plan_store_rebind_")
        self._db_path = os.path.join(self._tmpdir, "planning.db")
        self.store = PlanStore(self._db_path)

    def tearDown(self):
        try:
            self.store.close()
        finally:
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_rebind_map_identity_updates_scoped_records(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="demo",
            map_id="map_old1234",
            map_md5="old1234567890abcdef",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
        )
        self.store.set_active_map(map_name="demo_map", robot_id="local_robot")
        asset = self.store.resolve_map_asset(map_name="demo_map") or {}
        map_revision_id = str(asset.get("revision_id") or "").strip()
        self.store.upsert_map_alignment_config(
            map_name="demo_map",
            alignment_version="align_v1",
            map_id="map_old1234",
            map_version="old1234567890abcdef",
            raw_frame="map",
            aligned_frame="site_map",
            status="active",
        )

        cur = self.store.conn.cursor()
        cur.execute(
            """
            INSERT INTO zone_versions(
              map_revision_id, map_name, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?);
            """,
            (
                map_revision_id,
                "demo_map",
                "zone_demo",
                1,
                "map",
                "[]",
                "[]",
                "map_old1234",
                "old1234567890abcdef",
                1.0,
            ),
        )
        cur.execute(
            """
            INSERT INTO plans(
              plan_id, map_revision_id, map_name, zone_id, zone_version, frame_id, plan_profile_name, params_json, robot_json,
              blocks, total_length_m, exec_order_json, map_id, map_md5, constraint_version, planner_version, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                "plan_demo",
                map_revision_id,
                "demo_map",
                "zone_demo",
                1,
                "map",
                "default",
                "{}",
                "{}",
                0,
                0.0,
                "[]",
                "map_old1234",
                "old1234567890abcdef",
                "constraint_v1",
                "planner_v1",
                1.0,
            ),
        )
        cur.execute(
            """
            INSERT INTO map_constraint_versions(map_id, constraint_version, map_md5, created_ts, updated_ts)
            VALUES(?,?,?,?,?);
            """,
            ("map_old1234", "constraint_v1", "old1234567890abcdef", 1.0, 1.0),
        )
        cur.execute(
            """
            INSERT INTO map_no_go_areas(map_id, constraint_version, area_id, name, polygon_json, enabled, updated_ts)
            VALUES(?,?,?,?,?,?,?);
            """,
            ("map_old1234", "constraint_v1", "area_1", "area", "[]", 1, 1.0),
        )
        cur.execute(
            """
            INSERT INTO map_virtual_walls(
              map_id, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?);
            """,
            ("map_old1234", "constraint_v1", "wall_1", "wall", "[]", 0.2, 1, 1.0),
        )
        cur.execute(
            """
            INSERT INTO map_active_constraint_versions(map_id, active_constraint_version, updated_ts)
            VALUES(?,?,?);
            """,
            ("map_old1234", "constraint_v1", 1.0),
        )
        self.store.conn.commit()

        rebound = self.store.rebind_map_identity(
            map_name="demo_map",
            map_id="map_new5678",
            map_md5="new56789abcdef012345",
        )

        self.assertEqual(rebound.get("map_id"), "map_new5678")
        self.assertEqual(rebound.get("map_md5"), "new56789abcdef012345")

        asset = self.store.resolve_map_asset(map_name="demo_map") or {}
        self.assertEqual(asset.get("map_id"), "map_new5678")
        self.assertEqual(asset.get("map_md5"), "new56789abcdef012345")

        zone_row = self.store.conn.execute(
            "SELECT map_id, map_md5 FROM zone_versions WHERE map_name=? AND zone_id=? AND zone_version=?;",
            ("demo_map", "zone_demo", 1),
        ).fetchone()
        self.assertEqual(tuple(zone_row), ("map_new5678", "new56789abcdef012345"))

        plan_row = self.store.conn.execute(
            "SELECT map_id, map_md5 FROM plans WHERE plan_id=?;",
            ("plan_demo",),
        ).fetchone()
        self.assertEqual(tuple(plan_row), ("map_new5678", "new56789abcdef012345"))

        constraint_row = self.store.conn.execute(
            "SELECT map_id, map_md5 FROM map_constraint_versions WHERE map_id=? AND constraint_version=?;",
            ("map_new5678", "constraint_v1"),
        ).fetchone()
        self.assertEqual(tuple(constraint_row), ("map_new5678", "new56789abcdef012345"))
        old_constraint = self.store.conn.execute(
            "SELECT 1 FROM map_constraint_versions WHERE map_id=?;",
            ("map_old1234",),
        ).fetchone()
        self.assertIsNone(old_constraint)

        no_go_row = self.store.conn.execute(
            "SELECT map_id FROM map_no_go_areas WHERE map_id=? AND constraint_version=? AND area_id=?;",
            ("map_new5678", "constraint_v1", "area_1"),
        ).fetchone()
        self.assertEqual(tuple(no_go_row), ("map_new5678",))

        wall_row = self.store.conn.execute(
            "SELECT map_id FROM map_virtual_walls WHERE map_id=? AND constraint_version=? AND wall_id=?;",
            ("map_new5678", "constraint_v1", "wall_1"),
        ).fetchone()
        self.assertEqual(tuple(wall_row), ("map_new5678",))

        active_constraint_row = self.store.conn.execute(
            "SELECT map_id FROM map_active_constraint_versions WHERE map_id=?;",
            ("map_new5678",),
        ).fetchone()
        self.assertEqual(tuple(active_constraint_row), ("map_new5678",))

        alignment_row = self.store.conn.execute(
            """
            SELECT map_id, map_version
            FROM map_alignment_revision_configs
            WHERE map_revision_id=? AND alignment_version=?;
            """,
            (map_revision_id, "align_v1"),
        ).fetchone()
        self.assertEqual(tuple(alignment_row), ("map_new5678", "new56789abcdef012345"))


if __name__ == "__main__":
    unittest.main()
