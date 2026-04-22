#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import unittest


def _load_site_editor_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "site_editor_service_node.py"
    spec = importlib.util.spec_from_file_location("site_editor_service_node_guard_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


SITE_EDITOR_MODULE = _load_site_editor_module()


class _FakeStore:
    def __init__(self, asset, active_asset=None, preferred_revision=None):
        self._asset = dict(asset)
        self._active_asset = dict(active_asset or {})
        self._preferred_revision = dict(preferred_revision or asset)
        self.resolve_calls = []
        self.resolve_revision_calls = []

    def resolve_map_asset(self, **kwargs):
        self.resolve_calls.append(dict(kwargs))
        revision_id = str(kwargs.get("revision_id") or "").strip()
        map_name = str(kwargs.get("map_name") or "").strip()
        if revision_id and revision_id == str(self._active_asset.get("revision_id") or ""):
            return dict(self._active_asset)
        if revision_id and revision_id == str(self._asset.get("revision_id") or ""):
            return dict(self._asset)
        if map_name and map_name == str(self._asset.get("map_name") or ""):
            return dict(self._asset)
        return dict(self._asset)

    def get_active_map(self, *, robot_id: str):
        del robot_id
        return dict(self._active_asset or {})

    def resolve_map_revision(self, **kwargs):
        self.resolve_revision_calls.append(dict(kwargs))
        revision_id = str(kwargs.get("revision_id") or "").strip()
        map_name = str(kwargs.get("map_name") or "").strip()
        if revision_id and revision_id == str(self._active_asset.get("revision_id") or ""):
            return dict(self._active_asset)
        if revision_id and revision_id == str(self._preferred_revision.get("revision_id") or ""):
            return dict(self._preferred_revision)
        if revision_id and revision_id == str(self._asset.get("revision_id") or ""):
            return dict(self._asset)
        if map_name and map_name == str(self._asset.get("map_name") or ""):
            if str(self._active_asset.get("map_name") or "") == map_name:
                return dict(self._active_asset)
            return dict(self._preferred_revision)
        return {}


class _FakeAlignmentStore(_FakeStore):
    def __init__(self, asset, alignment_row, active_asset=None):
        super().__init__(asset, active_asset=active_asset)
        self._alignment_row = dict(alignment_row)

    def get_map_alignment_config(self, **kwargs):
        del kwargs
        return dict(self._alignment_row)


class SiteEditorVerifiedMapGuardTest(unittest.TestCase):
    def test_rejects_pending_map_asset(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore(
            {
                "map_name": "demo_map",
                "enabled": True,
                "verification_status": "pending",
                "lifecycle_status": "saved_unverified",
            }
        )

        with self.assertRaisesRegex(ValueError, "pending verification"):
            node._resolve_asset("demo_map")

    def test_accepts_verified_map_asset(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
                "map_id": "map_12345678",
            }
        )

        asset = node._resolve_asset("demo_map")
        self.assertEqual(str(asset.get("map_name") or ""), "demo_map")

    def test_accepts_revision_only_verified_map_asset(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
                "map_id": "map_12345678",
            }
        )

        asset = node._resolve_asset("", "rev_demo_01")

        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_01")
        self.assertEqual(
            node.store.resolve_calls[-1],
            {
                "map_name": "",
                "revision_id": "rev_demo_01",
                "robot_id": "robot_a",
            },
        )

    def test_rejects_map_revision_name_mismatch(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            }
        )

        with self.assertRaisesRegex(ValueError, "map revision does not match selected map"):
            node._resolve_asset("other_map", "rev_demo_01")

    def test_prefers_active_verified_revision_for_same_name(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_head",
                "enabled": True,
                "verification_status": "pending",
                "lifecycle_status": "saved_unverified",
            },
            active_asset={
                "map_name": "demo_map",
                "revision_id": "rev_demo_active",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
                "map_id": "map_active",
            },
        )

        asset = node._resolve_asset("demo_map")

        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_active")

    def test_prefers_verified_revision_scope_over_pending_head(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_head",
                "enabled": True,
                "verification_status": "pending",
                "lifecycle_status": "saved_unverified",
            },
            active_asset={
                "map_name": "other_map",
                "revision_id": "rev_other_active",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            },
            preferred_revision={
                "map_name": "demo_map",
                "revision_id": "rev_demo_verified",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
                "map_id": "map_demo_verified",
            },
        )

        asset = node._resolve_asset("demo_map")

        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_verified")
        self.assertEqual(
            node.store.resolve_revision_calls[-1],
            {
                "map_name": "demo_map",
                "robot_id": "robot_a",
            },
        )

    def test_resolve_alignment_accepts_revision_scoped_row_even_if_map_version_differs(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)
        node.default_aligned_frame = "site_map"
        node.store = _FakeAlignmentStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_id": "map_demo_new",
                "map_md5": "md5_demo_new",
                "frame_id": "map",
            },
            {
                "map_revision_id": "rev_demo_01",
                "map_name": "demo_map",
                "map_id": "map_demo_old",
                "map_version": "md5_demo_old",
                "alignment_version": "align_demo_01",
                "raw_frame": "map",
                "aligned_frame": "site_map",
                "yaw_offset_deg": 0.0,
                "pivot_x": 0.0,
                "pivot_y": 0.0,
                "source": "manual",
                "status": "active",
                "active": True,
            },
        )

        alignment = node._resolve_alignment(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_id": "map_demo_new",
                "map_md5": "md5_demo_new",
                "frame_id": "map",
            }
        )

        self.assertEqual(alignment.alignment_version, "align_demo_01")
        self.assertEqual(alignment.map_version, "md5_demo_old")

    def test_validate_asset_version_binding_prefers_revision_scope_over_stale_map_version(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)

        version = node._validate_asset_version_binding(
            {
                "revision_id": "rev_demo_01",
                "map_md5": "md5_demo_new",
            },
            "md5_demo_old",
        )

        self.assertEqual(version, "md5_demo_new")

    def test_validate_asset_version_binding_keeps_name_md5_guard_without_revision(self):
        node = SITE_EDITOR_MODULE.SiteEditorServiceNode.__new__(SITE_EDITOR_MODULE.SiteEditorServiceNode)

        with self.assertRaisesRegex(ValueError, "map_version does not match selected map asset"):
            node._validate_asset_version_binding(
                {
                    "revision_id": "",
                    "map_md5": "md5_demo_new",
                },
                "md5_demo_old",
            )


if __name__ == "__main__":
    unittest.main()
