#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.map_path_security import (  # noqa: E402
    MapPathSecurityError,
    copy_regular_file_exclusive,
    ensure_secure_parent_directory,
    map_asset_target_paths,
    resolve_yaml_image_path,
    validate_existing_regular_file,
    validate_map_name,
    validate_new_file_target,
    validate_revision_id,
)


def _load_runtime_assets_helper():
    module_path = os.path.join(
        SRC_DIR,
        "coverage_planner",
        "slam_workflow",
        "runtime_assets.py",
    )
    spec = importlib.util.spec_from_file_location("map_path_security_runtime_assets", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.CartographerRuntimeAssetHelper


CartographerRuntimeAssetHelper = _load_runtime_assets_helper()


def _load_flirt_migration():
    module_path = os.path.join(
        SRC_DIR,
        "coverage_planner",
        "slam_workflow",
        "flirt_pbstream_migration.py",
    )
    spec = importlib.util.spec_from_file_location("map_path_security_flirt_migration", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


FLIRT_MIGRATION = _load_flirt_migration()


class MapComponentValidationTest(unittest.TestCase):
    def test_valid_ascii_ids_and_optional_pbstream_suffix(self):
        self.assertEqual(validate_map_name("demo_map-1.2"), "demo_map-1.2")
        self.assertEqual(validate_map_name("demo.pbstream"), "demo")
        self.assertEqual(validate_map_name("A" * 128), "A" * 128)
        self.assertEqual(validate_map_name("", allow_empty=True), "")
        self.assertEqual(validate_revision_id("rev_demo_01"), "rev_demo_01")

    def test_rejects_traversal_absolute_separators_controls_unicode_and_length(self):
        invalid_names = (
            "",
            ".",
            "..",
            "../demo",
            "demo/child",
            "demo\\child",
            "/tmp/demo",
            "demo\x00suffix",
            "demo\n",
            "地图",
            "-demo",
            "a" * 129,
            ".pbstream",
        )
        for value in invalid_names:
            with self.subTest(value=repr(value)):
                with self.assertRaises(MapPathSecurityError) as raised:
                    validate_map_name(value)
                self.assertEqual(raised.exception.code, "invalid_map_name")

    def test_revision_id_uses_the_same_single_component_policy(self):
        for value in ("..", "rev/escape", "rev\\escape", "rev\x00bad", "r" * 129):
            with self.subTest(value=repr(value)):
                with self.assertRaises(MapPathSecurityError) as raised:
                    validate_revision_id(value)
                self.assertEqual(raised.exception.code, "invalid_map_revision_id")


class MapContainmentTest(unittest.TestCase):
    def test_target_paths_are_revision_scoped_and_contained(self):
        with tempfile.TemporaryDirectory() as root:
            paths = map_asset_target_paths(root, "demo", "rev_demo_01")
            expected_parent = os.path.join(root, "revisions", "demo", "rev_demo_01")
            self.assertEqual(paths["pbstream_path"], os.path.join(expected_parent, "demo.pbstream"))
            for path in paths.values():
                self.assertEqual(os.path.commonpath((root, path)), root)

    def test_target_paths_reject_traversal_and_symlinked_parent(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            with self.assertRaises(MapPathSecurityError):
                map_asset_target_paths(root, "../../../escape", "rev_safe_01")
            os.symlink(outside, os.path.join(root, "revisions"))
            with self.assertRaises(MapPathSecurityError):
                map_asset_target_paths(root, "demo", "rev_demo_01")

    def test_secure_parent_creation_rejects_parent_symlink(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            target = os.path.join(root, "revisions", "demo", "rev_01", "demo.pbstream")
            ensure_secure_parent_directory(root, target)
            self.assertTrue(os.path.isdir(os.path.dirname(target)))

            link_root = os.path.join(root, "unsafe")
            os.symlink(outside, link_root)
            with self.assertRaises(MapPathSecurityError):
                ensure_secure_parent_directory(root, os.path.join(link_root, "demo.pbstream"))

    def test_existing_source_rejects_symlink_dangling_and_fifo(self):
        with tempfile.TemporaryDirectory() as root:
            regular = os.path.join(root, "regular.pbstream")
            with open(regular, "wb") as handle:
                handle.write(b"pbstream")
            self.assertEqual(
                validate_existing_regular_file(root, regular, suffix=".pbstream"),
                regular,
            )

            symlink = os.path.join(root, "symlink.pbstream")
            os.symlink(regular, symlink)
            dangling = os.path.join(root, "dangling.pbstream")
            os.symlink(os.path.join(root, "missing.pbstream"), dangling)
            fifo = os.path.join(root, "fifo.pbstream")
            os.mkfifo(fifo)
            for path in (symlink, dangling, fifo):
                with self.subTest(path=path):
                    with self.assertRaises(MapPathSecurityError):
                        validate_existing_regular_file(root, path, suffix=".pbstream")

    def test_new_pbstream_sink_rejects_escape_existing_specials_and_symlink_parent(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            valid = os.path.join(root, "new.pbstream")
            self.assertEqual(
                validate_new_file_target(root, valid, suffix=".pbstream"),
                valid,
            )
            with self.assertRaises(MapPathSecurityError):
                validate_new_file_target(root, os.path.join(outside, "escape.pbstream"), suffix=".pbstream")
            with self.assertRaises(MapPathSecurityError):
                validate_new_file_target(root, os.path.join(root, "wrong.txt"), suffix=".pbstream")

            existing = os.path.join(root, "existing.pbstream")
            with open(existing, "wb") as handle:
                handle.write(b"old")
            dangling = os.path.join(root, "dangling.pbstream")
            os.symlink(os.path.join(root, "missing.pbstream"), dangling)
            fifo = os.path.join(root, "fifo.pbstream")
            os.mkfifo(fifo)
            for path in (existing, dangling, fifo):
                with self.subTest(path=path):
                    with self.assertRaises(MapPathSecurityError):
                        validate_new_file_target(root, path, suffix=".pbstream")

            os.symlink(outside, os.path.join(root, "linked_parent"))
            with self.assertRaises(MapPathSecurityError):
                validate_new_file_target(
                    root,
                    os.path.join(root, "linked_parent", "new.pbstream"),
                    suffix=".pbstream",
                )

    def test_yaml_image_must_be_relative_contained_regular_non_symlink_pgm(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as outside:
            yaml_path = os.path.join(root, "demo.yaml")
            image_path = os.path.join(root, "demo.pgm")
            with open(yaml_path, "w", encoding="utf-8") as handle:
                handle.write("image: demo.pgm\n")
            with open(image_path, "wb") as handle:
                handle.write(b"P5\n1 1\n255\n\x00")
            self.assertEqual(resolve_yaml_image_path(root, yaml_path, "demo.pgm"), image_path)

            outside_image = os.path.join(outside, "outside.pgm")
            with open(outside_image, "wb") as handle:
                handle.write(b"P5\n1 1\n255\n\x00")
            symlink_image = os.path.join(root, "linked.pgm")
            os.symlink(outside_image, symlink_image)
            fifo_image = os.path.join(root, "fifo.pgm")
            os.mkfifo(fifo_image)
            invalid = (
                outside_image,
                "../outside.pgm",
                "sub/../demo.pgm",
                "sub\\demo.pgm",
                "linked.pgm",
                "fifo.pgm",
            )
            for reference in invalid:
                with self.subTest(reference=reference):
                    with self.assertRaises(MapPathSecurityError):
                        resolve_yaml_image_path(root, yaml_path, reference)

    def test_exclusive_copy_accepts_only_regular_contained_source_and_fresh_target(self):
        with tempfile.TemporaryDirectory() as source_root, tempfile.TemporaryDirectory() as target_root:
            source = os.path.join(source_root, "demo.pbstream")
            target = os.path.join(target_root, "demo.pbstream")
            with open(source, "wb") as handle:
                handle.write(b"safe-pbstream")
            copy_regular_file_exclusive(
                source_root,
                source,
                target_root,
                target,
                suffix=".pbstream",
            )
            with open(target, "rb") as handle:
                self.assertEqual(handle.read(), b"safe-pbstream")
            with self.assertRaises(MapPathSecurityError):
                copy_regular_file_exclusive(
                    source_root,
                    source,
                    target_root,
                    target,
                    suffix=".pbstream",
                )


class RuntimeAssetLinkTest(unittest.TestCase):
    @staticmethod
    def _write_source(root: str, revision: str, content: bytes) -> str:
        directory = os.path.join(root, "revisions", "demo", revision)
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, "demo.pbstream")
        with open(path, "wb") as handle:
            handle.write(content)
        return path

    def test_repo_link_is_contained_repeatable_and_repointable(self):
        with tempfile.TemporaryDirectory() as maps_root:
            backend = SimpleNamespace(maps_root=maps_root, repo_map_root=maps_root)
            helper = CartographerRuntimeAssetHelper(backend)
            first = self._write_source(maps_root, "rev_demo_01", b"one")
            second = self._write_source(maps_root, "rev_demo_02", b"two")

            self.assertEqual(
                helper.ensure_repo_map_link({"map_name": "demo", "pbstream_path": first}),
                "demo.pbstream",
            )
            link_path = os.path.join(maps_root, "demo.pbstream")
            self.assertTrue(os.path.islink(link_path))
            self.assertEqual(os.path.realpath(link_path), first)
            helper.ensure_repo_map_link({"map_name": "demo", "pbstream_path": first})
            helper.ensure_repo_map_link({"map_name": "demo", "pbstream_path": second})
            self.assertEqual(os.path.realpath(link_path), second)

    def test_repo_link_rejects_dangling_outside_and_special_existing_target(self):
        with tempfile.TemporaryDirectory() as maps_root, tempfile.TemporaryDirectory() as outside:
            backend = SimpleNamespace(maps_root=maps_root, repo_map_root=maps_root)
            helper = CartographerRuntimeAssetHelper(backend)
            source = self._write_source(maps_root, "rev_demo_01", b"one")
            target = os.path.join(maps_root, "demo.pbstream")

            os.symlink(os.path.join(maps_root, "missing.pbstream"), target)
            with self.assertRaises(MapPathSecurityError):
                helper.ensure_repo_map_link({"map_name": "demo", "pbstream_path": source})
            self.assertTrue(os.path.lexists(target))
            os.unlink(target)

            outside_file = os.path.join(outside, "outside.pbstream")
            with open(outside_file, "wb") as handle:
                handle.write(b"outside")
            os.symlink(outside_file, target)
            with self.assertRaises(MapPathSecurityError):
                helper.ensure_repo_map_link({"map_name": "demo", "pbstream_path": source})
            os.unlink(target)

            os.mkfifo(target)
            with self.assertRaises(MapPathSecurityError):
                helper.ensure_repo_map_link({"map_name": "demo", "pbstream_path": source})


class FlirtMigrationPathGuardTest(unittest.TestCase):
    class _Store(object):
        def __init__(self, source):
            self.source = dict(source)

        def resolve_map_revision(self, *, revision_id):
            if revision_id == self.source.get("revision_id"):
                return dict(self.source)
            return None

        @staticmethod
        def generate_map_revision_id(_map_name):
            return "rev_demo_target"

    def _migrator(self, maps_root, source):
        backend = SimpleNamespace(maps_root=maps_root, repo_map_root=maps_root)
        assets = CartographerRuntimeAssetHelper(backend)
        runtime_probe = lambda: {
            "current_map_revision_id": source.get("revision_id"),
            "current_pbstream_path": source.get("pbstream_path"),
            "flirt_feature_backfill_state": "READY",
        }
        return FLIRT_MIGRATION.FlirtPbstreamRevisionMigrator(
            plan_store=self._Store(source),
            asset_helper=assets,
            runtime_probe=runtime_probe,
            state_writer=lambda *_args, **_kwargs: True,
            validator=lambda *_args, **_kwargs: {},
        )

    def test_prepare_accepts_only_contained_regular_source_and_safe_revision(self):
        with tempfile.TemporaryDirectory() as maps_root, tempfile.TemporaryDirectory() as outside:
            source_path = RuntimeAssetLinkTest._write_source(
                maps_root,
                "rev_demo_source",
                b"source",
            )
            source = {
                "revision_id": "rev_demo_source",
                "map_name": "demo",
                "pbstream_path": source_path,
            }
            plan = self._migrator(maps_root, source).prepare(
                source_revision_id="rev_demo_source",
                target_revision_id="rev_demo_target",
            )
            self.assertEqual(
                os.path.commonpath((maps_root, plan["target_pbstream_path"])),
                maps_root,
            )

            outside_path = os.path.join(outside, "demo.pbstream")
            with open(outside_path, "wb") as handle:
                handle.write(b"outside")
            unsafe_source = dict(source, pbstream_path=outside_path)
            with self.assertRaises(FLIRT_MIGRATION.FlirtPbstreamMigrationError) as raised:
                self._migrator(maps_root, unsafe_source).prepare(
                    source_revision_id="rev_demo_source",
                )
            self.assertEqual(raised.exception.code, "source_pbstream_invalid")

            with self.assertRaises(FLIRT_MIGRATION.FlirtPbstreamMigrationError) as raised:
                self._migrator(maps_root, source).prepare(
                    source_revision_id="rev_demo_source",
                    target_revision_id="../escape",
                )
            self.assertEqual(raised.exception.code, "target_revision_invalid")


if __name__ == "__main__":
    unittest.main()
