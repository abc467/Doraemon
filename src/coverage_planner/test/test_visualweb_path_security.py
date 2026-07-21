#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
VISUALWEB_HEADER = (
    REPO_ROOT
    / "src"
    / "cartographer_ros"
    / "cartographer_ros"
    / "cartographer_ros"
    / "visualweb.h"
)
CPP_TEST = VISUALWEB_HEADER.with_name("visualweb_path_security_test.cc")


def function_body(source, signature, next_signature):
    start = source.index(signature)
    end = source.index(next_signature, start)
    return source[start:end]


class VisualwebPathSecuritySourceTest(unittest.TestCase):
    def setUp(self):
        self.source = VISUALWEB_HEADER.read_text(encoding="utf-8")

    def test_config_entry_is_checked_before_path_construction(self):
        allowlist = function_body(
            self.source,
            "static bool is_allowed_config_entry",
            "static boost::filesystem::path commercial_map_root_path",
        )
        for entry in ("slam", "pure_location", "pure_location_odom"):
            self.assertIn('config_entry == "%s"' % entry, allowlist)

        init = function_body(
            self.source,
            "bool init_from_config",
            "static CommandResult make_result",
        )
        validation = init.index("is_allowed_config_entry(config_entry)")
        path_join = init.index("this->config_root) / config_entry")
        self.assertLess(validation, path_join)

    def test_file_commands_are_anchored_to_commercial_map_root(self):
        self.assertIn(
            'return boost::filesystem::path("/data/maps")', self.source
        )
        for handler, next_handler in (
            ("CommandResult handle_load_state", "CommandResult handle_save_state"),
            ("CommandResult handle_save_state", "CommandResult handle_get_dirlist"),
            ("CommandResult handle_get_dirlist", "CommandResult handle_add_trajectory"),
        ):
            body = function_body(self.source, handler, next_handler)
            self.assertIn("commercial_map_root_path()", body)

        dirlist = function_body(
            self.source,
            "CommandResult handle_get_dirlist",
            "CommandResult handle_add_trajectory",
        )
        self.assertNotIn("this->slam_root", dirlist)

    def test_canonical_containment_and_file_types_are_enforced(self):
        load = function_body(
            self.source,
            "static bool resolve_existing_pbstream_path_for_root",
            "static bool resolve_pbstream_save_path_for_root",
        )
        self.assertIn("boost::filesystem::canonical(candidate", load)
        self.assertIn("is_canonical_path_contained(canonical_candidate", load)
        self.assertIn('canonical_candidate.extension() != ".pbstream"', load)
        self.assertIn("is_regular_file(canonical_status)", load)
        self.assertNotIn("is_symlink(candidate_status)", load)

        save = function_body(
            self.source,
            "static bool resolve_pbstream_save_path_for_root",
            "static bool resolve_map_directory_path_for_root",
        )
        self.assertIn("canonical(candidate.parent_path()", save)
        self.assertIn('target_filename.extension() != ".pbstream"', save)
        self.assertIn("is_symlink(target_status)", save)
        self.assertIn("is_regular_file(target_status)", save)

        directory = function_body(
            self.source,
            "static bool resolve_map_directory_path_for_root",
            "static bool has_config_layout",
        )
        self.assertIn("has_parent_reference(requested)", directory)
        self.assertIn("boost::filesystem::canonical(candidate", directory)
        self.assertIn("is_canonical_path_contained(canonical_candidate", directory)

    def test_cpp_regression_models_runtime_asset_symlinks(self):
        tests = CPP_TEST.read_text(encoding="utf-8")
        self.assertIn('CreateSymlink(maps, "linked.pbstream", inside_file)', tests)
        self.assertIn(
            'CreateSymlink(maps, "outside_link.pbstream", outside_file)', tests
        )
        self.assertIn('"dangling.pbstream"', tests)
        self.assertIn('"escape/outside.pbstream"', tests)
        self.assertIn('"special.pbstream"', tests)
        self.assertIn('"sub/../sub"', tests)


if __name__ == "__main__":
    unittest.main()
