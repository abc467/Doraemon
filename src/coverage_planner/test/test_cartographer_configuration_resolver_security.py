#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
RESOLVER_SOURCE = (
    REPO_ROOT
    / "src"
    / "cartographer"
    / "cartographer"
    / "common"
    / "configuration_file_resolver.cc"
)
RESOLVER_TEST_SOURCE = RESOLVER_SOURCE.with_name(
    "configuration_file_resolver_test.cc"
)


class CartographerConfigurationResolverSecurityTest(unittest.TestCase):
    def test_basename_is_validated_before_directory_search(self):
        source = RESOLVER_SOURCE.read_text(encoding="utf-8")
        validation = source.index("CheckIsSafeBasename(basename);")
        directory_search = source.index(
            "for (const auto& path : configuration_files_directories_)",
            validation,
        )
        self.assertLess(validation, directory_search)

        for guard in (
            "basename.empty()",
            'basename == "."',
            'basename == ".."',
            "basename.find('/')",
            "basename.find('\\\\')",
            "basename.find('\\0')",
        ):
            self.assertIn(guard, source)

    def test_resolved_candidate_is_contained_and_regular_before_return(self):
        source = RESOLVER_SOURCE.read_text(encoding="utf-8")
        canonical_candidate = source.index(
            'RealPathOrDie(candidate, "configuration file candidate")'
        )
        containment = source.index(
            "IsPathContainedInDirectory(canonical_candidate,",
            canonical_candidate,
        )
        regular_file = source.index("S_ISREG(candidate_status.st_mode)", containment)
        returned = source.index("return canonical_candidate;", regular_file)

        self.assertLess(canonical_candidate, containment)
        self.assertLess(containment, regular_file)
        self.assertLess(regular_file, returned)

    def test_cpp_regression_covers_traversal_and_symlink_escape(self):
        tests = RESOLVER_TEST_SOURCE.read_text(encoding="utf-8")
        for regression in (
            "RejectsUnsafeBasenames",
            "RejectsNonRegularFile",
            "AllowsContainedSymlink",
            "RejectsSymlinkEscapeWithoutFallback",
            'GetFullPathOrDie("nested/robot.lua")',
            "basename_with_nul.push_back('\\0')",
        ):
            self.assertIn(regression, tests)


if __name__ == "__main__":
    unittest.main()
