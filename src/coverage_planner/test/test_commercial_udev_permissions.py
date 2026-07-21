#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pathlib
import re
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
ORBBEC_RULES = REPO_ROOT / "src/orbbec-ros-sdk/scripts/99-obsensor-ros1-libusb.rules"
BLUESEA_RULES = REPO_ROOT / "src/bluesea2/script/LHLiDAR.rules"
WHEELTEC_VENDOR_HELPER = REPO_ROOT / "src/fdilink_ahrs/wheeltec_udev.sh"
SHIPPED_UDEV_RULES = tuple(
    sorted((REPO_ROOT / "deploy").rglob("*.rules"))
    + sorted((REPO_ROOT / "src").rglob("*.rules"))
) + (
    REPO_ROOT / "deploy/udev/99-doraemon-a26022-serial.rules.example",
)
UNSAFE_GENERATED_UDEV_MODE = re.compile(
    r'\bMODE\s*:?=\s*["\']?0?(?:666|664|777)(?:["\']|\b)', re.IGNORECASE
)
UNSAFE_DEVICE_CHMOD = re.compile(r'\bchmod\s+0?777\b', re.IGNORECASE)


def active_rule_lines(path):
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


class CommercialUdevPermissionsTest(unittest.TestCase):
    def test_wheeltec_vendor_helper_is_retired_without_a_fixed_identity(self):
        source = WHEELTEC_VENDOR_HELPER.read_text(encoding="utf-8")
        for forbidden in (
            "/etc/udev/rules.d",
            "service udev",
            'serial}=="0003"',
            "SYMLINK",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn("deploy/udev/99-doraemon-a26022-serial.rules.example", source)
        self.assertRegex(source, r"(?m)^exit 2$")

    def test_shell_helpers_cannot_generate_world_writable_device_rules(self):
        shell_roots = (REPO_ROOT / "scripts", REPO_ROOT / "deploy", REPO_ROOT / "src")
        shell_files = sorted(
            path for root in shell_roots for path in root.rglob("*.sh")
        )
        self.assertTrue(shell_files)
        for path in shell_files:
            source = path.read_text(encoding="utf-8", errors="replace")
            with self.subTest(path=str(path.relative_to(REPO_ROOT))):
                self.assertIsNone(UNSAFE_GENERATED_UDEV_MODE.search(source))
                self.assertIsNone(UNSAFE_DEVICE_CHMOD.search(source))

    def test_commercial_hardware_rules_are_not_world_writable(self):
        self.assertTrue(SHIPPED_UDEV_RULES)
        for path in SHIPPED_UDEV_RULES:
            with self.subTest(path=str(path.relative_to(REPO_ROOT))):
                rules = active_rule_lines(path)
                self.assertTrue(rules, msg="commercial udev rule file is empty")
                for line_number, rule in enumerate(rules, start=1):
                    self.assertIn(
                        'MODE:="0660"',
                        rule,
                        msg="commercial udev rule must use exact 0660 at active rule %d"
                        % line_number,
                    )
                    self.assertIsNone(
                        UNSAFE_GENERATED_UDEV_MODE.search(rule),
                        msg="unsafe commercial udev mode at active rule %d"
                        % line_number,
                    )

    def test_orbbec_rules_are_video_group_read_write_only(self):
        rules = active_rule_lines(ORBBEC_RULES)
        self.assertTrue(rules, msg="Orbbec udev rule file is empty")
        for line_number, rule in enumerate(rules, start=1):
            self.assertIn(
                'ATTRS{idVendor}=="2bc5"' if "SUBSYSTEMS" in rule else 'ATTR{idVendor}=="2bc5"',
                rule,
                msg="unexpected non-Orbbec rule at active rule %d" % line_number,
            )
            self.assertIn(
                'GROUP:="video"',
                rule,
                msg="Orbbec rule lacks video group at active rule %d" % line_number,
            )
            self.assertIn(
                'MODE:="0660"',
                rule,
                msg="Orbbec rule lacks 0660 mode at active rule %d" % line_number,
            )

    def test_wheeltec_rule_uses_dialout_group_and_0660(self):
        path = REPO_ROOT / "deploy/udev/99-doraemon-wheeltec.rules"
        rules = active_rule_lines(path)
        self.assertEqual(len(rules), 1)
        self.assertIn('GROUP:="dialout"', rules[0])
        self.assertIn('MODE:="0660"', rules[0])

    def test_bluesea_vendor_rule_does_not_create_a_generic_alias(self):
        rules = active_rule_lines(BLUESEA_RULES)
        self.assertEqual(len(rules), 2)
        for rule in rules:
            self.assertIn('GROUP:="dialout"', rule)
            self.assertIn('MODE:="0660"', rule)
            self.assertNotIn("SYMLINK", rule)


if __name__ == "__main__":
    unittest.main()
