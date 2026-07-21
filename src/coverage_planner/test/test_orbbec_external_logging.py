#!/usr/bin/env python3

import os
import unittest
import xml.etree.ElementTree as ET


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
ORBBEC_ROOT = os.path.join(REPO_ROOT, "src", "orbbec-ros-sdk")
LOG_DIR = "/var/log/doraemon/orbbec"


def read_repo_file(*parts):
    with open(os.path.join(REPO_ROOT, *parts), "r", encoding="utf-8-sig") as handle:
        return handle.read()


class OrbbecExternalLoggingContractTest(unittest.TestCase):
    def test_sdk_xml_uses_external_log_directory(self):
        config_path = os.path.join(ORBBEC_ROOT, "config", "OrbbecSDKConfig_v1.0.xml")
        log_config = ET.parse(config_path).find("./Log")
        self.assertIsNotNone(log_config)
        output_dir = log_config.findtext("OutputDir")
        self.assertEqual(LOG_DIR, output_dir)
        self.assertEqual("5", log_config.findtext("FileLogLevel"))

    def test_shared_logging_contract_has_fixed_external_path(self):
        source = read_repo_file(
            "src", "orbbec-ros-sdk", "include", "orbbec_camera", "logging.h"
        )
        self.assertIn('kOrbbecLogDirectory[] = "%s"' % LOG_DIR, source)
        self.assertIn("disableOrbbecSdkFileLogging", source)
        self.assertIn("OB_LOG_SEVERITY_OFF", source)
        self.assertIn("ob::Context::setLoggerToFile", source)

    def test_device_enumeration_configures_logger_before_context(self):
        source = read_repo_file("src", "orbbec-ros-sdk", "src", "list_devices_node.cpp")
        configure_at = source.index("disableOrbbecSdkFileLogging")
        construct_at = source.index("std::make_shared<ob::Context>")
        self.assertLess(configure_at, construct_at)
        self.assertNotIn("context->setLoggerSeverity", source)

    def test_pipeline_tools_configure_logger_before_pipeline(self):
        for filename in ("list_depth_work_mode.cpp", "list_camera_profile_mode.cpp"):
            with self.subTest(filename=filename):
                source = read_repo_file("src", "orbbec-ros-sdk", "src", filename)
                configure_at = source.index("disableOrbbecSdkFileLogging")
                construct_at = source.index("std::make_shared<ob::Pipeline>")
                self.assertLess(configure_at, construct_at)

    def test_camera_context_uses_external_logging_contract(self):
        source = read_repo_file("src", "orbbec-ros-sdk", "src", "ob_camera_node_driver.cpp")
        configure_at = source.index("disableOrbbecSdkFileLogging")
        construct_at = source.index("std::make_shared<ob::Context>")
        self.assertLess(configure_at, construct_at)
        self.assertNotIn('log_dir = "Log/"', source)

    def test_fatal_signal_handler_is_async_signal_safe(self):
        source = read_repo_file("src", "orbbec-ros-sdk", "src", "ob_camera_node_driver.cpp")
        header = read_repo_file(
            "src", "orbbec-ros-sdk", "include", "orbbec_camera", "ob_camera_node_driver.h"
        )
        cmake = read_repo_file("src", "orbbec-ros-sdk", "CMakeLists.txt")
        handler = source[
            source.index("void fatalSignalHandler"):source.index("void installFatalSignalHandlers")
        ]
        self.assertIn("::write(STDERR_FILENO", handler)
        self.assertIn("_exit(128 + signum)", handler)
        for forbidden in (
            "std::",
            "boost::",
            "backward::",
            "ros::",
            "ofstream",
            "ioctl",
            "USBDEVFS_RESET",
            "std::exit(",
            "\n  exit(",
        ):
            self.assertNotIn(forbidden, handler)
        install_at = source.index("installFatalSignalHandlers();")
        construct_at = source.index("std::make_shared<ob::Context>")
        self.assertLess(install_at, construct_at)
        for fatal_signal in ("SIGSEGV", "SIGABRT", "SIGBUS", "SIGFPE", "SIGILL"):
            self.assertIn("sigaction(%s" % fatal_signal, source)
        self.assertNotIn("sigaction(SIGINT", source)
        self.assertNotIn("sigaction(SIGTERM", source)
        self.assertNotIn("backward::SignalHandling", source)
        self.assertNotIn("backward-cpp/backward.hpp", header)
        self.assertNotIn("ORBBEC_BACKWARD_DEFINITIONS", cmake)

    def test_installer_and_verifier_enforce_external_directory(self):
        installer = read_repo_file("scripts", "install_doraemon_runtime_service.sh")
        verifier = read_repo_file("scripts", "verify_x86_ubuntu20_deployment.sh")
        filesystem_security = read_repo_file(
            "scripts", "commercial_filesystem_security.sh"
        )
        self.assertIn(LOG_DIR, installer)
        self.assertIn("external Orbbec log directory ownership and mode", verifier)
        self.assertIn("commercial_find_forbidden_release_artifact", verifier)
        self.assertIn("-iname log", filesystem_security)


if __name__ == "__main__":
    unittest.main()
