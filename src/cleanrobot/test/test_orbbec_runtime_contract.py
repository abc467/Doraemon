#!/usr/bin/env python3

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]


class OrbbecRuntimeContractTest(unittest.TestCase):
    def read(self, relative):
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_cameras_start_left_right_front_with_frame_and_identity_gates(self):
        source = self.read("scripts/start_runtime.sh")
        left = source.index("start_depth_camera_window depth_left")
        right = source.index("start_depth_camera_window depth_right")
        front = source.index("start_depth_camera_window depth_front")
        self.assertLess(left, right)
        self.assertLess(right, front)
        camera_fn = source[source.index("start_depth_camera_window()") : source.index("start_depth_cameras_sequentially()")]
        self.assertIn("depth/image_raw", camera_fn)
        self.assertIn("runtime_wait_for_consecutive_topic_samples", camera_fn)
        self.assertIn("and any(m.data)", camera_fn)
        self.assertIn("m.point_step > 0", camera_fn)
        # The legacy Gemini Max side cameras can still publish mostly/all-zero
        # depth with the SDK filter, even on independent motherboard USB3 ports.
        self.assertEqual(source.count('false Y11 false'), 2)
        self.assertNotIn('false Y11 true', source)
        self.assertIn('true Y16 true', source)
        self.assertIn("runtime_require_orbbec_serial", camera_fn)

    def test_commercial_template_enables_all_fail_closed_gates(self):
        config = self.read("config/runtime.a26022.env")
        for assignment in (
            "RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true",
            "DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true",
            "RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE=true",
            "RUNTIME_DEPTH_CAMERA_PAUSE_AND_RECOVER_ON_STALE=false",
        ):
            self.assertIn(assignment, config)

    def test_watchdog_observes_all_raw_streams_and_never_auto_resumes(self):
        source = self.read("src/cleanrobot/scripts/orbbec_camera_watchdog_node.py")
        for camera in ("gemini_cf", "gemini_nj", "gemini_front"):
            self.assertIn("/%s/depth/image_raw" % camera, source)
            self.assertIn("/%s/depth/points" % camera, source)
        self.assertIn("ExeTaskRequest.PAUSE", source)
        self.assertNotIn("ExeTaskRequest.CONTINUE", source)
        self.assertIn("_frame_has_content", source)
        self.assertIn("raw depth stream stale or empty", source)
        self.assertIn("_wait_until_stopped", source)
        self.assertIn("continuous 1s stopped odometry proof", source)
        self.assertIn('rospy.Subscriber(\n            "/task_state", TaskState', source)
        self.assertIn("_task_is_definitely_idle", source)
        self.assertIn("skipped executor pause fallback: no running mission", source)
        self.assertIn('rospy.get_param("~pause_and_recover_on_stale", True)', source)
        self.assertIn("monitoring-only mode keeps the task running", source)

    def test_watchdog_monitor_only_policy_is_runtime_configurable(self):
        source = self.read("scripts/start_runtime.sh")
        self.assertIn(
            'RUNTIME_DEPTH_CAMERA_PAUSE_AND_RECOVER_ON_STALE="${RUNTIME_DEPTH_CAMERA_PAUSE_AND_RECOVER_ON_STALE:-true}"',
            source,
        )
        self.assertIn(
            '_pause_and_recover_on_stale:="${RUNTIME_DEPTH_CAMERA_PAUSE_AND_RECOVER_ON_STALE}"',
            source,
        )

    def test_startup_owns_camera_chain_until_hard_gate_passes(self):
        source = self.read("scripts/start_runtime.sh")
        session_fn = source[
            source.index("start_runtime_session()") : source.index("clear_previous_runtime()")
        ]
        self.assertIn("start_depth_cameras_with_startup_recovery", session_fn)
        self.assertNotIn("start_depth_camera_watchdog", session_fn)
        finalize_fn = source[
            source.index("finalize_depth_camera_supervision()") :
            source.index("start_runtime_session()")
        ]
        self.assertEqual(finalize_fn.count("ensure_depth_camera_chain_ready"), 1)
        self.assertEqual(finalize_fn.count("start_depth_camera_watchdog"), 1)
        self.assertLess(
            finalize_fn.index("ensure_depth_camera_chain_ready"),
            finalize_fn.index("start_depth_camera_watchdog"),
        )
        self.assertEqual(source.count("start_depth_camera_watchdog\n"), 1)
        self.assertEqual(source.count("finalize_depth_camera_supervision\n"), 4)

    def test_startup_camera_recovery_is_single_bounded_and_serial(self):
        source = self.read("scripts/start_runtime.sh")
        recovery_fn = source[
            source.index("recover_depth_camera_chain_once_during_startup()") :
            source.index("start_depth_camera_window()")
        ]
        self.assertEqual(recovery_fn.count("restart_orbbec_camera_chain.sh"), 1)
        self.assertIn("DORAEMON_ORBBEC_RECOVERY_CONTEXT=startup", recovery_fn)
        self.assertIn("DEPTH_CAMERA_STARTUP_RECOVERY_USED=1", recovery_fn)
        self.assertNotIn("start_depth_camera_watchdog", recovery_fn)
        ensure_fn = source[
            source.index("ensure_depth_camera_chain_ready()") :
            source.index("recover_depth_camera_chain_once_during_startup()")
        ]
        self.assertGreaterEqual(ensure_fn.count("verify_depth_camera_chain"), 2)
        self.assertIn("recover_depth_camera_chain_once_during_startup", ensure_fn)

    def test_hard_gate_requires_consecutive_content_and_identity(self):
        source = self.read("scripts/start_runtime.sh")
        verify_fn = source[
            source.index("verify_depth_camera_chain()") :
            source.index("ensure_depth_camera_chain_ready()")
        ]
        for camera in ("gemini_cf", "gemini_nj", "gemini_front"):
            self.assertIn("/%s/depth/image_raw" % camera, verify_fn)
            self.assertIn("/%s/depth/points" % camera, verify_fn)
            self.assertIn("runtime_require_orbbec_serial %s" % camera, verify_fn)
        self.assertEqual(verify_fn.count("runtime_wait_for_consecutive_topic_samples"), 6)
        self.assertIn("and any(m.data)", verify_fn)
        self.assertIn("m.point_step > 0", verify_fn)

    def test_driver_lock_is_owner_death_safe(self):
        source = self.read("src/orbbec-ros-sdk/src/ob_camera_node_driver.cpp")
        self.assertIn("flock(orb_device_lock_shm_fd_, LOCK_EX | LOCK_NB)", source)
        self.assertIn("flock(orb_device_lock_shm_fd_, LOCK_UN)", source)
        self.assertNotIn("pthread_mutex_trylock", source)
        self.assertNotIn("pthread_mutex_init", source)

    def test_usb_preflight_is_required_by_runtime(self):
        unit = self.read("deploy/systemd/doraemon-runtime.service")
        self.assertIn("Requires=doraemon-orbbec-usb-preflight.service", unit)
        preflight = self.read("scripts/configure_orbbec_usb.sh")
        self.assertIn("MIN_USBFS_MEMORY_MB", preflight)
        self.assertIn("usbfs_memory_mb", preflight)


if __name__ == "__main__":
    unittest.main()
