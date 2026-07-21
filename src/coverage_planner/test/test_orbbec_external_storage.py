#!/usr/bin/env python3

import os
import unittest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
ORBBEC_ROOT = os.path.join(REPO_ROOT, "src", "orbbec-ros-sdk")
CAPTURE_ROOT = "/var/lib/doraemon/orbbec-captures"


def read_orbbec_file(*parts):
    with open(os.path.join(ORBBEC_ROOT, *parts), "r", encoding="utf-8-sig") as handle:
        return handle.read()


class OrbbecExternalStorageContractTest(unittest.TestCase):
    def test_capture_contract_is_external_and_rejects_unsafe_components(self):
        storage = read_orbbec_file("include", "orbbec_camera", "storage.h")
        self.assertIn('kOrbbecCaptureDirectory[] = "%s"' % CAPTURE_ROOT, storage)
        self.assertIn("isSafeOrbbecCaptureComponent", storage)
        self.assertIn("component.find('/')", storage)
        self.assertIn("component.find('\\\\')", storage)
        self.assertIn("boost::filesystem::is_symlink", storage)
        self.assertIn("canonical_directory.parent_path() != canonical_root", storage)

    def test_camera_capture_services_never_use_the_working_directory(self):
        camera = read_orbbec_file("src", "ob_camera_node.cpp")
        self.assertIn('#include "orbbec_camera/storage.h"', camera)
        self.assertNotIn("boost::filesystem::current_path", camera)
        self.assertNotIn('current_path + "/image"', camera)
        self.assertNotIn('current_path + "/point_cloud"', camera)
        self.assertGreaterEqual(camera.count("makeOrbbecCapturePath"), 3)
        self.assertIn('makeOrbbecCapturePath("image"', camera)
        self.assertIn('makeOrbbecCapturePath("point_cloud"', camera)

    def test_capture_names_are_camera_scoped_and_collision_resistant(self):
        camera = read_orbbec_file("src", "ob_camera_node.cpp")
        self.assertGreaterEqual(
            camera.count("isSafeOrbbecCaptureComponent(camera_name_)"), 3
        )
        self.assertIn('camera_name_ + "_points_"', camera)
        self.assertIn('camera_name_ + "_colored_points_"', camera)
        self.assertIn(
            'camera_name_ + "_" + stream_name_[stream_index] + "_"', camera
        )

        timestamp = "20260721_153000"
        image_suffix = "depth_640x400_15hz_%s_0.raw" % timestamp
        camera_names = ("gemini_cf", "gemini_nj", "gemini_front")
        image_names = {"%s_%s" % (name, image_suffix) for name in camera_names}
        point_names = {"%s_points_%s.ply" % (name, timestamp) for name in camera_names}
        self.assertEqual(len(camera_names), len(image_names))
        self.assertEqual(len(camera_names), len(point_names))

    def test_storage_failures_are_explicit_and_stop_repeated_image_writes(self):
        camera = read_orbbec_file("src", "ob_camera_node.cpp")
        self.assertIn("Failed to prepare point cloud capture path", camera)
        self.assertIn("Failed to prepare colored point cloud capture path", camera)
        self.assertIn("Failed to prepare image capture path", camera)
        failure_at = camera.index("Failed to prepare image capture path")
        disable_at = camera.rfind("save_images_[stream_index] = false", 0, failure_at)
        self.assertNotEqual(-1, disable_at)


if __name__ == "__main__":
    unittest.main()
