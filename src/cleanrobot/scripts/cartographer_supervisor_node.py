#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shlex
import signal
import subprocess
import time

import rospy
from std_srvs.srv import Trigger, TriggerResponse


def _bool_flag(value):
    return "true" if bool(value) else "false"


class CartographerSupervisorNode:
    def __init__(self):
        self.runtime_ns = str(rospy.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/")
        self.cartographer_node_bin = str(
            rospy.get_param("~cartographer_node_bin", "/opt/carto/bin/cartographer/cartographer_node")
        ).strip()
        self.occupancy_grid_node_bin = str(
            rospy.get_param("~occupancy_grid_node_bin", "/opt/carto/bin/cartographer/cartographer_occupancy_grid_node")
        ).strip()
        self.config_root = os.path.expanduser(str(rospy.get_param("~config_root", ""))).strip()
        self.mapping_config_subdir = str(rospy.get_param("~mapping_config_subdir", "slam")).strip() or "slam"
        self.localization_config_subdir = (
            str(rospy.get_param("~localization_config_subdir", "pure_location")).strip() or "pure_location"
        )
        self.configuration_basename = str(rospy.get_param("~configuration_basename", "config.lua")).strip() or "config.lua"
        self.default_mode = str(rospy.get_param("~mode", "localization")).strip().lower() or "localization"
        self.default_map_name = str(rospy.get_param("~map_name", "")).strip()
        self.default_map_version = str(rospy.get_param("~map_version", "")).strip()
        self.default_pbstream_path = os.path.expanduser(str(rospy.get_param("~pbstream_path", ""))).strip()
        self.auto_start = bool(rospy.get_param("~auto_start", False))
        self.reload_service_name = str(rospy.get_param("~reload_service", "/cartographer/runtime/reload")).strip()
        self.save_state_service_name = str(
            rospy.get_param("~save_state_service", "/cartographer/runtime/save_state")
        ).strip()
        self.write_state_service_name = str(rospy.get_param("~write_state_service_name", "/write_state")).strip()
        self.landmark_param_yaml = os.path.expanduser(str(rospy.get_param("~landmark_param_yaml", ""))).strip()
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.map_resolution = float(rospy.get_param("~resolution", 0.05))
        self.publish_period_sec = float(rospy.get_param("~publish_period_sec", 1.0))
        self.include_frozen_submaps = bool(rospy.get_param("~include_frozen_submaps", True))
        self.include_unfrozen_submaps = bool(rospy.get_param("~include_unfrozen_submaps", False))
        self.startup_delay_s = max(0.0, float(rospy.get_param("~startup_delay_s", 0.5)))
        self.stop_timeout_s = max(0.5, float(rospy.get_param("~stop_timeout_s", 5.0)))

        self._node_proc = None
        self._grid_proc = None

        rospy.Service(self.reload_service_name, Trigger, self._handle_reload)
        rospy.Service(self.save_state_service_name, Trigger, self._handle_save_state)
        rospy.on_shutdown(self._shutdown)

        rospy.loginfo(
            "[cartographer_supervisor] ready mode=%s auto_start=%s config_root=%s",
            self.default_mode,
            str(self.auto_start),
            self.config_root,
        )
        if self.auto_start:
            try:
                self._restart_runtime()
            except Exception as e:
                rospy.logerr("[cartographer_supervisor] auto start failed: %s", str(e))

    def _runtime_param(self, key):
        return self.runtime_ns + "/" + str(key or "").strip()

    def _desired_runtime(self):
        mode = str(rospy.get_param(self._runtime_param("mode"), self.default_mode)).strip().lower() or self.default_mode
        map_name = str(rospy.get_param(self._runtime_param("map_name"), self.default_map_name)).strip()
        map_version = str(rospy.get_param(self._runtime_param("map_version"), self.default_map_version)).strip()
        pbstream_path = os.path.expanduser(
            str(rospy.get_param(self._runtime_param("pbstream_path"), self.default_pbstream_path)).strip()
        )
        if mode not in ("mapping", "localization"):
            raise RuntimeError("unsupported cartographer mode=%s" % mode)
        config_subdir = self.localization_config_subdir if mode == "localization" else self.mapping_config_subdir
        config_dir = os.path.join(self.config_root, config_subdir)
        if not os.path.isdir(config_dir):
            raise RuntimeError("cartographer config dir missing: %s" % config_dir)
        if mode == "localization" and not pbstream_path:
            raise RuntimeError("localization mode requires pbstream_path")
        if pbstream_path and not os.path.exists(pbstream_path):
            raise RuntimeError("pbstream not found: %s" % pbstream_path)
        return {
            "mode": mode,
            "map_name": map_name,
            "map_version": map_version,
            "pbstream_path": pbstream_path,
            "config_dir": config_dir,
        }

    def _build_cartographer_cmd(self, desired):
        cmd = [
            self.cartographer_node_bin,
            "-configuration_directory=%s" % str(desired["config_dir"]),
            "-configuration_basename=%s" % self.configuration_basename,
        ]
        if str(desired["mode"]) == "localization":
            cmd.extend(
                [
                    "-load_state_filename=%s" % str(desired["pbstream_path"]),
                    "-load_frozen_state=true",
                ]
            )
        return cmd

    def _build_occupancy_cmd(self, desired):
        include_unfrozen = self.include_unfrozen_submaps if str(desired["mode"]) == "mapping" else False
        return [
            self.occupancy_grid_node_bin,
            "--occupancy_grid_topic=%s" % self.map_topic,
            "--resolution=%s" % ("%.6f" % float(self.map_resolution)),
            "--publish_period_sec=%s" % ("%.6f" % float(self.publish_period_sec)),
            "--include_frozen_submaps=%s" % _bool_flag(self.include_frozen_submaps),
            "--include_unfrozen_submaps=%s" % _bool_flag(include_unfrozen),
        ]

    def _load_landmark_params(self):
        if not self.landmark_param_yaml:
            return
        if not os.path.exists(self.landmark_param_yaml):
            raise RuntimeError("landmark yaml missing: %s" % self.landmark_param_yaml)
        subprocess.check_call(["rosparam", "load", self.landmark_param_yaml])

    def _launch_proc(self, cmd, label):
        rospy.loginfo("[cartographer_supervisor] starting %s: %s", label, " ".join(shlex.quote(x) for x in cmd))
        return subprocess.Popen(cmd, preexec_fn=os.setsid)

    def _stop_proc(self, proc):
        if proc is None or proc.poll() is not None:
            return None
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            return None
        deadline = time.time() + self.stop_timeout_s
        while time.time() < deadline:
            if proc.poll() is not None:
                return None
            time.sleep(0.1)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            pass
        return None

    def _stop_all(self):
        self._grid_proc = self._stop_proc(self._grid_proc)
        self._node_proc = self._stop_proc(self._node_proc)

    def _publish_runtime_snapshot(self, desired):
        rospy.set_param(self._runtime_param("current_mode"), str(desired.get("mode") or ""))
        rospy.set_param(self._runtime_param("current_map_name"), str(desired.get("map_name") or ""))
        rospy.set_param(self._runtime_param("current_map_version"), str(desired.get("map_version") or ""))
        rospy.set_param(self._runtime_param("current_pbstream_path"), str(desired.get("pbstream_path") or ""))

    def _restart_runtime(self):
        desired = self._desired_runtime()
        self._stop_all()
        self._load_landmark_params()
        self._node_proc = self._launch_proc(self._build_cartographer_cmd(desired), "cartographer_node")
        if self.startup_delay_s > 0.0:
            time.sleep(self.startup_delay_s)
        self._grid_proc = self._launch_proc(
            self._build_occupancy_cmd(desired),
            "cartographer_occupancy_grid_node",
        )
        self._publish_runtime_snapshot(desired)
        return desired

    def _handle_reload(self, _req):
        try:
            desired = self._restart_runtime()
            return TriggerResponse(
                success=True,
                message="reloaded mode=%s map=%s@%s"
                % (
                    str(desired.get("mode") or ""),
                    str(desired.get("map_name") or ""),
                    str(desired.get("map_version") or ""),
                ),
            )
        except Exception as e:
            rospy.logerr("[cartographer_supervisor] reload failed: %s", str(e))
            return TriggerResponse(success=False, message=str(e))

    def _handle_save_state(self, _req):
        filename = os.path.expanduser(str(rospy.get_param(self._runtime_param("save_state_filename"), "")).strip())
        unfinished = bool(rospy.get_param(self._runtime_param("save_state_unfinished"), True))
        if not filename:
            return TriggerResponse(success=False, message="save_state_filename param is required")
        out_dir = os.path.dirname(filename)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        request = "{filename: '%s', include_unfinished_submaps: %s}" % (
            str(filename).replace("'", "\\'"),
            _bool_flag(unfinished),
        )
        cmd = ["rosservice", "call", self.write_state_service_name, request]
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=60.0)
            rospy.loginfo("[cartographer_supervisor] save_state ok: %s", str(filename))
            return TriggerResponse(success=True, message=str(out.decode("utf-8", errors="ignore").strip() or filename))
        except Exception as e:
            rospy.logerr("[cartographer_supervisor] save_state failed: %s", str(e))
            return TriggerResponse(success=False, message=str(e))

    def _shutdown(self):
        self._stop_all()


def main():
    rospy.init_node("cartographer_supervisor", anonymous=False)
    CartographerSupervisorNode()
    rospy.spin()


if __name__ == "__main__":
    main()
