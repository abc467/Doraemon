# -*- coding: utf-8 -*-

"""Low-level process and ROS service transport helpers for SLAM runtime."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from typing import Any, Dict, Tuple

import rospy
import rosnode
from cartographer_ros_msgs.srv import VisualCommand, WriteState
from nav_msgs.msg import OccupancyGrid
from robot_runtime_flags_msgs.srv import SetParam
from std_srvs.srv import Trigger


_INTERNAL_VISUAL_COMMAND_SERVICE = "/visual_command"
_INTERNAL_WRITE_STATE_SERVICE = "/write_state"
_INTERNAL_RUNTIME_SAVE_STATE_SERVICE = "/cartographer/runtime/save_state"


def _bool_flag(value: bool) -> str:
    return "true" if bool(value) else "false"


def _tail_text(text: str, limit: int = 40) -> str:
    lines = [str(line) for line in str(text or "").splitlines() if str(line).strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max(1, int(limit)):])


class CartographerRuntimeTransport:
    def __init__(self, backend: Any):
        self._backend = backend

    def _visual_command_service_name(self) -> str:
        return (
            str(getattr(self._backend, "visual_command_service", "") or _INTERNAL_VISUAL_COMMAND_SERVICE).strip()
            or _INTERNAL_VISUAL_COMMAND_SERVICE
        )

    def _write_state_service_name(self) -> str:
        return (
            str(getattr(self._backend, "write_state_service", "") or _INTERNAL_WRITE_STATE_SERVICE).strip()
            or _INTERNAL_WRITE_STATE_SERVICE
        )

    def _runtime_save_state_service_name(self) -> str:
        value = str(getattr(self._backend, "runtime_save_state_service", "") or "").strip()
        return value or _INTERNAL_RUNTIME_SAVE_STATE_SERVICE

    def _save_backend_mode(self) -> str:
        backend = self._backend
        service_available = backend._runtime_context.service_available
        if service_available(self._write_state_service_name(), "cartographer_ros_msgs/WriteState"):
            return "write_state"
        if service_available(self._runtime_save_state_service_name(), "std_srvs/Trigger"):
            return "trigger_fallback"
        return ""

    def node_available(self, node_name: str) -> bool:
        try:
            return str(node_name or "").strip() in set(rosnode.get_node_names())
        except Exception:
            return False

    def tail_log(self, log_path: str, limit: int = 40) -> str:
        path = str(log_path or "").strip()
        if not path or (not os.path.isfile(path)):
            return ""
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                return _tail_text(fh.read(), limit=limit)
        except Exception:
            return ""

    def format_process_failure(self, label: str, log_path: str) -> str:
        tail = self.tail_log(log_path)
        if tail:
            return "%s exited unexpectedly log=%s\n%s" % (str(label or "process"), str(log_path or "-"), tail)
        return "%s exited unexpectedly log=%s" % (str(label or "process"), str(log_path or "-"))

    def runtime_env(self) -> Dict[str, str]:
        backend = self._backend
        env = os.environ.copy()
        env["SLAM_ROOT"] = backend.workspace_root
        env["SLAM_CONFIG_ROOT"] = backend.slam_config_root
        env["SLAM_MAP_ROOT"] = getattr(backend, "repo_map_root", "") or backend.maps_root
        env["ROS_MASTER_URI"] = str(env.get("ROS_MASTER_URI") or "http://localhost:11311")
        if backend.workspace_setup_path:
            env["WORKSPACE_SETUP"] = backend.workspace_setup_path
        return env

    def launch_managed_proc(self, label: str, cmd) -> Tuple[int, str]:
        backend = self._backend
        self.stop_managed_proc(label)
        stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        log_path = os.path.join(backend.log_root, "%s_%s.log" % (str(label or "proc"), stamp))
        log_fh = open(log_path, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [str(part) for part in list(cmd or [])],
            cwd=backend.workspace_root,
            env=self.runtime_env(),
            stdin=subprocess.DEVNULL,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        log_fh.close()
        backend._managed_procs[str(label or "").strip()] = {
            "proc": proc,
            "log_path": log_path,
        }
        return int(proc.pid), log_path

    def stop_managed_proc(self, label: str):
        backend = self._backend
        key = str(label or "").strip()
        meta = dict(backend._managed_procs.get(key) or {})
        proc = meta.get("proc")
        if proc is None:
            backend._managed_procs.pop(key, None)
            return
        if proc.poll() is not None:
            backend._managed_procs.pop(key, None)
            return
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            backend._managed_procs.pop(key, None)
            return
        deadline = time.time() + backend.stop_timeout_s
        while time.time() < deadline:
            if proc.poll() is not None:
                backend._managed_procs.pop(key, None)
                return
            time.sleep(0.1)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            pass
        backend._managed_procs.pop(key, None)

    def wait_for_service_ready(
        self,
        service_name: str,
        expected_type: str,
        *,
        timeout_s: float,
        watched_labels=(),
    ):
        backend = self._backend
        service_available = backend._runtime_context.service_available
        deadline = time.time() + float(timeout_s)
        while (time.time() < deadline) and (not rospy.is_shutdown()):
            if service_available(service_name, expected_type):
                return
            for label in list(watched_labels or ()):
                meta = dict(backend._managed_procs.get(str(label or "").strip()) or {})
                proc = meta.get("proc")
                if proc is not None and proc.poll() is not None:
                    raise RuntimeError(self.format_process_failure(str(label or "process"), meta.get("log_path")))
            rospy.sleep(0.2)
        raise RuntimeError(
            "service %s not ready within %.1fs" % (str(service_name or ""), float(timeout_s))
        )

    def kill_nodes(self, node_names) -> bool:
        backend = self._backend
        existing = [str(name or "").strip() for name in list(node_names or []) if self.node_available(name)]
        if not existing:
            return False
        try:
            rosnode.kill_nodes(existing)
        except Exception as exc:
            rospy.logwarn("[slam_runtime_manager] failed to kill nodes %s: %s", existing, str(exc))
        deadline = time.time() + backend.stop_timeout_s
        while time.time() < deadline:
            remaining = [name for name in existing if self.node_available(name)]
            if not remaining:
                return True
            rospy.sleep(0.2)
        return False

    def pkill_pattern(self, pattern: str) -> bool:
        backend = self._backend
        pp = str(pattern or "").strip()
        if not pp:
            return False
        try:
            proc = subprocess.run(
                ["pkill", "-f", "--", pp],
                cwd=backend.workspace_root,
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=float(backend.stop_timeout_s),
                check=False,
            )
            return int(proc.returncode) == 0
        except Exception as exc:
            rospy.logwarn("[slam_runtime_manager] pkill pattern=%s failed: %s", pp, str(exc))
            return False

    def ensure_runtime_flag_server_ready(self):
        backend = self._backend
        service_available = backend._runtime_context.service_available
        if service_available(backend.set_param_service, "robot_runtime_flags_msgs/SetParam") and service_available(
            backend.get_param_service, "robot_runtime_flags_msgs/GetParam"
        ):
            return
        stale_nodes = []
        for node_name in ("/runtime_flag_server",):
            if self.node_available(node_name):
                stale_nodes.append(node_name)
        if stale_nodes:
            self.kill_nodes(stale_nodes)
        start_pid, start_log = self.launch_managed_proc(
            "runtime_flag_server_node",
            [backend.runtime_flag_server_bin],
        )
        self.wait_for_service_ready(
            backend.set_param_service,
            "robot_runtime_flags_msgs/SetParam",
            timeout_s=backend.runtime_process_ready_timeout_s,
            watched_labels=("runtime_flag_server_node",),
        )
        self.wait_for_service_ready(
            backend.get_param_service,
            "robot_runtime_flags_msgs/GetParam",
            timeout_s=backend.runtime_process_ready_timeout_s,
            watched_labels=("runtime_flag_server_node",),
        )
        rospy.loginfo(
            "[slam_runtime_manager] runtime_flag_server ready pid=%s log=%s",
            start_pid,
            start_log,
        )

    def start_runtime_processes(self, *, include_unfrozen_submaps: bool) -> Tuple[int, str]:
        backend = self._backend
        self.ensure_runtime_flag_server_ready()
        carto_pid, carto_log = self.launch_managed_proc(
            "cartographer_node",
            [backend.cartographer_node_bin],
        )
        if backend.runtime_startup_delay_s > 0.0:
            rospy.sleep(backend.runtime_startup_delay_s)
        occupancy_pid, occupancy_log = self.launch_managed_proc(
            "cartographer_occupancy_grid_node",
            [
                backend.occupancy_grid_node_bin,
                "--occupancy_grid_topic=%s" % backend.map_topic,
                "--resolution=%s" % ("%.6f" % float(backend.map_resolution)),
                "--publish_period_sec=%s" % ("%.6f" % float(backend.map_publish_period_sec)),
                "--include_frozen_submaps=%s" % _bool_flag(backend.include_frozen_submaps),
                "--include_unfrozen_submaps=%s" % _bool_flag(include_unfrozen_submaps),
            ],
        )
        self.wait_for_service_ready(
            self._visual_command_service_name(),
            "cartographer_ros_msgs/VisualCommand",
            timeout_s=backend.runtime_process_ready_timeout_s,
            watched_labels=("cartographer_node", "cartographer_occupancy_grid_node"),
        )
        return carto_pid, "cartographer_log=%s occupancy_pid=%s occupancy_log=%s" % (
            carto_log,
            occupancy_pid,
            occupancy_log,
        )

    def stop_runtime(self):
        self.stop_managed_proc("cartographer_occupancy_grid_node")
        self.stop_managed_proc("cartographer_node")
        self.kill_nodes(["/cartographer_occupancy_grid_node", "/cartographer_node"])
        self.pkill_pattern("(^|/)cartographer_occupancy_grid_node($| )")
        self.pkill_pattern("(^|/)cartographer_node($| )")

    def call_set_param(self, key: str, value: str) -> Tuple[bool, str]:
        backend = self._backend
        last_exc = None
        for _ in range(3):
            try:
                rospy.wait_for_service(backend.set_param_service, timeout=float(backend.command_timeout_s))
                cli = rospy.ServiceProxy(backend.set_param_service, SetParam)
                resp = cli(keys=[str(key or "")], vals=[str(value or "")])
                opcode = int(getattr(resp, "opcode", -1))
                message = str(getattr(resp, "msg", "") or "")
                return opcode == 0, message
            except Exception as exc:
                last_exc = exc
                rospy.sleep(0.5)
        return False, str(last_exc or "set_param failed")

    def call_trigger(self, service_name: str) -> Tuple[bool, str]:
        backend = self._backend
        rospy.wait_for_service(service_name, timeout=float(backend.command_timeout_s))
        cli = rospy.ServiceProxy(service_name, Trigger)
        resp = cli()
        return bool(getattr(resp, "success", False)), str(getattr(resp, "message", "") or "")

    def call_visual_command(self, command: str, payload) -> Tuple[int, str, str]:
        backend = self._backend
        payload_json = payload if isinstance(payload, str) else json.dumps(payload or {}, ensure_ascii=False)
        last_exc = None
        service_name = self._visual_command_service_name()
        for _ in range(3):
            try:
                rospy.wait_for_service(service_name, timeout=float(backend.command_timeout_s))
                cli = rospy.ServiceProxy(service_name, VisualCommand)
                resp = cli(command=str(command or "").strip(), payload_json=str(payload_json or ""))
                return (
                    int(getattr(resp, "code", -1)),
                    str(getattr(resp, "msg", "") or ""),
                    str(getattr(resp, "data_json", "") or ""),
                )
            except Exception as exc:
                last_exc = exc
                rospy.sleep(0.5)
        raise RuntimeError("visual_command failed: %s" % str(last_exc or "unknown error"))

    def wait_for_save_backend_ready(self, *, timeout_s: float, watched_labels=()) -> str:
        backend = self._backend
        deadline = time.time() + float(timeout_s)
        while (time.time() < deadline) and (not rospy.is_shutdown()):
            mode = self._save_backend_mode()
            if mode:
                return mode
            for label in list(watched_labels or ()):
                meta = dict(backend._managed_procs.get(str(label or "").strip()) or {})
                proc = meta.get("proc")
                if proc is not None and proc.poll() is not None:
                    raise RuntimeError(self.format_process_failure(str(label or "process"), meta.get("log_path")))
            rospy.sleep(0.2)
        raise RuntimeError("save_state backend not ready within %.1fs" % float(timeout_s))

    def save_pbstream(self, filename: str, include_unfinished_submaps: bool = True) -> Tuple[bool, str]:
        backend = self._backend
        runtime_param = backend._runtime_context.runtime_param
        filename = os.path.expanduser(str(filename or "").strip())
        write_state_service = self._write_state_service_name()
        runtime_save_state_service = self._runtime_save_state_service_name()
        save_backend_mode = self._save_backend_mode()
        if save_backend_mode == "write_state":
            last_exc = None
            for _ in range(3):
                try:
                    rospy.wait_for_service(write_state_service, timeout=float(backend.command_timeout_s))
                    cli = rospy.ServiceProxy(write_state_service, WriteState)
                    resp = cli(
                        filename=filename,
                        include_unfinished_submaps=bool(include_unfinished_submaps),
                    )
                    status = getattr(resp, "status", None)
                    code = int(getattr(status, "code", -1))
                    message = str(getattr(status, "message", "") or "")
                    return code == 0, message
                except Exception as exc:
                    last_exc = exc
                    rospy.sleep(0.5)
            return False, str(last_exc or "write_state failed")

        if save_backend_mode == "trigger_fallback":
            rospy.logwarn(
                "[slam_runtime_manager] save_pbstream degraded: using internal trigger fallback service=%s",
                runtime_save_state_service,
            )
            rospy.set_param(runtime_param("save_state_filename"), filename)
            rospy.set_param(runtime_param("save_state_unfinished"), bool(include_unfinished_submaps))
            return self.call_trigger(runtime_save_state_service)

        return False, "no save_state backend available"
