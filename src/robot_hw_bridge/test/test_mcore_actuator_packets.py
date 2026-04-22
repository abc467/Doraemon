#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import socket
import sys
import threading
import time
import unittest

import rospy
import rostest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
MCORE_SCRIPT_DIR = os.path.join(PKG_DIR, "scripts")
COVERAGE_EXECUTOR_SRC_DIR = os.path.abspath(
    os.path.join(PKG_DIR, "..", "coverage_executor", "src")
)
COVERAGE_PLANNER_SRC_DIR = os.path.abspath(
    os.path.join(PKG_DIR, "..", "coverage_planner", "src")
)

if MCORE_SCRIPT_DIR not in sys.path:
    sys.path.insert(0, MCORE_SCRIPT_DIR)
if COVERAGE_EXECUTOR_SRC_DIR not in sys.path:
    sys.path.insert(0, COVERAGE_EXECUTOR_SRC_DIR)
if COVERAGE_PLANNER_SRC_DIR not in sys.path:
    sys.path.insert(0, COVERAGE_PLANNER_SRC_DIR)

from coverage_executor.cleaning_actuator import CleaningActuator
from mcore_tcp_bridge import MCoreTCPBridge, sum_check_list


def build_tx_frame(cmd_id_list, cmd_data_list):
    packet_len = len(cmd_data_list) + 8
    length_hi = (packet_len >> 8) & 0xFF
    length_lo = packet_len & 0xFF
    send_data0 = [0x43, 0x4E, length_hi, length_lo] + list(cmd_id_list) + list(cmd_data_list)
    checksum = sum_check_list(send_data0, (length_lo - 2) & 0xFF)
    return bytes(send_data0 + [checksum, 0xDA])


class FakeMCoreServer:
    def __init__(self, host="127.0.0.1", port=0):
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((host, port))
        self._server.listen(1)
        self._server.settimeout(0.1)

        self.host, self.port = self._server.getsockname()
        self._stop_evt = threading.Event()
        self._connected_evt = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._conn = None
        self._rx_buf = bytearray()
        self._frames = []

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_evt.set()
        if self._conn is not None:
            try:
                self._conn.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
        try:
            self._server.close()
        except Exception:
            pass
        self._thread.join(timeout=2.0)

    def clear_frames(self):
        with self._lock:
            self._frames = []
            self._rx_buf = bytearray()

    def wait_connected(self, timeout=5.0):
        return self._connected_evt.wait(timeout)

    def wait_for_frames(self, count, timeout=3.0):
        deadline = time.time() + float(timeout)
        while time.time() < deadline and not self._stop_evt.is_set():
            with self._lock:
                if len(self._frames) >= count:
                    return list(self._frames[:count])
            time.sleep(0.01)
        with self._lock:
            frames = list(self._frames)
        raise AssertionError("timed out waiting for %d frame(s), got %d" % (count, len(frames)))

    def _run(self):
        while not self._stop_evt.is_set():
            try:
                conn, _addr = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                return

            self._conn = conn
            self._connected_evt.set()
            conn.settimeout(0.1)

            while not self._stop_evt.is_set():
                try:
                    data = conn.recv(4096)
                    if not data:
                        break
                    with self._lock:
                        self._rx_buf.extend(data)
                        while True:
                            frame = self._try_pop_one_frame()
                            if frame is None:
                                break
                            self._frames.append(frame)
                except socket.timeout:
                    self._send_status_frame(conn)
                except OSError:
                    break
                except Exception:
                    break
            return

    def _try_pop_one_frame(self):
        buf = self._rx_buf

        while len(buf) >= 1 and buf[0] not in (0x43, 0x42):
            del buf[0]

        if len(buf) < 4:
            return None

        length16 = (int(buf[2]) << 8) | int(buf[3])
        length_lo = int(buf[3])
        length = length16 if 8 <= length16 <= 4096 else length_lo

        if length < 8 or length > 4096:
            del buf[0]
            return None

        if len(buf) < length:
            return None

        frame = bytes(buf[:length])
        del buf[:length]
        return frame

    def _send_status_frame(self, conn):
        payload = [0x40, 0x70, 80, 0x34, 0x56, 0x00, 0x00, 0x00, 0x00, 0x00]
        packet_len = len(payload) + 6
        frame = [0x43, 0x4E, (packet_len >> 8) & 0xFF, packet_len & 0xFF] + payload
        frame += [sum_check_list(frame, packet_len - 2), 0xDA]
        try:
            conn.sendall(bytes(frame))
        except Exception:
            pass


class MCoreActuatorPacketTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = FakeMCoreServer()
        cls.server.start()

        rospy.set_param("~server_ip", cls.server.host)
        rospy.set_param("~server_port", cls.server.port)
        rospy.set_param("~timeout", 2.0)
        rospy.set_param("~reconnect_delay", 0.05)
        rospy.set_param("~water_tap_cmd_interval_s", 0.01)
        rospy.set_param("~actuator_profiles", {
            "max_test": {
                "vel_water_pump": 100,
                "suction_machine_pwm": 100,
                "vacuum_motor_pwm": 100,
                "height_scrub": 38,
            }
        })

        cls.bridge = MCoreTCPBridge()
        if not cls.server.wait_connected(timeout=5.0):
            raise RuntimeError("fake M-core server was not connected by bridge")

        deadline = time.time() + 3.0
        while time.time() < deadline:
            if getattr(cls.bridge, "_connected", False):
                break
            rospy.sleep(0.05)
        else:
            raise RuntimeError("bridge did not enter connected state")

        cls.actuator = CleaningActuator()
        cls.actuator.apply_profile("max_test")
        rospy.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        try:
            try:
                cls.bridge._timer.shutdown()
            except Exception:
                pass
            try:
                cls.bridge._disconnect("test shutdown")
            except Exception:
                pass
        finally:
            cls.server.stop()

    def setUp(self):
        self.server.clear_frames()
        self.actuator.apply_profile("max_test")
        rospy.sleep(0.05)

    def test_vacuum_on_sends_max_power_packets(self):
        self.actuator.vacuum_on()
        frames = self.server.wait_for_frames(2, timeout=2.0)
        expected = [
            build_tx_frame([0x50, 0x03], [0x05, 0x64]),
            build_tx_frame([0x50, 0x04], [0x64]),
        ]
        self.assertCountEqual(frames, expected)

    def test_water_on_sends_max_power_packets(self):
        self.actuator.water_on()
        frames = self.server.wait_for_frames(2, timeout=2.0)
        expected = [
            build_tx_frame([0x50, 0x03], [0x02, 0x01]),
            build_tx_frame([0x50, 0x03], [0x01, 0x64]),
        ]
        self.assertEqual(frames, expected)


if __name__ == "__main__":
    rospy.init_node("test_mcore_actuator_packets", anonymous=False)
    rostest.rosrun("robot_hw_bridge", "test_mcore_actuator_packets", MCoreActuatorPacketTest)
