#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import threading
import time
from typing import Optional

import rospy
from std_msgs.msg import Bool
from my_msg_srv.msg import StationStatus, ControlStation


def sum_check_list(data_list, length: int) -> int:
    checksum = 0
    for i in range(length):
        checksum = (checksum + int(data_list[i])) & 0xFF
    return checksum


def hexdump(bs: bytes) -> str:
    return " ".join(f"{b:02X}" for b in bs)


class StationTCPBridge:
    HDR0 = 0x43
    HDR_RX_1 = 0x4F     # station -> robot
    HDR_TX_1 = 0x4E     # robot -> station
    TAIL = 0xDA

    def __init__(self):
        self.server_ip = rospy.get_param('~server_ip', '192.168.16.123')
        self.server_port = int(rospy.get_param('~server_port', 5007))
        self.timeout = float(rospy.get_param('~timeout', 5.0))
        self.reconnect_delay = float(rospy.get_param('~reconnect_delay', 2.0))

        # 发送增强
        self.tx_repeat = int(rospy.get_param('~tx_repeat', 1))  # 每条命令重复发 N 次（抗丢）
        # 仅对电缸动作(8/9)做“脉冲触发”：发送 1 后，rod_pulse_ms 毫秒后自动发 0
        self.rod_pulse_ms = int(rospy.get_param('~rod_pulse_ms', 200))  # 0=不脉冲

        # 可选：接收调试（每秒打印一次 Data0/Data1）
        self.debug_rx = bool(rospy.get_param('~debug_rx', False))
        self._last_dbg = 0.0

        self._sock: Optional[socket.socket] = None
        self._connected = False
        self._send_lock = threading.Lock()

        self._rx_buf = bytearray()
        self._last_rx_ts = time.time()

        self.status_pub = rospy.Publisher('/station_status', StationStatus, queue_size=10)
        self.connected_pub = rospy.Publisher('~connected', Bool, queue_size=1, latch=True)
        self.control_sub = rospy.Subscriber('/station/control', ControlStation, self._on_control, queue_size=20)

        self._status = StationStatus()
        self._timer = rospy.Timer(rospy.Duration(0.05), self._tick)  # 20Hz

    # ---------------- connect / disconnect ----------------
    def _connect(self):
        while not rospy.is_shutdown() and not self._connected:
            try:
                rospy.loginfo('[STATION] connecting %s:%d ...', self.server_ip, self.server_port)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(self.timeout)
                s.connect((self.server_ip, self.server_port))
                self._sock = s
                self._connected = True
                self._rx_buf.clear()
                self._last_rx_ts = time.time()
                self.connected_pub.publish(Bool(data=True))
                rospy.loginfo('[STATION] connected')
            except Exception as e:
                rospy.logwarn_throttle(2.0, '[STATION] connect failed: %s', str(e))
                time.sleep(self.reconnect_delay)

    def _disconnect(self, reason: str = ''):
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
        self._sock = None
        if self._connected:
            rospy.logwarn('[STATION] disconnected: %s', reason)
        self._connected = False
        self.connected_pub.publish(Bool(data=False))

    # ---------------- protocol send (对齐 clean_robot.py) ----------------
    def _data_send_once(self, cmd_id_list, cmd_data_list):
        if not self._connected or self._sock is None:
            rospy.logwarn_throttle(2.0, '[STATION] send skipped: not connected')
            return

        cmd_header = [0x43, 0x4E]
        cmd_length = [0x00, 0x00]
        cmd_check = [0x00]
        cmd_end = [0xDA]

        packet_len = len(cmd_data_list) + 8
        cmd_length[0] = (packet_len >> 8) & 0xFF
        cmd_length[1] = packet_len & 0xFF

        sendData0 = cmd_header + cmd_length + cmd_id_list + cmd_data_list
        cmd_check[0] = sum_check_list(sendData0, packet_len - 2)
        pkt = bytes(sendData0 + cmd_check + cmd_end)

        with self._send_lock:
            try:
                self._sock.sendall(pkt)
                rospy.loginfo('[STATION][TX] %s', hexdump(pkt))
            except Exception as e:
                self._disconnect(reason=f'send failed: {e}')

    def _data_send(self, cmd_id_list, cmd_data_list):
        n = max(1, self.tx_repeat)
        for i in range(n):
            self._data_send_once(cmd_id_list, cmd_data_list)
            if n > 1:
                rospy.sleep(0.02)

    # ---------------- control mapping ----------------
    def _on_control(self, msg: ControlStation):
        op = int(msg.operation) & 0xFF
        on = 0x01 if bool(msg.status) else 0x00

        # 1: 充电（4102）
        if op == 1:
            rospy.loginfo('[STATION] cmd: charger on=%d', on)
            self._data_send([0x41, 0x02], [on])
            return

        # 4100 通道：进水/排水/电缸/其他
        # 和 clean_robot 对齐：进水使用 0x0B，排水 0x03
        if op in (2, 11):         # 2: 加水中 / 11: 进水阀
            proto_op = 0x0B
        elif op == 3:             # 排水中
            proto_op = 0x03
        else:
            # 8: 电缸伸出 -> 0x08
            # 9: 电缸缩回 -> 0x09
            proto_op = op

        rospy.loginfo('[STATION] cmd: op=%d -> proto_op=0x%02X on=%d', op, proto_op, on)
        self._data_send([0x41, 0x00], [proto_op, on])

        # 仅对电缸动作做脉冲：8/9
        if op in (8, 9) and on == 0x01 and self.rod_pulse_ms > 0:
            def _pulse_off(evt):
                try:
                    rospy.loginfo('[STATION] rod pulse off: proto_op=0x%02X', proto_op)
                    self._data_send([0x41, 0x00], [proto_op, 0x00])
                finally:
                    evt.shutdown()
            rospy.Timer(rospy.Duration(self.rod_pulse_ms / 1000.0), _pulse_off, oneshot=True)

    # ---------------- receive (宽松容错，按 clean_robot 风格) ----------------
    def _recv_into_buffer(self):
        if not self._connected or self._sock is None:
            return
        try:
            data = self._sock.recv(4096)
            if not data:
                self._disconnect('peer closed')
                return
            self._last_rx_ts = time.time()
            self._rx_buf.extend(data)
        except socket.timeout:
            if time.time() - self._last_rx_ts > self.timeout:
                self._disconnect('rx timeout')
        except Exception as e:
            self._disconnect(reason=f'rx error: {e}')

    def _try_pop_one_frame(self) -> Optional[bytes]:
        buf = self._rx_buf

        # resync 到 43 4F
        while len(buf) >= 2 and (buf[0] != self.HDR0 or buf[1] != self.HDR_RX_1):
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

    def _apply_frame(self, frame: bytes):
        if len(frame) < 8:
            return
        if frame[0] != self.HDR0 or frame[1] != self.HDR_RX_1:
            return
        if frame[-1] != self.TAIL:
            return

        cmd_id = (int(frame[4]) << 8) | int(frame[5])
        if cmd_id != 0x4070:
            return

        # clean_robot: Data0 = frame[6], Data1 = frame[7]
        data0 = int(frame[6])
        data1 = int(frame[7])

        if self.debug_rx and (time.time() - self._last_dbg) > 1.0:
            self._last_dbg = time.time()
            rospy.loginfo('[STATION][RX] Data0=0x%02X Data1=0x%02X', data0, data1)

        agv_in_place = bool(data1 & 0x10)
        rod_connected = bool(data1 & 0x80)   # 电缸缩回到位
        rod_reset = bool(data1 & 0x01)       # 电缸伸出到位

        st = [False] * 14
        st[11] = agv_in_place
        st[8] = rod_connected
        st[7] = rod_reset
        self._status.status = st
        self.status_pub.publish(self._status)

    def _tick(self, _evt):
        if rospy.is_shutdown():
            self._timer.shutdown()
            self._disconnect('shutdown')
            return
        if not self._connected:
            self._connect()
            return

        self._recv_into_buffer()
        for _ in range(50):
            f = self._try_pop_one_frame()
            if f is None:
                break
            self._apply_frame(f)


def main():
    rospy.init_node('station_tcp_bridge', anonymous=False)
    _ = StationTCPBridge()
    rospy.loginfo('[STATION] bridge started')
    rospy.spin()


if __name__ == '__main__':
    main()