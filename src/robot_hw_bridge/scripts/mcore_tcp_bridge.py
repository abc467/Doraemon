#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
M-core (底盘/M核) TCP bridge

目标：**完全按 clean_robot.py 的协议收发方式对齐**，因为该版本在实车已验证可稳定通信。
在此基础上仅做“不改变协议容错行为”的工程增强：
  - TCP 流粘包/半包重组（不再假设 recv 一次就是完整帧）
  - send() 加互斥锁，避免并发写 socket

ROS I/O:
  - Subscribe: /cmd_vel (geometry_msgs/Twist)
  - Publish  : /combined_status (my_msg_srv/CombinedStatus)
  - Publish  : /battery_state (sensor_msgs/BatteryState)
  - Publish  : ~connected (std_msgs/Bool, latch)
  - Publish  : /mcore/cleaning_params/current (my_msg_srv/CleaningParams, latch)

供补给/充电流程使用的控制输入（保持你们现有架构）:
  - Subscribe: /mcore/control_clean_tools (my_msg_srv/ControlCleanTools)
      cmd 0x5002: tool_id + operation
  - Subscribe: /mcore/control_water_tap (my_msg_srv/ControlWaterTap)
      tap_id: 0x01 水泵, 0x02 清水阀, 0x03 污水阀, 0x05 吸水机
      operation: 0/1 (阀门开关) 或 0..64(泵PWM/吸水机风力)
  - Subscribe: /mcore/control_motor (my_msg_srv/ControlMotor)
      cmd 0x5004: vel 0..64
  - Subscribe: /mcore/cleaning_params/set (my_msg_srv/CleaningParams)
      仅更新参数快照，不直接触发执行机构启停
  - Subscribe: /mcore/charge_enable (std_msgs/Bool)
      True -> cmd 0x5005 [0x01]
      False-> cmd 0x5005 [0x00]

协议要点（与 clean_robot.py 一致）:
  - 发送头: 0x43 0x4E
  - 接收头: clean_robot.py 只校验第1字节 0x43（第二字节可能是 0x4E/0x4F，实车以接收为准）
  - 长度: clean_robot.py 主要使用 length_low = data[3]（通常 length_hi=0）
  - 校验: clean_robot.py 接收端不做 checksum 校验（仅发送端计算）
  - 尾: 0xDA
"""

import socket
import struct
import threading
import time
from typing import Optional

import rospy
from geometry_msgs.msg import Twist
from sensor_msgs.msg import BatteryState
from std_msgs.msg import Bool

from my_msg_srv.msg import (
    CleaningParams,
    CombinedStatus,
    ControlCleanTools,
    ControlMotor,
    ControlWaterTap,
)


def float_to_list_le(val: float):
    """对齐 clean_robot.py: struct.pack('f', val) 在 x86 上为小端；这里显式用 <f。"""
    b = struct.pack('<f', float(val))
    return [int(x) for x in b]


def sum_check_list(data_list, length: int) -> int:
    """对齐 clean_robot.py 的 SumCheck: 8-bit sum over first `length` bytes."""
    checksum = 0
    for i in range(0, length):
        checksum = (checksum + int(data_list[i])) & 0xFF
    return checksum


def clamp_u8(val: int, lo: int = 0, hi: int = 255) -> int:
    return max(int(lo), min(int(hi), int(val)))


class MCoreTCPBridge:
    HDR0 = 0x43
    HDR0_ALT = 0x42
    HDR_TX_1 = 0x4E              # 发送给M核固定使用 0x4E（与 clean_robot.py 一致）
    TAIL = 0xDA

    def __init__(self):
        self.server_ip = rospy.get_param('~server_ip', '192.168.16.10')
        self.server_port = int(rospy.get_param('~server_port', 5001))
        self.timeout = float(rospy.get_param('~timeout', 5.0))
        self.reconnect_delay = float(rospy.get_param('~reconnect_delay', 1.0))
        self.charge_cmd_repeat = int(rospy.get_param('~charge_cmd_repeat', 2))
        self.connected_publish_interval_s = float(rospy.get_param('~connected_publish_interval_s', 1.0))
        self.cleaning_params_topic = str(rospy.get_param('~cleaning_params_topic', '/mcore/cleaning_params/set'))
        self.cleaning_params_state_topic = str(rospy.get_param('~cleaning_params_state_topic', '/mcore/cleaning_params/current'))

        self._sock: Optional[socket.socket] = None
        self._connected = False
        self._send_lock = threading.Lock()

        # 接收缓存（做流重组）
        self._rx_buf = bytearray()
        self._last_rx_ts = time.time()
        self._last_connected_pub_ts = 0.0

        self.combined_status_pub = rospy.Publisher('/combined_status', CombinedStatus, queue_size=10)
        self.battery_pub = rospy.Publisher('/battery_state', BatteryState, queue_size=10)
        self.connected_pub = rospy.Publisher('~connected', Bool, queue_size=1, latch=True)
        self.cleaning_params_pub = rospy.Publisher(self.cleaning_params_state_topic, CleaningParams, queue_size=1, latch=True)

        self._status_msg = CombinedStatus()
        self._battery_msg = BatteryState()
        self._battery_msg.power_supply_technology = BatteryState.POWER_SUPPLY_TECHNOLOGY_LION
        self._cleaning_params = CleaningParams()
        self._cleaning_params.profile_name = str(rospy.get_param('~profile_name', 'default'))
        self._cleaning_params.vel_water_pump = clamp_u8(rospy.get_param('~vel_water_pump', 0), 0, 64)
        self._cleaning_params.vel_water_suction = clamp_u8(rospy.get_param('~vel_water_suction', 0), 0, 64)
        self._cleaning_params.height_scrub = clamp_u8(rospy.get_param('~height_scrub', 38))

        self.cmd_vel_sub = rospy.Subscriber('/cmd_vel', Twist, self._on_cmd_vel, queue_size=20)
        self.clean_tools_sub = rospy.Subscriber('/mcore/control_clean_tools', ControlCleanTools, self._on_control_clean_tools, queue_size=10)
        self.tap_sub = rospy.Subscriber('/mcore/control_water_tap', ControlWaterTap, self._on_control_tap, queue_size=10)
        self.motor_sub = rospy.Subscriber('/mcore/control_motor', ControlMotor, self._on_control_motor, queue_size=10)
        self.cleaning_params_sub = rospy.Subscriber(self.cleaning_params_topic, CleaningParams, self._on_cleaning_params, queue_size=10)
        self.charge_sub = rospy.Subscriber('/mcore/charge_enable', Bool, self._on_charge_enable, queue_size=10)

        # clean_robot.py 是 10Hz Timer；这里 50Hz 只是更快重组，不影响协议
        self._timer = rospy.Timer(rospy.Duration(0.02), self._tick)  # 50Hz
        self._publish_cleaning_params()

    def _publish_connected(self, force: bool = False):
        now = time.time()
        interval = max(0.2, float(self.connected_publish_interval_s))
        if (not force) and self._last_connected_pub_ts > 0.0 and (now - self._last_connected_pub_ts) < interval:
            return
        self.connected_pub.publish(Bool(data=bool(self._connected)))
        self._last_connected_pub_ts = now

    # ---------------- TCP connect / close ----------------
    def _connect(self):
        while not rospy.is_shutdown() and not self._connected:
            try:
                rospy.loginfo('[MCORE] connecting %s:%d ...', self.server_ip, self.server_port)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(self.timeout)
                s.connect((self.server_ip, self.server_port))
                self._sock = s
                self._connected = True
                self._rx_buf.clear()
                self._last_rx_ts = time.time()
                self._publish_connected(force=True)
                rospy.loginfo('[MCORE] connected')
            except Exception as e:
                rospy.logwarn_throttle(2.0, '[MCORE] connect failed: %s', str(e))
                time.sleep(self.reconnect_delay)

    def _disconnect(self, reason: str = ''):
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
        self._sock = None
        if self._connected:
            rospy.logwarn('[MCORE] disconnected: %s', reason)
        self._connected = False
        self._publish_connected(force=True)

    # ---------------- protocol send (对齐 clean_robot.py) ----------------
    def _data_send(self, cmd_id_list, cmd_data_list):
        """
        对齐 clean_robot.py 的 data_send:
          header=[0x43,0x4E]
          length_low = len(cmd_data)+8
          checksum = SumCheck(sendData0, length-2)
          tail=0xDA
        """
        if not self._connected or self._sock is None:
            try:
                rospy.logwarn_throttle(
                    2.0,
                    "[MCORE] send skipped: not connected cmd=0x%02X%02X data=%s",
                    int(cmd_id_list[0]) & 0xFF if len(cmd_id_list) > 0 else 0,
                    int(cmd_id_list[1]) & 0xFF if len(cmd_id_list) > 1 else 0,
                    str(list(cmd_data_list or [])),
                )
            except Exception:
                pass
            return

        cmd_header = [self.HDR0, self.HDR_TX_1]
        cmd_length = [0x00, 0x00]   # clean_robot.py 只设置低字节（通常 <256）
        cmd_check = [0x00]
        cmd_end = [self.TAIL]

        packet_len = len(cmd_data_list) + 8
        cmd_length[1] = packet_len & 0xFF
        cmd_length[0] = (packet_len >> 8) & 0xFF  # 兼容但不改变低字节用法

        sendData0 = cmd_header + cmd_length + cmd_id_list + cmd_data_list

        # clean_robot.py: SumCheck(sendData0, cmd_length[1]-2)
        # 保持一致：优先用 low byte（实车通常为 0）
        sum_len = (cmd_length[1] - 2) & 0xFF
        cmd_check[0] = sum_check_list(sendData0, sum_len)

        sendData1 = sendData0 + cmd_check + cmd_end
        sendData2 = bytes(sendData1)

        with self._send_lock:
            try:
                self._sock.sendall(sendData2)
            except Exception as e:
                self._disconnect(reason=f'send failed: {e}')

    # ---------------- command inputs ----------------
    def _on_cmd_vel(self, msg: Twist):
        cmd_id = [0x40, 0x60]
        cmd_data = float_to_list_le(msg.linear.x) + float_to_list_le(msg.angular.z)
        self._data_send(cmd_id, cmd_data)

    def _publish_cleaning_params(self):
        self.cleaning_params_pub.publish(self._cleaning_params)

    def _on_cleaning_params(self, msg: CleaningParams):
        self._cleaning_params.profile_name = str(msg.profile_name or self._cleaning_params.profile_name or '')
        self._cleaning_params.vel_water_pump = clamp_u8(msg.vel_water_pump, 0, 64)
        self._cleaning_params.vel_water_suction = clamp_u8(msg.vel_water_suction, 0, 64)
        self._cleaning_params.height_scrub = clamp_u8(msg.height_scrub)

        rospy.loginfo(
            '[MCORE] cleaning params updated: profile=%s pump=%d suction=%d height_scrub=%d (stored only, no direct protocol mapping)',
            self._cleaning_params.profile_name,
            int(self._cleaning_params.vel_water_pump),
            int(self._cleaning_params.vel_water_suction),
            int(self._cleaning_params.height_scrub),
        )
        self._publish_cleaning_params()

    def _on_control_clean_tools(self, msg: ControlCleanTools):
        # cmd_id 0x5002 : [tool_id, operation]
        cmd_id = [0x50, 0x02]
        tool = int(msg.tool_id) & 0xFF
        op = int(msg.operation) & 0xFF
        self._data_send(cmd_id, [tool, op])

    def _on_control_tap(self, msg: ControlWaterTap):
        # cmd_id 0x5003 : [tap_id, operation]
        cmd_id = [0x50, 0x03]
        tap = int(msg.tap_id) & 0xFF
        op = int(msg.operation) & 0xFF
        self._data_send(cmd_id, [tap, op])

    def _on_control_motor(self, msg: ControlMotor):
        # cmd_id 0x5004 : [vel]
        cmd_id = [0x50, 0x04]
        vel = clamp_u8(msg.vel, 0, 64)
        self._data_send(cmd_id, [vel])

    def _on_charge_enable(self, msg: Bool):
        # cmd_id 0x5005 : [0x01] enable / [0x00] disable
        cmd_id = [0x50, 0x05]
        val = 0x01 if bool(msg.data) else 0x00
        repeats = max(1, self.charge_cmd_repeat)
        rospy.loginfo(
            '[MCORE] charge_enable cmd=0x5005 val=0x%02X repeat=%d',
            val,
            repeats,
        )
        for idx in range(repeats):
            rospy.loginfo(
                '[MCORE] charge_enable tx %d/%d cmd=0x5005 data=[0x%02X]',
                idx + 1,
                repeats,
                val,
            )
            self._data_send(cmd_id, [val])
            if idx + 1 < repeats:
                rospy.sleep(0.05)

    # ---------------- protocol receive (对齐 clean_robot.py 的容错) ----------------
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
        """
        从流中弹出一帧。
        现场实测 M 核上行存在两种帧头：
          - 0x43 xx
          - 0x42 0x44
        发送侧仍保持 0x43 0x4E，不改协议下行。
        """
        buf = self._rx_buf

        # resync: 找到可接受的帧头首字节
        while len(buf) >= 1 and buf[0] not in (self.HDR0, self.HDR0_ALT):
            del buf[0]

        if len(buf) < 4:
            return None

        # 读取长度字段（优先按 16-bit；不合理则退回使用低字节 data[3]，对齐 clean_robot.py）
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

    def _fuc_data_verify_and_apply(self, frame: bytes):
        """
        对齐 clean_robot.py 的 fuc_data_verify:
          - 实车兼容 frame[0] in {0x43, 0x42}
          - (len<8) AND (len != data[3]) 才判定长度错误（非常宽松）
          - 检查尾 0xDA
          - 不做 checksum 校验
          - apply(frame[4:len-2])
        """
        if len(frame) < 2:
            return
        if frame[0] not in (self.HDR0, self.HDR0_ALT):
            return
        if (len(frame) < 8) and (len(frame) != int(frame[3])):
            return
        if frame[-1] != self.TAIL:
            return

        payload = frame[4:len(frame) - 2]
        self._fuc_data_apply(payload)

    def _fuc_data_apply(self, data: bytes):
        if len(data) < 2:
            return

        cmd0 = int(data[0])
        cmd1 = int(data[1])

        # 0x4060 速度信息（此处不需要发布）
        if (cmd0 == 0x40) and (cmd1 == 0x60):
            return

        # 0x4070 状态信息（核心）
        if (cmd0 == 0x40) and (cmd1 == 0x70):
            d = data[2:]
            if len(d) < 8:
                return

            bat_pct = int(d[0])
            bat_mv = (int(d[1]) << 8) | int(d[2])

            self._status_msg.battery_percentage = bat_pct
            self._status_msg.battery_voltage = bat_mv

            sewage_level = 0
            if d[3] & 0x80:
                sewage_level = 1
            if d[3] & 0x40:
                sewage_level = 2
            if d[3] & 0x20:
                sewage_level = 3
            if d[3] & 0x10:
                sewage_level = 4
            self._status_msg.sewage_level = sewage_level

            self._status_msg.clean_level = int(d[4])
            self._status_msg.brush_position = (int(d[5]) >> 6) & 0x03
            self._status_msg.scraper_position = (int(d[5]) >> 4) & 0x03

            for i in range(8):
                self._status_msg.obstacle_status[i] = bool(int(d[6]) & (0x80 >> i))
            for i in range(3):
                self._status_msg.region[i] = bool(int(d[7]) & (0x80 >> i))

            # d[8] 可能不存在：对齐 clean_robot.py 的 try/except 容错
            if len(d) >= 9:
                for i in range(8):
                    self._status_msg.status[i] = bool(int(d[8]) & (0x80 >> i))
            else:
                for i in range(8):
                    self._status_msg.status[i] = False

            self.combined_status_pub.publish(self._status_msg)

            b = self._battery_msg
            b.header.stamp = rospy.Time.now()
            b.voltage = float(bat_mv) / 1000.0
            b.percentage = max(0.0, min(1.0, float(bat_pct) / 100.0))
            self.battery_pub.publish(b)
            return

        # 0x4071 超声（忽略）
        if (cmd0 == 0x40) and (cmd1 == 0x71):
            return

    # ---------------- tick ----------------
    def _tick(self, _evt):
        if rospy.is_shutdown():
            self._timer.shutdown()
            self._disconnect('shutdown')
            return

        if not self._connected:
            self._connect()
            return

        self._publish_connected()
        self._recv_into_buffer()

        for _ in range(50):
            f = self._try_pop_one_frame()
            if f is None:
                break
            self._fuc_data_verify_and_apply(f)


def main():
    rospy.init_node('mcore_tcp_bridge', anonymous=False)
    _ = MCoreTCPBridge()
    rospy.loginfo('[MCORE] bridge started')
    rospy.spin()


if __name__ == '__main__':
    main()
