# -*- coding: utf-8 -*-
import threading
from typing import Optional, Tuple

import rospy
from sensor_msgs.msg import BatteryState


class BatteryMonitor:
    """
    订阅 sensor_msgs/BatteryState:
      - percentage: 0..1  (推荐使用)
      - voltage/current/capacity 也可扩展
      - power_supply_status: CHARGING / DISCHARGING / FULL 等
    """
    def __init__(self, topic: str = "/battery_state", stale_timeout_s: float = 5.0):
        self._lock = threading.Lock()
        self._soc: Optional[float] = None
        self._status: Optional[int] = None
        self._stamp = rospy.Time(0)
        self._stale_timeout_s = float(stale_timeout_s)
        self._sub = rospy.Subscriber(topic, BatteryState, self._cb, queue_size=10)

    def _cb(self, msg: BatteryState):
        soc = msg.percentage
        if soc is not None:
            # 标准期望 0..1
            soc = max(0.0, min(1.0, float(soc)))
        st = msg.power_supply_status if msg.power_supply_status is not None else None
        with self._lock:
            self._soc = soc
            self._status = int(st) if st is not None else None
            self._stamp = rospy.Time.now()

    def snapshot(self) -> Tuple[Optional[float], Optional[int], float]:
        """
        return: (soc, status, age_s)
        """
        with self._lock:
            soc = self._soc
            st = self._status
            stamp = self._stamp
        age = (rospy.Time.now() - stamp).to_sec() if stamp != rospy.Time(0) else 1e9
        return soc, st, float(age)

    def is_stale(self) -> bool:
        _, _, age = self.snapshot()
        return age > self._stale_timeout_s

    def is_full(self) -> Optional[bool]:
        _, st, _ = self.snapshot()
        if st is None:
            return None
        return st == BatteryState.POWER_SUPPLY_STATUS_FULL

    def is_charging(self) -> Optional[bool]:
        _, st, _ = self.snapshot()
        if st is None:
            return None
        return st == BatteryState.POWER_SUPPLY_STATUS_CHARGING
