#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Helpers for canonical ROS contract parameter publication."""

from __future__ import annotations

import rospy


def publish_contract_param(rospy_module, param_ns: str, report, *, enabled: bool = True) -> None:
    name = str(param_ns or "").strip()
    if not name:
        return
    if bool(enabled):
        rospy_module.set_param(name, report)
        return
    try:
        rospy_module.delete_param(name)
    except Exception:
        pass
