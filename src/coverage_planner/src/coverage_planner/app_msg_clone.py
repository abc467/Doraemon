# -*- coding: utf-8 -*-

"""Minimal canonical app-message clone helpers for the SLAM public API."""

from __future__ import annotations

from cleanrobot_app_msgs.msg import SlamJobState as AppSlamJobState


def _copy_same_fields(src, dst):
    if src is None:
        return dst
    for field in getattr(dst, "__slots__", []):
        if hasattr(src, field):
            setattr(dst, field, getattr(src, field))
    return dst


def clone_app_slam_job_state(msg) -> AppSlamJobState:
    return _copy_same_fields(msg, AppSlamJobState())
