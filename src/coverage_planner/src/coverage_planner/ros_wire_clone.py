#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io


def clone_ros_message(src, dst_cls):
    """Clone a ROS message/service response into another generated class.

    This is safe when the source and destination share the same wire schema,
    even if they come from different ROS packages.
    """

    if src is None:
        return dst_cls()
    if isinstance(src, dst_cls):
        return src
    buf = io.BytesIO()
    src.serialize(buf)
    dst = dst_cls()
    dst.deserialize(buf.getvalue())
    return dst
