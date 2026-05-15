#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.coverage_planner_core.f2c_adapter import (  # noqa: E402
    snake_sorted_swaths,
    swath_polyline_xyz,
)


class _Point:
    def __init__(self, x, y):
        self._x = float(x)
        self._y = float(y)

    def getX(self):
        return self._x

    def getY(self):
        return self._y

    def getZ(self):
        return 0.0


class _LineString:
    def __init__(self, pts):
        self._pts = [_Point(x, y) for x, y in pts]

    def size(self):
        return len(self._pts)

    def getGeometry(self, idx):
        return self._pts[int(idx)]


class _Swath:
    def __init__(self, pts):
        self._path = _LineString(pts)

    def getPath(self):
        return self._path


class F2CAdapterSnakeSortTest(unittest.TestCase):
    def test_python_snake_sort_orders_parallel_swaths_and_alternates_direction(self):
        swaths = [
            _Swath([(0.0, 2.0), (10.0, 2.0)]),
            _Swath([(10.0, 0.0), (0.0, 0.0)]),
            _Swath([(0.0, 1.0), (10.0, 1.0)]),
        ]

        ordered = snake_sorted_swaths(swaths)
        paths = [
            [(round(x, 3), round(y, 3)) for x, y, _z in swath_polyline_xyz(sw)]
            for sw in ordered
        ]

        self.assertEqual(
            paths,
            [
                [(0.0, 0.0), (10.0, 0.0)],
                [(10.0, 1.0), (0.0, 1.0)],
                [(0.0, 2.0), (10.0, 2.0)],
            ],
        )


if __name__ == "__main__":
    unittest.main()
