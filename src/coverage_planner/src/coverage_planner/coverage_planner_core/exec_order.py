# coverage_planner_core/exec_order.py
# -*- coding: utf-8 -*-

from typing import List, Dict, Any, Tuple
from .geom import dist_xy

XY = Tuple[float, float]


def nearest_neighbor_exec_order(block_infos: List[Dict[str, Any]]) -> List[int]:
    """
    Identical to your script:
      - fixed start=block_0 if exists, else smallest id
      - greedy nearest neighbor by (current exit -> next entry)
    Returns list of indices (into block_infos).
    """
    if not block_infos:
        return []
    id2idx = {b["id"]: i for i, b in enumerate(block_infos)}
    if 0 in id2idx:
        start_idx = id2idx[0]
    else:
        start_idx = min(range(len(block_infos)), key=lambda i: block_infos[i]["id"])

    used = [False] * len(block_infos)
    order = []

    cur = start_idx
    used[cur] = True
    order.append(cur)
    cur_exit: XY = block_infos[cur]["exit_xy"]

    for _ in range(len(block_infos) - 1):
        best = None
        best_d = 1e18
        for j, b in enumerate(block_infos):
            if used[j]:
                continue
            d = dist_xy(cur_exit, b["entry_xy"])
            if d < best_d:
                best_d = d
                best = j
        used[best] = True
        order.append(best)
        cur_exit = block_infos[best]["exit_xy"]

    return order
