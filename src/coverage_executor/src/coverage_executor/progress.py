# -*- coding: utf-8 -*-
import math
from dataclasses import dataclass
from typing import List, Tuple, Optional


XY = Tuple[float, float]


def _dist2(a: XY, b: XY) -> float:
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    return dx * dx + dy * dy


def _dist(a: XY, b: XY) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])


def build_arclen(path_xy: List[XY]) -> List[float]:
    """prefix arc-length s[i] is distance from start to point i"""
    if not path_xy:
        return []
    s = [0.0]
    for i in range(1, len(path_xy)):
        s.append(s[-1] + _dist(path_xy[i - 1], path_xy[i]))
    return s


@dataclass
class Progress:
    seg_i: int          # segment index i for projection onto [i, i+1]
    t: float            # [0,1] along segment
    proj_xy: XY
    dist_m: float
    s_m: float          # arc-length position along path
    idx_hint: int       # the checkpoint hint used


def project_along_segments(
    path_xy: List[XY],
    arclen: List[float],
    p: XY,
    *,
    hint_index: int,
    search_back_pts: int = 40,
    search_fwd_pts: int = 300,
    min_s: Optional[float] = None,
    max_s: Optional[float] = None,
) -> Optional[Progress]:
    """
    Segment-projection with hint & monotonic constraints:
    - search only around hint window [hint-back, hint+fwd]
    - choose best projection by distance, but discard projections whose s is outside [min_s, max_s] if provided

    This prevents jumping to a parallel/nearby segment elsewhere.
    """
    n = len(path_xy)
    if n < 2:
        return None
    if len(arclen) != n:
        return None

    hi = max(0, min(int(hint_index), n - 1))
    i0 = max(0, hi - int(search_back_pts))
    i1 = min(n - 2, hi + int(search_fwd_pts))  # segment last i is n-2

    best: Optional[Progress] = None

    for i in range(i0, i1 + 1):
        ax, ay = path_xy[i]
        bx, by = path_xy[i + 1]
        vx = bx - ax
        vy = by - ay
        seg_len2 = vx * vx + vy * vy
        if seg_len2 < 1e-12:
            continue
        # projection t on infinite line
        t = ((p[0] - ax) * vx + (p[1] - ay) * vy) / seg_len2
        # clamp to segment
        if t < 0.0:
            t = 0.0
        elif t > 1.0:
            t = 1.0
        px = ax + t * vx
        py = ay + t * vy
        d = math.hypot(p[0] - px, p[1] - py)

        seg_len = math.sqrt(seg_len2)
        s_here = arclen[i] + t * seg_len

        if min_s is not None and s_here < min_s:
            continue
        if max_s is not None and s_here > max_s:
            continue

        cand = Progress(
            seg_i=i,
            t=float(t),
            proj_xy=(float(px), float(py)),
            dist_m=float(d),
            s_m=float(s_here),
            idx_hint=int(hint_index),
        )
        if best is None:
            best = cand
        else:
            # primary: smaller distance, secondary: closer to hint arc s
            if cand.dist_m < best.dist_m - 1e-6:
                best = cand
            elif abs(cand.dist_m - best.dist_m) <= 1e-6:
                if abs(cand.s_m - arclen[hi]) < abs(best.s_m - arclen[hi]):
                    best = cand

    return best


def index_from_s(arclen: List[float], s_target: float) -> int:
    """first index i such that arclen[i] >= s_target; clamp to [0, n-1]"""
    if not arclen:
        return 0
    n = len(arclen)
    if s_target <= 0.0:
        return 0
    if s_target >= arclen[-1]:
        return n - 1
    # linear scan is OK for <= 20k, but we can do binary
    lo, hi = 0, n - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if arclen[mid] < s_target:
            lo = mid + 1
        else:
            hi = mid
    return int(lo)
