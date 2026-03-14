#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scheduler for coverage_task_manager (canonical format)

✅ Only supports ONE correct schedule format (type/time/task):

schedules:
  - id: "weekly_mon_1010_zone_demo_1loop"
    job_id: "zone_demo_standard_daily"   # optional; defaults to id
    enabled: true
    type: "weekly"          # weekly | daily | once
    dow: [0]                # Monday=0..Sunday=6 (weekly only)
    time: "10:10"           # HH:MM (weekly/daily)
    oneshot: false

    task:
      zone_id: "zone_demo"
      loops: 1
      plan_profile_name: "cover_standard"
      sys_profile_name: "standard"
      clean_mode: "scrub"

For once:
  - id: "once_xxx"
    type: "once"
    at: "YYYY-MM-DD HH:MM"  # local time
    task: { ... }

Stable interface:
  - Scheduler.jobs: List[ScheduleJob]  (attribute, iterable)
  - Scheduler.update_next_fire(job, now_ts): recompute job.next_fire_ts
"""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple
import time
import math


def _parse_hhmm(s: str) -> Tuple[int, int]:
    if not isinstance(s, str):
        raise ValueError(f"time must be string 'HH:MM', got {type(s)}")
    ss = s.strip()
    if ":" not in ss:
        raise ValueError(f"bad time='{s}', expect 'HH:MM'")
    hh_s, mm_s = ss.split(":", 1)
    hh = int(hh_s)
    mm = int(mm_s)
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"bad time='{s}', hour/min out of range")
    return hh, mm


def _parse_at_local(at: str) -> float:
    """
    Parse local datetime string: 'YYYY-MM-DD HH:MM' (local time) -> epoch seconds.
    """
    if not isinstance(at, str):
        raise ValueError(f"at must be string 'YYYY-MM-DD HH:MM', got {type(at)}")
    s = at.strip()
    # allow 'YYYY/MM/DD HH:MM' too
    s2 = s.replace("/", "-")
    try:
        tt = time.strptime(s2, "%Y-%m-%d %H:%M")
    except Exception:
        raise ValueError(f"bad at='{at}', expect 'YYYY-MM-DD HH:MM'")
    # mktime treats tuple as LOCAL time
    return float(time.mktime((tt.tm_year, tt.tm_mon, tt.tm_mday, tt.tm_hour, tt.tm_min, 0, 0, 0, -1)))


def _require_no_legacy_keys(d: Dict[str, Any], where: str) -> None:
    legacy = []
    for k in ("mode", "plan_profile", "sys_profile", "profile", "profile_name"):
        if k in d:
            legacy.append(k)
    if legacy:
        raise ValueError(
            f"{where} contains legacy keys {legacy}. "
            f"use clean_mode / plan_profile_name / sys_profile_name"
        )


@dataclass
class ScheduleTask:
    map_name: str = ""
    map_version: str = ""
    zone_id: str = ""
    loops: int = 1
    plan_profile_name: str = ""
    sys_profile_name: str = ""
    clean_mode: str = ""

    def normalized(self) -> "ScheduleTask":
        map_name = str(self.map_name or "").strip()
        map_version = str(self.map_version or "").strip()
        z = str(self.zone_id or "").strip()
        loops = int(self.loops) if str(self.loops).strip() != "" else 1
        loops = max(1, loops)
        plan = str(self.plan_profile_name or "").strip()
        sysn = str(self.sys_profile_name or "").strip()
        mode = str(self.clean_mode or "").strip()
        return ScheduleTask(
            map_name=map_name,
            map_version=map_version,
            zone_id=z,
            loops=loops,
            plan_profile_name=plan,
            sys_profile_name=sysn,
            clean_mode=mode,
        )


@dataclass
class ScheduleJob:
    schedule_id: str
    job_id: str
    enabled: bool = True

    schedule_type: str = "weekly"  # weekly | daily | once
    dow: List[int] = field(default_factory=list)  # weekly only

    # for weekly/daily
    hh: int = 0
    mm: int = 0

    # for once
    at_ts: Optional[float] = None  # constant slot for oneshot

    oneshot: bool = False
    task: ScheduleTask = field(default_factory=ScheduleTask)

    # runtime
    next_fire_ts: float = math.inf
    last_fire_ts: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "schedule_id": self.schedule_id,
            "job_id": self.job_id,
            "enabled": bool(self.enabled),
            "schedule_type": self.schedule_type,
            "dow": list(self.dow or []),
            "hh": int(self.hh),
            "mm": int(self.mm),
            "at_ts": self.at_ts,
            "oneshot": bool(self.oneshot),
            "task": asdict(self.task),
            "next_fire_ts": float(self.next_fire_ts),
            "last_fire_ts": float(self.last_fire_ts),
        }
        return d


class Scheduler:
    def __init__(self, jobs: Optional[List[ScheduleJob]] = None):
        self.jobs: List[ScheduleJob] = jobs or []

    @staticmethod
    def from_param(schedules_param: Any, defaults: Optional[Dict[str, Any]] = None) -> "Scheduler":
        defaults = defaults or {}
        if schedules_param is None:
            schedules_param = []
        if not isinstance(schedules_param, list):
            raise ValueError(f"schedules param must be a list, got {type(schedules_param)}")

        jobs: List[ScheduleJob] = []

        for i, item in enumerate(schedules_param):
            if not isinstance(item, dict):
                raise ValueError(f"schedules[{i}] must be dict, got {type(item)}")

            # Reject legacy format explicitly to avoid silent “没触发”
            if any(k in item for k in ("action", "hhmm", "args")):
                raise ValueError(
                    f"schedules[{i}] uses legacy keys (action/hhmm/args). "
                    f"Use canonical keys: type/time/task (or once: type/at/task)."
                )

            schedule_id = str(item.get("id", "")).strip()
            if not schedule_id:
                raise ValueError(f"schedules[{i}] missing 'id'")
            job_id = str(item.get("job_id", "") or schedule_id).strip()
            if not job_id:
                raise ValueError(f"schedules[{i}] missing 'job_id'")

            enabled = bool(item.get("enabled", True))
            stype = str(item.get("type", "") or "").strip().lower()
            if stype not in ("weekly", "daily", "once"):
                raise ValueError(f"schedules[{i}].type must be weekly|daily|once, got '{stype}'")

            oneshot = bool(item.get("oneshot", False))
            if stype == "once":
                oneshot = True  # force

            dow = item.get("dow", [])
            if dow is None:
                dow = []
            if stype == "weekly":
                if not isinstance(dow, list) or len(dow) == 0:
                    raise ValueError(f"schedules[{i}].dow must be list[int] for weekly")
                for d in dow:
                    if not isinstance(d, int) or d < 0 or d > 6:
                        raise ValueError(f"schedules[{i}].dow invalid: {dow} (need 0..6)")
            else:
                dow = []  # ignore for daily/once

            hh = 0
            mm = 0
            at_ts = None

            if stype in ("weekly", "daily"):
                tstr = item.get("time", None)
                if tstr is None:
                    raise ValueError(f"schedules[{i}] type={stype} requires 'time: HH:MM'")
                hh, mm = _parse_hhmm(str(tstr))
            else:
                at = item.get("at", None)
                if at is None:
                    raise ValueError(f"schedules[{i}] type=once requires 'at: YYYY-MM-DD HH:MM'")
                at_ts = _parse_at_local(str(at))

            task_raw = item.get("task", {})
            if task_raw is None:
                task_raw = {}
            if not isinstance(task_raw, dict):
                raise ValueError(f"schedules[{i}].task must be dict, got {type(task_raw)}")

            _require_no_legacy_keys(task_raw, where=f"schedules[{i}].task")

            # apply defaults: plan_profile_name/sys_profile_name/clean_mode
            merged = dict(defaults)
            merged.update(task_raw)

            task = ScheduleTask(
                map_name=str(merged.get("map_name", "") or "").strip(),
                map_version=str(merged.get("map_version", "") or "").strip(),
                zone_id=str(merged.get("zone_id", "") or "").strip(),
                loops=int(merged.get("loops", 1) or 1),
                plan_profile_name=str(merged.get("plan_profile_name", "") or "").strip(),
                sys_profile_name=str(merged.get("sys_profile_name", "") or "").strip(),
                clean_mode=str(merged.get("clean_mode", "") or "").strip(),
            ).normalized()

            if not task.zone_id:
                raise ValueError(f"schedules[{i}] task.zone_id is required")

            job = ScheduleJob(
                schedule_id=schedule_id,
                job_id=job_id,
                enabled=enabled,
                schedule_type=stype,
                dow=list(dow),
                hh=int(hh),
                mm=int(mm),
                at_ts=at_ts,
                oneshot=oneshot,
                task=task,
            )
            jobs.append(job)

        sch = Scheduler(jobs)
        now = time.time()
        for job in sch.jobs:
            sch.update_next_fire(job, now, init=True)
        return sch

    def update_next_fire(self, job: ScheduleJob, now_ts: float, init: bool = False) -> None:
        if not job.enabled:
            job.next_fire_ts = math.inf
            return

        if job.schedule_type == "once":
            # keep constant; task_manager decides catch-up/window
            job.next_fire_ts = float(job.at_ts) if job.at_ts is not None else math.inf
            return

        # recurring: compute strictly future (>= now + eps)
        eps = 0.0 if init else 0.5
        job.next_fire_ts = self._compute_next_recurring(job, now_ts, eps=eps)

    def _compute_next_recurring(self, job: ScheduleJob, now_ts: float, eps: float = 0.5) -> float:
        lt = time.localtime(now_ts)
        now_wday = lt.tm_wday  # 0=Mon
        year, mon, mday = lt.tm_year, lt.tm_mon, lt.tm_mday

        if job.schedule_type == "daily":
            dows = list(range(7))
        else:
            dows = sorted(job.dow) if job.dow else list(range(7))

        best = None
        for target_wday in dows:
            delta = (target_wday - now_wday) % 7
            cand_tuple = (year, mon, mday + delta, int(job.hh), int(job.mm), 0, 0, 0, -1)
            cand_ts = float(time.mktime(cand_tuple))
            if cand_ts < float(now_ts) + eps:
                cand_ts += 7 * 86400.0
            if best is None or cand_ts < best:
                best = cand_ts

        return best if best is not None else math.inf

    def mark_fired(self, job: ScheduleJob, fired_slot_ts: float) -> None:
        job.last_fire_ts = float(fired_slot_ts)
        if job.oneshot or job.schedule_type == "once":
            job.enabled = False
            job.next_fire_ts = math.inf
        else:
            # move next into future
            self.update_next_fire(job, float(fired_slot_ts) + 1.0, init=False)
