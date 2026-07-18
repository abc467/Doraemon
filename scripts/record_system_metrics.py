#!/usr/bin/env python3

import argparse
import csv
import datetime
import signal
import time
from pathlib import Path


STOP_REQUESTED = False
IGNORED_BLOCK_DEVICE_PREFIXES = (
    "loop",
    "ram",
    "zram",
    "fd",
    "sr",
)


def request_stop(_signum, _frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def read_cpu_times():
    with open("/proc/stat", "r", encoding="ascii") as stat_file:
        fields = stat_file.readline().split()
    if not fields or fields[0] != "cpu":
        raise RuntimeError("cannot read aggregate CPU counters from /proc/stat")
    values = [int(value) for value in fields[1:9]]
    while len(values) < 8:
        values.append(0)
    return values


def read_memory():
    values = {}
    with open("/proc/meminfo", "r", encoding="ascii") as meminfo_file:
        for line in meminfo_file:
            key, raw_value = line.split(":", 1)
            parts = raw_value.split()
            if parts:
                values[key] = int(parts[0]) * 1024

    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable")
    if available is None:
        available = (
            values.get("MemFree", 0)
            + values.get("Buffers", 0)
            + values.get("Cached", 0)
        )
    swap_total = values.get("SwapTotal", 0)
    swap_free = values.get("SwapFree", 0)
    return {
        "total": total,
        "available": available,
        "used": max(0, total - available),
        "swap_total": swap_total,
        "swap_used": max(0, swap_total - swap_free),
    }


def read_load_average():
    with open("/proc/loadavg", "r", encoding="ascii") as loadavg_file:
        fields = loadavg_file.readline().split()
    return [float(value) for value in fields[:3]]


def find_physical_block_devices():
    devices = []
    for block_path in sorted(Path("/sys/block").glob("*")):
        name = block_path.name
        if name.startswith(IGNORED_BLOCK_DEVICE_PREFIXES):
            continue
        slaves_path = block_path / "slaves"
        if slaves_path.exists() and any(slaves_path.iterdir()):
            continue
        devices.append(name)
    return devices


def read_disk_counters(devices):
    wanted = set(devices)
    counters = {}
    with open("/proc/diskstats", "r", encoding="ascii") as diskstats_file:
        for line in diskstats_file:
            fields = line.split()
            if len(fields) < 13 or fields[2] not in wanted:
                continue
            counters[fields[2]] = {
                "reads": int(fields[3]),
                "read_sectors": int(fields[5]),
                "writes": int(fields[7]),
                "write_sectors": int(fields[9]),
                "io_ms": int(fields[12]),
            }
    return counters


def percentage(numerator, denominator):
    if denominator <= 0:
        return 0.0
    return 100.0 * numerator / denominator


def cpu_percentages(previous, current):
    delta = [max(0, end - start) for start, end in zip(previous, current)]
    total = sum(delta)
    idle = delta[3]
    iowait = delta[4]
    return {
        "usage": percentage(total - idle, total),
        "user": percentage(delta[0] + delta[1], total),
        "system": percentage(delta[2] + delta[5] + delta[6], total),
        "iowait": percentage(iowait, total),
        "idle": percentage(idle, total),
    }


def disk_rates(previous, current, interval):
    totals = {
        "read_bytes_s": 0.0,
        "write_bytes_s": 0.0,
        "read_iops": 0.0,
        "write_iops": 0.0,
        "busy_max_pct": 0.0,
    }
    for device, current_values in current.items():
        previous_values = previous.get(device)
        if previous_values is None:
            continue
        read_sectors = max(
            0, current_values["read_sectors"] - previous_values["read_sectors"]
        )
        write_sectors = max(
            0, current_values["write_sectors"] - previous_values["write_sectors"]
        )
        reads = max(0, current_values["reads"] - previous_values["reads"])
        writes = max(0, current_values["writes"] - previous_values["writes"])
        io_ms = max(0, current_values["io_ms"] - previous_values["io_ms"])

        totals["read_bytes_s"] += read_sectors * 512.0 / interval
        totals["write_bytes_s"] += write_sectors * 512.0 / interval
        totals["read_iops"] += reads / interval
        totals["write_iops"] += writes / interval
        totals["busy_max_pct"] = max(
            totals["busy_max_pct"], min(100.0, 100.0 * io_ms / (interval * 1000.0))
        )
    return totals


def mib(value):
    return value / (1024.0 * 1024.0)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Record Linux CPU, memory, load average, and disk I/O to CSV."
    )
    parser.add_argument("--output", required=True, help="CSV output path")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Sampling interval in seconds (default: 1.0)",
    )
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    return args


def main():
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    devices = find_physical_block_devices()
    previous_cpu = read_cpu_times()
    previous_disk = read_disk_counters(devices)
    previous_time = time.monotonic()
    next_sample_time = previous_time + args.interval

    fieldnames = [
        "timestamp_unix",
        "timestamp_iso",
        "sample_interval_s",
        "cpu_usage_pct",
        "cpu_user_pct",
        "cpu_system_pct",
        "cpu_iowait_pct",
        "cpu_idle_pct",
        "load_1m",
        "load_5m",
        "load_15m",
        "memory_total_mib",
        "memory_used_mib",
        "memory_available_mib",
        "memory_used_pct",
        "swap_used_mib",
        "swap_total_mib",
        "swap_used_pct",
        "disk_read_mib_s",
        "disk_write_mib_s",
        "disk_read_iops",
        "disk_write_iops",
        "disk_busy_max_pct",
        "disk_devices",
    ]

    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        output_file.flush()

        while not STOP_REQUESTED:
            remaining = next_sample_time - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            if STOP_REQUESTED:
                break

            sample_time = time.monotonic()
            interval = sample_time - previous_time
            current_cpu = read_cpu_times()
            current_disk = read_disk_counters(devices)
            memory = read_memory()
            load_1m, load_5m, load_15m = read_load_average()
            cpu = cpu_percentages(previous_cpu, current_cpu)
            disk = disk_rates(previous_disk, current_disk, interval)
            wall_time = time.time()

            writer.writerow(
                {
                    "timestamp_unix": f"{wall_time:.6f}",
                    "timestamp_iso": datetime.datetime.now()
                    .astimezone()
                    .isoformat(timespec="milliseconds"),
                    "sample_interval_s": f"{interval:.6f}",
                    "cpu_usage_pct": f"{cpu['usage']:.3f}",
                    "cpu_user_pct": f"{cpu['user']:.3f}",
                    "cpu_system_pct": f"{cpu['system']:.3f}",
                    "cpu_iowait_pct": f"{cpu['iowait']:.3f}",
                    "cpu_idle_pct": f"{cpu['idle']:.3f}",
                    "load_1m": f"{load_1m:.3f}",
                    "load_5m": f"{load_5m:.3f}",
                    "load_15m": f"{load_15m:.3f}",
                    "memory_total_mib": f"{mib(memory['total']):.3f}",
                    "memory_used_mib": f"{mib(memory['used']):.3f}",
                    "memory_available_mib": f"{mib(memory['available']):.3f}",
                    "memory_used_pct": f"{percentage(memory['used'], memory['total']):.3f}",
                    "swap_used_mib": f"{mib(memory['swap_used']):.3f}",
                    "swap_total_mib": f"{mib(memory['swap_total']):.3f}",
                    "swap_used_pct": f"{percentage(memory['swap_used'], memory['swap_total']):.3f}",
                    "disk_read_mib_s": f"{mib(disk['read_bytes_s']):.3f}",
                    "disk_write_mib_s": f"{mib(disk['write_bytes_s']):.3f}",
                    "disk_read_iops": f"{disk['read_iops']:.3f}",
                    "disk_write_iops": f"{disk['write_iops']:.3f}",
                    "disk_busy_max_pct": f"{disk['busy_max_pct']:.3f}",
                    "disk_devices": ";".join(devices),
                }
            )
            output_file.flush()

            previous_cpu = current_cpu
            previous_disk = current_disk
            previous_time = sample_time
            next_sample_time += args.interval
            if next_sample_time <= sample_time:
                next_sample_time = sample_time + args.interval


if __name__ == "__main__":
    main()
