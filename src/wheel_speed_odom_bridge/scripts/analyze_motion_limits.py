#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import math


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Analyze wheel_speed_odom_debug.csv and recommend motion guard limits. "
            "Collect data with motion_guard_mode=warn_only for best results."
        )
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default="wheel_speed_odom_debug.csv",
        help="Path to wheel_speed odom diagnostic CSV. Default: wheel_speed_odom_debug.csv",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default=99.9,
        help="Percentile used for baseline recommendations. Default: 99.9",
    )
    parser.add_argument(
        "--margin",
        type=float,
        default=1.20,
        help="Safety margin multiplier applied to the chosen percentile. Default: 1.20",
    )
    parser.add_argument(
        "--max-margin",
        type=float,
        default=1.10,
        help="Minimum margin multiplier applied to the observed maximum. Default: 1.10",
    )
    return parser.parse_args()


def safe_float(raw_value):
    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def percentile(values, percentile_value):
    if not values:
        return float("nan")
    if percentile_value <= 0.0:
        return values[0]
    if percentile_value >= 100.0:
        return values[-1]

    position = (len(values) - 1) * (percentile_value / 100.0)
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return values[lower_index]

    lower_value = values[lower_index]
    upper_value = values[upper_index]
    ratio = position - lower_index
    return lower_value * (1.0 - ratio) + upper_value * ratio


def load_rows(csv_path):
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        lines = [line.rstrip("\n") for line in handle if line.strip()]

    if not lines:
        raise RuntimeError(f"No data found in {csv_path}")

    header_line = lines[0]
    if header_line.startswith("#"):
        header_line = header_line[1:].strip()

    fieldnames = next(csv.reader([header_line]))
    reader = csv.DictReader(lines[1:], fieldnames=fieldnames)
    return list(reader)


def absolute_series(rows, column_name):
    values = []
    for row in rows:
        value = safe_float(row.get(column_name))
        if value is not None:
            values.append(abs(value))
    values.sort()
    return values


def summarize_series(name, values, percentile_value, margin, max_margin):
    if not values:
        return {
            "name": name,
            "count": 0,
            "max": float("nan"),
            "p99": float("nan"),
            "pctl": float("nan"),
            "recommended": float("nan"),
        }

    observed_max = values[-1]
    p99 = percentile(values, 99.0)
    selected_percentile = percentile(values, percentile_value)
    recommended = max(selected_percentile * margin, observed_max * max_margin)
    return {
        "name": name,
        "count": len(values),
        "max": observed_max,
        "p99": p99,
        "pctl": selected_percentile,
        "recommended": recommended,
    }


def format_float(value):
    if value is None or not math.isfinite(value):
        return "nan"
    return f"{value:.6f}"


def main():
    args = parse_args()
    rows = load_rows(args.csv_path)

    abs_linear_speed = absolute_series(rows, "linear_velocity")
    abs_angular_speed = absolute_series(rows, "angular_velocity")
    abs_linear_accel = absolute_series(rows, "linear_accel")
    abs_angular_accel = absolute_series(rows, "angular_accel")

    summaries = [
        summarize_series(
            "max_abs_linear_speed",
            abs_linear_speed,
            args.percentile,
            args.margin,
            args.max_margin,
        ),
        summarize_series(
            "max_abs_angular_speed",
            abs_angular_speed,
            args.percentile,
            args.margin,
            args.max_margin,
        ),
        summarize_series(
            "max_abs_linear_accel",
            abs_linear_accel,
            args.percentile,
            args.margin,
            args.max_margin,
        ),
        summarize_series(
            "max_abs_angular_accel",
            abs_angular_accel,
            args.percentile,
            args.margin,
            args.max_margin,
        ),
    ]

    print(f"CSV: {args.csv_path}")
    print(f"Rows: {len(rows)}")
    print(
        "Recommendation basis: "
        f"percentile={args.percentile:.3f}, margin={args.margin:.3f}, max_margin={args.max_margin:.3f}"
    )
    print()
    print("Observed statistics:")
    for summary in summaries:
        print(
            f"  {summary['name']}: "
            f"count={summary['count']}, "
            f"max={format_float(summary['max'])}, "
            f"p99={format_float(summary['p99'])}, "
            f"p{args.percentile:g}={format_float(summary['pctl'])}, "
            f"recommended={format_float(summary['recommended'])}"
        )

    print()
    print("Suggested launch params:")
    print("  motion_guard_mode: reject")
    for summary in summaries:
        print(f"  {summary['name']}: {format_float(summary['recommended'])}")


if __name__ == "__main__":
    main()
