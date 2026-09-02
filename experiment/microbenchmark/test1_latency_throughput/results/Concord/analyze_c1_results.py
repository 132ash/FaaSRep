#!/usr/bin/env python3
"""Summarize Concord c1 raw results by client concurrency.

Usage:
    python3 analyze_c1_results.py

The script reads raw_results/c1_<client_count>_raw.csv and writes
c1_summary_results.csv in this directory. Throughput matches run.py:
client_count divided by the mean end-to-end latency.
"""

from __future__ import annotations

import csv
import math
import re
import sys
from pathlib import Path
from statistics import fmean


SCRIPT_DIR = Path(__file__).resolve().parent
RAW_RESULTS_DIR = SCRIPT_DIR / "raw_results"
OUTPUT_FILE = SCRIPT_DIR / "c1_summary_results.csv"
RAW_FILE_PATTERN = re.compile(r"c1_(\d+)_raw\.csv$")


def percentile(values: list[float], quantile: float) -> float:
    """Return a linearly interpolated percentile, matching pandas.quantile."""
    sorted_values = sorted(values)
    position = (len(sorted_values) - 1) * quantile
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return sorted_values[lower_index]
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + (upper_value - lower_value) * (position - lower_index)


def read_latencies(raw_file: Path) -> list[float]:
    with raw_file.open(newline="") as file:
        reader = csv.DictReader(file)
        if "e2e_latency" not in (reader.fieldnames or []):
            raise ValueError("missing e2e_latency column")
        latencies = [float(row["e2e_latency"]) for row in reader]
    if not latencies:
        raise ValueError("no samples")
    return latencies


def main() -> int:
    if not RAW_RESULTS_DIR.is_dir():
        print(f"Error: raw results directory does not exist: {RAW_RESULTS_DIR}", file=sys.stderr)
        return 1

    rows = []
    for raw_file in RAW_RESULTS_DIR.iterdir():
        match = RAW_FILE_PATTERN.fullmatch(raw_file.name)
        if not match:
            continue
        client_count = int(match.group(1))
        try:
            latencies = read_latencies(raw_file)
        except (OSError, ValueError) as error:
            print(f"Error: cannot process {raw_file.name}: {error}", file=sys.stderr)
            return 1

        mean_latency = fmean(latencies)
        rows.append(
            {
                "workflow": "c1",
                "client_count": client_count,
                "sample_count": len(latencies),
                "p50_latency": percentile(latencies, 0.50),
                "p99_latency": percentile(latencies, 0.99),
                "avg_throughput": client_count / mean_latency,
            }
        )

    if not rows:
        print(
            f"Error: no files matching c1_<client_count>_raw.csv in {RAW_RESULTS_DIR}",
            file=sys.stderr,
        )
        return 1

    rows.sort(key=lambda row: row["client_count"])
    with OUTPUT_FILE.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "p50_latency": f"{row['p50_latency']:.4f}",
                    "p99_latency": f"{row['p99_latency']:.4f}",
                    "avg_throughput": f"{row['avg_throughput']:.4f}",
                }
            )

    print(f"Wrote {len(rows)} c1 concurrency summaries to: {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
