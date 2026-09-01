#!/usr/bin/env python3
"""Validate and display Boki-SN closed-loop skewness summaries."""
from __future__ import annotations

import csv
from pathlib import Path

from run import SUMMARY_FIELDS


def load_summary(path):
    path = Path(path)
    with path.open(newline='', encoding='utf-8') as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != SUMMARY_FIELDS:
            raise ValueError(f'incompatible summary header in {path}: {reader.fieldnames}')
        rows = list(reader)
    return sorted(rows, key=lambda row: (float(row['zipf']), int(row['client_count'])))


if __name__ == '__main__':
    summary_path = Path(__file__).parent / 'results/boki_style_single_node/summary_results.csv'
    for row in load_summary(summary_path):
        print(','.join(row[field] for field in SUMMARY_FIELDS))
