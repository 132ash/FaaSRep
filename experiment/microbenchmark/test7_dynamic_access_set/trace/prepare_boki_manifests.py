#!/usr/bin/env python3
"""Generate a Boki-SN input manifest without sending any gateway requests."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(ROOT_DIR))

from boki_manifest import create_manifest


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--segment', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trace', required=True)
    parser.add_argument('--dataset', type=Path,
                        default=ROOT_DIR / 'experiment/microbenchmark/db_keys.json')
    parser.add_argument('--seed', type=int, default=20260827)
    parser.add_argument('--zipf', type=float, default=0.9)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    metadata = create_manifest(args.segment, args.output, args.dataset, args.trace, args.seed, args.zipf)
    print(f"[manifest ready] path={args.output} sha256={metadata['manifest_sha256']} "
          f"requests={metadata['request_count']}")
