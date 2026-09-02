#!/usr/bin/env python3
"""汇总 Repair 模式下 c1 工作流的延迟与吞吐结果。

默认读取本脚本同级 ``raw_results/c1_<并发度>_raw.csv``，并生成
``c1_summary_results.csv``。吞吐量的计算方式与测试脚本 ``run.py`` 保持一致：
并发度 / 平均端到端延迟（RPS）。
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RAW_RESULTS_DIR = SCRIPT_DIR / "raw_results"
DEFAULT_OUTPUT = SCRIPT_DIR / "c1_summary_results.csv"
RAW_FILE_PATTERN = re.compile(r"^c1_(\d+)_raw\.csv$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="汇总 c1 在各并发度下的 p50、p99 延迟与吞吐量。"
    )
    parser.add_argument(
        "--raw-results-dir",
        type=Path,
        default=DEFAULT_RAW_RESULTS_DIR,
        help=f"原始 CSV 所在目录（默认：{DEFAULT_RAW_RESULTS_DIR}）",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"汇总 CSV 输出路径（默认：{DEFAULT_OUTPUT}）",
    )
    return parser.parse_args()


def find_c1_result_files(raw_results_dir: Path) -> list[tuple[int, Path]]:
    """返回按并发度升序排列的 c1 原始结果文件。"""
    files: list[tuple[int, Path]] = []
    for path in raw_results_dir.glob("c1_*_raw.csv"):
        match = RAW_FILE_PATTERN.fullmatch(path.name)
        if match:
            files.append((int(match.group(1)), path))
    return sorted(files)


def summarize(raw_results_dir: Path) -> pd.DataFrame:
    if not raw_results_dir.is_dir():
        raise FileNotFoundError(f"原始结果目录不存在：{raw_results_dir}")

    rows: list[dict[str, float | int | str]] = []
    for concurrency, path in find_c1_result_files(raw_results_dir):
        df = pd.read_csv(path)
        if "e2e_latency" not in df.columns:
            raise ValueError(f"{path} 缺少 e2e_latency 列")

        latencies = pd.to_numeric(df["e2e_latency"], errors="coerce").dropna()
        if latencies.empty:
            raise ValueError(f"{path} 没有有效的 e2e_latency 数据")

        mean_latency = latencies.mean()
        rows.append(
            {
                "workflow": "c1",
                "client_count": concurrency,
                "p50_latency": latencies.quantile(0.50),
                "p99_latency": latencies.quantile(0.99),
                "avg_throughput": concurrency / mean_latency,
            }
        )

    if not rows:
        raise FileNotFoundError(
            f"未在 {raw_results_dir} 找到符合 c1_<并发度>_raw.csv 格式的文件"
        )
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    result = summarize(args.raw_results_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False, float_format="%.4f")

    print(result.to_string(index=False, float_format=lambda value: f"{value:.4f}"))
    print(f"\n汇总结果已写入：{args.output}")


if __name__ == "__main__":
    main()
