# Trace workflow

This directory is split by responsibility:

- `prepare/`: raw trace inputs, RPM inputs, trace extraction scripts, and generated segment JSON files.
- `execute/`: scripts that replay segment JSON files against the FaaSnap gateway.
- `result/`: raw per-segment execution output, merged summaries, and result analysis scripts.

## Prepare

Useful traces:

- `lowload`: generated from `prepare/raw/trace_tidy.json` by `prepare/scripts/split_lowload_trace.py`.
- `highload`: formerly named `varying`; generated from `prepare/rpm/highload.csv` by `prepare/scripts/split_highload_from_rpm.py`.
- Both traces are split into 30 segments. Each segment has a 2-minute measured `core_interval`; segments after index 0 also include a 30-second warmup prefix in `actual_interval`.

Key paths:

- Raw Azure trace: `prepare/raw/trace_tidy.json` and `prepare/raw/trace_tidy.zip`.
- Raw 2019 function traces: `prepare/raw/2019trace/`.
- RPM inputs: `prepare/rpm/lowload.csv` and `prepare/rpm/highload.csv`.
- Segment outputs: `prepare/segments/lowload/` and `prepare/segments/highload/`.

## Execute

`execute/run_segments.sh` selects `WORKFLOW`, `SYSTEM`, `TRACE`, and target segment IDs, then runs:

```bash
python3 execute/run_segment.py --segment prepare/segments/<trace>/segment_<id>.json --output result/segment_result/<trace>/<system>/<workflow>/result_segment_<id>.json
```

Recommended representative segment IDs are recorded in `execute/run_segments.sh`:

- `highload`: `1 6 17 24 26`
- `lowload`: `1 8 17 19 29`

On this branch the execution scripts default to `SYSTEM=OCC` and `WORKFLOW=banking_system`. Override them with environment variables when replaying another workflow.

## Result

- Raw segment execution output: `result/segment_result/<trace>/<system>/<workflow>/`.
- Merged/analyzed output: `result/summary/<trace>/<system>/`.
- Analysis scripts: `result/scripts/`.
- `result/scripts/merge_results.py` drops each segment's warmup prefix before computing merged statistics.
