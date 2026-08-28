# Dynamic access-set open-loop trace experiment

This directory replays the same trace slices used by
`experiment/actual_apps/test7_colocate_apps/trace`, but generates `c4`
microbenchmark inputs for every arrival. Requests are fired at the recorded
timestamps without waiting for earlier responses, so the driver is open-loop.
The source trace's warmup prefix is executed but excluded from the summary.

Before running, initialize the current c4 schema and restart the long-lived
services and workflow containers. `FAST_PATH` and `OPTIMISTIC_REPAIR` must be
enabled, and `ABORT_PROB` must be zero.

Run the five abort probabilities over the representative low-load slices:

```bash
bash run_segments.sh
```

The default source is the existing actual-app trace directory, so no large
segment files are duplicated here. Useful overrides are:

```bash
TRACE=highload \
TARGET_SEGMENT_INDICES_OVERRIDE="1 6 17 24 26" \
ABORT_PROBS_OVERRIDE="0 0.50 1.00" \
bash run_segments.sh
```

`SEGMENTS_ROOT` may point at any directory containing
`<trace>/segment_<index>.json` files with `requests`, `actual_interval`, and
`core_interval` fields.

Results stay compact:

- `results/<mode>/raw_results/<trace>/`: append-as-responses-arrive raw CSVs.
- `results/<mode>/summary_results_<trace>.csv`: probability, internal OCC
  retries, successful count, P50/P99, and successful throughput per segment.
- `logging/<run_id>/client_progress.json`: waiting transaction IDs while a
  segment is running.

There are no request or join timeouts. If the system stalls, leave the driver
running and inspect `client_progress.json` together with the component logs.
No summary row is written unless every request returns `status=ok`.
