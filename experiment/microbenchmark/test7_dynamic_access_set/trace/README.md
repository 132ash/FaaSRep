# OCC open-loop dynamic-access-set trace

This directory replays the trace slices prepared by
`experiment/actual_apps/test7_colocate_apps/trace`, while generating a fresh
`c4` microbenchmark input for every recorded arrival. Requests are fired at
their recorded offsets without waiting for earlier responses, so this is an
open-loop OCC experiment.

This implementation is intentionally OCC-only. It does not import the Repair
branch's `REPAIR`, retry-abort probability, repair mode, or pessimistic fields.
The gateway's `rounds` value is reported as one initial OCC execution plus its
retries.

Before running, enable `c4` in `config.WORKFLOW_YAML_ADDR`, initialize it, and
restart the long-lived services and WorkerSP processes:

```bash
python3 src/initializer/initialize.py c4
bash experiment/microbenchmark/test7_dynamic_access_set/trace/run_segments.sh
```

Useful overrides:

```bash
TRACE=highload \
TARGET_SEGMENT_INDICES_OVERRIDE="1 6 17 24 26" \
ZIPF_PARAM=0.9 \
REQUEST_TIMEOUT=300 \
bash experiment/microbenchmark/test7_dynamic_access_set/trace/run_segments.sh
```

`SEGMENTS_ROOT` may point at another directory containing
`<trace>/segment_<index>.json`. Set `REQUEST_TIMEOUT=0` only when an indefinite
wait is desired.

Compact outputs are written below `results/occ/`:

- `raw_results/<trace>/`: one CSV per segment run;
- `summary_results_<trace>.csv`: successful core count, OCC retries, P50/P99,
  and throughput;
- `progress/<run_id>.json`: scheduled, completed, failed, and waiting IDs.

A summary row is written only when every request returns successfully. Raw and
progress files are retained when a timeout or request error occurs.
