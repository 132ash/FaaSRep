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
wait is desired. The highload splitter uses 2.5-minute core windows with a
10-second warmup prefix; the default target is `segment_5` (core `[750, 900]`,
actual replay interval `[740, 900]`).

Compact outputs are written below `results/occ/`:

- `raw_results/<trace>/`: one CSV per segment run;
- `summary_results_<trace>.csv`: successful core count, OCC retries, P50/P99,
  and throughput;
- `progress/<run_id>.json`: scheduled, completed, failed, and waiting IDs.

A summary row is written only when every request returns successfully. Raw and
progress files are retained when a timeout or request error occurs.

## Boki-style-SN preparation

The OCC runner above must not be used for Boki-SN results. Boki-SN has a
separate deterministic manifest and runner:

```bash
TRACE=highload TARGET_SEGMENT_INDICES_OVERRIDE="5" \
  bash prepare_boki_manifests.sh
```

This generates `manifests/<trace>/c4_zipf0.9_segment_<n>.jsonl` and a metadata
sidecar without contacting the gateway. When ready to run a Boki-SN replay,
start the SUT with `SYSTEM_MODE=BOKI_SN` and use:

```bash
SYSTEM_MODE=BOKI_SN TRACE=highload TARGET_SEGMENT_INDICES_OVERRIDE="5" \
  bash run_boki_segments.sh
```

Results are isolated below `results/boki_style_single_node/`; the Boki schema
includes term, retry/Wait-Die counters, lock, shadow and flush metrics.
