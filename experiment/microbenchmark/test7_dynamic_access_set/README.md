# Dynamic access-set retry-abort experiment

This experiment marks a configurable fraction of `c4` requests for OCC-style
handling. At validation, a selected request whose functions are all clean stays
in reconciliation and does not inject an abort. A selected dirty request is
removed from its current batch and retried from scratch with abort injection
disabled. Pessimistic repair never injects an abort, and every experiment
request is expected to return successfully. The gateway also treats an
unexpected `INJECTED_DYNAMIC_ACCESS_ABORT` as the same internal OCC retry
signal rather than exposing it as a failed request.

Fixed settings: `c4`, 32 closed-loop clients, 100 requests/client, 4 KiB
objects, Zipf 0.9, fast path and optimistic repair enabled, and the sink's
ordinary `ABORT_PROB` set to zero. `run.py` checks these system settings before
submitting any request.

Run all probability points only when the deployed cluster has been initialized:

```bash
./run.sh
```

Before running after a code/schema change, rebuild and re-register c4 from the
repository root, then restart gateway, commit manager, transaction sink, and
WorkerSP:

```bash
bash scripts/worker_setup.sh microbenchmark
python3 src/initializer/initialize.py c4
```

The initializer replaces only c4's CouchDB metadata. A full
`scripts/db_setup.sh` reset is not required for this schema update.

`run.py` checks the live CouchDB `c4_function_info` schema and refuses to submit
requests when `retry_abort_func` has not been deployed.

The summary contains only `configured_abort_prob`, `actual_abort_count`
(internal OCC retries), `success_count`, successful-request P50/P99 latency,
and successful-request throughput. Any terminal failure prevents a normal
summary row from being written.

There are deliberately no HTTP, process-join, collector, or watchdog timeouts.
Each probability point creates a unique directory under the repository's
`logging/` directory. Files are split by component, for example:

```text
logging/<run_id>/
├── manifest.json
├── driver.log
├── client.log
├── client_progress.json
├── gateway.log
├── gateway_runtime.log
├── workersp.log
├── workersp_runtime.log
├── workflow_manager_proxy.log
├── transaction_sink.log
├── transaction_sink_runtime.log
├── commit_manager_proxy.log
├── c4_validator_0.log
├── c4_serializer.log
└── container.log
```

If the system stalls, leave `run.py` running and inspect the active run with:

```bash
python3 inspect_progress.py
```

The inspector is read-only. It does not clean Redis, containers, batch state,
or client processes.
