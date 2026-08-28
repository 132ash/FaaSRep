# Dynamic access-set retry-abort experiment

This experiment injects an application-level abort at one uniformly selected
`c4` function during optimistic repair. Pessimistic repair never injects an
abort. The experiment does not alter an access set and it never retries an
injected-abort transaction.

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
