# Dynamic access-set retry-abort experiment

This experiment injects an application-level abort at one uniformly selected
`c4` function during repair. It does not alter an access set and it never
retries an injected-abort transaction.

Fixed settings: `c4`, 32 closed-loop clients, 100 requests/client, 4 KiB
objects, Zipf 0.9, fast path and optimistic repair enabled, and the sink's
ordinary `ABORT_PROB` set to zero. `run.py` checks these system settings before
submitting any request.

Run all probability points only when the deployed cluster has been initialized:

```bash
./run.sh
```

There are deliberately no HTTP, process-join, collector, or watchdog timeouts.
If the system stalls, leave `run.py` running and inspect the append-only raw CSV,
`logs/client_progress_*.json`, and component logs with:

```bash
python3 inspect_progress.py
```

The inspector is read-only. It does not clean Redis, containers, batch state,
or client processes.
