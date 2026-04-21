from __future__ import annotations

import argparse
import concurrent.futures
import os
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from correctness_lib import (
    ScenarioResult,
    default_gateway_url,
    get_data_item,
    put_data_items,
    run_workflow,
    safe_key_fragment,
    write_json,
)


ROOT_DIR = Path(__file__).resolve().parents[3]
LOGGING_DIR = ROOT_DIR / "logging"
LOG_RUN_ID = safe_key_fragment(os.environ.get("FAASNAP_LOG_RUN_ID", "").strip())

if not LOG_RUN_ID:
    raise RuntimeError(
        "FAASNAP_LOG_RUN_ID is required. Run via run_all.sh or export FAASNAP_LOG_RUN_ID first."
    )

RUN_LOG_DIR = LOGGING_DIR / "runs" / LOG_RUN_ID
RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)


ScenarioKeys = Tuple[str, str, str, str, str]
SCENARIO_PROGRESS: Dict[str, Dict[str, Any]] = {}
SCENARIO_PROGRESS_LOCK = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def scenario_dir(name: str) -> Path:
    return RUN_LOG_DIR / safe_key_fragment(name)


def append_runner_log(name: str, message: str) -> None:
    line = f"[{now_iso()}] {message}\n"
    run_log = RUN_LOG_DIR / "run_gateway_suite.log"
    run_log.parent.mkdir(parents=True, exist_ok=True)
    with run_log.open("a", encoding="utf-8") as f:
        f.write(line)

    if name:
        per_scenario_log = scenario_dir(name) / "run_gateway_suite.log"
        per_scenario_log.parent.mkdir(parents=True, exist_ok=True)
        with per_scenario_log.open("a", encoding="utf-8") as f:
            f.write(line)


def emit_console(message: str) -> None:
    print(message, flush=True)


def scenario_banner(index: int, total: int, name: str) -> str:
    return f"[{index}/{total}] {name}"


def summarize_responses(responses: List[Dict[str, Any]]) -> str:
    if not responses:
        return "ok=0 aborted=0 other=0"
    ok = sum(1 for item in responses if item.get("status") == "ok")
    aborted = sum(1 for item in responses if item.get("status") == "aborted")
    other = len(responses) - ok - aborted
    rounds3 = sum(1 for item in responses if item.get("rounds") == 3)
    return f"ok={ok} aborted={aborted} other={other} rounds3={rounds3}"


def set_scenario_progress(name: str, payload: Dict[str, Any] | None) -> None:
    with SCENARIO_PROGRESS_LOCK:
        if payload is None:
            SCENARIO_PROGRESS.pop(name, None)
        else:
            SCENARIO_PROGRESS[name] = dict(payload)


def get_scenario_progress(name: str) -> Dict[str, Any]:
    with SCENARIO_PROGRESS_LOCK:
        return dict(SCENARIO_PROGRESS.get(name, {}))


def write_scenario_status(name: str, payload: Dict[str, Any]) -> None:
    target = scenario_dir(name) / "status.json"
    status_payload = {"updated_at": now_iso(), **payload}
    write_json(target, status_payload)


def write_progress(
    output_path: Path,
    gateway_url: str,
    results: List[ScenarioResult],
    *,
    running: Dict[str, Any] | None = None,
    error: Dict[str, Any] | None = None,
) -> None:
    payload: Dict[str, Any] = {
        "gateway_url": gateway_url,
        "run_id": LOG_RUN_ID,
        "updated_at": now_iso(),
        "running": running,
        "results": [result.to_json() for result in results],
    }
    if error is not None:
        payload["error"] = error
    write_json(output_path, payload)


def set_active_experiment(name: str) -> None:
    experiment = safe_key_fragment(name)
    active_file = LOGGING_DIR / ".active_experiment"
    active_file.parent.mkdir(parents=True, exist_ok=True)
    active_file.write_text(experiment, encoding="utf-8")

    if experiment:
        (LOGGING_DIR / "runs" / LOG_RUN_ID / experiment).mkdir(parents=True, exist_ok=True)


def scenario_keys(name: str) -> ScenarioKeys:
    prefix = f"rc_{LOG_RUN_ID}_{safe_key_fragment(name)}"
    return (
        f"{prefix}_hot",
        f"{prefix}_tail",
        f"{prefix}_guard",
        f"{prefix}_audit",
        f"{prefix}_result",
    )


def params(
    label: str,
    hot_key: str,
    tail_key: str,
    guard_key: str,
    audit_key: str,
    result_key: str,
    delta: int = 1,
    guard_delta: int = 0,
    guard_abort_threshold: str = "",
) -> Dict[str, Dict[str, str]]:
    return {
        "claim": {
            "tx_label": label,
            "delta": str(delta),
            "guard_delta": str(guard_delta),
            "guard_abort_threshold": guard_abort_threshold,
            "hot_key": hot_key,
            "tail_key": tail_key,
            "guard_key": guard_key,
            "audit_key": audit_key,
            "result_key": result_key,
        }
    }


def reset_state(keys: ScenarioKeys) -> None:
    hot_key, tail_key, guard_key, audit_key, result_key = keys
    put_data_items({
        hot_key: "0",
        tail_key: "seed:0",
        guard_key: "0",
        audit_key: "seed:audit",
        result_key: "seed:result",
    })


def actual_hot(hot_key: str) -> int:
    return int(get_data_item(hot_key)["value"])


def assert_deployed_key(response: Dict[str, Any], keys: ScenarioKeys) -> None:
    if response.get("status") != "ok":
        return
    hot_key, tail_key, guard_key, audit_key, result_key = keys
    result = response.get("res", {})
    expected = {
        "final_hot_key": hot_key,
        "final_tail_key": tail_key,
        "final_guard_key": guard_key,
        "final_audit_key": audit_key,
        "final_result_key": result_key,
    }
    actual = {name: result.get(name) for name in expected}
    if actual != expected:
        raise AssertionError(
            "repair_correctness deployment is stale: expected final key echo "
            f"{expected}, got {actual}. "
            "Run `bash scripts/db_setup.sh repair_correctness`, "
            "`bash scripts/worker_setup.sh repair_correctness`, then restart gateway/workersp/validator/sink."
        )


def assert_deployed_keys(responses: List[Dict[str, Any]], keys: ScenarioKeys) -> None:
    for response in responses:
        assert_deployed_key(response, keys)


def run_concurrent(
    labels: List[str],
    gateway_url: str,
    keys: ScenarioKeys,
    workers: int = 4,
    request_timeout: float | None = None,
) -> List[Dict[str, Any]]:
    hot_key, tail_key, guard_key, audit_key, result_key = keys
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                run_workflow,
                params(label, hot_key, tail_key, guard_key, audit_key, result_key, guard_delta=1),
                "repair_correctness",
                gateway_url,
                request_timeout,
            )
            for label in labels
        ]
        return [future.result() for future in futures]


def optimistic_chain(gateway_url: str, concurrency: int, request_timeout: float | None) -> ScenarioResult:
    keys = scenario_keys("optimistic_chain")
    hot_key, tail_key, guard_key, audit_key, result_key = keys
    reset_state(keys)
    labels = [f"opt-{idx}" for idx in range(concurrency)]
    responses = run_concurrent(labels, gateway_url, keys, workers=concurrency, request_timeout=request_timeout)
    assert_deployed_keys(responses, keys)
    result = ScenarioResult(
        name="optimistic_chain",
        responses=responses,
        expected_hot=sum(1 for response in responses if response.get("status") == "ok"),
        actual_hot=actual_hot(hot_key),
        hot_key=hot_key,
        tail_key=tail_key,
        guard_key=guard_key,
        audit_key=audit_key,
        result_key=result_key,
    )
    result.assert_valid()
    return result


def pessimistic_fallback(gateway_url: str, stagger_s: float, request_timeout: float | None) -> ScenarioResult:
    keys = scenario_keys("pessimistic_fallback")
    hot_key, tail_key, guard_key, audit_key, result_key = keys
    reset_state(keys)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            run_workflow,
            params("fallback-writer", hot_key, tail_key, guard_key, audit_key, result_key, guard_delta=1),
            "repair_correctness",
            gateway_url,
            request_timeout,
        )
        time.sleep(stagger_s)
        second = pool.submit(
            run_workflow,
            params(
                "fallback-aborter",
                hot_key,
                tail_key,
                guard_key,
                audit_key,
                result_key,
                guard_abort_threshold="1",
            ),
            "repair_correctness",
            gateway_url,
            request_timeout,
        )
        responses = [first.result(), second.result()]
    assert_deployed_keys(responses, keys)

    result = ScenarioResult(
        name="pessimistic_fallback",
        responses=responses,
        expected_hot=sum(1 for response in responses if response.get("status") == "ok"),
        actual_hot=actual_hot(hot_key),
        hot_key=hot_key,
        tail_key=tail_key,
        guard_key=guard_key,
        audit_key=audit_key,
        result_key=result_key,
        min_aborts=1,
    )
    result.assert_valid()
    return result


def sequential_ryw(gateway_url: str, request_timeout: float | None) -> ScenarioResult:
    keys = scenario_keys("sequential_ryw")
    hot_key, tail_key, guard_key, audit_key, result_key = keys
    reset_state(keys)
    responses: List[Dict[str, Any]] = []
    target_successes = 2
    max_attempts = 10
    for attempt in range(max_attempts):
        emit_console(
            f"  sequential_ryw attempt {attempt + 1}/{max_attempts}: "
            f"successes={sum(1 for item in responses if item.get('status') == 'ok')}/{target_successes}"
        )
        label = f"ryw-{attempt}"
        response = run_workflow(
            params(label, hot_key, tail_key, guard_key, audit_key, result_key, guard_delta=1),
            "repair_correctness",
            gateway_url,
            request_timeout,
        )
        responses.append(response)
        if sum(1 for item in responses if item.get("status") == "ok") >= target_successes:
            break
    assert_deployed_keys(responses, keys)
    successes = sum(1 for response in responses if response.get("status") == "ok")
    result = ScenarioResult(
        name="sequential_ryw",
        responses=responses,
        expected_hot=successes,
        actual_hot=actual_hot(hot_key),
        hot_key=hot_key,
        tail_key=tail_key,
        guard_key=guard_key,
        audit_key=audit_key,
        result_key=result_key,
        min_successes=target_successes,
    )
    result.assert_valid()
    return result


def cascaded_pessimistic_retry(gateway_url: str, stagger_s: float, request_timeout: float | None) -> ScenarioResult:
    max_attempts = 5
    last_result = None
    for attempt in range(max_attempts):
        emit_console(f"  cascaded_pessimistic_retry attempt {attempt + 1}/{max_attempts}")
        keys = scenario_keys(f"cascaded_pessimistic_retry_{attempt}")
        hot_key, tail_key, guard_key, audit_key, result_key = keys
        reset_state(keys)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            base = pool.submit(
                run_workflow,
                params(
                    f"cascade-base-{attempt}",
                    hot_key,
                    tail_key,
                    guard_key,
                    audit_key,
                    result_key,
                    guard_delta=1,
                ),
                "repair_correctness",
                gateway_url,
                request_timeout,
            )
            time.sleep(stagger_s)
            aborting_predecessor = pool.submit(
                run_workflow,
                params(
                    f"cascade-abort-{attempt}",
                    hot_key,
                    tail_key,
                    guard_key,
                    audit_key,
                    result_key,
                    guard_abort_threshold="1",
                ),
                "repair_correctness",
                gateway_url,
                request_timeout,
            )
            time.sleep(stagger_s)
            successor = pool.submit(
                run_workflow,
                params(
                    f"cascade-successor-{attempt}",
                    hot_key,
                    tail_key,
                    guard_key,
                    audit_key,
                    result_key,
                ),
                "repair_correctness",
                gateway_url,
                request_timeout,
            )
            responses = [base.result(), aborting_predecessor.result(), successor.result()]

        assert_deployed_keys(responses, keys)
        successes = sum(1 for response in responses if response.get("status") == "ok")
        result = ScenarioResult(
            name="cascaded_pessimistic_retry",
            responses=responses,
            expected_hot=successes,
            actual_hot=actual_hot(hot_key),
            hot_key=hot_key,
            tail_key=tail_key,
            guard_key=guard_key,
            audit_key=audit_key,
            result_key=result_key,
            min_aborts=1,
            min_successes=2,
            min_pessimistic_successes=1,
        )
        last_result = result
        emit_console(
            "  cascaded_pessimistic_retry attempt result: "
            f"{summarize_responses(responses)} actual_hot={result.actual_hot}"
        )
        try:
            result.assert_valid()
            return result
        except AssertionError as exc:
            emit_console(f"  cascaded_pessimistic_retry retrying after validation failure: {exc}")
            if attempt == max_attempts - 1:
                raise
    return last_result


def stress_mixed_workload(
    gateway_url: str,
    concurrency: int,
    duration_s: float,
    key_groups: int,
    request_timeout: float | None,
) -> ScenarioResult:
    if duration_s <= 0:
        return ScenarioResult(
            name="stress_mixed_workload",
            responses=[],
            expected_hot=0,
            actual_hot=0,
            hot_key="stress_disabled",
        )

    concurrency = max(1, concurrency)
    key_groups = max(1, key_groups)
    group_keys = [scenario_keys(f"stress_g{group_id}") for group_id in range(key_groups)]
    for keys in group_keys:
        reset_state(keys)

    responses: List[Dict[str, Any]] = []
    future_info: Dict[concurrent.futures.Future, Tuple[int, ScenarioKeys, str, float]] = {}
    submitted = 0
    completed = 0
    ok_count = 0
    aborted_count = 0
    deadline = time.monotonic() + duration_s

    def publish_progress() -> None:
        set_scenario_progress(
            "stress_mixed_workload",
            {
                "duration_s": duration_s,
                "submitted": submitted,
                "completed": completed,
                "inflight": len(future_info),
                "ok": ok_count,
                "aborted": aborted_count,
                "oldest_inflight_age_s": max(
                    (time.monotonic() - submitted_at for _, _, _, submitted_at in future_info.values()),
                    default=0.0,
                ),
                "oldest_inflight_label": next(
                    (
                        label
                        for _, _, label, submitted_at in sorted(
                            future_info.values(), key=lambda item: item[3]
                        )[:1]
                    ),
                    "",
                ),
            },
        )

    def submit_one(pool: concurrent.futures.ThreadPoolExecutor) -> None:
        nonlocal submitted
        group_id = submitted % key_groups
        keys = group_keys[group_id]
        hot_key, tail_key, guard_key, audit_key, result_key = keys
        controlled_abort = submitted % 13 == 7
        label = f"stress-{submitted}-g{group_id}"
        future = pool.submit(
            run_workflow,
            params(
                label,
                hot_key,
                tail_key,
                guard_key,
                audit_key,
                result_key,
                guard_delta=0 if controlled_abort else 1,
                guard_abort_threshold="1" if controlled_abort else "",
            ),
            "repair_correctness",
            gateway_url,
            request_timeout,
        )
        future_info[future] = (group_id, keys, label, time.monotonic())
        submitted += 1
        publish_progress()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            while time.monotonic() < deadline and len(future_info) < concurrency:
                submit_one(pool)

            while future_info:
                wait_timeout = 0.2 if time.monotonic() < deadline else None
                done, _ = concurrent.futures.wait(
                    future_info,
                    timeout=wait_timeout,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if not done:
                    publish_progress()
                    continue
                for future in done:
                    group_id, keys, label, _submitted_at = future_info.pop(future)
                    try:
                        response = future.result()
                    except Exception as exc:
                        response = {
                            "status": "client_error",
                            "error": repr(exc),
                            "transaction_id": "",
                        }
                    response["_stress_group"] = group_id
                    response["_stress_label"] = label
                    responses.append(response)
                    completed += 1
                    if response.get("status") == "ok":
                        ok_count += 1
                    elif response.get("status") == "aborted":
                        aborted_count += 1
                    publish_progress()
                    assert_deployed_key(response, keys)

                while time.monotonic() < deadline and len(future_info) < concurrency:
                    submit_one(pool)
    finally:
        set_scenario_progress("stress_mixed_workload", None)

    groups = []
    total_expected = 0
    total_actual = 0
    for group_id, keys in enumerate(group_keys):
        hot_key, tail_key, guard_key, audit_key, result_key = keys
        expected = sum(
            1
            for response in responses
            if response.get("_stress_group") == group_id and response.get("status") == "ok"
        )
        actual = actual_hot(hot_key)
        total_expected += expected
        total_actual += actual
        groups.append({
            "group_id": group_id,
            "hot_key": hot_key,
            "tail_key": tail_key,
            "guard_key": guard_key,
            "audit_key": audit_key,
            "result_key": result_key,
            "expected_hot": expected,
            "actual_hot": actual,
        })

    result = ScenarioResult(
        name="stress_mixed_workload",
        responses=responses,
        expected_hot=total_expected,
        actual_hot=total_actual,
        hot_key="stress_total",
        min_aborts=1,
        min_successes=min(concurrency, max(1, submitted)),
        details={
            "duration_s": duration_s,
            "concurrency": concurrency,
            "key_groups": key_groups,
            "submitted": submitted,
            "groups": groups,
        },
    )
    result.assert_valid()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run end-to-end repair correctness scenarios through Gateway.")
    parser.add_argument("--gateway-url", default=default_gateway_url())
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "latest_results.json"))
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--stress-concurrency", type=int, default=32)
    parser.add_argument("--duration-s", type=float, default=120.0)
    parser.add_argument("--stress-key-groups", type=int, default=4)
    parser.add_argument("--request-timeout", type=float, default=60.0)
    parser.add_argument("--stagger-s", type=float, default=0.1)
    args = parser.parse_args()
    output_path = Path(args.output)

    results: List[ScenarioResult] = []
    failed = False
    error_info: Dict[str, Any] | None = None
    failed_scenarios: List[Dict[str, Any]] = []
    write_progress(output_path, args.gateway_url, results, running={"state": "starting"})
    append_runner_log("", f"suite start: gateway={args.gateway_url}, output={output_path}")
    emit_console(f"repair_correctness suite start")
    emit_console(f"run_id={LOG_RUN_ID}")
    emit_console(f"gateway={args.gateway_url}")
    emit_console(f"results={output_path}")
    scenarios = [
        ("sequential_ryw", lambda: sequential_ryw(args.gateway_url, args.request_timeout)),
        ("optimistic_chain", lambda: optimistic_chain(args.gateway_url, args.concurrency, args.request_timeout)),
        ("pessimistic_fallback", lambda: pessimistic_fallback(args.gateway_url, args.stagger_s, args.request_timeout)),
        ("cascaded_pessimistic_retry", lambda: cascaded_pessimistic_retry(args.gateway_url, args.stagger_s, args.request_timeout)),
    ]
    if args.duration_s > 0:
        scenarios.append(
            (
                "stress_mixed_workload",
                lambda: stress_mixed_workload(
                    args.gateway_url,
                    args.stress_concurrency,
                    args.duration_s,
                    args.stress_key_groups,
                    args.request_timeout,
                ),
            )
        )

    total_scenarios = len(scenarios)

    for index, (scenario_name, scenario_runner) in enumerate(scenarios, start=1):
        set_active_experiment(scenario_name)
        scenario_started_at = time.time()
        heartbeat_count = 0
        banner = scenario_banner(index, total_scenarios, scenario_name)
        append_runner_log(scenario_name, f"scenario start: {scenario_name}")
        if scenario_name == "stress_mixed_workload":
            emit_console(
                f"{banner} START duration={args.duration_s:.1f}s "
                f"concurrency={args.stress_concurrency} key_groups={args.stress_key_groups}"
            )
        else:
            emit_console(f"{banner} START")
        write_scenario_status(
            scenario_name,
            {
                "scenario": scenario_name,
                "run_id": LOG_RUN_ID,
                "state": "running",
                "started_at": now_iso(),
                "elapsed_s": 0.0,
                "heartbeat_count": heartbeat_count,
            },
        )
        write_progress(
            output_path,
            args.gateway_url,
            results,
            running={
                "scenario": scenario_name,
                "state": "running",
                "started_at": now_iso(),
                "elapsed_s": 0.0,
                "heartbeat_count": heartbeat_count,
            },
        )
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(scenario_runner)
                while True:
                    try:
                        result = future.result(timeout=1.0)
                        break
                    except concurrent.futures.TimeoutError:
                        elapsed_s = round(time.time() - scenario_started_at, 3)
                        heartbeat_count += 1
                        # 每 5 秒落盘一次心跳，避免在卡住时没有任何可见进展。
                        if heartbeat_count % 5 == 0:
                            append_runner_log(
                                scenario_name,
                                f"scenario heartbeat: {scenario_name}, elapsed_s={elapsed_s}",
                            )
                            progress = get_scenario_progress(scenario_name)
                            if scenario_name == "stress_mixed_workload" and progress:
                                emit_console(
                                    f"{banner} RUNNING elapsed={elapsed_s:.1f}s/{progress.get('duration_s', args.duration_s):.1f}s "
                                    f"heartbeats={heartbeat_count} inflight={progress.get('inflight', 0)} "
                                    f"submitted={progress.get('submitted', 0)} completed={progress.get('completed', 0)} "
                                    f"ok={progress.get('ok', 0)} aborted={progress.get('aborted', 0)} "
                                    f"oldest_inflight_age={progress.get('oldest_inflight_age_s', 0.0):.1f}s "
                                    f"oldest_inflight_label={progress.get('oldest_inflight_label', '')}"
                                )
                            else:
                                emit_console(f"{banner} RUNNING elapsed={elapsed_s:.1f}s heartbeats={heartbeat_count}")
                            write_scenario_status(
                                scenario_name,
                                {
                                    "scenario": scenario_name,
                                    "run_id": LOG_RUN_ID,
                                    "state": "running",
                                    "started_at": now_iso(),
                                    "elapsed_s": elapsed_s,
                                    "heartbeat_count": heartbeat_count,
                                },
                            )
                            write_progress(
                                output_path,
                                args.gateway_url,
                                results,
                                running={
                                    "scenario": scenario_name,
                                    "state": "running",
                                    "elapsed_s": elapsed_s,
                                    "heartbeat_count": heartbeat_count,
                                },
                            )
                        continue
            results.append(result)
            elapsed_s = round(time.time() - scenario_started_at, 3)
            append_runner_log(
                scenario_name,
                f"scenario success: {scenario_name}, elapsed_s={elapsed_s}, "
                f"successes={result.successes}, aborts={result.aborts}",
            )
            emit_console(
                f"{banner} OK elapsed={elapsed_s:.1f}s "
                f"successes={result.successes} aborts={result.aborts} "
                f"expected_hot={result.expected_hot} actual_hot={result.actual_hot}"
            )
            write_scenario_status(
                scenario_name,
                {
                    "scenario": scenario_name,
                    "run_id": LOG_RUN_ID,
                    "state": "succeeded",
                    "elapsed_s": elapsed_s,
                    "heartbeat_count": heartbeat_count,
                    "result": result.to_json(),
                },
            )
            write_progress(output_path, args.gateway_url, results, running={"state": "idle"})
        except Exception as exc:  # keep payload for post-mortem even on assertion failures
            failed = True
            elapsed_s = round(time.time() - scenario_started_at, 3)
            error_info = {
                "failed_scenario": scenario_name,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "elapsed_s": elapsed_s,
                "heartbeat_count": heartbeat_count,
            }
            failed_scenarios.append(error_info)
            append_runner_log(
                scenario_name,
                f"scenario failed: {scenario_name}, elapsed_s={elapsed_s}, error={type(exc).__name__}: {exc}",
            )
            emit_console(
                f"{banner} FAIL elapsed={elapsed_s:.1f}s "
                f"{type(exc).__name__}: {exc}"
            )
            write_scenario_status(
                scenario_name,
                {
                    "scenario": scenario_name,
                    "run_id": LOG_RUN_ID,
                    "state": "failed",
                    "elapsed_s": elapsed_s,
                    "heartbeat_count": heartbeat_count,
                    "error": error_info,
                },
            )
            write_progress(
                output_path,
                args.gateway_url,
                results,
                running={"state": "failed", "failed_scenarios": failed_scenarios},
                error=error_info,
            )
            append_runner_log(
                scenario_name,
                f"scenario failed but suite will continue: {scenario_name}",
            )
            continue

    set_active_experiment("")
    write_progress(
        output_path,
        args.gateway_url,
        results,
        running={"state": "done", "failed_scenarios": failed_scenarios},
        error=error_info,
    )
    append_runner_log(
        "",
        f"suite end: failed={failed}, results={len(results)}, failed_scenarios={len(failed_scenarios)}",
    )
    emit_console(
        f"suite end failed={failed} succeeded={len(results)} failed_scenarios={len(failed_scenarios)}"
    )
    print(f"Wrote {output_path}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
