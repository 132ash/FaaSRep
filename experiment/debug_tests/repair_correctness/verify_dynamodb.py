from __future__ import annotations

import argparse
from pathlib import Path

from correctness_lib import get_data_item, read_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify DynamoDB state against latest repair correctness results.")
    parser.add_argument("--results", default=str(Path(__file__).resolve().parent / "latest_results.json"))
    args = parser.parse_args()

    payload = read_json(Path(args.results))
    failures = []
    for result in payload["results"]:
        for group in result.get("details", {}).get("groups", []):
            if group["actual_hot"] != group["expected_hot"]:
                failures.append(
                    f"{result['name']} group {group['group_id']}: "
                    f"{group['hot_key']}={group['actual_hot']} expected={group['expected_hot']}"
                )
        if result["actual_hot"] != result["expected_hot"]:
            failures.append(
                f"{result['name']}: actual_hot={result['actual_hot']} expected={result['expected_hot']}"
            )
        if result["aborts"] < result["min_aborts"]:
            failures.append(
                f"{result['name']}: aborts={result['aborts']} min_aborts={result['min_aborts']}"
            )
        if result["successes"] < result.get("min_successes", 0):
            failures.append(
                f"{result['name']}: successes={result['successes']} min_successes={result.get('min_successes', 0)}"
            )
        if result.get("pessimistic_successes", 0) < result.get("min_pessimistic_successes", 0):
            failures.append(
                f"{result['name']}: pessimistic_successes={result.get('pessimistic_successes', 0)} "
                f"min_pessimistic_successes={result.get('min_pessimistic_successes', 0)}"
            )

    last_result = payload["results"][-1]
    last_groups = last_result.get("details", {}).get("groups", [])
    if last_groups:
        for group in last_groups:
            live_hot = int(get_data_item(group["hot_key"])["value"])
            if live_hot != group["expected_hot"]:
                failures.append(
                    f"live DynamoDB {group['hot_key']}={live_hot} expected last scenario value={group['expected_hot']}"
                )
    else:
        last_hot_key = last_result["hot_key"]
        live_hot = int(get_data_item(last_hot_key)["value"])
        last_expected = last_result["expected_hot"]
        if live_hot != last_expected:
            failures.append(f"live DynamoDB {last_hot_key}={live_hot} expected last scenario value={last_expected}")

    if failures:
        raise SystemExit("Correctness verification failed:\n- " + "\n- ".join(failures))

    print("Correctness verification passed.")


if __name__ == "__main__":
    main()
