from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import boto3
import requests


ROOT_DIR = Path(__file__).resolve().parents[3]
DEFAULT_TIMEOUT = float(os.environ.get("FAASREP_HTTP_TIMEOUT", "5"))


def load_config():
    import sys

    config_dir = ROOT_DIR / "config"
    if str(config_dir) not in sys.path:
        sys.path.insert(0, str(config_dir))
    import config

    return config


def default_gateway_url() -> str:
    config = load_config()
    return os.environ.get("GATEWAY_URL", f"http://{config.GATEWAY_ADDR}")


def dynamodb_resource(endpoint_url: Optional[str] = None):
    config = load_config()
    return boto3.resource(
        "dynamodb",
        endpoint_url=endpoint_url or os.environ.get("DYNAMODB_URL", config.DYNAMODB_URL),
        aws_secret_access_key=config.DYNAMODB_ACCESS_KEY,
        aws_access_key_id=config.DYNAMODB_KEY_ID,
        region_name=config.DYNAMODB_AREA,
    )


def startup_version() -> str:
    return datetime(2025, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")


def put_data_items(items: Dict[str, str], endpoint_url: Optional[str] = None) -> None:
    table = dynamodb_resource(endpoint_url).Table("data")
    version = startup_version()
    with table.batch_writer(overwrite_by_pkeys=["key"]) as batch:
        for key, value in items.items():
            batch.put_item(Item={"key": key, "value": str(value), "version": version})


def get_data_item(key: str, endpoint_url: Optional[str] = None) -> Dict[str, Any]:
    table = dynamodb_resource(endpoint_url).Table("data")
    response = table.get_item(Key={"key": key}, ConsistentRead=True)
    if "Item" not in response:
        raise AssertionError(f"DynamoDB data item {key!r} is missing")
    return response["Item"]


def run_workflow(
    parameters: Dict[str, Any],
    workflow: str = "repair_correctness",
    gateway_url: Optional[str] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    url = f"{gateway_url or default_gateway_url()}/run"
    response = requests.post(
        url,
        json={"workflow": workflow, "parameters": parameters},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


@dataclass
class ScenarioResult:
    name: str
    responses: List[Dict[str, Any]]
    expected_hot: int
    actual_hot: int
    hot_key: str = "rc_hot"
    tail_key: str = "rc_tail"
    guard_key: str = "rc_guard"
    audit_key: str = "rc_audit"
    result_key: str = "rc_result"
    min_aborts: int = 0
    min_successes: int = 0
    min_pessimistic_successes: int = 0
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def successes(self) -> int:
        return sum(1 for response in self.responses if response.get("status") == "ok")

    @property
    def aborts(self) -> int:
        return sum(1 for response in self.responses if response.get("status") == "aborted")

    @property
    def pessimistic_successes(self) -> int:
        return sum(
            1
            for response in self.responses
            if response.get("status") == "ok" and response.get("rounds") == 3
        )

    def assert_valid(self) -> None:
        for group in self.details.get("groups", []):
            if group["actual_hot"] != group["expected_hot"]:
                raise AssertionError(
                    f"{self.name}: group {group['group_id']} {group['hot_key']}="
                    f"{group['actual_hot']}, expected={group['expected_hot']}"
                )
        if self.actual_hot != self.expected_hot:
            raise AssertionError(
                f"{self.name}: {self.hot_key}={self.actual_hot}, expected={self.expected_hot}, "
                f"responses={json.dumps(self.responses, sort_keys=True)}"
            )
        if self.aborts < self.min_aborts:
            raise AssertionError(
                f"{self.name}: aborts={self.aborts}, expected at least {self.min_aborts}"
            )
        if self.successes < self.min_successes:
            raise AssertionError(
                f"{self.name}: successes={self.successes}, expected at least {self.min_successes}, "
                f"responses={json.dumps(self.responses, sort_keys=True)}"
            )
        if self.pessimistic_successes < self.min_pessimistic_successes:
            raise AssertionError(
                f"{self.name}: pessimistic_successes={self.pessimistic_successes}, "
                f"expected at least {self.min_pessimistic_successes}, "
                f"responses={json.dumps(self.responses, sort_keys=True)}"
            )

    def to_json(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "responses": self.responses,
            "successes": self.successes,
            "aborts": self.aborts,
            "expected_hot": self.expected_hot,
            "actual_hot": self.actual_hot,
            "hot_key": self.hot_key,
            "tail_key": self.tail_key,
            "guard_key": self.guard_key,
            "audit_key": self.audit_key,
            "result_key": self.result_key,
            "min_aborts": self.min_aborts,
            "min_successes": self.min_successes,
            "pessimistic_successes": self.pessimistic_successes,
            "min_pessimistic_successes": self.min_pessimistic_successes,
            "details": self.details,
        }


def safe_key_fragment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def wait_for_gateway(gateway_url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            response = requests.get(gateway_url.rstrip("/") + "/healthz", timeout=1)
            response.raise_for_status()
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)
            continue
        return
    if last_error:
        raise RuntimeError(f"Gateway did not become reachable: {last_error}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
