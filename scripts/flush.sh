#!/bin/bash
set -euo pipefail

CURRENT_SH_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR=$(readlink -f "$CURRENT_SH_DIR/..")

usage() {
    cat <<'EOF'
Usage:
  bash scripts/flush.sh [workflow ...]

Workflows:
  app              Expand to travel_reservation banking_system social_network
  all              Expand to app + microbenchmark
  microbenchmark
  travel_reservation
  banking_system
  social_network

If no workflow is provided, flush.sh reloads workflows listed in
config.WORKFLOW_YAML_ADDR. Components that are not running on this node are
skipped automatically.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

python3 - "$ROOT_DIR" "$@" <<'PY'
import importlib.util
import random
import socket
import string
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(sys.argv[1])
ARGS = sys.argv[2:]
SCRIPTS_DIR = ROOT_DIR / "scripts"


def log(message):
    print(f"[flush] {message}", flush=True)


def can_connect(host, port, timeout=0.35):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def load_config():
    config_path = ROOT_DIR / "config" / "config.py"
    spec = importlib.util.spec_from_file_location("faasnap_config", config_path)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return config


config = load_config()


def expand_workflows(workflows):
    if not workflows:
        configured = getattr(config, "WORKFLOW_YAML_ADDR", {})
        workflows = list(configured.keys())
        if workflows:
            log(f"No workflow argument provided; using config.WORKFLOW_YAML_ADDR: {' '.join(workflows)}")
        else:
            workflows = ["app"]
            log("No workflow argument provided and WORKFLOW_YAML_ADDR is empty; using app.")

    expanded = []
    for workflow in workflows:
        if workflow == "app":
            expanded.extend(["travel_reservation", "banking_system", "social_network"])
        elif workflow == "all":
            expanded.extend(["travel_reservation", "banking_system", "social_network", "microbenchmark"])
        else:
            expanded.append(workflow)

    deduped = []
    seen = set()
    for workflow in expanded:
        if workflow not in seen:
            deduped.append(workflow)
            seen.add(workflow)
    return deduped


WORKFLOW_SETUP = {
    "travel_reservation": SCRIPTS_DIR / "init" / "travel_reservation" / "DB_setup.py",
    "banking_system": SCRIPTS_DIR / "init" / "banking_system" / "DB_setup.py",
    "social_network": SCRIPTS_DIR / "init" / "social_network" / "DB_setup.py",
    "microbenchmark": SCRIPTS_DIR / "init" / "micro_benchmark" / "DB_setup.py",
}


def connect_local_couchdb():
    if not can_connect("127.0.0.1", 5984):
        log("CouchDB is not listening on localhost:5984; skipping CouchDB flush.")
        return None

    try:
        import couchdb
    except ImportError:
        log("Python package couchdb is not installed; skipping CouchDB flush.")
        return None

    return couchdb.Server("http://faasnap:faasnap@127.0.0.1:5984")


def reset_couchdb_runtime_dbs(couch):
    if couch is None:
        return

    runtime_dbs = ["workflow_latency", "results", "log"]
    for db_name in runtime_dbs:
        if db_name in couch:
            couch.delete(db_name)
            log(f"CouchDB database {db_name} deleted.")
        couch.create(db_name)
        log(f"CouchDB database {db_name} created.")


def ensure_common_db(couch):
    if couch is None:
        return

    worker_info_path = ROOT_DIR / "config" / "worker_info.yaml"
    if not worker_info_path.exists():
        log(f"{worker_info_path} does not exist; skipping CouchDB common metadata check.")
        return

    try:
        import yaml
    except ImportError:
        log("Python package pyyaml is not installed; skipping CouchDB common metadata check.")
        return

    common_missing = "common" not in couch
    needs_addrs = common_missing
    if common_missing:
        couch.create("common")
    else:
        common_db = couch["common"]
        needs_addrs = not any("addrs" in common_db[doc_id] for doc_id in common_db)

    if needs_addrs:
        nodes = yaml.load(open(worker_info_path, encoding="utf-8"), Loader=yaml.FullLoader)["nodes"]
        couch["common"].save({"addrs": list(nodes)})
        log("CouchDB common metadata saved.")
    else:
        log("CouchDB common metadata exists.")


def ensure_workflow_metadata(couch, workflows):
    if couch is None:
        return

    metadata_missing = []
    for workflow in workflows:
        required_dbs = [
            f"{workflow}_function_info",
            f"{workflow}_workflow_metadata",
        ]
        if any(db_name not in couch for db_name in required_dbs):
            metadata_missing.append(workflow)

    if not metadata_missing:
        log("CouchDB workflow metadata exists.")
        return

    initialize_script = ROOT_DIR / "src" / "initializer" / "initialize.py"
    if not initialize_script.exists():
        log(f"{initialize_script} does not exist; cannot rebuild workflow metadata.")
        return

    log(f"Rebuilding CouchDB workflow metadata for: {' '.join(metadata_missing)}")
    subprocess.run([sys.executable, str(initialize_script), *metadata_missing], cwd=str(initialize_script.parent), check=True)


def random_text(size):
    return "".join(random.choices(string.ascii_letters + string.digits, k=size))


def recreate_data_table():
    if not can_connect("127.0.0.1", 4567):
        log("DynamoDB/ScyllaDB Alternator is not listening on localhost:4567; skipping data table flush.")
        return False

    try:
        import boto3
    except ImportError:
        log("Python package boto3 is not installed; skipping data table flush.")
        return False

    dynamo = boto3.resource(
        "dynamodb",
        endpoint_url="http://127.0.0.1:4567",
        aws_access_key_id=getattr(config, "DYNAMODB_KEY_ID", "FAASNAPDYNAMODB"),
        aws_secret_access_key=getattr(config, "DYNAMODB_ACCESS_KEY", "FAASNAPDYNAMODBKEY"),
        region_name=getattr(config, "DYNAMODB_AREA", "us-west-2"),
    )

    try:
        table = dynamo.Table("data")
        table.delete()
        table.meta.client.get_waiter("table_not_exists").wait(TableName="data")
        log("DynamoDB table data deleted.")
    except Exception as exc:
        log(f"DynamoDB table data did not exist or could not be deleted: {exc}")

    table = dynamo.create_table(
        TableName="data",
        KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 100, "WriteCapacityUnits": 100},
    )
    table.meta.client.get_waiter("table_exists").wait(TableName="data")

    startup_version = datetime(2025, 1, 1).strftime("%Y-%m-%d %H:%M:%S.%f")
    table.put_item(
        Item={
            "key": "test_value",
            "value": random_text(4 * 1024),
            "version": startup_version,
        }
    )
    log("DynamoDB table data recreated.")
    return True


def run_dataset_setup(workflows):
    if not workflows:
        log("No workflow dataset selected for reload.")
        return

    for workflow in workflows:
        setup_script = WORKFLOW_SETUP.get(workflow)
        if setup_script is None:
            log(f"Unknown workflow {workflow}; skipping dataset reload.")
            continue
        if not setup_script.exists():
            log(f"Dataset setup script {setup_script} does not exist; skipping {workflow}.")
            continue

        log(f"Reloading dataset for {workflow}.")
        subprocess.run([sys.executable, str(setup_script)], cwd=str(ROOT_DIR), check=True)


def flush_redis_instance(port, db, name):
    if not can_connect("127.0.0.1", port):
        log(f"{name} Redis is not listening on localhost:{port}; skipping.")
        return

    try:
        import redis
    except ImportError:
        log("Python package redis is not installed; skipping Redis flush.")
        return

    client = redis.StrictRedis(host="127.0.0.1", port=port, db=db)
    try:
        client.flushall(asynchronous=True)
        log(f"{name} Redis on port {port} flushed.")
    except TypeError:
        client.flushall()
        log(f"{name} Redis on port {port} flushed.")


def flush_redis():
    redis_port = int(getattr(config, "REDIS_PORT", 6379))
    shadow_db = int(getattr(config, "SHADOWTABLE_DB", 0))
    cache_port = int(getattr(config, "REDIS_CACHE_PORT", 6380))
    cache_db = int(getattr(config, "CACHE_DB", 1))

    flush_redis_instance(redis_port, shadow_db, "shadow-table")
    flush_redis_instance(cache_port, cache_db, "cache")


def main():
    workflows = expand_workflows(ARGS)

    couch = connect_local_couchdb()
    reset_couchdb_runtime_dbs(couch)
    ensure_common_db(couch)
    ensure_workflow_metadata(couch, workflows)
    if recreate_data_table():
        run_dataset_setup(workflows)

    flush_redis()
    log("Flush completed.")


if __name__ == "__main__":
    main()
PY
