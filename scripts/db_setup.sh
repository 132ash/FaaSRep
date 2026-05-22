#!/bin/bash
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
ROOT_DIR=$(readlink -f "$CURRENT_SH_DIR/..")
# install docker
# apt-get update
# apt-get install -y \
#     ca-certificates \
#     curl \
#     gnupg \
#     lsb-release
# curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
# echo \
#   "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
#   $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
# apt-get update
# apt-get install -y docker-ce docker-ce-cli containerd.io
# apt-get install wondershaper

docker stop scylla 2>/dev/null || true
docker rm scylla 2>/dev/null || true
# Stop and remove containers for amazon/dynamodb-local:latest and couchdb
docker stop $(docker ps -q --filter ancestor=amazon/dynamodb-local:latest)
docker rm $(docker ps -aq --filter ancestor=amazon/dynamodb-local:latest)
docker stop couchdb
docker rm couchdb
# docker stop redis
# docker rm redis
# # install and initialize DynamoDB
# # docker pull amazon/dynamodb-local:latest
aws configure set aws_access_key_id FAASNAPDYNAMODB && aws configure set aws_secret_access_key FAASNAPDYNAMODBKEY && aws configure set default.region us-west-2
echo "Starting ScyllaDB..."
docker run --name scylla -d -p 4567:8000 scylladb/scylla \
    --alternator-port 8000 \
    --alternator-write-isolation always \
    --smp 20 --memory 20G
# Default region name: us-west-2
echo "Waiting for ScyllaDB to initialize (this may take a few seconds)..."
until python -c "import urllib.request; urllib.request.urlopen('http://localhost:4567')" > /dev/null 2>&1; do
    sleep 2
    echo -n "."
done
echo -e "\nScyllaDB started successfully."

# ... (前面的内容保持不变) ...

# install and initialize couchdb
# docker pull couchdb
docker run -itd -p 5984:5984 -e COUCHDB_USER=faasnap -e COUCHDB_PASSWORD=faasnap --name couchdb couchdb
echo "Waiting for CouchDB to initialize..."
until python -c "import urllib.request; urllib.request.urlopen('http://localhost:5984')" > /dev/null 2>&1; do
    sleep 2
    echo -n "."
done
echo -e "\nCouchDB started successfully."
# pip install -r requirements.txt
python $CURRENT_SH_DIR/db_starter.py

declare -A WORKFLOWS_INIT
# ... (后面的内容保持不变) ...

# init: generate workflow yaml and node assign (if not exists), then build DB.
WORKFLOWS_INIT=(
    ["microbenchmark"]="$CURRENT_SH_DIR/init/micro_benchmark/init.sh"
    ['travel_reservation']="$CURRENT_SH_DIR/init/travel_reservation/init.sh"
    ['banking_system']="$CURRENT_SH_DIR/init/banking_system/init.sh"
    ['social_network']="$CURRENT_SH_DIR/init/social_network/init.sh"
)

# List of microbenchmark workflows from c2 to w16
MICROBENCHMARK_WORKFLOWS=(c2 c4 c8 c16 w2 w4 w6 w8)

CONFIGURED_WORKFLOWS_OUTPUT=$(python - "$ROOT_DIR" <<'PY'
import importlib.util
from pathlib import Path
import sys

root = Path(sys.argv[1])
config_path = root / "config" / "config.py"
spec = importlib.util.spec_from_file_location("faasnap_config", config_path)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
print(" ".join(config.WORKFLOW_YAML_ADDR.keys()))
PY
)
if [ $? -ne 0 ]; then
    echo "Error: failed to read config.WORKFLOW_YAML_ADDR from $ROOT_DIR/config/config.py." >&2
    exit 1
fi
CONFIGURED_WORKFLOWS=($CONFIGURED_WORKFLOWS_OUTPUT)

if [ "${#CONFIGURED_WORKFLOWS[@]}" -eq 0 ]; then
    echo "Error: config.WORKFLOW_YAML_ADDR is empty. No workflow metadata can be initialized." >&2
    exit 1
fi

contains_workflow() {
    local target="$1"
    local wf
    for wf in "${CONFIGURED_WORKFLOWS[@]}"; do
        if [ "$wf" == "$target" ]; then
            return 0
        fi
    done
    return 1
}

filter_configured_workflows() {
    local wf
    for wf in "$@"; do
        if contains_workflow "$wf"; then
            echo "$wf"
        else
            echo "Warning: workflow '$wf' is not in config.WORKFLOW_YAML_ADDR; skip metadata initialization." >&2
        fi
    done
}

run_dataset_init_for_workflows() {
    local initialized_groups=()
    local wf group already
    for wf in "$@"; do
        group="$wf"
        for micro_wf in "${MICROBENCHMARK_WORKFLOWS[@]}"; do
            if [ "$wf" == "$micro_wf" ]; then
                group="microbenchmark"
                break
            fi
        done

        already=false
        for existing in "${initialized_groups[@]}"; do
            if [ "$existing" == "$group" ]; then
                already=true
                break
            fi
        done
        if [ "$already" == true ]; then
            continue
        fi

        if [ -n "${WORKFLOWS_INIT[$group]}" ]; then
            echo "Running init script for: $group"
            bash "${WORKFLOWS_INIT[$group]}"
            initialized_groups+=("$group")
        else
            echo "Warning: no init script for workflow group '$group'." >&2
        fi
    done
}

initialize_workflow_metadata() {
    local workflows=("$@")
    if [ "${#workflows[@]}" -eq 0 ]; then
        echo "Error: no workflow metadata to initialize. Check config.WORKFLOW_YAML_ADDR." >&2
        exit 1
    fi
    echo "Running initialize.py for: ${workflows[@]}"
    python "$CURRENT_SH_DIR/../src/initializer/initialize.py" "${workflows[@]}"
}

# Read workflow name from argument
WORKFLOW_NAME="${1:-}"

if [ "$WORKFLOW_NAME" == "app" ]; then
    echo "Initializing configured actual application workflows."
    
    ACTUAL_WORKFLOWS=($(filter_configured_workflows "travel_reservation" "banking_system" "social_network"))
    
    run_dataset_init_for_workflows "${ACTUAL_WORKFLOWS[@]}"
    
    initialize_workflow_metadata "${ACTUAL_WORKFLOWS[@]}"

elif [ -n "$WORKFLOW_NAME" ] && [ -n "${WORKFLOWS_INIT[$WORKFLOW_NAME]}" ]; then
    echo "Initializing workflow: $WORKFLOW_NAME"
    
    # 根据工作流名称决定传递给 initialize.py 的参数
    if [ "$WORKFLOW_NAME" == "microbenchmark" ]; then
        WORKFLOWS_TO_INITIALIZE=($(filter_configured_workflows "${MICROBENCHMARK_WORKFLOWS[@]}"))
    else
        WORKFLOWS_TO_INITIALIZE=($(filter_configured_workflows "$WORKFLOW_NAME"))
    fi
    run_dataset_init_for_workflows "${WORKFLOWS_TO_INITIALIZE[@]}"
    initialize_workflow_metadata "${WORKFLOWS_TO_INITIALIZE[@]}"
else
    echo "No specific workflow provided or workflow not found. Initializing workflows in config.WORKFLOW_YAML_ADDR."
    run_dataset_init_for_workflows "${CONFIGURED_WORKFLOWS[@]}"
    initialize_workflow_metadata "${CONFIGURED_WORKFLOWS[@]}"
fi
