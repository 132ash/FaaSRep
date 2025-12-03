#!/bin/bash
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
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


# Stop and remove containers for amazon/dynamodb-local:latest and couchdb
# 1. 清理旧的 ScyllaDB 容器
docker stop scylla 2>/dev/null || true
docker rm scylla 2>/dev/null || true

# 2. 清理旧的 DynamoDB Local 容器
docker stop $(docker ps -q --filter ancestor=amazon/dynamodb-local:latest) 2>/dev/null || true
docker rm $(docker ps -aq --filter ancestor=amazon/dynamodb-local:latest) 2>/dev/null || true
# docker stop couchdb
# docker rm couchdb
# docker stop redis
# docker rm redis
# # install and initialize DynamoDB
# # docker pull amazon/dynamodb-local:latest
# # aws configure set aws_access_key_id FAASNAPDYNAMODB && aws configure set aws_secret_access_key FAASNAPDYNAMODBKEY && aws configure set default.region us-west-2
echo "Starting ScyllaDB..."
docker run --name scylla -d -p 4567:8000 scylladb/scylla \
    --alternator-port 8000 \
    --alternator-write-isolation always \
    --smp 20 --memory 20G

# 4. 等待 ScyllaDB 启动完成
# ScyllaDB 启动比 DynamoDB Local 慢，必须等待端口可连接
echo "Waiting for ScyllaDB to initialize (this may take a few seconds)..."
until python -c "import urllib.request; urllib.request.urlopen('http://localhost:4567')" > /dev/null 2>&1; do
    sleep 2
    echo -n "."
done
echo -e "\nScyllaDB started successfully."
# Default region name: us-west-2


# ... (前面的内容保持不变) ...

# install and initialize couchdb
# docker pull couchdb
#docker run -itd -p 5984:5984 -e COUCHDB_USER=faasnap -e COUCHDB_PASSWORD=faasnap --name couchdb couchdb
# pip install -r requirements.txt

aws dynamodb create-table \
    --table-name data \
    --attribute-definitions AttributeName=key,AttributeType=S \
    --key-schema AttributeName=key,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1000,WriteCapacityUnits=1000 \
    --endpoint-url http://localhost:4567

# 2. 创建全局 shadow_table (使用复合主键)
aws dynamodb create-table \
    --table-name shadow_table \
    --attribute-definitions AttributeName=txid,AttributeType=S AttributeName=key,AttributeType=S \
    --key-schema AttributeName=txid,KeyType=HASH AttributeName=key,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1000,WriteCapacityUnits=1000 \
    --endpoint-url http://localhost:4567

# 3. 创建全局 lock_shadow_table (使用复合主键)
aws dynamodb create-table \
    --table-name lock_shadow_table \
    --attribute-definitions AttributeName=txid,AttributeType=S AttributeName=key,AttributeType=S \
    --key-schema AttributeName=txid,KeyType=HASH AttributeName=key,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=1000,WriteCapacityUnits=1000 \
    --endpoint-url http://localhost:4567


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

# Read workflow name from argument
WORKFLOW_NAME="$1"

if [ "$WORKFLOW_NAME" == "app" ]; then
    echo "Initializing actual application workflows: travel_reservation, banking_system, social_network"
    
    ACTUAL_WORKFLOWS=("travel_reservation" "banking_system" "social_network")
    
    # 运行每个工作流的 init.sh 脚本
    for wf in "${ACTUAL_WORKFLOWS[@]}"; do
        echo "Running init script for: $wf"
        bash "${WORKFLOWS_INIT[$wf]}"
    done
    
    # 调用 initialize.py 来初始化这三个工作流
    echo "Running initialize.py for: ${ACTUAL_WORKFLOWS[@]}"
    python $CURRENT_SH_DIR/../src/initializer/initialize.py "${ACTUAL_WORKFLOWS[@]}"

elif [ -n "$WORKFLOW_NAME" ] && [ -n "${WORKFLOWS_INIT[$WORKFLOW_NAME]}" ]; then
    echo "Initializing workflow: $WORKFLOW_NAME"
    bash "${WORKFLOWS_INIT[$WORKFLOW_NAME]}"
    
    # 根据工作流名称决定传递给 initialize.py 的参数
    if [ "$WORKFLOW_NAME" == "microbenchmark" ]; then
        echo "Initializing microbenchmark workflows: ${MICROBENCHMARK_WORKFLOWS[@]}"
        python $CURRENT_SH_DIR/../src/initializer/initialize.py "${MICROBENCHMARK_WORKFLOWS[@]}"
    else
        echo "Initializing single workflow: $WORKFLOW_NAME"
        python $CURRENT_SH_DIR/../src/initializer/initialize.py "$WORKFLOW_NAME"
    fi
else
    echo "No specific workflow provided or workflow not found. Initializing all workflows."
    
    # 执行所有工作流的初始化脚本
    for wf in "${!WORKFLOWS_INIT[@]}"; do
        echo "Initializing workflow: $wf"
        bash "${WORKFLOWS_INIT[$wf]}"
    done

    # 构建完整的工作流列表
    ALL_WORKFLOWS=()
    
    # 添加 microbenchmark 工作流
    ALL_WORKFLOWS+=("${MICROBENCHMARK_WORKFLOWS[@]}")
    
    # 添加除了 microbenchmark 之外的其他工作流
    for wf in "${!WORKFLOWS_INIT[@]}"; do
        if [ "$wf" != "microbenchmark" ]; then
            ALL_WORKFLOWS+=("$wf")
        fi
    done
    
    echo "Initializing all workflows: ${ALL_WORKFLOWS[@]}"
    python $CURRENT_SH_DIR/../src/initializer/initialize.py "${ALL_WORKFLOWS[@]}"
fi