#!/bin/bash
# filepath: /home/shao/FaaSnap/scripts/db_setup.sh

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
# docker stop $(docker ps -q --filter ancestor=amazon/dynamodb-local:latest)
# docker rm $(docker ps -aq --filter ancestor=amazon/dynamodb-local:latest)
# docker stop couchdb
# docker rm couchdb
# docker stop redis
# docker rm redis
# # install and initialize DynamoDB
# # docker pull amazon/dynamodb-local:latest
# # aws configure set aws_access_key_id FAASNAPDYNAMODB && aws configure set aws_secret_access_key FAASNAPDYNAMODBKEY && aws configure set default.region us-west-2
# docker run -d -p 4567:8000 amazon/dynamodb-local:latest
# Default region name: us-west-2

# install and initialize couchdb
# docker pull couchdb
# docker run -itd -p 5984:5984 -e COUCHDB_USER=faasnap -e COUCHDB_PASSWORD=faasnap --name couchdb couchdb
# pip install -r requirements.txt
python $CURRENT_SH_DIR/db_starter.py

# # install redis
# # docker pull redis
# docker run -itd -p 6379:6379 --name redis redis

# 定义工作流初始化脚本映射的函数
get_workflow_init_script() {
    case "$1" in
        "microbenchmark")
            echo "$CURRENT_SH_DIR/init/micro_benchmark/init.sh"
            ;;
        "travel_reservation")
            echo "$CURRENT_SH_DIR/init/travel_reservation/init.sh"
            ;;
        "banking_system")
            echo "$CURRENT_SH_DIR/init/banking_system/init.sh"
            ;;
        "social_network")
            echo "$CURRENT_SH_DIR/init/social_network/init.sh"
            ;;
        *)
            echo ""
            ;;
    esac
}

# 检查工作流是否存在
workflow_exists() {
    local workflow_script=$(get_workflow_init_script "$1")
    [ -n "$workflow_script" ]
}

# List of microbenchmark workflows, including the single-function c1 baseline.
MICROBENCHMARK_WORKFLOWS="c1 c2 c4 c8 c16 w2 w4 w6 w8"

# 所有支持的工作流列表
ALL_SUPPORTED_WORKFLOWS="microbenchmark travel_reservation banking_system social_network"

# Read workflow name from argument
WORKFLOW_NAME="$1"

if [ -n "$WORKFLOW_NAME" ] && workflow_exists "$WORKFLOW_NAME"; then
    echo "Initializing workflow: $WORKFLOW_NAME"
    INIT_SCRIPT=$(get_workflow_init_script "$WORKFLOW_NAME")
    bash "$INIT_SCRIPT"
    
    # 根据工作流名称决定传递给 initialize.py 的参数
    if [ "$WORKFLOW_NAME" == "microbenchmark" ]; then
        echo "Initializing microbenchmark workflows: $MICROBENCHMARK_WORKFLOWS"
        python $CURRENT_SH_DIR/../src/initializer/initialize.py $MICROBENCHMARK_WORKFLOWS
    else
        echo "Initializing single workflow: $WORKFLOW_NAME"
        python $CURRENT_SH_DIR/../src/initializer/initialize.py "$WORKFLOW_NAME"
    fi
else
    echo "No specific workflow provided or workflow not found. Initializing all workflows."
    
    # 执行所有工作流的初始化脚本
    for wf in $ALL_SUPPORTED_WORKFLOWS; do
        echo "Initializing workflow: $wf"
        INIT_SCRIPT=$(get_workflow_init_script "$wf")
        if [ -n "$INIT_SCRIPT" ]; then
            bash "$INIT_SCRIPT"
        fi
    done

    # 构建完整的工作流列表
    ALL_WORKFLOWS="$MICROBENCHMARK_WORKFLOWS"
    
    # 添加除了 microbenchmark 之外的其他工作流
    for wf in $ALL_SUPPORTED_WORKFLOWS; do
        if [ "$wf" != "microbenchmark" ]; then
            ALL_WORKFLOWS="$ALL_WORKFLOWS $wf"
        fi
    done
    
    echo "Initializing all workflows: $ALL_WORKFLOWS"
    python $CURRENT_SH_DIR/../src/initializer/initialize.py $ALL_WORKFLOWS
fi
