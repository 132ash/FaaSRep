CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
# aws configure set aws_access_key_id FAASNAPDYNAMODB && aws configure set aws_secret_access_key FAASNAPDYNAMODBKEY && aws configure set default.region us-west-2


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
# install python packages
# pip3 install -r requirements.txt
# install redis
# # docker pull redis

# Stop and remove the container named redis if it exists
# if [ "$(docker ps -aq -f name=redis)" ]; then
#     docker stop redis
#     docker rm redis
# fi
# docker run -itd -p 6379:6379 --name redis redis

echo "Docker running on worker. Initializing basic images"
# docker build --no-cache -t workflow_sub $CURRENT_SH_DIR/workflow_sub
docker build --no-cache -t workflow_base $CURRENT_SH_DIR/../src/container

# Define available workflows and their initialization scripts
declare -A WORKFLOWS
WORKFLOWS=(
    ["microbenchmark"]="$CURRENT_SH_DIR/init/micro_benchmark/gen_image.sh"
    ['travel_reservation']="$CURRENT_SH_DIR/init/travel_reservation/gen_image.sh"
    ['banking_system']="$CURRENT_SH_DIR/init/banking_system/gen_image.sh"
    ['social_network']="$CURRENT_SH_DIR/init/social_network/gen_image.sh"
)

# Read workflow name from argument
WORKFLOW_NAME="$1"

if [ -n "$WORKFLOW_NAME" ] && [ -n "${WORKFLOWS[$WORKFLOW_NAME]}" ]; then
    echo "Initializing workflow: $WORKFLOW_NAME"
    bash "${WORKFLOWS[$WORKFLOW_NAME]}"
else
    echo "No specific workflow provided or workflow not found. Initializing all workflows."
    for wf in "${!WORKFLOWS[@]}"; do
        echo "Initializing workflow: $wf"
        bash "${WORKFLOWS[$wf]}"
    done
fi