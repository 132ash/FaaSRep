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
if [ "$(docker ps -aq -f name=redis)" ]; then
    docker stop redis
    docker rm redis
fi
docker run -itd -p 6379:6379 --name redis redis
# docker run -itd -p 3000:3000 --name redis_shadow_table redis_shadow_table

# aws configure set aws_access_key_id FAASNAPDYNAMODB && aws configure set aws_secret_access_key FAASNAPDYNAMODBKEY && aws configure set default.region us-west-2


docker build --no-cache -t workflow_sub ./workflow_sub
docker build --no-cache -t workflow_base ../src/container
docker build --no-cache -t micro_func ./microbenchmark_func
# ../benchmark/testflow/create_image.sh
# ../benchmark/testflow/create_image.sh
../benchmark/textseq/create_image.sh