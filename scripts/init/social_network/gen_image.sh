echo "Build images for workflow social_network"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

docker build --no-cache -t social_login $SCRIPT_DIR/../../../benchmark/social_network/social_login
docker build --no-cache -t check_mailbox $SCRIPT_DIR/../../../benchmark/social_network/check_mailbox
docker build --no-cache -t send_comment $SCRIPT_DIR/../../../benchmark/social_network/send_comment
docker build --no-cache -t publish $SCRIPT_DIR/../../../benchmark/social_network/publish
docker build --no-cache -t notify_follower $SCRIPT_DIR/../../../benchmark/social_network/notify_follower
docker build --no-cache -t modify_timeline $SCRIPT_DIR/../../../benchmark/social_network/modify_timeline

