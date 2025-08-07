echo "Build images for workflow social_network"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

docker build --no-cache -t social_login $SCRIPT_DIR/../../../benchmark/travel_reservation/social_login
docker build --no-cache -t check_mailbox $SCRIPT_DIR/../../../benchmark/travel_reservation/check_mailbox
docker build --no-cache -t send_comment $SCRIPT_DIR/../../../benchmark/travel_reservation/send_comment
docker build --no-cache -t publish $SCRIPT_DIR/../../../benchmark/travel_reservation/publish
docker build --no-cache -t notify_follower $SCRIPT_DIR/../../../benchmark/travel_reservation/notify_follower
docker build --no-cache -t modify_timeline $SCRIPT_DIR/../../../benchmark/travel_reservation/modify_timeline

