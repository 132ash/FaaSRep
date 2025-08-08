echo "Build images for workflow social_network"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

docker build --no-cache -t social_login $SCRIPT_DIR/../../../benchmark/social_network/social_login
docker build --no-cache -t comment_post_1 $SCRIPT_DIR/../../../benchmark/social_network/comment_post_1
docker build --no-cache -t comment_post_2 $SCRIPT_DIR/../../../benchmark/social_network/comment_post_2
docker build --no-cache -t comment_post_3 $SCRIPT_DIR/../../../benchmark/social_network/comment_post_3
docker build --no-cache -t comment_user $SCRIPT_DIR/../../../benchmark/social_network/comment_user
docker build --no-cache -t publish $SCRIPT_DIR/../../../benchmark/social_network/publish
docker build --no-cache -t modify_timeline $SCRIPT_DIR/../../../benchmark/social_network/modify_timeline