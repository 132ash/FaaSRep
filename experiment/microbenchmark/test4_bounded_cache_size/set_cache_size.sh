#!/bin/bash
# 用法: ./set_cache_size.sh <cache_bound_proportion>
# 例如: ./set_cache_size.sh 0.5
# 该脚本会根据输入的比例，自动计算最终的缓存大小。

# --- 内部全局配置 ---
# 1. 定义工作负载在无限制下使用的最大缓存 (单位: MB)
#    您可以根据需要修改这个基准值。
MAX_WORKLOAD_CACHE_MB=41
# 2. 定义 Redis 自身的固定开销 (单位: KB)
REDIS_OVERHEAD_KB=2048

# --- 脚本主逻辑 ---
if [ -z "$1" ]; then
    echo "错误: 请提供缓存大小的比例 (例如 0.1, 0.5, 1.0)。"
    echo "用法: $0 <cache_bound_proportion>"
    exit 1
fi

CACHE_BOUND=$1

# --- 核心计算逻辑 ---
# 计算工作负载部分的缓存大小 (KB)
WORKLOAD_PART_KB=$(echo "$MAX_WORKLOAD_CACHE_MB * $CACHE_BOUND * 1024" | bc)
# 加上 Redis 固定开销，得到最终的 maxmemory 值 (KB)
# 使用 /1 技巧确保结果为整数
FINAL_CACHE_SIZE_KB=$(echo "($WORKLOAD_PART_KB + $REDIS_OVERHEAD_KB) / 1" | bc)

# 获取当前脚本所在的目录
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))

# 配置文件路径相对于当前脚本
CONFIG_FILE_PATH="$CURRENT_SH_DIR/../../../config/redis-cache.conf"
CONTAINER_NAME="redis-cache"

echo "🔧 正在根据比例 $CACHE_BOUND 设置 Redis 缓存大小..."
echo "   - 计算公式: (${MAX_WORKLOAD_CACHE_MB}MB * ${CACHE_BOUND}) + ${REDIS_OVERHEAD_KB}KB"
echo "   - 最终缓存限制: ${FINAL_CACHE_SIZE_KB}KB"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE_PATH" ]; then
    echo "❌ 错误: Redis 配置文件未找到于 $CONFIG_FILE_PATH"
    exit 1
fi

# 使用 sed 动态修改 maxmemory 配置
sed -i "s/^maxmemory .*/maxmemory ${FINAL_CACHE_SIZE_KB}kb/" "$CONFIG_FILE_PATH"

echo "📝 配置文件已更新: $CONFIG_FILE_PATH"
cat "$CONFIG_FILE_PATH" | grep "maxmemory"

# --- 按照 worker_setup.sh 的方式重启 Redis ---
echo "🔄 正在重启 Docker 容器 '$CONTAINER_NAME'..."

# 1. 检查容器是否存在，如果存在则停止并删除
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "   - 正在停止并删除现有容器 '$CONTAINER_NAME'..."
    docker stop "$CONTAINER_NAME" > /dev/null
    docker rm "$CONTAINER_NAME" > /dev/null
fi

# 2. 重新创建容器，加载更新后的配置
echo "   - 正在创建新容器 '$CONTAINER_NAME'..."
docker run -itd \
    -p 6380:6380 \
    --name "$CONTAINER_NAME" \
    -v "$CONFIG_FILE_PATH":/usr/local/etc/redis/redis.conf \
    redis redis-server /usr/local/etc/redis/redis.conf > /dev/null

# 检查容器是否成功启动
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "✅ 容器 '$CONTAINER_NAME' 重启成功，新缓存大小已生效。"
    # 等待几秒钟确保 Redis 完全启动
    sleep 3
else
    echo "❌ 错误: 创建容器 '$CONTAINER_NAME' 失败。"
    echo "   请检查 Docker 服务状态和镜像 'redis' 是否存在。"
    exit 1
fi