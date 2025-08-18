#!/bin/bash
# 示例：测试不同缓存大小限制下的系统性能
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

WORKFLOW="c4"
CLIENT_CNT=32
SYSTEM_MODE="optimistic" # 或者 "pessimistic"
ZIPF_PARAM=1 # 固定一个数据倾斜度

# --- 缓存大小配置 ---
# 定义一个基准的最大缓存使用量 (单位: MB)，例如预先测出 c4 在无限制下大约使用 41MB
MAX_CACHE_SIZE_MB=41
# 定义要测试的缓存大小比例
# CACHE_BOUNDS=(0.01 0.1 0.5 1.0) 
# fixed mem: 1.05mb
CACHE_BOUNDS=(0.01) 
# CACHE_SIZES=(358 3580 17900 35800)

echo "🚀 开始测试 $SYSTEM_MODE 模式下的缓存大小影响"
echo "   - 工作流: $WORKFLOW, 客户端: $CLIENT_CNT, Zipf: $ZIPF_PARAM"
echo "   - 最大缓存基准: ${MAX_CACHE_SIZE_MB}MB"

# 测试不同的缓存大小
for CACHE_BOUND in "${CACHE_BOUNDS[@]}"; do
    # 计算实际的缓存大小 (MB)
    # 使用 bc 进行浮点数计算
    ACTUAL_CACHE_SIZE=$(echo "$MAX_CACHE_SIZE_MB * $CACHE_BOUND" | bc)

    # 运行Python测试脚本，传递 cache_bound 用于记录
    python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_CNT $ZIPF_PARAM $CACHE_BOUND
    
    if [ $? -ne 0 ]; then
        echo "❌ 测试失败, 缓存比例: $CACHE_BOUND"
        exit 1
    fi
    
    echo "✅ 完成缓存比例测试: $CACHE_BOUND"
done


echo "✅ $SYSTEM_MODE 模式测试完成"