#!/bin/bash
# 示例：测试不同批处理大小下的系统性能
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

WORKFLOW="c4"
CLIENT_CNT=64
SYSTEM_MODE="repair" # 或者 "pessimistic"

# 定义要测试的批处理大小
# BATCH_SIZES=(1 2 4 6 8 12 16)
BATCH_SIZES=(8)

echo "🚀 开始测试 $SYSTEM_MODE 模式下的批处理大小影响"
echo "   - 工作流: $WORKFLOW, 客户端: $CLIENT_CNT"

# 测试不同的批处理大小
for BATCH_SIZE in "${BATCH_SIZES[@]}"; do
    echo ""
    echo "----------------------------------------------------"
    echo "📋 开始测试批处理大小: $BATCH_SIZE"

    # 运行Python测试脚本，传递 batch_size 用于记录
    python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_CNT $BATCH_SIZE
    
    if [ $? -ne 0 ]; then
        echo "❌ 测试失败, 批处理大小: $BATCH_SIZE"
        exit 1
    fi
    
    echo "✅ 完成批处理大小测试: $BATCH_SIZE"
done

echo "✅ $SYSTEM_MODE 模式测试完成"