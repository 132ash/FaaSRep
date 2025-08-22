#!/bin/bash
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

WORKFLOW="c16"
CLIENT_CNT=32
SYSTEM_MODE="beldi" # 或者 "pessimistic"
# READ_RATIO=(1 0.75 0.5 0.25 0)\
READ_RATIOS=(1)

echo "🚀 开始测试 $SYSTEM_MODE 模式下的吞吐量"

# 测试不同的读写比例 (0, 0.25, 0.5, 0.75, 1.0)
for READ_RATIO in "${READ_RATIOS[@]}"; do
    echo "📋 测试读比例: $READ_RATIO"
    python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_CNT $READ_RATIO
    
    if [ $? -ne 0 ]; then
        echo "❌ 测试失败，读比例: $READ_RATIO"
        exit 1
    fi
    
    echo "✅ 完成读比例: $READ_RATIO"
    echo ""
done

echo "✅ $SYSTEM_MODE 模式测试完成"