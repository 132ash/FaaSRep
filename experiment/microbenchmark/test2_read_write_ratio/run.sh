#!/bin/bash
# 示例：测试不同读写比例下的吞吐量

WORKFLOW="c16"
CLIENT_CNT=16
SYSTEM_MODE="optimistic" # 或者 "pessimistic"

echo "🚀 开始测试 $SYSTEM_MODE 模式下的吞吐量"

# 清空旧的结果文件
RESULT_FILE="results/${SYSTEM_MODE}_res.csv"
if [ -f "$RESULT_FILE" ]; then
    rm "$RESULT_FILE"
    echo "🗑️ 已删除旧的结果文件: $RESULT_FILE"
fi

# 测试不同的读写比例 (0.0, 0.1, ..., 1.0)
for i in $(seq 0 10); do
    READ_RATIO=$(echo "scale=1; $i / 10" | bc)
    echo "📋 测试读比例: $READ_RATIO"
    python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_CNT $READ_RATIO
    
    if [ $? -ne 0 ]; then
        echo "❌ 测试失败，读比例: $READ_RATIO"
        exit 1
    fi
    
    echo "✅ 完成读比例: $READ_RATIO"
    echo ""
done

echo "📊 显示 $SYSTEM_MODE 模式的所有测试结果:"
python3 process_results.py $SYSTEM_MODE --show

echo "🔧 整理 $SYSTEM_MODE 模式的结果文件 (排序):"
python3 process_results.py $SYSTEM_MODE

echo "✅ $SYSTEM_MODE 模式测试完成"