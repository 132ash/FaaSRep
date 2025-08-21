#!/bin/bash
# 示例：测试不同数据倾斜度下的吞吐量
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

WORKFLOW="c4" #c16
CLIENT_CNT=32
SYSTEM_MODE="Concord" # 或者 "pessimistic"
# 定义要测试的 Zipf 参数
# ZIPF_PARAMS=(1.0 1.25 1.5 2)
ZIPF_PARAMS=(1.25)

echo "🚀 开始测试 $SYSTEM_MODE 模式下的数据倾斜度影响"



# 测试不同的 Zipf 参数
for ZIPF_PARAM in "${ZIPF_PARAMS[@]}"; do
    echo ""
    echo "📋 开始测试 Zipf 参数: $ZIPF_PARAM"
    python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_CNT $ZIPF_PARAM
    
    if [ $? -ne 0 ]; then
        echo "❌ 测试失败, Zipf 参数: $ZIPF_PARAM"
        exit 1
    fi
    
    echo "✅ 完成 Zipf 参数: $ZIPF_PARAM"
    echo ""
done

echo "📊 显示 $SYSTEM_MODE 模式的所有测试结果:"
python3 process_results.py $SYSTEM_MODE --show

echo "🔧 整理 $SYSTEM_MODE 模式的结果文件 (按zipf_param排序):"
python3 process_results.py $SYSTEM_MODE

echo "✅ $SYSTEM_MODE 模式测试完成"