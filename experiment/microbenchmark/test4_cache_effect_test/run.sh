CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

WORKFLOW="c4"
CLIENT_CNT=32
SYSTEM_MODE="repair" # 或者 "pessimistic"
ZIPF_PARAM=0.9 # 固定一个数据倾斜度

# --- 缓存失效概率配置 ---
# 定义要测试的概率列表
# REMOTE_PROBS=(0.01 0.1 0.5 1) 
REMOTE_PROBS=(1) 

echo "🚀 开始测试 $SYSTEM_MODE 模式下的缓存失效概率影响"
echo "   - 工作流: $WORKFLOW, 客户端: $CLIENT_CNT, Zipf: $ZIPF_PARAM"

# 测试不同的失效概率
for REMOTE_PROB in "${REMOTE_PROBS[@]}"; do
    echo "   - 当前测试概率: $REMOTE_PROB"

    # 运行Python测试脚本，传递 remote_prob 用于配置和记录
    python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_CNT $ZIPF_PARAM $REMOTE_PROB
    
    if [ $? -ne 0 ]; then
        echo "❌ 测试失败, 失效概率: $REMOTE_PROB"
        exit 1
    fi
    
    echo "✅ 完成失效概率测试: $REMOTE_PROB"
done

echo "✅ $SYSTEM_MODE 模式测试完成"