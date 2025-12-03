#!/bin/bash
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

# 定义测试参数
WORKFLOWS=(w6)
CLIENT_COUNTS=(32)
# 定义不同的 sleep_time
# SLEEP_TIMES=(0 0.01 0.05 0.1 0.2 0.5 1)
SLEEP_TIMES=(0)

TEXT_SIZE=4096  # 固定为 4KB
SYSTEM_MODE="Concord"

echo "=== 开始实验 ==="
echo "测试时间: $(date)"
echo "工作流: ${WORKFLOWS[*]}"
echo "客户端数量: ${CLIENT_COUNTS[*]}"
echo "Sleep Times: ${SLEEP_TIMES[*]}"
echo "数据大小: ${TEXT_SIZE} bytes (4KB)"
echo "系统模式: $SYSTEM_MODE"
echo "================================"

# 创建结果目录
mkdir -p results
mkdir -p logs

echo ""
echo "🔄 开始测试数据大小: ${TEXT_SIZE} bytes (4KB)"
echo "================================"

# 遍历不同的 Sleep Time
for SLEEP_TIME in "${SLEEP_TIMES[@]}"; do
    echo ""
    echo "💤 开始测试 Sleep Time: ${SLEEP_TIME}s"
    echo "================================"

    # 遍历工作流和客户端 (虽然这里只有一个配置)
    for WORKFLOW in "${WORKFLOWS[@]}"; do
        for CLIENT_COUNT in "${CLIENT_COUNTS[@]}"; do
            echo ""
            echo "📊 测试配置:"
            echo "   - Sleep Time: $SLEEP_TIME"
            echo "   - 工作流: $WORKFLOW"
            echo "   - 客户端数量: $CLIENT_COUNT"
            echo "   - 开始时间: $(date '+%H:%M:%S')"
            
            # 返回测试目录
            cd "$CURRENT_SH_DIR"
            
            # 运行测试
            echo "🚀 开始运行测试..."
            LOG_FILE="logs/${WORKFLOW}_client_${CLIENT_COUNT}_sleep_${SLEEP_TIME}.log"
            
            # 使用 stdbuf 确保实时输出，同时保存日志
            echo "📝 实时日志将保存到: $LOG_FILE"
            echo "📺 以下是实时测试输出:"
            echo "----------------------------------------"
            
            # 传入 sleep_time 参数
            stdbuf -oL -eL python3 run.py "$WORKFLOW" "$CLIENT_COUNT" "$SLEEP_TIME" 2>&1 | tee "$LOG_FILE"
            
            # 检查执行结果
            PYTHON_EXIT_CODE=${PIPESTATUS[0]}
            
            echo "----------------------------------------"
            if [ $PYTHON_EXIT_CODE -eq 0 ]; then
                echo "✅ 测试完成 (Sleep: $SLEEP_TIME)"
            else
                echo "❌ 测试失败 (Sleep: $SLEEP_TIME)"
            fi
        
        echo "   - 结束时间: $(date '+%H:%M:%S')"
        echo "--------------------------------"
    done
    
    echo "✅ 工作流 $WORKFLOW 测试完成"
    done
done

