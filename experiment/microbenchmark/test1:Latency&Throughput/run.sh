CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

# 定义测试参数
WORKFLOWS=(c2 c4 c8 c16 w2 w4 w8 w16)
CLIENT_COUNTS=(2 4 8 12 16 24 32)
TEXT_SIZE=4096  # 固定为 4KB
SYSTEM_MODE="OPTIMISTIC"

echo "=== 开始微基准测试 ==="
echo "测试时间: $(date)"
echo "客户端数量: ${CLIENT_COUNTS[*]}"
echo "数据大小: ${TEXT_SIZE} bytes (4KB)"
echo "系统模式: $SYSTEM_MODE"
echo "延迟指标: P50"
echo "================================"

# 创建结果目录
mkdir -p results
mkdir -p logs

echo ""
echo "🔄 开始测试数据大小: ${TEXT_SIZE} bytes (4KB)"
echo "================================"

# 遍历不同的客户端数量
for WORKFLOW in "${WORKFLOWS[@]}"; do
    echo ""
    echo "🔄 开始测试工作流: ${WORKFLOW}"
    echo "================================"
    
    # 为每个工作流创建临时结果文件
    TEMP_RESULT_FILE="results/${WORKFLOW}_temp.csv"
    echo "workflow,client_count,median_latency,avg_throughput" > "$TEMP_RESULT_FILE"

    for CLIENT_COUNT in "${CLIENT_COUNTS[@]}"; do
        echo ""
        echo "📊 测试配置:"
        echo "   - 客户端数量: $CLIENT_COUNT"
        echo "   - 数据大小: ${TEXT_SIZE} bytes (4KB)"
        echo "   - 开始时间: $(date '+%H:%M:%S')"
        
        # 刷新数据库
        echo "🗃️  刷新数据库..."
        cd "$CURRENT_SH_DIR/../../../scripts/init/micro_benchmark"
        python3 DB_setup.py flush
        if [ $? -eq 0 ]; then
            echo "✅ 数据库刷新成功"
        else
            echo "❌ 数据库刷新失败，跳过此测试"
            continue
        fi
        
        # 返回测试目录
        cd "$CURRENT_SH_DIR"
        
        # 运行测试
        echo "🚀 开始运行测试..."
        LOG_FILE="logs/${WORKFLOW}_client_${CLIENT_COUNT}_4KB.log"
        
        # 调用 run.py 进行测试，捕获输出
        TEST_OUTPUT=$(python3 run.py $WORKFLOW $SYSTEM_MODE $CLIENT_COUNT 2>&1 | tee "$LOG_FILE")
        
        if [ $? -eq 0 ]; then
            echo "✅ 测试完成 (工作流: $WORKFLOW, 客户端: $CLIENT_COUNT)"
            echo "📝 日志保存到: $LOG_FILE"
            
            # 从输出中提取结果 (格式: RESULT:workflow,client_count,median_latency,avg_throughput)
            RESULT_LINE=$(echo "$TEST_OUTPUT" | grep "^RESULT:" | tail -1)
            if [ -n "$RESULT_LINE" ]; then
                RESULT_DATA=$(echo "$RESULT_LINE" | cut -d':' -f2-)
                echo "$RESULT_DATA" >> "$TEMP_RESULT_FILE"
                echo "📊 结果: $RESULT_DATA"
            else
                echo "⚠️  未找到结果数据"
            fi
        else
            echo "❌ 测试失败 (工作流: $WORKFLOW, 客户端: $CLIENT_COUNT)"
        fi
        
        echo "   - 结束时间: $(date '+%H:%M:%S')"
        echo "--------------------------------"
        
        # 等待一段时间让系统稳定
        echo "⏳ 等待系统稳定..."
        sleep 2
    done

    # 处理该工作流的结果
    echo ""
    echo "📈 处理工作流 $WORKFLOW 的结果..."
    python3 process_results.py "$WORKFLOW" "$TEMP_RESULT_FILE"
    
    if [ $? -eq 0 ]; then
        echo "✅ 工作流 $WORKFLOW 结果处理完成"
        # 删除临时文件
        rm -f "$TEMP_RESULT_FILE"
    else
        echo "❌ 工作流 $WORKFLOW 结果处理失败"
    fi
    
    echo "✅ 工作流 $WORKFLOW 测试完成"
done

echo ""
echo "✅ 4KB 数据大小的所有测试完成"

echo ""
echo "🎉 所有测试完成!"
echo "结束时间: $(date)"
echo "结果文件位于: $CURRENT_SH_DIR/results/"
echo "日志文件位于: $CURRENT_SH_DIR/logs/"
