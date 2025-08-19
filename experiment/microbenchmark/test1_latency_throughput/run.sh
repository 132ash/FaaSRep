#!/bin/bash
CURRENT_SH_DIR=$(dirname $(readlink -f "$0"))
cd "$CURRENT_SH_DIR"

# 定义测试参数
WORKFLOWS=(c4)
# WORKFLOWS=(c2 c4 c8 c16 w2 w4 w8 w16)
# CLIENT_COUNTS=(16 32 48)
CLIENT_COUNTS=(16)
TEXT_SIZE=4096  # 固定为 4KB
SYSTEM_MODE="PESSIMISTIC"

echo "=== 开始消融实验 ==="
echo "测试时间: $(date)"
echo "工作流: ${WORKFLOWS[*]}"
echo "客户端数量: ${CLIENT_COUNTS[*]}"
echo "数据大小: ${TEXT_SIZE} bytes (4KB)"
echo "系统模式: $SYSTEM_MODE"
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

    for CLIENT_COUNT in "${CLIENT_COUNTS[@]}"; do
        echo ""
        echo "📊 测试配置:"
        echo "   - 工作流: $WORKFLOW"
        echo "   - 客户端数量: $CLIENT_COUNT"
        echo "   - 数据大小: ${TEXT_SIZE} bytes (4KB)"
        echo "   - 开始时间: $(date '+%H:%M:%S')"
        
        # 刷新数据库
        # echo "🗃️  刷新数据库..."
        # cd "$CURRENT_SH_DIR/../../../scripts/init/micro_benchmark"
        # python3 DB_setup.py flush
        # if [ $? -eq 0 ]; then
        #     echo "✅ 数据库刷新成功"
        # else
        #     echo "❌ 数据库刷新失败，跳过此测试"
        #     continue
        # fi
        
        # 返回测试目录
        cd "$CURRENT_SH_DIR"
        
        # 运行测试
        echo "🚀 开始运行测试..."
        LOG_FILE="logs/${WORKFLOW}_client_${CLIENT_COUNT}_4KB.log"
        
        # 使用 stdbuf 确保实时输出，同时保存日志
        echo "📝 实时日志将保存到: $LOG_FILE"
        echo "📺 以下是实时测试输出:"
        echo "----------------------------------------"
        
        # 使用 stdbuf -oL 确保行缓冲，-eL 确保错误输出也是行缓冲
        # tee 命令将输出同时显示在终端和保存到文件
        stdbuf -oL -eL python3 run.py "$WORKFLOW" "$SYSTEM_MODE" "$CLIENT_COUNT" 2>&1 | tee "$LOG_FILE"
        
        # 检查执行结果
        PYTHON_EXIT_CODE=${PIPESTATUS[0]}  # 获取 python 命令的退出码
        
        echo "----------------------------------------"
        if [ $PYTHON_EXIT_CODE -eq 0 ]; then
            echo "✅ 测试完成 (工作流: $WORKFLOW, 客户端: $CLIENT_COUNT)"
            echo "📝 完整日志已保存到: $LOG_FILE"
        else
            echo "❌ 测试失败 (工作流: $WORKFLOW, 客户端: $CLIENT_COUNT)"
            echo "📝 错误日志已保存到: $LOG_FILE"
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
    TEMP_RESULT_FILE="results/${WORKFLOW}_temp.csv"
    
    if [ -f "$TEMP_RESULT_FILE" ]; then
        echo "📊 发现结果文件: $TEMP_RESULT_FILE"
        echo "📋 文件内容:"
        cat "$TEMP_RESULT_FILE"
        
        # 调用结果处理脚本
        python3 process_results.py "$WORKFLOW" "$TEMP_RESULT_FILE"
        
        if [ $? -eq 0 ]; then
            echo "✅ 工作流 $WORKFLOW 结果处理完成"
            # 删除临时文件
            rm -f "$TEMP_RESULT_FILE"
            echo "🗑️  已删除临时文件: $TEMP_RESULT_FILE"
        else
            echo "❌ 工作流 $WORKFLOW 结果处理失败"
            echo "🔍 保留临时文件以供调试: $TEMP_RESULT_FILE"
        fi
    else
        echo "⚠️  未找到结果文件: $TEMP_RESULT_FILE"
        echo "🔍 检查测试是否正确执行"
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

# 显示最终结果汇总
echo ""
echo "📊 最终结果汇总:"
echo "================================"
for WORKFLOW in "${WORKFLOWS[@]}"; do
    FINAL_RESULT_FILE="results/${WORKFLOW}_results.csv"
    if [ -f "$FINAL_RESULT_FILE" ]; then
        echo "📈 工作流 $WORKFLOW 结果:"
        cat "$FINAL_RESULT_FILE"
        echo ""
    fi
done