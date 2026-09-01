# 重试阶段访问集变化实验设计

## 1. 实验目的

本文档描述一个针对 FaaSRep 的补充实验：研究事务在重试（repair/reconciliation）阶段访问集发生变化时，对系统性能和执行进度的影响。

论文 `atc26_FaaSRep.pdf` 的 §7.3 指出，FaaSRep 的 reconciliation 默认假设同一请求在首次执行和重试阶段具有稳定访问集。如果系统检测到访问集变化，则把该动态事务视为 application-level abort：丢弃该事务的 buffered writes，将其从提交历史中移除；如果该事务是其他事务的依赖前驱，则受影响事务可能需要由 optimistic repair 转为 pessimistic repair。

本实验不真正改变访问集，而是用 `retry_abort_func` 抽样需要按 OCC 语义处理的请求。validation 时，若该请求的所有函数均为 clean，则保留在 reconciliation 流程中且不触发主动 abort；若任一函数为 dirty，则将请求从当前 batch 的依赖和 writer 集合中移除，待 batch 提交后由 gateway 清理旧上下文并从头重试。重试时清除 `retry_abort_func`，因此同一请求不会再次触发实验主动 abort，最终客户端只应收到成功结果。

实验还需要验证高并发下的 optimistic-to-pessimistic 转换是否存在并发缺陷。系统一旦卡住，实验程序不得因 timeout 自动退出、重试或清理现场，而应保留阻塞状态，并通过 workersp、container、transaction sink、commit manager、serializer 和 gateway 的日志定位最后一次有效状态转移。

## 2. 实验配置

实验目录建议为：

```text
experiment/microbenchmark/test7_dynamic_access_set/
```

固定配置如下：

| 配置项 | 值 |
|---|---|
| Workflow | `c4` |
| 并发 client 数 | 32 |
| 每个 client 的事务数 | 100 |
| 每个概率点提交的事务数 | 3200 |
| 数据对象大小 | 4 KB |
| Zipf factor | 0.9 |
| Fast path | 开启 |
| Optimistic repair | 开启 |
| Sink 现有 `ABORT_PROB` | 0，避免二次随机 abort |
| Retry abort probability | 0%、25%、50%、75%、100% |

### 2.1 NO_PESSI 变种

`config/config.py` 中的 `NO_PESSI` 控制无悲观修复变种：

- `NO_PESSI = True`：optimistic repair 的依赖前继 abort 后，所有受影响的后继事务丢弃当前 attempt，并通过现有 OCC retry 通道从头重试；该规则沿事务依赖图向后传播。重试请求再次 validation 时仍使用 optimistic repair，不会进入 pessimistic repair。
- `NO_PESSI = False`：恢复原始 dynamic 行为，受影响后继从 optimistic repair 转为 pessimistic repair。

该开关只在 `OPTIMISTIC_REPAIR = True` 时生效。修改后需要重启 transaction sink、commit manager、gateway 等长驻服务，并重新创建 workflow containers。

Zipf factor 固定为 0.9，以延续当前 `experiment/microbenchmark/test3_data_skewness/run.sh` 使用的高竞争 c4、32-client 设置。该值应作为 `run.sh` 中的显式常量，方便后续调整。

实验采用 closed-loop client：每个 client 在前一个事务返回后才发送下一个事务。如果某个事务卡住，对应 client 应一直阻塞；其他未被阻塞的 client 可以继续运行，直到它们也完成或阻塞。不能使用请求 timeout、client join timeout、serializer timeout 退出或 watchdog 自动终止进程。

## 3. Abort 抽样语义

每个事务只能进行一次 abort 决策：

```text
以概率 p 选择该事务在 retry 阶段 abort：
    retry_abort_func = uniform(f1, f2, f3, f4)
否则：
    retry_abort_func = NONE
```

不能让四个函数分别以概率 `p` 独立抽样。否则事务的实际 abort 概率会变成：

```text
1 - (1 - p)^4
```

而且先执行的函数会更容易成为实际 abort 位置，无法满足“均等地出现在 c4 任意函数”的要求。

参数生成器应接受可选的 `retry_abort_prob` 和随机种子。每个事务的抽样结果和随机种子必须写入 raw result，以便重现实验。建议保留 Bernoulli 抽样语义，同时在汇总结果中报告实际 abort 选择率以及 f1～f4 的目标分布。

## 4. Abort 注入数据流

### 4.1 参数生成

扩展 `experiment/common/generate_param.py` 的 microbenchmark 参数生成接口，增加：

```python
retry_abort_prob=None
retry_abort_seed=None
```

生成给 f1 的输入增加：

```python
"retry_abort_func": "f1" | "f2" | "f3" | "f4" | "NONE"
```

默认值必须为 `NONE`，从而保证现有 microbenchmark 调用不启用 abort 注入。

### 4.2 c4 workflow 参数传播

修改 `benchmark/micro_benchmark/c4/workflow.yaml`：

- f1 从 GLOBAL 接收 `retry_abort_func`；
- f1、f2、f3 将其传递给下一个函数；
- f1～f4 的 input/output schema 声明该字段为 `str`；
- 只修改 c4，其他 microbenchmark workflow 维持原有输入输出格式。

### 4.3 函数代码

修改 `scripts/init/micro_benchmark/microbenchmark_func/main.py`。每个函数取得 `retry_abort_func` 后，仅在 optimistic repair 阶段且自身是目标函数时调用现有的 `store.abort_tx()`：

```python
func_input = store.fetch_input()
retry_abort_func = func_input.get("retry_abort_func", "NONE")

if store.is_optimistic_repair and retry_abort_func == function_name:
    store.abort_tx(
        f"INJECTED_DYNAMIC_ACCESS_ABORT target={function_name}"
    )
```

abort 检查应在当前函数的数据读写之前执行。首次执行和 pessimistic repair 均不得注入 abort。异常消息必须带固定标识 `INJECTED_DYNAMIC_ACCESS_ABORT`，用来区分实验注入、应用自身异常和系统错误。

## 5. Validation 阶段的 OCC 分流

`retry_abort_func` 作为轻量 transaction metadata 沿控制路径传递：

```text
microbenchmark function / Store
  -> container response
  -> WorkerSP TransactionState
  -> transaction sink batch
  -> validator
  -> RepairInfo
```

建议在 Store 中增加 transaction metadata 字段和设置接口。函数在首次执行时登记 `retry_abort_func`，container response 将其与 read set、write set、RYW metadata 一起返回。workersp 汇总并随 validation 请求传给 sink，sink 将其纳入 transformed batch，validator 再传给 `RepairInfo.construct_repair_metadata()`。

serializer 完成正常 stale-read/dependency 检查后再处理被选中的请求：

- 所有函数 clean：保留请求，不强制目标函数 dirty；fast path 复用 clean 结果，因此不会运行主动 abort 代码。
- 存在 dirty 函数：在该请求写入 `key_writers` 之前将其标记为 OCC retry，并删除以该请求为后继的 batch/transaction dependency。

validator、sink 和 repair engine 只接收过滤后的 transaction list。原请求的 buffered writes 不提交；batch 提交后 gateway 收到独立的 `retry_txs` 通知，清理 WorkerSP/container/shadow state，用同一 txid 重新执行，并把输入中的 `retry_abort_func` 改为 `NONE`。

作为防御性兜底，gateway 若仍收到带 `INJECTED_DYNAMIC_ACCESS_ABORT` 标识的终态 abort，也必须将其转换为同样的内部 OCC retry，而不能返回失败请求。

## 6. OCC retry 的系统行为

被选中且 dirty 的请求预期状态流为：

```text
serializer detects dirty OCC request
  -> remove request from current batch dependencies and writer selection
  -> retained requests reconcile and commit normally
  -> validator notifies gateway through retry_txs
  -> gateway clears the old attempt
  -> retry_abort_func becomes NONE
  -> same txid executes and validates again
  -> final successful result returns to the client
```

该内部 validation abort 计入 `occ_retries`，但不作为 terminal-aborted 请求写入 raw result。真正的应用异常仍使用原有 application-level abort 路径。

## 7. Abort writer 的提交语义修正

现有实现已经在 `src/commit_manager/pessimistic_repairer.py` 中从 successful transaction table 和 batch-local writer table 移除 abort 事务。但当前提交接口仍存在缺口：

- serializer 在 validation 时只保存某个 batch 对每个 key 的最后一个初始 writer；
- abort 后，`pessimistic_get_commit_keys()` 只返回 `key: True`；
- 如果最后一个初始 writer abort，而更早的 writer 没有 abort，serializer 仍可能引用 abort writer 的 buffered value。

为了真正实现“移除 abort 事务”，`pessimistic_get_commit_keys()` 应返回每个 key 最后一个未 abort writer，而不是布尔值：

```python
{
    key: [selected_tx_id, selected_function]
}
```

`src/commit_manager/serializer.py` 在 commit 时使用该 writer override 选择实际 shadow write。如果某个 key 的所有 writer 均已 abort，则该 key 不进入 commit set。该行为对应论文中的：

```text
MergeLatestWrites(non-aborted transactions, serial order)
```

该修正需要覆盖普通 commit 和 cascaded commit，且日志中必须同时记录 validation 阶段的原 writer 和 abort 过滤后的 selected writer。

## 8. 可能导致卡住的并发窗口

### 8.1 Optimistic attempt 与 pessimistic attempt 重叠

当前 container 在任意 repair attempt 完成后会无条件删除 transaction context。可能发生如下交错：

```text
optimistic attempt 正在执行
  -> predecessor abort
  -> sink/validator 将本事务切换为 pessimistic repair
  -> pessimistic request 更新或开始使用 context
  -> 旧 optimistic attempt 返回
  -> 旧 attempt 删除了 pessimistic attempt 的 context
  -> pessimistic attempt 找不到 context，或后续函数永远等不到触发
```

应为每个 repair attempt 增加：

```text
repair_epoch
repair_mode
attempt_id
```

context cleanup 只有在 context identity、repair epoch 和 repair mode 全部仍与当前 attempt 相同时才允许执行。旧 optimistic result 在模式已经变化后只能记录为 `STALE_RESULT_DROPPED`，不能更新新状态、触发 terminal event 或清理新 context。

### 8.2 Sink terminal event 重复或乱序

每个 `(batch_id, tx_id, repair_epoch, repair_mode)` 的 `/abort` 或 `/fin_repair` 必须具备幂等性。重复事件应记录 `DUPLICATE_TERMINAL_EVENT`，但不得：

- 重复增加 batch finished count；
- 重复减少 successor 的依赖计数；
- 重复发送 pessimistic repair；
- 重复向 validator 报告 abort。

状态检查、terminal 标记、finished count 和 successor readiness 更新应位于同一个受保护的状态转移中。

### 8.3 乐观结果晚于模式转换到达

当 `need_pessimistic_repair=True` 后收到旧 optimistic finish，sink 应显式记录并拒绝该结果。拒绝动作不能被误认为事务已经完成，也不能覆盖已经开始的 pessimistic attempt。

### 8.4 Commit 等待链

如果 batch 卡住，需要区分：

- 事务尚未向 sink 报告 terminal；
- sink finished count 未达到 total；
- successor 的 predecessor count 未归零；
- pessimistic repair 已 ready 但 validator 未触发；
- validator 已触发但 workersp/container 未接收；
- repair 已完成但 serializer 认为前序 batch 未 ready；
- serializer 已允许 commit 但 gateway 未收到 notify。

这些状态都必须能从周期快照和事件日志中直接识别。

## 9. 不使用 Timeout 的阻塞观测原则

本实验不设置以下任何 timeout：

- client 到 gateway 的 HTTP timeout；
- workersp、sink、validator、serializer 之间状态请求的 timeout；
- client process join timeout；
- gateway `waitTX()` timeout；
- serializer response timeout；
- 自动 watchdog kill 或自动重启 timeout。

现有 validator 中 serializer response 的 10 秒 timeout 和随后 `os._exit(1)` 的行为，需要在本实验路径中移除。等待应保持阻塞，使系统卡住时 validator 进程和内存状态仍然存在。

“不设置 timeout”不等于不观测进度。需要增加一个只读 progress reporter，它只输出状态，不改变系统状态，也不终止任何请求。建议每隔固定时间输出一次快照，例如每 10 秒：

```text
PROGRESS_SNAPSHOT
component
active_batches
active_transactions
queue_depth
last_transition_timestamp
waiting_by_reason
```

该时间间隔只是日志采样周期，不是 timeout，也不用于宣告失败。即使若干分钟没有状态变化，实验仍继续等待，由实验人员根据日志显式确认系统已经卡住。

客户端也应周期性输出：

```text
CLIENT_PROGRESS
configured_probability
submitted
returned_committed
returned_aborted
currently_waiting_tx_ids
last_response_timestamp
```

由于 gateway 在响应前不会把 server-generated transaction id 返回客户端，实验程序应提前生成 transaction id，并在调用 `/run` 时显式传入。这样某个请求长时间不返回时，仍能用 txid 串联所有组件日志。

## 10. 结构化诊断日志

所有状态日志使用单行结构化格式，至少包含：

```text
event
workflow
batch_id
tx_id
function
repair_mode
repair_epoch
attempt_id
state_before
state_after
timestamp
```

日志必须在关键状态转换后立即 flush。不得记录 4 KB value 或完整 payload；read/write set 只记录 key 数量、必要 key 名和摘要。

### 10.1 Workersp

在 `src/workflow_manager/workersp.py` 和 `src/workflow_manager/proxy.py` 记录：

- `REQUEST_RECEIVED`
- `STATE_CREATE`
- `STATE_UPDATE`
- `REPAIR_MODE_CHANGE`
- `FUNCTION_TRIGGER_LOCAL`
- `FUNCTION_TRIGGER_REMOTE`
- `FUNCTION_TRIGGER_CONTAINER`
- `RUNNABLE_CHECK`，包括 parent executed、workflow parent count、upstream wait count
- `FUNCTION_START`
- `FUNCTION_FINISH`
- `FUNCTION_ABORT`
- `SINK_NOTIFY_START`
- `SINK_NOTIFY_FINISH`
- `STALE_TRIGGER_REJECTED`
- `WORKERSP_PROGRESS_SNAPSHOT`

### 10.2 Container

在 `src/container/proxy.py` 记录：

- `REPAIR_REQUEST_RECEIVED`
- `REPAIR_METADATA_INSTALLED`
- `CONTEXT_MODE_CHANGE`
- `RUNNABLE_CHECK`
- `APPLICATION_EXEC_START`
- `APPLICATION_EXEC_FINISH`
- `INJECTED_DYNAMIC_ACCESS_ABORT`
- `STALE_RESULT_DROPPED`
- `CONTEXT_CLEANUP_ACCEPTED`
- `CONTEXT_CLEANUP_REJECTED`
- `DOWNSTREAM_TRIGGER`
- `CONTAINER_PROGRESS_SNAPSHOT`

### 10.3 Transaction sink

在 `src/transaction_sink/validate_struct.py`、`batch_state_struct.py` 和 `proxy.py` 记录：

- `BATCH_REGISTER`
- `DEPENDENCY_REGISTER`
- `TX_TERMINAL_EVENT_RECEIVED`
- `TX_REPAIRED`
- `TX_ABORTED`
- `DUPLICATE_TERMINAL_EVENT`
- `OPT_TO_PESSI`
- `PESSI_READY`
- `SUCCESSOR_DEP_COUNT_CHANGE`
- `BATCH_PROGRESS`，包括 finished/total 和尚未 terminal 的 txid
- `BATCH_FINISHED`
- `VALIDATOR_TASK_EMITTED`
- `SINK_PROGRESS_SNAPSHOT`

### 10.4 Commit manager 与 serializer

在 `src/commit_manager/validator.py`、`repair_engine.py`、`pessimistic_repairer.py`、`serializer.py` 和 `repair_info.py` 记录：

- `VALIDATE_BEGIN`
- `VALIDATE_END`
- `REPAIR_PLAN`，包括 optimistic、pessimistic、waiting transaction
- `REPAIR_TRIGGER_SENT`
- `REPAIR_FINISH_RECEIVED`
- `ABORT_WRITER_REMOVED`
- `COMMIT_WRITER_SELECTED`
- `SERIALIZER_REQUEST_ENQUEUED`
- `SERIALIZER_REQUEST_DEQUEUED`
- `SERIALIZER_RESPONSE_ENQUEUED`
- `BATCH_COMMIT_READY`
- `BATCH_COMMIT_SUSPENDED`
- `CASCADED_COMMIT`
- `GATEWAY_NOTIFY_START`
- `GATEWAY_NOTIFY_FINISH`
- `VALIDATOR_PROGRESS_SNAPSHOT`
- `SERIALIZER_PROGRESS_SNAPSHOT`

serializer 快照还应包含：

```text
batch_write_info.keys()
commit_suspended_batches
batch_validator_assignment
key_writers 中各 batch 的等待关系摘要
validator response_events
```

### 10.5 Gateway 与 client

Gateway 记录：

- `TX_REGISTER`
- `INITIAL_EXEC_TRIGGER`
- `WAIT_TX_BEGIN`
- `NOTIFY_RECEIVED`
- `TX_TERMINAL_COMMIT`
- `TX_TERMINAL_ABORT`
- `CLEAR_BEGIN`
- `CLEAR_FINISH`
- `GATEWAY_PROGRESS_SNAPSHOT`

Client 记录每个 txid 的 submit 和 response。卡住时，client 日志中的 `currently_waiting_tx_ids` 应能直接作为跨组件检索入口。

## 11. 新实验目录内容

```text
experiment/microbenchmark/test7_dynamic_access_set/
├── README.md
├── run.py
├── run.sh
├── process_results.py
├── inspect_progress.py
├── logs/
└── results/
    └── hybrid/
        ├── raw_results/
        └── summary_results.csv
```

`run.sh` 依次执行：

```bash
ABORT_PROBS=(0 0.25 0.50 0.75 1.00)
```

如果某个概率点卡住，脚本会自然停留在该点，不会继续下一个概率，也不会杀死 client。此时运行 `inspect_progress.py` 只读收集：

- 当前仍在等待的 client/txid；
- gateway running transaction；
- sink active batch 与 finished count；
- validator active batch、repair plan 和 response event；
- serializer suspended batch；
- workersp/container 最后一次函数状态。

`inspect_progress.py` 不清理 Redis、CouchDB、容器、batch state 或 client process。

## 12. 实验结果

每个已经返回的请求立即追加到 raw CSV，不能等所有 client 完成后统一写入，否则系统卡住时会丢失已完成结果。建议同时使用 flush 和 append-only 格式。

Raw result 至少包含：

```text
tx_id
client_id
round
configured_abort_prob
expected_abort
abort_target
status
e2e_latency
rounds
pessimistic
submit_timestamp
response_timestamp
occ_retries
error
```

周期性 progress 文件还应包含尚未返回的 txid。正常完成概率点后，summary 仅保留：

- `configured_abort_prob`；
- `actual_abort_count`，即所有成功请求的 `occ_retries` 之和；
- `success_count`；
- `success_p50`、`success_p99`；
- `success_throughput`。

由于没有 timeout，正常完成的汇总中不存在 timeout 类别。若系统卡住，则该概率点不生成“正常完成”的 summary row，而是保留：

- 已经 append 的 raw results；
- 当前 waiting txid 清单；
- 所有组件的 progress snapshots；
- 最后一条状态转移时间；
- 实验仍在运行的现场。

任一请求最终返回非 `ok` 状态时，该概率点不得生成正常 summary row。

## 13. 验证顺序

正式运行前按以下顺序验证：

1. `p=0`：没有 injected abort，结果应接近原 c4 基线。
2. 被选中且全 clean：留在 repair 流程，不执行主动 abort。
3. 被选中且存在 dirty 函数：从当前 batch 移除，writer/dependency 中均不再出现该 txid。
4. gateway 收到 `retry_txs`：清理旧上下文、保留同一 txid、将 `retry_abort_func` 改为 `NONE` 后重试。
5. batch 中全部请求均需 OCC retry：空 batch 正常提交和释放，不残留 sink/serializer 状态。
6. 32 clients、`p=1`：全部请求最终返回 `ok`，summary 的 `actual_abort_count` 等于内部 OCC retry 总次数。
7. 执行完整的五个概率点。

## 14. 预期修改文件

应用与 workflow：

- `scripts/init/micro_benchmark/microbenchmark_func/main.py`
- `benchmark/micro_benchmark/c4/workflow.yaml`
- `experiment/common/generate_param.py`

Abort metadata 与目标函数 dirty 标记：

- `src/container/Store.py`
- `src/container/proxy.py`
- `src/workflow_manager/workersp.py`
- `src/workflow_manager/proxy.py`
- `src/transaction_sink/proxy.py`
- `src/transaction_sink/validate_struct.py`
- `src/commit_manager/validator.py`
- `src/commit_manager/repair_info.py`

Abort writer 移除和 commit：

- `src/commit_manager/pessimistic_repairer.py`
- `src/commit_manager/serializer.py`

并发状态与诊断日志：

- `src/container/proxy.py`
- `src/workflow_manager/workersp.py`
- `src/workflow_manager/proxy.py`
- `src/transaction_sink/batch_state_struct.py`
- `src/transaction_sink/validate_struct.py`
- `src/transaction_sink/proxy.py`
- `src/commit_manager/validator.py`
- `src/commit_manager/repair_engine.py`
- `src/commit_manager/pessimistic_repairer.py`
- `src/commit_manager/repair_info.py`
- `src/commit_manager/serializer.py`
- `src/gateway/gateway.py`
- `src/gateway/transaction_info.py`

实验目录：

- `experiment/microbenchmark/test7_dynamic_access_set/README.md`
- `experiment/microbenchmark/test7_dynamic_access_set/run.py`
- `experiment/microbenchmark/test7_dynamic_access_set/run.sh`
- `experiment/microbenchmark/test7_dynamic_access_set/process_results.py`
- `experiment/microbenchmark/test7_dynamic_access_set/inspect_progress.py`

除以上设计文档外，本阶段不修改这些代码文件。
