# FaaSRep / FaaSnap 项目研究报告

## 1. 总览

本仓库名为 `FaaSnap`，README 中描述为 “A serverless engine for efficient transactional workflow with snapshot-level isolation”。但仓库根目录的论文 `FaaSRep.pdf` 题为 **FaaSRep: Enabling Strict Serializability with Efficiency for Stateful Serverless Workflows via Transaction Repair**，论文目标是为有状态 serverless workflow 提供严格可串行化事务支持，并用 transaction repair 降低传统并发控制的反复 abort/retry 成本。

从代码看，这个项目实现的是一个研究型 serverless transactional workflow runtime，核心思想与论文 FaaSRep 基本一致：第一次执行采用类似 OCC 的乐观执行，函数通过 sidecar API 读写外部状态，运行时收集 read set / write set / RYW 关系；transaction sink 将事务批量送到 validator；validator 在验证阶段延迟构建依赖图；随后系统只重跑受冲突影响的函数，并通过乐观 repair 与必要时的悲观 repair 保证严格可串行化。

仓库的实现不是一个生产级平台，而是一个论文原型。它使用 Flask/gevent HTTP 服务、Docker 容器、Redis、CouchDB、ScyllaDB Alternator/DynamoDB API 组合实现控制面、数据面和实验环境。论文中一些描述与当前代码存在差异，尤其是 fast-path 的通信实现、benchmark 的确定性约束、主动 abort 的注入方式等，后文会单独列出。

## 2. 论文核心问题

论文关注的应用是 **有状态 serverless workflow**。这类应用由 DAG 形式的函数组成，每个函数可能访问外部状态，例如电商库存、银行转账、旅行预订、社交网络状态更新。为了避免超卖、余额错误、脏读等问题，这些 workflow 需要事务语义，而且论文目标是比 snapshot isolation 更强的 **strict serializability**。

现有方案的问题是：

- 2PL/remote locking 类方案，例如 Beldi，需要跨节点获取远程锁，高冲突时等待和 abort 成本高。
- OCC 类方案先乐观执行，验证失败后整笔事务 abort 并盲目重试；高冲突下大量事务会陷入多轮失败尝试。
- 一些 serverless consistency 系统只提供弱一致性或 snapshot isolation，不能阻止并发写冲突。
- serverless workflow 的函数分散在多节点，运行时提前构建全局依赖图会引入大量远程协调。
- workflow 经过控制面调度，重试整个事务会重复执行大量无冲突函数，并放大调度开销。

FaaSRep 的答案是 **transaction repair**：先让事务执行一次，收集实际读写集合，再在验证阶段构建依赖图。之后不是盲目重跑事务，而是按照依赖关系进行一次有组织的重执行，让冲突事务按拓扑顺序读取正确版本。

## 3. 论文提出的关键技术

### 3.1 延迟依赖构建

FaaSRep 不在函数运行期间维护全局依赖图，而是在 validator 执行 OCC stale-read 检查时顺手构建依赖。validator 为每个 key 维护：

- 当前全局版本 `key_version_table`
- 未提交 writer 列表 `key_writers`

验证一个事务时，对于每个函数的每个读 key：

1. 如果 `key_writers[key]` 非空，说明前面已有未提交事务写了这个 key，当前函数依赖最近的 writer，标记为 dirty。
2. 如果没有未提交 writer，则比较函数读到的版本和全局版本。如果读版本落后，说明 stale read，标记为 dirty。
3. 函数读写检查完成后，validator 将该事务写过的 key 插入 writers 列表。

这个过程在代码中主要对应 [src/commit_manager/serializer.py](src/commit_manager/serializer.py)：

- `SerializerProcess.accessed_set_validate`
- `SerializerProcess.get_expired_set_and_subjection`
- `SerializerProcess.update_key_writers`
- `SerializerProcess.get_commitable_batches`

论文强调这种构建方式只增加每次数据访问常数级操作，整体仍与 OCC 验证同阶。

### 3.2 乐观 repair

乐观 repair 假设上游事务不会主动 abort。validator 将依赖图变成 repair metadata，发送到 worker。每个 dirty 函数等待其 upstream 完成后重跑，读 upstream shadow table 中的正确版本；非 dirty 函数可以跳过执行，但仍需要唤醒下游。

代码对应：

- [src/commit_manager/repair_info.py](src/commit_manager/repair_info.py) 构造每个函数的 `dirty`、`upstream_keys`、`RYW_keys`、`successor_port`。
- [src/commit_manager/repair_engine.py](src/commit_manager/repair_engine.py) 将 metadata 预置到 worker，并触发 start functions 进入 repair。
- [src/container/proxy.py](src/container/proxy.py) 的 `Runner.fetch_repair_metadata`、`Runner.check_runnable`、`Runner.run` 控制容器侧重跑或跳过。
- [src/container/Store.py](src/container/Store.py) 的 `Store.get` 在 repair 时优先读取 RYW 或 upstream 预取数据。

### 3.3 悲观 repair 与主动 abort

单纯乐观 repair 无法处理 proactive abort。原因是一个上游事务在 repair 中主动 abort 后，下游依赖图可能失效，继续提交会破坏串行化顺序。

论文因此引入悲观 repair：

- 每个事务维护 pessimistic batch dependency（PBD）和 pessimistic transaction dependency（PTD）。
- 事务只有在相关前序 batch 已提交、同 batch 的相关前序事务已 resolved 后，才被认为是 pessimistic-ready。
- 此时再构建依赖，主动 abort 不会继续使依赖失效。

代码对应：

- [src/transaction_sink/batch_state_struct.py](src/transaction_sink/batch_state_struct.py) 中 `PessimisticBatchState` 和 `OptimisticTransactionState` 跟踪悲观依赖和乐观状态。
- [src/transaction_sink/validate_struct.py](src/transaction_sink/validate_struct.py) 中 `RepairingBatchState.after_transaction_finish` 处理 abort 后的 cascading mode switch。
- [src/commit_manager/pessimistic_repairer.py](src/commit_manager/pessimistic_repairer.py) 在悲观 repair 前基于当前 batch 内 writer table 重新计算依赖，并剔除已 abort 事务的写。

论文给出的上界是：一次请求最多经历三轮，即首次执行、乐观 repair、悲观 repair。代码中 gateway 返回的 `rounds` 也使用这个模型：没有悲观 repair 时为 2，发生悲观 repair 时为 3。

### 3.4 函数级 repair

FaaSRep 不以整笔事务为最小 repair 单位，而是以函数为单位。每个函数都有自己的读写集合和 dirty 标记。这样一个 workflow 中只有读到 stale data 或依赖未提交 writer 的函数需要重跑，其余函数可以跳过。

在代码里，repair metadata 按 `tx_id -> func -> metadata` 或按 worker IP 聚合：

```text
{
  dirty: bool,
  up_cnt: int,
  upstream_keys: {key: [prev_tx_id, prev_func]},
  RYW_keys: {key: upstream_func},
  successor_port: {next_func: port}
}
```

这些字段主要在 [src/commit_manager/repair_info.py](src/commit_manager/repair_info.py) 构建，在 worker 或容器侧消费。

### 3.5 Fast-path

论文中的 fast-path 是 repair 阶段容器之间的 peer-to-peer 直接通信，用来绕过 workflow control plane。设计上，初次执行时记录每个函数容器的 IP/port，repair 时直接触发下游容器并传递依赖数据。

当前代码实现中，fast-path 是 **HTTP + Redis shadow table** 组合，而不是论文文字中提到的 gRPC：

- validator/repair engine 仍会先调用 worker 的 `/prepare` 和 `/repair`。
- worker `/repair` 调用指定容器端口的 `/run`。
- 容器修复完成后，通过 `successor_port` 直接 POST 下游容器 `/run`。
- upstream data 通过 Redis 的 `UPSTREAM`、`SUCCESSOR`、`STATE` key 传递和唤醒。

相关代码在 [src/container/redis_component.py](src/container/redis_component.py) 的 `RepairSidecar` 和 [src/container/proxy.py](src/container/proxy.py) 的 `Runner.trigger_downstream_functions`。

## 4. 仓库结构与模块职责

### 4.1 配置与元数据

- [config/config.py](config/config.py)：全局配置，包括 storage node IP、CouchDB/DynamoDB endpoint、Redis 端口、workflow 路径、container 数、validator 数、batch size、fast path、optimistic repair 等。
- [config/worker_info.yaml](config/worker_info.yaml)：worker 节点列表。
- [src/initializer](src/initializer)：解析 workflow YAML、分配函数到节点，并把 workflow metadata 写入 CouchDB。

当前 `config/config.py` 中启用的 workflow 只有 `social_network`，其他 workflow 配置被注释。关键默认值：

- `DEFAULT_CONTAINER_NUM = 64`
- `VALIDATORS_PER_POOL = 4`
- `BATCH_SIZE = 1`
- `FAST_PATH = True`
- `OPTIMISTIC_REPAIR = True`
- `TRACE_TEST = True`

### 4.2 Gateway

入口在 [src/gateway/gateway.py](src/gateway/gateway.py)。

Gateway 提供：

- `POST /run`：接收客户端请求，注册 transaction id，写入全局输入，触发 workflow start functions，然后等待 validator 通知。
- `POST /notify`：由 validator 在 commit 或 abort 后通知 gateway；gateway 唤醒等待中的客户端请求。
- `POST /clear_container`：清理 workflow 容器。

Gateway 的 `Repository` 使用 CouchDB 保存 workflow metadata 和 results，使用 Redis 将 workflow input 写到 start function 所在节点的 shadow table。最终结果从 end function 的 `RET` key 读取。

### 4.3 WorkerSP / Workflow Manager

Worker 入口在 [src/workflow_manager/proxy.py](src/workflow_manager/proxy.py) 和 [src/workflow_manager/workersp.py](src/workflow_manager/workersp.py)。

WorkerSP 的职责：

- 根据 CouchDB metadata 创建每个 workflow 的 `WorkerSPManager`。
- 维护每个 transaction 的 DAG 执行状态。
- 触发本地或远程函数。
- 首次执行结束后将 read/write set 发给 transaction sink。
- 接收 validator 准备好的 repair metadata。
- 执行 commit，将 Redis shadow table 中的写刷到全局数据库。

`WorkerSPManager` 中重要数据结构是 `TransactionState`：

- `read_set`: `{func: {key: version}}`
- `write_set`: `{key: func}`
- `RYW_subjection`: `{func: {key: upstream_func}}`
- `container_port`: `{func: port}`
- `parent_executed`: workflow DAG 父节点完成计数
- `repair_states`: validator 下发的函数级 repair metadata

### 4.4 Function Manager 与容器池

相关文件：

- [src/function_manager/function_manager.py](src/function_manager/function_manager.py)
- [src/function_manager/function.py](src/function_manager/function.py)
- [src/function_manager/container.py](src/function_manager/container.py)

每个函数对应一个 `Function` 对象和一个 `ContainerPool`。容器镜像由 benchmark 下各函数目录构建，基础镜像为 `workflow_base`。容器对外暴露 5000 端口，宿主机为每个容器分配一个本地端口。

请求调度流程：

1. `Function.send_request` 将请求放入函数队列。
2. `_dispatch_loop` 定期从队列取请求。
3. 从 `ContainerPool` 取一个热容器，或创建新容器。
4. 调用容器 `/run`。
5. 将容器 port 放入返回结果，用于后续 repair fast-path。

代码中原本有 “fast-path 时 reserve container” 的逻辑，但当前被注释掉；容器执行后会回到 pool。不过容器内 `Runner` 按 transaction id 保存上下文，因此只要容器未被销毁，repair 仍能通过记录的 port 找到对应上下文。

### 4.5 容器侧 sidecar 与 Store API

容器入口在 [src/container/proxy.py](src/container/proxy.py)，用户函数通过 [src/container/Store.py](src/container/Store.py) 暴露的 `store` API 访问状态。

`Runner.init` 会：

- 初始化本地 Redis shadow table、Redis cache、DynamoDB/Scylla API。
- 编译用户 `main.py`。
- 创建全局 `store` 对象。

用户函数写法类似：

```python
def main():
    func_input = store.fetch_input()
    value = store.get(key)
    store.put(key, new_value)
    store.ret({"result": value})
```

`Store` 的语义：

- `fetch_input()`：从上游函数的 `RET` key 读取 workflow 参数。
- `get(key)` 首次执行：
  - 如果当前事务 `write_set` 已有该 key，说明是 read-your-writes，从 upstream 函数 shadow table 读，并记录 RYW。
  - 否则从 Redis cache 读；cache miss 时从全局 DB 读，并记录版本到 read set。
- `put(key, value)`：写入本函数 shadow table，更新 `write_set[key] = function_name`。
- `ret(output)`：把函数输出写到 shadow table 的 `RET` key，供下游函数读取。
- `abort_tx(message)`：抛异常，容器侧将其转为 abort。

Repair 时，`Store.get` 的优先级变成：

1. `RYW_keys`：从同事务上游函数的 shadow table 读。
2. `upstream_keys`：从预取的 `UPSTREAM` key 读。
3. 其他 key：从 cache/global DB 读。

### 4.6 Transaction Sink

入口在 [src/transaction_sink/proxy.py](src/transaction_sink/proxy.py)，核心逻辑在 [src/transaction_sink/validate_struct.py](src/transaction_sink/validate_struct.py)。

Transaction sink 是 workflow 的批处理和 repair 状态管理器：

- `/validate` 接收 WorkerSP 上报的事务 read/write set。
- `TransactionSink.validate_batch_check` 根据 `BATCH_SIZE` 和 `BATCH_TIMEOUT` 形成 batch。
- `process_batch` 注册 batch state，并发送给 validator。
- `/fin_repair` 和 `/abort` 接收函数或 WorkerSP 的 repair 完成/abort 通知。
- `RepairingBatchState` 维护乐观 repair 状态、悲观 ready 条件、abort 后的 cascading fallback。

论文中的 PBD/PTD 概念主要体现在：

- `PessimisticBatchState.next_txs_after_batch`
- `PessimisticBatchState.pessi_transaction_info`
- `PessimisticBatchState.pessimistic_repair_ready`
- `OptimisticTransactionState.need_pessimistic_repair`

### 4.7 Validator / Commit Manager

入口在 [src/commit_manager/proxy.py](src/commit_manager/proxy.py)。

每个 workflow 有一个 `ValidatorPool`：

- 多个 `ValidatorProcess` 并行处理 batch。
- 一个 `SerializerProcess` 串行维护全局 key version 和 writers list。
- ValidatorProcess 与 SerializerProcess 通过 multiprocessing queue 通信。

关键职责：

- validation：检查 stale read，构建依赖图。
- repair metadata：把依赖图转成每个函数的 metadata。
- repair triggering：触发乐观或悲观 repair。
- commit：按 writers list 顺序提交 batch，并处理 cascaded commit。
- gateway notify：commit 后通知 gateway，使客户端返回。

`SerializerProcess.key_writers` 是严格串行化的关键。commit 前，batch 必须成为每个写 key 的 writers list 头部。提交后移除自己的 writer entry，可能使后续 suspended batch 变为 ready。

### 4.8 存储层

仓库使用三类存储：

- CouchDB：workflow metadata、结果库、日志库等。
- ScyllaDB Alternator/DynamoDB API：全局 `data` 表，记录 key/value/version。
- Redis：
  - `6379` / db 0：shadow table 和 repair sidecar 状态。
  - `6380` / db 1：cache。

脚本 [scripts/db_setup.sh](scripts/db_setup.sh) 启动 ScyllaDB 和 CouchDB，然后执行 [scripts/db_starter.py](scripts/db_starter.py) 初始化 CouchDB 数据库和全局 `data` 表。worker 侧 [scripts/worker_setup.sh](scripts/worker_setup.sh) 启动两个 Redis 容器并构建函数镜像。

## 5. 一次事务的端到端生命周期

### 5.1 初始化

1. `scripts/db_setup.sh` 启动 storage backend，并创建 CouchDB 的 `common/results/log/workflow_latency` 等库。
2. app-specific `DB_setup.py` 写入初始业务数据。
3. `src/initializer/initialize.py` 解析 workflow YAML，将函数信息、start/end function、worker 地址写入 CouchDB。
4. `scripts/worker_setup.sh` 在每个 worker 上启动 Redis、构建 `workflow_base` 和各函数镜像。
5. 启动 gateway、WorkerSP proxy、transaction sink、validator proxy。

### 5.2 首次执行

1. 客户端向 gateway `/run` 发请求。
2. Gateway 生成或接收 `transaction_id`，把 start function 的输入写到对应 worker Redis。
3. Gateway 调用 start function 所在 worker 的 `/request`。
4. WorkerSP 根据 workflow DAG 触发函数。
5. 容器执行用户 `main.py`，通过 `store.get/put/ret` 访问状态。
6. 每个函数返回 read set、write set、RYW、io latency 和容器 port。
7. WorkerSP 合并事务状态；当 DAG 到达 `END`，将 read/write set 发送到 transaction sink `/validate`。

### 5.3 批量验证

1. Transaction sink 形成 batch，发送到 validator `/validate`。
2. ValidatorProcess 将 batch 交给 SerializerProcess。
3. SerializerProcess 按 batch 内事务顺序检查 read set：
   - 构造 `expired_set`
   - 构造跨事务依赖 `subjection_set`
   - 构造悲观 repair 的 sink metadata
   - 更新 writers list
4. ValidatorProcess 用 `RepairInfo` 合并 RYW 信息并构造函数级 repair metadata。

### 5.4 Repair

1. RepairEngine 先调用各 worker `/prepare`：
   - 更新过期 cache key。
   - 将 repair metadata 写入本地 Redis。
2. RepairEngine 触发需要 repair 的事务 start functions：
   - fast-path 开启时走 worker `/repair`，再由 worker 调对应容器 `/run`。
   - fast-path 关闭时走普通 `/request`。
3. 容器 `Runner` 加载 metadata，检查 `parent_cnt + subjection_waiting_cnt`。
4. dirty 函数重跑；clean 函数跳过执行但设置为 repaired 并触发下游。
5. 如果 upstream 还没 repaired，下游会把自己登记到 upstream 的 SUCCESSOR list，等待 upstream push 数据。
6. 函数 repair 完成后通知 sink `/fin_repair`；若用户逻辑 abort，则通知 sink `/abort`。

### 5.5 Commit 与返回

1. Sink 发现 batch 内事务都 resolved 后，通知 validator `/fin_repair`。
2. Validator 根据成功事务和 abort 情况计算最终 commit keys。
3. SerializerProcess 判断 batch 是否可提交：
   - 如果该 batch 对每个写 key 都是 writers list 头部，则提交。
   - 否则挂起，等待前序 batch 提交后 cascaded commit。
4. Validator 调 worker `/commit`，worker 从 Redis shadow table 取值写入全局 DB，并更新 cache。
5. Validator 通知 gateway `/notify`。
6. Gateway 读取 end function output，返回客户端。

## 6. Benchmark 与实验

仓库包含三类 benchmark：

- `benchmark/micro_benchmark`：链式 `c2/c4/c8/c16` 和 fan-out `w2/w4/w6/w8/w16` workflow，数据访问服从 Zipf 分布。
- `benchmark/travel_reservation`：航班、租车、支付、确认。
- `benchmark/banking_system`：登录、扣款、入账。
- `benchmark/social_network`：登录、并发评论、发布、修改 timeline。

论文评估设置：

- 8 节点集群：1 个 storage node + 7 个 worker node。
- 每个函数容器限制 1 CPU core、256 MB memory。
- Storage backend 使用 ScyllaDB，强一致读。
- Baseline 包括 Beldi、Concord、OCC。

论文报告的主要结果：

- 实际应用上，FaaSRep 对 Concord 的平均吞吐提升为 2.03 到 3.69 倍，对 Beldi 为 4.55 到 9.01 倍。
- 微基准中，随着事务长度、fan-out 或并发增加，FaaSRep 相对 2PL/OCC 类系统优势更明显。
- 缓存 miss rate 增大只造成有限吞吐下降，说明主要收益来自 repair 而不只是 cache。
- batch size 从 1 到 2 收益最大，继续增大边际收益递减。
- 消融实验中，cache+batch、repair、函数级 repair、fast-path 都有增益；fast-path 额外带来约 12.7% 到 39.6% 吞吐提升。
- p99 完成轮数在实际应用中被 FaaSRep 限制到 3 轮，而 baseline 可达到 9 或 14 轮。
- validator 8 processors 时峰值约 2078.47 rps。
- FaaSRep 相比 OCC 的模块 CPU/内存开销较小，带宽增幅相对值较高但绝对值不大。

仓库中已有结果文件能对上部分论文数据，例如：

- [experiment/microbenchmark/test1_latency_throughput/results/Repair/summary_results.csv](experiment/microbenchmark/test1_latency_throughput/results/Repair/summary_results.csv) 记录 repair 模式下不同 workflow/client 的吞吐和延迟。
- [experiment/actual_apps/test7_colocate_apps/results/repair_summary_single.csv](experiment/actual_apps/test7_colocate_apps/results/repair_summary_single.csv) 记录实际应用单独部署的 repair 结果。
- [experiment/actual_apps/test10_validator_scalability/results/validator_scalability_summary_w8.csv](experiment/actual_apps/test10_validator_scalability/results/validator_scalability_summary_w8.csv) 包含 8 validator workers、64 clients 时约 2078.47 txns/s 的结果。

## 7. 当前实现与论文描述的差异

### 7.1 Fast-path 不是 gRPC

论文第 7 节说 fast-path 使用 gRPC，sidecar 同时作为 gRPC server/client。当前代码中容器侧是 Flask HTTP server，fast-path 通过 HTTP POST 容器 `/run` 加 Redis 传递依赖数据实现。

这不一定影响论文机制的原型验证，但它意味着当前仓库的延迟和 CPU profile 与论文描述的 gRPC/RDMA 扩展版本不完全一致。

### 7.2 容器 reserve pool 逻辑被弱化

论文说首次执行后 workflow engine 会将使用过的容器放入 reserving pool，防止被回收。代码中 [src/function_manager/function.py](src/function_manager/function.py) 里 “fast path reserve container” 的逻辑被注释掉，容器执行后直接回到 pool。

当前能工作依赖两个条件：

- 容器不会在 repair 前被销毁。
- `Runner.tx_contexts` 按 transaction id 保存上下文，因此同一个容器可以持有多个事务上下文。

这是一种比论文 reserve pool 更松的实现，对实验环境通常可行，但在高 churn 或 aggressive cleanup 下可能影响 fast-path repair。

### 7.3 主动 abort 在应用代码中多处被注释

论文把 proactive abort 作为关键挑战。当前实际应用 benchmark 中，多处业务 abort 条件被注释：

- travel `reserve_flight` 中航班满员 abort 被注释。
- banking `banking_login` 和 `withdraw` 中密码错误、余额不足 abort 被注释。
- social `social_login` 中密码错误 abort 被注释。

与此同时，transaction sink 中有 `ABORT_PROB`，会在 repair 完成处理中随机把事务状态变成 aborted，用于模拟 abort/fallback 路径。这说明当前实验对主动 abort 的覆盖更像合成注入，而不是完全来自业务逻辑。

### 7.4 benchmark 中存在非确定性逻辑

论文第 7.3 节明确指出 FaaSRep 依赖确定性：同样输入下 repair 重跑必须产生相同读写集合和执行路径。但仓库 benchmark 中存在非确定性：

- social comment 使用 `datetime.datetime.now()`。
- social publish 使用 `random.choices()` 和当前时间。
- travel payment 使用 Python `hash()` 生成 payment id；Python hash 默认受进程随机种子影响。
- textseq 使用 `random.shuffle()` 和随机 abort。

如果这些函数在 repair 中被标记 dirty 并重跑，输出可能不同。这与 FaaSRep 的严格前提有冲突。实验若要作为严格正确性证明，应将这类非确定性移出事务边界，或把随机种子/时间作为输入记录下来，使 repair 可重放。

### 7.5 当前配置 batch size 为 1

论文强调 batching 对性能和 writers list 顺序有帮助。当前 [config/config.py](config/config.py) 中 `BATCH_SIZE = 1`。即使 batch size 为 1，validator 的 `key_writers` 仍可跨未提交 batch 建立依赖；但论文中 batch 内事务顺序、batch size 敏感性分析等，需要通过实验脚本调整配置复现。

### 7.6 `CACHE_ENABLED` 基本不是核心开关

配置中有 `CACHE_ENABLED`，但核心读路径直接使用 Redis cache 和 `EXPIRED_CACHE/FILLUP_CACHE` 等配置。实际 cache 行为主要由 `RedisCache.cache_get`、`Repository.fillup_cache` 和 `/prepare` 的 expired key 更新决定，而不是由 `CACHE_ENABLED` 统一控制。

### 7.7 ScyllaDB 通过 DynamoDB API 使用

论文说 global storage 使用 ScyllaDB。代码里所有全局 DB 操作通过 `boto3.resource('dynamodb', endpoint_url=...)` 访问。脚本启动的是 ScyllaDB Alternator 兼容 DynamoDB API。因此代码层面看是 DynamoDB API，部署层面是 ScyllaDB。

## 8. 正确性直觉

FaaSRep 保证 strict serializability 的直觉是：

1. Validator 串行处理事务 read/write set，形成一个明确的验证顺序。
2. 每个 key 的 writers list 记录未提交写者，读者如果读到旧值或前方未提交 writer，就在 repair metadata 中获得依赖。
3. Repair 时，dirty 函数等待 upstream repaired，并从 upstream shadow table 读取正确值，因此 repair 后读结果与某个串行顺序一致。
4. Commit 时 batch 必须成为所有写 key 的 writers list 头部；这防止后来的 batch 越过前序 writer。
5. 如果主动 abort 破坏乐观依赖，则下游切换到悲观 repair；悲观 repair 等待足够前序状态 resolved 后再重新构造依赖。
6. Gateway 只有在 validator commit/abort 通知后才返回结果，因此 real-time order 能通过 batch commit 和后续 validation snapshot 体现。

代码中最重要的正确性状态集中在：

- `SerializerProcess.key_version_table`
- `SerializerProcess.key_writers`
- `RepairingBatchState.optimistic_state_per_transaction`
- `RepairingBatchState.pessimistic_state_per_batch`
- Redis 中的 `STATE`、`SUCCESSOR`、`UPSTREAM`、`REPAIR_*` key

## 9. 重要数据 key 命名

Redis shadow table 中常见 key：

- `{txid}:RET:{func}:{key}`：函数输出，供 DAG 下游或 gateway 读。
- `{txid}:PUT:{func}:{key}`：函数写入的事务私有 shadow value。
- `{txid}:UPSTREAM:{func}:{key}`：repair 时 upstream 推给下游函数的正确版本。
- `{txid}:STATE:{func}`：repair 状态，值为 RUNNING / REPAIRED / ABORTED。
- `{txid}:SUCCESSOR:{func}:INFO`：等待该函数的下游 repair 函数列表。
- `{txid}:SUCCESSOR:{func}:KEYS:{downstream_txid}:{downstream_func}`：下游等待的 key 列表。
- `{txid}:REPAIR_{mode}:{func}:`：validator 预置的函数级 repair metadata。

这些 key 分布在 worker 的 Redis db 0。全局 cache key 则直接使用业务 key，value 是 `{value, version}` JSON。

## 10. 部署和复现实验的理解

典型部署顺序应为：

1. 在 storage node 执行 `scripts/db_setup.sh <workflow>` 或 `scripts/db_setup.sh app`。
2. 在每个 worker 执行 `scripts/worker_setup.sh <workflow>`。
3. 启动 validator proxy：`python src/commit_manager/proxy.py <storage_ip> 9000`。
4. 启动 gateway：`python src/gateway/gateway.py <storage_ip> 8000`。
5. 在 worker 启动 WorkerSP proxy：`python src/workflow_manager/proxy.py <worker_ip> 7500`。
6. 在 end function 所在节点启动 transaction sink：`python src/transaction_sink/proxy.py <worker_ip> 6000`。
7. 使用 `experiment/.../run.py` 生成 workload 并向 gateway 发请求。

实际脚本中没有看到一个完整的一键启动所有服务的编排器，因此复现实验通常需要多节点手动启动或外部脚本配合。

## 11. 我对系统设计的判断

FaaSRep 的核心贡献很清楚：它把冲突处理从 “发现冲突后丢掉已有执行” 改为 “保留首次执行暴露出的依赖信息，并按依赖有组织地修复”。这对 serverless workflow 很合适，因为 workflow 本身已经是 DAG，函数级状态天然可以拆开；很多冲突只影响局部函数，没必要整笔事务重跑。

代码中最有价值的实现点是：

- 在 validator 的 stale-read check 中延迟构建依赖图，避免首次执行时跨节点协调。
- 将 RYW 关系和跨事务依赖合并，避免同事务内部写被错误解释成跨事务依赖。
- 在 repair metadata 中保留 `successor_port`，使容器间直接触发成为可能。
- 使用 `key_writers` 同时承担依赖构建和 commit 排序。
- sink 侧保存乐观状态和悲观 ready 条件，把主动 abort 的复杂性从容器侧隔离出来。

当前实现里最需要小心的是：

- benchmark 非确定性与论文确定性假设冲突。
- 主动 abort 的业务路径多处注释，真实 proactive abort 覆盖可能不足。
- fast-path 与论文描述不同，若要写论文 artifact 文档，需要明确当前是 HTTP/Redis fast-path。
- 容器不 reserve 会降低实现与论文设计的一致性，需要确认高负载下容器回收不会破坏 repair context。
- 当前配置只启用 social_network，切换 workload 需要修改 `WORKFLOW_YAML_ADDR` 并重新初始化 CouchDB metadata。

## 12. 适合后续深入的方向

如果继续改进这个项目，我建议优先做以下几件事：

1. 将 benchmark 中的非确定性操作改成可重放输入，例如把 timestamp/random seed/payment id 由 gateway 参数传入。
2. 恢复或重新设计业务级 proactive abort，而不是主要依赖 `ABORT_PROB` 注入。
3. 明确 fast-path 实现口径：要么补 gRPC sidecar，要么在文档中承认当前 artifact 是 HTTP/Redis 版本。
4. 为 `FAST_PATH`、`OPTIMISTIC_REPAIR`、`BATCH_SIZE`、cache 开关建立统一实验配置脚本，避免手改 config。
5. 增加一个单机 smoke test，用小 workflow 启动 Redis/Scylla/CouchDB 后验证一次完整事务、一次 optimistic repair、一次 pessimistic fallback。
6. 为 validator 的 `key_writers`、`PessimisticRepairer`、RYW merge 写纯单元测试；这些是正确性的核心，最适合用确定性小例子覆盖。

