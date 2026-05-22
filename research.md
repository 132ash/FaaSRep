# FaaSnap 项目研究报告

## 1. 项目定位

FaaSnap 是一个面向 Serverless 事务型工作流的原型系统。README 对它的概括是："A serverless engine for efficient transactional workflow with snapshot-level isolation"。从代码来看，它试图解决的问题是：多个函数组成的工作流在并发运行时会读写共享数据，系统希望以接近 Serverless/FaaS 的函数执行方式提供事务语义，并通过批量验证、快照级隔离、乐观/悲观修复和容器复用降低开销。

这个仓库不是一个普通 Web 应用，而是一个分布式实验系统。它包含运行时组件、函数容器模板、数据初始化脚本、实际应用 benchmark、microbenchmark、实验驱动脚本和历史结果数据。核心运行时主要在 `src/` 下：

- `gateway/`：外部请求入口，启动工作流并等待验证/提交结果。
- `workflow_manager/`：每个 worker 节点上的调度代理，负责函数 DAG 调度、事务状态维护、repair 触发、提交写回。
- `function_manager/`：函数容器池和请求派发层。
- `container/`：被打包进每个函数镜像的容器代理与 `Store` API。
- `transaction_sink/`：工作流尾部所在节点的 batch sink，聚合事务读写集，按批发给 validator。
- `commit_manager/`：validator、serializer、repair engine 和悲观修复逻辑。
- `initializer/`：解析工作流 YAML，并将函数图、节点分配、元数据写入 CouchDB。

项目默认依赖 Docker、gevent、Flask、Redis、CouchDB、boto3/DynamoDB 兼容接口。脚本中实际使用 ScyllaDB Alternator 作为 DynamoDB 兼容后端，端口为 `4567`。

## 2. 数据与配置模型

系统使用三类存储：

- CouchDB：保存工作流元数据、函数信息、运行结果占位、日志和 latency 记录。
- DynamoDB/ScyllaDB：作为持久化的全局数据表 `data`，每个数据项包含 `key`、`value`、`version`。
- Redis：每个 worker 有一个 main Redis，作为 shadow table；另有一个 cache Redis，缓存 DynamoDB 数据及版本。

典型 key 模式如下：

- Shadow table 函数输出：`<txid>:RET:<func>:<key>`。
- Shadow table 数据写入：`<txid>:PUT:<func>:<key>`。
- Repair 上游数据：`<txid>:UPSTREAM:<func>:<key>`。
- Repair 状态：`<txid>:STATE:<func>`。
- Repair 等待者列表：`<txid>:SUCCESSOR:<func>:INFO` 和 `...:KEYS:<downstream_tx>:<downstream_func>`。

`config/config.py` 当前在工作区中处于删除状态，但 HEAD 中可以看到它定义了系统拓扑和模式开关，例如：

- `STORAGE_NODE_IP`、`GATEWAY_ADDR`、`VALIDATOR_ADDR`。
- `WORKFLOW_YAML_ADDR`：启用哪些工作流以及其 YAML 路径。
- `DEFAULT_CONTAINER_NUM`、`VALIDATORS_PER_POOL`、`BATCH_SIZE`、`BATCH_TIMEOUT`。
- `FAST_PATH`、`OPTIMISTIC_REPAIR`：决定修复路径。
- `CLEAR_MEM`、`FILLUP_CACHE`、`EXPIRED_CACHE`：控制内存清理和缓存行为。

`config/worker_info.yaml` 当前也被删除，但 HEAD 里有 3 个 worker IP。初始化阶段会把这些 IP 写入 CouchDB 的 `common` 数据库中，之后 gateway、validator、worker 都会读取它。

## 3. 工作流描述与初始化

工作流由两类 YAML 定义：

- `workflow.yaml`：定义函数 DAG、每个函数的输入输出以及后继节点。
- `function_info.yaml`：定义函数镜像名、函数名和最大容器数。

`src/initializer/parse_yaml.py` 会读取 `workflow.yaml`，构造 `component.workflow` 对象。它计算：

- `start_functions`：入度为 0 的函数。
- `end_function`：`next.type == FINISH` 的函数。
- `parent_cnt`：每个函数需要等待的 DAG 上游数量。
- `nodes`：函数名到函数对象的映射。

`assign_function.py` 负责把函数映射到 worker 节点。逻辑有两个分支：

- 函数数小于等于 worker 数时，尽量一函数一节点，并强制 end function 放在 sink 节点。
- 函数数大于 worker 数时，对函数做拓扑排序后分组，再把包含 end function 的组换到 sink 节点。

`initialize.py` 会对命令行传入的工作流逐个处理，并把以下内容写入 CouchDB：

- `<workflow>_function_info`：每个函数的 IP、parent count、输入输出、next。
- `<workflow>_workflow_metadata`：start functions、end function、所有 worker 地址。

这种初始化方式意味着运行时高度依赖 CouchDB 元数据，YAML 文件不是运行时实时读取的唯一来源。

## 4. 一次事务的完整路径

### 4.1 Gateway 接收请求

外部通过 `POST /run` 调用 `src/gateway/gateway.py`。请求包含：

- `workflow`：工作流名。
- `parameters`：起始函数输入。
- 可选 `transaction_id`。

Gateway 做以下事情：

1. 注册 transaction 到 `RunningTXTable`。
2. 懒加载 workflow metadata，包括 start functions、起点函数 IP 和所有 worker 地址。
3. 为事务创建 CouchDB `results` 文档。
4. 把起点函数输入写入起点函数所在 worker 的 Redis shadow table，key 为 `<txid>:RET:GLOBAL:<key>`。
5. 对所有 start functions 并发向对应 worker 的 `/request` 发请求。
6. 阻塞等待 `/notify` 被 validator 回调。
7. 从 end function 所在 worker 的 Redis shadow table 读取最终输出。
8. 可选调用所有 worker 的 `/clear` 清理该事务内存。

Gateway 本身不执行函数，不做验证，也不直接提交数据。它只负责启动、等待、收尾和返回 latency 分解。

### 4.2 WorkerSP 调度 DAG

worker 节点上的入口是 `src/workflow_manager/proxy.py`，监听 `7500`。启动时会：

- 清理已有 label 为 `workflow` 的 Docker 容器。
- 初始化本地 shadow table 和缓存。
- 为每个启用的 workflow 创建 `WorkerSPManager`。
- 每个 `WorkerSPManager` 创建 `FunctionManager`，后者会按函数信息建立容器池。

`/request` 收到函数触发请求后，会调用 `WorkerSPManager.get_state` 取出或创建 `TransactionState`。`TransactionState` 是一次事务在某个 worker 上的状态，包含：

- `read_set`、`write_set`。
- `container_port`：每个函数实际使用过的容器端口，repair 时可复用。
- `RYW_subjection`：同事务内读自己前面写的依赖关系。
- `parent_executed`：DAG 父节点完成计数。
- repair 相关字段：`repair`、`repair_mode`、`repair_states`、`repair_subjection_upcnt`。

当某个函数可运行时，WorkerSP 会：

1. 如果函数在本机，调用本地 `FunctionManager.run`。
2. 如果函数在远端，向远端 worker 的 `/request` 转发整个事务上下文。
3. 函数完成后更新 read/write set、container port 和 RYW 依赖。
4. 对 DAG 后继函数调用 `trigger_function`。
5. 如果后继是 `END`，第一次运行会把事务提交到 transaction sink 的 `/validate`。

WorkerSP 用 gevent 做并发，父节点计数和事务状态用 `BoundedSemaphore` 保护。

### 4.3 函数容器执行

函数容器的基础镜像来自 `src/container/Dockerfile`，它复制：

- `proxy.py`：容器内 HTTP 代理。
- `Store.py`：业务函数使用的读写 API。
- `redis_component.py`：Redis shadow table、cache、repair sidecar。
- `container_config.py`：容器内配置。
- `main.py`：业务函数代码，由具体 benchmark 镜像覆盖。

每个函数容器暴露内部 `5000`，外部由 `FunctionManager` 映射到本机端口。容器生命周期：

1. 创建后，worker 调用容器 `/init`，传入 workflow、function、sink、validator、node list、input/output schema、parent count、函数位置、模式开关等。
2. 容器编译 `/proxy/main.py` 中的业务代码。
3. worker 调用容器 `/run`，传入 transaction id、write set、repair 信息。
4. 容器构造一个新的 `Store` 对象并通过 `exec`/`eval('main()')` 执行业务函数。

业务函数代码直接使用全局 `store` 和 `function_name`。例如 banking 的 `withdraw` 会：

- `store.fetch_input()` 读取 DAG 输入。
- `store.get(src_balance_key)` 从 cache 或 shadow table 读取全局数据。
- `store.put(src_balance_key, new_balance)` 写入 shadow table。
- `store.ret(...)` 写入函数输出。

`Store.get` 的重要语义：

- 第一次执行时，如果 key 已在当前事务 `write_set` 中，则从上游函数的 `PUT` 读取，形成 RYW 依赖。
- 否则从 Redis cache 读取，cache miss 时由 `RedisCache` 到 DynamoDB 取 `(version, value)`，并把版本写入 read set。
- repair 执行时，会优先根据 repair metadata 从 RYW 或 upstream shadow table 读取。

`Store.put` 不直接写 DynamoDB，而是写入 Redis shadow table，并更新本事务 write set。真正提交发生在 validator 通知 worker `/commit` 后。

### 4.4 Transaction sink 批处理

每个 workflow 的 end function 所在 worker 同时承担 transaction sink，监听 `6000`。WorkerSP 在 DAG 走到 `END` 时向 sink `/validate` 提交：

- transaction id。
- read set。
- write set。
- container port。
- RYW subjection。

`TransactionSink` 使用 gevent queue 收集事务。达到 `BATCH_SIZE` 或超过 `BATCH_TIMEOUT` 后，它把事务转换为 batch：

- `batch_id` 默认取 batch 中第一个 transaction id。
- `transaction_list` 保存事务顺序。
- read/write set、RYW、container port 按 transaction id 组织。

sink 会先在本地 `RepairingBatchState` 注册 batch，再把 batch 发给 validator `/validate`。

sink 还维护 repair 完成状态，尤其在悲观修复中负责判断后继 batch/transaction 何时可修复。它同时记录 optimistic repair 的状态，若乐观修复中的上游事务 abort，会把下游事务转入 pessimistic repair。

## 5. 验证、序列化与提交

Validator 的入口是 `src/commit_manager/proxy.py`，监听 `9000`。它为每个启用 workflow 创建一个 `ValidatorPool`。每个 pool 包含：

- 多个 `ValidatorProcess`，数量由 `VALIDATORS_PER_POOL` 控制。
- 一个 `SerializerProcess`，负责全局按 key 维护版本和写者顺序。

### 5.1 ValidatorPool

`ValidatorPool.submit(batch_id, op, data)` 把任务放入队列。调度时按 `hash(batch_id) % num_validators` 将同一 batch 分配给固定 validator process。

### 5.2 SerializerProcess

Serializer 是项目中实现 snapshot-level isolation 的核心之一。它维护：

- `key_version_table`：当前已提交版本，启动时从 DynamoDB `data` 表扫描。
- `key_writers`：每个 key 的未提交写者队列，元素是 `(batch_id, tx_id, func)`。
- `batch_write_info`：每个 batch 写了哪些 key、写入是否 ready、batch 版本。
- `commit_suspended_batches`：因前置写者未提交而暂缓提交的 batch。

验证阶段 `accessed_set_validate` 会：

1. 为 batch 分配一个 timestamp 字符串作为版本。
2. 对每个 transaction 的 read set 判断是否过期。
3. 如果某个读 key 已有前序未提交 writer，则生成 cross-transaction subjection。
4. 更新 `key_writers`，记录本 batch 的写入。

提交阶段 `get_commitable_batches` 会：

1. 判断当前 batch 的所有写 key 是否 ready。
2. 如果不 ready，挂起。
3. 如果 ready，则出队 key writers，将最终写者写入 `commit_keys_on_worker`。
4. 更新 `key_version_table`。
5. 递归释放因此变 ready 的 cascaded batches。

也就是说，系统不是每个事务独立串行提交，而是按 key writer 队列和 batch 依赖做级联提交。

### 5.3 ValidatorProcess

收到 `VALIDATE` 时，validator 会：

1. 保存 batch 的 transaction list、read set、write set、container port。
2. 调 serializer 获取 expired keys、cross-tx subjection 和 pessimistic sink info。
3. 调 `RepairInfo.construct_repair_metadata` 构造 repair metadata。
4. 调 `RepairEngine.repair_batch_after_validate` 执行 repair。

收到 `REPAIR_FINISH` 时，validator 会：

1. 记录 abort 的事务。
2. 用 `PessimisticRepairer` 删除 abort 事务在 batch write table 中的写入。
3. 如果 batch 已全部修复完成，计算最终 commit keys。
4. 调 serializer 提交 ready batches。
5. 向各 worker `/commit` 发送需要落库的 Redis shadow table key。
6. 通知 gateway `/notify`，gateway 才会结束等待并返回客户端。

`commit_batch_list` 会把 commit key 按 writer 函数所在 worker 分组。worker 的 `/commit` 调用 `repo.commit_tx_writes`，从本机 Redis shadow table 读取 `<txid>:PUT:<func>:<key>`，写入 DynamoDB `data` 表，并更新 cache。

## 6. Repair 机制

FaaSnap 同时实现了 optimistic repair 和 pessimistic repair，模式开关由 `OPTIMISTIC_REPAIR` 和 `FAST_PATH` 决定。

### 6.1 Repair metadata

Repair metadata 主要描述每个事务、每个函数是否需要重跑，以及重跑时数据应该来自哪里：

- `dirty`：函数是否受到过期读或上游写影响，需要重新执行。
- `up_cnt`：需要等待的 cross-transaction 上游数。
- `upstream_keys`：key 到 `[upstream_txid, upstream_func]` 的映射。
- `RYW_keys`：同一事务内 key 到上游函数的映射。
- `successor_port`：fast path 下后继函数容器端口。

如果 `FAST_PATH=True`，repair metadata 按 worker IP 写入 Redis，容器自己读取并触发下游。如果 `FAST_PATH=False`，metadata 由 WorkerSP 在 `/request` 中传递。

### 6.2 Optimistic repair

乐观修复的基本想法是：对检测到依赖的事务先尝试局部重跑 dirty 函数，函数间或事务间通过 shadow table 的状态和 successor 列表传递数据。

在 WorkerSP 路径下：

- repair 前，`SubjectionCollector.fetch_upstream_keys` 会尝试读取上游事务函数的状态和数据。
- 如果上游仍 `RUNNING`，当前函数增加等待计数。
- 如果上游已 `REPAIRED`，把上游 PUT 数据写到当前事务的 `UPSTREAM` key。
- 上游函数修复完成后，会触发等待中的 downstream transaction/function。

在 fast path 容器路径下，类似逻辑由 `RepairSidecar` 在容器内执行，容器可以直接触发后继容器端口或通知 sink 修复完成。

### 6.3 Pessimistic repair

悲观修复用于处理乐观修复不适用或上游 abort 导致的级联依赖。`PessimisticRepairer` 为每个 batch 维护：

- `tx_write_table_per_batch`：每个 key 在 batch 中按事务顺序的 writer 列表。
- `tx_read_table_per_batch`：原 read set。
- `last_subjection_for_tx_per_batch`：某事务最后依赖的前序事务。
- `transaction_idx_per_batch`：事务在 batch 中的顺序。

准备悲观 repair 时，它根据当前 writer table 重新计算每个事务函数读 key 的依赖：

- 如果依赖来自已 abort 或不存在的 batch 内 writer，则 key 需要从缓存/数据库刷新。
- 如果依赖来自 batch 内仍有效的前序 writer，则产生 upstream dependency。
- 所有函数会被标记 dirty，确保悲观路径更保守。

如果某事务 abort，`modify_batch_write_table_for_abort` 会把该事务写过的 key 从 writer table 中删除。最终 `pessimistic_get_commit_keys` 只提交每个 key 最右侧非 abort 的 writer。

### 6.4 Sink 中的 repair 状态传播

`RepairingBatchState` 同时追踪 optimistic 和 pessimistic 的完成状态：

- `PessimisticBatchState` 管理 batch 间、事务间的前驱计数，只有前驱完成后后继事务才可悲观修复。
- `OptimisticTransactionState` 记录事务的乐观修复状态和下游依赖。如果乐观修复 abort，下游事务会被标记为需要悲观修复。

这种设计让系统能先用乐观修复减少重跑范围，在冲突无法安全消解时再切换到悲观修复。

## 7. 容器池与调度特性

每个函数由 `Function` 对象管理一个 `ContainerPool`。启动时，如果函数被分配到当前 host，会预热 `DEFAULT_CONTAINER_NUM` 个容器。请求到来后：

1. `send_request` 把请求放入函数队列，并等待 `AsyncResult`。
2. `FunctionManager` 每 `dispatch_interval=0.005` 秒扫描函数队列。
3. 从容器池尾部取最近使用的热容器。
4. 若没有可用容器，创建新容器，直到 `max_containers` 限制。
5. 请求完成后把容器放回池。

容器池中注释显示原先 fast path 可能会把容器 reserve 到事务池中，但当前代码把 reserve 逻辑注释掉了，执行后总是直接放回 pool。repair fast path 仍依赖 `container_port` 回到之前使用的容器端口，因此这部分实现值得额外核查：容器没有被事务独占时，复用端口是否总能对应到仍保存事务上下文的容器，是一个潜在敏感点。

## 8. Benchmark 与应用

仓库包含四类 benchmark。

### 8.1 Microbenchmark

`benchmark/micro_benchmark/` 下有 `c2/c4/c6/c8/c16` 和 `w2/w4/w6/w8/w16` 等配置。函数镜像统一使用 `micro_func`，业务逻辑在 `scripts/init/micro_benchmark/microbenchmark_func/main.py`：

- 输入包含 JSON 编码的 keys 和 payload size。
- 每个函数读取自己对应的 key 操作列表。
- `R` 调 `store.get`，`W` 调 `store.put` 写随机 payload。
- 函数把剩余 key 列表继续传给下游。

实验脚本支持测试：

- latency/throughput。
- read/write ratio。
- data skewness。
- cache effect。
- batch size。
- ablation study。

### 8.2 Banking system

`benchmark/banking_system` 是三段顺序工作流：

1. `banking_login`：读取账号密码，输出源账号、目标账号和金额。
2. `withdraw`：读取源账户余额并扣款。
3. `deposit`：读取目标账户余额并加款。

初始化脚本会生成账户密码和余额。代码中密码校验和余额不足 abort 逻辑被注释掉，因此当前 benchmark 更偏向事务读写冲突测试，而不是完整业务规则测试。

### 8.3 Travel reservation

`benchmark/travel_reservation` 是四段顺序工作流：

1. `reserve_flight`
2. `reserve_car_rental`
3. `payment`
4. `confirm_reservation`

初始化脚本写入 flight capacity 和日期范围内 car rental capacity。

### 8.4 Social network

`benchmark/social_network` 是一个有分叉和汇合的 DAG：

- `social_login` 后并行触发 3 个 `comment_post_*`。
- 三个 comment 分支汇入 `comment_user`。
- 后续 `publish`，最后 `modify_timeline`。

初始化脚本生成用户密码、启动帖子和 timeline。

## 9. 部署与运行方式

脚本分为数据库节点和 worker 节点两类。

### 9.1 数据库节点

`scripts/db_setup.sh` 会：

1. 停止并删除旧的 ScyllaDB/DynamoDB/CouchDB 容器。
2. 配置 AWS 本地凭证。
3. 启动 ScyllaDB，打开 Alternator 端口 `4567`。
4. 启动 CouchDB，端口 `5984`，账号密码 `faasnap/faasnap`。
5. 执行 `scripts/db_starter.py` 创建 CouchDB 数据库和 DynamoDB `data` 表。
6. 根据参数初始化 app 或 microbenchmark 数据集和 workflow metadata。

### 9.2 Worker 节点

`scripts/worker_setup.sh` 会：

1. 启动 Redis main：`6379`。
2. 启动 Redis cache：`6380`。
3. 构建 `workflow_base` 镜像。
4. 根据工作流构建函数镜像。

实际运行时还需要手动启动：

- gateway：`python3 src/gateway/gateway.py <ip> 8000`。
- validator proxy：`python3 src/commit_manager/proxy.py <ip> 9000`。
- worker proxy：`python3 src/workflow_manager/proxy.py <worker_ip> 7500`。
- transaction sink proxy：`python3 src/transaction_sink/proxy.py <worker_ip> 6000`。

代码注释中给出了若干启动命令示例，但 README 没有完整 runbook。

## 10. 实验脚本与结果

`experiment/` 下既有实验驱动脚本，也有大量 CSV 结果。

- `experiment/microbenchmark/test1_latency_throughput/run.py`：多进程客户端压测 microbenchmark，保存 raw results 和 summary。
- `test2_read_write_ratio`、`test3_data_skewness`、`test4_cache_effect_test`、`test5_different_batch_size`、`test6_ablation_study`：分别围绕读写比例、Zipf skew、缓存、batch size 和消融测试。
- `experiment/actual_apps/individual_app_test/run.py`：对实际应用跑多客户端、多轮测试，记录 e2e latency、throughput、repair rounds。
- `test7_colocate_apps`：包含 trace 驱动和 colocated apps 结果。
- `test8_latency_breakdown`、`test9_costs_and_overhead`、`test10_validator_scalability`：分别关注 repair latency breakdown、系统资源开销和 validator scalability。

脚本风格更像研究原型，路径和配置依赖较强，有些测试文件是临时 debug 或历史残留。

## 11. 代码中特别值得注意的实现细节

1. 全局 monkey patch：gateway、worker、sink、validator 和容器代理大量使用 `gevent.monkey.patch_all()`，I/O 并发模型依赖 gevent。

2. 多进程和 gevent 混用：validator pool 使用 `multiprocessing.Process`，每个进程内部再用 gevent。serializer 也是独立进程，通过 multiprocessing queue 与 validator process 通信。

3. 版本是字符串 timestamp：serializer 用 `datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')` 作为 batch 版本。字符串比较在该格式下通常等价于时间比较，但依赖时钟单调和格式固定。

4. Gateway 的 retry 逻辑基本被注释：`run()` 中 while 循环保留了 abort/retry 的轮廓，但 abort 后重新运行的逻辑被注释掉了。目前 abort 主要通过 notify 返回。

5. `workersp_repo.get_current_node_functions` 名称和实现不一致：注释说获取当前节点函数，但代码返回 workflow 中所有函数。这导致每个 worker manager 都知道所有函数，真正是否本地执行由 `function_pos` 比较判断。

6. `function_pos` 的形式在不同层略有差异：有时是 `ip:7500`，有时被 `extract_ip` 去掉端口。这些转换对 URL 组装非常关键。

7. Redis client decode 设置不统一：部分 shadow table 使用 `decode_responses=True`，部分没有。代码里有些地方手动 `.decode('utf-8')`，有些直接字符串比较。这在跨模块改动时容易踩坑。

8. 容器内业务代码通过 `exec` 动态执行：灵活但安全性弱，且依赖全局变量 `store`、`function_name` 注入。

9. 日志文件路径多为相对路径：例如 `../../logging/*.log`，实际启动工作目录不对时可能写到意外位置或失败。

10. 当前工作区缺失 `config/config.py`、`config/worker_info.yaml`、`config/redis-cache.conf`。代码运行强依赖这些文件，若删除不是有意为之，需要恢复或重新生成。

## 12. 潜在风险与维护建议

### 12.1 可运行性风险

当前仓库工作区显示几个配置文件被删除，运行时会直接 import `config` 或读取 `worker_info.yaml`。如果不恢复，初始化、gateway、worker、validator 和实验脚本都会失败。

README 过短，没有描述多节点启动顺序、必要端口、配置文件格式、模式开关含义。对新接手者来说，真正 runbook 需要从脚本和代码里反推。

### 12.2 一致性与并发风险

Serializer 是全局提交顺序和版本管理的核心，但它只存在于单个 validator proxy 进程树中。如果部署多个 validator proxy，需要额外保证 serializer 单例或外部一致性。

容器上下文与 fast path repair 之间存在潜在耦合。当前函数执行后容器直接回 pool，而 fast path repair 仍会按记录端口触发容器。如果同一容器端口已经执行过其他事务，上下文是否仍存在且未被清理，需要结合压测验证。

`RunningTXTable.notifyTX` 假设被通知的 tx_id 一定仍在 `running_txs` 中；如果 gateway 超时、重复通知或 abort 通知乱序，可能 KeyError。

### 12.3 代码组织风险

相同概念在多个层重复实现，例如 `SubjectionCollector` 和 `RepairSidecar` 都有 upstream fetch 和 successor 通知逻辑。两套逻辑需要同步维护，否则 fast path 和 non-fast-path 行为可能分叉。

配置常量同时存在于 `config/config.py` 和 `src/container/container_config.py`。容器配置中的 `STORAGE_NODE_IP` 是硬编码，容易和实际 `config` 不一致。

### 12.4 测试与验证建议

建议补充最小端到端测试矩阵：

- 单事务、无冲突、单函数/多函数 DAG。
- 两事务读写同 key，触发 expired read 和 optimistic repair。
- optimistic repair 上游 abort，触发 pessimistic repair。
- batch size > 1，跨 batch 同 key 写入，验证 cascaded commit。
- fast path on/off、optimistic on/off 的组合测试。

同时建议把当前 debug tests 中散落的脚本整理为可重复的 pytest 或 integration test harness。

## 13. 总结

FaaSnap 的核心设计可以概括为：

1. 工作流 first run 以 Serverless 函数容器执行，所有写入先进入 Redis shadow table。
2. 每个事务收集 read set、write set、RYW 依赖和函数容器端口。
3. End function 所在节点把事务按 batch 交给 validator。
4. Serializer 维护 key 版本和未提交 writer 队列，判断哪些读过期、哪些事务依赖前序写。
5. Repair engine 根据依赖生成 optimistic 或 pessimistic repair metadata，触发局部重跑。
6. 修复完成后，serializer 决定哪些 batch 可级联提交。
7. Worker 从 shadow table 写回 DynamoDB，gateway 收到通知后返回结果。

这个项目的特性很鲜明：它不是只做简单的函数编排，而是在 FaaS 执行模型上叠加事务隔离、批处理验证、repair 和容器热复用。代码中很多实现都服务于减少全量重跑和提交等待，例如 RYW 依赖收集、cross-transaction subjection、fast path repair metadata、pessimistic writer table 和 cascaded commit。整体上它更像论文原型系统：机制丰富、实验脚本完整，但文档、配置一致性和工程化测试还比较薄。
