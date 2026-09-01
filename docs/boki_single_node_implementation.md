# Boki-style 单节点基线实现设计

## 1. 目的与结论

本文档描述如何从当前 `boki` 分支（与 `OCC` 分支同一提交点）实现一个单节点、Boki-style 的悲观并发控制基线。当前阶段只做设计，不修改运行时代码。

目标实现采用：

- 单节点集中式锁服务；
- 严格两阶段锁（Strict 2PL），支持共享锁 `S` 和排他锁 `X`；
- Wait-Die 死锁预防：年轻事务遇到年老持锁者时 abort，年老事务遇到年轻持锁者时等待；
- 独立部署 transaction-private shadow service，执行阶段的写只暂存在该服务；
- commit 时先把该事务的 staged writes flush 到主数据库，成功后再释放锁；
- abort 时先丢弃该事务的 staged writes，再释放锁；
- 整个 workflow 结束时统一 commit/abort 和释放锁；
- 基于 `c4`、Zipf=0.9 和现有动态访问集 trace 做开放环实验。

建议在代码、结果目录和论文中把该实现命名为 **Boki-style-SN** 或 **Boki-inspired 2PL (single node)**，而不是直接写成完整的 Boki。它模拟的是 Boki/Beldi 类 workflow 的 2PL、Wait-Die 和冲突重试关键路径，不复现 Boki 的 LogBook、metalog、共享日志排序、索引订阅、缓存和故障恢复机制。

## 2. 当前实现与改造边界

当前 OCC 路径为：

1. `src/container/Store.py` 从本地缓存或数据库读，并记录版本；写入先进入 transaction-private shadow table。
2. `src/workflow_manager/workersp.py` 汇总每个函数的 read/write set。
3. workflow 到达 `END` 后，经 `src/transaction_sink` 批量发送给 `src/commit_manager`。
4. validator/serializer 校验版本和批内冲突，成功事务再把 shadow table 写入同步到主表。
5. gateway 收到 validator 通知；冲突事务清理状态并完整重试。

Boki-style-SN 的目标路径为：

1. gateway 为逻辑事务注册稳定的事务优先级和 attempt `term`。
2. 每次 `Store.get/put` 都先向集中式锁服务申请对应数据项的 `S/X` 锁。
3. `get` 获锁后先查询本事务在 shadow service 中的 staged value，未命中才 consistent-read 主数据库；共享数据缓存保持关闭。
4. `put` 获得 `X` 锁后只写 shadow service，不修改主数据库；同一事务重复写同一 key 时覆盖 staged value。
5. workflow 到达 `END` 后，先请求 shadow service flush 该事务的全部写，flush 成功后再请求锁服务释放全部锁，不再经过 transaction sink 和 validator。
6. Wait-Die 或应用异常触发 abort 时，先清除 shadow service 中该事务的写，再取消等待并释放全部锁；冲突 abort 可以用同一事务优先级和新 `term` 重试。

其中 workflow 参数和函数返回值（当前 `RET:*` 数据）不是共享应用数据。它们仍需保留 transaction-private 的传递通道，否则 `c4` 的 `f1 -> f2 -> f3 -> f4` 无法传递剩余 key 列表。第一版可继续使用现有 `shadow_table` 保存 RET，但必须按 `(txid, term)` 隔离。新 shadow service 只负责共享应用数据的 staged writes，避免把 workflow 参数传递和事务提交状态混在一起；后续如需合并，也必须使用独立 namespace。

## 3. 正确性模型

### 3.1 锁类型与兼容性

| 已持有锁 | 请求 `S` | 请求 `X` |
| --- | --- | --- |
| 无 | 立即授予 | 立即授予 |
| 其他事务的 `S` | 兼容 | 冲突 |
| 其他事务的 `X` | 冲突 | 冲突 |
| 本事务的 `S` | 重入成功 | 尝试升级 |
| 本事务的 `X` | 重入成功 | 重入成功 |

锁的所有者以逻辑事务 `txid` 为单位，而不是以函数或容器为单位。因此同一 workflow 跨函数访问同一 key 时仍属于同一持锁者。所有 `S/X` 锁一直持有到整个 workflow commit/abort，函数结束时不得提前 unlock。

锁升级 `S -> X` 只有在不存在其他事务的 reader 时才能立即成功；否则把其他 reader 视为冲突持锁者并执行 Wait-Die。这样可处理两个事务先读后写同一 key 的升级死锁。

### 3.2 事务优先级

优先级只由 client 随 `/run` 传入的不可变整数 `global_req_id` 决定：数值越小，事务越老。锁服务首次 `begin` 时把该值保存为内部 `birth_seq`，完整比较键为 `(global_req_id, txid)`。服务到达顺序、容器 wall-clock 和重试次数均不参与排序。

同一逻辑事务因 Wait-Die abort 后重试时必须保留原 `global_req_id`（及对应的 `birth_seq`），只增加 `term`。锁服务拒绝同一 `txid` 改变 `global_req_id`，也拒绝两个不同 `txid` 使用相同的全局序号。

`term` 标识执行轮次：首次为 0，每次冲突重试加 1。所有 begin、lock、shadow、unlock/abort、函数触发和 gateway notify 消息均携带 `(txid, term)`。锁服务和 shadow service 都拒绝旧 `term` 的迟到请求，从而防止上一轮容器在清理后继续写 staged data 或错误完成当前事务。

### 3.3 Wait-Die 决策

设请求事务为 `T`，与它冲突的持锁事务集合为 `H`：

```text
若 H 为空：grant
若 H 中存在比 T 更老的事务：abort T（年轻者 die）
否则：T 比所有冲突持锁者都老，进入等待队列（年老者 wait）
```

对多个共享 reader，不能只记录一个读锁 owner。写者请求 `X` 时，只要任一冲突 reader 比它老就 abort；仅当所有冲突 reader 都比它年轻时才等待。当前 `beldi` 分支中“发现已有读锁便直接返回成功”的实现没有完整 reader owner set，不能直接复制到集中式锁服务。

事务等待边只可能从老事务指向年轻事务，因此等待图中的 `birth_seq` 严格递增，不可能形成环。Wait-Die 负责死锁预防；超时只作为进程异常或实现 bug 的保护机制，不是正常的并发控制分支。等待超时应按被动 abort 处理，并记录独立的 `timeout_abort` 指标。

等待队列按事务优先级从老到年轻排序，并禁止新请求绕过已经排队的互斥请求，以避免连续共享读导致写者饥饿。若把排队请求作为 anti-barging 的阻塞条件，也要按上述优先级规则决定 wait/die，不能引入不受 Wait-Die 约束的新等待边。

### 3.4 Shadow service 与提交原子性

独立 shadow service 保存每个 attempt 尚未提交的最终写集：

```text
staged_writes[(txid, term)][key] = {
    value,
    writer_function,
    last_op_id
}
```

它不保存 before-image，也不在执行阶段修改主数据库。数据操作顺序为：

```text
get(key):
    acquire S(key)
    shadow.get(txid, term, key)
    HIT  -> 返回 staged value
    MISS -> consistent-read data[key]

put(key, value):
    acquire X(key)
    shadow.put(txid, term, key, value, op_id)
```

同一事务多次写同一 key 时，shadow service 保留最新值；`op_id` 保证容器 HTTP 重试不会产生重复副作用。由于事务对该 key 已持有 `X` 锁，其他事务在 commit 前无法读取它；本事务通过 shadow-first read 获得 RYW。

shadow service 为每个 `(txid, term)` 维护：

```text
ACTIVE -> FLUSHING -> FLUSHED -> COMPLETED
ACTIVE -> DISCARDING -> DISCARDED
```

提交与 abort 的固定顺序为：

- commit：shadow `flush(txid, term)` -> 确认全部 staged writes 已写入 DB -> lock service `unlock(all)` -> shadow `complete` 清理写集/保留短期 tombstone -> 通知 gateway；
- abort：shadow `discard(txid, term)` -> 确认 staged writes 已清除 -> lock service `abort/unlock(all)` -> 通知 gateway。

flush 期间锁必须继续持有。即使多个 key 是逐项写入 DB，其他事务也不能看到部分提交状态；只有全部 key flush 成功后才能解锁。主表 `version` 可在 flush 时统一写成 `(birth_seq, term)` 对应的提交版本，Boki-style 路径不依赖该 version 做 OCC 校验。

`flush` 必须幂等：第一次调用把写集冻结为不可修改快照并进入 `FLUSHING`；数据库写以 `(txid, term, key)` 对应的确定值反复执行是安全的；全部成功后保存 `FLUSHED` 状态。若响应丢失，协调方重试 `flush` 时直接继续未完成 key 或返回既有成功结果。`FLUSHED` 标记至少保留到锁释放得到确认，不能 flush 完就立即忘记事务。

如果 flush 在任何 DB 写入发生前失败，可以安全地将事务转为 discard + abort；一旦至少一个 key 已写入主表，就已经越过可普通 abort 的边界。此后遇到暂时性数据库错误，服务应在仍持锁的前提下重试剩余 key，不能清 shadow 后解锁。永久错误按 fatal transaction 处理：不解锁、暴露告警并由实验控制面停止该轮。若底层 DynamoDB-compatible 存储可靠支持覆盖全部 c4 写集的原子 `TransactWriteItems`，可用它缩小该失败窗口；不应在未验证 Scylla Alternator 兼容性的情况下假设它可用。

flush 成功后如果 lock `/unlock` 响应丢失，协调方只能以同一 `(txid, term)` 幂等重试 unlock，不能再发 discard/abort。unlock 已确认后，shadow `/complete` 失败只造成临时状态泄漏，不影响已经提交的数据；该清理可由后台任务重试。反之，在 abort 路径中，shadow `/discard` 必须先原子切换到 `DISCARDING`，从这一刻起拒绝该 term 的新 put，然后才能清值并联系锁服务 abort。

本单节点基线不评估 shadow/lock 服务进程崩溃恢复。若未来要覆盖 fail-stop，需要把事务状态、flush progress 和锁意图持久化；仅靠当前内存服务无法宣称 Boki 的故障恢复语义。

## 4. 集中式锁服务设计

### 4.1 进程与内存状态

保留 `src/commit_manager/proxy.py` 作为端口 9000 的服务入口，但不再构造 `ValidatorPool`。新增独立的 `LockManager`；数据库 flush 不属于锁服务职责。核心状态如下：

```text
lock_table[key]:
    writer: optional tx owner
    readers: {tx owner -> reentrant count}
    waiters: ordered queue of (txid, term, priority, mode, event, enqueue_time)

tx_table[txid]:
    birth_seq
    current_term
    state: ACTIVE | ABORTING | ABORTED | RELEASING | RELEASED
    held_locks: {key -> mode/count}
    waiting_requests
    metrics
```

所有状态变更由一个很短的 `gevent` mutex 保护。阻塞等待时必须先释放全局 mutex，再等待该请求自己的 event；unlock/abort 在持 mutex 时修改状态并唤醒可运行 waiter，不能在全局 mutex 内发数据库或 HTTP 请求。

### 4.2 HTTP 接口

建议接口如下：

| 接口 | 主要字段 | 返回/作用 |
| --- | --- | --- |
| `POST /begin` | `txid`、`global_req_id`、可选 `term` | 保存 client 指定的全局优先级；重试校验并复用原优先级 |
| `POST /lock` | `txid`, `term`, `birth_seq`, `key`, `mode`, `op_id` | `GRANTED`、`ABORT`、`STALE`；允许长轮询等待 |
| `POST /unlock` | `txid`, `term`, `all=true` | commit flush 成功后的幂等统一放锁，并返回事务锁指标 |
| `POST /abort` | `txid`, `term`, `abort_type` | 取消全部 waiter、拒绝该 term 的新 lock、释放已持锁并返回指标 |
| `GET /debug/tx/<txid>` | 无 | 仅测试时查看状态和持锁集合 |
| `GET /health` | 无 | 启动检查 |

`op_id` 用于 HTTP 重试幂等。同一个 `(txid, term, op_id)` 重发不能重复入队或重复增加重入计数。`unlock(all)` 和 `abort` 也必须幂等，以容忍 WorkerSP 和 gateway 的重复通知。虽然保留 `/unlock` 命名，正常运行时不允许函数按 key 提前释放；严格 2PL 只在事务结束时使用 `all=true`。

### 4.3 唤醒规则

每次 unlock/abort 后只重新检查受影响 key 的 waiter：

1. 删除已 stale、已 abort 的 waiter。
2. 从最老 waiter 开始检查兼容性。
3. 可授予的 `X` 一次授予一个；可兼容的连续 `S` 可批量授予。
4. 授锁状态写入 `lock_table` 和 `tx_table.held_locks` 后，再设置 waiter event。

为了避免 HTTP 断连留下幽灵 waiter，等待请求需要服务端 deadline；客户端断连或 deadline 到期时调用统一的事务 abort 路径，而不是只删除当前 waiter 后继续事务。

### 4.4 独立 Shadow service

新增 `src/shadow_service`，作为与 lock manager 分离的常驻进程，建议使用独立端口 9100。第一版面向单节点性能实验，可用进程内字典保存 staged writes；它不提供进程崩溃后的持久恢复。服务自身使用细粒度 transaction mutex 或单个短临界区保护状态，实际 DB flush 不应持有全局 mutex。

建议接口如下：

| 接口 | 主要字段 | 返回/作用 |
| --- | --- | --- |
| `POST /begin` | `txid`, `term`, `birth_seq` | 建立当前 attempt；推进 term 前要求旧 term 已 discarded/completed |
| `POST /put` | `txid`, `term`, `key`, `value`, `function`, `op_id` | 幂等覆盖该 key 的 staged value |
| `POST /get` | `txid`, `term`, `key` | 返回 `HIT(value)`、`MISS` 或 `STALE` |
| `POST /flush` | `txid`, `term`, `flush_id` | 冻结写集、幂等写入 DB，全部完成后返回 `FLUSHED` 和指标 |
| `POST /discard` | `txid`, `term`, `reason` | 幂等清除未提交写并标记 `DISCARDED` |
| `POST /complete` | `txid`, `term` | 锁释放确认后回收 value，短期保留完成 tombstone |
| `GET /debug/tx/<txid>` | 无 | 测试时查看 staged keys、状态和 flush progress |
| `GET /health` | 无 | 启动检查 |

服务必须在 `FLUSHING/FLUSHED/DISCARDING/DISCARDED` 状态拒绝新的 `put`。`get` 只允许当前 ACTIVE term；commit 已开始后 workflow 理论上已经没有数据操作，出现此类请求应记录为协议错误。value 大小在 c4 中为 4 KiB，应同时记录当前 staged bytes、峰值 staged bytes、put/get QPS、flush keys 和 flush latency，以判断该服务是否成为额外瓶颈。

shadow service 不主动联系锁服务。事务协调方负责严格执行 shadow -> lock 的顺序，避免两个服务互相 RPC 导致循环依赖。第一版由到达 `END` 的 WorkerSP 充当 commit coordinator；gateway 仍负责重试和最终客户端响应。

## 5. 事务生命周期与调用链

### 5.1 正常提交

```text
trace client
  -> gateway /run(global_req_id)
     -> lock service /begin(global_req_id) => birth_seq=global_req_id, term=0
     -> shadow service /begin(txid, term, birth_seq)
     -> WorkerSP/container 执行 c4
        -> Store.get(key): /lock S -> shadow /get -> HIT 或 consistent DB read
        -> Store.put(key): /lock X -> shadow /put（主 DB 不变）
     -> WorkerSP 到达 END
        -> shadow service /flush（锁继续持有）
        -> lock service /unlock(all=true)
        -> shadow service /complete
        -> gateway /notify(txid, term, committed, lock metrics)
     -> gateway 读取最终 RET 输出并清理 transaction-private 数据
```

到达 `END` 即替代当前的 `validate_tx()`。不存在 batch validation 和 OCC 版本校验；commit 的数据阶段就是独立 shadow service 执行的 flush。只有 `flush` 明确返回 `FLUSHED` 后才能请求锁服务放锁。

两个 `/begin` 也需要补偿：若 lock begin 成功而 shadow begin 失败，gateway 必须调用 lock `/abort`，不能启动 workflow。abort 路径若暂时无法确认 shadow `/discard`，也必须继续持锁并重试 discard，不能为了恢复吞吐而先解锁。

### 5.2 Wait-Die 被动 abort 与重试

```text
Store.get/put -> /lock 返回 ABORT
  -> 容器抛出 PassiveAbortException
  -> WorkerSP 使当前 state 失效
  -> shadow /discard(txid, term, WAIT_DIE)
  -> lock /abort(txid, term, WAIT_DIE)：取消 waiter、解锁
  -> gateway 收到带 term 的 PASSIVE abort
  -> term += 1，保留 global_req_id/birth_seq，可配置短退避
  -> 清理上一 term 的 RET/函数状态
  -> lock /begin(txid, new term)；shadow /begin(txid, new term)
  -> 从 workflow 起点完整重试
```

短退避建议做成配置项，并记录实际值。默认可先沿用 `beldi` 分支的 200 ms 作为实验参数，但必须做一次敏感性检查，因为固定 200 ms 会显著影响高负载 tail latency 和吞吐。

### 5.3 应用主动 abort

`store.abort_tx()` 抛出 `ActiveAbortException`。后续同样先执行 shadow `/discard`，再执行 lock `/abort`，但 gateway 不重试，返回 `status=aborted`。普通代码异常应使用单独的 `ERROR` 类型：先保证 staged writes 清除并解锁，再把请求记为失败，不能默认为可无限重试的锁冲突。

### 5.4 迟到消息

gateway、WorkerSP、容器、锁服务和 shadow service 均检查 `term`：

- `term < current_term`：直接返回 `STALE`，不得读写主表；
- `term > current_term`：只有两个服务各自的 `/begin` 可以推进 term；普通 lock/shadow/unlock 请求报协议错误；
- lock term 已进入 `ABORTING/ABORTED/RELEASING/RELEASED`，或 shadow term 已进入 `FLUSHING/FLUSHED/DISCARDING/DISCARDED`：拒绝新的数据操作。

即便同一事务已经持有某个 key，每次 `Store.get/put` 仍需经过 `/lock` 的重入检查，不能只靠容器本地 held-key cache 跳过服务端状态检查，否则旧函数可能在全局 abort 后继续写。

## 6. 各模块预计改造

| 模块 | 设计修改 |
| --- | --- |
| `src/commit_manager/proxy.py` | 从 `/validate` 服务改为 `/begin`、`/lock`、`/unlock`、`/abort` 和健康检查 |
| `src/commit_manager/lock_manager.py`（新增） | 锁表、事务表、Wait-Die、等待队列、幂等和指标 |
| `src/commit_manager/validator.py`、`serializer.py`、`validator_repo.py` | 不再由启动入口引用；先保留文件便于与 OCC 对照，稳定后再决定删除 |
| `src/shadow_service/proxy.py`（新增） | 独立服务入口，提供 `/begin`、`/put`、`/get`、`/flush`、`/discard`、`/complete` |
| `src/shadow_service/shadow_store.py`（新增） | 按 `(txid, term)` 保存 staged writes、状态机、幂等 op、flush progress 和指标 |
| `src/shadow_service/db_repo.py`（新增） | 只在 flush 阶段把冻结写集写入 data 表，不参与锁管理 |
| `src/transaction_sink/*` | Boki-style 运行时不启动，WorkerSP 不再向端口 6000 发送 validation |
| `src/container/Store.py` | `get` 申请 `S` 后 shadow-first read；`put` 申请 `X` 后调用 shadow `/put`；不在执行阶段修改 data 表 |
| `src/container/shadow_client.py`（新增） | 封装 shadow service 的 get/put 和 `term/op_id` 协议；容器不直接执行 flush/discard |
| `src/container/proxy.py` | 保存并向 Store 传递 `birth_seq/term`；区分 ACTIVE、PASSIVE、ERROR abort；返回 lock/shadow/DB 指标 |
| `src/container/redis_component.py` | 共享应用数据 cache 关闭；现有组件只负责按 `(txid, term)` 隔离的输入和 RET 数据 |
| `src/function_manager/function.py`、`function_manager.py` | 容器请求增加 `birth_seq/term`，响应透传 abort 类型和锁指标 |
| `src/workflow_manager/workersp.py` | TransactionState 改存 `birth_seq/term`，去掉 OCC read set；END 按 shadow flush -> lock unlock 提交；abort 按 shadow discard -> lock abort |
| `src/workflow_manager/proxy.py` | `/request` 接收 `birth_seq/term`；删除 `/commit` 的 validator 回调路径；`/clear` 按 term 清理 |
| `src/gateway/gateway.py` | 在 lock/shadow 两个服务 begin、稳定优先级、term 重试、按 abort 类型决策、读取提交指标；通知必须校验 term |
| `src/gateway/transaction_info.py` | 状态中加入 `birth_seq/term/abort_type/retry_count`，忽略旧 term 通知 |
| `config/config.py`、`src/container/container_config.py` | 增加 lock/shadow service 地址、等待 deadline、flush retry、retry backoff 和 `SYSTEM_MODE=BOKI_SN`；强制共享数据 cache 关闭 |
| 启动/清理脚本 | 启停独立 shadow service；实验开始前重建 data，并清空 shadow/RET/锁服务内存 |

`write_set` 不再需要跨函数传递来定位某个函数的私有 PUT。所有 staged writes 都由 shadow service 按 `(txid, term, key)` 统一索引，后续函数直接 shadow-first read 即可获得 RYW。为了 c4 的函数参数传递，RET 元数据仍保留，但不能与 staged application write set 混用。

## 7. 单节点部署约束

本设计假设“单节点”指所有 SUT 服务位于同一物理机：gateway、lock manager、shadow service、WorkerSP、四类函数容器、Scylla/DynamoDB-compatible storage、CouchDB/Redis。负载发生器应放在另一台机器，避免开放环客户端与 SUT 争抢 CPU。

当前 `config/worker_info.yaml` 配置了 3 个 worker，不能直接用于该实验。需要提供实验专用的单节点 worker 配置，并重新执行 `initialize.py c4`，因为函数位置已经写入 CouchDB。不要只改 YAML 而沿用旧的 function metadata。

公平比较要求：

- Boki-style-SN 和 OCC 都在相同单节点拓扑上重新运行；
- 不能直接把现有 3-worker `results/occ` 与单节点 Boki-style-SN 比较；
- 两种系统使用相同容器数、CPU/内存限制、Scylla `--smp/--memory`、数据库初始镜像和网络路径；
- 记录 `DEFAULT_CONTAINER_NUM`。当前 c4 四个函数、每类默认 32 个容器，在单节点会预热 128 个容器，必须确认不会因内存压力或 CPU oversubscription 改变结论；
- 锁服务最好固定 CPU 集或至少记录其 CPU 使用，防止集中式服务与函数容器资源争用无法解释；
- 每次系统切换后重建相同的 10,000 key、每项 4 KiB 数据集，并清理 staged writes、shadow tombstone、RET 和锁状态。

单节点锁服务和 shadow service 都是实验的潜在瓶颈，这本身是该模拟实现的一部分，但必须同时报告二者的 CPU、请求率；锁服务额外报告等待队列长度，shadow service 额外报告 staged bytes 和 flush throughput，以区分“锁冲突”“锁服务饱和”和“提交服务饱和”。

## 8. c4 / Zipf=0.9 trace 实验设计

### 8.1 工作负载保持不变

沿用 `experiment/microbenchmark/test7_dynamic_access_set/trace` 的开放环回放方式：

- workflow：`c4`，四个串行函数；
- 每个函数 3 个互不重复 key，默认 `R, R, W`，每个事务共 12 次数据访问；
- 数据集：10,000 keys；
- value：4 KiB；
- Zipf alpha：0.9；
- arrival offset、`actual_interval/core_interval`、segment 和随机 seed 与 OCC 完全一致；
- 请求按 trace 时间触发，不因先前请求未返回而停发。

当前 `run_segment.py` 会忽略 segment JSON 中的 `params` 并重新生成输入。为保证配对实验，建议先按 `(trace, segment, seed, zipf=0.9)` 生成一次不可变的 c4 参数 manifest，OCC 与 Boki-style-SN 都按 `global_req_id` 读取同一条参数。manifest 同时保存 hash；结果文件记录该 hash。这样可以避免 CouchDB 中函数枚举顺序或脚本变化造成两种系统访问的 key 不一致。

### 8.2 实验目录

建议保留现有 runner 的公共部分，新增系统参数而不是复制两套逐渐漂移的脚本：

```text
experiment/microbenchmark/test7_dynamic_access_set/trace/
  manifests/<trace>/c4_zipf0.9_segment_<n>.jsonl
  results/occ_single_node/...
  results/boki_style_single_node/...
```

若先用复制脚本快速实现，Boki 结果至少写入 `results/boki_style_single_node`，UUID namespace 从 `faasnap:occ-trace` 改为独立的 `faasnap:boki-sn-trace`，且 summary 字段不能继续命名为 `occ_retry_count`。

### 8.3 原始结果字段

每个请求建议记录：

- 公共字段：`system`、`trace`、`segment_index`、`global_req_id`、`tx_id`、`in_core`、scheduled/fire offset、status、submit/response timestamp；
- 时延：`e2e_latency`、`workflow_exec_latency`、`lock_wait_latency`、`shadow_get_put_latency`、`flush_latency`、`db_io_latency`、`unlock_latency`；
- 重试：`rounds`、`retry_count`、`wait_die_abort_count`、`timeout_abort_count`、`active_abort_count`；
- 锁：`lock_request_count`、`immediate_grant_count`、`wait_count`；
- shadow：`shadow_get_count`、`shadow_hit_count`、`shadow_put_count`、`flushed_key_count`；
- 失败诊断：`error`、最终 `term`。

主结果仍为 core interval 内成功请求的 P50、P99 和吞吐。为了与现有脚本兼容，可保留当前 `count / (last_response - first_submit)` 的 achieved throughput，同时额外记录 trace 的 offered request rate 和固定 core-window completion throughput；论文中必须统一选择同一种定义比较所有系统。

### 8.4 运行矩阵

最小可发表矩阵：

| 维度 | 取值 |
| --- | --- |
| 系统 | OCC-single-node、Boki-style-SN |
| trace | lowload、highload |
| workflow | c4 |
| Zipf | 0.9 |
| 重复 | 每个选定 segment/system 至少 3 次，系统顺序交错 |

若时间有限，优先使用相同的 3 个 highload 和 3 个 lowload segment 做配对重复，而不是只挑一个对某系统有利的 segment。segment 0 没有前置 halo；其他 segment 带前 10 秒预热区，更适合稳定态统计。每轮运行前检查数据库和锁状态干净，运行后检查：

- lock table 无 owner/waiter；
- shadow service 无 ACTIVE/FLUSHING/DISCARDING 事务，staged writes 已清空；
- gateway/WorkerSP 无 active transaction；
- scheduled = completed，且 core request 无 client error；
- 无 stale attempt 写入 shadow 或参与 flush；
- 数据库和服务无节流、OOM 或容器创建失败。

## 9. 测试与验收

### 9.1 锁服务单元测试

1. `S/S` 可并发；`S/X`、`X/S`、`X/X` 冲突。
2. 同事务 `S/S`、`X/X`、持有 `X` 后请求 `S` 均幂等成功。
3. `S -> X` 在无其他 reader 时升级；存在其他 reader 时执行 Wait-Die。
4. 年轻事务请求年老事务持有的 key 立即 abort。
5. 年老事务请求年轻事务持有的 key 等待，年轻事务 unlock 后被唤醒。
6. 多 reader 中只要有一个更老 owner，写者 abort；全部更年轻时写者等待。
7. 三事务、多 key 的经典环形访问不会形成死锁，至少一个年轻事务被 abort。
8. unlock 校验 owner，重复 unlock/abort 幂等。
9. abort 能取消阻塞中的 lock request 并唤醒 HTTP handler。
10. 旧 term 的 lock/unlock/abort 被拒绝，不影响新 term。
11. HTTP 重试同一个 `op_id` 不重复排队、不重复计数。

### 9.2 Shadow service 与数据路径集成测试

1. `put` 后、commit 前主表保持旧值，shadow `/get` 返回 staged value。
2. 未写过的 key 在 shadow `/get` 返回 MISS，Store 再 consistent-read 主表。
3. 同一事务多次写同一 key 只保留最后值，重复 `op_id` 不重复计数或产生副作用。
4. commit flush 后主表包含全部最终值；随后 unlock、complete，shadow staged writes 和锁均清空。
5. 在一次事务 stage 多个 key 后注入 Wait-Die abort，discard 后主表完全不变且不发生 flush。
6. 应用主动 abort 同样先 discard 再 unlock，且不重试。
7. 其他事务在 writer flush 完成并解锁前不能读取被写 key，包括逐 key flush 的中间状态。
8. 模拟 flush 响应丢失，使用相同 `flush_id` 重试不会产生错误结果，并能继续放锁。
9. 模拟 DB 暂时失败，flush 在持锁状态下继续未完成 key；不得错误转为 discard + unlock。
10. 上一 term 的延迟函数不能写 shadow；旧 term 不能 flush 或清除新 term 的数据。
11. c4 单请求结果与 OCC 无并发时语义一致。

### 9.3 实验验收门槛

- 低负载无竞争时 `rounds=1` 占绝大多数，且不存在异常 abort。
- 所有成功或 abort 请求结束后锁和 staged writes 均无泄漏。
- 高负载下 `wait_die_abort_count` 与 gateway `retry_count` 对得上；timeout abort 单独统计且应接近 0。
- 相同 manifest 下 OCC 与 Boki-style-SN 的请求数、key/op 序列 hash 完全一致。
- 连续重复运行结果方差可解释；若锁服务 CPU 达到饱和，明确报告而不是把它误判为数据冲突成本。

## 10. 实施顺序

1. 先实现纯内存 LockManager 及完整单元测试。
2. 接入 lock `/begin`、`/lock`、`/unlock`、`/abort`，完成 `term` 和幂等协议测试。
3. 实现独立 shadow service 及 get/put/discard/term 单元测试。
4. 实现 shadow flush 状态机、DB 写入进度和幂等故障注入测试。
5. 改造 Store：关闭共享数据缓存，接入“加锁 -> shadow-first get / shadow put”。
6. 移除 WorkerSP -> transaction sink -> validator 路径，接入 END 的 shadow flush -> lock unlock，以及 abort 的 shadow discard -> lock abort。
7. 用单请求和小规模并发 c4 做端到端正确性测试。
8. 建立单节点配置、共享 manifest 和统一结果 schema。
9. 先跑短 trace 验证无泄漏，再跑完整 lowload/highload 配对实验。

## 11. 已知限制与论文表述

该基线不能宣称复现完整 Boki，原因包括：

- Boki 的核心是共享日志/LogBook 和 metalog，本设计没有这些组件；
- Boki 的日志排序、索引更新、缓存局部性和故障恢复开销均未建模；
- 本设计把锁状态放在单节点内存中，延迟和扩展性不同于 Boki 的日志记录锁；
- 本设计把 staged writes 放在独立的单节点内存 shadow service；它与 Beldi/Boki 的持久日志或存储内 shadow table 在延迟和容错上并不等价；
- commit 采用 shadow flush 成功后再放锁，提供正常执行和冲突 abort 下的原子可见性，但不提供 shadow/lock 服务崩溃后的自动恢复；
- 本实验只评估 c4、Zipf=0.9 和给定单节点资源条件下的并发控制性能。

论文中建议表述为：实现了一个遵循 Boki/Beldi workflow 事务关键并发控制策略的单节点 Boki-style baseline（strict 2PL + Wait-Die），用于隔离比较悲观锁竞争与重试成本；同时明确它没有复现 Boki 的共享日志基础设施。若审稿人要求系统级 Boki 对比，仍需运行官方 Boki 或复现其 LogBook 路径，该单节点模拟不能完全替代。

参考资料：

- 仓库中的 `FaaSRep_ATC26.pdf`：将 Beldi/Boki 归为悲观并发控制，并讨论 Wait-Die 下的冲突重试。
- [Beldi: Fault-Tolerant and Transactional Stateful Serverless Workflows](https://www.usenix.org/system/files/osdi20-zhang_haoran.pdf)：2PL、Wait-Die、transaction context 和 commit/abort 传播。
- [Boki: Stateful Serverless Computing with Shared Logs](https://pdos.csail.mit.edu/6.5840/papers/jia21sosp-boki.pdf)：Boki 的 LogBook/metalog 架构和 BokiFlow 边界。
