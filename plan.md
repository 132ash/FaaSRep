# FaaSRep Repair/Validation 重构计划

## 目标

本计划面向一次性完成的重构提交，不拆成多阶段落地。目标是在不改变 FaaSRep 核心协议语义的前提下，把 `commit_manager`、`transaction_sink` 以及 repair metadata 相关路径从当前“多层嵌套字典 + 隐式状态机”的形态，重构为更清晰、可测试、可维护且更高性能的实现。

重点目标：

- 明确验证、依赖构建、repair 调度、commit 排序之间的数据边界。
- 用 typed dataclass / small class 替换关键路径上的深层嵌套字典。
- 修复乐观 repair 退化到悲观 repair 时可能丢依赖、丢触发任务并导致系统卡死的问题。
- 避免 gevent lock、multiprocessing queue、HTTP 调用中的潜在死锁/永久等待。
- 提升 `commit_manager` 侧 hot path 的性能和可读性，尤其是 serializer 的 writers list、commit cascade、validation result 构造。
- 保持现有外部 HTTP API 兼容，避免一次性牵动 gateway、workflow manager、container sidecar 的协议。

## 当前主要问题判断

### 1. transaction_sink 的状态机隐式且存在卡死风险

当前 `RepairingBatchState.after_transaction_finish()` 同时处理：

- 乐观 repair 完成。
- 乐观 repair 被拒绝。
- 主动 abort 触发下游悲观退化。
- 悲观 ready 条件推进。
- batch 完成。
- 向 validator 发送后续 repair/commit 任务。

这些状态混在一个嵌套循环里，且返回值有时是 dict，有时是 `False, []`。最危险的路径是：

```python
rejected, successors = optimistic_state_change_after_repair(...)
if rejected:
    return False, []
```

当一个事务已经被标记为 `need_pessimistic_repair`，但它的乐观 repair 完成通知晚到时，当前代码直接丢弃这个通知，并且没有把该事务加入悲观 repair 队列。如果该事务已经 `pessimistic_repair_ready`，它会永远停在未完成状态；如果它是后续事务的 PTD/PBD 释放条件，整个 batch 或后续 batch 会卡死。

另一个相关风险是依赖注册和状态更新不是原子操作。`update_subjection_info()`、`after_transaction_finish()`、`clear_opt_table_after_finish()` 都会修改同一组表，但 `state_lock` 当前没有实际使用。gevent 请求并发进入时，可能出现：

- 先收到 finish，再注册完整依赖。
- abort cascade 和 pessi-ready 推进交错。
- 同一事务被重复 finish，计数重复增加。
- batch state 被清理后又收到迟到通知，触发 KeyError 或静默丢失。

### 2. 悲观依赖计数缺乏去重和兜底

`PessimisticBatchState` 使用 `prev_fin_cnt` 表示一个事务还要等待多少前驱条件。当前依赖结构是普通 list，缺少显式去重和来源标识。虽然 serializer 当前会尽量只记录 nearest batch / nearest tx，但后续维护或多 key 场景中很容易引入重复依赖，导致：

- `prev_fin_cnt` 被加多次。
- 前驱完成只释放一次。
- 后继永远无法变为 pessi-ready。

此外，如果依赖指向的 batch/tx 已经被清理、已经 committed，或者因为异常没有注册到 sink，当前代码通常选择忽略。这种“忽略”在某些场景是正确的，在另一些场景会丢失退化路径。需要把这些情况显式建模：

- 前驱已 committed：依赖已稳定，可视为 satisfied。
- 前驱已 aborted：当前事务必须悲观 repair，并在构建悲观依赖时排除前驱写。
- 前驱未知：不能静默忽略，应记录异常并触发安全兜底。

### 3. commit_manager hot path 字典层次过深

当前 serializer 和 repair info 的数据形态类似：

```python
expired_set[tx_id][func][key]
subjection_set[tx_id][func]["upstream_keys"][key]
pessi_sink_info["whole_tx_sub"][prev_tx][next_tx]
repair_metadata_per_batch_by_ip[mode][batch_id][ip][tx_id][func]
```

这种结构的问题：

- 默认字段不统一，容易 KeyError，例如 `opt_func_info["upstream_keys"]` 假设一定存在。
- 字段语义分散，`dirty`、`up_cnt`、`RYW_keys`、`successor_port` 混在 dict 里。
- 读写路径需要大量 `setdefault`，可读性差，也增加 hot path 开销。
- serializer 的 `key_writers[key]` 使用 list，并在 commit 时 `pop(0)`，这是 O(n)。
- commit cascade 使用 list `pop(0)`，也是 O(n)。

### 4. lock 与网络调用存在永久阻塞风险

目前多个地方手动 `acquire()` / `release()`，缺少 `try/finally`：

- `RepairEngine.pessi_register_lock`
- `PessimisticRepairer.write_table_lock_per_batch`
- 多处 state lock 未来启用后也必须避免外部调用期间持锁

另外，validator、sink、worker、gateway 之间的 `requests.post()` 基本没有 timeout。任何一个 worker 卡住或网络请求不返回，都可能让 repair/commit greenlet 永远等待，从外部看就是“系统卡死”。

### 5. repair metadata 构造存在正确性隐患

`RepairInfo.construct_repair_metadata()` 里 RYW 合并逻辑有几个潜在问题：

- `opt_func_info["upstream_keys"]` 可能尚未初始化。
- RYW merge 时直接覆盖 `dirty`，而不是与已有 dirty 状态做 OR。
- downstream 是否 dirty 被 upstream dirty 状态影响，但当前传播规则隐式且难以检查。
- `up_cnt` 与实际等待的 upstream function 数量可能不一致，应该从去重后的 dependency refs 派生，而不是手工累加。

这些问题在复杂 workflow 或从乐观退化悲观时会放大。

## 目标架构

保持现有进程和 HTTP API 不变，但在进程内部引入清晰的数据模型和服务边界。

### 新增/重构数据模型

建议新增以下内部模块，不改变外部 API payload：

- `src/commit_manager/models.py`
- `src/transaction_sink/models.py`
- 必要时新增公共轻量模型文件，例如 `src/common/repair_models.py`，但为了避免 import 路径复杂，优先在各组件内局部定义。

核心模型建议：

```python
@dataclass(frozen=True)
class WriterRef:
    batch_id: str
    tx_id: str
    func: str

@dataclass(frozen=True)
class UpstreamRef:
    tx_id: str
    func: str

@dataclass
class FunctionRepairPlan:
    dirty: bool = False
    upstream_keys: dict[str, UpstreamRef] = field(default_factory=dict)
    ryw_keys: dict[str, str] = field(default_factory=dict)
    successor_port: dict[str, int | str] = field(default_factory=dict)

@dataclass
class TransactionRepairPlan:
    tx_id: str
    functions: dict[str, FunctionRepairPlan] = field(default_factory=dict)

@dataclass
class ValidationResult:
    expired_keys: dict[str, dict[str, set[str]]]
    repair_deps: dict[str, TransactionRepairPlan]
    pessimistic_sink_info: PessimisticSinkInfo

@dataclass
class PessimisticSinkInfo:
    batch_deps: dict[str, set[str]]
    tx_deps: dict[str, set[str]]
    last_tx: dict[str, str]
    tx_successors: dict[str, set[str]]
```

对外发送到 worker/sink 前，再统一转换为现有 JSON dict。这样外部协议稳定，内部代码可读性提升。

### commit_manager 内部边界

把当前 `SerializerProcess` 中混在一起的逻辑拆成几个类：

- `GlobalVersionTable`
  - 保存 `key_version_table`
  - 提供 stale read 判断

- `WriterIndex`
  - 保存 `key -> deque[WriterRef]`
  - 提供 `latest_writer(key)`、`add_writer(key, writer)`、`commit_batch(batch_id, commit_keys)`
  - commit 时使用 `deque.popleft()`，避免 list `pop(0)`

- `BatchCommitTracker`
  - 保存 batch 的写集合、ready write count、version、handler assignment
  - 提供 `is_ready(batch_id)`、`mark_key_unblocked(key)`、`pop_ready_cascade(batch_id)`

- `DependencyBuilder`
  - 输入 batch read/write set
  - 输出 `ValidationResult`
  - 只做依赖构建，不处理 repair metadata 下发

- `RepairMetadataBuilder`
  - 合并 validation deps、RYW、container_port、workflow topo
  - 产出 fast-path 或 non-fast-path 需要的 JSON payload

这样 `ValidatorProcess.validate()` 只负责 orchestration：

1. 向 serializer 请求 `ValidationResult`。
2. 用 `RepairMetadataBuilder` 构造 worker payload。
3. 调用 `RepairEngine` 发起 repair。

### transaction_sink 内部边界

将当前 `RepairingBatchState` 重构成显式状态机：

- `SinkStateStore`
  - 保存所有 batch/tx 状态。
  - 所有状态修改必须在同一把 lock 内完成。
  - lock 内只做纯内存计算，返回 `SinkCommand`，不发网络请求。

- `TransactionRepairState`
  - 字段包括：
    - `tx_id`
    - `batch_id`
    - `optimistic_state`
    - `needs_pessimistic`
    - `pessimistic_ready`
    - `pessimistic_running`
    - `final_state`
    - `successors`
    - `finish_recorded`

- `BatchRepairState`
  - 字段包括：
    - `batch_id`
    - `tx_order`
    - `finished_count`
    - `resolved_prefix_index`
    - `tx_deps`
    - `batch_successors`
    - `ready_pessi_queue`
    - `batch_finished`

- `SinkCommand`
  - `trigger_pessimistic_repair(batch_id, tx_ids)`
  - `notify_validator_batch_finished(batch_id, aborted_txs, pessi_txs)`
  - `notify_validator_aborts(batch_id, aborted_txs)`
  - `noop_with_reason(reason)`

所有入口最终变成：

- `register_batch(batch_id, tx_list)`
- `register_dependencies(batch_id, PessimisticSinkInfo)`
- `mark_repair_finished(batch_id, tx_id, mode, state)`
- `mark_needs_pessimistic(tx_id, reason)`
- `release_optimistic_state(batch_ids)`

关键要求：任何 `mark_*` 方法都必须是幂等的，迟到或重复通知不能让计数重复增加。

## 乐观退化悲观卡死修复设计

核心修复原则：**乐观 repair 被拒绝不是终点，它必须转换成悲观 repair 等待/触发状态。任何事务只要被标记为 `needs_pessimistic`，系统必须保证最终会触发悲观 repair 或被显式 abort/commit，不允许静默 no-op。**

具体规则：

1. 当上游事务 abort：
   - 标记所有乐观依赖后继为 `needs_pessimistic=True`。
   - 如果后继已经 `pessimistic_ready` 且未 running/finished，立即产生 `trigger_pessimistic_repair` command。
   - 如果后继尚未 ready，只记录状态；后续 ready 时必须触发。

2. 当某事务的乐观 repair finish 到达，但事务已 `needs_pessimistic=True`：
   - 不把它计入 finished。
   - 不返回 `False, []`。
   - 记录 `optimistic_result_rejected=True`。
   - 如果 `pessimistic_ready=True`，立即触发悲观 repair。
   - 否则等待 PBD/PTD 条件释放。

3. 当悲观依赖条件释放：
   - 将事务标记为 `pessimistic_ready=True`。
   - 如果它 `needs_pessimistic=True` 或全局配置是纯悲观 repair，立即触发悲观 repair。
   - 如果它已乐观 repaired 且不需要悲观 repair，则将其计入 final resolved。

4. 当事务悲观 repair finish：
   - 只接受 mode 为 `PESSI_REPAIR` 的 finish。
   - 将 final state 记为 repaired/aborted。
   - 推进 batch prefix、后继 ready 条件和 commit。

5. 对丢失依赖的兜底：
   - 如果依赖的 predecessor batch/tx 不存在：
     - 若 predecessor batch 已 committed，则视为 satisfied。
     - 若 predecessor tx 已 aborted，则当前 tx 标记为 needs pessimistic，并在悲观依赖构造时排除该写。
     - 若无法判断，记录 error metric/log，并触发安全路径：将当前 tx 标记为 needs pessimistic，且不允许 commit 直到 validator 重新构建依赖或显式 abort。

6. 对重复/迟到消息：
   - 如果 tx 已 final resolved，后续 finish/abort 通知只记录 debug log，不再改变 counters。
   - 如果 batch 已 cleaned，sink 返回明确状态给 caller，而不是 KeyError 或静默。

## commit_manager 性能与可读性改造

### Serializer hot path

- 将 `key_writers[key]` 从 list 改为 `collections.deque[WriterRef]`。
- 将 commit cascade queue 从 list 改为 deque。
- 将 `pessi_sink_info` 从深层 dict 改为 `PessimisticSinkInfo`，内部使用 set 去重。
- 将 `subjection_set` 构造改为 `TransactionRepairPlan`，避免在 inner loop 里反复 `setdefault`。
- 将 batch 内 tx index、read set、write set 在进入 serializer 前转换为局部变量，减少多层 dict 查找。
- 将 `BatchWriteInfo` 改为 dataclass，字段包括：
  - `version`
  - `writes: set[str]`
  - `ready_write_count`
  - `all_write_count`
- 统一版本比较策略。短期保持现有 timestamp string 兼容；内部可以用单调递增序列或 tuple 缓存排序值，避免重复字符串比较。

### RepairInfo / metadata 构造

- 引入 `FunctionRepairPlan` 默认字段，消除 `upstream_keys` 缺失问题。
- RYW merge 改为显式函数：
  - `merge_ryw(plan, ryw_keys, upstream_plans)`
  - dirty 使用 OR 语义，不能被覆盖为 False。
  - `up_cnt` 不再手写维护，而是由去重后的 `upstream_keys` 中不同 `(tx_id, func)` 数量派生。
- 生成 fast-path payload 时只在最后一步转 JSON dict。
- `expired_keys_per_ip` 用 `defaultdict(set)`，避免提前为所有 worker 创建空 set。

### PessimisticRepairer

- 将 batch 内 writer table 改为：
  - `key -> list[WriterRef | None]`
  - `tx_index -> tx_id`
  - `tx_id -> index`
- 所有 lock 使用 `try/finally`。
- `prepare_pessimistic_info` 在一把锁内只读取 writer table 和生成依赖，不调用外部组件；更新 repair_info 可在锁外完成。
- `modify_batch_write_table_for_abort` 支持幂等 abort：重复 abort 同一 tx 不应 KeyError 或重复修改。
- `pessimistic_get_commit_keys` 直接返回 `set[str]`，由外层转换为现有 dict payload。

### RepairEngine

- `pessi_register_lock` 使用 `try/finally`。
- 所有 HTTP 请求加 timeout，例如 2s 或配置项，并记录失败。
- repair trigger 请求失败时返回明确错误 command，避免 batch 永久等待。
- `repair_transactions` 对 `ready_transactions` 去重并保持 tx order。
- 对 `container_port[tx_id][func]` 缺失提供显式异常处理；不能让 KeyError 杀掉 greenlet 后无通知。

## 死锁/卡死防护

需要统一处理以下情况：

- gevent lock 必须用 context manager 或 `try/finally`。
- 状态机 lock 内禁止发 HTTP 请求。
- 所有 `requests.post/get` 必须有 timeout。
- `gevent.joinall()` 应设置 timeout 或检查 greenlet exception。
- repair/commit request 失败时必须向 validator/sink 返回可观察错误，不能静默。
- serializer 等待不做超时 fail-fast 或自动 abort；如果 serializer 长时间不返回，由 watchdog 输出 batch/op/data key 等上下文，等待人工终止和排查。
- 每个 batch 增加可观测状态：
  - registered
  - validated
  - repairing
  - waiting_pessimistic
  - committing
  - committed
  - failed
- 增加 watchdog 日志：如果 batch 在某状态超过阈值，输出缺失的 tx、依赖、ready 条件。

## 兼容性策略

本次重构应保持现有外部 payload 兼容：

- validator `/validate` 输入不变。
- sink `/repair_pessi` 输入可先保持不变，在内部转换为 `PessimisticSinkInfo`。
- worker `/prepare` payload 的最终 JSON 结构保持不变。
- worker `/commit` payload 保持不变。
- gateway `/notify` payload 保持不变。

这样可以把改动集中在 `commit_manager` 和 `transaction_sink`，不需要同时改 container sidecar、workflow manager、gateway 的 HTTP 协议。

## 验证计划

虽然这次计划文件不改代码，但真正实现时需要在同一次提交里加入测试。建议以纯单元测试为主，避免依赖多节点环境。

必须覆盖：

- Serializer stale read：
  - 无 writer，版本过期。
  - 有 uncommitted writer，生成 upstream dependency。
  - 同 batch nearest tx dependency。
  - 跨 batch nearest batch dependency。

- RYW merge：
  - RYW 覆盖跨事务 dependency。
  - RYW 不覆盖其他 dirty key。
  - dirty OR 语义正确。

- Sink 乐观到悲观退化：
  - 后继已 pessi-ready 时，上游 abort 立即触发悲观 repair。
  - 后继未 pessi-ready 时，上游 abort 先记录，ready 后触发。
  - 乐观 finish 迟到且已 needs-pessi，不再静默丢弃，而是触发或等待悲观 repair。
  - 重复 finish 不重复增加 finished_count。
  - 已清理 batch 收到迟到通知不会 KeyError。

- PessimisticRepairer：
  - abort 后 writer table 排除对应写。
  - 悲观依赖只找 PTD 之前最近未 abort writer。
  - 多 key 依赖去重后不会卡住。

- Commit cascade：
  - writers deque 正确 popleft。
  - 前序 batch commit 后后序 suspended batch 自动 ready。
  - abort batch 的 commit keys 不包含 aborted tx 的写。

## TODO List

- [x] 新增 commit_manager 内部 dataclass：`WriterRef`、`UpstreamRef`、`BatchWriteInfo`、`PessimisticSinkInfo`、`FunctionRepairPlan`、`TransactionRepairPlan`、`ValidationResult`。
- [x] 将 `SerializerProcess` 中的 validation/commit 状态拆成 `WriterIndex`、`BatchCommitTracker`、`DependencyBuilder`，并使用 deque 替换 list `pop(0)`。
- [x] 将 serializer 输出从嵌套 dict 改为 typed result，再在进程边界转换为兼容的 JSON dict。
- [x] 重写 `RepairInfo.construct_repair_metadata()`，使用 `FunctionRepairPlan` 默认字段，修复 `upstream_keys` 缺失、dirty 被覆盖、up_cnt 非去重等问题。
- [x] 重写 `PessimisticRepairer` 的 writer table 和依赖构造，加入去重、幂等 abort、`try/finally` lock 释放。
- [x] 在 transaction_sink 新增显式 `TransactionRepairState` 和 `BatchRepairState`，替代 `optimistic_state_per_transaction`、`pessimistic_state_per_batch`、`tx_finished_table_per_batch` 等散落 dict。
- [x] 重写 sink 状态机入口：`register_batch`、`register_dependencies`、`mark_repair_finished`、`mark_needs_pessimistic`、`release_optimistic_state`。
- [x] 修复乐观 repair 被 rejected 后直接 `return False, []` 的卡死路径，确保事务进入悲观 repair 等待/触发状态。
- [x] 对 PBD/PTD dependency 和 whole-tx optimistic dependency 使用 set 去重，避免 `prev_fin_cnt` 不可释放。
- [x] 对已 committed、已 aborted、未知 predecessor 分别建模，禁止静默丢失依赖。
- [x] 让 sink 状态机所有 mutation 在 lock 内完成，网络请求只根据返回的 `SinkCommand` 在 lock 外执行。
- [x] 跨组件 HTTP 调用不设置自动超时、不因网络等待自动 abort；若请求返回错误则记录详细日志，卡住时依赖 watchdog 输出上下文并由人工终止。
- [x] 对 `gevent.joinall()` 增加 greenlet 异常检查，但不设置 timeout/kill；卡住时由 watchdog 输出 batch/tx 状态。
- [x] 改造 `RepairEngine`：repair trigger/prepare 失败只记录阻塞上下文，不自动向 sink `/abort`。
- [x] 增加 batch/tx watchdog 日志，输出长时间等待的依赖、ready 条件、当前 mode 和缺失前驱。
- [x] 保持所有外部 HTTP API payload 兼容，只在内部使用 typed model。
- [x] 增加 serializer 单元测试：stale read、writer dependency、commit cascade。
- [x] 增加 repair metadata 单元测试：RYW merge、dirty 传播、upstream 去重。
- [x] 增加 transaction_sink 单元测试：乐观到悲观退化、迟到 finish、重复 finish、丢失 predecessor 兜底。
- [x] 增加 PessimisticRepairer 单元测试：abort 后重建依赖、commit key 选择、重复 abort 幂等。
- [x] 按本次约束不跑真实 debug/basic experiment；补一个小型 deterministic workflow smoke test，覆盖 optimistic repair 和 pessimistic fallback。
- [x] 复核前三项勾选但可能被 stash 覆盖的问题：`serializer.py` 主路径已实际切换到 `DependencyBuilder`、`WriterIndex`、`BatchCommitTracker` 和 typed `ValidationResult`。
- [x] 按调试需求移除自动 timeout/kill/abort 语义：HTTP、greenlet join 和 serializer response 等待均不因超时推进状态，只由 watchdog 输出卡死上下文。
- [x] 重构 validator batch runtime：用 `BatchRuntimeState` 和 `ValidatorBatchStore` 替代 `tx_list_per_batch`、`read_set_per_batch`、`write_set_per_batch`、`container_port_per_batch`、`successed_tx_list_per_batch`、`aborted_tx_list_per_batch`、`time_tuple_per_batch` 等平行字典。
- [x] 将 `repair_correctness` workflow 初始化移入 `scripts/db_setup.sh repair_correctness` 和 `scripts/worker_setup.sh repair_correctness`；debug runner 只负责播种本轮数据、运行 Gateway 场景和校验结果。

## 实现时优先检查的具体卡死路径

最需要优先用测试复现的路径：

1. T1 -> T2 存在依赖。
2. T2 先进入 optimistic repair。
3. T1 repair abort，sink 将 T2 标记为 `need_pessimistic_repair=True`。
4. T2 的 optimistic finish 迟到。
5. 当前代码 `optimistic_state_change_after_repair()` 返回 `rejected=True`，`after_transaction_finish()` 直接 `return False, []`。
6. 如果 T2 已经 pessi-ready，应该触发 pessimistic repair；当前不会触发。
7. T2 不 final resolved，batch finished_count 不增加，validator 不 commit，gateway 一直等待。

修复后的预期：

- 第 5 步不再返回 no-op。
- sink 记录 T2 的 optimistic result 已被拒绝。
- 若 T2 pessi-ready，则立即生成 `trigger_pessimistic_repair(T2)`。
- 若 T2 未 ready，则等 PTD/PBD release 后生成 trigger。
- 任一情况下，T2 都不会从状态机中消失。
