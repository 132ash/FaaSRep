# Boki-style-SN 启动与实验指南

本文档说明如何启动本仓库的 **Boki-style-SN** 基线，并做可重复的单请求和小规模并发正确性实验。该实现是单节点的 strict 2PL + Wait-Die + 独立 shadow service 基线，不是完整 Boki；不要将结果表述为官方 Boki 的结果。

本文档**不会启动或执行 trace 实验**。当前 `experiment/microbenchmark/test7_dynamic_access_set/trace/run_segment.py` 仍是 OCC 专用 runner：它使用 OCC UUID namespace、`results/occ` 输出目录和 `occ_retries` 字段。因此，不能直接以它跑 Boki-SN 并把输出当作 Boki 实验结果。

## 1. 拓扑与前置条件

所有下列服务部署在同一台 SUT 主机。负载发生器应位于另一台机器，避免与 SUT 争抢 CPU：

```text
load generator
      |
gateway :8000 -> WorkerSP :7500 -> function containers
      |                    |
      +-> lock manager :9000
      +-> shadow service :9100 -> Scylla Alternator :4567
                                -> CouchDB :5984 / Redis :6379, :6380
```

需要具备：Python 3、Docker、Docker daemon 访问权限，以及 `scripts/requirements.txt` 中的 Python 依赖。以下命令均假定仓库位于 `/home/shao/FaaSnap`；其他目录请相应替换。

> 警告：`scripts/db_setup_bash.sh` 会重建 CouchDB/Scylla 的数据库和表，只能在专用实验环境运行，不能对含有需要保留数据的主机执行。

## 2. 一次性配置

### 2.1 固定单节点地址

在 SUT 主机确认其 IP，例如：

```bash
export SUT_IP=10.2.29.142
cd /home/shao/FaaSnap
```

编辑 [config/config.py](../config/config.py)，使 `STORAGE_NODE_IP` 等于 `${SUT_IP}`。容器使用的 [src/container/container_config.py](../src/container/container_config.py) 也必须使用相同 IP；当前两处均为静态配置，不能只改其中一个。

为避免覆盖原来的三节点设置，先备份并切换到单节点 worker 文件：

```bash
cp config/worker_info.yaml config/worker_info.yaml.before_boki_sn
cp config/worker_info_single_node.yaml config/worker_info.yaml
# 若 SUT_IP 不是模板中的 10.2.29.142，编辑 worker_info.yaml，将 nodes[0] 改为该 IP。
```

`worker_info.yaml` 变更后必须重新执行 `initialize.py c4`，因为函数位置会被写入 CouchDB；只修改 YAML 而复用旧 metadata 会导致 WorkerSP 仍尝试访问旧节点。

### 2.2 确认 workflow-private `shadow_table`

Boki-SN 的应用 staged writes 保存在独立的 `shadow_service`，但 workflow 的输入参数和跨函数 `RET` 仍使用 DynamoDB/Alternator 的 `shadow_table`。因此该表仍是必需依赖。启动 gateway 前运行一次非破坏性预检/创建：

```bash
cd /home/shao/FaaSnap
python3 scripts/ensure_boki_tables.py
python3 scripts/ensure_boki_tables.py --check
```

第一个命令只会在表缺失时创建 `shadow_table(txid HASH, key RANGE)`；它不会删除 `data` 或既有 shadow 数据。第二个命令应返回 `"table_status": "ACTIVE"`。

### 2.3 安装依赖与构建函数镜像

在 SUT 主机执行：

```bash
cd /home/shao/FaaSnap
python3 -m pip install -r scripts/requirements.txt
bash scripts/worker_setup.sh microbenchmark
```

该脚本会启动 worker 侧 Redis，并构建 `workflow_base` 和 c4 使用的 `micro_func` 镜像。若 Docker 镜像、Redis 已正确准备，可跳过重复构建，但实验切换前要确认 Redis 服务存在。

## 3. 每轮实验前重建数据与 c4 metadata

在没有旧的 gateway、WorkerSP、lock manager 或 shadow service 运行时，执行：

```bash
cd /home/shao/FaaSnap
bash scripts/db_setup_bash.sh microbenchmark
```

此命令会启动/重建 Scylla 与 CouchDB，创建 `shadow_table`，初始化 10,000 个 4 KiB 数据项，并为 microbenchmark 工作流建立 metadata。随后显式再做一次 c4 初始化，作为单节点切换的检查点：

```bash
python3 src/initializer/initialize.py c4
```

可用下面命令确认 CouchDB 中的 c4 函数都已落到单一地址：

```bash
curl -s "http://faasnap:faasnap@${SUT_IP}:5984/c4_function_info/_all_docs?include_docs=true" \
  | python3 -m json.tool
```

输出中的 `f1`、`f2`、`f3`、`f4` 的 `ip` 应均为 `${SUT_IP}:7500`。

## 4. 启动顺序

所有长驻进程都必须带有 `SYSTEM_MODE=BOKI_SN`。推荐在 `tmux` 的四个窗口分别启动，以便观察日志；不要启动 `src/transaction_sink`，Boki-SN 不使用它。

窗口 1，启动 lock manager 和 shadow service：

```bash
cd /home/shao/FaaSnap
SYSTEM_MODE=BOKI_SN bash scripts/start_boki_sn.sh "${SUT_IP}"
```

窗口 2，启动 WorkerSP：

```bash
cd /home/shao/FaaSnap
SYSTEM_MODE=BOKI_SN python3 src/workflow_manager/proxy.py "${SUT_IP}" 7500
```

窗口 3，启动 gateway：

```bash
cd /home/shao/FaaSnap
SYSTEM_MODE=BOKI_SN python3 src/gateway/gateway.py "${SUT_IP}" 8000
```

等待容器预热完成后，在任意可访问 SUT 的主机检查控制面：

```bash
curl -fsS "http://${SUT_IP}:9000/health" | python3 -m json.tool
curl -fsS "http://${SUT_IP}:9100/health" | python3 -m json.tool
```

两者都应返回 `"status": "ok"`。lock health 中的 `waiter_count` 在空闲时为 0；shadow health 中的 `staged_bytes` 在空闲时为 0。

## 5. c4 冒烟测试

以下请求构造一个完整 c4 workflow：每个函数按 `R, R, W` 访问三个不同 key。`keys` 是 JSON 字符串，`payload_size` 为 4 KiB。

```bash
cat >/tmp/boki_c4_request.json <<'JSON'
{
  "workflow": "c4",
  "global_req_id": 1,
  "transaction_id": "boki-smoke-001",
  "parameters": {
    "f1": {
      "keys": "{\"f1\":{\"key1\":\"R\",\"key2\":\"R\",\"key3\":\"W\"},\"f2\":{\"key4\":\"R\",\"key5\":\"R\",\"key6\":\"W\"},\"f3\":{\"key7\":\"R\",\"key8\":\"R\",\"key9\":\"W\"},\"f4\":{\"key10\":\"R\",\"key11\":\"R\",\"key12\":\"W\"}}",
      "payload_size": 4096
    }
  }
}
JSON

curl -fsS -X POST "http://${SUT_IP}:8000/run" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/boki_c4_request.json | python3 -m json.tool
```

`global_req_id` 是 Boki-SN 必填的全局竞争优先级，数值越小越老；同一逻辑事务的重试始终使用同一值。预期返回 `status: ok`、`rounds: 1`，以及 Boki 字段：`term`、`retry_count`、`wait_die_abort_count`、`lock_request_count`、`flushed_key_count` 和 `flush_latency`。无冲突的该请求通常有 12 次 lock request、4 个 flush key。

请求完成后验证没有泄漏：

```bash
curl -fsS "http://${SUT_IP}:9000/debug/tx/boki-smoke-001" | python3 -m json.tool
curl -fsS "http://${SUT_IP}:9100/debug/tx/boki-smoke-001" | python3 -m json.tool
```

lock 服务应显示 `RELEASED` 且 `held_locks` 为空；shadow 服务应显示 `COMPLETED` 且 `staged_keys` 为空。若看到 `ACTIVE`、`FLUSHING`、`DISCARDING` 或非空 waiter，停止测试并检查相应日志，不应继续收集性能数据。

## 6. 小规模并发与 Wait-Die 验证

先用单元测试验证服务状态机：

```bash
cd /home/shao/FaaSnap
python3 -m pytest -q tests/test_boki_services.py
```

预期 7 项通过，覆盖 S/X 兼容性、升级、Wait-Die、等待取消、旧 term、discard 和幂等 flush。

然后在负载发生器机器上使用同一个热点 key 集并发提交第 5 节的请求（给每次请求不同 `transaction_id`）。例如先把 `key1`、`key2`、`key3` 固定为相同热点，再以多个后台 `curl` 或你自己的 open-loop client 提交。高冲突下应看到：

- gateway 返回的 `retry_count` 和 `wait_die_abort_count` 增加；
- lock `/health` 的 `metrics.wait_die_abort_count`、`metrics.wait_count` 增加；
- `metrics.timeout_abort_count` 应接近 0；
- 所有请求完成后 `waiter_count=0`，shadow 的 `staged_bytes=0`。

应用主动调用 `store.abort_tx()` 时，gateway 应返回 `status: aborted`，不应重试。普通容器错误返回 `status: error`；如果 flush 已经进入 `FLUSHING` 后发生永久 DB 错误，系统会保留锁以避免暴露部分写入，这属于 fatal 条件，必须停止该轮实验并人工排查，而不是强制解锁。

## 7. 每轮结果与清理检查

记录至少以下内容：

- 配置快照：Git commit、`SYSTEM_MODE`、`DEFAULT_CONTAINER_NUM`、SUT CPU/内存、Scylla `--smp/--memory`；
- gateway：E2E latency、rounds、retry/abort 计数及最终 term；
- lock health：请求数、immediate grant、wait、Wait-Die/timeout abort、`waiter_count`；
- shadow health：get/put、hit、flush key、flush latency、staged/peak staged bytes；
- 每轮结束后的 lock/shadow debug 状态和服务日志。

每次系统切换或新重复试验前，都应执行第 3 节的数据重建步骤。不要把旧的三节点 OCC 结果与单节点 Boki-SN 结果直接比较。

停止长驻服务时，在它们所在的 `tmux` 窗口中使用 `Ctrl-C`。恢复原有多节点实验前，恢复 worker 配置并重新初始化 metadata：

```bash
cd /home/shao/FaaSnap
cp config/worker_info.yaml.before_boki_sn config/worker_info.yaml
python3 src/initializer/initialize.py c4
```

## 8. Trace 实验准备（不执行回放）

仓库已提供 Boki-SN 专用的 manifest 生成与回放脚本。它们与 OCC runner 分离：UUID namespace 为 `faasnap:boki-sn-trace`，结果写入 `results/boki_style_single_node/`，且不使用 `occ_retries` 字段。

在负载发生器上先生成不可变 manifest；这一步只读取 segment 和 `db_keys.json`，**不会向 gateway 发请求**：

```bash
cd /home/shao/FaaSnap
TRACE=highload \
TARGET_SEGMENT_INDICES_OVERRIDE="1 6 17" \
ZIPF_PARAM=0.9 \
SEED=20260827 \
bash experiment/microbenchmark/test7_dynamic_access_set/trace/prepare_boki_manifests.sh
```

输出位于 `trace/manifests/<trace>/`，每个 JSONL manifest 都有 `.metadata.json` sidecar，记录 segment/dataset/manifest SHA-256、Zipf 参数及 seed。使用相同 manifest 时，OCC-single-node 与 Boki-SN 可获得相同的 `global_req_id → key/op` 映射。

准备完成后，先检查 manifest 的 metadata、控制面 health 和第 7 节的无泄漏条件。之后（不是当前步骤）可通过下列命令执行一次 Boki-SN 回放：

```bash
cd /home/shao/FaaSnap
SYSTEM_MODE=BOKI_SN \
TRACE=highload \
TARGET_SEGMENT_INDICES_OVERRIDE="1" \
ZIPF_PARAM=0.9 \
REQUEST_TIMEOUT=300 \
bash experiment/microbenchmark/test7_dynamic_access_set/trace/run_boki_segments.sh
```

该回放脚本会拒绝缺失 manifest 的运行。它为每个 segment 写入原始 CSV、进度 JSON 和 summary；summary 仅在 `scheduled=completed` 且所有请求成功时生成。正式实验应按 `lowload/highload × c4 × Zipf 0.9 × 配对重复` 运行，并在每个 segment 后检查 lock/shadow 无泄漏。
