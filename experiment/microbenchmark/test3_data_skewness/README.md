# Boki-SN c4 闭环数据倾斜测试

该测试复现 dynamic 分支 `test3_data_skewness` 的闭环模型：每个 client 只有在前一个 workflow 返回后才提交下一次。默认且固定的实验配置为：

- workflow：`c4`；
- client：`32`；
- Zipf alpha：`0.9`；
- 每 client 100 轮（可用 `ROUNDS` 环境变量缩短为冒烟测试）。

先启动 Boki-SN 服务，并在负载发生器上执行：

```bash
cd /home/shao/FaaSnap
bash experiment/microbenchmark/test3_data_skewness/run_boki_sn.sh
```

例如，10 轮冒烟：

```bash
ROUNDS=10 bash experiment/microbenchmark/test3_data_skewness/run_boki_sn.sh
```

运行时父进程每 5 秒会输出一行进度，例如：

```text
[progress +15.0s] completed=96/3200 ok=96 failed=0 clients_done=0/32 clients_active=32 retries=18 rate=6.40 req/s
```

`completed` 是已返回的逻辑请求数；闭环中每个仍活跃的 client 至多有一个在途请求。若长时间没有增加，可据此区分是少数请求重试过多，还是 gateway/WorkerSP 已无响应。可直接运行 `run.py --progress-interval 1` 缩短报告间隔。

每个 client 在提交前从共享计数器分配唯一的 `global_req_id`。Boki-SN 只用这个 ID 决定 Wait-Die 优先级；同一逻辑事务重试时优先级不变。

结果写入 `results/boki_style_single_node/`：每次运行生成带时间戳的 raw CSV，并向 `summary_results.csv` 追加一行。summary 报告成功请求的 P50/P99、闭环吞吐、retry/Wait-Die/timeout/active-abort 计数；任何失败请求都会保留 raw CSV 并使脚本返回非零。
