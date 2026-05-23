
## 7.1 Experimental Setup

## 7.2 Azure Trace Evaluation

* workload：Banking，Travel，Social三个真实应用
* Trace：Azure High load/Low Load
* Baseline：Concord，Beldi，OCC

* 实验设置
  * High/Low Load下展示三个应用的延迟箱型图

## 7.3 Execution Time Breakdown

* workload：Banking，Travel
* Baseline：OCC
* 分解延迟，分析Repair收益

## 7.4 Impact of Data Access Skew

* workload：Microbenchmark
* Baseline：Concord，OCC
* 控制从数据集取样的zipf参数，写明冲突率。说明之后采用适中的zipf=0.9

## 7.5 Impact of Read/Write Ratio.

* workload：Microbenchmark
* Baseline：Concord，OCC
* 控制工作流读写比例，写明冲突率

## 7.6 Sensitivity Analysis

### 7.6.1 工作流形态

* 不同长度/fanout

### 7.6.2 缓存命中率

### 7.6.3 batch size

## 7.7 Ablation Study

* 对比OCC，各组件带来的性能提升

## 7.8 Overhead&Scalability