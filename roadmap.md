# 目标
* 初版FaaSnap，无其他优化，评价乐观控制的性能
# 实现方案
## 组件
* 事务工作流运行（FaaSFlow）
  * gateway
    * 接收运行信息，进行图划分。将子图发送给本地工作流引擎
  * Workflow Engine
    * 本地触发函数，运行工作流
* local Cache
  * 具备HTTP服务。
  * 函数从缓存中读数据。缓存失效则从数据库拉取。
* dataBank
  * 函数向数据库中写的数据以及其返回值会存储在这里。
* coordinator
  * 进行加锁验证，触发函数提交。
## 运行流程
* 事务工作流开始运行
  * gateway接收上传的工作流配置，划分子图，发送给workflow。
  * 从起始函数开始运行整个工作流，类似FaaSFlow。
* 本地读写
  * 函数运行过程中，运行时库捕获该函数的读写集（读：key，version；写：key）
  * 函数运行过程中的写和运行后的返回写入当前节点上的dataBank。运行结束后运行时库将当前函数的读写集返回给engine。
  * engine使用dataBank中的返回值调用下一个函数。
  * engine维护一个不断扩大的读写集，最后提交验证。
* 验证和提交
  * 单个coordinator实例，存储key-version的全局可信集，以及一个tuple锁管理器。
  * 接收到commit后，对读写集中的元组上锁，检查读集是否过期。收集需要修复的元组，交给gateway，进行第二次运行。
  * gateway根据coordinator传来的信息决定是否commit。
    * 若commit，gateway触发每个workflow engine提交dataBank中的内容(2PC). coordinator收到提交/放弃信息后返回给gateway，gateway返回给客户。
      * workflow engine提交时，根据标记位[]生成写数据库的格式。
    * 若需要修复，workflow engine使用delta set更新缓存，每个函数重新运行。某节点上的函数重新运行结束后，再次触发提交，更新版本，放锁。

# 对比方案
* TCC：carry interval，更新promise由coordinator负责。
* Beldi：远程拿锁的同步方案。这里由coordinator上锁。
* AFT: 远程pod serve所有事务请求
# 下一步改进
* 基于RDMA的cache更新和数据写
* 缓存更新配置
* 冲突查询优化
* 通信优化：节点间/容器和engine