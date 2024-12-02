# 目标
* 初版FaaSnap，无其他优化，评价乐观控制的性能
# 实现方案
## 组件
* parser
  * 划分工作流图，存储每个函数信息（在哪个节点上跑）
  * 初期：直接指定每个函数对应的节点
* gateway
  * 接收运行信息，触发起始函数，将子图发送给本地工作流引擎
  * 等待每个TXID的运行。接收到结果后说明运行完毕，等待验证。
  * 接收coordinator的验证信息，重新触发一次修复。等待coordinator的提交完成信号后，返回给用户。
  * 定期检查存储的ID和运行时间，超时（用户设置）后，回收之前TxID数据，重跑。
* Workflow Engine
  * 本地触发函数，运行工作流
  * 根据函数信息触发后续
* container
  * 传参由faas层负责，可能在运行时读取数据（get，put）
  * 一个API库：调用本地put/get函数时，容器server进行转发
* data engine
  * local Cache
    * 失效则从数据库拉取
  * dataBank
    * 函数向数据库中写的数据以及其返回值会存储在这里。
  * 都使用redis实现。
    * C++ HTTP server+Redis
    * dataBank：多级键值存储
      * 存储模式：Tx_id -> func_id -> {write:{k:v}, internal:{k,v}}
    * cache: 从Redis读取，不命中则从数据库读
    * 分别对应两个数据库
* coordinator
  * 进行加锁验证，触发函数修复。
  * 修复完成后触发每个workflow engine提交数据。全部完成后修改提交位，告知gateway。
* cluster Node
  * 存储配置
  * 存储每个调用ID的返回值、调用参数
* storage node
  * 存储持久化的数据
## 运行流程
* 事务工作流开始运行
  * parse DAG：明确每个工作流的运行图，ASSIGN ip，每个节点初始化时得知这些信息
  * 从起始函数开始运行整个工作流，类似FaaSFlow。开始运行。需要一个全局有序的时间戳。
    * 保证ID唯一即可，Gateway指派一个UUID。
* 本地读写
  * 函数运行过程中，运行时库捕获该函数的读写集（读：key，version；写：key）
  * 函数运行过程中的写和运行后的返回写入当前节点上的dataBank。分开中间参数和需要持久保存的数据。
  * engine将函数用到的参数传入（配置文件），函数自己从DataBank中读取
  * engine维护一个不断扩大的读写集，最后一个函数提交验证。只验证external。
  * 最后一个函数所在的工作流引擎将运行结果写入request_id对应的结果数据库中。
* 验证和提交
  * 单个coordinator实例，存储key-version的全局可信集，以及一个tuple锁管理器。
  * 接收到commit后，对读写集中的元组上锁，检查读集是否过期。否则收集需要修复的元组，更新目标机器（从config查询）上的缓存，进行第二次运行或提交。将提交时间戳发送。
    * 若可以提交，coordinator告知每个目标机器的workflow engine提交databank中的内容。告知gateway事务已运行完毕。
    * 否则更新缓存后, coordinator再触发一次工作流运行过程。成功提交后告知gateway结果。
  * gateway
    * 若需要修复，gateway触发每个函数重新运行一次，使用标记位告知这是repair过程。sink函数运行结束后，再次触发提交，更新版本，放锁。

# 对比方案
* TCC：carry interval，更新promise由coordinator负责。
* Beldi：远程拿锁的同步方案。这里由coordinator上锁。
* AFT: 远程pod serve所有事务请求
# 下一步改进
* 基于RDMA的cache更新和数据写
* 缓存更新配置
* 冲突查询优化
* 通信优化：节点间/容器和engine