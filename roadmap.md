# 目标
* 初版FaaSnap，无其他优化，评价本地乐观控制的可用性
# 实现方案
* 每个节点一个cache引擎，提供函数容器访问的get接口，定期从远端接收key更新
    * 使用HTTP端口和容器交互
    * 接收更新时先暂时封存key-value，更新好后重新开放
* 远程shadow table
  * 事务写数据时写入该shadow table，用于RYW以及最后的提交
* 持久存储
  * 采用MongoDB：只存储key-value
* workflow engine
  * 多节点备份方式（目前使用单节点），处理尾节点提交、分配函数调度
* gateway
  * 和coordinator配合出现。接收请求。
* 事务运行时：
  * carry读集合和写集合。在到达新节点/读写数据时check是否过期。
  * 写数据写到shadow table。
# 对比方案
* TCC：carry interval，更新promise由coordinator负责。
* Beldi：远程拿锁的同步方案。这里由coordinator上锁。
* AFT: 远程pod serve所有事务请求
# 下一步改进
* 基于RDMA的cache更新和数据写
* shadow table的本地化
* 缓存更新配置
* 冲突查询优化
* 通信优化：节点间/容器和engine