# 目标
* 初版FaaSnap，无其他优化，评价本地乐观控制的可用性
# 实现方案
* 每个节点一个LocalEngine
    * 使用HTTP代理和外界交互, 路由如下：
      * get：[key，version] -> [value, status]  使用key和version获取缓存中的value
      * write：[key, version,value] -> [status] 事务写
      * update：使用一批[key，version.value]list更新缓存
        * get和write先调用内部check函数。如果版本不匹配，返回abort信息。
    * 一个key-value缓存，缓存数据项，使用update被coordinator更新
    * 一个key-version表，执行版本检查。
    * 一个shadow table，缓存事务写。
* workflow engine
  * 容器维护本地事务的运行上下文，包括ID、版本集[key, version]、写集[key, pos].
  * 初始化时注册local engine端口，生成容器时将信息告诉容器。
  * 容器写入本地的
  * 本地容器全部结束运行之后
* gateway
  * 和coordinator配合出现。接收请求。
* 事务运行时：
  * carry读集合和写集合。在到达新节点/读写数据时check是否过期。
  * 写数据写到shadow table。
# 组件
* Engine
  * Cache
    * 标准的LRU Cache，某一固定容量，存储key-version-value三元组
    * 被访问时，携带key-version两元组。若版本不匹配则从远端获取新版本。
    * 更新策略：LRU。
  * KVTable
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