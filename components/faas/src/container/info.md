# Redis key 结构
* 按照redis_key -> Txid : func : in/out : key的层级存放函数输入/输出
    * redis_key ： {key: value}
* container内的Store根据传入参数生成Redis输出key
* 由于不知道上游函数名，container需要输入中存在GLOBAL/func关键字
