import logging
import time
import math
from gevent import event
import gevent
import sys
from function_info import FunctionInfo
from container import Container, ContainerPool
sys.path.append('../../config')
import config
# data structure for request info
class RequestInfo:
    def __init__(self, transaction_id, data):
        self.transaction_id = transaction_id
        self.data = data
        self.result = event.AsyncResult()
        self.arrival = time.time()

exec_lifetime = 600

# manage a function's container pool
class Function:
    def __init__(self, host_addr, client, transaction_sink_addr, function_info:FunctionInfo, port_controller, node_list, default_container_num, reserve_pool, input, output, parent_cnt, function_pos):
        self.host_addr = host_addr
        self.client = client
        self.info:FunctionInfo = function_info
        self.transaction_sink_addr = transaction_sink_addr
        self.validator_addr = config.VALIDATOR_ADDR
        self.port_controller = port_controller
        self.node_list = node_list
        self.default_container_num = default_container_num
        self.reserve_pool = reserve_pool
        self.input = input
        self.output = output    
        self.parent_cnt = parent_cnt
        self.function_pos = function_pos
        
        self.num_processing = 0
        self.rq = []
        self.FAST_PATH = config.FAST_PATH
        self.OPTIMISTIC_REPAIR = config.OPTIMISTIC_REPAIR



        # container pool
        self.container_pool = ContainerPool(self.info.max_containers, self.info.function_name)

        if self.function_pos[self.info.function_name] == self.host_addr:
            while self.container_pool.len() < self.default_container_num:
                container = self.create_container()
                if container == None:
                    raise Exception("Container creation failed")
                self.container_pool.put(container)
            print(f"function: {self.info.function_name} container pool created, len {self.container_pool.len()}")
        
    # put the request into request queue
    def send_request(self, transaction_id, write_set, is_repair,repair_mode,batch_id, repair_states):
        data = {'transaction_id': transaction_id, 'repair': is_repair, 'batch_id':batch_id, 'repair_mode':repair_mode,
                 'write_set':write_set, 'repair_states':repair_states}
        req = RequestInfo(transaction_id, data)
        self.rq.append(req)
        res = req.result.get()
        return res

    # receive a request from upper layer
    def dispatch_request(self):
        # no request to dispatch
        if len(self.rq) - self.num_processing == 0:
            return
        self.num_processing += 1
        
        # 1. try to get a workable container from pool
        container = self.container_pool.pop()
        
        # create a new container
        while container is None:
        # if container is None:
            logging.warning(f"Container pool is empty, creating a new container for function: {self.info.function_name}, processing: {self.num_processing}, rq len: {len(self.rq)}")
            container = self.create_container()
           
        # the number of exec container hits limit
        if container is None:
            logging.warning(f"Function {self.info.function_name} reaches max container limit, cannot create new container.")
            self.num_processing -= 1
            return

        req = self.rq.pop(0)
        self.num_processing -= 1
        # 2. send request to the container
        res = container.send_request(req.data)
        res['port'] = container.port
        req.result.set(res)
        
        # 3. in fastpath, reserve the container into reserve pool
        # else, return the container to pool
        # if self.FAST_PATH:
        #     # if the container is not used in fast path, reserve it into reserve pool
        #     self.reserve_pool.reserve(req.transaction_id, container)
        # else:
        self.container_pool.put(container)


    # create a new container
    def create_container(self):
        # do not create new exec container
        # when the number of execs hits the limit
        if not self.container_pool.check_pool_full_and_occupy():
            logging.warning(f"Function {self.info.function_name} cannot create new container as it hits max limit.")
            return None

        max_retries = 10  # 设置最大重试次数，防止无限循环
        for attempt in range(max_retries):
            port = -1  # 初始化端口号
            try:
                # 步骤 1: 从控制器获取一个端口
                port = self.port_controller.get()
                
                # 步骤 2: 尝试使用该端口创建容器
                container = Container.create(self.client, self.info.img_name, port, 'exec', self.container_pool)
                
                # 如果成功，初始化并返回容器
                self.init_container(container)
                return container

            except Exception as e:
                # 步骤 3: 如果创建失败（很可能是端口冲突）
                logging.warning(f"创建容器失败 (端口: {port}, 尝试: {attempt + 1}/{max_retries})")
                # 短暂等待后重试，给操作系统一点时间清理端口
                gevent.sleep(0.1)

    # after the destruction of container
    # its port should be give back to port manager
    def remove_container(self, container):
        # #logging.info('remove container: %s, pool size: %d', self.info.function_name, len(self.container_pool))
        container.destroy()
        self.port_controller.put(container.port)

    # do the function specific initialization work
    def init_container(self, container: Container):
        container.init(self.host_addr, self.info.workflow_name, self.info.function_name, self.transaction_sink_addr, self.validator_addr, self.node_list, self.input,self.output,self.parent_cnt, self.function_pos, container.port, self.FAST_PATH, self.OPTIMISTIC_REPAIR)

    # do the repack and cleaning work regularly
    def repack_and_clean(self, all_pool=False):
        # find the old containers
        old_container = []
        self.container_pool.clean_pool(exec_lifetime, old_container, self.default_container_num, all_pool)

        # time consuming work is put here
        for c in old_container:
            self.remove_container(c)

    def clear_containers_all_pool(self):
        self.repack_and_clean(True)

def favg(a):
    return math.fsum(a) / len(a)




