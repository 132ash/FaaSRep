import time
import math
from gevent import event
from src.function_manager.function_info import FunctionInfo
from  src.function_manager.container import Container, ContainerPool
import sys
import logging
import gevent   
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
    def __init__(self, host_addr, client, function_info:FunctionInfo, port_controller, node_list, default_container_num, input, output, function_pos):
        self.host_addr = host_addr
        self.client = client
        self.info:FunctionInfo = function_info
        self.port_controller = port_controller
        self.node_list = node_list
        self.default_container_num = default_container_num
        self.input = input
        self.output = output    
        self.function_pos = function_pos
        
        self.num_processing = 0
        self.rq = []


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
    def send_request(self, transaction_id, write_set):
        data = {'transaction_id': transaction_id,'write_set':write_set}
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
            # logging.warning(f"Container pool is empty, creating a new container for function: {self.info.function_name}")
            container = self.create_container()
           
        # the number of exec container hits limit
        if container is None:
            self.num_processing -= 1
            return

        req = self.rq.pop(0)
        self.num_processing -= 1
        # 2. send request to the container
        res = container.send_request(req.data)
        res['port'] = container.port
        req.result.set(res)
        
        self.container_pool.put(container)


    def create_container(self):
        # do not create new exec container
        # when the number of execs hits the limit
        if not self.container_pool.check_pool_full_and_occupy():
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
        container.init(self.host_addr, self.info.workflow_name, self.info.function_name, self.node_list, self.input,self.output, self.function_pos)

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




