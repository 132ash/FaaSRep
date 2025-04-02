import logging
import time
import math
from gevent import event
from container import Container, ContainerPool
from function_info import FunctionInfo

# data structure for request info
class RequestInfo:
    def __init__(self, transaction_id, data):
        self.transaction_id = transaction_id
        self.data = data
        self.result = event.AsyncResult()
        self.arrival = time.time()


# manage a function's container pool
class Function:
    def __init__(self, client, function_info, port_controller, node_list, default_container_num, reserve_pool):
        self.client = client
        self.info = function_info
        self.port_controller = port_controller
        self.node_list = node_list
        self.default_container_num = default_container_num
        self.reserve_pool = reserve_pool
        
        self.num_processing = 0
        self.rq = []

        # container pool
        self.container_pool = ContainerPool()

        while len(self.container_pool) < self.default_container_num:
            container = self.create_container()
            if container is None:
                raise Exception("Container creation failed")
            self.container_pool.put(container)
        print(f"function: {self.info.function_name} container pool created, len {len(self.container_pool)}")
        

    
    # put the request into request queue
    def send_request(self, transaction_id, function_pos, input, output, write_set, is_repair, next_funcs, parent_cnt, no_parent_execution):
        data = {'transaction_id': transaction_id, "function_pos":function_pos, 'input': input,'is_repair': is_repair,
                 'output': output, 'write_set':write_set, "next_functions":next_funcs, "parent_cnt":parent_cnt,'no_parent_execution':no_parent_execution}
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
            container = self.create_container()
           
        # the number of exec container hits limit
        if container is None:
            self.num_processing -= 1
            return

        req = self.rq.pop(0)
        self.num_processing -= 1
        # 2. send request to the container
        logging.info('send request to: %s of: %s, rq len: %d, data: %s', self.info.function_name, req.transaction_id, len(self.rq), str(req.data))
        res = container.send_request(req.data)
        res['port'] = container.port
        req.result.set(res)
        
        # 3. reserve the container into reserve pool
        self.reserve_pool.reserve(req.transaction_id, container)

        # self.container_pool.put(container)

    # create a new container
    def create_container(self):
        # do not create new exec container
        # when the number of execs hits the limit
        if self.container_pool.check_pool_full_and_occupy() is None:
            return None

        logging.info('create container of function: %s', self.info.function_name)
        try:
            container = Container.create(self.client, self.info.img_name, self.port_controller.get(), 'exec', self.container_pool)
        except Exception as e:
            print(e)
            self.num_exec -= 1
            return None
        logging.info('function: %s container created', self.info.function_name)
        self.init_container(container)
        return container

    # after the destruction of container
    # its port should be give back to port manager
    def remove_container(self, container):
        logging.info('remove container: %s, pool size: %d', self.info.function_name, len(self.container_pool))
        container.destroy()
        self.port_controller.put(container.port)

    # do the function specific initialization work
    def init_container(self, container):
        container.init(self.info.workflow_name, self.info.function_name, self.node_list)

    # do the repack and cleaning work regularly
    def repack_and_clean(self):
        # find the old containers
        old_container = []
        self.container_pool.clean_pool(exec_lifetime, old_container, self.default_container_num)

        # time consuming work is put here
        for c in old_container:
            self.remove_container(c)

def favg(a):
    return math.fsum(a) / len(a)

# life time of three different kinds of containers
exec_lifetime = 600



