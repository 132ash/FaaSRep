import logging
import time
import math
from gevent import event
import sys
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


# manage a function's container pool
class Function:
    def __init__(self, client, transaction_sink_addr, function_info, port_controller, node_list, default_container_num, reserve_pool, input, output,function_pos, fast_path_enabled, remote_lock_enabled, optimistic_repair):
        self.client = client
        self.info = function_info
        self.transaction_sink_addr = transaction_sink_addr
        self.port_controller = port_controller
        self.node_list = node_list
        self.default_container_num = default_container_num
        self.reserve_pool = reserve_pool
        self.input = input
        self.output = output    
        self.function_pos = function_pos
        self.fast_path_enabled = fast_path_enabled
        self.remote_lock_enabled = remote_lock_enabled
        self.optimistic_repair = optimistic_repair
        
        self.num_processing = 0
        self.rq = []

        # container pool
        self.container_pool = ContainerPool(self.info.max_containers, self.info.function_name)

        while self.container_pool.len() < self.default_container_num:
            container = self.create_container()
            if container == None:
                raise Exception("Container creation failed")
            self.container_pool.put(container)
        print(f"function: {self.info.function_name} container pool created, len {self.container_pool.len()}")
    
    # put the request into request queue
    def send_request(self, transaction_id, write_set, is_repair, parent_cnt,batch_id,lock_set, repair_states, snapshot_interval):
        data = {'transaction_id': transaction_id, 'repair': is_repair, 'batch_id':batch_id,
                 'write_set':write_set, "parent_cnt":parent_cnt,"lock_set":lock_set, 'repair_states':repair_states, 'snapshot_interval':snapshot_interval}
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
        
        # 3. in fastpath, reserve the container into reserve pool
        # else, return the container to pool
        if config.REPAIR and config.FAST_PATH:
            # if the container is not used in fast path, reserve it into reserve pool
            self.reserve_pool.reserve(req.transaction_id, container)
        else:
            self.container_pool.put(container)


    # create a new container
    def create_container(self):
        # do not create new exec container
        # when the number of execs hits the limit
        if not self.container_pool.check_pool_full_and_occupy():
            return None

        logging.info('create container of function: %s', self.info.function_name)
        try:
            container = Container.create(self.client, self.info.img_name, self.port_controller.get(), 'exec', self.container_pool)
        except Exception as e:
            print(e)
            self.container_pool.num_exec -= 1
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
    def init_container(self, container: Container):
        container.init(self.info.workflow_name, self.info.function_name, self.transaction_sink_addr, config.validator_addr, self.node_list, self.input,self.output, self.function_pos, container.port, self.fast_path_enabled, self.remote_lock_enabled, self.optimistic_repair)

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



