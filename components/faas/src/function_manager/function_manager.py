import sys
import logging

import gevent
import docker
from function_info import parse
from port_controller import PortController
from function import Function

sys.path.append('../../config')
import config

logging.basicConfig(
    # 设置日志级别为 INFO
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S',  # 设置日期格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ],
    force=True 
)


repack_clean_interval = 5.000 # repack and clean every 5 seconds
dispatch_interval = 0.005 # 200 qps at most

# the class for scheduling functions' inter-operations
class FunctionManager:
    def __init__(self, config_path, min_port, node_list, reserve_pool):
        self.function_info = parse(config_path)

        self.port_controller = PortController(min_port, min_port + 4999)
        self.client = docker.from_env()
        self.default_container_num = config.DEFAULT_CONTAINER_NUM

        self.functions = {
            x.function_name: Function(self.client, x, self.port_controller, node_list, self.default_container_num, reserve_pool, config.FAST_PATH, config.REMOTE_LOCK)
            for x in self.function_info
        }
        self.init()
       
    def init(self):
        gevent.spawn_later(repack_clean_interval, self._clean_loop)
        gevent.spawn_later(dispatch_interval, self._dispatch_loop)
    
    def _clean_loop(self):
        gevent.spawn_later(repack_clean_interval, self._clean_loop)
        for function in self.functions.values():
            gevent.spawn(function.repack_and_clean)

    def _dispatch_loop(self):
        gevent.spawn_later(dispatch_interval, self._dispatch_loop)
        for function in self.functions.values():
            gevent.spawn(function.dispatch_request)
    
    def run(self, function_pos, function_name, transaction_id, input, output, write_set,RYW_upstream,is_repair, next_funcs, parent_cnt,batch_id, downstream_func_table, lock_set=None):
        # print('run', function_name, request_id, runtime, input, output, to, keys)
        if function_name not in self.functions:
            raise Exception("No such function!")
        return self.functions[function_name].send_request(transaction_id, function_pos, input, output, write_set,RYW_upstream, is_repair, next_funcs, parent_cnt,batch_id, downstream_func_table, lock_set)
