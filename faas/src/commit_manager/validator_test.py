import time
from multiprocessing import Queue
from validator import ValidatorProcess
from serializer import SerializerProcess
from validator_repo import Repository

# 构造测试数据
test_data = {
    'first_run_finish_time': time.time(),
    'batch_id': 'f5466fa0-8362-43f2-b263-a960baafd148',
    'batch': {
    'batch_id': 'f5466fa0-8362-43f2-b263-a960baafd148',
    'read_set': {
        'f5466fa0-8362-43f2-b263-a960baafd148': {
            'f1': {'t1': '1970-01-01 00:00:00.000000', 't2': '1970-01-01 00:00:00.000000'},
            'f2': {'t5': '1970-01-01 00:00:00.000000'},
            'f3': {'t8': '1970-01-01 00:00:00.000000'},
            'f4': {'t11': '1970-01-01 00:00:00.000000'},
            'f5': {'t14': '1970-01-01 00:00:00.000000'}
        }
    },
    'write_set': {
        'f5466fa0-8362-43f2-b263-a960baafd148': {
            't3': 'f1', 't6': 'f2', 't9': 'f3', 't12': 'f4', 't15': 'f5'
        }
    },
    'RYW_subjection': {
        'f5466fa0-8362-43f2-b263-a960baafd148': {
            'f1': {},
            'f2': {'t3': 'f1'},
            'f3': {'t6': 'f2'},
            'f4': {'t9': 'f3'},
            'f5': {'t12': 'f4'}
        }
    },
    'container_port': {
        'f5466fa0-8362-43f2-b263-a960baafd148': {
            'f1': 20000, 'f2': 20001, 'f3': 20002, 'f4': 20003, 'f5': 20004
        }
    },
    'transaction_list': ['f5466fa0-8362-43f2-b263-a960baafd148'],
    'first_run_finish_time': time.time()
}}

if __name__ == "__main__":
    repo = Repository()
    workflow_name = "textseq"
    function_info = repo.get_function_info(repo.get_all_functions(workflow_name), workflow_name)
    function_pos = {func: info['ip'] for func, info in function_info.items()}
    workflow_graph_topo = {func: info['next'] for func, info in function_info.items()}
    worker_ip_set = set(info['ip'] for info in function_info.values())

    # 创建通信队列
    task_queue = Queue()
    serializer_req_queue = Queue()
    serializer_return_pipe = Queue()
    handler_task_queues = [task_queue]
    serializer_return_pipes = [serializer_return_pipe]

    # 启动SerializerProcess
    serializer = SerializerProcess(
        req_queue=serializer_req_queue,
        result_pipes=serializer_return_pipes,
        handler_task_queues=handler_task_queues,
        function_pos=function_pos
    )
    serializer.daemon = True
    serializer.start()

    # 启动ValidatorProcess
    validator = ValidatorProcess(
        validator_id=0,
        workflow_name=workflow_name,
        task_queue=task_queue,
        serializer_req_queue=serializer_req_queue,
        child_get=serializer_return_pipe,
        function_pos=function_pos,
        workflow_graph_topo=workflow_graph_topo,
        worker_ip_set=worker_ip_set,
        repo=repo
    )
    validator.daemon = True
    validator.start()

    # 发送VALIDATE请求
    batch_id = test_data['batch_id']

    task_queue.put((batch_id, 1, test_data))

    # 等待一会儿让进程处理
    time.sleep(3)
    print("Test finished. Check validator和serializer日志文件和相关输出。")