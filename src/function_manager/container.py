import requests
import docker
import time
import config.config as config
import gevent
import redis
import os
from docker.types import Mount
from gevent.lock import BoundedSemaphore


base_url = 'http://127.0.0.1:{}/{}'

class ContainerPool:

    def __init__(self, max_containers, function_name):
        self.pool = []
        self.lock = BoundedSemaphore()
        self.num_exec = 0 # the number of containers in execution, not in container pool
        self.max_containers = max_containers
        self.function_name = function_name

    # the pool list is in order:
    # - at the tail is the hottest containers (most recently used)
    # - at the head is the coldest containers (least recently used)
    def clean_pool(self, lifetime, old_container, default_container_num, all_pool=False):
        self.lock.acquire()
        if all_pool:
            old_container.extend(self.pool)
            self.pool = []
            self.lock.release()
            return 
        cur_time = time.time()
        idx = -1
        for i, c in enumerate(self.pool):
            if cur_time - c.lasttime < lifetime:
                idx = i
                break
        # all containers in pool are old, or the pool is empty
        if idx < 0:
            idx = len(self.pool)
        
        if len(self.pool) - idx <= default_container_num:
            idx = max(0, len(self.pool) - default_container_num)

        old_container.extend(self.pool[:idx])
        self.pool = self.pool[idx:]
        self.lock.release()
    
    def check_pool_full_and_occupy(self):
        self.lock.acquire()
        if self.num_exec + len(self.pool) > self.max_containers:
            # logging.info('hit container limit, function: %s', self.function_name)
            self.lock.release()
            return False
        self.num_exec += 1
        self.lock.release()
        return True

    def len(self):
        return len(self.pool)

    def pop(self):  
        res = None
        self.lock.acquire()
        if len(self.pool) != 0:
            res = self.pool.pop(-1)
            self.num_exec += 1
            # logging.info(f"[{self.function_name}] Pop container, pool size:{len(self.pool)}")
        self.lock.release()  
        return res
    
    def put(self, container):
        self.lock.acquire()
        self.pool.append(container)
        self.num_exec -= 1
        self.lock.release()

class Container:
    @classmethod
    def create(cls, client, image_name, port, attr, container_pool: ContainerPool):
        container = client.containers.run(image_name,
                                          detach=True,
                                          ports={'5000/tcp': str(port)},
                                          labels=['workflow'])
        res = cls(container, port, attr, container_pool)
        res.wait_start()
        return res

    # get the wrapper of an existed container
    # container_id is the container's docker id
    @classmethod
    def inherit(cls, client, container_id, port, attr):
        container = client.containers.get(container_id)
        return cls(container, port, attr)

    def __init__(self, container, port, attr, container_pool):
        self.container = container
        self.port = port
        self.attr = attr
        self.lasttime = time.time()
        self.container_pool = container_pool

    # wait for the container cold start
    def wait_start(self):
        while True:
            try:
                r = requests.get(base_url.format(self.port, 'status'))
                if r.status_code == 200:
                    break
            except Exception as e:
                pass
            gevent.sleep(0.005)

    # send a request to container and wait for result
    def send_request(self, data = {}):
        # logging.info(f"Dispatching request data {data}, container port: {self.port}")
        r = requests.post(base_url.format(self.port, 'run'), json=data)
        self.lasttime = time.time()
        return r.json()

    # initialize the container
    def init(self, host_addr, workflow_name, function_name, node_list, input,output, function_pos):
        data = { 'host_addr':host_addr, 'workflow': workflow_name, 'function': function_name,
                "node_list": node_list,"input": input, "output": output, "function_pos": function_pos, 'cache_enable':config.CACHE_ENABLED}
        r = requests.post(base_url.format(self.port, 'init'), json=data)
        self.lasttime = time.time()
        return r.status_code == 200
    
    def return_to_pool(self):
        self.lasttime = time.time()
        self.container_pool.put(self)

    # kill and remove the container
    def destroy(self):
        self.container.remove(force=True)



if __name__ == '__main__':

    os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, db=0)

    def deleteAll():
        keys = redis_client.keys('*')
        for key in keys:
            redis_client.delete(key)

    def checkredis():
        keys = redis_client.keys('*')
        if not keys:
            print("No keys found in Redis database.")   
        # 打印每个键及其对应的值
        else:
            for key in keys:
                value = redis_client.get(key)
                print(f"Key: {key.decode('utf-8')}, Value: {value.decode('utf-8')}")
    checkredis()
    deleteAll() 
    redis_client["1:GLOBAL:chained_num_0"] = 0         
    client = docker.from_env()
    input = {"chained_num_0":{"from": "GLOBAL", "type": "int", "value": 0}}    
    output = {"chained_num_1":{"type": "int"}}
    data = {"transaction_id":1, "input":input, "output":output}

    container = Container.create(client, 'testflow_func1', 20000, 'exec')
    container.init("testflow", "func1")
    checkredis()

    print(container.send_request(data))
    checkredis()

    # docker run -d -p 5000:8080 --label workflow testflow_func1
    # docker rm -f $(docker ps -aq --filter label=workflow)