# basic settings
import os
import re
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent
STORAGE_NODE_IP = '10.2.29.142'

# Global switch for run-scoped files under ``logging/``. Disable this for
# performance measurements; restart long-lived services and recreate workflow
# containers after changing it.
ENABLE_EXPERIMENT_LOGGING = False

def _append_no_proxy_hosts(*hosts):
    existing = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    no_proxy_hosts = [host.strip() for host in existing.split(",") if host.strip()]
    seen = set(no_proxy_hosts)
    for host in hosts:
        if host and host not in seen:
            no_proxy_hosts.append(host)
            seen.add(host)
    value = ",".join(no_proxy_hosts)
    os.environ["NO_PROXY"] = value
    os.environ["no_proxy"] = value


def _load_worker_hosts():
    worker_info = ROOT_DIR / "config" / "worker_info.yaml"
    if not worker_info.exists():
        return []
    text = worker_info.read_text(encoding="utf-8")
    return re.findall(r"^\s*-\s*([0-9]+(?:\.[0-9]+){3})(?::[0-9]+)?\s*$", text, re.MULTILINE)


_append_no_proxy_hosts("127.0.0.1", "localhost", STORAGE_NODE_IP, *_load_worker_hosts())

COUCHDB_URL = f'http://faasnap:faasnap@{STORAGE_NODE_IP}:5984'
DYNAMODB_URL = f'http://{STORAGE_NODE_IP}:4567'
DYNAMODB_KEY_ID = 'FAASNAPDYNAMODB'
# ... (前面的内容保持不变) ...
DYNAMODB_ACCESS_KEY = 'FAASNAPDYNAMODBKEY'
DYNAMODB_AREA = 'us-west-2'

# --- 修改 Redis 配置 ---
# 通用 Redis 实例，用于 Shadow Table 等
REDIS_HOST = '127.0.0.1' 
REDIS_PORT = 6379
SHADOWTABLE_DB = 0
REDIS_CACHE_PORT = 6380
CACHE_DB = 1 

GATEWAY_ADDR = f'{STORAGE_NODE_IP}:8000' # need to update as your private_ip
# ... (后面的内容保持不变) ...

VALIDATOR_ADDR = f'{STORAGE_NODE_IP}:9000'
WORKERSP_PORT = '7500'


# workflow setting
# workflow setting
WORKFLOW_YAML_ADDR = {
                   # 'textseq': f"{ROOT_DIR}/benchmark/textseq",
                    #  'c2': f"{ROOT_DIR}/benchmark/micro_benchmark/c2",
                      'c4': f"{ROOT_DIR}/benchmark/micro_benchmark/c4",
                    #    'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                    #  'c16': f"{ROOT_DIR}/benchmark/micro_benchmark/c16",
                    #    'w2': f"{ROOT_DIR}/benchmark/micro_benchmark/w2",
                    #     'w4': f"{ROOT_DIR}/benchmark/micro_benchmark/w4",
                    #     'w6': f"{ROOT_DIR}/benchmark/micro_benchmark/w6",
                    #     'w8': f"{ROOT_DIR}/benchmark/micro_benchmark/w8",
                    #'travel_reservation': f"{ROOT_DIR}/benchmark/travel_reservation",
                    #  'banking_system': f"{ROOT_DIR}/benchmark/banking_system",   
                    # 'social_network': f"{ROOT_DIR}/benchmark/social_network",  
                    }
DEFAULT_CONTAINER_NUM = 32
# cache setting
CACHE_ENABLED = True
CLEAR_MEM = True
FILLUP_CACHE = False
EXPIRED_CACHE = True

# Latency breakdown collection.
# Keep disabled for trace latency/throughput experiments. Enable only when
# running dedicated breakdown experiments because it adds CouchDB writes/reads.
COLLECT_BREAKDOWN_LATENCY = False
COLLECT_FUNCTION_LATENCY = COLLECT_BREAKDOWN_LATENCY
LATENCY_BATCH_SIZE = 128
LATENCY_FLUSH_INTERVAL = 0.05

# validator setting
VALIDATORS_PER_POOL = 4
VALIDATE_INTERVAL = 0.01
BATCH_TIMEOUT = 0.015
ABORT_PROB = 0

# batch setting
BATCH_SIZE = 4

# mode setting
FAST_PATH = True
OPTIMISTIC_REPAIR = True


# repair setting
RUNNING = '1'
REPAIRED = '2'
ABORTED = '3'

OPT_REPAIR = 1
PESSI_REPAIR = 2

# used in scalabiliy test.
SCALABILITY_TEST = False
TRACE_TEST = True
FAKE_SINK_URL =  f'http://{STORAGE_NODE_IP}:6000/fake_repair_pessi'
FAKE_NOTIFY_URL = f'http://{STORAGE_NODE_IP}:8000/fake_notify'
FAKE_REQUEST_URL = f'http://{STORAGE_NODE_IP}:8000/fake_request'

# microbenchmark configuration
DB_SIZE = 10000
DATA_ITEM_SIZE = 4 * 1024 

## OLD APPLICATION PARAMETERS (not used in current experiments, but kept here for reference)

# # travel reservation
# FLIGHT_IDS = 50			
# FLIGHT_CAPACITY = "100"			
# RENTAL_START = '2025-07-01'			
# RENTAL_END = '2025-07-31'			
# CAR_NUM = '300'			
# DATE_FORMAT = "%Y-%m-%d"			
			
# # banking system			
# BANKING_ACCOUNTS = 50
# BANKING_ORIGINAL_BALANCE = "10000"			
# LOGIN_FAIL_PROB = 0			
			
# # social network			
# SOCIAL_NETWORK_USERS = 50			
# STARTUP_POSTS = 1
# travel reservation

## APPLICATION PARAMETERS V1
# travel reservation
FLIGHT_IDS = 200			
FLIGHT_CAPACITY = "1000"			
RENTAL_START = '2025-07-01'			
RENTAL_END = '2025-09-30'			
CAR_NUM = '1000'			
DATE_FORMAT = "%Y-%m-%d"			
					
			
# banking system			
BANKING_ACCOUNTS = 100
BANKING_ORIGINAL_BALANCE = "100000"			
LOGIN_FAIL_PROB = 0			
			
# social network			
SOCIAL_NETWORK_USERS = 75			
STARTUP_POSTS = 2


# # APPLICATION PARAMETERS V2
# # travel reservation
# FLIGHT_IDS = 200			
# FLIGHT_CAPACITY = "200"			
# RENTAL_START = '2025-07-01'			
# RENTAL_END = '2025-07-31'			
# CAR_NUM = '300'			
# DATE_FORMAT = "%Y-%m-%d"			
			
# # banking system			
# BANKING_ACCOUNTS = 200
# BANKING_ORIGINAL_BALANCE = "10000"			
# LOGIN_FAIL_PROB = 0			
			
# # social network			
# SOCIAL_NETWORK_USERS = 200			
# STARTUP_POSTS = 3
