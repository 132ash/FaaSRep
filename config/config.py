# basic settings
import os
from pathlib import Path
from urllib.parse import urlparse


def _host_from_endpoint(endpoint):
    if not endpoint:
        return None
    endpoint = str(endpoint)
    if "://" in endpoint:
        host = urlparse(endpoint).hostname
        return host
    return endpoint.split(":", 1)[0].strip("[]")


def _extend_no_proxy(*endpoints):
    entries = [
        "localhost",
        "127.0.0.1",
        "::1",
        "0.0.0.0",
        "host.docker.internal",
    ]
    for endpoint in endpoints:
        host = _host_from_endpoint(endpoint)
        if host:
            entries.append(host)

    for name in ("NO_PROXY", "no_proxy"):
        current = [item.strip() for item in os.environ.get(name, "").split(",") if item.strip()]
        merged = current[:]
        for entry in entries:
            if entry not in merged:
                merged.append(entry)
        os.environ[name] = ",".join(merged)

ROOT_DIR = Path(__file__).parent.parent
STORAGE_NODE_IP = '10.2.30.50'
GATEWAY_PORT = int(os.getenv('GATEWAY_PORT', '8000'))
GATEWAY_NOTIFY_PORT = int(os.getenv('GATEWAY_NOTIFY_PORT', '8001'))
TX_SINK_PORT = int(os.getenv('TX_SINK_PORT', '6000'))
TX_SINK_REPAIR_PORT = int(os.getenv('TX_SINK_REPAIR_PORT', '6001'))
HTTP_SERVER_BACKLOG = int(os.getenv('HTTP_SERVER_BACKLOG', '512'))

COUCHDB_PORT = int(os.getenv('COUCHDB_PORT', '5995'))
COUCHDB_URL = f'http://faasnap:faasnap@{STORAGE_NODE_IP}:{COUCHDB_PORT}'
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

GATEWAY_ADDR = f'{STORAGE_NODE_IP}:{GATEWAY_PORT}' # need to update as your private_ip
GATEWAY_NOTIFY_ADDR = f'{STORAGE_NODE_IP}:{GATEWAY_NOTIFY_PORT}'
# ... (后面的内容保持不变) ...

VALIDATOR_ADDR = f'{STORAGE_NODE_IP}:9000'
WORKERSP_PORT = '7500'

_extend_no_proxy(
    STORAGE_NODE_IP,
    COUCHDB_URL,
    DYNAMODB_URL,
    REDIS_HOST,
    GATEWAY_ADDR,
    GATEWAY_NOTIFY_ADDR,
    VALIDATOR_ADDR,
)


# workflow setting
# workflow setting
WORKFLOW_YAML_ADDR = {
                   # 'textseq': f"{ROOT_DIR}/benchmark/textseq",
                    #  'c2': f"{ROOT_DIR}/benchmark/micro_benchmark/c2",
                    #   'c4': f"{ROOT_DIR}/benchmark/micro_benchmark/c4",
                    #    'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                    #  'c16': f"{ROOT_DIR}/benchmark/micro_benchmark/c16",
                    #    'w2': f"{ROOT_DIR}/benchmark/micro_benchmark/w2",
                    #     'w4': f"{ROOT_DIR}/benchmark/micro_benchmark/w4",
                     #    'w6': f"{ROOT_DIR}/benchmark/micro_benchmark/w6",
                    #     'w8': f"{ROOT_DIR}/benchmark/micro_benchmark/w8",
                    #'travel_reservation': f"{ROOT_DIR}/benchmark/travel_reservation",
                     # 'banking_system': f"{ROOT_DIR}/benchmark/banking_system",   
                    'repair_correctness': f"{ROOT_DIR}/experiment/debug_tests/repair_correctness/benchmark",
                    #'social_network': f"{ROOT_DIR}/benchmark/social_network",  
                    }
DEFAULT_CONTAINER_NUM = 32
# cache setting
CACHE_ENABLED = True
CLEAR_MEM = True
FILLUP_CACHE = False
EXPIRED_CACHE = True

# validator setting
VALIDATORS_PER_POOL = 4
VALIDATE_INTERVAL = 0.01
BATCH_TIMEOUT = 0.015
ABORT_PROB = 0

# batch setting
BATCH_SIZE = 1

# mode setting
FAST_PATH = True
OPTIMISTIC_REPAIR = True


# repair setting
RUNNING = '1'
REPAIRED = '2'
ABORTED = '3'

OPT_REPAIR = 1
PESSI_REPAIR = 2

CLEAR_MEM = True

# used in scalabiliy test.
SCALABILITY_TEST = False
TRACE_TEST = True
FAKE_SINK_URL =  f'http://{STORAGE_NODE_IP}:6000/fake_repair_pessi'
FAKE_NOTIFY_URL = f'http://{STORAGE_NODE_IP}:8000/fake_notify'
FAKE_REQUEST_URL = f'http://{STORAGE_NODE_IP}:8000/fake_request'
_extend_no_proxy(FAKE_SINK_URL, FAKE_NOTIFY_URL, FAKE_REQUEST_URL)

# microbenchmark configuration
DB_SIZE = 10000
DATA_ITEM_SIZE = 4 * 1024 

# travel reservation
FLIGHT_IDS = 50			
FLIGHT_CAPACITY = "100"			
RENTAL_START = '2025-07-01'			
RENTAL_END = '2025-07-31'			
CAR_NUM = '300'			
DATE_FORMAT = "%Y-%m-%d"			
			
# banking system			
BANKING_ACCOUNTS = 50
BANKING_ORIGINAL_BALANCE = "10000"			
LOGIN_FAIL_PROB = 0			
			
# social network			
SOCIAL_NETWORK_USERS = 50			
STARTUP_POSTS = 1
