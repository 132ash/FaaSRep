# basic settings
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent
STORAGE_NODE_IP = '10.2.29.142'

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
                    #  'c4': f"{ROOT_DIR}/benchmark/micro_benchmark/c4",
                    #    'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                    # 'c16': f"{ROOT_DIR}/benchmark/micro_benchmark/c16",
                    #   'w2': f"{ROOT_DIR}/benchmark/micro_benchmark/w2",
                    #    'w4': f"{ROOT_DIR}/benchmark/micro_benchmark/w4",
                    #    'w8': f"{ROOT_DIR}/benchmark/micro_benchmark/w8",
                    #    'w16': f"{ROOT_DIR}/benchmark/micro_benchmark/w16",
                    'travel_reservation': f"{ROOT_DIR}/benchmark/travel_reservation",
                    'banking_system': f"{ROOT_DIR}/benchmark/banking_system",   
                    'social_network': f"{ROOT_DIR}/benchmark/social_network",  
                    }
# cache setting
CACHE_ENABLED = True
CLEAR_MEM = True
FILLUP_CACHE = False
EXPIRED_CACHE = True

# validator setting
VALIDATORS_PER_POOL = 4
VALIDATE_INTERVAL = 0.015
BATCH_TIMEOUT = 0.045

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

DEFAULT_CONTAINER_NUM = 32
CLEAR_MEM = True

# microbenchmark configuration
DB_SIZE = 10000
DATA_ITEM_SIZE = 4 * 1024

# travel reservation
FLIGHT_IDS = 100
FLIGHT_CAPACITY = "100"
RENTAL_START = '2025-07-01'
RENTAL_END = '2025-08-31'
CAR_NUM = '200'
DATE_FORMAT = "%Y-%m-%d"

# banking system
BANKING_ACCOUNTS = 100
BANKING_ORIGINAL_BALANCE = "10000"
LOGIN_FAIL_PROB = 0

# social network
SOCIAL_NETWORK_USERS = 300
STARTUP_POSTS = 3





