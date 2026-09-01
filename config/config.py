# basic settings
import os
import re
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent
STORAGE_NODE_IP = '10.2.29.142'

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
# Boki-style single-node services.  VALIDATOR_ADDR remains for the untouched
# OCC path; port 9000 is the lock manager when SYSTEM_MODE is BOKI_SN.
SYSTEM_MODE = os.environ.get('SYSTEM_MODE', 'BOKI_SN').upper()
LOCK_MANAGER_ADDR = os.environ.get('LOCK_MANAGER_ADDR', VALIDATOR_ADDR)
SHADOW_SERVICE_ADDR = os.environ.get('SHADOW_SERVICE_ADDR', f'{STORAGE_NODE_IP}:9100')
LOCK_WAIT_DEADLINE_SECONDS = float(os.environ.get('LOCK_WAIT_DEADLINE_SECONDS', '30'))
SHADOW_FLUSH_RETRY_SECONDS = float(os.environ.get('SHADOW_FLUSH_RETRY_SECONDS', '0.05'))
# Keep retries from re-entering the same hot-key collision in lockstep.  The
# gateway samples uniformly around this value, preserving it as the mean.
BOKI_RETRY_BACKOFF_SECONDS = float(os.environ.get('BOKI_RETRY_BACKOFF_SECONDS', '0.2'))
BOKI_RETRY_BACKOFF_JITTER_RATIO = float(os.environ.get('BOKI_RETRY_BACKOFF_JITTER_RATIO', '0.5'))
BOKI_WORKFLOW_WAIT_SECONDS = float(os.environ.get('BOKI_WORKFLOW_WAIT_SECONDS', '120'))
WORKERSP_PORT = '7500'


# workflow setting
# workflow setting
WORKFLOW_YAML_ADDR = {
                   # 'textseq': f"{ROOT_DIR}/benchmark/textseq",
                    # 'c2': f"{ROOT_DIR}/benchmark/micro_benchmark/c2",
                    #   'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                    # 'c16': f"{ROOT_DIR}/benchmark/micro_benchmark/c16",
                    # 'w2': f"{ROOT_DIR}/benchmark/micro_benchmark/w2",
                    #  'w4': f"{ROOT_DIR}/benchmark/micro_benchmark/w4",
                    #   'w8': f"{ROOT_DIR}/benchmark/micro_benchmark/w8",
                    #   'w16': f"{ROOT_DIR}/benchmark/micro_benchmark/w16",
                     'c4': f"{ROOT_DIR}/benchmark/micro_benchmark/c4",
                     #'travel_reservation': f"{ROOT_DIR}/benchmark/travel_reservation",
                     #'banking_system': f"{ROOT_DIR}/benchmark/banking_system",   
                    #'social_network': f"{ROOT_DIR}/benchmark/social_network",  
                    }
# cache setting
# Boki-SN never uses the shared application-data cache.  Redis/DynamoDB is
# still used for workflow-private RET/input values.
CACHE_ENABLED = False if SYSTEM_MODE == 'BOKI_SN' else True
CLEAR_MEM = True
COLLECT_FUNCTION_LATENCY = False
FILLUP_CACHE = False
EXPIRED_CACHE = True


# validator setting
VALIDATORS_PER_POOL = 4
VALIDATE_INTERVAL = 0.015
BATCH_TIMEOUT = 0.045

# batch setting
BATCH_SIZE = 4

DEFAULT_CONTAINER_NUM = 32
CLEAR_MEM = True

# microbenchmark configuration
DB_SIZE = 10000
DATA_ITEM_SIZE = 4 * 1024

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
