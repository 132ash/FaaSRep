# basic settings
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent
STOREGE_NODE_IP = '10.2.27.22'

COUCHDB_URL = f'http://faasnap:faasnap@{STOREGE_NODE_IP}:5984'
DYNAMODB_URL = f'http://{STOREGE_NODE_IP}:4567'
DYNAMODB_KEY_ID = 'FAASNAPDYNAMODB'
DYNAMODB_ACCESS_KEY = 'FAASNAPDYNAMODBKEY'
DYNAMODB_AREA = 'us-west-2'
REDIS_HOST = '127.0.0.1' # it serves to connect with the local redis, so it should be 127.0.0.1
REDIS_PORT = 6379 # it follows the same configuration as created redis by docker (e.g., -p 6379:6379)
SHADOWTABLE_DB = 0
CACHE_DB = 1
GATEWAY_ADDR = f'{STOREGE_NODE_IP}:8000' # need to update as your private_ip
VALIDATOR_ADDR = f'{STOREGE_NODE_IP}:9000'
WORKERSP_PORT = '7500'


# workflow setting
WORKFLOW_YAML_ADDR = {
                    # 'textseq': f"{ROOT_DIR}/benchmark/textseq",
                    # 'c2': f"{ROOT_DIR}/benchmark/micro_benchmark/c2",
                    # 'c4': f"{ROOT_DIR}/benchmark/micro_benchmark/c4",
                    # 'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                    # 'c16': f"{ROOT_DIR}/benchmark/micro_benchmark/c16",
                    # 'w2': f"{ROOT_DIR}/benchmark/micro_benchmark/w2",
                    # 'w4': f"{ROOT_DIR}/benchmark/micro_benchmark/w4",
                    # 'w8': f"{ROOT_DIR}/benchmark/micro_benchmark/w8",
                    # 'w16': f"{ROOT_DIR}/benchmark/micro_benchmark/w16",
                    'travel_reservation': f"{ROOT_DIR}/benchmark/travel_reservation",
                    # 'c4': '/home/shao/FaaSnap/benchmark/micro_benchmark/c4',
                    # "sectestflow": '/home/ash/FaaSnap/benchmark/sectestflow/workflow.yaml',
                    # "testflow": "/home/ash/FaaSnap/benchmark/testflow/workflow.yaml"
                    # #   'simpleseq': '/home/ash/FaaSnap/benchmark/simpleseq/workflow.yaml'         
                      }

DEFAULT_CONTAINER_NUM = 9
CLEAR_MEM = True

# app configuration

# travel reservation
FLIGHT_IDS = 100
FLIGHT_CAPACITY = "100"
RENTAL_START = '2025-07-01'
RENTAL_END = '2025-07-31'
CAR_NUM = '100'
DATE_FORMAT = "%Y-%m-%d"



