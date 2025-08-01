# basic settings
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent
STOREGE_NODE_IP = '10.2.27.24'

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
                    'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                    # 'c4': '/home/shao/FaaSnap/benchmark/micro_benchmark/c4',
                    # "sectestflow": '/home/ash/FaaSnap/benchmark/sectestflow/workflow.yaml',
                    # "testflow": "/home/ash/FaaSnap/benchmark/testflow/workflow.yaml"
                    # #   'simpleseq': '/home/ash/FaaSnap/benchmark/simpleseq/workflow.yaml'         
                      }
FUNCTION_INFO_ADDRS = {
                        #  'textseq': f"{ROOT_DIR}/benchmark/textseq",
                          'c8': f"{ROOT_DIR}/benchmark/micro_benchmark/c8",
                        #  'c4': '/home/shao/FaaSnap/benchmark/micro_benchmark/c4'
#                         'sectestflow': '../../../../benchmark/sectestflow',
#                         'testflow': '../../../../benchmark/testflow',
                       }
# cache setting
CLEAR_MEM = True
FILLUP_CACHE = True
EXPIRED_CACHE = True
DEFAULT_CONTAINER_NUM = 6

# validator setting
VALIDATORS_PER_POOL = 2

# batch setting
BATCH_SIZE = 3

# mode setting
FAST_PATH = True
OPTIMISTIC_REPAIR = False

# repair setting
RUNNING = '1'
REPAIRED = '2'
ABORTED = '3'

OPT_REPAIR = 1
PESSI_REPAIR = 2





