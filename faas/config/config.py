# basic settings
COUCHDB_URL = 'http://faasnap:faasnap@192.168.162.132:5984'
DYNAMODB_URL = 'http://192.168.162.132:4567'
DYNAMODB_KEY_ID = 'FAASNAPDYNAMODB'
DYNAMODB_ACCESS_KEY = 'FAASNAPDYNAMODBKEY'
DYNAMODB_AREA = 'us-west-2'
REDIS_HOST = '127.0.0.1' # it serves to connect with the local redis, so it should be 127.0.0.1
REDIS_PORT = 6379 # it follows the same configuration as created redis by docker (e.g., -p 6379:6379)
SHADOWTABLE_DB = 0
CACHE_DB = 1
GATEWAY_ADDR = '192.168.162.132:8000' # need to update as your private_ip
VALIDATOR_ADDR = '192.168.162.132:9000'


# workflow setting
WORKFLOW_YAML_ADDR = {
                    'textseq': '../../../../benchmark/textseq',
                    # "sectestflow": '/home/ash/FaaSnap/benchmark/sectestflow/workflow.yaml',
                    # "testflow": "/home/ash/FaaSnap/benchmark/testflow/workflow.yaml"
                    # #   'simpleseq': '/home/ash/FaaSnap/benchmark/simpleseq/workflow.yaml'         
                      }
FUNCTION_INFO_ADDRS = { 
                         'textseq': '../../../../benchmark/textseq'
#                         'sectestflow': '../../../../benchmark/sectestflow',
#                         'testflow': '../../../../benchmark/testflow',
                       }
# cache setting
CLEAR_MEM = False
FILLUP_CACHE = True
EXPIRED_CACHE = True
DEFAULT_CONTAINER_NUM = 3

# validator setting
VALIDATORS_PER_POOL = 2

# batch setting
BATCH_SIZE = 1
BATCH_INTERVAL = 0.005

# mode setting
BASIC = True
REPAIR = False
FAST_PATH = False
REMOTE_LOCK = False
OPTIMISTIC_REPAIR = True
PESSIMISTIC_REPAIR = False
FAASTCC = False
CONCORD = False

# repair setting
RUNNING = 1
REPAIRED = 2
ABORTED = 3

# FaaSTCC setting
DEFAULT_SNAPSHOT_INTERVAL = []




