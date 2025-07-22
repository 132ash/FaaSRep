# basic settings
COUCHDB_URL = 'http://faasnap:faasnap@10.2.27.24:5984'
DYNAMODB_URL = 'http://10.2.27.24:4567'
DYNAMODB_KEY_ID = 'FAASNAPDYNAMODB'
DYNAMODB_ACCESS_KEY = 'FAASNAPDYNAMODBKEY'
DYNAMODB_AREA = 'us-west-2'
REDIS_HOST = '127.0.0.1' # it serves to connect with the local redis, so it should be 127.0.0.1
REDIS_PORT = 6379 # it follows the same configuration as created redis by docker (e.g., -p 6379:6379)
SHADOWTABLE_DB = 0
CACHE_DB = 1
GATEWAY_ADDR = '10.2.27.24:8000' # need to update as your private_ip


# workflow setting
WORKFLOW_YAML_ADDR = {
                    'textseq': '/home/ash/FaaSnap/benchmark/textseq',
                    # "sectestflow": '/home/ash/FaaSnap/benchmark/sectestflow/workflow.yaml',
                    # "testflow": "/home/ash/FaaSnap/benchmark/testflow/workflow.yaml"
                    # #   'simpleseq': '/home/ash/FaaSnap/benchmark/simpleseq/workflow.yaml'         
                      }
FUNCTION_INFO_ADDRS = { 
                         'textseq': '/home/ash/FaaSnap/benchmark/textseq'
#                         'sectestflow': '../../../../benchmark/sectestflow',
#                         'testflow': '../../../../benchmark/testflow',
                       }
# cache setting
CLEAR_MEM = False
FILLUP_CACHE = True
EXPIRED_CACHE = True
DEFAULT_CONTAINER_NUM = 4

# validator setting
VALIDATORS_PER_POOL = 2

# batch setting
BATCH_SIZE = 1
BATCH_INTERVAL = 0.005





