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
WORKFLOW_YAML_ADDR = {"sectestflow": '/home/ash/FaaSnap/benchmark/sectestflow/workflow.yaml',
                    "testflow": "/home/ash/FaaSnap/benchmark/testflow/workflow.yaml"
                    #   'simpleseq': '/home/ash/FaaSnap/benchmark/simpleseq/workflow.yaml'         
                      }
FUNCTION_INFO_ADDRS = { 'sectestflow': '../../../../benchmark/sectestflow',
                        'testflow': '../../../../benchmark/testflow'
                    #    'simpleseq': '../../../../benchmark/simpleseq'
                       }
# cache setting
CLEAR_MEM = True
FILLUP_CACHE = True
EXPIRED_CACHE = True
DEFAULT_CONTAINER_NUM = 1

# batch setting
BATCH_SIZE = 1
BATCH_INTERVAL = 0.05
