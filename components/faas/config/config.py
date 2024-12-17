COUCHDB_URL = 'http://faasnap:faasnap@192.168.162.132:5984'
REDIS_HOST = '127.0.0.1' # it serves to connect with the local redis, so it should be 127.0.0.1
REDIS_PORT = 6379 # it follows the same configuration as created redis by docker (e.g., -p 6379:6379)
REDIS_DB = 0
CACHE_DB = 1
GATEWAY_ADDR = '192.168.162.132:8000' # need to update as your private_ip

WORKFLOW_YAML_ADDR = {"testflow": "/home/ash/FaaSnap/benchmark/testflow/workflow.yaml"}
# get function image info for container initialization
FUNCTION_INFO_ADDRS = {'testflow': '../../../../benchmark/testflow'}
CLEAR_DB_AND_MEM = False
DEFAULT_CONTAINER_NUM = 1
