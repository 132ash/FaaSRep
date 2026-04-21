import os
from urllib.parse import urlparse


def _host_from_endpoint(endpoint):
    if not endpoint:
        return None
    endpoint = str(endpoint)
    if "://" in endpoint:
        return urlparse(endpoint).hostname
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


STORAGE_NODE_IP = '10.2.30.50'
HTTP_SERVER_BACKLOG = int(os.getenv('HTTP_SERVER_BACKLOG', '512'))

COUCHDB_PORT = int(os.getenv('COUCHDB_PORT', '5995'))
COUCHDB_URL = f'http://faasnap:faasnap@{STORAGE_NODE_IP}:{COUCHDB_PORT}'
DATADB_URL = ""
CACHE_HOST = '172.17.0.1' 
REDIS_PORT = 6379
REDIS_CACHE_PORT = 6380
REDIS_SHADOW_TABLE_DB = 0
REDIS_CACHE_DB = 1
DYNAMODB_URL = f'http://{STORAGE_NODE_IP}:4567'
DYNAMODB_KEY_ID = 'FAASNAPDYNAMODB'
DYNAMODB_ACCESS_KEY = 'FAASNAPDYNAMODBKEY'
DYNAMODB_AREA = 'us-west-2'

GATEWAY_ADDR = f'{STORAGE_NODE_IP}:8000'

_extend_no_proxy(
    STORAGE_NODE_IP,
    COUCHDB_URL,
    DYNAMODB_URL,
    CACHE_HOST,
    GATEWAY_ADDR,
)

RUNNING = '1'
REPAIRED = '2'
ABORTED = '3'

OPT_REPAIR = 1
PESSI_REPAIR = 2

# workflow setting

REMOTE_PROB = 0
