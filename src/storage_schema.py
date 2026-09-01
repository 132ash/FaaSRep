"""Small, idempotent DynamoDB/Alternator schema helpers."""
from __future__ import annotations

from botocore.exceptions import ClientError


SHADOW_TABLE_NAME = 'shadow_table'


def ensure_shadow_table(dynamo_resource, wait=True):
    """Create the workflow-private RET table when it is absent.

    Boki-SN's application writes live in the independent shadow service, but
    the pre-existing workflow input/RET transport remains in this table when
    the shared Redis data cache is disabled.  Therefore it is still required.
    """
    client = dynamo_resource.meta.client
    try:
        description = client.describe_table(TableName=SHADOW_TABLE_NAME)
        created = False
    except ClientError as exc:
        code = exc.response.get('Error', {}).get('Code')
        if code not in {'ResourceNotFoundException', 'ResourceNotFound'}:
            raise
        table = dynamo_resource.create_table(
            TableName=SHADOW_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'txid', 'KeyType': 'HASH'},
                {'AttributeName': 'key', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'txid', 'AttributeType': 'S'},
                {'AttributeName': 'key', 'AttributeType': 'S'},
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 1000, 'WriteCapacityUnits': 1000},
        )
        description = {'Table': {'TableStatus': table.table_status}}
        created = True
    if wait:
        client.get_waiter('table_exists').wait(TableName=SHADOW_TABLE_NAME)
        description = client.describe_table(TableName=SHADOW_TABLE_NAME)
    return {'table_name': SHADOW_TABLE_NAME, 'created': created,
            'table_status': description.get('Table', {}).get('TableStatus', 'UNKNOWN')}
