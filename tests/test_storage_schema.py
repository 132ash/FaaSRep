from botocore.exceptions import ClientError

from src.storage_schema import SHADOW_TABLE_NAME, ensure_shadow_table


class Waiter:
    def __init__(self):
        self.waited_for = None

    def wait(self, **kwargs):
        self.waited_for = kwargs


class Client:
    def __init__(self, missing=False):
        self.missing = missing
        self.waiter = Waiter()

    def describe_table(self, **_kwargs):
        if self.missing:
            self.missing = False
            raise ClientError({'Error': {'Code': 'ResourceNotFoundException'}}, 'DescribeTable')
        return {'Table': {'TableStatus': 'ACTIVE'}}

    def get_waiter(self, name):
        assert name == 'table_exists'
        return self.waiter


class Table:
    table_status = 'CREATING'


class Resource:
    def __init__(self, missing=False):
        self.meta = type('Meta', (), {'client': Client(missing)})()
        self.created = None

    def create_table(self, **kwargs):
        self.created = kwargs
        return Table()


def test_shadow_table_is_created_only_when_missing():
    resource = Resource(missing=True)
    result = ensure_shadow_table(resource)
    assert result == {'table_name': SHADOW_TABLE_NAME, 'created': True, 'table_status': 'ACTIVE'}
    assert resource.created['TableName'] == SHADOW_TABLE_NAME
    assert resource.created['KeySchema'][0] == {'AttributeName': 'txid', 'KeyType': 'HASH'}
    assert resource.meta.client.waiter.waited_for == {'TableName': SHADOW_TABLE_NAME}


def test_existing_shadow_table_is_not_recreated():
    resource = Resource(missing=False)
    result = ensure_shadow_table(resource)
    assert result['created'] is False
    assert resource.created is None
