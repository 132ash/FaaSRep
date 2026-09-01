"""Database adapter deliberately isolated from the in-memory shadow state."""
from __future__ import annotations

import boto3


class DynamoDBRepository:
    def __init__(self, endpoint_url, access_key, secret_key, region):
        self.resource = boto3.resource('dynamodb', endpoint_url=endpoint_url,
                                       aws_access_key_id=access_key,
                                       aws_secret_access_key=secret_key,
                                       region_name=region)
        self.table = self.resource.Table('data')

    def put(self, key, value, version):
        self.table.put_item(Item={'key': key, 'value': value, 'version': version})
