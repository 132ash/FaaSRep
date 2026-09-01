#!/usr/bin/env python3
"""Non-destructively create/check tables required by Boki-style-SN."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

import boto3
from config import config
from src.storage_schema import SHADOW_TABLE_NAME, ensure_shadow_table


def dynamo_resource():
    return boto3.resource('dynamodb', endpoint_url=config.DYNAMODB_URL,
                          aws_secret_access_key=config.DYNAMODB_ACCESS_KEY,
                          aws_access_key_id=config.DYNAMODB_KEY_ID,
                          region_name=config.DYNAMODB_AREA)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true',
                        help='only verify the table; do not create it')
    args = parser.parse_args()
    resource = dynamo_resource()
    if args.check:
        description = resource.meta.client.describe_table(TableName=SHADOW_TABLE_NAME)
        result = {'table_name': SHADOW_TABLE_NAME, 'created': False,
                  'table_status': description['Table']['TableStatus']}
    else:
        result = ensure_shadow_table(resource)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
