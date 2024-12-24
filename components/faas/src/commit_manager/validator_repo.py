from typing import Any, List
import couchdb
import sys
import boto3

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL
dynamodb_url = config.DYNAMODB_URL
dynamodb_key_id = config.DYNAMODB_KEY_ID
dynamodb_access_key = config.DYNAMODB_ACCESS_KEY
dynamodb_area = config.DYNAMODB_AREA

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)
        self.dynamo = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)


    def get_initial_data_version(self):
        table = self.dynamo.Table('data')
        response = table.scan()
        items = response.get('Items', [])
        try:
            key_version_dict = {item['key']: item['version'] for item in items}
        except KeyError:
            for i in items:
                print(i['key'])
                print(i.keys())
        return key_version_dict

    def get_start_functions(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'start_functions' in doc:
                return doc['start_functions']

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']
            
    def save_latency(self, log):
        latency_db = self.couch['workflow_latency']
        latency_db.save(log)

    
if __name__ == '__main__':
    repo = Repository()
    print(repo.get_initial_data_version())