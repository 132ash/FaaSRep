from gevent import monkey
monkey.patch_all()

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
        

    def get_initial_global_table(self):
        table = self.dynamo.Table('data')
        response = table.scan()
        items = response.get('Items', [])
        global_table_dict = {}
        try:
            for item in items:
                global_table_dict[item['key']] = item['version']
        except KeyError:
            for i in items:
                print(i['key'])
                print(i.keys())
        return global_table_dict

    def get_start_functions(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'start_functions' in doc:
                return doc['start_functions']
            
    def get_end_function(self, workflow_name) -> str:
        db = self.couch[workflow_name+ '_workflow_metadata']
        for item in db:
            doc = db[item]
            if 'end_function' in doc:
                return doc['end_function']['name']
        
            
    def get_all_functions(self, workflow_name) -> List[str]:
        db = self.couch[workflow_name+ '_function_info']
        functions = []
        for item in db:
            functions.append(db[item]['function_name'])
        return functions
            
    def get_function_info(self, all_functions, workflow_name) -> Any:
        db = self.couch[workflow_name + '_function_info']
        function_info = {}
        for function_name in all_functions:
            for it in db.find({'selector': {'function_name': function_name}}):
                function_info[function_name] = it
        return function_info

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']
            
    def save_latency(self, log):
        latency_db = self.couch['workflow_latency']
        latency_db.save(log)

    def sync_shadow_to_data_db_with_version(self, transaction_id, version):
        shadow_table_name = f"{transaction_id}_shadow_table"
        shadow_table = self.dynamo.Table(shadow_table_name)
        data_db = self.dynamo.Table('data')

        # 扫描 shadow table 中的所有数据
        response = shadow_table.scan()
        items = response.get('Items', [])

        for item in items:
            key = item['key']
            value = item['value']  # Ensure value is stored as a string
            # only flush the items func write.
            if not key.startswith('RET'):
                data_db.update_item(
                    Key={'key': key},
                    UpdateExpression="SET #v = :value, #ver = :version",
                    ExpressionAttributeNames={
                        '#v': 'value',
                        '#ver': 'version'
                    },
                    ExpressionAttributeValues={
                        ':value': value,
                        ':version': version
                    },
                    ReturnValues="UPDATED_NEW"
                )

    
if __name__ == '__main__':
    repo = Repository()
    print(repo.get_initial_data_version())