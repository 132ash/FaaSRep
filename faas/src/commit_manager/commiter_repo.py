from gevent import monkey
monkey.patch_all()
from typing import Dict, List, Any
import couchdb
import redis
import sys

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)

    def get_all_functions(self, workflow_name: str) -> List[str]:
        db = self.couch[workflow_name+ '_function_info']
        functions = []
        for item in db:
            functions.append(db[item]['function_name'])
        return functions

    def get_function_info(self, function_name: str, workflow_name: str) -> Any:
        db = self.couch[workflow_name+ '_function_info']
        for item in db.find({'selector': {'function_name': function_name}}):
            return item