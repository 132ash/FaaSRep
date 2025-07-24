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

    def get_function_info(self, all_functions, workflow_name) -> Any:
        db = self.couch[workflow_name + '_function_info']
        function_info = {}
        for function_name in all_functions:
            for it in db.find({'selector': {'function_name': function_name}}):
                function_info[function_name] = it
        return function_info