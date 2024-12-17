from typing import Any, List
import couchdb
import sys

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)

    # get all function_name for every node seems to solve the problem of KeyError Exception in manager.py, line 103
    def get_current_node_functions(self, ip: str, mode: str) -> List[str]:
        db = self.couch[mode]
        functions = []
        for item in db:
            functions.append(db[item]['function_name'])
        return functions
    
    def get_initial_data_version(self):
        db = self.couch['data']
        initial_data = {}
        for item in db:
            initial_data[item] = db[item]['version']
        return initial_data

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
    