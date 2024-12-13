from typing import Any, List
import couchdb
import redis
import threading
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
    
    def get_initial_data(self):
        db = self.couch['initial_data']
        for item in db:
            doc = db[item]
            return doc

    