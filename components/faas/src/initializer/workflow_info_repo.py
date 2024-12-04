import couchdb
import redis
from typing import Dict, List
import sys

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL

class Repository:
    def __init__(self, workflow_name, remove_old_db=True):
        self.couch = couchdb.Server(couchdb_url)
        if remove_old_db:
            db_list = [workflow_name + '_function_info', workflow_name + '_function_info_raw', workflow_name + '_workflow_metadata']
            for db_name in db_list:
                if db_name in self.couch:
                    self.couch.delete(db_name)

    def save_function_info(self, function_info, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        for name in function_info:
            db[name] = function_info[name]

    def save_all_addrs(self, addrs, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'addrs': list(addrs)})

    def save_start_functions(self, start_functions, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'start_functions': start_functions})

    def save_end_function(self, end_function, db_name):
        if db_name not in self.couch:
            self.couch.create(db_name)
        db = self.couch[db_name]
        db.save({'end_function': end_function})