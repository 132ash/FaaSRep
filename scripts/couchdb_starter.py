import couchdb
import time

time.sleep(2)
db = couchdb.Server('http://faasnap:faasnap@127.0.0.1:5984')

for d in ["workflow_latency", "common", "results", "log"]:
    if d not in db:
        db.create(d)

