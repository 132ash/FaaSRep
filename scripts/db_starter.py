import couchdb
import time

time.sleep(2)
db = couchdb.Server('http://faasnap:faasnap@127.0.0.1:5984')

for d in ["workflow_latency", "common", "results", "log", "data"]:
    if d not in db:
        db.create(d)


def save_data(db, key, version, value):
    doc = {
        '_id': key,
        'version': version,
        'value': value
    }
    db.save(doc)

save_data(db["data"], "test_value", "Txid:CommitStamp", 1)

