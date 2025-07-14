
import requests
batch =   {'batch_id': 'c359de70-d1ed-46c4-b535-2d82714c00a2', 
                   'read_set': {'c359de70-d1ed-46c4-b535-2d82714c00a2': 
                                {'f1': 
                                 {'t1': '1970-01-01 00:00:00.000000', 't2': '1970-01-01 00:00:00.000000'}, 'f2': {'t5': '1970-01-01 00:00:00.000000'}, 'f3': {'t8': '1970-01-01 00:00:00.000000'}, 'f4': {'t11': '1970-01-01 00:00:00.000000'}, 'f5': {'t14': '1970-01-01 00:00:00.000000'}}},
                                 'write_set': {'c359de70-d1ed-46c4-b535-2d82714c00a2': {'t3': 'f1', 't6': 'f2', 't9': 'f3', 't12': 'f4', 't15': 'f5'}}, 'RYW_subjection': {'c359de70-d1ed-46c4-b535-2d82714c00a2': {'f1': {}, 'f2': {'t3': 'f1'}, 'f3': {'t6': 'f2'}, 'f4': {'t9': 'f3'}, 'f5': {'t12': 'f4'}}}, 'container_port': {'c359de70-d1ed-46c4-b535-2d82714c00a2': 
                                 {'f1': 20000, 'f2': 20001, 'f3': 20002, 'f4': 20003, 'f5': 20004}}, 'transaction_list': ['c359de70-d1ed-46c4-b535-2d82714c00a2']}

remote_url = 'http://192.168.162.132:9000/validate'
data = {
    'workflow_name': 'textseq',
    "batch": batch,
    "batch_id": batch["batch_id"],
    'first_run_finish_time': 1752485578.817894
}
requests.post(remote_url, json=data) 