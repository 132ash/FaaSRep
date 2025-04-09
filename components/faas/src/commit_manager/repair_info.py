from gevent import monkey
monkey.patch_all()

class RepairInfo:
    def __init__(self):
        # downstream function table: {txid:{func: {cnt, key:{upstream_func；xx, upstream_ip:xx}}}}, cnt is the number of functions it subject to.

        # upstream function table: {txid: {func:{key:[(func,ip)]}}, for each key it writes, recording the functions subject to it.
        # { next_func: {txid: {func: [{func_name:xxx, ip:xx, transaction_id, xxx, workflow_name:xx},...]},  
        #   next_dict: {txid: {func: {downstream_tx_id:True}}}
        # }
        self.repair_metadata_per_batch = {}

        # preventing adding the same function to downstream subjection table
        # {batchid:{txid:{func:{successor_txid:{func_name:True}}}}}
        self.downstream_func_dict = {}

        # modify in a whole table: {batch_id: {ip:{txid:{func:{RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}}}

    def batch_init(self, batch_id):
        self.repair_metadata_per_batch[batch_id] = {}
        self.downstream_func_dict[batch_id] = {}

        # self.downstream_func_table[batch_id] = {}
        # self.upstream_func_table[batch_id] = {"next_func":{}, "next_dict":{}}

    # downstream function needs:
    # 1. cnt of upstream functions to know when to run
    # 2. func name and ip to get upstream data 
    def add_downstream_func_key(self, batch_id, upstream_tx_id, downstream_tx_id, upstream_func, downstream_func, downstream_ip, key):
        func_dict = self.repair_metadata_per_batch[batch_id].setdefault(downstream_ip, {}).setdefault(downstream_tx_id, {}).setdefault(downstream_func, {"RYW":{}, "dirty":False, "downstream": {"up_cnt": 0, "upstream_keys": {}}, "upstream":[]})
        downstream_dict = func_dict["downstream"]
        downstream_dict["up_cnt"] += 1
        downstream_dict["upstream_keys"].setdefault(key, {"transaction_id": upstream_tx_id, "func": upstream_func})

    def add_upstream_func_key(self, batch_id, upstream_tx_id, downstream_tx_id, upstream_func, downstream_func, upstream_ip):
        downstream_func_dict = self.downstream_func_dict[batch_id].setdefault(upstream_tx_id, {}).setdefault(upstream_func, {}).setdefault(downstream_tx_id, {})
        upstream_func_successor = self.repair_metadata_per_batch[batch_id].setdefault(upstream_ip, {}).setdefault(upstream_tx_id, {}).setdefault(upstream_func, {"RYW":{}, "dirty":False, "downstream": {"up_cnt": 0, "upstream_keys": {}}, "upstream":[]})  
        if not downstream_func_dict.get(downstream_func, False): 
            downstream_func_dict[downstream_func] = True
            upstream_func_successor['upstream'].append({"transaction_id": downstream_tx_id, "function_name": downstream_func})

    def update_crosstx_subjection_table(self, batch_id, downstream_workflow_name, upstream_tx_id,  downstream_tx_id, upstream_func, downstream_func, upstream_ip, downstream_ip, key):
        print(f"update subjection table: batch_id:{batch_id}, downstream_workflow_name:{downstream_workflow_name}, upstream_tx_id:{upstream_tx_id}, downstream_tx_id:{downstream_tx_id}, upstream_func:{upstream_func}, downstream_func:{downstream_func}, upstream_ip:{upstream_ip}, downstream_ip:{downstream_ip}, key:{key}")
        self.add_downstream_func_key(batch_id, upstream_tx_id, downstream_tx_id, upstream_func, downstream_func, downstream_ip, key)
        self.add_upstream_func_key(batch_id, upstream_tx_id, downstream_tx_id, upstream_func,downstream_func, upstream_ip)

    def update_introtx_RYW_subjection_table(self, batch_id, ip, tx_id, write_func, RYW_subjection):
        func_dict = self.repair_metadata_per_batch[batch_id].setdefault(ip, {}).setdefault(tx_id, {}).setdefault(write_func, {"RYW":{}, "dirty":False, "downstream": {"up_cnt": 0, "upstream_keys": {}}, "upstream":[]})
        func_dict['RYW'] = RYW_subjection

    def target_function_dirty(self, batch_id, ip, tx_id, func):
        func_dict = self.repair_metadata_per_batch[batch_id].setdefault(ip, {}).setdefault(tx_id, {}).setdefault(func, {"RYW":{}, "dirty":False, "downstream": {"up_cnt": 0, "upstream_keys": {}}, "upstream":[]})
        func_dict['dirty'] = True
    
    def get_repair_metadata_for_ip(self, batch_id, ip):
        return self.repair_metadata_per_batch[batch_id].get(ip, {})

    def clean_table_of_batch(self, batch_id):
        self.repair_metadata_per_batch.pop(batch_id, None)
        self.downstream_func_dict.pop(batch_id, None)