class SubjectionTable:
    def __init__(self):
        # downstream function table: {txid:{func: {cnt, key:{upstream_func；xx, upstream_ip:xx}}}}, cnt is the number of functions it subject to.
        self.downstream_func_table = {}
        # upstream function table: {txid: {func:{key:[(func,ip)]}}, for each key it writes, recording the functions subject to it.
        # { next_func: {txid: {func: [{func_name:xxx, ip:xx, transaction_id, xxx, workflow_name:xx},...]},  
        #   next_dict: {txid: {func: {downstream_tx_id:True}}}
        # }
        self.upstream_func_table = {}

    def init(self, batch_id):
        self.downstream_func_table[batch_id] = {}
        self.upstream_func_table[batch_id] = {}

    # downstream function needs:
    # 1. cnt of upstream functions to know when to run
    # 2. func name and ip to get upstream data 
    def add_downstream_func_key(self, batch_id, upstream_tx_id, downstream_tx_id, upstream_func, downstream_func, upstream_ip, key):
        downstream_func_table = self.downstream_func_table[batch_id].get(downstream_tx_id, {})
        downstream_func_info = downstream_func_table.get(downstream_func, {})
        cnt = downstream_func_info.get("cnt", 0)
        downstream_func_info["cnt"] = cnt + 1
        downstream_func_info[key] = {"upstream_tx_id":upstream_tx_id, "upstream_func":upstream_func, "upstream_ip":upstream_ip}
        downstream_func_table[downstream_func] = downstream_func_info
        self.downstream_func_table[batch_id][downstream_tx_id] = downstream_func_table
         

    # upstream function needs:
    # 1. list of function-ip pairs waiting for each key.
    def add_upstream_func_key(self, batch_id, downstream_workflow_name, upstream_tx_id, downstream_tx_id, upstream_func, downstream_func, downstream_ip):
        next_funcs_per_batch = self.upstream_func_table[batch_id].get("next_func", {})
        next_dicts_per_batch = self.upstream_func_table[batch_id].get("next_dict", {})
        next_funcs_in_tx = next_funcs_per_batch.get(upstream_tx_id, {})
        next_dicts_in_tx = next_dicts_per_batch.get(upstream_tx_id, {})
        next_func = next_funcs_in_tx.get(upstream_func, [])
        next_dict = next_dicts_in_tx.get(upstream_func, {})
       
        if downstream_tx_id not in next_dict:
            next_dict[downstream_tx_id] = {}
        if downstream_func not in next_dict[downstream_tx_id]:
            next_dict[downstream_tx_id][downstream_func] = True  
            next_func.append({"downstream_tx_id":downstream_tx_id,"downstream_workflow_name":downstream_workflow_name, "downstream_func":downstream_func, "downstream_ip":downstream_ip})
        next_funcs_in_tx[upstream_func] = next_func
        next_dicts_in_tx[upstream_func] = next_dict
        self.upstream_func_table[batch_id]["next_func"] = next_funcs_in_tx
        self.upstream_func_table[batch_id]["next_dict"] = next_dicts_in_tx

    def update_subjection_table(self,batch_id, downstream_workflow_name, upstream_tx_id,  downstream_tx_id, upstream_func, downstream_func, upstream_ip, downstream_ip, key):
        self.add_downstream_func_key(batch_id, upstream_tx_id, downstream_tx_id, upstream_func, downstream_func, upstream_ip, key)
        self.add_upstream_func_key(batch_id, downstream_workflow_name, upstream_tx_id, downstream_tx_id, upstream_func,downstream_func, downstream_ip)
    
    def get_table_for_batch(self, batch_id):
        return self.downstream_func_table[batch_id], self.upstream_func_table[batch_id]["next_func"]

    def clean_table_of_batch(self, batch_id):
        self.downstream_func_table.pop(batch_id, None)
        self.upstream_func_table.pop(batch_id, None)