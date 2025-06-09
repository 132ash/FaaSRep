import sys
sys.path.append('../../config')
import config

class RepairInfo:
    def __init__(self, function_info):
        self.function_info = function_info
        self.fast_path_enabled = config.REPAIR and config.FAST_PATH
        # downstream function table: {txid:{func: {cnt, key:{upstream_func；xx, upstream_ip:xx}}}}, cnt is the number of functions it subject to.

        # upstream function table: {txid: {func:{key:[(func,ip)]}}, for each key it writes, recording the functions subject to it.
        # { next_func: {txid: {func: [{func_name:xxx, ip:xx, transaction_id, xxx, workflow_name:xx},...]},  
        #   next_dict: {txid: {func: {downstream_tx_id:True}}}
        # }

        self.repair_metadata_per_batch_by_ip = {}
        self.repair_metadata_per_batch_by_txid = {}

        # preventing adding the same function to downstream subjection table
        # {batchid:{txid:{func:{successor_txid:{func_name:True}}}}}

        # modify in a whole table: {batch_id: {ip:{txid:{func:{RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}}}

    def batch_init(self, batch_id):
        if self.fast_path_enabled:
            self.repair_metadata_per_batch_by_ip[batch_id] = {}
        else:
            self.repair_metadata_per_batch_by_txid[batch_id] = {}

        # self.downstream_func_table[batch_id] = {}
        # self.upstream_func_table[batch_id] = {"next_func":{}, "next_dict":{}}

    def construct_repair_metadata(self, batch_id, expired_keys, crosstx_subjection, RYW_subjection, function_pos_per_tx, worker_set, txid_list):
        
        expired_keys_per_ip = {ip:set() for ip in worker_set}
        for tx_id in txid_list:
            for func in self.function_info:
                func_ip = function_pos_per_tx[tx_id][func]['ip']
                tx_dict = self.get_info_dict(batch_id, func_ip, tx_id)
                crosstx_info = crosstx_subjection.get(tx_id, {}).get(func, {})
                tx_dict[func] = crosstx_info if crosstx_info else {}
                func_info_dict = tx_dict[func]

                # RYW info
                RYW_sub = RYW_subjection.get(tx_id, {}).get(func, {})
                if RYW_sub:
                    func_info_dict['RYW_keys'] = RYW_sub
                    for key, introtx_upstream_func in RYW_sub.items():
                        # Remove keys from func_info_dict['upstream_keys'] if they appear in RYW_keys
                        if key in func_info_dict['upstream_keys']:
                            func_info_dict['upstream_keys'].pop(key)
                            func_info_dict['up_cnt'] -= 1
                        upstream_func_ip = function_pos_per_tx[tx_id][introtx_upstream_func]['ip']
                        upstream_func_dict = self.get_info_dict(batch_id, upstream_func_ip, tx_id, introtx_upstream_func)
                        func_info_dict['dirty'] = upstream_func_dict.get('dirty', False) 
                        # this key is RYW, remove from expired keys.
                        expired_keys.get(tx_id, {}).get(func, {}).pop(key, None)

                if self.function_info[func]['next'][0] == 'END':
                    func_info_dict['successor_pos'] = {'END':{}}
                else:
                    func_info_dict['successor_pos'] = {f:{function_pos_per_tx[tx_id][f]} for f in self.function_info[func]['next']}
                

                expired_keys_dict = expired_keys.get(tx_id, {}).get(func, {})
                expired_keys_per_ip[func_ip].union(set(expired_keys_dict.keys()))
        return expired_keys_per_ip



    
    def get_info_dict(self, batch_id, ip, tx_id, func=''):
        if self.fast_path_enabled:
            tx_dict =  self.repair_metadata_per_batch_by_ip[batch_id].setdefault(ip, {}).setdefault(tx_id, {})
        else:
             tx_dict = self.repair_metadata_per_batch_by_txid[batch_id].setdefault(tx_id, {})
        return tx_dict.setdefault(func, {}) if func else tx_dict
        
    def get_repair_metadata(self, batch_id, ip="", txid=""):
        if self.fast_path_enabled:
            return self.repair_metadata_per_batch_by_ip[batch_id].get(ip, {})
        else:
            return self.repair_metadata_per_batch_by_txid[batch_id].get(txid, {})
    
    def clean_table_of_batch(self, batch_id):
        self.repair_metadata_per_batch_by_ip.pop(batch_id, None)
        self.repair_metadata_per_batch_by_txid.pop(batch_id, None)