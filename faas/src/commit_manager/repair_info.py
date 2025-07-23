import sys
sys.path.append('../../config')
import config
from subprocess_log import log_validator_message

class RepairInfo:
    def __init__(self, logger, workflow_graph_topo, function_pos):
        self.workflow_graph_topo = workflow_graph_topo
        self.logger = logger
        self.function_pos = function_pos
        self.fast_path_enabled = config.FAST_PATH
        self.optimistic_repair_enabled = config.OPTIMISTIC_REPAIR
        self.repair_metadata_per_batch_by_ip = {}
        self.repair_metadata_per_batch_by_txid = {}


# TODO: construct repair metadata, check code.

    def batch_init(self, batch_id):
        if self.fast_path_enabled:
            self.repair_metadata_per_batch_by_ip[batch_id] = {}
        else:
            self.repair_metadata_per_batch_by_txid[batch_id] = {}

        # self.downstream_func_table[batch_id] = {}
        # self.upstream_func_table[batch_id] = {"next_func":{}, "next_dict":{}}

    def construct_repair_metadata(self, batch_id, expired_keys, crosstx_subjection, RYW_subjection, worker_set, txid_list, container_port):
        '''
        Construct the repair metadata for the given batch. Only add RYW info and expired keys to the metadata when pessimistic repair is enabled.        
        '''
        #  crosstx_subjection {"dirty":False, "up_cnt": 0, "upstream_keys": {key:{'tx_id': prev_tx_id, 'func': prev_func}}}
        expired_keys_per_ip = {ip:set() for ip in worker_set}
        for tx_id in txid_list:
            for func, next_funcs in self.workflow_graph_topo.items():
                func_ip = self.function_pos[func]
                RYW_sub = RYW_subjection.get(tx_id, {}).get(func, {})
                tx_dict = self.get_info_dict(batch_id, func_ip, tx_id)
                crosstx_info = crosstx_subjection.get(tx_id, {}).get(func, {})
                tx_dict[func] = crosstx_info if crosstx_info else {}
                func_info_dict = tx_dict[func]
                func_info_dict['RYW_keys'] = RYW_sub
                if next_funcs[0] == 'END':
                    func_info_dict['successor_port'] = {'END':''}
                else:
                    func_info_dict['successor_port'] = {}
                    for f in next_funcs:
                        func_info_dict['successor_port'][f] = container_port[tx_id][f]
                if not self.optimistic_repair_enabled:
                    expired_keys_dict = expired_keys.get(tx_id, {}).get(func, {})
                    expired_keys_per_ip[func_ip].union(set(expired_keys_dict.keys()))
                    continue
                # RYW info: merged with crosstx subjection info. when optimistic repair is enabled
                if RYW_sub:
                    for key, introtx_upstream_func in RYW_sub.items():
                        # Remove keys from func_info_dict['upstream_keys'] if they appear in RYW_keys
                        if key in func_info_dict['upstream_keys']:
                            func_info_dict['upstream_keys'].pop(key)
                            func_info_dict['up_cnt'] -= 1
                        upstream_func_ip = self.function_pos[introtx_upstream_func]
                        upstream_func_dict = self.get_info_dict(batch_id, upstream_func_ip, tx_id, introtx_upstream_func)
                        func_info_dict['dirty'] = upstream_func_dict.get('dirty', False) 
                        # this key is RYW, remove from expired keys.
                        expired_keys.get(tx_id, {}).get(func, {}).pop(key, None)
                expired_keys_dict = expired_keys.get(tx_id, {}).get(func, {})
                expired_keys_per_ip[func_ip].union(set(expired_keys_dict.keys()))
        return expired_keys_per_ip


    def update_pessimistic_repair_metadata(self, batch_id, tx_id, tx_dependency, expired_keys):
        """
        Update the repair metadata for the given transaction in the batch.
        and update the expired keys due to abort of previous transactions.
        """
        for func, func_dependency in tx_dependency.items():
            func_ip = self.function_pos[func]
            func_info_dict = self.get_info_dict(batch_id, func_ip, tx_id, func)
            RYW_info = func_info_dict.get('RYW_keys', {})
            for key, dependency in func_dependency.items():
                # this key is RYW, already be included
                if key in RYW_info:
                    continue
                # this key isn't from its batch, is expired.
                elif dependency is None:
                    expired_keys.setdefault(func_ip, set()).add(key)
                else:
                    func_info_dict['upstream_keys'][key] = dependency
                    func_info_dict['up_cnt'] += 1 

        if self.fast_path_enabled:
            for ip, tx_dict in self.repair_metadata_per_batch_by_ip[batch_id].items():
                tx_info = tx_dict.setdefault(tx_id, {})
                for func, keys in tx_dependency.items():
                    func_info = tx_info.setdefault(func, {})
                    for key, dependency in keys.items():
                        if dependency:
                            prev_tx_id, prev_func = dependency
                            func_info.setdefault('upstream_keys', {})[key] = {'tx_id': prev_tx_id, 'func': prev_func}
                            func_info['up_cnt'] = func_info.get('up_cnt', 0) + 1
                        else:
                            func_info.setdefault('RYW_keys', {})[key] = True
        else:
            tx_dict = self.repair_metadata_per_batch_by_txid[batch_id].setdefault(tx_id, {})
            for func, keys in tx_dependency.items():
                func_info = tx_dict.setdefault(func, {})
                for key, dependency in keys.items():
                    if dependency:
                        prev_tx_id, prev_func = dependency
                        func_info.setdefault('upstream_keys', {})[key] = {'tx_id': prev_tx_id, 'func': prev_func}
                        func_info['up_cnt'] = func_info.get('up_cnt', 0) + 1
                    else:
                        func_info.setdefault('RYW_keys', {})[key] = True

    
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