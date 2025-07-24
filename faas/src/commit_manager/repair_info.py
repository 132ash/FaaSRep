import sys
sys.path.append('../../config')
import config
from subprocess_log import log_validator_message


OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR


class RepairInfo:
    def __init__(self, logger, workflow_graph_topo, function_pos):
        self.workflow_graph_topo = workflow_graph_topo
        self.logger = logger
        self.function_pos = function_pos
        self.fast_path_enabled = config.FAST_PATH
        self.optimistic_repair_enabled = config.OPTIMISTIC_REPAIR
        self.repair_metadata_per_batch_by_ip = {OPT_REPAIR: {}, PESSI_REPAIR: {}}
        self.repair_metadata_per_batch_by_txid = {OPT_REPAIR: {}, PESSI_REPAIR: {}}
        self.repair_basic_info_dict = {}
        self.container_port_per_batch = {}


# TODO: construct repair metadata, check code.

    def batch_init(self, batch_id):
        self.repair_basic_info_dict[batch_id] = {}
        if self.fast_path_enabled:
            self.repair_metadata_per_batch_by_ip[OPT_REPAIR][batch_id] = {}
            self.repair_metadata_per_batch_by_ip[PESSI_REPAIR][batch_id] = {}
        else:
            self.repair_metadata_per_batch_by_txid[OPT_REPAIR][batch_id] = {}
            self.repair_metadata_per_batch_by_txid[PESSI_REPAIR][batch_id] = {}

        # self.downstream_func_table[batch_id] = {}
        # self.upstream_func_table[batch_id] = {"next_func":{}, "next_dict":{}}

    def construct_repair_metadata(self, batch_id, expired_keys, crosstx_subjection, RYW_subjection, worker_set, txid_list, container_port):
        '''
        Construct the repair metadata for the given batch. Only add RYW info and expired keys to the metadata when pessimistic repair is enabled.        
        '''
        #  metadata: {"dirty":False, "up_cnt": 0, "RYW_keys":{key:func}, "upstream_keys": {key:{'tx_id': prev_tx_id, 'func': prev_func}}}
        expired_keys_per_ip = {ip:set() for ip in worker_set}
        for tx_id in txid_list:
            for func, next_funcs in self.workflow_graph_topo.items():
                # update basic info used in opt and pessi: RYW and next_port.
                RYW_sub = RYW_subjection.get(tx_id, {}).get(func, {})
                basic_info_dict = self.get_func_basic_info_dict(batch_id, tx_id, func)
                basic_info_dict['RYW_keys'] = RYW_sub
                if next_funcs[0] == 'END':
                    basic_info_dict['successor_port'] = {'END':''}
                else:
                    basic_info_dict['successor_port'] = {}
                    for f in next_funcs:
                        basic_info_dict['successor_port'][f] = container_port[tx_id][f]       
                func_ip = self.function_pos[func]
                crosstx_info = crosstx_subjection.get(tx_id, {}).get(func, {})
                opt_func_info = self.get_func_subjection_info_dict(OPT_REPAIR, batch_id, func_ip, tx_id, func)
                for key, cross_info in crosstx_info.items():
                    opt_func_info[key] = cross_info
                for key, basic_info in basic_info_dict.items():
                    opt_func_info[key] = basic_info
                # RYW info: merged with crosstx subjection info. when optimistic repair is enabled
                for key, introtx_upstream_func in RYW_sub.items():
                    # Remove keys from func_info_dict['upstream_keys'] if they appear in RYW_keys
                    if key in opt_func_info['upstream_keys']:
                        opt_func_info['upstream_keys'].pop(key)
                        opt_func_info['up_cnt'] -= 1
                    upstream_func_ip = self.function_pos[introtx_upstream_func]
                    upstream_func_dict = self.get_func_subjection_info_dict(OPT_REPAIR, batch_id, upstream_func_ip, tx_id, introtx_upstream_func)
                    opt_func_info['dirty'] = upstream_func_dict.get('dirty', False) 
                    # this key is RYW, remove from expired keys.
                    expired_keys.get(tx_id, {}).get(func, {}).pop(key, None)
                expired_keys_dict = expired_keys.get(tx_id, {}).get(func, {})
                expired_keys_per_ip[func_ip].union(set(expired_keys_dict.keys()))
                log_validator_message(self.logger, f"[VALIDATE OPTIMISTIC METADATA] Constructing repair metadata for batch {batch_id}, tx {tx_id}, func {func}, opt_func_info: {opt_func_info}, expired_keys_per_ip: {expired_keys_per_ip}")    
        return expired_keys_per_ip


    def update_pessimistic_repair_metadata(self, batch_id, tx_id, tx_dependency, expired_keys):
        """
        Update the repair metadata for the given transaction in the batch.
        and update the expired keys due to abort of previous transactions.
        """
        for func, func_dependency in tx_dependency.items():
            pessi_func_info = self.get_func_subjection_info_dict(PESSI_REPAIR, batch_id, func_ip, tx_id, func)
            basic_info_dict = self.get_func_basic_info_dict(batch_id, tx_id, func)
            for key, basic_info in basic_info_dict.items():
                pessi_func_info[key] = basic_info
            pessi_func_info['dirty'] = True
            pessi_func_info['up_cnt'] = 0
            pessi_func_info['upstream_keys'] = {}
            func_ip = self.function_pos[func]
            RYW_info = pessi_func_info.get('RYW_keys', {})
            for key, dependency in func_dependency.items():
                # this key is RYW, already be included
                if key in RYW_info:
                    continue
                # this key isn't from its batch, is expired.
                elif dependency is None:
                    expired_keys.setdefault(func_ip, set()).add(key)
                else:
                    pessi_func_info['upstream_keys'][key] = dependency
                    pessi_func_info['up_cnt'] += 1 
            log_validator_message(self.logger, f"[PESSIMISTIC METADATA] Updated repair metadata for batch {batch_id}, tx {tx_id}, func {func}, pessi_func_info: {pessi_func_info}, expired_keys: {expired_keys}")
    
    def get_func_basic_info_dict(self, batch_id, tx_id, func):
        return self.repair_basic_info_dict[batch_id].setdefault(tx_id, {}).setdefault(func, {})

    def get_func_subjection_info_dict(self, repair_mode, batch_id, ip, tx_id, func=''):
        if self.fast_path_enabled:
            tx_dict =  self.repair_metadata_per_batch_by_ip[repair_mode][batch_id].setdefault(ip, {}).setdefault(tx_id, {})
        else:
            tx_dict = self.repair_metadata_per_batch_by_txid[repair_mode][batch_id].setdefault(tx_id, {})
        return tx_dict.setdefault(func, {}) if func else tx_dict
        
    def get_repair_metadata(self, repair_mode, batch_id, ip="", txid=""):
        if self.fast_path_enabled:
            return self.repair_metadata_per_batch_by_ip[repair_mode][batch_id].get(ip, {})
        else:
            return self.repair_metadata_per_batch_by_txid[repair_mode][batch_id].get(txid, {})
    
    def clean_table_of_batch(self, batch_id):
        self.repair_metadata_per_batch_by_ip[OPT_REPAIR].pop(batch_id, None)
        self.repair_metadata_per_batch_by_ip[PESSI_REPAIR].pop(batch_id, None)
        self.repair_metadata_per_batch_by_txid[OPT_REPAIR].pop(batch_id, None)
        self.repair_metadata_per_batch_by_txid[PESSI_REPAIR].pop(batch_id, None)