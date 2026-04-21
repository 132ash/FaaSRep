import sys
sys.path.append('../../config')
import config
from subprocess_log import log_message
from collections import defaultdict

try:
    from models import FunctionRepairPlan, UpstreamRef
except ImportError:  # pragma: no cover - package import path
    from .models import FunctionRepairPlan, UpstreamRef


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


# b94ad94b0080's upstream key is substituted by RYW.

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
        # metadata: {"dirty": False, "up_cnt": 0, "RYW_keys": {key: func},
        #            "upstream_keys": {key: [prev_tx_id, prev_func]}}
        expired_keys_per_ip = defaultdict(set)
        plans_by_tx = {}
        for tx_id in txid_list:
            tx_plans = plans_by_tx.setdefault(tx_id, {})
            for func, next_funcs in self.workflow_graph_topo.items():
                ryw_sub = dict(RYW_subjection.get(tx_id, {}).get(func, {}))
                plan = FunctionRepairPlan.from_legacy(
                    crosstx_subjection.get(tx_id, {}).get(func, {})
                )
                plan.ryw_keys = ryw_sub
                plan.successor_port = self._successor_port_for(tx_id, next_funcs, container_port)

                for key in ryw_sub:
                    # Same-transaction RYW wins over any stale/cross-tx read on
                    # the same key. Dirty is propagated from the producer below.
                    plan.upstream_keys.pop(key, None)
                    expired_keys.get(tx_id, {}).get(func, {}).pop(key, None)

                basic_info_dict = self.get_func_basic_info_dict(batch_id, tx_id, func)
                basic_info_dict.clear()
                basic_info_dict.update(
                    {
                        "RYW_keys": dict(plan.ryw_keys),
                        "successor_port": dict(plan.successor_port),
                    }
                )
                tx_plans[func] = plan

        self._propagate_ryw_dirty(plans_by_tx)

        for tx_id, tx_plans in plans_by_tx.items():
            for func, plan in tx_plans.items():
                func_ip = self.function_pos[func]
                opt_func_info = self.get_func_subjection_info_dict(
                    OPT_REPAIR, batch_id, func_ip, tx_id, func
                )
                opt_func_info.clear()
                opt_func_info.update(plan.to_legacy())
                expired_keys_per_ip[func_ip].update(expired_keys.get(tx_id, {}).get(func, {}))
                log_message(self.logger, f"[VALIDATE OPTIMISTIC METADATA] Constructing repair metadata for batch {batch_id}, tx {tx_id}, func {func}, opt_func_info: {opt_func_info}, expired_keys_per_ip: {expired_keys_per_ip}")
        return dict(expired_keys_per_ip)


    def update_pessimistic_repair_metadata(self, batch_id, tx_id, tx_dependency, expired_keys):
        """
        Update the repair metadata for the given transaction in the batch.
        and update the expired keys due to abort of previous transactions.
        """
        for func in self.workflow_graph_topo.keys():
            func_dependency = tx_dependency.get(func, {})
            func_ip = self.function_pos[func]
            basic_info_dict = self.get_func_basic_info_dict(batch_id, tx_id, func)
            plan = FunctionRepairPlan(
                dirty=True,
                ryw_keys=dict(basic_info_dict.get("RYW_keys", {})),
                successor_port=dict(basic_info_dict.get("successor_port", {})),
            )
            for key, dependency in func_dependency.items():
                # this key is RYW, already be included
                if key in plan.ryw_keys:
                    continue
                # this key isn't from its batch, is expired.
                elif dependency is None:
                    expired_keys.setdefault(func_ip, set()).add(key)
                else:
                    plan.upstream_keys[key] = UpstreamRef.from_legacy(dependency)
            pessi_func_info = self.get_func_subjection_info_dict(
                PESSI_REPAIR, batch_id, func_ip, tx_id, func
            )
            pessi_func_info.clear()
            pessi_func_info.update(plan.to_legacy())
            log_message(self.logger, f"[PESSIMISTIC METADATA] Updated repair metadata for batch {batch_id}, tx {tx_id}, func {func}, pessi_func_info: {pessi_func_info}, expired_keys: {expired_keys}")
    
    def get_func_basic_info_dict(self, batch_id, tx_id, func):
        return self.repair_basic_info_dict[batch_id].setdefault(tx_id, {}).setdefault(func, {})

    def get_func_subjection_info_dict(self, repair_mode, batch_id, ip, tx_id, func=''):
        if self.fast_path_enabled:
            tx_dict =  self.repair_metadata_per_batch_by_ip[repair_mode][batch_id].setdefault(ip, {}).setdefault(tx_id, {})
        else:
            tx_dict = self.repair_metadata_per_batch_by_txid[repair_mode][batch_id].setdefault(tx_id, {})
        return tx_dict.setdefault(func, FunctionRepairPlan().to_legacy()) if func else tx_dict
        
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
        self.repair_basic_info_dict.pop(batch_id, None)

    def _successor_port_for(self, tx_id, next_funcs, container_port):
        if next_funcs and next_funcs[0] == 'END':
            return {'END': ''}
        tx_container_ports = container_port.get(tx_id, {})
        successor_port = {}
        for next_func in next_funcs:
            successor_port[next_func] = tx_container_ports.get(next_func, '')
        return successor_port

    def _propagate_ryw_dirty(self, plans_by_tx):
        changed = True
        while changed:
            changed = False
            for tx_plans in plans_by_tx.values():
                for plan in tx_plans.values():
                    if plan.dirty:
                        continue
                    for upstream_func in plan.ryw_keys.values():
                        upstream_plan = tx_plans.get(upstream_func)
                        if upstream_plan is not None and upstream_plan.dirty:
                            plan.dirty = True
                            changed = True
                            break
