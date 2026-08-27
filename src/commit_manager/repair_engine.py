from gevent import monkey
monkey.patch_all()
import requests
import gevent
import logging
import sys
import gevent.lock
from gevent import event
import time
from validator_repo import Repository
from pessimistic_repairer import PessimisticRepairer
from subprocess_log import log_message

sys.path.append('../../config')
import config
from repair_info import RepairInfo

REPAIRED = 1
ABORTED = 2

FAST_PATH_ENABLED = config.FAST_PATH
OPTIMISTIC_REPAIR = config.OPTIMISTIC_REPAIR

OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR

SCALABILITY_TEST = config.SCALABILITY_TEST
FAKE_SINK_URL = config.FAKE_SINK_URL    

class RepairEngine:

    def __init__(self, logger, repair_info:RepairInfo, function_pos, worker_ip_set, workflow_name, tx_sink_addr, repo: Repository):
        self.logger = logger
        self.repair_info = repair_info
        self.tx_sink_addr = tx_sink_addr
        self.workflow_name = workflow_name
        self.function_pos = function_pos
        self.worker_ip_set = worker_ip_set
        self.pessi_register_lock = gevent.lock.BoundedSemaphore()
        self.pessimistic_repair_txs_per_batch = {}
        self.repair_attempts_per_batch = {}

        self.repo = repo
        self.start_functions = self.repo.get_start_functions(self.workflow_name + '_workflow_metadata')
        self.PessimisticRepairer = PessimisticRepairer(logger, workflow_name, self.repair_info, self.function_pos)

    def repair_batch_after_validate(self,batch_id,container_port, read_set, write_set,tx_list, expired_keys, pessi_sink_info):
        # allocate works
        start = time.time()
        self.pessi_register_lock.acquire()
        self.pessimistic_repair_txs_per_batch[batch_id] = {}
        self.repair_attempts_per_batch[batch_id] = {}
        self.PessimisticRepairer.register_repair_info(batch_id, read_set, write_set, tx_list, pessi_sink_info['last_tx'])
        if SCALABILITY_TEST:
            requests.post(FAKE_SINK_URL, json={'batch_id': batch_id})
            self.pessi_register_lock.release() 
            return
        ready_txs, opt_txs_become_pessi = self.register_on_sink(batch_id, pessi_sink_info)
        self.pessi_register_lock.release()    
        txs_for_optimistic_repair = []
        txs_for_pessimistic_repair = []
        if OPTIMISTIC_REPAIR:
            for tx_id in tx_list:
                if opt_txs_become_pessi.get(tx_id, False):
                    if ready_txs.get(tx_id, False):
                        txs_for_pessimistic_repair.append(tx_id)
                else:
                    txs_for_optimistic_repair.append(tx_id)
        else:
            txs_for_pessimistic_repair = ready_txs
        log_message(self.logger, (
            f"REPAIR_PLAN workflow={self.workflow_name} batch_id={batch_id} "
            f"optimistic={txs_for_optimistic_repair} "
            f"pessimistic={txs_for_pessimistic_repair} "
            f"waiting={[tx for tx in tx_list if tx not in txs_for_optimistic_repair and tx not in txs_for_pessimistic_repair]}"
        ))
        #log_message(self.logger, f"[REPAIR AFTER VALIDATE] Batch {batch_id} PESSI ready transactions: {ready_txs},opt_txs_become_pessi:{opt_txs_become_pessi} optimistic repair transactions: {txs_for_optimistic_repair}, pessimistic repair transactions: {txs_for_pessimistic_repair}")
        repair_jobs = []
        if txs_for_pessimistic_repair:
            expired_keys_pessi = {}
            for tx_id in txs_for_pessimistic_repair:
                self.pessimistic_repair_txs_per_batch[batch_id][tx_id] = True
            self.PessimisticRepairer.prepare_pessimistic_info(batch_id, expired_keys_pessi, txs_for_pessimistic_repair)
            repair_jobs.append(gevent.spawn(self.repair_transactions, batch_id, txs_for_pessimistic_repair, expired_keys_pessi, container_port, PESSI_REPAIR))
        if txs_for_optimistic_repair:
            repair_jobs.append(gevent.spawn(self.repair_transactions, batch_id, txs_for_optimistic_repair, expired_keys, container_port, OPT_REPAIR))
        gevent.joinall(repair_jobs)
        return time.time() - start
                
    def send_pessimistic_repair_req(self, batch_id, container_port_per_batch, cascaded_ready_txs):
        expired_keys = {}
        #log_message(self.logger, f"[PESSIMISTIC REPAIR] Sending repair request for batch {batch_id}, cascaded ready transactions: {cascaded_ready_txs}")
        self.PessimisticRepairer.prepare_pessimistic_info(batch_id, expired_keys, cascaded_ready_txs)
        for tx_id in cascaded_ready_txs:
            self.pessimistic_repair_txs_per_batch[batch_id][tx_id] = True
        self.repair_transactions(batch_id, cascaded_ready_txs, expired_keys, container_port_per_batch, PESSI_REPAIR)

    def repair_transactions(self, batch_id, ready_transactions, expired_keys, container_port, mode=OPT_REPAIR):
        repair_prepare_jobs = []
        trigger_jobs = []
        for ip in self.worker_ip_set:
            repair_metadata_local = self.repair_info.get_repair_metadata(mode, batch_id, ip) if FAST_PATH_ENABLED else {}
            repair_prepare_jobs.append(gevent.spawn(self.prepare_repairing_on_worker, batch_id, ip, repair_metadata_local, expired_keys.get(ip, set()), mode))
        gevent.joinall(repair_prepare_jobs) 
        # metadata filled. Trigger start functions to repair workflow.
        repair_metadata_no_fast = {}
        for tx_id in ready_transactions:
            repair_epoch, attempt_id = self.get_or_create_attempt(batch_id, tx_id, mode)
            if not FAST_PATH_ENABLED:
                repair_metadata_no_fast = self.repair_info.get_repair_metadata(mode, batch_id, '', tx_id)
            #log_message(self.logger, f"[REPAIR] repairing transaction {tx_id} in batch {batch_id}, repair_metadata_no_fast:{repair_metadata_no_fast}, mode: {mode}")
            # trigger start functions
            for n in self.start_functions:
                ip = self.function_pos[n]
                port = container_port[tx_id][n]
                trigger_jobs.append(gevent.spawn(self.trigger_function, FAST_PATH_ENABLED, self.workflow_name, tx_id, n, ip, port,batch_id, repair_metadata_no_fast, mode, repair_epoch, attempt_id) )
        gevent.joinall(trigger_jobs)
        

    def get_or_create_attempt(self, batch_id, transaction_id, mode):
        attempts = self.repair_attempts_per_batch.setdefault(batch_id, {})
        current = attempts.get(transaction_id)
        if current and current['mode'] == mode:
            return current['epoch'], current['attempt_id']
        epoch = 1 if current is None else current['epoch'] + 1
        attempt_id = f'{batch_id}:{transaction_id}:{epoch}:{mode}'
        attempts[transaction_id] = {
            'epoch': epoch, 'mode': mode, 'attempt_id': attempt_id,
        }
        return epoch, attempt_id

    def trigger_function(self, FAST_PATH_ENABLED, workflow_name, transaction_id, function_name, ip, port, batch_id, repair_metadata_per_tx, mode, repair_epoch, attempt_id):
        route = "repair" if FAST_PATH_ENABLED else "request"
        url = f'http://{ip}/{route}'
        # print(f"-----repair function: {function_name}, ip: {ip}, port: {port}, batch_id: {batch_id}, repair_states:{repair_metadata_per_tx}-----")
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': True,
            'port': port,
            'repair': True,
            'repair_mode': mode,
            'repair_states': repair_metadata_per_tx
            ,'repair_epoch': repair_epoch
            ,'attempt_id': attempt_id
        }
        log_message(self.logger, (
            f"REPAIR_TRIGGER_SENT workflow={workflow_name} batch_id={batch_id} "
            f"tx_id={transaction_id} function={function_name} repair_mode={mode} "
            f"repair_epoch={repair_epoch} attempt_id={attempt_id}"
        ))
        requests.post(url, json=data)

    def finish_batch_skipping_repair(self, batch_id):
        #log_message(self.logger, f"[PESSIMISTIC REPAIR SKIP] Skipping repair for batch {batch_id}. finish on sink")
        url = f'http://{self.tx_sink_addr}:6000/fin_repair'
        data = {
                'batch_id': batch_id,
                'workflow_name': self.workflow_name,
                'transaction_id': '',
                'repair_mode': PESSI_REPAIR,
                'skip_repair': True
            }
        requests.post(url, json=data)

    def sink_release_optimistic_info(self, batch_list):
        url = f'http://{self.tx_sink_addr}:6000/release_opt'
        data = {
            'workflow_name':self.workflow_name,
            'batch_list': batch_list
        }
        requests.post(url, json=data)

    def register_on_sink(self,batch_id, pessi_sink_info):
        ip = self.tx_sink_addr
        url = f'http://{ip}:6000/repair_pessi'
        data = {'batch_id': batch_id,'workflow_name': self.workflow_name,'batch_sub': pessi_sink_info['batch_sub'],'tx_sub': pessi_sink_info['tx_sub'],'whole_tx_sub': pessi_sink_info['whole_tx_sub']}
        res = requests.post(url, json=data).json()
        #log_message(self.logger, f"[PESSI] registering repair metadata on sink {ip}, batch_id: {batch_id}, data: {data}, ready_txs: {res['ready_txs']}")
        return res['ready_txs'], res['opt_txs_become_pessi']

    # repair_metadata: {txid:{func:{ RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}
    # send metadata to the proxy on worker node.
    # all functions' ip and port need to be sent(?)
    def prepare_repairing_on_worker(self, batch_id, worker_ip, repair_metadata, expired_keys:set, mode):
        #log_message(self.logger, f"[PESSIMISTIC REPAIR] Preparing repair on worker {worker_ip} for batch {batch_id}, repair_metadata: {repair_metadata}, expired_keys: {expired_keys}, mode:{mode}")
        if not repair_metadata and not expired_keys:
            return
        url = 'http://{}/prepare'.format(worker_ip)
        data = {
            'batch_id': batch_id,
            'repair_metadata': repair_metadata,
            'expired_keys': list(expired_keys),
            'mode': mode
        }
        requests.post(url, json=data)

    def clean_table_of_batch(self, batch_id):
        """
        Clean the write table and locks for the given batch_id.
        """
        self.PessimisticRepairer.clean_table_of_batch(batch_id)
        self.repair_info.clean_table_of_batch(batch_id)
        self.pessimistic_repair_txs_per_batch.pop(batch_id, None)
        self.repair_attempts_per_batch.pop(batch_id, None)
