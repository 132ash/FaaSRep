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
from subprocess_log import log_validator_message

sys.path.append('../../config')
import config
from repair_info import RepairInfo

REPAIRED = 1
ABORTED = 2

FAST_PATH_ENABLED = config.FAST_PATH
OPTIMISTIC_REPAIR = config.OPTIMISTIC_REPAIR

OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR

class RepairEngine:

    def __init__(self, logger, repair_info:RepairInfo, function_pos, worker_ip_set, workflow_name, tx_sink_addr, repo: Repository):
        self.logger = logger
        self.repair_info = repair_info
        self.tx_sink_addr = tx_sink_addr
        self.workflow_name = workflow_name
        self.function_pos = function_pos
        self.worker_ip_set = worker_ip_set
        self.pessi_register_lock = gevent.lock.BoundedSemaphore()

        self.repo = repo
        self.start_functions = self.repo.get_start_functions(self.workflow_name + '_workflow_metadata')
        self.PessimisticRepairer = PessimisticRepairer(logger, workflow_name, self.repair_info, self.function_pos)

    def repair_batch_after_validate(self,batch_id,container_port, read_set, write_set,tx_list, expired_keys, pessi_sink_info):
        # allocate works
        start = time.time()
        self.pessi_register_lock.acquire()
        self.PessimisticRepairer.register_repair_info(batch_id, read_set, write_set, tx_list, pessi_sink_info['last_tx'])
        ready_txs = self.register_on_sink(batch_id, pessi_sink_info)
        self.pessi_register_lock.release()
        if not OPTIMISTIC_REPAIR:
            expired_keys = {}
            self.PessimisticRepairer.prepare_pessimistic_info(batch_id, expired_keys, ready_txs)
            mode = PESSI_REPAIR
        else:
            ready_txs = tx_list
            mode = OPT_REPAIR
        self.repair_transactions(batch_id, ready_txs, expired_keys, container_port, mode)
        return time.time() - start
                
    def send_pessimistic_repair_req(self, batch_id, container_port_per_batch, cascaded_ready_txs):
        expired_keys = {}
        log_validator_message(self.logger, f"[PESSIMISTIC REPAIR] Sending repair request for batch {batch_id}, cascaded ready transactions: {cascaded_ready_txs}")
        self.PessimisticRepairer.prepare_pessimistic_info(batch_id, expired_keys, cascaded_ready_txs)
        self.repair_transactions(batch_id, cascaded_ready_txs, expired_keys, container_port_per_batch, PESSI_REPAIR)

    def repair_transactions(self, batch_id, ready_transactions, expired_keys, container_port, mode=OPT_REPAIR):
        repair_prepare_jobs = []
        trigger_jobs = []
        for ip in self.worker_ip_set:
            repair_metadata_local = self.repair_info.get_repair_metadata(mode, batch_id, ip) if FAST_PATH_ENABLED else {}
            repair_prepare_jobs.append(gevent.spawn(self.prepare_repairing_on_worker, batch_id, ip, repair_metadata_local, expired_keys.get(ip, set())))
        gevent.joinall(repair_prepare_jobs) 
        # metadata filled. Trigger start functions to repair workflow.
        repair_metadata_no_fast = {}
        for tx_id in ready_transactions:
            if not FAST_PATH_ENABLED:
                repair_metadata_no_fast = self.repair_info.get_repair_metadata(mode, batch_id, '', tx_id)
            log_validator_message(self.logger, f"repairing transaction {tx_id} in batch {batch_id}, repair_metadata_no_fast:{repair_metadata_no_fast}")
            # trigger start functions
            for n in self.start_functions:
                ip = self.function_pos[n]
                port = container_port[tx_id][n]
                trigger_jobs.append(gevent.spawn(self.trigger_function, FAST_PATH_ENABLED, self.workflow_name, tx_id, n, ip, port,batch_id, repair_metadata_no_fast, mode) )
        gevent.joinall(trigger_jobs)
        

    def trigger_function(self, FAST_PATH_ENABLED, workflow_name, transaction_id, function_name, ip, port, batch_id, repair_metadata_per_tx, mode):
        route = "repair" if FAST_PATH_ENABLED else "request"
        if not ip.endswith(":7000"):
            url = f'http://{ip}:7000/{route}'
        else:
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
        }
        print(f"triggering {function_name}, sending req to {url}, batch_id: {batch_id}")
        requests.post(url, json=data)

    def register_on_sink(self,batch_id, pessi_sink_info):
        ip = self.tx_sink_addr
        if not ip.endswith(":7000"):
            url = f'http://{ip}:7000/repair_pessi'
        else:
            url = f'http://{ip}/repair_pessi'
        data = {'batch_id': batch_id,'workflow_name': self.workflow_name,'batch_sub': pessi_sink_info['batch_sub'],'tx_sub': pessi_sink_info['tx_sub'],'whole_tx_sub': pessi_sink_info['whole_tx_sub']}
        res = requests.post(url, json=data).json()
        log_validator_message(self.logger, f"[PESSI] registering repair metadata on sink {ip}, batch_id: {batch_id}, data: {data}, ready_txs: {res['ready_txs']}")
        return res['ready_txs']

    # repair_metadata: {txid:{func:{ RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}
    # send metadata to the proxy on worker node.
    # all functions' ip and port need to be sent(?)
    def prepare_repairing_on_worker(self, batch_id, worker_ip, repair_metadata, expired_keys:set):
        if not repair_metadata and not expired_keys:
            return
        if not worker_ip.endswith(":7000"):
            url = 'http://{}:7000/prepare'.format(worker_ip)
        else:
            url = 'http://{}/prepare'.format(worker_ip)
        data = {
            'batch_id': batch_id,
            'repair_metadata': repair_metadata,
            'expired_keys': list(expired_keys)
        }
        requests.post(url, json=data)

    def clean_table_of_batch(self, batch_id):
        """
        Clean the write table and locks for the given batch_id.
        """
        self.PessimisticRepairer.clean_table_of_batch(batch_id)
        self.repair_info.clean_table_of_batch(batch_id)
