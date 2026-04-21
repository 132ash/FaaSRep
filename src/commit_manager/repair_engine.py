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
TX_SINK_PORT = config.TX_SINK_PORT
TX_SINK_REPAIR_PORT = config.TX_SINK_REPAIR_PORT

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

        self.repo = repo
        self.start_functions = self.repo.get_start_functions(self.workflow_name + '_workflow_metadata')
        self.PessimisticRepairer = PessimisticRepairer(logger, workflow_name, self.repair_info, self.function_pos)

    def repair_batch_after_validate(self,batch_id,container_port, read_set, write_set,tx_list, expired_keys, pessi_sink_info):
        # allocate works
        start = time.time()
        log_message(
            self.logger,
            f"[REPAIR START] batch={batch_id}, tx_count={len(tx_list)}, opt_enabled={OPTIMISTIC_REPAIR}"
        )
        self.pessi_register_lock.acquire()
        try:
            self.pessimistic_repair_txs_per_batch[batch_id] = {}
            self.PessimisticRepairer.register_repair_info(batch_id, read_set, write_set, tx_list, pessi_sink_info['last_tx'])
            if SCALABILITY_TEST:
                self._post_json(FAKE_SINK_URL, {'batch_id': batch_id}, "fake sink register")
                return
            sink_registration = self.register_on_sink(batch_id, pessi_sink_info)
            if sink_registration is None:
                log_message(self.logger, f"[REPAIR BLOCKED] batch={batch_id} sink registration failed; waiting for operator intervention.")
                return
            ready_txs, opt_txs_become_pessi = sink_registration
            log_message(
                self.logger,
                f"[REPAIR SINK REGISTERED] batch={batch_id}, ready_txs={sorted(ready_txs.keys())}, "
                f"opt_to_pessi={sorted(opt_txs_become_pessi.keys())}"
            )
        finally:
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
        log_message(self.logger, f"[REPAIR AFTER VALIDATE] Batch {batch_id} PESSI ready transactions: {ready_txs},opt_txs_become_pessi:{opt_txs_become_pessi} optimistic repair transactions: {txs_for_optimistic_repair}, pessimistic repair transactions: {txs_for_pessimistic_repair}")
        repair_jobs = []
        if txs_for_pessimistic_repair:
            expired_keys_pessi = {}
            for tx_id in txs_for_pessimistic_repair:
                self.pessimistic_repair_txs_per_batch[batch_id][tx_id] = True
            self.PessimisticRepairer.prepare_pessimistic_info(batch_id, expired_keys_pessi, txs_for_pessimistic_repair)
            repair_jobs.append(gevent.spawn(self.repair_transactions, batch_id, txs_for_pessimistic_repair, expired_keys_pessi, container_port, PESSI_REPAIR))
        if txs_for_optimistic_repair:
            repair_jobs.append(gevent.spawn(self.repair_transactions, batch_id, txs_for_optimistic_repair, expired_keys, container_port, OPT_REPAIR))
        log_message(
            self.logger,
            f"[REPAIR PLAN] batch={batch_id}, opt_txs={txs_for_optimistic_repair}, pessi_txs={txs_for_pessimistic_repair}"
        )
        self._join_and_report(repair_jobs, f"repair batch {batch_id}")
        return time.time() - start
                
    def send_pessimistic_repair_req(self, batch_id, container_port, cascaded_ready_txs):
        expired_keys = {}
        log_message(
            self.logger,
            f"[PESSI CASCADE] batch={batch_id}, cascaded_ready_txs={list(cascaded_ready_txs)}"
        )
        self.PessimisticRepairer.prepare_pessimistic_info(batch_id, expired_keys, cascaded_ready_txs)
        for tx_id in cascaded_ready_txs:
            self.pessimistic_repair_txs_per_batch[batch_id][tx_id] = True
        return self.repair_transactions(batch_id, cascaded_ready_txs, expired_keys, container_port, PESSI_REPAIR)

    def repair_transactions(self, batch_id, ready_transactions, expired_keys, container_port, mode=OPT_REPAIR):
        repair_prepare_jobs = []
        trigger_jobs = []
        ready_transactions = list(dict.fromkeys(ready_transactions))
        log_message(
            self.logger,
            f"[REPAIR DISPATCH] batch={batch_id}, mode={mode}, ready_txs={ready_transactions}"
        )
        for ip in self.worker_ip_set:
            repair_metadata_local = self.repair_info.get_repair_metadata(mode, batch_id, ip) if FAST_PATH_ENABLED else {}
            repair_prepare_jobs.append(gevent.spawn(self.prepare_repairing_on_worker, batch_id, ip, repair_metadata_local, expired_keys.get(ip, set()), mode))
        prepare_ok = self._join_and_report(repair_prepare_jobs, f"prepare repair {batch_id}")
        if not prepare_ok or any(job.value is False for job in repair_prepare_jobs if job.ready() and job.exception is None):
            log_message(self.logger, f"[REPAIR BLOCKED] batch={batch_id}, mode={mode}, ready_txs={ready_transactions}: prepare failed; not aborting automatically.")
            return False
        # metadata filled. Trigger start functions to repair workflow.
        repair_metadata_no_fast = {}
        for tx_id in ready_transactions:
            if not FAST_PATH_ENABLED:
                repair_metadata_no_fast = self.repair_info.get_repair_metadata(mode, batch_id, '', tx_id)
            log_message(self.logger, f"[REPAIR] repairing transaction {tx_id} in batch {batch_id}, repair_metadata_no_fast:{repair_metadata_no_fast}, mode: {mode}")
            # trigger start functions
            for n in self.start_functions:
                ip = self.function_pos[n]
                port = container_port.get(tx_id, {}).get(n)
                if port is None:
                    log_message(self.logger, f"[REPAIR BLOCKED] Missing container port for batch={batch_id}, tx={tx_id}, func={n}; not aborting automatically.")
                    continue
                log_message(
                    self.logger,
                    f"[REPAIR TARGET] batch={batch_id}, tx={tx_id}, func={n}, "
                    f"worker={ip}, container_port={port}, mode={mode}"
                )
                trigger_jobs.append((tx_id, gevent.spawn(self.trigger_function, FAST_PATH_ENABLED, self.workflow_name, tx_id, n, ip, port,batch_id, repair_metadata_no_fast, mode)))
        jobs = [job for _, job in trigger_jobs]
        trigger_ok = self._join_and_report(jobs, f"trigger repair {batch_id}")
        failed_txs = set()
        for tx_id, job in trigger_jobs:
            if job.exception is not None or job.value is False:
                failed_txs.add(tx_id)
        if failed_txs:
            log_message(self.logger, f"[REPAIR BLOCKED] batch={batch_id}, mode={mode}, failed_txs={sorted(failed_txs)}: trigger failed; not aborting automatically.")
        return trigger_ok and not failed_txs
        

    def trigger_function(self, FAST_PATH_ENABLED, workflow_name, transaction_id, function_name, ip, port, batch_id, repair_metadata_per_tx, mode):
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
        }
        return self._post_json(url, data, f"trigger {transaction_id}/{function_name}") is not None

    def finish_batch_skipping_repair(self, batch_id):
        log_message(self.logger, f"[PESSIMISTIC REPAIR SKIP] Skipping repair for batch {batch_id}. finish on sink")
        url = f'http://{self.tx_sink_addr}:{TX_SINK_PORT}/fin_repair'
        data = {
                'batch_id': batch_id,
                'workflow_name': self.workflow_name,
                'transaction_id': '',
                'repair_mode': PESSI_REPAIR,
                'skip_repair': True
            }
        return self._post_json(url, data, f"finish skip repair {batch_id}") is not None

    def sink_release_optimistic_info(self, batch_list):
        url = f'http://{self.tx_sink_addr}:{TX_SINK_REPAIR_PORT}/release_opt'
        data = {
            'workflow_name':self.workflow_name,
            'batch_list': batch_list
        }
        return self._post_json(url, data, f"release optimistic info {batch_list}") is not None

    def register_on_sink(self,batch_id, pessi_sink_info):
        ip = self.tx_sink_addr
        url = f'http://{ip}:{TX_SINK_REPAIR_PORT}/repair_pessi'
        data = {'batch_id': batch_id,'workflow_name': self.workflow_name,'batch_sub': pessi_sink_info['batch_sub'],'tx_sub': pessi_sink_info['tx_sub'],'whole_tx_sub': pessi_sink_info['whole_tx_sub']}
        response = self._post_json(url, data, f"register pessimistic info {batch_id}")
        if response is None:
            return None
        res = response.json()
        log_message(self.logger, f"[PESSI] registering repair metadata on sink {ip}, batch_id: {batch_id}, data: {data}, ready_txs: {res['ready_txs']}")
        return res['ready_txs'], res['opt_txs_become_pessi']

    # repair_metadata: {txid:{func:{ RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}
    # send metadata to the proxy on worker node.
    # all functions' ip and port need to be sent(?)
    def prepare_repairing_on_worker(self, batch_id, worker_ip, repair_metadata, expired_keys:set, mode):
        log_message(self.logger, f"[PESSIMISTIC REPAIR] Preparing repair on worker {worker_ip} for batch {batch_id}, repair_metadata: {repair_metadata}, expired_keys: {expired_keys}, mode:{mode}")
        if not repair_metadata and not expired_keys:
            return
        url = 'http://{}/prepare'.format(worker_ip)
        data = {
            'batch_id': batch_id,
            'repair_metadata': repair_metadata,
            'expired_keys': list(expired_keys),
            'mode': mode
        }
        return self._post_json(url, data, f"prepare repair {batch_id} on {worker_ip}") is not None

    def clean_table_of_batch(self, batch_id):
        """
        Clean the write table and locks for the given batch_id.
        """
        self.PessimisticRepairer.clean_table_of_batch(batch_id)
        self.repair_info.clean_table_of_batch(batch_id)
        self.pessimistic_repair_txs_per_batch.pop(batch_id, None)

    def _post_json(self, url, data, context):
        started_at = time.time()
        log_message(self.logger, f"[HTTP POST START] {context}: {url}")
        try:
            response = requests.post(url, json=data)
            response.raise_for_status()
            elapsed_s = time.time() - started_at
            log_message(
                self.logger,
                f"[HTTP POST OK] {context}: {url}, status={response.status_code}, elapsed_s={elapsed_s:.3f}",
            )
            return response
        except requests.RequestException as exc:
            elapsed_s = time.time() - started_at
            log_message(self.logger, f"[HTTP ERROR] {context}: {url}: {exc}, elapsed_s={elapsed_s:.3f}")
            return None

    def _join_and_report(self, jobs, context):
        if not jobs:
            return True
        gevent.joinall(jobs)
        ok = True
        for job in jobs:
            if job.exception is not None:
                ok = False
                log_message(self.logger, f"[GEVENT ERROR] {context}: {job.exception}")
        return ok
