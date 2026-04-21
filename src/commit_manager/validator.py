from gevent import monkey
monkey.patch_all()
import gevent
from gevent import event
import sys
import gevent.lock
import gevent.queue
import re
from subprocess_log import log_message, setup_validator_logger
import time
from serializer import SerializerProcess
import requests
from multiprocessing import Process, Queue
from repair_info import RepairInfo
from repair_engine import RepairEngine
from validator_repo import Repository
from validator_state import BatchRuntimeState, ValidatorBatchStore

sys.path.append('../../config')
import config

PESSIMISTIC_REPAIR_ENABLED = not config.OPTIMISTIC_REPAIR
FAST_PATH_ENABLED = config.FAST_PATH

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")

VALIDATE = 1
REPAIR_FINISH = 2
COMMIT = 3
CASCADED_COMMIT = 4
GATEWAY_NOTIFY_ADDR = config.GATEWAY_NOTIFY_ADDR
DISPATCH_INTERVAL = 0.005 
SCALABILITY_TEST = config.SCALABILITY_TEST
FAKE_NOTIFY_URL = config.FAKE_NOTIFY_URL
DIAGNOSTIC_INTERVAL_S = 5.0
STUCK_BATCH_AFTER_S = 5.0


class ValidatorPool:
    def __init__(self, num_validators, workflow_name=None):
        self.num_validators = num_validators
        self.pool_task_queue = gevent.queue.Queue()
        self.serializer_req_queue = Queue()
        self.assign_lock = gevent.lock.BoundedSemaphore()
        self.repo = Repository()
        self.handler_task_queues = []
        self.serializer_return_pipes = []
        self.workflow_name = workflow_name
        self.batch_processor_table = {}  # {batch_id: processor_id}
        self.function_pos = {}
        function_info = self.repo.get_function_info(self.repo.get_all_functions(self.workflow_name), self.workflow_name)
        for func, info in function_info.items():
            self.function_pos[func] = info['ip']

        for i in range(self.num_validators):
            task_queue = Queue()
            serializer_return_pipe = Queue()
            p = ValidatorProcess(i, workflow_name, task_queue, self.serializer_req_queue,  serializer_return_pipe)
            p.daemon = True
            p.start()
            self.handler_task_queues.append(task_queue)
            self.serializer_return_pipes.append(serializer_return_pipe)
        self.serializer = SerializerProcess(self.workflow_name, self.serializer_req_queue, self.serializer_return_pipes, self.handler_task_queues, self.function_pos)
        self.serializer.daemon = True
        self.serializer.start()
        self.init()

    def init(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)

    def _dispatch_loop(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        gevent.spawn(self.dispatch)
    
    def dispatch(self):
        while not self.pool_task_queue.empty():
            req = self.pool_task_queue.get()
            batch_id = req[0]
            processor_id_to_assign = hash(batch_id) % self.num_validators
            self.handler_task_queues[processor_id_to_assign].put(req)

    def submit(self, batch_id, op, data={}):
        # logging.info(f"[{self.workflow_name}] submit batch {batch_id}.")
        self.pool_task_queue.put((batch_id, op, data))

class ValidatorProcess(Process):

    def __init__(self, validator_id, workflow_name, task_queue, serializer_req_queue, child_get):
        super().__init__()
        self.validator_id = validator_id
        self.workflow_name = workflow_name
        self.task_queue = task_queue
        self.serializer_req_queue = serializer_req_queue
        self.serializer_return_pipe = child_get

    def _dispatch_loop(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        gevent.spawn(self.dispatch_serilizer_response)

    def _diagnostic_loop(self):
        gevent.spawn_later(DIAGNOSTIC_INTERVAL_S, self._diagnostic_loop)
        gevent.spawn(self.report_stuck_batches)

    def dispatch_serilizer_response(self):
        if not self.serializer_return_pipe.empty():
            batch_id, data = self.serializer_return_pipe.get()
            self.response_lock.acquire()
            try:
                return_event = self.response_events.pop(batch_id, None)
                self.serializer_pending.pop(batch_id, None)
            finally:
                self.response_lock.release()
            if return_event is not None:
                return_event.set(data)

    def report_stuck_batches(self):
        snapshots = self.batch_store.stuck_snapshots(STUCK_BATCH_AFTER_S)
        for snapshot in snapshots[:10]:
            log_message(
                self.logger,
                f"[VALIDATOR STUCK] batch={snapshot['batch_id']}, status={snapshot['status']}, "
                f"age={snapshot['age']:.2f}s, idle={snapshot['idle']:.2f}s, "
                f"txs={snapshot['transaction_list']}, aborted={snapshot['aborted_txs']}, "
                f"ports={snapshot.get('container_ports', {})}"
            )


    def run(self):
        # 进程内初始化所有共享属性
        self.logger = setup_validator_logger(self.workflow_name, self.validator_id)
        self.repo = Repository()
        self.function_pos = {}
        self.workflow_graph_topo = {}
        self.worker_ip_set = set()
        function_info = self.repo.get_function_info(self.repo.get_all_functions(self.workflow_name), self.workflow_name)
        for func, info in function_info.items():
            self.function_pos[func] = info['ip']
            self.workflow_graph_topo[func] = info['next']
            self.worker_ip_set.add(info['ip'])
        self.worker_ip_set = list(self.worker_ip_set)
        self.tx_sink_addr = extract_ip(self.function_pos[self.repo.get_end_function(self.workflow_name)])
        self.repair_info = RepairInfo(self.logger, self.workflow_graph_topo,  self.function_pos)
        self.repair_engine = RepairEngine(self.logger, self.repair_info, self.function_pos, self.worker_ip_set, self.workflow_name, self.tx_sink_addr, self.repo)
        self.response_events = {} 
        self.response_lock = gevent.lock.BoundedSemaphore()
        self.serializer_pending = {}
        self.batch_store = ValidatorBatchStore()
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        gevent.spawn_later(DIAGNOSTIC_INTERVAL_S, self._diagnostic_loop)
        last_task_time = time.time()
        while True:
            try:
                batch_id, op, data = self.task_queue.get_nowait()
                gevent.spawn(self.handle_task, batch_id, op, data)
                last_task_time = time.time()
            except:
                if time.time() - last_task_time > 1:
                    gevent.sleep(0.1)

    def handle_task(self, batch_id, op, data):
        if op == VALIDATE:
            batch = data['batch'] 
            state = BatchRuntimeState.from_validate_payload(
                batch_id,
                batch,
                data.get("first_run_finish_time", time.time()),
            )
            self.batch_store.register(state)
            state.mark_status("validating")
            log_message(
                self.logger,
                f"[VALIDATOR VALIDATE] batch={batch_id}, txs={state.transaction_list}"
            )
            expired_keys_per_ip, pessi_sink_info = self.validate(state)
            state.mark_repairing()
            self.repair_engine.repair_batch_after_validate(
                batch_id,
                state.container_port,
                state.read_set,
                state.write_set,
                state.transaction_list,
                expired_keys_per_ip,
                pessi_sink_info,
            )

        elif op == REPAIR_FINISH:
            state = self.batch_store.get(batch_id)
            if state is None:
                log_message(self.logger, f"[VALIDATOR WARNING] Repair finish for unknown batch {batch_id}: {data}")
                return
            batch_finished = data['batch_finished']
            pessi_repair_txs = data['pessi_repair_txs']
            aborted_txs = data['aborted_txs']
            log_message(
                self.logger,
                f"[VALIDATOR REPAIR FINISH] batch={batch_id}, batch_finished={batch_finished}, "
                f"pessi_repair_txs={pessi_repair_txs}, aborted_txs={aborted_txs}"
            )
            # {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]}
            state.record_aborts(aborted_txs)
            self.repair_engine.PessimisticRepairer.modify_batch_write_table_for_abort(
                batch_id,
                aborted_txs,
                state.write_set,
                state.successed_tx_table,
            )
            if batch_finished:
                log_message(self.logger, f"[COMMIT] Batch {batch_id} repair finish, txlist:{state.transaction_list}")
                commit_keys_all = self.repair_engine.PessimisticRepairer.pessimistic_get_commit_keys(batch_id)
                state.mark_repair_finished()
                state.mark_committing()
                ready_batch_list, keys_for_commit_on_worker = self.serializer_request(batch_id, COMMIT, {'commit_keys':commit_keys_all})
                self.commit_batch_list(ready_batch_list, keys_for_commit_on_worker)
            else:
                state.mark_waiting_pessimistic()
                if not pessi_repair_txs:
                    log_message(
                        self.logger,
                        f"[VALIDATOR WARNING] batch={batch_id} entered waiting_pessimistic with empty pessi_repair_txs"
                    )
                log_message(
                    self.logger,
                    f"[VALIDATOR WAIT PESSI] batch={batch_id}, txs={pessi_repair_txs}, "
                    f"ports={state.container_ports_for(pessi_repair_txs)}"
                )
                self.repair_engine.send_pessimistic_repair_req(batch_id, state.container_port, pessi_repair_txs)
        elif op == CASCADED_COMMIT:
            aborted_txs = []
            txid_lists = []
            timestamps = []
            pes_transactions = []
            if SCALABILITY_TEST:
                self.clean_batch_info(data)
                self._post_json(FAKE_NOTIFY_URL, {'batch_id_list': data}, "fake notify cascaded")
                return
            for batch_id in data:
                state = self.batch_store.get(batch_id)
                if state is None:
                    log_message(self.logger, f"[VALIDATOR WARNING] Cascaded commit for unknown batch {batch_id}")
                    continue
                aborted_txs.extend(state.aborted_txs)
                txid_lists.append(state.successed_tx_table)
                timestamps.append(state.timestamps)
                pes_transactions.append(self.repair_engine.pessimistic_repair_txs_per_batch[batch_id])
            # if FAST_PATH_ENABLED:
            #     jobs = [
            #         gevent.spawn(requests.post, url=f"http://{worker_ip}/release", json={"tx_lists":txid_lists, 'workflow_name':self.workflow_name})
            #         for worker_ip in self.worker_ip_set
            #         ]
            #     gevent.joinall(jobs)
            log_message(self.logger, f"[CASCADED COMMIT] : {data} WITH {txid_lists}")
            self.notify_gateway(txid_lists, True, timestamps, aborted_txs, pes_transactions)
            self.clean_batch_info(data)

    def serializer_request(self, batch_id, op, data):
        res_event = event.AsyncResult()
        self.response_lock.acquire()
        self.response_events[batch_id] = res_event
        self.serializer_pending[batch_id] = {
            "op": op,
            "started_at": time.time(),
            "data_keys": sorted(data.keys()),
        }
        self.response_lock.release()
        self.serializer_req_queue.put((self.validator_id, batch_id, op, data))
        return res_event.get()


    def validate(self, state: BatchRuntimeState):
        batch_id = state.batch_id
        self.repair_info.batch_init(batch_id)
        serializer_input = {
            'transaction_list': state.transaction_list,
            'read_set': state.read_set,
            'write_set': state.write_set,
        }
        expired_keys, subjection_set, pessi_sink_info = self.serializer_request(batch_id, VALIDATE, serializer_input)
        expired_keys_per_ip = self.repair_info.construct_repair_metadata(
            batch_id,
            expired_keys,
            subjection_set,
            state.ryw_subjection,
            self.worker_ip_set,
            state.transaction_list,
            state.container_port,
        )
        log_message(self.logger, f"[VALIDATE] Batch {batch_id} validation result: expired_keys={expired_keys}, subjection_set={subjection_set},pessi_sink_info={pessi_sink_info}")
        return expired_keys_per_ip, pessi_sink_info

    def clean_batch_info(self, batch_id_list):
        log_message(self.logger, f"[CLEAN] Cleaning batch info for batches: {batch_id_list}")
        states = self.batch_store.pop_many(batch_id_list)
        for state in states:
            state.mark_committed()
            self.repair_engine.clean_table_of_batch(state.batch_id)

    # commit_batch_list : [(batch_id, version), ...]
    def commit_batch_list(self, commit_batch_list, keys_for_commit_per_ip):
        if SCALABILITY_TEST:
            txid_lists = []
            for batch_id in commit_batch_list:
                state = self.batch_store.get(batch_id)
                if state is not None:
                    txid_lists.append(state.transaction_list)
            self.clean_batch_info(commit_batch_list)
            self._post_json(FAKE_NOTIFY_URL, {'batch_id_list': commit_batch_list}, "fake notify commit")
            return
        if commit_batch_list:
            txid_lists, timestamps, abort_txs, pes_txs = [], [], [], []
            worker_commit_set = {worker_ip:{'keys':[], 'txs':[], 'aborted_txs': []} for worker_ip in self.worker_ip_set}
            for key, commit_key_info in keys_for_commit_per_ip.items():
                writer_tx_id, writer_func, version = commit_key_info
                target_ip = self.function_pos[writer_func]
                worker_commit_set[target_ip]['keys'].append([f'{writer_tx_id}:PUT:{writer_func}:{key}', version])

            for batch_id in commit_batch_list:
                state = self.batch_store.get(batch_id)
                if state is None:
                    log_message(self.logger, f"[VALIDATOR WARNING] Commit for unknown batch {batch_id}")
                    continue
                timestamps.append(state.timestamps)
                successed_tx_list = state.successed_tx_table
                txid_lists.append(successed_tx_list)
                aborted_txs_this_batch = state.aborted_txs
                abort_txs.extend(state.aborted_txs)
                pes_txs.append(self.repair_engine.pessimistic_repair_txs_per_batch[batch_id])
                for worker_ip in self.worker_ip_set:
                    worker_commit_set[worker_ip]['txs'].extend(successed_tx_list)
                    worker_commit_set[worker_ip]['aborted_txs'].extend(aborted_txs_this_batch)

            log_message(self.logger, f"[COMMIT] Commit batch list: {commit_batch_list}, txid_lists: {txid_lists}, aborted_txs:{abort_txs}, timestamps:{timestamps}")
            jobs = [
                gevent.spawn(self.trigger_worker_commit, ip, worker_commit_set[ip])
                for ip in worker_commit_set
            ]
            self._join_and_report(jobs, f"worker commit {commit_batch_list}")
            self.repair_engine.sink_release_optimistic_info(commit_batch_list)
            self.notify_gateway(txid_lists, True, timestamps, abort_txs, pes_txs)
            self.clean_batch_info(commit_batch_list)


    def trigger_worker_commit(self, ip, commit_list):
        if not ip.endswith(":7500"):
            url = f"http://{ip}:7500/commit"
        else:
            url = f"http://{ip}/commit"
        
        data = {
            'workflow_name': self.workflow_name,
            "commit_list": commit_list
        }

        return self._post_json(url, data, f"commit worker {ip}") is not None

    def notify_gateway(self, txid_lists, success:bool, timestamps, aborted_txs, pessi_txs):
        url = 'http://{}/notify'.format(GATEWAY_NOTIFY_ADDR)
        log_message(self.logger, f"[NOTIFY] Notify gateway: {url}, transaction_id_lists: {txid_lists}, timestamps:{timestamps}, pessimistic_txs:{pessi_txs}")
        data = {
            'transaction_id_lists': txid_lists,
            'success': success,
            'timestamps':timestamps,
            'aborted_txs': aborted_txs,
            'pessimistic_txs': pessi_txs
        }
        r = self._post_json(url, data, "notify gateway")
        return r.json() if r is not None else {}

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
