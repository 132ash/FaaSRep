from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import gevent.lock
import time


@dataclass
class BatchRuntimeState:
    batch_id: str
    transaction_list: List[str]
    read_set: Dict[str, Dict[str, Dict[str, Any]]]
    write_set: Dict[str, Dict[str, str]]
    ryw_subjection: Dict[str, Dict[str, Dict[str, str]]]
    container_port: Dict[str, Dict[str, Any]]
    first_run_finish_time: float
    repair_start_time: float = 0.0
    repair_finish_time: float = 0.0
    successed_tx_table: Dict[str, bool] = field(default_factory=dict)
    aborted_txs: List[str] = field(default_factory=list)
    status: str = "registered"
    registered_at: float = field(default_factory=time.time)
    last_update_at: float = field(default_factory=time.time)

    @classmethod
    def from_validate_payload(
        cls,
        batch_id: str,
        batch: Dict[str, Any],
        first_run_finish_time: float,
    ) -> "BatchRuntimeState":
        tx_list = list(batch["transaction_list"])
        return cls(
            batch_id=batch_id,
            transaction_list=tx_list,
            read_set=batch["read_set"],
            write_set=batch["write_set"],
            ryw_subjection=batch["RYW_subjection"],
            container_port=batch["container_port"],
            first_run_finish_time=first_run_finish_time,
            successed_tx_table={tx_id: True for tx_id in tx_list},
        )

    @property
    def timestamps(self) -> List[float]:
        return [
            self.first_run_finish_time,
            self.repair_start_time,
            self.repair_finish_time,
        ]

    def mark_status(self, status: str) -> None:
        self.status = status
        self.last_update_at = time.time()

    def mark_repairing(self) -> None:
        self.repair_start_time = time.time()
        self.mark_status("repairing")

    def mark_waiting_pessimistic(self) -> None:
        self.mark_status("waiting_pessimistic")

    def mark_repair_finished(self) -> None:
        self.repair_finish_time = time.time()
        self.mark_status("repair_finished")

    def mark_committing(self) -> None:
        self.mark_status("committing")

    def mark_committed(self) -> None:
        self.mark_status("committed")

    def record_aborts(self, aborted_txs: Iterable[str]) -> None:
        for tx_id in aborted_txs:
            if tx_id not in self.aborted_txs:
                self.aborted_txs.append(tx_id)
            self.successed_tx_table.pop(tx_id, None)
        if aborted_txs:
            self.last_update_at = time.time()

    def container_ports_for(self, tx_ids: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
        selected = list(tx_ids) if tx_ids is not None else list(self.transaction_list)
        return {
            tx_id: dict(self.container_port.get(tx_id, {}))
            for tx_id in selected
        }

    def debug_snapshot(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "age": time.time() - self.registered_at,
            "idle": time.time() - self.last_update_at,
            "transaction_list": list(self.transaction_list),
            "successed_txs": list(self.successed_tx_table.keys()),
            "aborted_txs": list(self.aborted_txs),
            "read_set_txs": sorted(self.read_set.keys()),
            "write_set_txs": sorted(self.write_set.keys()),
            "container_port_txs": sorted(self.container_port.keys()),
            "container_ports": self.container_ports_for(),
            "timestamps": self.timestamps,
        }


class ValidatorBatchStore:
    def __init__(self):
        self._batches: Dict[str, BatchRuntimeState] = {}
        self._lock = gevent.lock.BoundedSemaphore()

    def register(self, state: BatchRuntimeState) -> None:
        self._lock.acquire()
        try:
            self._batches[state.batch_id] = state
        finally:
            self._lock.release()

    def get(self, batch_id: str) -> Optional[BatchRuntimeState]:
        self._lock.acquire()
        try:
            return self._batches.get(batch_id)
        finally:
            self._lock.release()

    def pop(self, batch_id: str) -> Optional[BatchRuntimeState]:
        self._lock.acquire()
        try:
            return self._batches.pop(batch_id, None)
        finally:
            self._lock.release()

    def pop_many(self, batch_ids: Iterable[str]) -> List[BatchRuntimeState]:
        states: List[BatchRuntimeState] = []
        self._lock.acquire()
        try:
            for batch_id in batch_ids:
                state = self._batches.pop(batch_id, None)
                if state is not None:
                    states.append(state)
        finally:
            self._lock.release()
        return states

    def stuck_snapshots(self, stuck_after: float) -> List[Dict[str, Any]]:
        now = time.time()
        snapshots: List[Dict[str, Any]] = []
        self._lock.acquire()
        try:
            for state in self._batches.values():
                if now - state.last_update_at >= stuck_after:
                    snapshots.append(state.debug_snapshot())
        finally:
            self._lock.release()
        return snapshots
