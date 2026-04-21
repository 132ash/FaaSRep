from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

import sys

sys.path.append('../../config')
import config


REPAIRED = config.REPAIRED
ABORTED = config.ABORTED
WAITING = config.RUNNING

OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR


@dataclass
class TransactionRepairState:
    tx_id: str
    batch_id: str
    optimistic_state: str = WAITING
    needs_pessimistic: bool = False
    pessimistic_ready: bool = False
    pessimistic_running: bool = False
    final_state: Optional[str] = None
    finish_recorded: bool = False
    optimistic_result_rejected: bool = False
    optimistic_successors: Set[str] = field(default_factory=set)
    missing_predecessors: Set[str] = field(default_factory=set)

    def add_optimistic_successors(self, successors: Iterable[str]) -> None:
        self.optimistic_successors.update(successors)

    def mark_needs_pessimistic(self, reason: str = "") -> None:
        self.needs_pessimistic = True
        if reason:
            self.missing_predecessors.add(reason)

    def can_trigger_pessimistic(self) -> bool:
        return (
            self.needs_pessimistic
            and self.pessimistic_ready
            and not self.pessimistic_running
            and self.final_state is None
        )


@dataclass
class BatchRepairState:
    batch_id: str
    tx_order: List[str]
    finished_count: int = 0
    resolved_prefix_index: int = -1
    batch_finished: bool = False
    batch_successors: Dict[str, Set[str]] = field(default_factory=dict)
    tx_successors: Dict[str, Set[str]] = field(default_factory=dict)
    prev_fin_count: Dict[str, int] = field(default_factory=dict)
    ready_pessi_queue: Set[str] = field(default_factory=set)
    finished_by_index: List[bool] = field(default_factory=list)
    missing_predecessors: Dict[str, Set[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.tx_order = list(self.tx_order)
        self.tx_idx = {tx_id: idx for idx, tx_id in enumerate(self.tx_order)}
        self.prev_fin_count = {tx_id: 0 for tx_id in self.tx_order}
        self.finished_by_index = [False] * len(self.tx_order)

    @property
    def batch_size(self) -> int:
        return len(self.tx_order)

    def add_batch_dependency(self, tx_ids: Iterable[str], predecessor: str = "") -> None:
        for tx_id in set(tx_ids):
            if tx_id not in self.prev_fin_count:
                self.missing_predecessors.setdefault(tx_id, set()).add(predecessor or "unknown_batch")
                continue
            self.prev_fin_count[tx_id] += 1

    def add_tx_dependencies(self, tx_sub: Dict[str, Iterable[str]]) -> None:
        for prev_tx_id, next_txs in tx_sub.items():
            next_tx_set = set(next_txs)
            if prev_tx_id not in self.tx_idx:
                for tx_id in next_tx_set:
                    self.missing_predecessors.setdefault(tx_id, set()).add(prev_tx_id)
                continue
            self.tx_successors.setdefault(prev_tx_id, set()).update(next_tx_set)
            for tx_id in next_tx_set:
                if tx_id not in self.prev_fin_count:
                    self.missing_predecessors.setdefault(tx_id, set()).add(prev_tx_id)
                    continue
                self.prev_fin_count[tx_id] += 1

    def mark_initial_ready(self) -> Set[str]:
        ready = {
            tx_id
            for tx_id, count in self.prev_fin_count.items()
            if count == 0
        }
        self.ready_pessi_queue.update(ready)
        return ready

    def add_successor_batch(self, next_batch_id: str, tx_ids: Iterable[str]) -> None:
        self.batch_successors.setdefault(next_batch_id, set()).update(tx_ids)

    def release_transactions_after_tx_finish(self, tx_id: str) -> Dict[str, Set[str]]:
        ready: Dict[str, Set[str]] = {}
        tx_idx = self.tx_idx.get(tx_id)
        if tx_idx is None:
            return ready
        self.finished_by_index[tx_idx] = True
        if self.resolved_prefix_index == tx_idx - 1:
            while (
                self.resolved_prefix_index < self.batch_size - 1
                and self.finished_by_index[self.resolved_prefix_index + 1]
            ):
                self.resolved_prefix_index += 1
                current_tx_id = self.tx_order[self.resolved_prefix_index]
                self._release_successors(current_tx_id, ready)
        return ready

    def release_successors_after_batch_finish(self) -> Dict[str, Set[str]]:
        ready: Dict[str, Set[str]] = {}
        for next_batch_id, next_txs in self.batch_successors.items():
            ready.setdefault(next_batch_id, set()).update(next_txs)
        return ready

    def mark_tx_ready(self, tx_ids: Iterable[str]) -> Set[str]:
        ready: Set[str] = set()
        for tx_id in set(tx_ids):
            if tx_id not in self.prev_fin_count:
                self.missing_predecessors.setdefault(tx_id, set()).add("unknown_release")
                continue
            if self.prev_fin_count[tx_id] > 0:
                self.prev_fin_count[tx_id] -= 1
            if self.prev_fin_count[tx_id] == 0:
                self.ready_pessi_queue.add(tx_id)
                ready.add(tx_id)
        return ready

    def _release_successors(self, prev_tx_id: str, ready: Dict[str, Set[str]]) -> None:
        successors = self.tx_successors.get(prev_tx_id, set())
        newly_ready = self.mark_tx_ready(successors)
        if newly_ready:
            ready.setdefault(self.batch_id, set()).update(newly_ready)


@dataclass
class SinkCommand:
    batch_finished: bool = False
    pessi_repair_txs: Set[str] = field(default_factory=set)
    aborted_txs: Set[str] = field(default_factory=set)


# Compatibility aliases for older imports. The state machine now uses the
# explicit dataclasses above.
PessimisticBatchState = BatchRepairState
OptimisticTransactionState = TransactionRepairState
