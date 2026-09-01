"""In-memory strict-2PL lock manager used by the Boki-style single-node mode.

The manager intentionally has no database dependency.  Its public methods are
also used directly by the unit tests; ``proxy.py`` is only the HTTP wrapper.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import time
from typing import Dict, Optional, Tuple

import gevent
from gevent.event import Event
from gevent.lock import Semaphore


Owner = Tuple[str, int]


@dataclass
class Waiter:
    owner: Owner
    mode: str
    op_id: str
    event: Event = field(default_factory=Event)
    enqueued_at: float = field(default_factory=time.monotonic)
    result: Optional[dict] = None


@dataclass
class LockEntry:
    writer: Optional[Owner] = None
    readers: Dict[Owner, int] = field(default_factory=dict)
    waiters: list[Waiter] = field(default_factory=list)


@dataclass
class Transaction:
    txid: str
    birth_seq: int
    current_term: int
    state: str = "ACTIVE"
    held_locks: Dict[str, str] = field(default_factory=dict)
    waiters: Dict[str, Waiter] = field(default_factory=dict)
    op_results: Dict[str, dict] = field(default_factory=dict)
    metrics: dict = field(default_factory=lambda: defaultdict(float))

    @property
    def priority(self):
        return (self.birth_seq, self.txid)


class LockManager:
    """Strict 2PL with S/X locks, upgrades and Wait-Die prevention."""

    def __init__(self, wait_deadline_seconds: float = 30.0):
        self._mutex = Semaphore(1)
        self._locks: Dict[str, LockEntry] = {}
        self._tx: Dict[str, Transaction] = {}
        self._priority_owners: Dict[int, str] = {}
        self._metrics = defaultdict(float)
        self.wait_deadline_seconds = wait_deadline_seconds

    def begin(self, txid: str, term: int = 0, global_req_id=None) -> dict:
        """Begin an attempt using the immutable client-assigned global order.

        ``global_req_id`` is deliberately not inferred from service arrival
        order.  A retry changes only ``term`` and must retain this priority.
        """
        try:
            priority = int(global_req_id)
        except (TypeError, ValueError):
            return {"status": "PROTOCOL_ERROR", "error": "global_req_id must be an integer"}
        if priority < 0:
            return {"status": "PROTOCOL_ERROR", "error": "global_req_id must be non-negative"}
        with self._mutex:
            tx = self._tx.get(txid)
            if tx is None:
                if term != 0:
                    return {"status": "PROTOCOL_ERROR", "error": "first term must be 0"}
                owner = self._priority_owners.get(priority)
                if owner is not None and owner != txid:
                    return {"status": "PROTOCOL_ERROR", "error": "global_req_id already belongs to another transaction"}
                tx = Transaction(txid=txid, birth_seq=priority, current_term=term)
                self._tx[txid] = tx
                self._priority_owners[priority] = txid
                return self._begin_result(tx)
            if tx.birth_seq != priority:
                return {"status": "PROTOCOL_ERROR", "error": "global_req_id changed across attempts",
                        "birth_seq": tx.birth_seq}
            if term < tx.current_term:
                return {"status": "STALE", "birth_seq": tx.birth_seq, "current_term": tx.current_term}
            if term == tx.current_term:
                if tx.state == "ACTIVE":
                    return self._begin_result(tx)
                return {"status": "PROTOCOL_ERROR", "error": f"term is {tx.state}", "birth_seq": tx.birth_seq}
            if term != tx.current_term + 1 or tx.state not in {"ABORTED", "RELEASED"}:
                return {"status": "PROTOCOL_ERROR", "error": "cannot advance active term", "birth_seq": tx.birth_seq}
            tx.current_term = term
            tx.state = "ACTIVE"
            tx.held_locks.clear()
            tx.waiters.clear()
            tx.op_results.clear()
            tx.metrics = defaultdict(float)
            return self._begin_result(tx)

    def _begin_result(self, tx: Transaction) -> dict:
        return {"status": "ACTIVE", "txid": tx.txid, "term": tx.current_term, "birth_seq": tx.birth_seq}

    def lock(self, txid: str, term: int, birth_seq: int, key: str, mode: str, op_id: str,
             deadline_seconds: Optional[float] = None) -> dict:
        if mode not in {"S", "X"}:
            return {"status": "PROTOCOL_ERROR", "error": "mode must be S or X"}
        owner = (txid, term)
        waiter = None
        with self._mutex:
            self._metrics['lock_request_count'] += 1
            tx, failure = self._active_tx_locked(txid, term, birth_seq)
            if failure:
                return failure
            cached = tx.op_results.get(op_id)
            if cached is not None:
                return dict(cached)
            pending = tx.waiters.get(op_id)
            if pending is not None:
                waiter = pending
            else:
                entry = self._locks.setdefault(key, LockEntry())
                blockers = self._blockers(entry, owner, mode)
                # A mutually exclusive queued request is a virtual blocker.  It
                # prevents readers from repeatedly barging ahead of a writer.
                queued = [w.owner for w in entry.waiters if self._modes_conflict(mode, w.mode) and w.owner != owner]
                all_blockers = blockers + queued
                if not blockers and not queued:
                    result = self._grant_locked(tx, entry, key, owner, mode, op_id, 0.0)
                    return result
                if any(self._priority_of_locked(other) < tx.priority for other in all_blockers):
                    tx.state = "ABORTING"
                    tx.metrics["wait_die_abort_count"] += 1
                    self._metrics['wait_die_abort_count'] += 1
                    result = {"status": "ABORT", "abort_type": "WAIT_DIE", "key": key}
                    tx.op_results[op_id] = result
                    return result
                waiter = Waiter(owner=owner, mode=mode, op_id=op_id)
                entry.waiters.append(waiter)
                entry.waiters.sort(key=lambda w: self._priority_of_locked(w.owner))
                tx.waiters[op_id] = waiter
                tx.metrics["wait_count"] += 1
                self._metrics['wait_count'] += 1

        timeout = self.wait_deadline_seconds if deadline_seconds is None else deadline_seconds
        if not waiter.event.wait(timeout=timeout):
            with self._mutex:
                tx = self._tx.get(txid)
                if tx and tx.current_term == term and tx.waiters.get(op_id) is waiter:
                    self._remove_waiter_locked(key, waiter)
                    tx.waiters.pop(op_id, None)
                    tx.state = "ABORTING"
                    tx.metrics["timeout_abort_count"] += 1
                    self._metrics['timeout_abort_count'] += 1
                    result = {"status": "ABORT", "abort_type": "TIMEOUT", "key": key}
                    tx.op_results[op_id] = result
                    self._drain_keys_locked({key})
                    return result
        return waiter.result or {"status": "ABORT", "abort_type": "CANCELLED", "key": key}

    def unlock(self, txid: str, term: int, all: bool = True) -> dict:
        if not all:
            return {"status": "PROTOCOL_ERROR", "error": "strict 2PL only supports all=true"}
        with self._mutex:
            tx, failure = self._term_tx_locked(txid, term)
            if failure:
                return failure
            if tx.state == "RELEASED":
                return self._terminal_result(tx, "RELEASED")
            if tx.state != "ACTIVE":
                return {"status": "PROTOCOL_ERROR", "error": f"cannot unlock {tx.state}"}
            tx.state = "RELEASING"
            affected = self._release_all_locked(tx)
            tx.state = "RELEASED"
            self._drain_keys_locked(affected)
            return self._terminal_result(tx, "RELEASED")

    def abort(self, txid: str, term: int, abort_type: str = "ERROR") -> dict:
        with self._mutex:
            tx, failure = self._term_tx_locked(txid, term)
            if failure:
                return failure
            if tx.state == "ABORTED":
                return self._terminal_result(tx, "ABORTED")
            if tx.state == "RELEASED":
                return {"status": "PROTOCOL_ERROR", "error": "cannot abort released transaction"}
            tx.state = "ABORTING"
            affected = self._release_all_locked(tx)
            affected |= self._cancel_waiters_locked(tx, {"status": "ABORT", "abort_type": abort_type})
            tx.state = "ABORTED"
            self._drain_keys_locked(affected)
            result = self._terminal_result(tx, "ABORTED")
            result["abort_type"] = abort_type
            return result

    def debug_tx(self, txid: str) -> dict:
        with self._mutex:
            tx = self._tx.get(txid)
            if not tx:
                return {"status": "MISSING"}
            return {"status": tx.state, "txid": txid, "term": tx.current_term,
                    "birth_seq": tx.birth_seq, "held_locks": dict(tx.held_locks),
                    "waiting_requests": list(tx.waiters), "metrics": dict(tx.metrics)}

    def health(self) -> dict:
        with self._mutex:
            return {"status": "ok", "transactions": len(self._tx), "keys": len(self._locks),
                    "waiter_count": sum(len(entry.waiters) for entry in self._locks.values()),
                    "metrics": dict(self._metrics)}

    def _term_tx_locked(self, txid, term):
        tx = self._tx.get(txid)
        if tx is None:
            return None, {"status": "STALE"}
        if term != tx.current_term:
            return None, {"status": "STALE", "current_term": tx.current_term}
        return tx, None

    def _active_tx_locked(self, txid, term, birth_seq):
        tx, failure = self._term_tx_locked(txid, term)
        if failure:
            return None, failure
        if tx.birth_seq != birth_seq:
            return None, {"status": "PROTOCOL_ERROR", "error": "birth_seq mismatch"}
        if tx.state != "ACTIVE":
            return None, {"status": "STALE" if tx.state in {"ABORTED", "RELEASED"} else "ABORT", "abort_type": "CANCELLED"}
        return tx, None

    def _priority_of_locked(self, owner: Owner):
        tx = self._tx.get(owner[0])
        if tx is None:
            return (float("inf"), owner[0])
        return tx.priority

    @staticmethod
    def _modes_conflict(left: str, right: str) -> bool:
        return left == "X" or right == "X"

    def _blockers(self, entry: LockEntry, owner: Owner, mode: str) -> list[Owner]:
        blockers = []
        if entry.writer is not None and entry.writer != owner:
            blockers.append(entry.writer)
        if mode == "X":
            blockers.extend(reader for reader in entry.readers if reader != owner)
        return blockers

    def _grant_locked(self, tx: Transaction, entry: LockEntry, key: str, owner: Owner, mode: str,
                      op_id: str, waited: float) -> dict:
        held = tx.held_locks.get(key)
        # X is sufficient for a later S request and is intentionally kept X.
        if held == "X":
            pass
        elif held == "S" and mode == "X":
            entry.readers.pop(owner, None)
            entry.writer = owner
            tx.held_locks[key] = "X"
        elif mode == "X":
            entry.writer = owner
            tx.held_locks[key] = "X"
        else:
            entry.readers[owner] = entry.readers.get(owner, 0) + 1
            tx.held_locks[key] = "S"
        tx.metrics["lock_request_count"] += 1
        if waited:
            tx.metrics["lock_wait_latency"] += waited
            self._metrics['lock_wait_latency'] += waited
        else:
            tx.metrics["immediate_grant_count"] += 1
            self._metrics['immediate_grant_count'] += 1
        result = {"status": "GRANTED", "key": key, "mode": tx.held_locks[key], "wait_latency": waited}
        tx.op_results[op_id] = result
        tx.waiters.pop(op_id, None)
        return result

    def _release_all_locked(self, tx: Transaction) -> set[str]:
        owner = (tx.txid, tx.current_term)
        affected = set(tx.held_locks)
        for key in affected:
            entry = self._locks.get(key)
            if not entry:
                continue
            if entry.writer == owner:
                entry.writer = None
            entry.readers.pop(owner, None)
        tx.held_locks.clear()
        return affected

    def _cancel_waiters_locked(self, tx: Transaction, result: dict) -> set[str]:
        affected = set()
        for waiter in list(tx.waiters.values()):
            for key, entry in self._locks.items():
                if waiter in entry.waiters:
                    entry.waiters.remove(waiter)
                    affected.add(key)
                    break
            waiter.result = dict(result)
            waiter.event.set()
        tx.waiters.clear()
        return affected

    def _remove_waiter_locked(self, key: str, waiter: Waiter) -> None:
        entry = self._locks.get(key)
        if entry and waiter in entry.waiters:
            entry.waiters.remove(waiter)

    def _drain_keys_locked(self, keys: set[str]) -> None:
        for key in keys:
            entry = self._locks.get(key)
            if not entry:
                continue
            entry.waiters[:] = [w for w in entry.waiters if self._waiter_active_locked(w)]
            while entry.waiters:
                waiter = entry.waiters[0]
                tx = self._tx[waiter.owner[0]]
                if self._blockers(entry, waiter.owner, waiter.mode):
                    break
                entry.waiters.pop(0)
                result = self._grant_locked(tx, entry, key, waiter.owner, waiter.mode, waiter.op_id,
                                            time.monotonic() - waiter.enqueued_at)
                waiter.result = result
                waiter.event.set()
                if waiter.mode == "X":
                    break
                # Grant an initial run of shared waiters; stop before a writer.
                if entry.waiters and entry.waiters[0].mode == "X":
                    break
            if entry.writer is None and not entry.readers and not entry.waiters:
                self._locks.pop(key, None)

    def _waiter_active_locked(self, waiter: Waiter) -> bool:
        tx = self._tx.get(waiter.owner[0])
        return bool(tx and tx.current_term == waiter.owner[1] and tx.state == "ACTIVE" and tx.waiters.get(waiter.op_id) is waiter)

    @staticmethod
    def _terminal_result(tx: Transaction, status: str) -> dict:
        return {"status": status, "txid": tx.txid, "term": tx.current_term, "metrics": dict(tx.metrics)}
