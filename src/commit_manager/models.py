from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional, Set, Tuple


@dataclass(frozen=True)
class WriterRef:
    batch_id: str
    tx_id: str
    func: str

    @classmethod
    def from_legacy(cls, value: Iterable[str]) -> "WriterRef":
        batch_id, tx_id, func = value
        return cls(str(batch_id), str(tx_id), str(func))

    def to_legacy(self) -> Tuple[str, str, str]:
        return (self.batch_id, self.tx_id, self.func)


@dataclass(frozen=True)
class UpstreamRef:
    tx_id: str
    func: str

    @classmethod
    def from_legacy(cls, value: Any) -> "UpstreamRef":
        if isinstance(value, UpstreamRef):
            return value
        tx_id, func = value
        return cls(str(tx_id), str(func))

    def to_legacy(self) -> list[str]:
        return [self.tx_id, self.func]


@dataclass
class FunctionRepairPlan:
    dirty: bool = False
    upstream_keys: Dict[str, UpstreamRef] = field(default_factory=dict)
    ryw_keys: Dict[str, str] = field(default_factory=dict)
    successor_port: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_legacy(cls, value: Optional[Mapping[str, Any]]) -> "FunctionRepairPlan":
        if not value:
            return cls()
        upstream = {
            key: UpstreamRef.from_legacy(ref)
            for key, ref in value.get("upstream_keys", {}).items()
        }
        return cls(
            dirty=bool(value.get("dirty", False)),
            upstream_keys=upstream,
            ryw_keys=dict(value.get("RYW_keys", {})),
            successor_port=dict(value.get("successor_port", {})),
        )

    def merge_dependency(self, other: "FunctionRepairPlan") -> None:
        self.dirty = self.dirty or other.dirty
        self.upstream_keys.update(other.upstream_keys)

    def add_upstream(self, key: str, upstream: UpstreamRef) -> None:
        self.dirty = True
        self.upstream_keys[key] = upstream

    def add_expired_key(self) -> None:
        self.dirty = True

    @property
    def upstream_count(self) -> int:
        return len(set(self.upstream_keys.values()))

    def to_legacy(self) -> Dict[str, Any]:
        return {
            "dirty": self.dirty,
            "up_cnt": self.upstream_count,
            "upstream_keys": {
                key: upstream.to_legacy()
                for key, upstream in self.upstream_keys.items()
            },
            "RYW_keys": dict(self.ryw_keys),
            "successor_port": dict(self.successor_port),
        }


@dataclass
class TransactionRepairPlan:
    tx_id: str
    functions: Dict[str, FunctionRepairPlan] = field(default_factory=dict)

    def ensure_function(self, func: str) -> FunctionRepairPlan:
        return self.functions.setdefault(func, FunctionRepairPlan())

    def to_legacy(self) -> Dict[str, Dict[str, Any]]:
        return {func: plan.to_legacy() for func, plan in self.functions.items()}


@dataclass
class PessimisticSinkInfo:
    batch_deps: Dict[str, Set[str]] = field(default_factory=dict)
    tx_deps: Dict[str, Set[str]] = field(default_factory=dict)
    last_tx: Dict[str, str] = field(default_factory=dict)
    tx_successors: Dict[str, Set[str]] = field(default_factory=dict)

    def add_batch_dependency(self, prev_batch_id: str, tx_id: str) -> None:
        self.batch_deps.setdefault(prev_batch_id, set()).add(tx_id)

    def add_tx_dependency(self, prev_tx_id: str, tx_id: str) -> None:
        self.tx_deps.setdefault(prev_tx_id, set()).add(tx_id)
        self.last_tx[tx_id] = prev_tx_id

    def add_tx_successor(self, prev_tx_id: str, tx_id: str) -> None:
        self.tx_successors.setdefault(prev_tx_id, set()).add(tx_id)

    def to_legacy(self) -> Dict[str, Any]:
        return {
            "batch_sub": {
                batch_id: sorted(tx_ids)
                for batch_id, tx_ids in self.batch_deps.items()
            },
            "tx_sub": {
                tx_id: sorted(next_txs)
                for tx_id, next_txs in self.tx_deps.items()
            },
            "last_tx": dict(self.last_tx),
            "whole_tx_sub": {
                tx_id: {next_tx: True for next_tx in sorted(next_txs)}
                for tx_id, next_txs in self.tx_successors.items()
            },
        }


@dataclass
class ValidationResult:
    expired_keys: Dict[str, Dict[str, Set[str]]] = field(default_factory=dict)
    repair_deps: Dict[str, TransactionRepairPlan] = field(default_factory=dict)
    pessimistic_sink_info: PessimisticSinkInfo = field(default_factory=PessimisticSinkInfo)

    def ensure_func_plan(self, tx_id: str, func: str) -> FunctionRepairPlan:
        tx_plan = self.repair_deps.setdefault(tx_id, TransactionRepairPlan(tx_id))
        return tx_plan.ensure_function(func)

    def ensure_expired_func(self, tx_id: str, func: str) -> Set[str]:
        return self.expired_keys.setdefault(tx_id, {}).setdefault(func, set())

    def to_legacy(self) -> Tuple[Dict[str, Dict[str, Dict[str, bool]]], Dict[str, Any], Dict[str, Any]]:
        expired = {
            tx_id: {
                func: {key: True for key in sorted(keys)}
                for func, keys in funcs.items()
            }
            for tx_id, funcs in self.expired_keys.items()
        }
        repair_deps = {
            tx_id: tx_plan.to_legacy()
            for tx_id, tx_plan in self.repair_deps.items()
        }
        return expired, repair_deps, self.pessimistic_sink_info.to_legacy()


@dataclass
class BatchWriteInfo:
    version: str
    writes: Set[str] = field(default_factory=set)
    ready_write_count: int = 0
    all_write_count: int = 0

    def record_write(self, key: str, ready: bool) -> None:
        if key in self.writes:
            return
        self.writes.add(key)
        self.all_write_count += 1
        if ready:
            self.ready_write_count += 1

    @property
    def ready(self) -> bool:
        return self.ready_write_count == self.all_write_count
