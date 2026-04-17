from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Deque, Dict, Iterable, Mapping, Optional, Set, Tuple

try:
    from models import (
        BatchWriteInfo,
        PessimisticSinkInfo,
        UpstreamRef,
        ValidationResult,
        WriterRef,
    )
except ImportError:  # pragma: no cover - package import path
    from .models import (
        BatchWriteInfo,
        PessimisticSinkInfo,
        UpstreamRef,
        ValidationResult,
        WriterRef,
    )


class GlobalVersionTable:
    def __init__(self, initial_versions: Optional[Mapping[str, Any]] = None):
        self.versions: Dict[str, Any] = dict(initial_versions or {})

    def is_stale(self, key: str, read_version: Any) -> bool:
        current_version = self.versions.get(key)
        return current_version is not None and read_version < current_version

    def mark_committed(self, key: str, version: Any) -> None:
        self.versions[key] = version


class WriterIndex:
    def __init__(self):
        self._writers: Dict[str, Deque[WriterRef]] = defaultdict(deque)

    def has_writers(self, key: str) -> bool:
        return bool(self._writers.get(key))

    def latest_writer(self, key: str) -> Optional[WriterRef]:
        writers = self._writers.get(key)
        return writers[-1] if writers else None

    def first_writer(self, key: str) -> Optional[WriterRef]:
        writers = self._writers.get(key)
        return writers[0] if writers else None

    def add_writer(self, key: str, writer: WriterRef) -> None:
        writers = self._writers[key]
        if writers and writers[-1].batch_id == writer.batch_id:
            writers[-1] = writer
        else:
            writers.append(writer)

    def pop_committed_writer(self, key: str) -> Optional[WriterRef]:
        writers = self._writers.get(key)
        if not writers:
            return None
        writer = writers.popleft()
        if not writers:
            self._writers.pop(key, None)
        return writer

    def as_legacy(self) -> Dict[str, list[tuple[str, str, str]]]:
        return {
            key: [writer.to_legacy() for writer in writers]
            for key, writers in self._writers.items()
        }


class BatchCommitTracker:
    def __init__(self):
        self.batch_write_info: Dict[str, BatchWriteInfo] = {}
        self.batch_validator_assignment: Dict[str, int] = {}
        self.commit_suspended_batches: Dict[str, int] = {}

    def register_batch(self, batch_id: str, handler_id: int, version: str) -> None:
        self.batch_write_info[batch_id] = BatchWriteInfo(version=version)
        self.batch_validator_assignment[batch_id] = handler_id

    def record_write(self, batch_id: str, key: str, ready: bool) -> None:
        self.batch_write_info[batch_id].record_write(key, ready)

    def version_for(self, batch_id: str) -> str:
        return self.batch_write_info[batch_id].version

    def is_ready(self, batch_id: str) -> bool:
        return self.batch_write_info[batch_id].ready

    def suspend(self, batch_id: str, handler_id: int) -> None:
        self.commit_suspended_batches[batch_id] = handler_id

    def mark_key_unblocked(self, batch_id: str) -> bool:
        info = self.batch_write_info.get(batch_id)
        if info is None:
            return False
        info.ready_write_count += 1
        return info.ready

    def pop_suspended_if_ready(self, batch_id: str) -> bool:
        if batch_id not in self.commit_suspended_batches:
            return False
        if not self.is_ready(batch_id):
            return False
        self.commit_suspended_batches.pop(batch_id, None)
        return True

    def pop_batch(self, batch_id: str) -> tuple[BatchWriteInfo, int]:
        info = self.batch_write_info.pop(batch_id)
        handler_id = self.batch_validator_assignment.pop(batch_id)
        self.commit_suspended_batches.pop(batch_id, None)
        return info, handler_id


class DependencyBuilder:
    def __init__(
        self,
        versions: GlobalVersionTable,
        writers: WriterIndex,
        commits: BatchCommitTracker,
    ):
        self.versions = versions
        self.writers = writers
        self.commits = commits

    def validate_batch(
        self,
        handler_id: int,
        batch_id: str,
        version: str,
        transaction_list: Iterable[str],
        read_set_per_batch: Mapping[str, Mapping[str, Mapping[str, Any]]],
        write_set_per_batch: Mapping[str, Mapping[str, str]],
    ) -> ValidationResult:
        tx_order = list(transaction_list)
        tx_index = {tx_id: idx for idx, tx_id in enumerate(tx_order)}
        result = ValidationResult()
        self.commits.register_batch(batch_id, handler_id, version)

        for tx_id in tx_order:
            self._validate_transaction(
                result,
                batch_id,
                tx_id,
                tx_index,
                read_set_per_batch.get(tx_id, {}),
            )
            self._record_writes(
                batch_id,
                tx_id,
                write_set_per_batch.get(tx_id, {}),
            )
        return result

    def _validate_transaction(
        self,
        result: ValidationResult,
        batch_id: str,
        tx_id: str,
        tx_index: Mapping[str, int],
        read_set: Mapping[str, Mapping[str, Any]],
    ) -> None:
        nearest_batch_id: Optional[str] = None
        nearest_batch_version: Optional[str] = None
        nearest_tx_id: Optional[str] = None

        for func, kv_pairs in read_set.items():
            func_plan = result.ensure_func_plan(tx_id, func)
            expired_keys = result.ensure_expired_func(tx_id, func)
            for key, read_version in kv_pairs.items():
                writer = self.writers.latest_writer(key)
                if writer is not None:
                    func_plan.add_upstream(key, UpstreamRef(writer.tx_id, writer.func))
                    result.pessimistic_sink_info.add_tx_successor(writer.tx_id, tx_id)
                    if writer.batch_id != batch_id:
                        expired_keys.add(key)
                        writer_version = self.commits.version_for(writer.batch_id)
                        if (
                            nearest_batch_id is None
                            or nearest_batch_version is None
                            or nearest_batch_version < writer_version
                        ):
                            nearest_batch_id = writer.batch_id
                            nearest_batch_version = writer_version
                    else:
                        if (
                            nearest_tx_id is None
                            or tx_index[nearest_tx_id] < tx_index[writer.tx_id]
                        ):
                            nearest_tx_id = writer.tx_id
                elif self.versions.is_stale(key, read_version):
                    expired_keys.add(key)
                    func_plan.add_expired_key()

        sink_info: PessimisticSinkInfo = result.pessimistic_sink_info
        if nearest_batch_id is not None:
            sink_info.add_batch_dependency(nearest_batch_id, tx_id)
        if nearest_tx_id is not None:
            sink_info.add_tx_dependency(nearest_tx_id, tx_id)

    def _record_writes(
        self,
        batch_id: str,
        tx_id: str,
        write_set: Mapping[str, str],
    ) -> None:
        for key, writer_func in write_set.items():
            ready = not self.writers.has_writers(key)
            self.commits.record_write(batch_id, key, ready)
            self.writers.add_writer(key, WriterRef(batch_id, tx_id, writer_func))


def normalize_commit_keys(commit_keys: Any) -> Set[str]:
    if commit_keys is None:
        return set()
    if isinstance(commit_keys, dict):
        return {key for key, should_commit in commit_keys.items() if should_commit}
    return set(commit_keys)
