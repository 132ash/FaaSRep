"""State machine for staged writes.

``ShadowStore`` is independent of Flask and DynamoDB so its term and flush
semantics can be unit-tested with a tiny fake database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import time
from collections import defaultdict
from gevent.lock import Semaphore


@dataclass
class Attempt:
    term: int
    birth_seq: int
    state: str = 'ACTIVE'
    writes: dict = field(default_factory=dict)
    op_results: dict = field(default_factory=dict)
    flush_id: str | None = None
    frozen: list | None = None
    flushed_keys: set = field(default_factory=set)
    metrics: dict = field(default_factory=lambda: defaultdict(float))


class ShadowStore:
    def __init__(self, db_repo):
        self.db = db_repo
        self._lock = Semaphore(1)
        self._attempts = {}
        self._current_terms = {}
        self._metrics = defaultdict(float)

    def begin(self, txid, term, birth_seq):
        with self._lock:
            current = self._current_terms.get(txid)
            if current is None:
                if term != 0:
                    return {'status': 'PROTOCOL_ERROR', 'error': 'first term must be 0'}
                attempt = Attempt(term, birth_seq)
                self._attempts[(txid, term)] = attempt
                self._current_terms[txid] = term
                return self._active_result(txid, attempt)
            if term < current:
                return {'status': 'STALE', 'current_term': current}
            attempt = self._attempts[(txid, current)]
            if term == current:
                if attempt.birth_seq != birth_seq:
                    return {'status': 'PROTOCOL_ERROR', 'error': 'birth_seq mismatch'}
                return self._active_result(txid, attempt) if attempt.state == 'ACTIVE' else {'status': attempt.state}
            if term != current + 1 or attempt.state not in {'DISCARDED', 'COMPLETED'}:
                return {'status': 'PROTOCOL_ERROR', 'error': 'previous term is not terminal'}
            new_attempt = Attempt(term, birth_seq)
            self._attempts[(txid, term)] = new_attempt
            self._current_terms[txid] = term
            return self._active_result(txid, new_attempt)

    def get(self, txid, term, key):
        with self._lock:
            attempt, error = self._active_attempt_locked(txid, term)
            if error:
                return error
            attempt.metrics['get_count'] += 1
            self._metrics['get_count'] += 1
            if key not in attempt.writes:
                return {'status': 'MISS'}
            attempt.metrics['hit_count'] += 1
            self._metrics['hit_count'] += 1
            return {'status': 'HIT', 'value': attempt.writes[key]['value']}

    def put(self, txid, term, key, value, function, op_id):
        with self._lock:
            attempt, error = self._active_attempt_locked(txid, term)
            if error:
                return error
            if op_id in attempt.op_results:
                return dict(attempt.op_results[op_id])
            old = attempt.writes.get(key)
            old_bytes = old['bytes'] if old else 0
            value_bytes = len(json.dumps(value, ensure_ascii=False).encode('utf-8'))
            attempt.writes[key] = {'value': value, 'function': function, 'op_id': op_id, 'bytes': value_bytes}
            attempt.metrics['put_count'] += 1
            attempt.metrics['staged_bytes'] += value_bytes - old_bytes
            attempt.metrics['peak_staged_bytes'] = max(attempt.metrics['peak_staged_bytes'], attempt.metrics['staged_bytes'])
            self._metrics['put_count'] += 1
            self._metrics['staged_bytes'] += value_bytes - old_bytes
            self._metrics['peak_staged_bytes'] = max(self._metrics['peak_staged_bytes'], self._metrics['staged_bytes'])
            result = {'status': 'STAGED'}
            attempt.op_results[op_id] = result
            return result

    def discard(self, txid, term, reason='ERROR'):
        with self._lock:
            attempt, error = self._term_attempt_locked(txid, term)
            if error:
                return error
            if attempt.state == 'DISCARDED':
                return {'status': 'DISCARDED'}
            if attempt.state in {'FLUSHING', 'FLUSHED', 'COMPLETED'}:
                return {'status': 'PROTOCOL_ERROR', 'error': f'cannot discard {attempt.state}'}
            attempt.state = 'DISCARDING'
            self._remove_bytes_locked(attempt)
            attempt.writes.clear()
            attempt.state = 'DISCARDED'
            attempt.metrics['discard_count'] += 1
            return {'status': 'DISCARDED', 'reason': reason}

    def flush(self, txid, term, flush_id):
        """Freeze once, write outside the mutex, and resume partial progress."""
        with self._lock:
            attempt, error = self._term_attempt_locked(txid, term)
            if error:
                return error
            if attempt.state == 'FLUSHED':
                if attempt.flush_id != flush_id:
                    return {'status': 'PROTOCOL_ERROR', 'error': 'flush_id mismatch'}
                return self._flush_result(attempt)
            if attempt.state == 'ACTIVE':
                attempt.state = 'FLUSHING'
                attempt.flush_id = flush_id
                attempt.frozen = [(key, dict(item)) for key, item in attempt.writes.items()]
            elif attempt.state != 'FLUSHING' or attempt.flush_id != flush_id:
                return {'status': 'PROTOCOL_ERROR', 'error': f'cannot flush {attempt.state}'}
            frozen = list(attempt.frozen or [])
            version = f'{attempt.birth_seq}:{attempt.term}'

        started = time.monotonic()
        try:
            for key, item in frozen:
                with self._lock:
                    if key in attempt.flushed_keys:
                        continue
                self.db.put(key, item['value'], version)
                with self._lock:
                    attempt.flushed_keys.add(key)
        except Exception as exc:
            # The state remains FLUSHING.  The coordinator must retain locks and
            # retry this identical flush_id; discarding would expose a partial commit.
            return {'status': 'RETRY', 'error': str(exc), 'flushed_key_count': len(attempt.flushed_keys)}

        with self._lock:
            attempt.state = 'FLUSHED'
            elapsed = time.monotonic() - started
            attempt.metrics['flush_latency'] += elapsed
            attempt.metrics['flushed_key_count'] = len(attempt.flushed_keys)
            self._metrics['flush_count'] += 1
            self._metrics['flushed_key_count'] += len(attempt.flushed_keys)
            self._metrics['flush_latency'] += elapsed
            return self._flush_result(attempt)

    def complete(self, txid, term):
        with self._lock:
            attempt, error = self._term_attempt_locked(txid, term)
            if error:
                return error
            if attempt.state == 'COMPLETED':
                return {'status': 'COMPLETED'}
            if attempt.state != 'FLUSHED':
                return {'status': 'PROTOCOL_ERROR', 'error': f'cannot complete {attempt.state}'}
            self._remove_bytes_locked(attempt)
            attempt.writes.clear()
            attempt.state = 'COMPLETED'
            return {'status': 'COMPLETED'}

    def debug_tx(self, txid):
        with self._lock:
            term = self._current_terms.get(txid)
            if term is None:
                return {'status': 'MISSING'}
            a = self._attempts[(txid, term)]
            return {'status': a.state, 'term': term, 'birth_seq': a.birth_seq,
                    'staged_keys': sorted(a.writes), 'flush_progress': sorted(a.flushed_keys),
                    'metrics': dict(a.metrics)}

    def health(self):
        with self._lock:
            return {'status': 'ok', 'attempts': len(self._attempts), 'metrics': dict(self._metrics)}

    def _active_result(self, txid, attempt):
        return {'status': 'ACTIVE', 'txid': txid, 'term': attempt.term, 'birth_seq': attempt.birth_seq}

    def _term_attempt_locked(self, txid, term):
        current = self._current_terms.get(txid)
        if current is None or term < current:
            return None, {'status': 'STALE', 'current_term': current}
        if term > current:
            return None, {'status': 'PROTOCOL_ERROR', 'error': 'begin must advance term'}
        return self._attempts[(txid, term)], None

    def _active_attempt_locked(self, txid, term):
        attempt, error = self._term_attempt_locked(txid, term)
        if error:
            return None, error
        if attempt.state != 'ACTIVE':
            return None, {'status': 'STALE' if attempt.state in {'DISCARDED', 'COMPLETED'} else 'PROTOCOL_ERROR',
                          'error': f'attempt is {attempt.state}'}
        return attempt, None

    def _remove_bytes_locked(self, attempt):
        self._metrics['staged_bytes'] -= attempt.metrics['staged_bytes']
        attempt.metrics['staged_bytes'] = 0

    @staticmethod
    def _flush_result(attempt):
        return {'status': 'FLUSHED', 'flushed_key_count': len(attempt.flushed_keys),
                'flush_latency': attempt.metrics['flush_latency']}
