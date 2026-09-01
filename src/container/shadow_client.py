"""Small, synchronous HTTP client for the lock and staged-write services."""
from __future__ import annotations

import time
import requests

import container_config
from transaction_errors import PassiveAbortException


class BokiClient:
    def __init__(self, txid, term, birth_seq, function_name):
        self.txid = txid
        self.term = term
        self.birth_seq = birth_seq
        self.function_name = function_name
        self.sequence = 0
        self.metrics = {'lock_wait_latency': 0.0, 'shadow_get_put_latency': 0.0,
                        'db_io_latency': 0.0, 'lock_request_count': 0,
                        'shadow_get_count': 0, 'shadow_hit_count': 0, 'shadow_put_count': 0}

    def _op_id(self, operation):
        self.sequence += 1
        return f'{self.function_name}:{self.sequence}:{operation}'

    @staticmethod
    def _post(addr, path, payload, timeout=35):
        # Keep the exact payload (and therefore op_id/flush_id) across a lost
        # response.  Service-side idempotency then makes this safe.
        last_error = None
        for _ in range(2):
            try:
                response = requests.post(f'http://{addr}{path}', json=payload, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
        raise last_error

    def lock(self, key, mode):
        started = time.monotonic()
        result = self._post(container_config.LOCK_MANAGER_ADDR, '/lock', {
            'txid': self.txid, 'term': self.term, 'birth_seq': self.birth_seq,
            'key': key, 'mode': mode, 'op_id': self._op_id('lock'),
            'deadline_seconds': container_config.LOCK_WAIT_DEADLINE_SECONDS,
        })
        elapsed = time.monotonic() - started
        self.metrics['lock_request_count'] += 1
        self.metrics['lock_wait_latency'] += result.get('wait_latency', elapsed if result.get('status') == 'GRANTED' else 0)
        if result.get('status') == 'ABORT':
            raise PassiveAbortException(result.get('abort_type', 'WAIT_DIE'))
        if result.get('status') != 'GRANTED':
            raise PassiveAbortException(result.get('status', 'LOCK_ERROR'))

    def get(self, key):
        started = time.monotonic()
        result = self._post(container_config.SHADOW_SERVICE_ADDR, '/get',
                            {'txid': self.txid, 'term': self.term, 'key': key})
        self.metrics['shadow_get_put_latency'] += time.monotonic() - started
        self.metrics['shadow_get_count'] += 1
        if result.get('status') == 'HIT':
            self.metrics['shadow_hit_count'] += 1
            return True, result['value']
        if result.get('status') == 'MISS':
            return False, None
        raise PassiveAbortException(result.get('status', 'SHADOW_ERROR'))

    def put(self, key, value):
        started = time.monotonic()
        result = self._post(container_config.SHADOW_SERVICE_ADDR, '/put', {
            'txid': self.txid, 'term': self.term, 'key': key, 'value': value,
            'function': self.function_name, 'op_id': self._op_id('put'),
        })
        self.metrics['shadow_get_put_latency'] += time.monotonic() - started
        self.metrics['shadow_put_count'] += 1
        if result.get('status') != 'STAGED':
            raise PassiveAbortException(result.get('status', 'SHADOW_ERROR'))
