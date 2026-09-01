import gevent

from src.commit_manager.lock_manager import LockManager
from src.shadow_service.shadow_store import ShadowStore


class FakeDB:
    def __init__(self):
        self.rows = {}
        self.fail_once = False

    def put(self, key, value, version):
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError('temporary database error')
        self.rows[key] = (value, version)


def start(manager, txid, global_req_id):
    result = manager.begin(txid, 0, global_req_id)
    assert result['status'] == 'ACTIVE'
    return result['birth_seq']


def acquire(manager, txid, birth, key, mode, op):
    return manager.lock(txid, 0, birth, key, mode, op, deadline_seconds=1)


def test_shared_locks_upgrade_and_wait_die():
    manager = LockManager(wait_deadline_seconds=1)
    old = start(manager, 'old', 10)
    young = start(manager, 'young', 20)
    assert acquire(manager, 'old', old, 'k', 'S', 'old-s')['status'] == 'GRANTED'
    assert acquire(manager, 'young', young, 'k', 'S', 'young-s')['status'] == 'GRANTED'
    # A younger upgrader sees an older reader and is selected as the victim.
    assert acquire(manager, 'young', young, 'k', 'X', 'young-x')['status'] == 'ABORT'
    assert manager.abort('young', 0, 'WAIT_DIE')['status'] == 'ABORTED'
    assert acquire(manager, 'old', old, 'k', 'X', 'old-x')['status'] == 'GRANTED'


def test_old_transaction_waits_and_is_woken_by_unlock():
    manager = LockManager(wait_deadline_seconds=1)
    old = start(manager, 'old', 10)
    young = start(manager, 'young', 20)
    assert acquire(manager, 'young', young, 'k', 'X', 'young-x')['status'] == 'GRANTED'
    waiter = gevent.spawn(acquire, manager, 'old', old, 'k', 'S', 'old-s')
    gevent.sleep(0.01)
    assert not waiter.ready()
    assert manager.unlock('young', 0)['status'] == 'RELEASED'
    assert waiter.get(timeout=1)['status'] == 'GRANTED'


def test_abort_cancels_a_blocked_handler():
    manager = LockManager(wait_deadline_seconds=1)
    old = start(manager, 'old', 10)
    young = start(manager, 'young', 20)
    assert acquire(manager, 'young', young, 'k', 'X', 'young-x')['status'] == 'GRANTED'
    waiter = gevent.spawn(acquire, manager, 'old', old, 'k', 'S', 'old-s')
    gevent.sleep(0.01)
    assert manager.abort('old', 0, 'ERROR')['status'] == 'ABORTED'
    assert waiter.get(timeout=1)['status'] == 'ABORT'


def test_any_older_reader_makes_a_writer_die():
    manager = LockManager()
    old_one = start(manager, 'old-one', 10)
    old_two = start(manager, 'old-two', 20)
    young = start(manager, 'young', 30)
    assert acquire(manager, 'old-one', old_one, 'k', 'S', 'one')['status'] == 'GRANTED'
    assert acquire(manager, 'old-two', old_two, 'k', 'S', 'two')['status'] == 'GRANTED'
    assert acquire(manager, 'young', young, 'k', 'X', 'write')['abort_type'] == 'WAIT_DIE'


def test_old_terms_and_lock_retries_are_safe():
    manager = LockManager()
    birth = start(manager, 'tx', 10)
    first = acquire(manager, 'tx', birth, 'k', 'S', 'op')
    assert first['status'] == 'GRANTED'
    assert acquire(manager, 'tx', birth, 'k', 'S', 'op') == first
    assert manager.abort('tx', 0)['status'] == 'ABORTED'
    assert manager.begin('tx', 1, 10)['status'] == 'ACTIVE'
    assert manager.lock('tx', 0, birth, 'k', 'S', 'late')['status'] == 'STALE'


def test_global_request_id_not_begin_arrival_order_defines_age():
    manager = LockManager()
    # The later trace request reaches /begin first, but must remain younger.
    later = start(manager, 'later-arrival-first', 20)
    earlier = start(manager, 'earlier-arrival-second', 10)
    assert earlier < later
    assert acquire(manager, 'later-arrival-first', later, 'k', 'X', 'later')['status'] == 'GRANTED'
    waiter = gevent.spawn(acquire, manager, 'earlier-arrival-second', earlier, 'k', 'S', 'earlier')
    gevent.sleep(0.01)
    assert not waiter.ready()
    assert manager.unlock('later-arrival-first', 0)['status'] == 'RELEASED'
    assert waiter.get(timeout=1)['status'] == 'GRANTED'


def test_global_request_id_cannot_change_on_retry_or_be_reused():
    manager = LockManager()
    birth = start(manager, 'tx', 10)
    assert manager.abort('tx', 0)['status'] == 'ABORTED'
    assert manager.begin('tx', 1, 11)['status'] == 'PROTOCOL_ERROR'
    assert manager.begin('other', 0, birth)['status'] == 'PROTOCOL_ERROR'


def test_shadow_flush_retry_is_idempotent_and_commit_is_visible():
    db = FakeDB()
    shadow = ShadowStore(db)
    assert shadow.begin('tx', 0, 7)['status'] == 'ACTIVE'
    assert shadow.put('tx', 0, 'a', 'one', 'f1', 'p1')['status'] == 'STAGED'
    assert shadow.put('tx', 0, 'a', 'two', 'f1', 'p2')['status'] == 'STAGED'
    assert shadow.get('tx', 0, 'a') == {'status': 'HIT', 'value': 'two'}
    db.fail_once = True
    assert shadow.flush('tx', 0, 'flush')['status'] == 'RETRY'
    assert shadow.flush('tx', 0, 'flush')['status'] == 'FLUSHED'
    assert db.rows['a'] == ('two', '7:0')
    assert shadow.complete('tx', 0)['status'] == 'COMPLETED'
    assert shadow.begin('tx', 1, 7)['status'] == 'ACTIVE'
    assert shadow.put('tx', 0, 'late', 'no', 'f1', 'late')['status'] == 'STALE'


def test_discard_never_writes_the_main_database():
    db = FakeDB()
    shadow = ShadowStore(db)
    shadow.begin('tx', 0, 1)
    shadow.put('tx', 0, 'a', 'value', 'f', 'p')
    assert shadow.discard('tx', 0, 'WAIT_DIE')['status'] == 'DISCARDED'
    assert db.rows == {}
