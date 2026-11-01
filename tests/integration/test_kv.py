"""Redis coordination primitives against a real redis-server."""
import threading
import time

from tests.conftest import wait_for


def seed(store, tid="r1:a", run="r1", state="pending", max_attempts=3):
    store.r.hset(store.keys.task(tid), mapping={"state": state, "attempt": 0, "failures": 0,
                                                "max_attempts": max_attempts, "run_id": run, "task_key": "a",
                                                "type": "noop"})
    return tid


def test_enqueue_is_idempotent(store):
    tid = seed(store)
    assert store.queue.enqueue(tid, "r1") is True
    assert store.queue.enqueue(tid, "r1") is False  # already queued -> no duplicate message
    assert store.r.xlen(store.keys.queue_ready) == 1


def test_only_one_of_many_workers_claims(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    wins, lock = [], threading.Lock()

    def attempt(i):
        f = store.leases.claim(tid, f"w{i}", 1, 5)
        if f is not None:
            with lock:
                wins.append(i)
    ts = [threading.Thread(target=attempt, args=(i,)) for i in range(50)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(wins) == 1


def test_stale_attempt_cannot_claim(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    assert store.leases.claim(tid, "w", attempt=99, ttl_s=5) is None


def test_complete_requires_current_fence(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    fence = store.leases.claim(tid, "w1", 1, 5)
    assert store.leases.complete(tid, fence + 1, '{"x":1}') == 0
    assert store.leases.complete(tid, fence, '{"x":1}') == 1
    assert store.leases.complete(tid, fence, '{"x":2}') == 0  # double completion rejected
    assert store.r.get(store.keys.result(tid)) == '{"x":1}'


def test_lease_expiry_requeues_and_old_owner_is_fenced(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    f1 = store.leases.claim(tid, "w1", 1, ttl_s=0.2)
    time.sleep(0.4)  # w1 stalls past its lease
    rec = store.leases.requeue_expired()
    assert [(r.task_id, r.outcome) for r in rec] == [(tid, "requeued")]
    assert store.leases.renew(tid, "w1", f1, 5) == 0           # zombie cannot renew
    msgs = store.queue.read("c", count=10, block_ms=100)
    newest = max(msgs, key=lambda m: m.attempt)
    assert newest.attempt == 2
    f2 = store.leases.claim(tid, "w2", 2, 5)
    assert f2 > f1
    assert store.leases.complete(tid, f1, '"zombie"') == 0     # fenced out
    assert store.leases.complete(tid, f2, '"good"') == 1
    assert store.r.get(store.keys.result(tid)) == '"good"'


def test_slow_worker_finishing_before_recovery_still_completes(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    f = store.leases.claim(tid, "w1", 1, ttl_s=0.1)
    time.sleep(0.25)  # lease key expired, but nobody has taken over yet
    assert store.leases.complete(tid, f, "1") == 1


def test_renew_extends_lease(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    f = store.leases.claim(tid, "w1", 1, ttl_s=0.3)
    for _ in range(4):
        time.sleep(0.15)
        assert store.leases.renew(tid, "w1", f, 0.3) == 1
    assert store.leases.requeue_expired() == []


def test_retry_then_dead_letter(store):
    tid = seed(store, max_attempts=2)
    store.queue.enqueue(tid, "r1")
    f = store.leases.claim(tid, "w", 1, 5)
    assert store.leases.fail(tid, "r1", f, "boom", True, delay_ms=0) == "retry_wait"
    assert store.queue.promote_due_retries() == 1
    assert store.queue.promote_due_retries() == 0                # promoted exactly once
    f = store.leases.claim(tid, "w", 2, 5)
    assert store.leases.fail(tid, "r1", f, "boom2", True, 0) == "failed"
    assert store.queue.dead_letters()[0]["task_id"] == tid
    assert store.r.hget(store.keys.task(tid), "state") == "failed"


def test_retry_not_promoted_before_due(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    f = store.leases.claim(tid, "w", 1, 5)
    store.leases.fail(tid, "r1", f, "x", True, delay_ms=400)
    assert store.queue.promote_due_retries() == 0
    wait_for(lambda: store.queue.promote_due_retries() == 1, timeout=2, msg="retry due")


def test_non_retryable_dead_letters_immediately(store):
    tid = seed(store, max_attempts=5)
    store.queue.enqueue(tid, "r1")
    f = store.leases.claim(tid, "w", 1, 5)
    assert store.leases.fail(tid, "r1", f, "fatal", False, 0) == "failed"


def test_unacked_message_is_autoclaimed(store):
    tid = seed(store)
    store.queue.enqueue(tid, "r1")
    got = store.queue.read("dead-consumer")           # consumer reads, then "crashes" before claiming
    assert len(got) == 1
    assert store.queue.autoclaim("rescuer", min_idle_ms=10_000) == []
    time.sleep(0.25)
    rescued = store.queue.autoclaim("rescuer", min_idle_ms=200)
    assert [m.task_id for m in rescued] == [tid]


def test_idempotency_reserve(store):
    assert store.idem.reserve("k", "run1", 60) is None
    assert store.idem.reserve("k", "run2", 60) == "run1"
    store.idem.release("k", "run1")
    assert store.idem.reserve("k", "run3", 60) is None


def test_concurrency_slot(store):
    assert store.acquire_slot("f", "r1", 1, True)
    assert not store.acquire_slot("f", "r2", 1, True)
    assert store.acquire_slot("f", "r3", 1, False)  # 'allow' policy ignores the cap
    store.release_slot("f", "r1")
