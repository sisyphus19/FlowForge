"""Deliberately injected failures: killed workers, zombies, duplicate delivery, mid-transition crashes."""
import os
import signal
import subprocess
import sys
import time

import pytest

from tests.conftest import wait_for, wait_run, wf

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def spawn_worker(settings, concurrency=1):
    env = {**os.environ, "FLOWFORGE_REDIS_URL": settings.redis_url, "FLOWFORGE_KEY_PREFIX": settings.key_prefix,
           "FLOWFORGE_DATABASE_URL": "", "FLOWFORGE_LEASE_TTL_S": str(settings.lease_ttl_s),
           "FLOWFORGE_MAINTENANCE_INTERVAL_S": str(settings.maintenance_interval_s),
           "FLOWFORGE_PENDING_IDLE_MS": str(settings.pending_idle_ms), "FLOWFORGE_ENABLE_TEST_TASKS": "1",
           "FLOWFORGE_WORKER_CONCURRENCY": str(concurrency), "PYTHONPATH": ROOT}
    return subprocess.Popen([sys.executable, "-m", "app.workers.worker"], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def executions(store):
    return store.r.lrange(f"{store.keys.p}test:executions", 0, -1)


@pytest.mark.slow
def test_sigkill_worker_mid_task_is_recovered(orch, store, settings, start_worker):
    d = wf(tasks=[{"id": "a", "type": "count_exec", "config": {"seconds": 2},
                   "retries": {"max_attempts": 3}}])
    proc = spawn_worker(settings)
    try:
        rid = orch.trigger(d).run_id
        tid = f"{rid}:a"
        wait_for(lambda: orch.task_hash(tid).get("state") == "running", msg="task running in subprocess")
        killed_at = time.time()
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        start_worker()  # a healthy worker; recovery must find the abandoned task by itself
        snap = wait_run(orch, rid, timeout=20)
        recovery_s = time.time() - killed_at
    finally:
        proc.kill()
    assert snap["state"] == "succeeded"
    assert snap["tasks"]["a"]["attempt"] == 2 and snap["tasks"]["a"]["failures"] == 1
    assert len(executions(store)) == 2               # at-least-once: it really ran twice...
    assert snap["tasks"]["a"]["result"] == {"attempt": 2}  # ...but exactly one result was committed
    assert recovery_s < 12
    kinds = [e["type"] for e in orch.history.get_events(rid)]
    assert "lease_expired" in kinds


@pytest.mark.slow
def test_batch_survives_killing_one_of_two_workers(orch, store, settings, start_worker):
    d = wf(tasks=[{"id": f"t{i}", "type": "count_exec", "config": {"seconds": 0.3}} for i in range(12)])
    procs = [spawn_worker(settings), spawn_worker(settings)]
    try:
        rid = orch.trigger(d).run_id
        wait_for(lambda: len(executions(store)) >= 2, msg="work started")
        procs[0].send_signal(signal.SIGKILL)
        start_worker()
        snap = wait_run(orch, rid, timeout=30)
    finally:
        for p in procs:
            p.kill()
    assert snap["state"] == "succeeded"
    assert all(t["state"] == "succeeded" for t in snap["tasks"].values())  # nothing silently lost
    done = {e.split("#")[0] for e in executions(store)}
    assert len(done) == 12


def test_zombie_worker_is_fenced_end_to_end(orch, store, start_worker):
    d = wf(tasks=[{"id": "a", "type": "echo", "config": {"value": "real"}}])
    rid = orch.trigger(d).run_id
    tid = f"{rid}:a"
    msg = store.queue.read("zombie-consumer")[0]
    f_old = store.leases.claim(tid, "zombie", msg.attempt, ttl_s=0.2)
    time.sleep(0.4)                                     # zombie stalls (GC pause / network partition)
    start_worker()                                      # recovery requeues; a live worker finishes the task
    snap = wait_run(orch, rid)
    assert snap["tasks"]["a"]["result"]["value"] == "real" and snap["tasks"]["a"]["attempt"] == 2
    assert orch.task_succeeded(tid, f_old, {"value": "ZOMBIE"}) is False   # late write rejected
    assert orch.task_failed(tid, f_old, "late failure") is False
    assert orch.get_result(tid)["value"] == "real"
    assert "stale_completion_rejected" in [e["type"] for e in orch.history.get_events(rid)]


def test_duplicate_dispatch_messages_execute_once(orch, store, start_worker):
    d = wf(tasks=[{"id": "a", "type": "count_exec"}])
    rid = orch.trigger(d).run_id
    for _ in range(3):  # inject duplicate deliveries of the same (task, attempt)
        store.r.xadd(store.keys.queue_ready, {"task_id": f"{rid}:a", "run_id": rid, "attempt": 1})
    start_worker(concurrency=3)
    wait_run(orch, rid)
    time.sleep(0.5)
    assert len(executions(store)) == 1
    assert store.queue.depth()["ready"] == 0


def test_consumer_dies_between_delivery_and_claim(orch, store, start_worker):
    d = wf(tasks=[{"id": "a", "type": "count_exec"}])
    rid = orch.trigger(d).run_id
    assert len(store.queue.read("doomed")) == 1         # delivered to a consumer that then vanishes
    start_worker()                                      # XAUTOCLAIM hands the stuck message to a live consumer
    snap = wait_run(orch, rid, timeout=15)
    assert snap["state"] == "succeeded" and snap["tasks"]["a"]["attempt"] == 1  # no wasted attempt


def test_crash_between_task_done_and_advance_is_reconciled(orch, store, settings):
    """Process dies after Redis records task success but before dependents are released."""
    d = wf(tasks=[{"id": "a", "type": "noop"}, {"id": "b", "type": "noop", "depends_on": ["a"]}])
    rid = orch.trigger(d).run_id
    tid = f"{rid}:a"
    msg = store.queue.read("c")[0]
    fence = store.leases.claim(tid, "w", msg.attempt, 5)
    assert store.leases.complete(tid, fence, "{}") == 1   # ...and the process "dies" here: no advance()
    assert orch.task_hash(f"{rid}:b")["state"] == "pending"
    from app.workers.recovery import maintenance_once
    maintenance_once(orch)
    assert orch.task_hash(f"{rid}:b")["state"] == "queued"


def test_worker_loss_exhausting_attempts_dead_letters(orch, store, alerts, start_worker):
    d = wf(tasks=[{"id": "a", "type": "noop", "retries": {"max_attempts": 1}}])
    rid = orch.trigger(d).run_id
    tid = f"{rid}:a"
    msg = store.queue.read("c")[0]
    store.leases.claim(tid, "w", msg.attempt, ttl_s=0.2)
    time.sleep(0.4)
    start_worker()
    snap = wait_run(orch, rid)
    assert snap["state"] == "failed" and snap["tasks"]["a"]["last_error"].startswith("lease expired")
    assert len(alerts.sent) == 1


def test_concurrent_recovery_passes_do_not_double_requeue(orch, store):
    import threading
    d = wf(tasks=[{"id": f"t{i}", "type": "noop"} for i in range(10)])
    rid = orch.trigger(d).run_id
    for m in store.queue.read("c", count=10):
        store.leases.claim(m.task_id, "w", m.attempt, ttl_s=0.1)
    time.sleep(0.3)
    results = []
    ts = [threading.Thread(target=lambda: results.append(len(store.leases.requeue_expired()))) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(results) == 10                              # each abandoned task recovered exactly once
    assert store.r.xlen(store.keys.queue_ready) >= 10
    attempts = {orch.task_hash(f"{rid}:t{i}")["attempt"] for i in range(10)}
    assert attempts == {"2"}
