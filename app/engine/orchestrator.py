"""Run lifecycle: create runs, release ready tasks, finish runs, handle failures/recovery/replay/cancel.

`advance(run_id)` is the heart of the engine. It is idempotent and safe to call concurrently from any
process: every state change it makes goes through an atomic Lua transition that only succeeds from the
expected prior state.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass

from app.config import Settings
from app.db.repository import NullHistory
from app.kv.redis_store import RedisStore
from app.observability import metrics
from app.tasks.notifications import AlertSink, LoggingAlertSink

from .dependency_resolver import topological_order
from .retry import compute_delay
from .state_machine import RUN_TERMINAL_VALUES, TASK_TERMINAL_VALUES, UPSTREAM_BAD
from .workflow import WorkflowDef

log = logging.getLogger(__name__)
MAX_RESULT_BYTES = 512 * 1024


@dataclass
class TriggerResult:
    run_id: str | None
    created: bool        # False when an idempotency key matched an existing run
    skipped: bool = False  # True when the concurrency policy rejected the run


def make_task_id(run_id: str, task_key: str) -> str:
    return f"{run_id}:{task_key}"


def split_task_id(task_id: str) -> tuple[str, str]:
    run_id, key = task_id.split(":", 1)
    return run_id, key


class Orchestrator:
    def __init__(self, store: RedisStore, history=None, settings: Settings | None = None,
                 alerts: AlertSink | None = None) -> None:
        self.store = store
        self.r = store.r
        self.k = store.keys
        self.history = history or NullHistory()
        self.settings = settings or Settings()
        self.alerts = alerts or LoggingAlertSink()
        self._def_cache: dict[str, WorkflowDef] = {}

    # ------------------------------------------------------------------ run creation
    def trigger(self, defn: WorkflowDef, *, trigger: str = "api", idempotency_key: str | None = None,
                params: dict | None = None) -> TriggerResult:
        run_id = uuid.uuid4().hex[:16]
        if idempotency_key:
            existing = self.store.idem.reserve(idempotency_key, run_id, self.settings.idempotency_ttl_s)
            if existing:
                return TriggerResult(existing, created=False)
        pol = defn.concurrency
        if not self.store.acquire_slot(defn.name, run_id, pol.max_runs, enforce=pol.on_overlap == "skip"):
            if idempotency_key:  # a skipped trigger must not poison the key for a later retry
                self.store.idem.release(idempotency_key, run_id)
            metrics.RUN_EVENTS.labels("skipped_overlap").inc()
            return TriggerResult(None, created=False, skipped=True)
        try:
            self._create_run(run_id, defn, trigger, idempotency_key, params or {})
        except Exception:
            self.store.release_slot(defn.name, run_id)
            self.r.srem(self.k.active_runs, run_id)
            if idempotency_key:
                self.store.idem.release(idempotency_key, run_id)
            raise
        self._event(run_id, None, "run_created", f"workflow={defn.name} trigger={trigger}")
        metrics.RUN_EVENTS.labels("created").inc()
        self.advance(run_id)
        return TriggerResult(run_id, created=True)

    def _create_run(self, run_id: str, defn: WorkflowDef, trigger: str, idem: str | None, params: dict) -> None:
        now_ms = int(time.time() * 1000)
        pipe = self.r.pipeline(transaction=True)
        pipe.hset(self.k.run(run_id), mapping={
            "state": "running", "workflow": defn.name, "trigger": trigger, "created_ms": now_ms,
            "cancel_requested": 0, "params": json.dumps(params), "idempotency_key": idem or ""})
        pipe.set(self.k.run_def(run_id), defn.model_dump_json())
        for t in defn.tasks:
            pipe.hset(self.k.task(make_task_id(run_id, t.id)), mapping={
                "state": "pending", "attempt": 0, "failures": 0, "max_attempts": t.retries.max_attempts,
                "run_id": run_id, "task_key": t.id, "type": t.type})
        pipe.sadd(self.k.active_runs, run_id)
        pipe.execute()
        self.history.upsert_run({"id": run_id, "workflow": defn.name, "state": "running", "trigger": trigger,
                                 "idempotency_key": idem, "params": params, "created_ms": now_ms})
        for t in defn.tasks:
            self.persist_task(make_task_id(run_id, t.id))

    # ------------------------------------------------------------------ definitions / snapshots
    def load_def(self, run_id: str) -> WorkflowDef | None:
        if run_id in self._def_cache:
            return self._def_cache[run_id]
        raw = self.r.get(self.k.run_def(run_id))
        if raw is None:
            return None
        defn = WorkflowDef.model_validate_json(raw)
        if len(self._def_cache) > 256:
            self._def_cache.clear()
        self._def_cache[run_id] = defn
        return defn

    def task_hash(self, task_id: str) -> dict:
        return self.r.hgetall(self.k.task(task_id))

    def get_result(self, task_id: str):
        raw = self.r.get(self.k.result(task_id))
        return json.loads(raw) if raw is not None else None

    def snapshot(self, run_id: str) -> dict | None:
        run = self.r.hgetall(self.k.run(run_id))
        defn = self.load_def(run_id)
        if not run or defn is None:
            return None
        pipe = self.r.pipeline()
        for t in defn.tasks:
            pipe.hgetall(self.k.task(make_task_id(run_id, t.id)))
            pipe.get(self.k.result(make_task_id(run_id, t.id)))
        flat = pipe.execute()
        tasks = {}
        for i, t in enumerate(defn.tasks):
            h, res = flat[2 * i], flat[2 * i + 1]
            tasks[t.id] = {"state": h.get("state"), "attempt": int(h.get("attempt", 0)),
                           "failures": int(h.get("failures", 0)), "last_error": h.get("last_error"),
                           "worker": h.get("worker"), "depends_on": t.depends_on,
                           "result": json.loads(res) if res is not None else None}
        ms = lambda v: int(v) if v else None  # noqa: E731
        return {"id": run_id, "workflow": run["workflow"], "state": run["state"], "trigger": run.get("trigger"),
                "created_ms": ms(run.get("created_ms")), "finished_ms": ms(run.get("finished_ms")),
                "cancel_requested": run.get("cancel_requested") == "1", "tasks": tasks, "source": "redis"}

    def persist_task(self, task_id: str) -> None:
        if not self.history.enabled:
            return
        h = self.task_hash(task_id)
        if not h:
            return
        res = self.r.get(self.k.result(task_id))
        self.history.upsert_task({
            "id": task_id, "run_id": h["run_id"], "task_key": h["task_key"], "type": h["type"],
            "state": h["state"], "attempt": int(h.get("attempt", 0)), "failures": int(h.get("failures", 0)),
            "last_error": h.get("last_error"), "worker": h.get("worker"),
            "result": json.loads(res) if res is not None else None,
            "started_ms": h.get("started_ms"), "finished_ms": h.get("finished_ms")})

    def _event(self, run_id: str, task_id: str | None, type_: str, detail: str = "") -> None:
        self.history.add_event(run_id, task_id, type_, detail)

    # ------------------------------------------------------------------ the scheduler core
    def advance(self, run_id: str) -> None:
        run = self.r.hgetall(self.k.run(run_id))
        if not run:
            self.r.srem(self.k.active_runs, run_id)  # expired / flushed run: nothing to do
            return
        if run.get("state") in RUN_TERMINAL_VALUES:
            return
        defn = self.load_def(run_id)
        if defn is None:
            return
        cancelling = run.get("cancel_requested") == "1"
        by_id = {t.id: t for t in defn.tasks}
        pipe = self.r.pipeline()
        for t in defn.tasks:
            pipe.hget(self.k.task(make_task_id(run_id, t.id)), "state")
        states = dict(zip((t.id for t in defn.tasks), pipe.execute()))

        for key in topological_order(defn.tasks):  # upstream first, so skips propagate in one pass
            tid, st = make_task_id(run_id, key), states[key]
            if cancelling:
                if st in ("pending", "queued", "retry_wait", "running"):
                    # running tasks only get a cancel flag (cooperative); the worker sees it on its next renewal
                    states[key] = self.store.leases.cancel_task(tid)
                    if states[key] != st:
                        self.persist_task(tid)
                continue
            if st != "pending":
                continue
            deps = [states[d] for d in by_id[key].depends_on]
            if any(s in UPSTREAM_BAD for s in deps):
                if self.store.leases.transition(tid, "pending", "skipped"):
                    states[key] = "skipped"
                    self._event(run_id, tid, "task_skipped", "upstream did not succeed")
                    self.persist_task(tid)
            elif all(s == "succeeded" for s in deps):
                if self.store.queue.enqueue(tid, run_id):
                    states[key] = "queued"

        if all(s in TASK_TERMINAL_VALUES for s in states.values()):
            if cancelling:
                final = "cancelled"
            elif any(s == "failed" for s in states.values()):
                final = "failed"
            else:
                final = "succeeded"
            self._finish_run(run_id, defn, final)

    def _finish_run(self, run_id: str, defn: WorkflowDef, final: str) -> None:
        if not self.store.finalize_run(run_id, defn.name, final):
            return  # another process already finalized it
        ttl = self.settings.run_retention_s
        pipe = self.r.pipeline()
        for t in defn.tasks:
            tid = make_task_id(run_id, t.id)
            for key in (self.k.task(tid), self.k.result(tid), self.k.fence(tid), self.k.lease(tid)):
                pipe.expire(key, ttl)
        pipe.expire(self.k.run(run_id), ttl)
        pipe.expire(self.k.run_def(run_id), ttl)
        pipe.execute()
        snap = self.snapshot(run_id)
        if snap:
            self.history.upsert_run({**snap, "params": {}})  # authoritative final write
            for t in defn.tasks:
                self.persist_task(make_task_id(run_id, t.id))
        self._event(run_id, None, f"run_{final}")
        metrics.RUN_EVENTS.labels(final).inc()
        self._def_cache.pop(run_id, None)

    # ------------------------------------------------------------------ worker callbacks
    def task_started(self, task_id: str) -> None:
        run_id, _ = split_task_id(task_id)
        self.persist_task(task_id)
        self._event(run_id, task_id, "task_started")

    def task_succeeded(self, task_id: str, fence: int, result) -> bool:
        run_id, _ = split_task_id(task_id)
        try:
            payload = json.dumps(result)
        except (TypeError, ValueError) as e:
            return self.task_failed(task_id, fence, f"result not JSON-serializable: {e}", retryable=False)
        if len(payload) > MAX_RESULT_BYTES:
            return self.task_failed(task_id, fence, "result exceeds 512KB; store large data externally",
                                    retryable=False)
        code = self.store.leases.complete(task_id, fence, payload)
        if code == 0:
            metrics.TASK_EVENTS.labels("stale_completion_rejected").inc()
            self._event(run_id, task_id, "stale_completion_rejected", f"fence={fence}")
            return False
        metrics.TASK_EVENTS.labels("succeeded" if code == 1 else "cancelled").inc()
        self.persist_task(task_id)
        self._event(run_id, task_id, "task_succeeded" if code == 1 else "task_cancelled")
        self.advance(run_id)
        return True

    def task_failed(self, task_id: str, fence: int, error: str, retryable: bool = True) -> bool:
        run_id, key = split_task_id(task_id)
        defn = self.load_def(run_id)
        failures = int(self.r.hget(self.k.task(task_id), "failures") or 0) + 1
        delay_ms = 0
        if defn is not None:
            delay_ms = int(compute_delay(defn.task(key).retries, failures) * 1000)
        outcome = self.store.leases.fail(task_id, run_id, fence, error, retryable, delay_ms)
        if outcome is None:
            metrics.TASK_EVENTS.labels("stale_completion_rejected").inc()
            self._event(run_id, task_id, "stale_failure_rejected", f"fence={fence}")
            return False
        self.persist_task(task_id)
        if outcome == "retry_wait":
            metrics.TASK_EVENTS.labels("retry_scheduled").inc()
            self._event(run_id, task_id, "task_retry_scheduled", f"in {delay_ms}ms: {error}")
        elif outcome == "failed":
            self._dead_lettered(run_id, task_id, error)
        else:
            metrics.TASK_EVENTS.labels("cancelled").inc()
        self.advance(run_id)
        return True

    def _dead_lettered(self, run_id: str, task_id: str, error: str) -> None:
        metrics.TASK_EVENTS.labels("dead_lettered").inc()
        self._event(run_id, task_id, "task_dead_lettered", error)
        self.alerts.send(f"Task {task_id} dead-lettered", error, run_id=run_id, task_id=task_id)

    # ------------------------------------------------------------------ maintenance
    def promote_retries(self) -> int:
        return self.store.queue.promote_due_retries()

    def recover_abandoned(self) -> list:
        recovered = self.store.leases.requeue_expired()
        for rec in recovered:
            metrics.TASK_EVENTS.labels(f"lease_expired_{rec.outcome}").inc()
            self._event(rec.run_id, rec.task_id, "lease_expired", rec.outcome)
            if rec.outcome == "failed":
                self._dead_lettered(rec.run_id, rec.task_id, "lease expired (worker lost)")
            self.persist_task(rec.task_id)
            self.advance(rec.run_id)
        return recovered

    def reconcile(self) -> int:
        """Re-evaluate every unfinished run. Heals crashes between 'task done' and 'advance'."""
        run_ids = list(self.r.smembers(self.k.active_runs))
        for rid in run_ids:
            self.advance(rid)
        return len(run_ids)

    # ------------------------------------------------------------------ operator actions
    def cancel_run(self, run_id: str) -> bool:
        run = self.r.hgetall(self.k.run(run_id))
        if not run or run["state"] in RUN_TERMINAL_VALUES:
            return False
        self.r.hset(self.k.run(run_id), "cancel_requested", 1)
        self._event(run_id, None, "run_cancel_requested")
        self.advance(run_id)
        return True

    def replay_task(self, run_id: str, task_key: str) -> bool:
        """Dead-letter replay: reset a failed task (fresh retry budget) and let downstream tasks resume."""
        defn = self.load_def(run_id)
        if defn is None:
            raise KeyError(run_id)
        defn.task(task_key)  # KeyError if unknown
        tid = make_task_id(run_id, task_key)
        run_state = self.r.hget(self.k.run(run_id), "state")
        if run_state in ("cancelled", "succeeded"):
            raise ValueError(f"cannot replay tasks of a {run_state} run")
        if not self.store.leases.replay(tid):
            return False
        self.store.reopen_run(run_id, defn.name)
        for t in defn.tasks:  # downstream tasks skipped because of this failure may now proceed
            self.store.leases.transition(make_task_id(run_id, t.id), "skipped", "pending")
        self.persist_task(tid)
        self._event(run_id, tid, "task_replayed")
        metrics.TASK_EVENTS.labels("replayed").inc()
        self.advance(run_id)
        return True
