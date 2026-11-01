"""Worker process: claim -> execute (with lease renewal) -> complete/fail -> ack."""
from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
import uuid

from app.config import Settings
from app.engine.orchestrator import Orchestrator, split_task_id
from app.kv.redis_store import RedisStore
from app.kv.task_queue import Message
from app.observability import metrics
from app.tasks import registry
from app.tasks.registry import NonRetryableError, TaskContext
from app.workers.executor import run_handler
from app.workers.recovery import maintenance_once

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, orch: Orchestrator, settings: Settings | None = None, worker_id: str | None = None,
                 concurrency: int | None = None) -> None:
        self.orch, self.store = orch, orch.store
        self.settings = settings or orch.settings
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency or self.settings.worker_concurrency
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.services = {"history": orch.history, "settings": self.settings, "store": self.store}

    # ---------------------------------------------------------------- one message
    def process_message(self, msg: Message) -> str:
        """Returns: stale | succeeded | retry | failed | lost | cancelled."""
        lease_ttl = self.settings.lease_ttl_s
        fence = self.store.leases.claim(msg.task_id, self.worker_id, msg.attempt, lease_ttl)
        if fence is None:  # duplicate delivery, cancelled, or superseded by recovery
            self.store.queue.ack(msg.id)
            metrics.TASK_EVENTS.labels("stale_message_dropped").inc()
            return "stale"
        run_id, key = split_task_id(msg.task_id)
        stop, lost, cancel = threading.Event(), threading.Event(), threading.Event()
        renewer = threading.Thread(target=self._renew_loop, args=(msg.task_id, fence, stop, lost, cancel),
                                   daemon=True)
        renewer.start()
        outcome = "failed"
        try:
            self.orch.task_started(msg.task_id)
            defn = self.orch.load_def(run_id)
            if defn is None:
                raise NonRetryableError("run definition expired")
            task = defn.task(key)
            ctx = self._context(msg, run_id, key, task, defn, cancel)
            started = time.monotonic()
            try:
                result = run_handler(registry.get_handler(task.type), ctx, task.timeout_seconds)
            finally:
                metrics.TASK_DURATION.labels(task.type).observe(time.monotonic() - started)
            ok = self.orch.task_succeeded(msg.task_id, fence, result)
            outcome = "succeeded" if ok else "lost"
        except NonRetryableError as e:
            outcome = "failed" if self.orch.task_failed(msg.task_id, fence, str(e), retryable=False) else "lost"
        except Exception as e:  # noqa: BLE001 - any handler error is a (retryable) task failure
            log.warning("task failed", extra={"ctx": {"task_id": msg.task_id, "error": repr(e)}})
            outcome = "retry" if self.orch.task_failed(msg.task_id, fence, f"{type(e).__name__}: {e}") else "lost"
        finally:
            stop.set()
        self.store.queue.ack(msg.id)
        return outcome

    def _context(self, msg, run_id, key, task, defn, cancel) -> TaskContext:
        inputs = {d: self.orch.get_result(f"{run_id}:{d}") for d in task.depends_on}
        import json
        params = json.loads(self.store.r.hget(self.store.keys.run(run_id), "params") or "{}")
        return TaskContext(run_id=run_id, task_id=msg.task_id, task_key=key, attempt=msg.attempt,
                           config=task.config, inputs=inputs, params=params, services=self.services,
                           _cancelled=cancel.is_set)

    def _renew_loop(self, task_id, fence, stop: threading.Event, lost, cancel) -> None:
        interval = max(self.settings.lease_ttl_s / 3, 0.05)
        while not stop.wait(interval):
            try:
                res = self.store.leases.renew(task_id, self.worker_id, fence, self.settings.lease_ttl_s)
            except Exception:  # Redis blip: keep trying; the lease only dies if we stay silent for a full TTL
                log.exception("lease renew failed")
                continue
            if res == 0:
                lost.set()
                return
            if res == 2:
                cancel.set()

    # ---------------------------------------------------------------- loops
    def _consume_loop(self, idx: int) -> None:
        consumer = f"{self.worker_id}-{idx}"
        last_claim = 0.0
        while not self._stop.is_set():
            try:
                msgs: list[Message] = []
                if time.monotonic() - last_claim >= self.settings.maintenance_interval_s:
                    last_claim = time.monotonic()
                    msgs += self.store.queue.autoclaim(consumer, self.settings.pending_idle_ms or 0)
                msgs += self.store.queue.read(consumer, count=1, block_ms=500)
                for m in msgs:
                    if self._stop.is_set():
                        break
                    self.process_message(m)
            except Exception:
                log.exception("consumer loop error")
                time.sleep(0.5)

    def _maintenance_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.store.heartbeat(self.worker_id, self.settings.maintenance_interval_s * 3 + 5)
                maintenance_once(self.orch)
            except Exception:
                log.exception("maintenance error")
            self._stop.wait(self.settings.maintenance_interval_s)

    def start(self) -> "Worker":
        self._stop.clear()
        self._threads = [threading.Thread(target=self._maintenance_loop, daemon=True, name="maintenance")]
        self._threads += [threading.Thread(target=self._consume_loop, args=(i,), daemon=True, name=f"consumer-{i}")
                          for i in range(self.concurrency)]
        for t in self._threads:
            t.start()
        return self

    def stop(self, timeout: float = 10) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout)
        self.store.r.delete(self.store.keys.worker_hb(self.worker_id))

    def run_forever(self) -> None:
        self.start()
        done = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: done.set())
        done.wait()
        self.stop()


def build_orchestrator(settings: Settings):
    from app.db.repository import HistoryStore, NullHistory
    store = RedisStore(url=settings.redis_url, prefix=settings.key_prefix)
    history = HistoryStore(settings.database_url) if settings.database_url else NullHistory()
    history.init_schema()
    if settings.enable_test_tasks:
        from app.tasks.testing import register_test_tasks
        register_test_tasks()
    return Orchestrator(store, history, settings)


def main() -> None:
    from app.logging_config import configure_logging
    configure_logging(os.getenv("FLOWFORGE_LOG_LEVEL", "INFO"))
    settings = Settings.from_env()
    if settings.metrics_port:
        from prometheus_client import start_http_server
        start_http_server(settings.metrics_port)
    Worker(build_orchestrator(settings), settings).run_forever()


if __name__ == "__main__":
    main()
