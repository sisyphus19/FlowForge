"""Task ownership: claim, renew (heartbeat), complete/fail with fencing, and abandoned-task recovery."""
from __future__ import annotations

from dataclasses import dataclass

import redis

from . import scripts
from .keys import Keys


@dataclass(frozen=True)
class RecoveredTask:
    task_id: str
    outcome: str  # requeued | failed | cancelled
    run_id: str


class LeaseManager:
    def __init__(self, r: redis.Redis, keys: Keys) -> None:
        self.r, self.k = r, keys
        reg = r.register_script
        self._claim, self._renew = reg(scripts.CLAIM), reg(scripts.RENEW)
        self._complete, self._fail = reg(scripts.COMPLETE), reg(scripts.FAIL)
        self._requeue = reg(scripts.REQUEUE_EXPIRED)
        self._cancel, self._transition = reg(scripts.CANCEL_TASK), reg(scripts.TRANSITION)
        self._replay = reg(scripts.REPLAY)

    def claim(self, task_id: str, worker_id: str, attempt: int, ttl_s: float) -> int | None:
        """Returns a fencing token, or None if the message is stale/duplicate (state or attempt mismatch)."""
        res = self._claim(keys=[self.k.task(task_id), self.k.lease(task_id), self.k.fence(task_id), self.k.running],
                          args=[task_id, worker_id, attempt, int(ttl_s * 1000)])
        return int(res[1]) if res[0] == 1 else None

    def renew(self, task_id: str, worker_id: str, fence: int, ttl_s: float) -> int:
        """0 = ownership lost, 1 = renewed, 2 = renewed but cancellation was requested."""
        return int(self._renew(keys=[self.k.task(task_id), self.k.lease(task_id)],
                               args=[worker_id, fence, int(ttl_s * 1000)]))

    def complete(self, task_id: str, fence: int, result_json: str) -> int:
        """0 = rejected (stale owner), 1 = succeeded, 2 = recorded as cancelled."""
        return int(self._complete(keys=[self.k.task(task_id), self.k.lease(task_id), self.k.result(task_id),
                                        self.k.running], args=[fence, result_json, task_id]))

    def fail(self, task_id: str, run_id: str, fence: int, error: str, retryable: bool, delay_ms: int) -> str | None:
        """Returns 'retry_wait' | 'failed' | 'cancelled', or None if the caller no longer owns the task."""
        res = self._fail(keys=[self.k.task(task_id), self.k.lease(task_id), self.k.running, self.k.retry,
                               self.k.queue_dead],
                         args=[fence, error[:2000], task_id, "1" if retryable else "0", delay_ms, run_id])
        return res[1] if res[0] == 1 else None

    def requeue_expired(self, limit: int = 100) -> list[RecoveredTask]:
        flat = self._requeue(keys=[self.k.running, self.k.queue_ready, self.k.queue_dead],
                             args=[limit, self.k.task_prefix, self.k.lease_suffix, self.k.state_suffix])
        return [RecoveredTask(*flat[i:i + 3]) for i in range(0, len(flat), 3)]

    def cancel_task(self, task_id: str) -> str:
        return self._cancel(keys=[self.k.task(task_id), self.k.retry], args=[task_id])

    def transition(self, task_id: str, from_state: str, to_state: str) -> bool:
        return bool(self._transition(keys=[self.k.task(task_id)], args=[from_state, to_state]))

    def replay(self, task_id: str) -> bool:
        return bool(self._replay(keys=[self.k.task(task_id)]))
