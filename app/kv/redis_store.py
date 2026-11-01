"""Facade bundling the Redis coordination primitives."""
from __future__ import annotations

import redis

from . import scripts
from .idempotency import IdempotencyStore
from .keys import Keys
from .leases import LeaseManager
from .task_queue import TaskQueue


class RedisStore:
    def __init__(self, client: redis.Redis | None = None, *, url: str = "redis://localhost:6379/0",
                 prefix: str = "") -> None:
        self.r = client or redis.Redis.from_url(url, decode_responses=True)
        self.keys = Keys(prefix)
        self.queue = TaskQueue(self.r, self.keys)
        self.leases = LeaseManager(self.r, self.keys)
        self.idem = IdempotencyStore(self.r, self.keys)
        self.queue.ensure_group()
        self._finalize = self.r.register_script(scripts.RUN_FINALIZE)
        self._reopen = self.r.register_script(scripts.RUN_REOPEN)
        self._slot = self.r.register_script(scripts.ACQUIRE_SLOT)

    def ping(self) -> bool:
        return bool(self.r.ping())

    def acquire_slot(self, flow: str, run_id: str, max_runs: int, enforce: bool) -> bool:
        return bool(self._slot(keys=[self.keys.flow_active(flow)], args=[run_id, max_runs, "1" if enforce else "0"]))

    def release_slot(self, flow: str, run_id: str) -> None:
        self.r.srem(self.keys.flow_active(flow), run_id)

    def finalize_run(self, run_id: str, flow: str, final_state: str) -> bool:
        return bool(self._finalize(keys=[self.keys.run(run_id), self.keys.active_runs, self.keys.flow_active(flow)],
                                   args=[final_state, run_id]))

    def reopen_run(self, run_id: str, flow: str) -> bool:
        return bool(self._reopen(keys=[self.keys.run(run_id), self.keys.active_runs, self.keys.flow_active(flow)],
                                 args=[run_id]))

    def heartbeat(self, worker_id: str, ttl_s: float, info: str = "") -> None:
        self.r.set(self.keys.worker_hb(worker_id), info or "alive", px=int(ttl_s * 1000))

    def live_workers(self) -> list[str]:
        pre, suf = f"{self.keys.p}worker:", ":heartbeat"
        return [k[len(pre):-len(suf)] for k in self.r.scan_iter(self.keys.worker_hb_pattern)]

    def close(self) -> None:
        self.r.close()
