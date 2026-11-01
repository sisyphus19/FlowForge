"""Dispatch queue on a Redis Stream + consumer group, plus retry scheduling and dead letters."""
from __future__ import annotations

from dataclasses import dataclass

import redis

from . import scripts
from .keys import Keys

GROUP = "workers"


@dataclass(frozen=True)
class Message:
    id: str
    task_id: str
    run_id: str
    attempt: int


def _to_msg(mid: str, fields: dict) -> Message:
    return Message(mid, fields["task_id"], fields["run_id"], int(fields["attempt"]))


class TaskQueue:
    def __init__(self, r: redis.Redis, keys: Keys) -> None:
        self.r, self.k = r, keys
        self._enqueue = r.register_script(scripts.ENQUEUE)
        self._promote = r.register_script(scripts.PROMOTE_DUE)

    def ensure_group(self) -> None:
        try:
            self.r.xgroup_create(self.k.queue_ready, GROUP, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def enqueue(self, task_id: str, run_id: str) -> bool:
        """Atomically move pending -> queued and publish one dispatch message (no-op otherwise)."""
        return bool(self._enqueue(keys=[self.k.task(task_id), self.k.queue_ready], args=[task_id, run_id]))

    def read(self, consumer: str, count: int = 1, block_ms: int = 1000) -> list[Message]:
        resp = self.r.xreadgroup(GROUP, consumer, {self.k.queue_ready: ">"}, count=count, block=block_ms)
        out: list[Message] = []
        for _stream, entries in resp or []:
            out.extend(_to_msg(mid, f) for mid, f in entries)
        return out

    def autoclaim(self, consumer: str, min_idle_ms: int, count: int = 10) -> list[Message]:
        """Take over messages a (probably dead) consumer received but never acknowledged."""
        resp = self.r.xautoclaim(self.k.queue_ready, GROUP, consumer, min_idle_ms, start_id="0-0", count=count)
        entries = resp[1]
        return [_to_msg(mid, f) for mid, f in entries if f]

    def ack(self, message_id: str) -> None:
        pipe = self.r.pipeline()
        pipe.xack(self.k.queue_ready, GROUP, message_id)
        pipe.xdel(self.k.queue_ready, message_id)
        pipe.execute()

    def promote_due_retries(self, limit: int = 100) -> int:
        return int(self._promote(keys=[self.k.retry, self.k.queue_ready],
                                 args=[limit, self.k.task_prefix, self.k.state_suffix]))

    def dead_letters(self, count: int = 100) -> list[dict]:
        return [{"id": mid, **f} for mid, f in self.r.xrevrange(self.k.queue_dead, count=count)]

    def depth(self) -> dict[str, int]:
        info = {}
        for g in self.r.xinfo_groups(self.k.queue_ready):
            if g["name"] == GROUP:
                info = g
        pending = int(info.get("pending", 0))
        lag = info.get("lag")
        ready = int(lag) if lag is not None else max(int(self.r.xlen(self.k.queue_ready)) - pending, 0)
        return {"ready": ready, "in_flight": pending, "retry_scheduled": int(self.r.zcard(self.k.retry)),
                "running": int(self.r.scard(self.k.running)), "dead": int(self.r.xlen(self.k.queue_dead))}
