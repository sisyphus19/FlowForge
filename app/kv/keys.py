"""Redis key namespace. One place defines every key so the layout is auditable."""
from __future__ import annotations


class Keys:
    def __init__(self, prefix: str = "") -> None:
        self.p = prefix
        self.queue_ready = f"{prefix}queue:ready"      # stream: task dispatch messages
        self.queue_dead = f"{prefix}queue:dead"        # stream: dead-letter entries
        self.retry = f"{prefix}retry:scheduled"        # zset: task_id -> due time (ms)
        self.running = f"{prefix}tasks:running"        # set: tasks currently leased
        self.active_runs = f"{prefix}runs:active"      # set: unfinished runs (reconciler input)
        self.task_prefix = f"{prefix}task:"            # Lua builds task keys dynamically from this
        self.state_suffix = ":state"
        self.lease_suffix = ":lease"

    def task(self, tid: str) -> str:        # hash: state, attempt, failures, fence, worker, ...
        return f"{self.task_prefix}{tid}{self.state_suffix}"

    def lease(self, tid: str) -> str:       # string with TTL: liveness signal for the owner
        return f"{self.task_prefix}{tid}{self.lease_suffix}"

    def fence(self, tid: str) -> str:       # monotonically increasing fencing counter
        return f"{self.task_prefix}{tid}:fence"

    def result(self, tid: str) -> str:
        return f"{self.task_prefix}{tid}:result"

    def run(self, rid: str) -> str:
        return f"{self.p}run:{rid}:state"

    def run_def(self, rid: str) -> str:
        return f"{self.p}run:{rid}:def"

    def flow_active(self, name: str) -> str:
        return f"{self.p}flow:{name}:active_runs"

    def idem(self, key: str) -> str:
        return f"{self.p}idem:{key}"

    def worker_hb(self, wid: str) -> str:
        return f"{self.p}worker:{wid}:heartbeat"

    @property
    def worker_hb_pattern(self) -> str:
        return f"{self.p}worker:*:heartbeat"
