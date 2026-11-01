"""Prometheus metrics. Counters live in the process that does the work (API or worker);
queue-depth gauges are computed from Redis at scrape time."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

TASK_EVENTS = Counter("flowforge_task_events_total", "Task lifecycle outcomes", ["outcome"])
TASK_DURATION = Histogram("flowforge_task_duration_seconds", "Handler execution time", ["type"],
                          buckets=(.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300))
RUN_EVENTS = Counter("flowforge_run_events_total", "Run lifecycle outcomes", ["outcome"])
QUEUE_DEPTH = Gauge("flowforge_queue_depth", "Queue sizes", ["queue"])
WORKERS_ALIVE = Gauge("flowforge_workers_alive", "Workers with a live heartbeat key")


def refresh_gauges(store) -> None:
    for name, value in store.queue.depth().items():
        QUEUE_DEPTH.labels(name).set(value)
    WORKERS_ALIVE.set(len(store.live_workers()))
