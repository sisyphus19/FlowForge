"""Reproducible benchmark: throughput vs worker count, task-start latency at low load, recovery time.

    python scripts/benchmark.py --tasks 600 --workers 1 2 4        # starts its own throwaway redis-server
    python scripts/benchmark.py --redis-url redis://localhost:6379/5

Fixed workload: single-task `noop` runs, 1 consumer thread per worker process, Redis on localhost,
no durable history (isolates the coordination layer). Numbers depend entirely on the machine.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
import uuid

import redis

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from app.config import Settings  # noqa: E402
from app.engine.orchestrator import Orchestrator  # noqa: E402
from app.engine.workflow import WorkflowDef  # noqa: E402
from app.kv.redis_store import RedisStore  # noqa: E402
from app.tasks.testing import register_test_tasks  # noqa: E402

register_test_tasks()
WF = WorkflowDef.model_validate({"name": "bench", "concurrency": {"max_runs": 100000, "on_overlap": "allow"},
                                 "tasks": [{"id": "t", "type": "noop"}]})
SLEEP_WF = WorkflowDef.model_validate({"name": "bench-sleep", "concurrency": {"max_runs": 100000, "on_overlap": "allow"},
                                       "tasks": [{"id": "t", "type": "count_exec", "config": {"seconds": 2}}]})


def start_redis():
    exe = shutil.which("redis-server")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    p = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}/0"
    for _ in range(100):
        try:
            redis.Redis.from_url(url).ping()
            return p, url
        except redis.ConnectionError:
            time.sleep(0.05)
    raise RuntimeError("redis did not start")


def spawn(url, prefix, lease=1.0):
    env = {**os.environ, "FLOWFORGE_REDIS_URL": url, "FLOWFORGE_KEY_PREFIX": prefix, "FLOWFORGE_DATABASE_URL": "",
           "FLOWFORGE_ENABLE_TEST_TASKS": "1", "FLOWFORGE_LEASE_TTL_S": str(lease),
           "FLOWFORGE_MAINTENANCE_INTERVAL_S": "0.2", "PYTHONPATH": ROOT}
    return subprocess.Popen([sys.executable, "-m", "app.workers.worker"], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))]


def times(orch, run_ids):
    pipe = orch.r.pipeline()
    for rid in run_ids:
        pipe.hmget(orch.k.task(f"{rid}:t"), "queued_ms", "started_ms", "finished_ms", "state")
    return pipe.execute()


def wait_done(orch, run_ids, timeout=300):
    end = time.time() + timeout
    while time.time() < end:
        rows = times(orch, run_ids)
        if all(r[3] == "succeeded" for r in rows):
            return rows
        time.sleep(0.1)
    raise TimeoutError("benchmark did not finish")


def cleanup(r, prefix):
    for k in r.scan_iter(f"{prefix}*"):
        r.delete(k)


def run_case(url, n_tasks, n_workers):
    prefix = f"bench{uuid.uuid4().hex[:6]}:"
    settings = Settings(redis_url=url, database_url="", key_prefix=prefix, lease_ttl_s=1.0)
    store = RedisStore(url=url, prefix=prefix)
    orch = Orchestrator(store, None, settings)
    procs = [spawn(url, prefix) for _ in range(n_workers)]
    try:
        while len(store.live_workers()) < n_workers:
            time.sleep(0.1)
        # Phase 1: burst throughput
        ids = [orch.trigger(WF).run_id for _ in range(n_tasks)]
        rows = wait_done(orch, ids)
        started = [int(r[1]) for r in rows]
        finished = [int(r[2]) for r in rows]
        thr = n_tasks / ((max(finished) - min(started)) / 1000)
        # Phase 2: start latency at low load (paced 20 tasks/s)
        lat_ids = []
        for _ in range(100):
            lat_ids.append(orch.trigger(WF).run_id)
            time.sleep(0.05)
        rows = wait_done(orch, lat_ids)
        lat = [int(r[1]) - int(r[0]) for r in rows]
        mem = store.r.info("memory")["used_memory"]
        return {"workers": n_workers, "tasks": n_tasks, "throughput_tps": round(thr, 1),
                "start_latency_ms": {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99)},
                "redis_bytes_per_finished_task": round((mem - 1_000_000) / (n_tasks + 100))}
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait(10)
        cleanup(store.r, prefix)


def recovery_case(url, trials=3):
    out = []
    for _ in range(trials):
        prefix = f"bench{uuid.uuid4().hex[:6]}:"
        settings = Settings(redis_url=url, database_url="", key_prefix=prefix, lease_ttl_s=1.0)
        store = RedisStore(url=url, prefix=prefix)
        orch = Orchestrator(store, None, settings)
        a = spawn(url, prefix)
        try:
            rid = orch.trigger(SLEEP_WF).run_id
            tid = f"{rid}:t"
            while orch.task_hash(tid).get("state") != "running":
                time.sleep(0.02)
            b = spawn(url, prefix)  # survivor
            t0 = time.time()
            a.send_signal(signal.SIGKILL)
            a.wait()
            while True:  # recovery = time until the task is running again under a new attempt
                h = orch.task_hash(tid)
                if h.get("state") == "running" and h.get("attempt") == "2":
                    out.append(time.time() - t0)
                    break
                time.sleep(0.02)
            while orch.task_hash(tid).get("state") != "succeeded":
                time.sleep(0.05)
            b.terminate()
            b.wait(10)
        finally:
            a.kill()
            cleanup(store.r, prefix)
    return {"lease_ttl_s": 1.0, "trials": trials, "recovery_s": [round(x, 2) for x in out],
            "median_s": round(statistics.median(out), 2)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tasks", type=int, default=600)
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    ap.add_argument("--redis-url")
    ap.add_argument("--out", default="benchmark_results.json")
    a = ap.parse_args()
    proc, url = (None, a.redis_url) if a.redis_url else start_redis()
    try:
        results = {"cpu_count": os.cpu_count(), "scaling": [run_case(url, a.tasks, w) for w in a.workers],
                   "recovery": recovery_case(url)}
    finally:
        if proc:
            proc.terminate()
    json.dump(results, open(a.out, "w"), indent=2)
    print(json.dumps(results, indent=2))
