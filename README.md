# FlowForge — distributed Python automation engine

Define multi-step workflows in YAML/JSON; FlowForge schedules them, runs the tasks on a pool of independent
workers, retries failures with backoff, recovers work from crashed workers, and keeps a durable audit trail.

```
client ─▶ FastAPI ─▶ Orchestrator ─▶ Redis (streams, leases, retry zset, idempotency) ◀─▶ Worker × N
                          │                                                                  │
                          └──────────────▶ PostgreSQL (definitions, runs, tasks, events) ◀───┘
```

## Quick start
```bash
pip install -e ".[dev]" && pytest -q                 # needs a redis-server binary on PATH (tests spawn their own)
docker compose up --build --scale worker=3           # API :8000, Prometheus :9090
curl -X POST localhost:8000/workflows/yaml --data-binary @examples/daily_data_pipeline.yaml
curl -X POST localhost:8000/workflows/daily_book_pipeline/runs -H 'Idempotency-Key: demo-1'
curl localhost:8000/runs/<run_id>
```
Interactive docs: `/docs`. If `FLOWFORGE_API_KEY` is set, send `X-API-Key`.

## How reliability works
| Problem | Mechanism (file) |
|---|---|
| Two workers grab one task | `CLAIM` Lua script: only `queued` + matching `attempt` can be claimed (`kv/scripts.py`) |
| Worker dies mid-task | lease key with TTL, renewed by a heartbeat thread; `REQUEUE_EXPIRED` re-queues tasks whose lease vanished |
| Zombie worker finishes late | **fencing**: every claim gets a monotonically increasing token; `COMPLETE`/`FAIL` reject any other token |
| Duplicate/stale stream messages | message carries `attempt`; claim fails on mismatch and the message is dropped |
| Temporary failure | exponential backoff + full jitter via Redis ZSET; `PROMOTE_DUE` is atomic so retries fire once |
| Permanent failure | `NonRetryableError` or exhausted attempts → dead-letter stream, alert, downstream tasks `skipped`; replay via API |
| Duplicate API submissions | `Idempotency-Key` → `SET NX` reservation → same run returned |
| Overlapping scheduled runs | per-workflow `max_runs` / `on_overlap: skip|allow`; scheduler fires use per-minute idempotency keys |
| Crash between "task done" and "release dependents" | `reconcile()` re-advances every active run each maintenance tick |
| Run "completed" just because messages were consumed | run state derived from actual terminal task states, finalized once by Lua |

Ownership authority is the task hash (`state=running` + `fence`), not the lease key: an expired lease only *permits*
recovery. A slow worker that finishes before recovery runs commits safely; after takeover it is rejected.

## Delivery semantics (be honest in interviews)
At-least-once. A task can execute twice (e.g. worker killed after a side effect but before completing); only one
result is ever committed. Handlers with external side effects must be idempotent — e.g. `database_upsert` keys on a
stable id so retries and replays overwrite rather than duplicate.

## Known limitations
- **Redis and PostgreSQL are not transactional together.** Redis is authoritative for live state; PostgreSQL is
  written best-effort from Redis snapshots (idempotent upserts, healed at the next transition/finalization). A
  transactional outbox would close the remaining gap; it is not implemented.
- If Redis loses data (no persistence, failover with lost writes), in-flight runs are lost; history up to the last
  persisted transition survives in PostgreSQL. Compose enables AOF.
- Timeouts cannot kill a Python thread: a timed-out handler is abandoned (daemon thread) and the attempt fails.
  Cancellation of running tasks is cooperative (`ctx.is_cancelled()`).
- Lua scripts build some keys dynamically, so Redis Cluster is not supported (single node / Sentinel).
- `on_overlap: queue` is not implemented. No dashboard UI yet (Prometheus metrics + REST only).
- DNS-rebinding is not fully mitigated in `http_fetch` (use `FLOWFORGE_ALLOWED_HTTP_HOSTS` and an egress proxy).
- Docker/Compose files and the PostgreSQL path were **not executed** in the authoring environment (tests use SQLite).

## Tests (54)
`tests/unit` (validation, resolver, backoff, state machine) · `tests/integration` (Lua primitives against a real
redis-server, end-to-end runs, API) · `tests/failure_recovery` (SIGKILL of real worker processes, zombie fencing,
duplicate deliveries, crash between steps, concurrent recovery).

## Benchmark
`python scripts/benchmark.py --tasks 600 --workers 1 2 4` (fixed `noop` workload, Redis on localhost, no DB).
Results from the authoring sandbox (**1 CPU core** shared by Redis, workers and the load generator, so worker-count
scaling is not meaningful there — rerun on a multi-core machine):

| workers | throughput (tasks/s) | start latency p50/p95/p99 (ms, low load) |
|---|---|---|
| 1 | 346 | 1 / 1 / 3 |
| 2 | 402 | 0 / 1 / 1 |
| 4 | 392 | 0 / 1 / 6 |

Recovery after `kill -9` of a worker (lease TTL 1 s, 3 trials): 1.05–1.18 s until the task is running again.
