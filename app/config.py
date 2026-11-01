"""Runtime configuration, read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "sqlite:///flowforge.db"  # empty string disables durable history
    key_prefix: str = ""  # namespace for all Redis keys (used for test isolation)
    lease_ttl_s: float = 30.0  # a worker must renew within this window or lose the task
    maintenance_interval_s: float = 5.0  # retry promotion / lease recovery / reconcile cadence
    pending_idle_ms: int | None = None  # redelivery threshold for un-claimed stream messages
    run_retention_s: int = 86_400  # how long finished-run keys stay in Redis (history lives in DB)
    idempotency_ttl_s: int = 86_400
    api_key: str | None = None  # if set, API requires X-API-Key
    enable_scheduler: bool = False
    enable_test_tasks: bool = False  # registers sleep/flaky/crash handlers - never in production
    allowed_http_hosts: tuple[str, ...] = ()
    worker_concurrency: int = 1
    metrics_port: int = 0  # worker Prometheus port, 0 = disabled

    def __post_init__(self) -> None:
        if self.pending_idle_ms is None:
            self.pending_idle_ms = int(self.lease_ttl_s * 2000)

    @classmethod
    def from_env(cls) -> "Settings":
        hosts = tuple(h.strip() for h in os.getenv("FLOWFORGE_ALLOWED_HTTP_HOSTS", "").split(",") if h.strip())
        pend = os.getenv("FLOWFORGE_PENDING_IDLE_MS")
        return cls(
            redis_url=os.getenv("FLOWFORGE_REDIS_URL", cls.redis_url),
            database_url=os.getenv("FLOWFORGE_DATABASE_URL", cls.database_url),
            key_prefix=os.getenv("FLOWFORGE_KEY_PREFIX", ""),
            lease_ttl_s=float(os.getenv("FLOWFORGE_LEASE_TTL_S", cls.lease_ttl_s)),
            maintenance_interval_s=float(os.getenv("FLOWFORGE_MAINTENANCE_INTERVAL_S", cls.maintenance_interval_s)),
            pending_idle_ms=int(pend) if pend else None,
            run_retention_s=int(os.getenv("FLOWFORGE_RUN_RETENTION_S", cls.run_retention_s)),
            idempotency_ttl_s=int(os.getenv("FLOWFORGE_IDEMPOTENCY_TTL_S", cls.idempotency_ttl_s)),
            api_key=os.getenv("FLOWFORGE_API_KEY") or None,
            enable_scheduler=_flag("FLOWFORGE_ENABLE_SCHEDULER"),
            enable_test_tasks=_flag("FLOWFORGE_ENABLE_TEST_TASKS"),
            allowed_http_hosts=hosts,
            worker_concurrency=int(os.getenv("FLOWFORGE_WORKER_CONCURRENCY", 1)),
            metrics_port=int(os.getenv("FLOWFORGE_METRICS_PORT", 0)),
        )
