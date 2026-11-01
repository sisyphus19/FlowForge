"""FastAPI control plane."""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api import routes_runs, routes_tasks, routes_workflows
from app.config import Settings
from app.db.repository import HistoryStore
from app.engine.orchestrator import Orchestrator
from app.engine.scheduler import FlowScheduler
from app.kv.redis_store import RedisStore
from app.observability import metrics
from app.tasks import registry


@dataclass
class Ctx:
    settings: Settings
    store: RedisStore
    history: Any
    orch: Orchestrator
    scheduler: FlowScheduler | None


def create_app(settings: Settings | None = None, store: RedisStore | None = None, history=None) -> FastAPI:
    settings = settings or Settings.from_env()
    if settings.enable_test_tasks:
        from app.tasks.testing import register_test_tasks
        register_test_tasks()
    store = store or RedisStore(url=settings.redis_url, prefix=settings.key_prefix)
    if history is None:
        if not settings.database_url:
            raise RuntimeError("the API requires FLOWFORGE_DATABASE_URL (durable workflow definitions/history)")
        history = HistoryStore(settings.database_url)
    history.init_schema()
    orch = Orchestrator(store, history, settings)
    scheduler = FlowScheduler(orch, history) if settings.enable_scheduler else None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if scheduler:
            scheduler.start()
        yield
        if scheduler:
            scheduler.shutdown()

    app = FastAPI(title="FlowForge", version="0.1.0", lifespan=lifespan)
    app.state.ctx = Ctx(settings, store, history, orch, scheduler)
    for r in (routes_workflows.router, routes_runs.router, routes_tasks.router):
        app.include_router(r)

    @app.get("/health")
    def health():
        try:
            return {"status": "ok", "redis": store.ping(), "task_types": sorted(registry.known_types())}
        except Exception as e:  # noqa: BLE001
            return Response(content=f'{{"status":"degraded","error":"{type(e).__name__}"}}', status_code=503,
                            media_type="application/json")

    @app.get("/metrics")
    def prom():
        metrics.refresh_gauges(store)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def app_factory() -> FastAPI:  # uvicorn --factory app.main:app_factory
    return create_app()
