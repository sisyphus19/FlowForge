"""Durable history repository. All writes are idempotent upserts built from authoritative Redis snapshots,
so a failed write heals itself on the next transition / reconcile pass."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.engine.workflow import WorkflowDef

from .models import Base, EventRow, RecordRow, RunRow, TaskRow, WorkflowRow

log = logging.getLogger(__name__)


def _dt(ms: Any) -> datetime | None:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).replace(tzinfo=None) if ms else None


def make_engine(url: str):
    if url.startswith("sqlite"):
        kw: dict[str, Any] = {"connect_args": {"check_same_thread": False, "timeout": 30}}
        if ":memory:" in url or url in ("sqlite://", "sqlite:///"):
            kw["poolclass"] = StaticPool
        eng = create_engine(url, **kw)

        @event.listens_for(eng, "connect")
        def _pragma(dbapi_con, _):  # WAL lets readers and the single writer coexist
            dbapi_con.execute("PRAGMA journal_mode=WAL")
        return eng
    return create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=10)


class NullHistory:
    """Used when no database is configured (Redis-only operation, e.g. in unit tests)."""
    enabled = False

    def init_schema(self): ...
    def upsert_run(self, run: dict): ...
    def upsert_task(self, task: dict): ...
    def add_event(self, run_id, task_id, type_, detail=""): ...
    def upsert_records(self, records, key_field): raise RuntimeError("no database configured")


class HistoryStore:
    enabled = True

    def __init__(self, url: str) -> None:
        self.engine = make_engine(url)
        self.Session = sessionmaker(self.engine, expire_on_commit=False)

    def init_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    # ---- workflows -------------------------------------------------------------------------
    def save_workflow(self, defn: WorkflowDef) -> int:
        with self.Session.begin() as s:
            row = s.get(WorkflowRow, defn.name)
            payload = defn.model_dump(mode="json")
            if row is None:
                s.add(WorkflowRow(name=defn.name, definition=payload, schedule=defn.schedule))
                return 1
            if row.definition != payload:
                row.definition, row.schedule, row.version = payload, defn.schedule, row.version + 1
            return row.version

    def get_workflow(self, name: str) -> dict | None:
        with self.Session() as s:
            row = s.get(WorkflowRow, name)
            if row is None:
                return None
            return {"definition": WorkflowDef.model_validate(row.definition), "version": row.version,
                    "paused": row.paused}

    def list_workflows(self) -> list[dict]:
        with self.Session() as s:
            return [{"name": r.name, "version": r.version, "schedule": r.schedule, "paused": r.paused,
                     "tasks": len(r.definition.get("tasks", []))}
                    for r in s.scalars(select(WorkflowRow).order_by(WorkflowRow.name))]

    def set_paused(self, name: str, paused: bool) -> bool:
        with self.Session.begin() as s:
            row = s.get(WorkflowRow, name)
            if row is None:
                return False
            row.paused = paused
            return True

    # ---- runs / tasks / events -------------------------------------------------------------
    def upsert_run(self, run: dict) -> None:
        try:
            with self.Session.begin() as s:
                row = s.get(RunRow, run["id"]) or RunRow(id=run["id"], workflow_name=run["workflow"])
                row.workflow_name, row.state, row.trigger = run["workflow"], run["state"], run.get("trigger", "api")
                row.idempotency_key, row.params = run.get("idempotency_key"), run.get("params") or {}
                if run.get("created_ms"):
                    row.created_at = _dt(run["created_ms"])
                row.finished_at = _dt(run.get("finished_ms"))
                s.merge(row)
        except Exception:  # best effort; healed by the next upsert from the Redis snapshot
            log.exception("history: upsert_run failed", extra={"ctx": {"run_id": run.get("id")}})

    def upsert_task(self, t: dict) -> None:
        try:
            with self.Session.begin() as s:
                row = s.get(TaskRow, t["id"]) or TaskRow(id=t["id"], run_id=t["run_id"], task_key=t["task_key"],
                                                         type=t["type"], state=t["state"])
                row.state, row.attempt, row.failures = t["state"], t["attempt"], t["failures"]
                row.last_error, row.worker, row.result = t.get("last_error"), t.get("worker"), t.get("result")
                row.started_at, row.finished_at = _dt(t.get("started_ms")), _dt(t.get("finished_ms"))
                s.merge(row)
        except Exception:
            log.exception("history: upsert_task failed", extra={"ctx": {"task_id": t.get("id")}})

    def add_event(self, run_id: str, task_id: str | None, type_: str, detail: str = "") -> None:
        try:
            with self.Session.begin() as s:
                s.add(EventRow(run_id=run_id, task_id=task_id, type=type_, detail=detail[:4000]))
        except Exception:
            log.exception("history: add_event failed")

    def get_run(self, run_id: str) -> dict | None:
        with self.Session() as s:
            run = s.get(RunRow, run_id)
            if run is None:
                return None
            tasks = s.scalars(select(TaskRow).where(TaskRow.run_id == run_id).order_by(TaskRow.task_key)).all()
            return {"id": run.id, "workflow": run.workflow_name, "state": run.state, "trigger": run.trigger,
                    "created_at": run.created_at.isoformat() if run.created_at else None,
                    "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                    "source": "history",
                    "tasks": {t.task_key: {"state": t.state, "attempt": t.attempt, "failures": t.failures,
                                           "last_error": t.last_error, "worker": t.worker, "result": t.result}
                              for t in tasks}}

    def list_runs(self, workflow: str | None = None, limit: int = 50) -> list[dict]:
        with self.Session() as s:
            q = select(RunRow).order_by(RunRow.created_at.desc()).limit(limit)
            if workflow:
                q = q.where(RunRow.workflow_name == workflow)
            return [{"id": r.id, "workflow": r.workflow_name, "state": r.state, "trigger": r.trigger,
                     "created_at": r.created_at.isoformat() if r.created_at else None} for r in s.scalars(q)]

    def get_events(self, run_id: str) -> list[dict]:
        with self.Session() as s:
            rows = s.scalars(select(EventRow).where(EventRow.run_id == run_id).order_by(EventRow.id))
            return [{"ts": e.ts.isoformat(), "type": e.type, "task_id": e.task_id, "detail": e.detail}
                    for e in rows]

    # ---- built-in task target --------------------------------------------------------------
    def upsert_records(self, records: list[dict], key_field: str) -> int:
        """Idempotent: re-running with the same stable key overwrites instead of duplicating."""
        with self.Session.begin() as s:
            for rec in records:
                s.merge(RecordRow(id=str(rec[key_field]), payload=rec))
        return len(records)

    def count_records(self) -> int:
        with self.Session() as s:
            return len(s.scalars(select(RecordRow.id)).all())
