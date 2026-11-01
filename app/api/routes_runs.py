from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import BaseModel, Field

from .deps import Auth, get_ctx

router = APIRouter(tags=["runs"], dependencies=[Auth])


class TriggerBody(BaseModel):
    params: dict = Field(default_factory=dict)


@router.post("/workflows/{name}/runs")
def trigger_run(name: str, response: Response, body: TriggerBody | None = None,
                idempotency_key: str | None = Header(default=None, max_length=200), ctx=Depends(get_ctx)):
    wf = ctx.history.get_workflow(name)
    if not wf:
        raise HTTPException(404, "unknown workflow")
    res = ctx.orch.trigger(wf["definition"], trigger="api", idempotency_key=idempotency_key,
                           params=(body.params if body else {}))
    if res.skipped:
        raise HTTPException(409, "a run is already active and the workflow's concurrency policy is 'skip'")
    response.status_code = 201 if res.created else 200
    return {"run_id": res.run_id, "created": res.created}


@router.get("/runs")
def list_runs(workflow: str | None = None, limit: int = 50, ctx=Depends(get_ctx)):
    return ctx.history.list_runs(workflow, min(limit, 200))


@router.get("/runs/{run_id}")
def get_run(run_id: str, ctx=Depends(get_ctx)):
    snap = ctx.orch.snapshot(run_id) or ctx.history.get_run(run_id)  # live state first, durable history after expiry
    if not snap:
        raise HTTPException(404, "unknown run")
    return snap


@router.get("/runs/{run_id}/events")
def run_events(run_id: str, ctx=Depends(get_ctx)):
    return ctx.history.get_events(run_id)


@router.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str, ctx=Depends(get_ctx)):
    if not ctx.orch.cancel_run(run_id):
        raise HTTPException(409, "run is unknown or already finished")
    return {"run_id": run_id, "cancel_requested": True}
