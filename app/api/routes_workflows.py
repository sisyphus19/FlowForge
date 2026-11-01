from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import ValidationError

from app.engine.workflow import WorkflowDef, load_workflow
from app.tasks import registry

from .deps import Auth, get_ctx

router = APIRouter(prefix="/workflows", tags=["workflows"], dependencies=[Auth])


def _register(ctx, defn: WorkflowDef) -> dict:
    try:
        defn.validate_handlers(registry.known_types())
    except ValueError as e:
        raise HTTPException(422, str(e))
    version = ctx.history.save_workflow(defn)
    if ctx.scheduler:
        ctx.scheduler.schedule_workflow(defn)
    return {"name": defn.name, "version": version, "schedule": defn.schedule}


@router.post("", status_code=201)
def create_workflow(defn: WorkflowDef, ctx=Depends(get_ctx)):
    return _register(ctx, defn)


@router.post("/yaml", status_code=201)
async def create_workflow_yaml(request: Request, ctx=Depends(get_ctx)):
    try:
        defn = load_workflow((await request.body()).decode())
    except (ValidationError, ValueError) as e:
        raise HTTPException(422, str(e))
    return _register(ctx, defn)


@router.get("")
def list_workflows(ctx=Depends(get_ctx)):
    return ctx.history.list_workflows()


@router.get("/{name}")
def get_workflow(name: str, ctx=Depends(get_ctx)):
    wf = ctx.history.get_workflow(name)
    if not wf:
        raise HTTPException(404, "unknown workflow")
    return {"version": wf["version"], "paused": wf["paused"], "definition": wf["definition"].model_dump()}


@router.post("/{name}/pause")
def pause(name: str, ctx=Depends(get_ctx)):
    if not ctx.history.set_paused(name, True):
        raise HTTPException(404, "unknown workflow")
    return {"name": name, "paused": True}


@router.post("/{name}/resume")
def resume(name: str, ctx=Depends(get_ctx)):
    if not ctx.history.set_paused(name, False):
        raise HTTPException(404, "unknown workflow")
    return {"name": name, "paused": False}
