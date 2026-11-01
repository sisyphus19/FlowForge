from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from .deps import Auth, get_ctx

router = APIRouter(tags=["tasks"], dependencies=[Auth])


@router.get("/dead-letters")
def dead_letters(ctx=Depends(get_ctx)):
    return ctx.store.queue.dead_letters()


@router.post("/runs/{run_id}/tasks/{task_key}/replay")
def replay(run_id: str, task_key: str, ctx=Depends(get_ctx)):
    try:
        ok = ctx.orch.replay_task(run_id, task_key)
    except KeyError:
        raise HTTPException(404, "unknown run or task")
    except ValueError as e:
        raise HTTPException(409, str(e))
    if not ok:
        raise HTTPException(409, "task is not dead-lettered")
    return {"run_id": run_id, "task": task_key, "replayed": True}


@router.get("/queues")
def queues(ctx=Depends(get_ctx)):
    return {**ctx.store.queue.depth(), "workers": ctx.store.live_workers()}
