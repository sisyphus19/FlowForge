"""Runs one handler with a timeout. Python threads cannot be killed, so a timed-out handler is abandoned
(daemon thread) and the attempt is failed; handlers should poll ctx.is_cancelled() and use their own timeouts."""
from __future__ import annotations

import threading
from typing import Any

from app.tasks.registry import Handler, TaskContext, TaskTimeout


def run_handler(fn: Handler, ctx: TaskContext, timeout_s: float | None) -> Any:
    if timeout_s is None:
        return fn(ctx)
    box: dict[str, Any] = {}

    def target() -> None:
        try:
            box["result"] = fn(ctx)
        except BaseException as e:  # noqa: BLE001 - re-raised in the caller's thread
            box["error"] = e

    t = threading.Thread(target=target, daemon=True, name=f"task-{ctx.task_key}")
    t.start()
    t.join(timeout_s)
    if t.is_alive():
        raise TaskTimeout(f"task exceeded {timeout_s}s timeout")
    if "error" in box:
        raise box["error"]
    return box["result"]
