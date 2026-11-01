"""Handlers used by tests/benchmarks (enabled only with FLOWFORGE_ENABLE_TEST_TASKS=1)."""
from __future__ import annotations

import os
import time

from .registry import NonRetryableError, TaskContext, TaskError, register

_REGISTERED = False


def register_test_tasks() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    _REGISTERED = True

    @register("noop")
    def noop(ctx: TaskContext):
        return {"ok": True}

    @register("echo")
    def echo(ctx: TaskContext):
        return {"value": ctx.config.get("value"), "inputs": ctx.inputs, "params": ctx.params}

    @register("sleep")
    def sleep(ctx: TaskContext):
        end = time.time() + float(ctx.config.get("seconds", 1))
        while time.time() < end:
            if ctx.is_cancelled():
                return {"cancelled_early": True}
            time.sleep(0.02)
        return {"slept": ctx.config.get("seconds", 1)}

    @register("flaky")
    def flaky(ctx: TaskContext):
        """Fails on attempts <= config.fail_attempts, then succeeds."""
        if ctx.attempt <= int(ctx.config.get("fail_attempts", 1)):
            raise TaskError(f"induced failure on attempt {ctx.attempt}")
        return {"succeeded_on_attempt": ctx.attempt}

    @register("always_fail")
    def always_fail(ctx: TaskContext):
        raise TaskError("always fails")

    @register("fatal")
    def fatal(ctx: TaskContext):
        raise NonRetryableError("permanent failure")

    @register("count_exec")
    def count_exec(ctx: TaskContext):
        """Records every execution in Redis so tests can count duplicate executions."""
        store = ctx.services["store"]
        store.r.rpush(f"{store.keys.p}test:executions", f"{ctx.task_id}#{ctx.attempt}")
        time.sleep(float(ctx.config.get("seconds", 0)))
        return {"attempt": ctx.attempt}

    @register("crash")
    def crash(ctx: TaskContext):
        os._exit(137)
