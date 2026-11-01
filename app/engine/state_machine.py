"""Task / run state machines. The Lua scripts enforce these atomically; this module documents and tests them."""
from __future__ import annotations

from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"        # waiting for dependencies
    QUEUED = "queued"          # dispatch message published
    RUNNING = "running"        # leased by a worker
    RETRY_WAIT = "retry_wait"  # failed, delayed retry scheduled
    SUCCEEDED = "succeeded"
    FAILED = "failed"          # retries exhausted -> dead-lettered
    SKIPPED = "skipped"        # an upstream task failed / was skipped / cancelled
    CANCELLED = "cancelled"


class RunState(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TASK_TERMINAL = {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.SKIPPED, TaskState.CANCELLED}
TASK_TERMINAL_VALUES = {s.value for s in TASK_TERMINAL}
UPSTREAM_BAD = {TaskState.FAILED.value, TaskState.SKIPPED.value, TaskState.CANCELLED.value}
RUN_TERMINAL_VALUES = {RunState.SUCCEEDED.value, RunState.FAILED.value, RunState.CANCELLED.value}

T = TaskState
ALLOWED: dict[TaskState, set[TaskState]] = {
    T.PENDING: {T.QUEUED, T.SKIPPED, T.CANCELLED},
    T.QUEUED: {T.RUNNING, T.CANCELLED},
    T.RUNNING: {T.SUCCEEDED, T.RETRY_WAIT, T.FAILED, T.QUEUED, T.CANCELLED},  # RUNNING->QUEUED = lease recovery
    T.RETRY_WAIT: {T.QUEUED, T.CANCELLED},
    T.FAILED: {T.PENDING},  # dead-letter replay
    T.SKIPPED: {T.PENDING},  # reset when an upstream is replayed
    T.SUCCEEDED: set(),
    T.CANCELLED: set(),
}


class InvalidTransition(ValueError):
    pass


def assert_transition(src: TaskState, dst: TaskState) -> None:
    if dst not in ALLOWED[src]:
        raise InvalidTransition(f"{src.value} -> {dst.value}")
