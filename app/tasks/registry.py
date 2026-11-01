"""Registry of permitted task handlers. Workflow definitions can only reference these names."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


class TaskError(Exception):
    """Retryable failure (the default for any exception)."""


class NonRetryableError(TaskError):
    """Permanent failure: skip remaining retries and dead-letter immediately."""


class TaskTimeout(TaskError):
    pass


@dataclass
class TaskContext:
    run_id: str
    task_id: str
    task_key: str
    attempt: int
    config: dict[str, Any]
    inputs: dict[str, Any]          # results of upstream tasks, keyed by task id
    params: dict[str, Any]          # run-level parameters supplied at trigger time
    services: dict[str, Any] = field(default_factory=dict)  # history, settings, ...
    _cancelled: Callable[[], bool] = lambda: False

    def is_cancelled(self) -> bool:
        """Cooperative cancellation: long handlers should poll this."""
        return self._cancelled()


Handler = Callable[[TaskContext], Any]
_HANDLERS: dict[str, Handler] = {}


def register(name: str) -> Callable[[Handler], Handler]:
    def deco(fn: Handler) -> Handler:
        _HANDLERS[name] = fn
        return fn
    return deco


def get_handler(name: str) -> Handler:
    try:
        return _HANDLERS[name]
    except KeyError:
        raise NonRetryableError(f"no handler registered for task type {name!r}") from None


def known_types() -> set[str]:
    return set(_HANDLERS)
