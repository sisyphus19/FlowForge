"""Workflow definition models, parsing and validation."""
from __future__ import annotations

import re
from typing import Any, Literal

import yaml
from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, Field, field_validator, model_validator

from .dependency_resolver import topological_order

ID_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"
_ID_RE = re.compile(ID_PATTERN)


class RetryPolicy(BaseModel):
    max_attempts: int = Field(3, ge=1, le=20, description="total executions allowed, including the first")
    backoff: Literal["exponential", "fixed"] = "exponential"
    base_delay_s: float = Field(1.0, ge=0)
    max_delay_s: float = Field(60.0, ge=0)
    factor: float = Field(2.0, ge=1)
    jitter: bool = True


class ConcurrencyPolicy(BaseModel):
    max_runs: int = Field(1, ge=1)
    on_overlap: Literal["skip", "allow"] = "skip"


class TaskDef(BaseModel):
    id: str = Field(pattern=ID_PATTERN)
    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    retries: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout_seconds: float | None = Field(None, gt=0)


class WorkflowDef(BaseModel):
    name: str = Field(pattern=ID_PATTERN)
    schedule: str | None = None
    concurrency: ConcurrencyPolicy = Field(default_factory=ConcurrencyPolicy)
    tasks: list[TaskDef] = Field(min_length=1, max_length=200)

    @field_validator("schedule")
    @classmethod
    def _cron(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                CronTrigger.from_crontab(v)
            except ValueError as e:
                raise ValueError(f"invalid cron expression {v!r}: {e}") from e
        return v

    @model_validator(mode="after")
    def _graph(self) -> "WorkflowDef":
        ids = [t.id for t in self.tasks]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(f"duplicate task ids: {', '.join(dupes)}")
        known = set(ids)
        for t in self.tasks:
            if t.id in t.depends_on:
                raise ValueError(f"task {t.id!r} depends on itself")
            missing = [d for d in t.depends_on if d not in known]
            if missing:
                raise ValueError(f"task {t.id!r} depends on unknown task(s): {', '.join(missing)}")
        topological_order(self.tasks)  # raises CycleError (a ValueError) on cycles
        return self

    def task(self, task_id: str) -> TaskDef:
        for t in self.tasks:
            if t.id == task_id:
                return t
        raise KeyError(task_id)

    def validate_handlers(self, known_types: set[str]) -> None:
        """Only registered handlers may run: definitions can never smuggle in arbitrary code."""
        unknown = sorted({t.type for t in self.tasks} - known_types)
        if unknown:
            raise ValueError(f"unknown task type(s): {', '.join(unknown)}")


def load_workflow(text: str) -> WorkflowDef:
    """Parse a YAML (or JSON) document into a validated WorkflowDef."""
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("workflow definition must be a mapping")
    return WorkflowDef.model_validate(data)


def is_valid_id(s: str) -> bool:
    return bool(_ID_RE.match(s))
