"""Built-in data tasks: validate_records, database_upsert, report_generation."""
from __future__ import annotations

from typing import Any

from .registry import NonRetryableError, TaskContext, TaskError, register


def _upstream(ctx: TaskContext, name: str = "from") -> Any:
    """Result of the upstream task named by config[name], or of the single dependency."""
    src = ctx.config.get(name)
    if src is None:
        if len(ctx.inputs) != 1:
            raise NonRetryableError(f"set config.{name} to choose one of {sorted(ctx.inputs)}")
        return next(iter(ctx.inputs.values()))
    if src not in ctx.inputs:
        raise NonRetryableError(f"{src!r} is not a dependency of this task")
    return ctx.inputs[src]


def _dig(data: Any, path: str | None) -> Any:
    for part in (path.split(".") if path else []):
        data = data.get(part) if isinstance(data, dict) else None
    return data


@register("validate_records")
def validate_records(ctx: TaskContext):
    up = _upstream(ctx)
    data = up.get("data") if isinstance(up, dict) and "data" in up else up
    records = _dig(data, ctx.config.get("records_path")) if ctx.config.get("records_path") else data
    if not isinstance(records, list):
        raise NonRetryableError("could not locate a list of records (set config.records_path)")
    required = list(ctx.config.get("required_fields", []))
    valid, rejected = [], []
    for rec in records:
        ok = isinstance(rec, dict) and all(rec.get(f) not in (None, "") for f in required)
        (valid if ok else rejected).append(rec)
    for rec in valid:  # normalise: trim strings
        for k, v in list(rec.items()):
            if isinstance(v, str):
                rec[k] = v.strip()
    ratio = len(rejected) / len(records) if records else 0.0
    if records and ratio > float(ctx.config.get("max_reject_ratio", 0.5)):
        raise TaskError(f"{len(rejected)}/{len(records)} records malformed (>{ctx.config.get('max_reject_ratio', 0.5)})")
    return {"valid": valid, "rejected_count": len(rejected), "total": len(records)}


@register("database_upsert")
def database_upsert(ctx: TaskContext):
    history = ctx.services.get("history")
    if history is None or not getattr(history, "enabled", False):
        raise NonRetryableError("no database configured")
    up = _upstream(ctx)
    records = up.get("valid") if isinstance(up, dict) else up
    key = ctx.config.get("key_field", "id")
    if not isinstance(records, list):
        raise NonRetryableError("upstream result has no 'valid' list")
    if any(key not in r for r in records):
        raise NonRetryableError(f"every record needs a stable {key!r} field")
    return {"upserted": history.upsert_records(records, key)}  # same key -> overwrite: retry-safe


@register("report_generation")
def report_generation(ctx: TaskContext):
    lines = [f"Run {ctx.run_id} report", "-" * 24]
    for task_id, res in ctx.inputs.items():
        if isinstance(res, dict):
            summary = ", ".join(f"{k}={v}" for k, v in res.items() if isinstance(v, (int, float, str)))
        else:
            summary = str(res)
        lines.append(f"{task_id}: {summary}")
    return {"report": "\n".join(lines)}
