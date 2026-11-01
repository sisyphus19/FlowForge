"""`notify` task and pluggable alert sinks (used for dead-letter alerts)."""
from __future__ import annotations

import logging
from typing import Protocol

from .registry import TaskContext, register

log = logging.getLogger("flowforge.alerts")


class AlertSink(Protocol):
    def send(self, subject: str, body: str, **ctx) -> None: ...


class LoggingAlertSink:
    """Default sink. Swap for Slack/email/PagerDuty by implementing AlertSink."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []  # kept for tests / inspection

    def send(self, subject: str, body: str, **ctx) -> None:
        self.sent.append((subject, body))
        log.error("ALERT %s: %s", subject, body, extra={"ctx": ctx})


@register("notify")
def notify(ctx: TaskContext):
    message = str(ctx.config.get("message", "workflow notification"))
    summary = {k: v for k, v in ctx.inputs.items()}
    log.info("notify: %s", message, extra={"ctx": {"run_id": ctx.run_id, "inputs": list(summary)}})
    return {"notified": True, "message": message}
