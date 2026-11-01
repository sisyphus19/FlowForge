"""Recurring triggers via APScheduler. Multiple scheduler instances are safe: each firing uses an
idempotency key derived from the workflow and the scheduled minute, so only one run is created."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .orchestrator import Orchestrator, TriggerResult
from .workflow import WorkflowDef

log = logging.getLogger(__name__)


class FlowScheduler:
    def __init__(self, orch: Orchestrator, history) -> None:
        self.orch, self.history = orch, history
        self.sched = BackgroundScheduler(timezone="UTC")

    def start(self) -> None:
        self.sync()
        self.sched.start()

    def shutdown(self) -> None:
        if self.sched.running:
            self.sched.shutdown(wait=False)

    def sync(self) -> None:
        for row in self.history.list_workflows():
            wf = self.history.get_workflow(row["name"])
            if wf:
                self.schedule_workflow(wf["definition"])

    def schedule_workflow(self, defn: WorkflowDef) -> None:
        job_id = f"flow:{defn.name}"
        if self.sched.get_job(job_id):
            self.sched.remove_job(job_id)
        if defn.schedule:
            self.sched.add_job(self.fire, CronTrigger.from_crontab(defn.schedule, timezone="UTC"), args=[defn.name],
                               id=job_id, coalesce=True, misfire_grace_time=60, max_instances=1)

    def fire(self, name: str, now: datetime | None = None) -> TriggerResult | None:
        wf = self.history.get_workflow(name)
        if wf is None or wf["paused"]:
            return None
        minute = (now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
        return self.orch.trigger(wf["definition"], trigger="schedule", idempotency_key=f"sched:{name}:{minute.isoformat()}")
