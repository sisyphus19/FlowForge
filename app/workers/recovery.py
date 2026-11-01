"""Background maintenance shared by all workers. Every step is atomic/idempotent in Redis, so any number of
workers can run it concurrently without coordination."""
from __future__ import annotations

import logging

from app.engine.orchestrator import Orchestrator

log = logging.getLogger(__name__)


def maintenance_once(orch: Orchestrator) -> dict:
    return {"promoted": orch.promote_retries(),
            "recovered": len(orch.recover_abandoned()),
            "reconciled": orch.reconcile()}
