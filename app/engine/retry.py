"""Retry delay computation (exponential backoff with optional full jitter)."""
from __future__ import annotations

import random

from .workflow import RetryPolicy


def compute_delay(policy: RetryPolicy, failures: int, rng: random.Random | None = None) -> float:
    """Seconds to wait before the retry that follows the `failures`-th failure (1-based)."""
    failures = max(failures, 1)
    if policy.backoff == "fixed":
        delay = policy.base_delay_s
    else:
        delay = policy.base_delay_s * (policy.factor ** (failures - 1))
    delay = min(delay, policy.max_delay_s)
    if policy.jitter:
        delay = (rng or random).uniform(0, delay)  # "full jitter" avoids synchronized retry storms
    return delay
