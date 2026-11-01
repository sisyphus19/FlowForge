"""Idempotency keys: the same logical submission maps to exactly one run."""
from __future__ import annotations

import redis

from .keys import Keys


class IdempotencyStore:
    def __init__(self, r: redis.Redis, keys: Keys) -> None:
        self.r, self.k = r, keys

    def reserve(self, key: str, run_id: str, ttl_s: int) -> str | None:
        """Reserve `key` for `run_id`. Returns None if we won, else the run_id that already owns it.

        Keys expire after `ttl_s`; a key reused after expiry creates a new run (documented policy).
        """
        for _ in range(3):
            if self.r.set(self.k.idem(key), run_id, nx=True, ex=ttl_s):
                return None
            existing = self.r.get(self.k.idem(key))
            if existing is not None:
                return existing
        raise RuntimeError("idempotency key flapping")

    def release(self, key: str, run_id: str) -> None:
        if self.r.get(self.k.idem(key)) == run_id:
            self.r.delete(self.k.idem(key))
