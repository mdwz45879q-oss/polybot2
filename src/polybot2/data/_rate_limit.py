"""Shared async rate limiter primitives for data sync modules."""

from __future__ import annotations

import asyncio
import time
from collections import deque


class SlidingWindowRateLimiter:
    """Simple async sliding-window limiter."""

    def __init__(self, max_calls: int, window_seconds: float = 1.0):
        self._max_calls = max(1, int(max_calls))
        self._window_seconds = max(1e-6, float(window_seconds))
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            wait_seconds = 0.0
            async with self._lock:
                now = time.monotonic()
                cutoff = now - self._window_seconds
                while self._timestamps and self._timestamps[0] <= cutoff:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._max_calls:
                    self._timestamps.append(now)
                    return

                wait_seconds = (self._timestamps[0] + self._window_seconds) - now

            await asyncio.sleep(wait_seconds if wait_seconds > 0 else 0.0)

