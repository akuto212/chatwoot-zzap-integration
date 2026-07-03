from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ZZapRateLimiter:
    interval_seconds: float
    last_request_started_at: float | None = None

    def delay_until_next(self, *, now: float) -> float:
        if self.last_request_started_at is None:
            return 0.0
        elapsed = now - self.last_request_started_at
        return max(0.0, self.interval_seconds - elapsed)

    def mark_request_started(self, *, now: float) -> None:
        self.last_request_started_at = now
