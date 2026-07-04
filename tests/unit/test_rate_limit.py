from __future__ import annotations

from app.workers.rate_limit import ZZapRateLimiter


def test_rate_limiter_first_request_is_ready() -> None:
    limiter = ZZapRateLimiter(interval_seconds=3.0)
    assert limiter.delay_until_next(now=10.0) == 0.0


def test_rate_limiter_waits_between_requests() -> None:
    limiter = ZZapRateLimiter(interval_seconds=3.0)
    limiter.mark_request_finished(now=10.0)
    assert limiter.delay_until_next(now=11.0) == 2.0
