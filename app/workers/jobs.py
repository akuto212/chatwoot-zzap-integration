from __future__ import annotations

from datetime import timedelta

OUTBOUND_RETRY_DELAYS = [timedelta(minutes=1), timedelta(minutes=5), timedelta(minutes=15)]
INBOUND_RETRY_DELAYS = [
    timedelta(seconds=10),
    timedelta(seconds=30),
    timedelta(minutes=1),
    timedelta(minutes=5),
    timedelta(minutes=15),
]


def retry_delay_for_attempt(*, direction: str, attempt_count: int) -> timedelta | None:
    delays = OUTBOUND_RETRY_DELAYS if direction == "outbound" else INBOUND_RETRY_DELAYS
    index = attempt_count - 1
    if index < 0 or index >= len(delays):
        return None
    return delays[index]
