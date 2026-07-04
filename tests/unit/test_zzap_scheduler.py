from __future__ import annotations

from app.workers.zzap_scheduler import ZZapActionQueue


def test_summary_poll_is_coalesced() -> None:
    queue = ZZapActionQueue()
    queue.enqueue_summary_poll()
    queue.enqueue_summary_poll()

    assert queue.size() == 1


def test_thread_fetch_is_coalesced_per_thread() -> None:
    queue = ZZapActionQueue()
    queue.enqueue_thread_fetch("a")
    queue.enqueue_thread_fetch("a")
    queue.enqueue_thread_fetch("b")

    assert queue.size() == 2


def test_summary_poll_is_scheduled_when_due() -> None:
    queue = ZZapActionQueue()

    assert queue.enqueue_summary_poll_if_due(now=10.0, interval_seconds=3.0)
    assert not queue.enqueue_summary_poll_if_due(now=11.0, interval_seconds=3.0)
    assert queue.size() == 1

    queue.pop_next()

    assert queue.enqueue_summary_poll_if_due(now=13.0, interval_seconds=3.0)
    assert queue.size() == 1


def test_summary_poll_can_be_delayed_after_error() -> None:
    queue = ZZapActionQueue()

    queue.delay_summary_until(now=10.0, delay_seconds=300.0)

    assert not queue.enqueue_summary_poll_if_due(now=100.0, interval_seconds=3.0)
    assert queue.enqueue_summary_poll_if_due(now=310.0, interval_seconds=3.0)
