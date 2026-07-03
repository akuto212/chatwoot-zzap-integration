from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import StrEnum


class ZZapActionType(StrEnum):
    SUMMARY_POLL = "summary_poll"
    THREAD_FETCH = "thread_fetch"


@dataclass(frozen=True)
class ZZapAction:
    action_type: ZZapActionType
    thread_user_key: str | None = None


class ZZapActionQueue:
    def __init__(self) -> None:
        self._queue: deque[ZZapAction] = deque()
        self._pending_summary = False
        self._pending_thread_fetches: set[str] = set()

    def enqueue_summary_poll(self) -> None:
        if self._pending_summary:
            return
        self._pending_summary = True
        self._queue.append(ZZapAction(ZZapActionType.SUMMARY_POLL))

    def enqueue_thread_fetch(self, thread_user_key: str) -> None:
        if thread_user_key in self._pending_thread_fetches:
            return
        self._pending_thread_fetches.add(thread_user_key)
        self._queue.append(ZZapAction(ZZapActionType.THREAD_FETCH, thread_user_key))

    def pop_next(self) -> ZZapAction | None:
        if not self._queue:
            return None
        action = self._queue.popleft()
        if action.action_type == ZZapActionType.SUMMARY_POLL:
            self._pending_summary = False
        if action.action_type == ZZapActionType.THREAD_FETCH and action.thread_user_key:
            self._pending_thread_fetches.discard(action.thread_user_key)
        return action

    def size(self) -> int:
        return len(self._queue)
