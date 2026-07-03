from __future__ import annotations

from enum import StrEnum
from typing import Any


class ChatwootWebhookDecision(StrEnum):
    ACCEPT = "accept"
    IGNORE = "ignore"


def classify_chatwoot_message_created(
    payload: dict[str, Any],
    expected_inbox_id: int,
) -> ChatwootWebhookDecision:
    if payload.get("event") != "message_created":
        return ChatwootWebhookDecision.IGNORE
    if payload.get("message_type") != "outgoing":
        return ChatwootWebhookDecision.IGNORE
    if payload.get("private") is True:
        return ChatwootWebhookDecision.IGNORE

    conversation = payload.get("conversation") or {}
    if int(conversation.get("inbox_id") or 0) != expected_inbox_id:
        return ChatwootWebhookDecision.IGNORE

    sender = payload.get("sender") or {}
    if sender.get("type") not in {"user", None}:
        return ChatwootWebhookDecision.IGNORE

    return ChatwootWebhookDecision.ACCEPT
