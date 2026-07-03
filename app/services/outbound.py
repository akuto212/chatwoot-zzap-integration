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

    conversation = payload.get("conversation")
    if not isinstance(conversation, dict):
        return ChatwootWebhookDecision.IGNORE

    try:
        inbox_id = int(conversation.get("inbox_id") or 0)
    except (TypeError, ValueError):
        return ChatwootWebhookDecision.IGNORE

    if inbox_id != expected_inbox_id:
        return ChatwootWebhookDecision.IGNORE

    sender = payload.get("sender")
    if not isinstance(sender, dict):
        return ChatwootWebhookDecision.IGNORE
    if sender.get("type") != "user":
        return ChatwootWebhookDecision.IGNORE

    return ChatwootWebhookDecision.ACCEPT
