from __future__ import annotations

import pytest

from app.services.outbound import ChatwootWebhookDecision, classify_chatwoot_message_created


def test_classify_ignores_wrong_event() -> None:
    decision = classify_chatwoot_message_created(
        {"event": "conversation_updated"},
        2,
    )
    assert decision == ChatwootWebhookDecision.IGNORE


def test_classify_accepts_public_outgoing_operator_message() -> None:
    decision = classify_chatwoot_message_created(
        payload={
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "user"},
        },
        expected_inbox_id=2,
    )
    assert decision == ChatwootWebhookDecision.ACCEPT


def test_classify_ignores_private_note() -> None:
    decision = classify_chatwoot_message_created(
        payload={
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": True,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "user"},
        },
        expected_inbox_id=2,
    )
    assert decision == ChatwootWebhookDecision.IGNORE


@pytest.mark.parametrize(
    "payload",
    [
        {
            "event": "message_created",
            "id": 10,
            "message_type": "incoming",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "contact"},
        },
        {
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 999},
            "sender": {"type": "user"},
        },
        {
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "agent_bot"},
        },
        {
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
        },
        {
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": None,
            "sender": {"type": "user"},
        },
        {
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
            "conversation": {"id": 20, "inbox_id": "not-int"},
            "sender": {"type": "user"},
        },
    ],
)
def test_classify_ignores_non_operator_or_malformed_payloads(payload: dict[str, object]) -> None:
    decision = classify_chatwoot_message_created(payload, 2)

    assert decision == ChatwootWebhookDecision.IGNORE
