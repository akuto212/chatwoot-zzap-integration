from __future__ import annotations

import pytest

from app.services.outbound import ChatwootWebhookDecision, classify_chatwoot_message_created


@pytest.mark.parametrize("message_type", ["outgoing", "template"])
def test_classify_ignores_wrong_event(message_type: str) -> None:
    decision = classify_chatwoot_message_created(
        {
            "event": "conversation_updated",
            "message_type": message_type,
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "user"},
        },
        2,
    )
    assert decision == ChatwootWebhookDecision.IGNORE


@pytest.mark.parametrize("message_type", ["outgoing", "template"])
@pytest.mark.parametrize(
    "sender_fields",
    [
        {"sender": {"type": "user"}},
        {"sender": {"type": "agent_bot"}},
        {"sender": {"type": "automation"}},
        {"sender": {}},
        {"sender": None},
        {"sender": "automation"},
        {},
    ],
)
def test_classify_accepts_public_outbound_regardless_of_sender(
    message_type: str,
    sender_fields: dict[str, object],
) -> None:
    decision = classify_chatwoot_message_created(
        payload={
            "event": "message_created",
            "id": 10,
            "message_type": message_type,
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            **sender_fields,
        },
        expected_inbox_id=2,
    )
    assert decision == ChatwootWebhookDecision.ACCEPT


@pytest.mark.parametrize("message_type", ["outgoing", "template"])
def test_classify_ignores_private_note(message_type: str) -> None:
    decision = classify_chatwoot_message_created(
        payload={
            "event": "message_created",
            "id": 10,
            "message_type": message_type,
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
            "message_type": "activity",
            "private": False,
            "conversation": {"id": 20, "inbox_id": 2},
            "sender": {"type": "user"},
        },
        {
            "event": "message_created",
            "id": 10,
            "message_type": "outgoing",
            "private": False,
        },
        {
            "event": "message_created",
            "message_type": "template",
            "conversation": {"inbox_id": 999},
        },
        {
            "event": "message_created",
            "message_type": "outgoing",
            "conversation": [],
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
def test_classify_ignores_ineligible_or_malformed_payloads(payload: dict[str, object]) -> None:
    decision = classify_chatwoot_message_created(payload, 2)

    assert decision == ChatwootWebhookDecision.IGNORE
