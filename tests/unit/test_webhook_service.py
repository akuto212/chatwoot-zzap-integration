from __future__ import annotations

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
