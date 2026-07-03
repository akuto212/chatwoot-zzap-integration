from __future__ import annotations

import hmac
from hashlib import sha256

import pytest

from app.services.webhooks import WebhookSignatureError, verify_chatwoot_signature


def _signature(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, sha256).hexdigest()
    return f"sha256={digest}"


def test_verify_chatwoot_signature_accepts_valid_signature() -> None:
    body = b'{"event":"message_created"}'
    timestamp = "1000"
    secret = "secret"

    verify_chatwoot_signature(
        raw_body=body,
        timestamp=timestamp,
        signature=_signature(secret, timestamp, body),
        secret=secret,
        now_seconds=1100,
        tolerance_seconds=300,
    )


def test_verify_chatwoot_signature_rejects_invalid_signature() -> None:
    with pytest.raises(WebhookSignatureError):
        verify_chatwoot_signature(
            raw_body=b"{}",
            timestamp="1000",
            signature="sha256=bad",
            secret="secret",
            now_seconds=1100,
            tolerance_seconds=300,
        )


def test_verify_chatwoot_signature_rejects_non_ascii_signature() -> None:
    with pytest.raises(WebhookSignatureError):
        verify_chatwoot_signature(
            raw_body=b"{}",
            timestamp="1000",
            signature="sha256=не-hex",
            secret="secret",
            now_seconds=1100,
            tolerance_seconds=300,
        )


def test_verify_chatwoot_signature_rejects_old_timestamp() -> None:
    body = b"{}"
    timestamp = "1000"
    secret = "secret"

    with pytest.raises(WebhookSignatureError):
        verify_chatwoot_signature(
            raw_body=body,
            timestamp=timestamp,
            signature=_signature(secret, timestamp, body),
            secret=secret,
            now_seconds=2000,
            tolerance_seconds=300,
        )
