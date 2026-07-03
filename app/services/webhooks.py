from __future__ import annotations

import hmac
from hashlib import sha256


class WebhookSignatureError(ValueError):
    pass


def verify_chatwoot_signature(
    *,
    raw_body: bytes,
    timestamp: str | None,
    signature: str | None,
    secret: str,
    now_seconds: int,
    tolerance_seconds: int = 300,
) -> None:
    if not timestamp or not signature:
        raise WebhookSignatureError("missing signature headers")

    try:
        timestamp_int = int(timestamp)
    except ValueError as exc:
        raise WebhookSignatureError("invalid timestamp") from exc

    if abs(now_seconds - timestamp_int) > tolerance_seconds:
        raise WebhookSignatureError("timestamp outside tolerance")

    expected_digest = hmac.new(
        secret.encode(),
        f"{timestamp}.".encode() + raw_body,
        sha256,
    ).hexdigest()
    expected = f"sha256={expected_digest}"

    if not hmac.compare_digest(expected, signature):
        raise WebhookSignatureError("invalid signature")
