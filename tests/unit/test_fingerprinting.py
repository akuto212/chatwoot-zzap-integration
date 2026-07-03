from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.fingerprinting import (
    build_zzap_fingerprint,
    normalize_message_text,
    parse_zzap_datetime,
    sha256_hex,
)


def test_normalize_message_text_preserves_outer_whitespace() -> None:
    assert normalize_message_text("  hello\r\nworld  ") == "  hello\nworld  "


def test_normalize_message_text_uses_unicode_nfc() -> None:
    assert normalize_message_text("e\u0301") == "\u00e9"


def test_parse_zzap_datetime_assigns_moscow_timezone() -> None:
    parsed = parse_zzap_datetime("2025-04-29T21:06:45")
    assert parsed == datetime(2025, 4, 29, 21, 6, 45, tzinfo=ZoneInfo("Europe/Moscow"))


def test_build_zzap_fingerprint_is_stable() -> None:
    message_date = datetime(2025, 4, 29, 21, 6, 45, tzinfo=ZoneInfo("Europe/Moscow"))
    fingerprint = build_zzap_fingerprint(
        integration_id="11111111-1111-4111-8111-111111111111",
        thread_user_key="thread-key",
        sender_user_key="sender-key",
        message_date=message_date,
        message_text="hello\r\nworld",
    )

    assert len(fingerprint.message_hash) == 64
    assert len(fingerprint.fingerprint) == 64
    assert fingerprint.message_hash == sha256_hex("hello\nworld")
