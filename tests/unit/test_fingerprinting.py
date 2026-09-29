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


def test_tab_and_space_have_the_same_normalized_text_and_message_hash() -> None:
    tab_text = "5/1/2023\tDiscontinued"
    space_text = "5/1/2023 Discontinued"
    message_date = datetime(2026, 9, 29, 11, 20, 50, tzinfo=ZoneInfo("UTC"))

    assert normalize_message_text(tab_text) == normalize_message_text(space_text)
    tab_fingerprint = build_zzap_fingerprint(
        integration_id="integration-1",
        thread_user_key="thread-1",
        sender_user_key="sender-1",
        message_date=message_date,
        message_text=tab_text,
    )
    space_fingerprint = build_zzap_fingerprint(
        integration_id="integration-1",
        thread_user_key="thread-1",
        sender_user_key="sender-1",
        message_date=message_date,
        message_text=space_text,
    )
    assert tab_fingerprint.message_hash == space_fingerprint.message_hash
    assert normalize_message_text("line  one\n\nline two") == "line  one\n\nline two"


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


def test_build_zzap_fingerprint_uses_unambiguous_source_encoding() -> None:
    message_date = datetime(2025, 4, 29, 21, 6, 45, tzinfo=ZoneInfo("Europe/Moscow"))

    first = build_zzap_fingerprint(
        integration_id="a|b",
        thread_user_key="c",
        sender_user_key="d",
        message_date=message_date,
        message_text="hello",
    )
    second = build_zzap_fingerprint(
        integration_id="a",
        thread_user_key="b|c",
        sender_user_key="d",
        message_date=message_date,
        message_text="hello",
    )

    assert first.fingerprint != second.fingerprint
