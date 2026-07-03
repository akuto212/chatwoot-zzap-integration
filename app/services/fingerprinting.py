from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from unicodedata import normalize
from zoneinfo import ZoneInfo

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class MessageFingerprint:
    message_hash: str
    fingerprint: str


def normalize_message_text(value: str) -> str:
    return normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))


def sha256_hex(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def parse_zzap_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=MOSCOW_TZ)
    return parsed.astimezone(MOSCOW_TZ)


def build_zzap_fingerprint(
    *,
    integration_id: str,
    thread_user_key: str,
    sender_user_key: str,
    message_date: datetime,
    message_text: str,
) -> MessageFingerprint:
    normalized_text = normalize_message_text(message_text)
    message_hash = sha256_hex(normalized_text)
    fingerprint_source = json.dumps(
        [
            integration_id,
            thread_user_key,
            sender_user_key,
            message_date.isoformat(),
            message_hash,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return MessageFingerprint(message_hash=message_hash, fingerprint=sha256_hex(fingerprint_source))
