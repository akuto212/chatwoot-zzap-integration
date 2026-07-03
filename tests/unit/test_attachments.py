from __future__ import annotations

import pytest

from app.services.attachments import AttachmentTooLargeError, ensure_attachment_size


def test_ensure_attachment_size_accepts_limit_boundary() -> None:
    ensure_attachment_size(10, 10)


def test_ensure_attachment_size_rejects_large_file() -> None:
    with pytest.raises(AttachmentTooLargeError):
        ensure_attachment_size(size_bytes=11, max_bytes=10)
