from __future__ import annotations


class AttachmentTooLargeError(ValueError):
    pass


def ensure_attachment_size(size_bytes: int, max_bytes: int) -> None:
    if size_bytes > max_bytes:
        raise AttachmentTooLargeError(
            f"attachment is {size_bytes} bytes, max allowed is {max_bytes} bytes",
        )
