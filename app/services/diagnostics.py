from __future__ import annotations

import errno
import hashlib
import ipaddress
import json
import logging
import re
import socket
import ssl
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

logger = logging.getLogger("app.zzap")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    logging.basicConfig(format="%(message)s")
# HTTPX INFO logs full thread URLs, including user_key.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def emit(event: str, *, warning: bool = False, **fields: Any) -> None:
    logger.log(
        logging.WARNING if warning else logging.INFO,
        json.dumps(
            {
                "event": event,
                "timestamp": datetime.now(UTC).isoformat(),
                **fields,
            },
            sort_keys=True,
        ),
    )


def network_category(exc: BaseException) -> str:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    for kind, category in [
        (httpx.TimeoutException, "timeout"),
        (socket.gaierror, "dns"),
        (ssl.SSLError, "tls"),
        (ConnectionResetError, "connection_reset"),
    ]:
        if any(isinstance(item, kind) for item in chain):
            return category
    if any(
        isinstance(item, OSError)
        and item.errno in {errno.ECONNREFUSED, errno.EHOSTUNREACH, errno.ENETUNREACH}
        for item in chain
    ):
        return "tcp"
    return "connection" if isinstance(exc, httpx.ConnectError) else "network_unknown"


def response_diagnostics(
    response: httpx.Response,
    sensitive_values: tuple[str, ...] = (),
) -> dict[str, Any]:
    body_format = "unknown"
    envelope = False
    code: int | None = None
    try:
        payload = response.json()
        body_format = "json"
        if isinstance(payload, dict):
            envelope = isinstance(payload.get("success"), bool) and "code" in payload
            candidate = payload.get("code")
            if type(candidate) is int and 0 <= candidate <= 999999:
                code = candidate
    except ValueError:
        if "text/html" in response.headers.get(
            "content-type", ""
        ) or response.content.lstrip().lower().startswith((b"<html", b"<!doctype html")):
            body_format = "html"
    headers: dict[str, str] = {}
    for name in (
        "server",
        "retry-after",
        "x-request-id",
        "request-id",
        "x-correlation-id",
        "traceparent",
        "cf-ray",
        "ratelimit-limit",
        "ratelimit-remaining",
        "ratelimit-reset",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
    ):
        value = response.headers.get(name, "")
        if any(secret and secret.lower() in value.lower() for secret in sensitive_values):
            continue
        if name == "server":
            valid = bool(
                re.fullmatch(
                    r"(?:cloudflare|cloudfront|nginx|apache|envoy|awselb)(?:/[0-9.]{1,16})?",
                    value.lower(),
                )
            )
        elif "ratelimit" in name or name == "retry-after":
            valid = bool(re.fullmatch(r"[0-9]{1,12}", value))
            if name == "retry-after" and not valid and len(value) <= 40:
                try:
                    parsed = parsedate_to_datetime(value)
                    if parsed.tzinfo is not None:
                        value = parsed.astimezone(UTC).isoformat()
                        valid = True
                except ValueError, TypeError, OverflowError:
                    pass
        elif name == "cf-ray":
            valid = bool(re.fullmatch(r"[0-9a-f]{16,32}-[A-Z]{3}", value))
        elif name == "traceparent":
            valid = bool(re.fullmatch(r"[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", value))
        else:
            valid = bool(re.fullmatch(r"[0-9a-fA-F-]{16,64}", value))
        if valid:
            headers[name] = value
    category = {401: "authentication", 403: "forbidden", 429: "rate_limit"}.get(
        response.status_code, "http_server" if response.status_code >= 500 else "http_client"
    )
    source = "unknown"
    if body_format == "html" and (
        headers.get("server", "").lower().startswith(("cloudflare", "cloudfront"))
        or "cf-ray" in headers
    ):
        source = "cdn_waf_suspected"
    elif envelope:
        source = "api_envelope_suspected"
    result: dict[str, Any] = {
        "http_status": response.status_code,
        "category": category,
        "source": source,
        "response_format": body_format,
        "response_bytes": len(response.content),
        "response_sha256": hashlib.sha256(response.content).hexdigest(),
        "headers": headers,
    }
    content_type = response.headers.get("content-type", "").split(";")[0].lower()
    if content_type in {"application/json", "text/html", "text/plain", "application/problem+json"}:
        result["content_type"] = content_type
    if code is not None and str(code) not in sensitive_values:
        result["api_code"] = code
    stream = response.extensions.get("network_stream")
    if stream is not None:
        try:
            result["destination_ip"] = str(
                ipaddress.ip_address(stream.get_extra_info("server_addr")[0])
            )
        except Exception:
            pass
    return result
