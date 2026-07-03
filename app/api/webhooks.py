from __future__ import annotations

import time
from typing import Any

from litestar import Controller, Request, Response, post
from litestar.exceptions import HTTPException
from litestar.status_codes import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_500_INTERNAL_SERVER_ERROR

from app.db.session import create_engine, create_session_factory, session_scope
from app.services.outbound import (
    ChatwootWebhookDecision,
    classify_chatwoot_message_created,
    persist_outbound_webhook_event,
)
from app.services.webhooks import WebhookSignatureError, verify_chatwoot_signature
from app.settings import Settings


class ChatwootWebhookController(Controller):
    path = "/webhooks/chatwoot"

    @post()
    async def receive(self, request: Request, settings: Settings) -> Response[dict[str, str]]:
        raw_body = await request.body()
        try:
            verify_chatwoot_signature(
                raw_body=raw_body,
                timestamp=request.headers.get("X-Chatwoot-Timestamp"),
                signature=request.headers.get("X-Chatwoot-Signature"),
                secret=settings.chatwoot_webhook_secret,
                now_seconds=int(time.time()),
            )
        except WebhookSignatureError as exc:
            raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="invalid signature") from exc

        payload = await request.json()
        if not isinstance(payload, dict):
            return Response({"status": "ignored"}, status_code=HTTP_200_OK)
        decision = classify_chatwoot_message_created(
            payload,
            settings.chatwoot_inbox_id,
        )
        if decision == ChatwootWebhookDecision.IGNORE:
            return Response({"status": "ignored"}, status_code=HTTP_200_OK)

        try:
            created = await self._persist_event(request, payload, settings)
        except Exception as exc:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed to persist outbound job",
            ) from exc

        return Response({"status": "accepted" if created else "duplicate"}, status_code=HTTP_200_OK)

    async def _persist_event(
        self,
        request: Request,
        payload: dict[str, Any],
        settings: Settings,
    ) -> bool:
        session_factory = request.app.state.get("session_factory")
        if session_factory is None:
            engine = create_engine(settings.database_url)
            session_factory = create_session_factory(engine)
            request.app.state["session_factory"] = session_factory

        async with session_scope(session_factory) as session:
            return await persist_outbound_webhook_event(
                session,
                payload=payload,
                delivery_id=request.headers.get("X-Chatwoot-Delivery"),
                integration_id=settings.integration_id,
            )
