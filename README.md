# ZZap Chatwoot Integration

Litestar microservice for bidirectional message sync between ZZap and a self-hosted Chatwoot instance.

The service imports new ZZap messages into Chatwoot, sends public outgoing Chatwoot operator messages back to ZZap, and uploads Chatwoot attachments to ZZap before sending them as file links. PostgreSQL is the only stateful dependency; durable jobs, idempotency records, mappings, polling cursors, and readiness state are stored there.

## Runtime Modes

- `web`: serves `/health`, `/ready`, and the Chatwoot webhook endpoint.
- `worker`: polls ZZap and processes durable jobs.
- `all`: runs web and worker in one process for local Docker Compose usage.

Production should run separate `web` and `worker` containers against the same PostgreSQL database. The worker uses a PostgreSQL advisory lock so only one active worker polls ZZap and processes jobs.

## Environment

Create a `.env` from `.env.example` and set real secrets/IDs.

Required variables:

```dotenv
APP_MODE=all
DATABASE_URL=postgresql+asyncpg://postgres:postgres@db:5432/chatwoot_zzap
INTEGRATION_ID=11111111-1111-4111-8111-111111111111
ZZAP_BASE_URL=https://b52-api.zzap.pro
ZZAP_API_KEY=replace-me
CHATWOOT_BASE_URL=https://chatwoot.example.com
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_INBOX_ID=1
CHATWOOT_API_TOKEN=replace-me
CHATWOOT_WEBHOOK_SECRET=replace-me
```

Optional variables:

```dotenv
MAX_ATTACHMENT_BYTES=10485760
SUCCESSFUL_MESSAGE_RETENTION_DAYS=60
FAILED_RECORD_RETENTION_DAYS=30
WEBHOOK_DELIVERY_RETENTION_DAYS=30
```

`INTEGRATION_ID` is a stable UUID for this ZZap to Chatwoot binding. The first release supports one active binding configured through environment variables.

## Local Start

```bash
docker compose up --build
```

The container entrypoint runs:

```bash
uv run alembic upgrade head
```

before starting the selected runtime mode. If migrations fail, the container exits.

Local endpoints:

- `GET http://localhost:8000/health`
- `GET http://localhost:8000/ready`
- `POST http://localhost:8000/webhooks/chatwoot`

## Chatwoot Webhook

Configure a Chatwoot webhook for `message_created` events:

```text
https://your-service.example.com/webhooks/chatwoot
```

The webhook must include Chatwoot HMAC headers:

- `X-Chatwoot-Signature`
- `X-Chatwoot-Timestamp`
- `X-Chatwoot-Delivery`

The service verifies the signature with `CHATWOOT_WEBHOOK_SECRET` over `timestamp + "." + raw_body` and expects the signature format `sha256=<hex>`. Invalid signatures return `403`.

Only public outgoing operator messages for `CHATWOOT_INBOX_ID` are sent to ZZap. Private notes, system/bot messages, incoming/imported messages, wrong inbox events, and unknown conversation mappings are ignored with `200 OK`.

## ZZap Rate Limit

All ZZap API calls share one global limiter: at most one request every 3 seconds. This includes:

- summary polling;
- per-thread message fetches;
- file uploads;
- outbound message sends.

Summary polling is scheduled every 3 seconds as a target, not a guarantee. If fetches, uploads, or outbound sends are queued, polling waits in the same FIFO queue. On ZZap `401`, polling backs off to rare retries and `/ready` becomes unhealthy. On `429`, rate-limit, or captcha-like responses, the scheduler backs off while readiness stays healthy.

## Known Limitations

- Only the first ZZap thread page is tracked: `page=1&page_size=100`.
- ZZap messages do not expose stable message IDs, so deduplication uses a synthetic fingerprint.
- Exact duplicate messages from the same sender in the same second may be treated as duplicates.
- Old read ZZap history is not imported.
- Read/unread state is not synchronized.
- Chatwoot edits and deletes are not synchronized.
- ZZap edits and deletes are not synchronized.
- ZZap to Chatwoot attachments are not converted to native Chatwoot attachments; links remain text.
- Chatwoot reconciliation polling is not implemented in the first release.

## Tests

```bash
rtk env UV_CACHE_DIR=/tmp/uv-cache uv run ruff check .
rtk env UV_CACHE_DIR=/tmp/uv-cache uv run mypy app
rtk env UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q
```
