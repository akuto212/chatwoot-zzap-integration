# ZZap Chatwoot Integration Design

Date: 2026-07-03

## Goal

Build a Litestar microservice that synchronizes messages between ZZap and a self-hosted Chatwoot instance.

The first release supports full bidirectional messaging:

- ZZap incoming messages are imported into Chatwoot.
- Public outgoing operator messages from Chatwoot are sent back to ZZap.
- Chatwoot attachments are uploaded to ZZap and sent as file links in the ZZap message text.

The service is intentionally minimalistic: PostgreSQL is the only stateful dependency besides Chatwoot and ZZap. Redis, Celery, and external message brokers are out of scope for the first release.

## External APIs

HTTP timeout defaults:

- 30 seconds for regular ZZap and Chatwoot API requests.
- 60 seconds for attachment download and upload operations.

ZZap API:

- Swagger: https://b52-api.zzap.pro/swagger/v1/swagger.json
- Auth: `zzap-api-key` header.
- Relevant endpoints:
  - `GET /api/client/v1/messages`: list user chat threads.
  - `GET /api/client/v1/messages/{user_key}`: list messages in one chat.
  - `POST /api/client/v1/messages`: send a message.
  - `POST /api/client/v1/upload`: upload a file and receive a file URL.

Chatwoot:

- Self-hosted instance.
- Standard API token authentication.
- Uses an existing Chatwoot inbox configured by `CHATWOOT_INBOX_ID`.
- Webhooks use Chatwoot HMAC verification with:
  - `X-Chatwoot-Signature`
  - `X-Chatwoot-Timestamp`
  - `X-Chatwoot-Delivery`

## Architecture

The service is one Python/Litestar project with three runtime modes:

- `web`: Litestar HTTP API, `/health`, `/ready`, Chatwoot webhook endpoint.
- `worker`: ZZap polling, durable job processing, retry, cleanup.
- `all`: web and worker in one process for local development.

Production deployment uses separate `web` and `worker` containers sharing one PostgreSQL database. The worker acquires a PostgreSQL advisory lock before starting polling and job processing. If another worker already holds the lock, the extra worker does not process jobs or poll ZZap.

PostgreSQL is the source of truth for:

- ZZap to Chatwoot mappings.
- Message deduplication.
- Webhook delivery deduplication.
- Durable jobs and retry state.
- Cross-process service state used by `/ready`.

No external queue is used. The `sync_jobs` table is the durable outbox/job queue.

Planned package structure:

- `app/api`: Litestar routes, webhook, health, readiness.
- `app/clients`: ZZap and Chatwoot HTTP clients.
- `app/db`: SQLAlchemy models, sessions, repositories, Alembic metadata.
- `app/services`: sync pipelines, idempotency, fingerprinting, attachment handling.
- `app/workers`: ZZap scheduler, job runner, cleanup, advisory lock.
- `app/settings`: typed environment settings.

## Configuration

Configuration is read from environment variables through typed settings.

Docker Compose may use `.env` locally. The repository should include `.env.example` without secrets. Secrets must not be logged.

Core settings:

- `APP_MODE`: `web`, `worker`, or `all`.
- `DATABASE_URL`
- `INTEGRATION_ID`: UUID identifying the configured integration binding.
- `ZZAP_BASE_URL`
- `ZZAP_API_KEY`
- `CHATWOOT_BASE_URL`
- `CHATWOOT_ACCOUNT_ID`
- `CHATWOOT_INBOX_ID`
- `CHATWOOT_API_TOKEN`
- `CHATWOOT_WEBHOOK_SECRET`
- `MAX_ATTACHMENT_BYTES`: default 10 MB.
- Successful message mapping retention: default 60 days.
- Failed jobs/message records retention: default 30 days.
- Webhook delivery retention: default 30 days.

First release supports one active ZZap to Chatwoot binding from env. Tables still include `integration_id` so multiple bindings can be added later without reshaping every table. There is no `integrations` table in the first release.

## Docker And Migrations

The project uses:

- Python 3.14.
- `uv` for dependencies and lockfile.
- Litestar.
- SQLAlchemy async ORM, with Core expressions for lock/job operations.
- Alembic for migrations.

Required Docker artifacts:

- `Dockerfile`
- local `docker-compose.yml` with app and PostgreSQL
- production-oriented Compose example with separate `web` and `worker`

The container entrypoint runs `alembic upgrade head` before starting the selected app mode. If migrations fail, the container exits immediately.

## ZZap To Chatwoot Flow

The worker polls ZZap with an adaptive summary-first scheduler.

1. Every 3 seconds, if no summary poll is already pending, the worker schedules `GET /api/client/v1/messages?page=1&page_size=100`.
2. All ZZap requests, including uploads and outbound sends, pass through one global rate limiter: not more than one request every 3 seconds.
3. Only the first ZZap thread page is tracked in the first release.
4. Each returned thread updates `zzap_threads` with:
   - `user_name`
   - `message_last_date`
   - `message_last_hash`
   - `unread_count`
   - `read_only`
   - polling state
5. A thread is considered changed if any of these changed:
   - `message_last_date`
   - `message_last_hash`
   - `unread_count`
6. Changed threads schedule one fetch action per thread. Duplicate pending fetches for the same thread are coalesced.
7. Thread messages are fetched through `GET /api/client/v1/messages/{user_key}` with:
   - `page_size = min(100, max(20, unread_count + 5))`
8. Fetched messages are normalized and imported in chronological order. If timestamps are equal, the original response index is used as a tie-breaker. The actual ZZap response ordering must be verified during integration testing.
9. New messages are detected through cursor plus fingerprint overlap.
10. For every new message fingerprint, the service creates an inbound job.
11. The inbound job creates the Chatwoot contact and conversation if needed, reopens the conversation as `open`, and creates an incoming Chatwoot message.
12. After successful delivery, message text and temporary payload data are cleared. Hashes, fingerprints, external IDs, and status remain.

Bootstrap behavior:

- Threads without `unread_count` are recorded as baseline only. No Chatwoot contact or conversation is created.
- Threads with `unread_count > 0` are treated as current unread work and imported.
- Old read history is not imported.

ZZap timestamps without timezone are interpreted as `Europe/Moscow` and stored as timezone-aware timestamps.

## Chatwoot To ZZap Flow

The first release uses Chatwoot webhooks only. The internal outbound pipeline is designed so future Chatwoot reconciliation polling can be added as another producer without rewriting send logic.

1. Chatwoot sends `message_created` to the Litestar webhook endpoint.
2. The endpoint verifies HMAC using the raw request body, timestamp, signature, and `CHATWOOT_WEBHOOK_SECRET`.
3. Invalid signatures return `403 Forbidden`.
4. Valid but irrelevant events return `200 OK` without creating jobs.
5. Duplicate webhook deliveries or duplicate `chatwoot_message_id` values return `200 OK` without creating jobs.
6. Relevant public outgoing operator messages are stored as outbound jobs and the endpoint quickly returns success.
7. The worker processes outbound jobs asynchronously.
8. Private notes, system messages, bot messages, imported ZZap messages, wrong inbox events, and unknown conversation mappings are ignored.
9. If the Chatwoot message has attachments, the worker downloads each file, checks `MAX_ATTACHMENT_BYTES`, uploads it to ZZap through `/upload`, and stores returned URLs temporarily in the job payload for retry.
10. The worker sends one ZZap message through `POST /api/client/v1/messages`.
11. The ZZap message body is the operator text followed by file URLs. If the Chatwoot message only contains attachments, only file URLs are sent.
12. `is_online` is always `true`.
13. `message_date` is the Chatwoot message creation time.
14. After successful send, temporary text, attachment metadata, and URLs are cleared.

All ZZap uploads and sends share the global ZZap rate limiter.

If one attachment upload fails, the final ZZap message is not sent. The job retries from its saved state and reuses already uploaded URLs when available. If retries are exhausted, the operator receives a private note with a short error reason. Already uploaded file URLs are not shown in that note.

## Chatwoot Entity Model

Mapping rules:

- One ZZap chat/user maps to one Chatwoot contact.
- One ZZap chat maps to one Chatwoot conversation.
- Chatwoot contact and conversation are created only when a new ZZap message needs to be imported.
- The service does not search existing Chatwoot contacts.
- Contact name is ZZap `user_name`; fallback is `ZZap <user_key_short>`.
- Contact custom attributes:
  - `source=zzap`
  - `zzap_user_key=<user_key>`
- No Chatwoot labels are assigned.
- No conversation custom attributes are written.
- Conversations are created or reopened as `open`.
- If a Chatwoot conversation is closed and an operator sends a public outgoing message, it is still sent to ZZap.

If the ZZap thread is `read_only=true`, outbound Chatwoot messages are not sent. The job is marked blocked, and the service creates a private note in the Chatwoot conversation explaining that the ZZap thread is read-only.

## Database Design

All main tables use PostgreSQL `uuid` primary keys and include `integration_id`.

Tables:

- `zzap_threads`
  - Technical state for ZZap thread summary.
  - Stores `user_key`, `user_name`, `message_last_date`, `message_last_hash`, `unread_count`, `read_only`, polling state.
  - Does not store full last message text.
- `chatwoot_contacts`
  - Maps `(integration_id, zzap_user_key)` to `chatwoot_contact_id`.
- `chatwoot_conversations`
  - Maps ZZap thread/contact to `chatwoot_conversation_id`.
- `message_mappings`
  - Stores direction, message fingerprint, message hash, external message IDs when available, status, and timestamps.
  - Successful records are retained by configurable retention, default 60 days.
- `sync_jobs`
  - Universal durable job table.
  - Indexed fields for job type, status, next attempt, external IDs, mappings.
  - JSONB payload only for temporary job details.
- `webhook_deliveries`
  - Stores delivery ID, event name, received time, optional Chatwoot message ID.
  - Does not store raw payload.
- `service_state`
  - Stores cross-process readiness and worker state:
    - current ZZap auth/status state
    - current Chatwoot auth/status state
    - last successful poll
    - last worker heartbeat
    - last error metadata

`sync_jobs.job_type` values:

- `inbound_zzap_message_to_chatwoot`
- `outbound_chatwoot_message_to_zzap`
- `chatwoot_private_note`

`sync_jobs.status` values:

- `pending`
- `processing`
- `succeeded`
- `failed`
- `ignored`
- `blocked`

Retrying jobs remain `pending` with `attempt_count > 0` and `next_attempt_at`.

The worker processes one job per cycle, FIFO by `next_attempt_at` and then `created_at`.

## Idempotency And Fingerprints

ZZap message IDs are not available in the swagger schema. The service therefore uses a synthetic fingerprint.

Message text normalization for hashes:

- Unicode normalization: NFC.
- Convert `\r\n` and `\r` to `\n`.
- Do not trim.
- Do not collapse whitespace.
- Do not change case.

The service stores:

- `message_hash = sha256(normalized_message_text)`
- `fingerprint = sha256(integration_id + zzap_thread_user_key + sender_user_key + message_date + message_hash)`

The original text may be cleared after successful delivery, but hashes and fingerprints are retained for deduplication.

This cannot guarantee perfect deduplication if the same sender sends the exact same text twice in the same second in the same ZZap thread. This is an accepted limitation because ZZap does not expose a stable message ID.

Chatwoot webhook deduplication uses two layers:

- `X-Chatwoot-Delivery`, when present.
- `chatwoot_message_id` for `message_created`.

Raw webhook payload is never stored.

## Rate Limiting And Scheduling

ZZap has a strict global request limit for this integration: not more than one request every 3 seconds.

All ZZap actions go through the same FIFO limiter:

- summary polling
- thread message fetches
- file uploads
- outbound message sends

Polling actions are coalesced:

- At most one pending summary poll.
- At most one pending thread fetch per ZZap thread.

Outbound ZZap sends and ZZap uploads are not coalesced. Chatwoot private note jobs are not coalesced either, but they do not use the ZZap rate limiter.

The first release does not run direct polling for active Chatwoot conversations. Summary-first polling is considered sufficient.

## Error Handling

Retry policies:

- Outbound Chatwoot to ZZap:
  - 3 attempts
  - backoff: 1 minute, 5 minutes, 15 minutes
  - after exhaustion: job `failed`, create Chatwoot private note with a short reason
- Inbound ZZap to Chatwoot:
  - 5 attempts
  - backoff: 10 seconds, 30 seconds, 1 minute, 5 minutes, 15 minutes
  - after exhaustion: job `failed`, structured log only, no private note

ZZap errors:

- `401`: worker keeps running but polling moves to rare retry, approximately every 5 minutes. State is written to `service_state`; `/ready` becomes unhealthy.
- `429`, rate-limit, or captcha-like response: scheduler increases backoff; `/ready` remains healthy.
- `5xx`, timeout, network errors: scheduler/jobs retry with backoff and structured logs.

Chatwoot errors:

- `401` or `403`: relevant jobs fail after their retry policy; `/ready` becomes unhealthy because token/access is broken.
- `5xx`, timeout, network errors: jobs retry according to policy.
- Private note creation errors are logged but do not change the original job result.

Webhook responses:

- Invalid HMAC: `403`.
- Valid but irrelevant event: `200 OK`.
- Duplicate delivery/message: `200 OK`.
- Relevant event cannot be persisted to PostgreSQL: `500`, so Chatwoot can retry.

## Health And Readiness

`/health`:

- Simple liveness.
- Does not check external dependencies.

`/ready`:

- Checks PostgreSQL availability.
- Checks required config is valid.
- Checks current auth failure state from `service_state`.
- Unhealthy on actual ZZap `401`.
- Unhealthy on actual Chatwoot auth failure.
- Healthy during temporary external `5xx`, timeout, or backoff states.

## Retention And Cleanup

Cleanup runs:

- once when the worker starts;
- then once per day.

Retention:

- Successful message mappings: configurable, default 60 days.
- Failed jobs/message records: default 30 days.
- Webhook deliveries: configurable, default 30 days.

Cleanup is a periodic worker function, not a `sync_jobs` job.

## Logging And Privacy

The first release uses structured logs in stdout.

Logs should include useful technical identifiers:

- `integration_id`
- job ID
- ZZap user/thread key where appropriate
- Chatwoot conversation/message ID where appropriate
- external API status/error class

Do not log:

- secrets;
- raw webhook payloads;
- full message text after it is no longer needed;
- attachment URLs after successful delivery.

Message text and attachment metadata are stored only while needed for pending/failed retry. After successful delivery, temporary payload fields are cleared.

## Tests

First release testing scope:

- Unit tests for scheduler/rate limiter/coalescing.
- Unit tests for fingerprinting and idempotency.
- Unit tests for Chatwoot webhook HMAC verification.
- Unit tests for webhook filtering and deduplication.
- Unit tests for job retry/status transitions.
- Unit tests with mocked ZZap and Chatwoot HTTP clients.

No tests call real ZZap or Chatwoot APIs.

Tooling:

- `pytest`
- `ruff`
- `mypy` in minimal mode

## Known Limitations

- Only the first ZZap thread page is tracked: `page=1&page_size=100`.
- ZZap messages do not expose stable message IDs, so deduplication uses a synthetic fingerprint.
- Exact duplicate messages from the same sender in the same second may be treated as duplicates.
- Old read ZZap history is not imported.
- Read/unread state is not synchronized.
- Chatwoot edits and deletes are not synchronized.
- ZZap edits and deletes are not synchronized.
- ZZap to Chatwoot attachments are not converted to native Chatwoot attachments. Links remain text.
- Chatwoot reconciliation polling is not implemented in the first release, but the outbound pipeline is designed for it.

## Future Extensions

Potential future work:

- Track additional ZZap thread pages beyond the first 100.
- Add Chatwoot reconciliation polling for missed outgoing messages.
- Add multiple integration bindings with an `integrations` table.
- Add metrics or an admin status endpoint.
- Add optional contact profile enrichment from `GET /api/client/v1/user/{user_key}`.
- Convert detected ZZap file links into Chatwoot native attachments.
