# Graph Report - chatwoot-zzap-integration  (2026-09-18)

## Corpus Check
- Corpus is ~24,999 words - fits in a single context window. You may not need a graph.

## Summary
- 667 nodes · 1854 edges · 32 communities (19 shown, 13 thin omitted)
- Extraction: 83% EXTRACTED · 17% INFERRED · 0% AMBIGUOUS · INFERRED: 320 edges (avg confidence: 0.93)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Repository Mappings
- Sync Job Processing
- Health and Webhooks
- Database Migrations
- Chatwoot API Client
- ZZap Messages and Fingerprints
- Attachments and Outbound Webhooks
- Deployment Architecture
- ZZap API Client
- ZZap Polling Scheduler
- Worker Locks and Retries
- Test Doubles
- Webhook Signature Security
- Worker Integration Tests
- Failure Handling Tests
- Migration Tests
- Rate Limiting
- Cleanup Test Fixtures
- Rate-Limited ZZap Client
- Lock Test Fixtures
- Test Statement Protocols
- Fingerprint Test Fixtures
- Container Entrypoint
- Project Overview

## God Nodes (most connected - your core abstractions)
1. `SyncJob` - 68 edges
2. `JobStatus` - 39 edges
3. `JobType` - 36 edges
4. `MessageMapping` - 31 edges
5. `ChatwootClient` - 25 edges
6. `ZZapThread` - 25 edges
7. `Settings` - 25 edges
8. `ZZapActionQueue` - 23 edges
9. `process_claimed_job()` - 22 edges
10. `MessageStatus` - 21 edges

## Surprising Connections (you probably didn't know these)
- `test_claim_index_matches_global_claim_query_shape()` --uses--> `SyncJob`  [INFERRED]
  tests/unit/test_job_claiming.py → app/db/models.py
- `PostgreSQL Stateful Dependency` --semantically_similar_to--> `Durable Sync Job Queue`  [INFERRED] [semantically similar]
  README.md → docs/superpowers/specs/2026-07-03-zzap-chatwoot-integration-design.md
- `Global ZZap Rate Limit` --semantically_similar_to--> `Shared FIFO ZZap Rate Limiter`  [INFERRED] [semantically similar]
  README.md → docs/superpowers/specs/2026-07-03-zzap-chatwoot-integration-design.md
- `_FakeChatwootClient` --uses--> `ChatwootContactDto`  [INFERRED]
  tests/unit/test_inbound_service.py → app/clients/chatwoot.py
- `test_outbound_echo_is_not_imported_as_inbound_message()` --uses--> `ZZapMessageDto`  [INFERRED]
  tests/unit/test_job_retry.py → app/clients/zzap.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **PostgreSQL Backed Runtime Topology** — readme_postgresql_stateful_dependency, docker_compose_postgresql_service, docs_superpowers_specs_2026_07_03_zzap_chatwoot_integration_design_durable_sync_jobs [INFERRED 0.85]
- **Bidirectional Sync Implementation** — docs_superpowers_specs_2026_07_03_zzap_chatwoot_integration_design_bidirectional_sync, docs_superpowers_plans_2026_07_03_zzap_chatwoot_integration_implementation_inbound_pipeline, docs_superpowers_plans_2026_07_03_zzap_chatwoot_integration_implementation_outbound_pipeline [EXTRACTED 1.00]

## Communities (32 total, 13 thin omitted)

### Community 0 - "Repository Mappings"
Cohesion: 0.06
Nodes (65): ChatwootContact, MessageDirection, MessageMapping, MessageStatus, StrEnum, build_claim_job_statement(), claim_next_job(), create_chatwoot_contact_mapping() (+57 more)

### Community 1 - "Sync Job Processing"
Cohesion: 0.06
Nodes (49): JobStatus, JobType, SyncJob, build_zzap_outbound_message(), OutboundProcessor, _clear_job_lock(), _mark_inbound_mapping_failed(), _mark_job_failed_or_pending() (+41 more)

### Community 2 - "Health and Webhooks"
Cohesion: 0.06
Nodes (69): check_readiness(), health(), Any, NamedDependency, Response, ready(), _state_auth_failed(), ChatwootWebhookController (+61 more)

### Community 3 - "Database Migrations"
Cohesion: 0.05
Nodes (51): alembic, do_run_migrations(), get_url(), run_async_migrations(), run_migrations_offline(), run_migrations_online(), app_db, get_alembic_database_url() (+43 more)

### Community 4 - "Chatwoot API Client"
Cohesion: 0.08
Nodes (27): ChatwootApiError, ChatwootClient, ChatwootContactDto, _contact_from_payload(), _contact_object(), _ensure_content_length_allowed(), Any, AsyncClient (+19 more)

### Community 5 - "ZZap Messages and Fingerprints"
Cohesion: 0.11
Nodes (48): ZZapMessageDto, ZZapThread, build_zzap_fingerprint(), MessageFingerprint, normalize_message_text(), parse_zzap_datetime(), datetime, sha256_hex() (+40 more)

### Community 6 - "Attachments and Outbound Webhooks"
Cohesion: 0.12
Nodes (31): AttachmentTooLargeError, ensure_attachment_size(), ValueError, _attachment_file_name(), _attachment_payloads(), ChatwootWebhookDecision, classify_chatwoot_message_created(), _file_extension() (+23 more)

### Community 7 - "Deployment Architecture"
Cohesion: 0.08
Nodes (33): All Mode Application Service, Local Compose Stack, PostgreSQL Service, Production Compose Stack, Web Runtime Service, Worker Runtime Service, Atomic Job Claim Implementation, Container Deployment Task (+25 more)

### Community 8 - "ZZap API Client"
Cohesion: 0.16
Nodes (16): Any, AsyncClient, datetime, RuntimeError, _result_data(), _result_object(), ZZapApiError, ZZapClient (+8 more)

### Community 9 - "ZZap Polling Scheduler"
Cohesion: 0.18
Nodes (10): StrEnum, ZZapAction, ZZapActionQueue, ZZapActionType, collections, test_existing_zzap_actions_are_paused_during_backoff(), test_summary_poll_can_be_delayed_after_error(), test_summary_poll_is_coalesced() (+2 more)

### Community 10 - "Worker Locks and Retries"
Cohesion: 0.24
Nodes (12): app_workers, timedelta, retry_delay_for_attempt(), AsyncSession, release_worker_advisory_lock(), try_worker_advisory_lock(), sqlalchemy_ext_asyncio, test_inbound_retry_schedule() (+4 more)

### Community 11 - "Test Doubles"
Cohesion: 0.15
Nodes (5): _ClockedZZapApiClient, _FakeZZapApiClient, _MutableClock, datetime, test_rate_limited_zzap_client_waits_between_requests()

### Community 12 - "Webhook Signature Security"
Cohesion: 0.38
Nodes (10): ValueError, verify_chatwoot_signature(), WebhookSignatureError, hashlib, hmac, _signature(), test_verify_chatwoot_signature_accepts_valid_signature(), test_verify_chatwoot_signature_rejects_invalid_signature() (+2 more)

### Community 13 - "Worker Integration Tests"
Cohesion: 0.20
Nodes (8): _async_noop(), _FakeSettings, MonkeyPatch, test_outbound_echo_is_not_imported_as_inbound_message(), fake_known_outbound_echo_guards(), test_periodic_cleanup_runs_only_after_daily_interval(), fake_persist_inbound_message_job(), test_zzap_polling_limiter_waits_after_request_finishes()

### Community 14 - "Failure Handling Tests"
Cohesion: 0.20
Nodes (4): _FailingZZapClient, _FakeSessionScope, _FakeThread, test_failed_thread_fetch_is_requeued_after_zzap_error()

### Community 15 - "Migration Tests"
Cohesion: 0.25
Nodes (7): importlib_util, ModuleType, pathlib, sys, _load_initial_migration(), test_initial_migration_enum_types_are_not_auto_created_by_tables(), types

### Community 16 - "Rate Limiting"
Cohesion: 0.43
Nodes (3): ZZapRateLimiter, test_rate_limiter_first_request_is_ready(), test_rate_limiter_waits_between_requests()

### Community 17 - "Cleanup Test Fixtures"
Cohesion: 0.29
Nodes (4): _FakeCleanupSession, _CompilableStatement, _RowcountResult, test_cleanup_deletes_old_records_and_preserves_cursor_guards()

### Community 20 - "Test Statement Protocols"
Cohesion: 0.50
Nodes (3): _CompilableStatement, Any, Protocol

## Knowledge Gaps
- **9 isolated node(s):** `chatwoot-zzap-integration`, `docker-entrypoint.sh script`, `Message Fingerprint Idempotency`, `PostgreSQL Schema and Alembic`, `Atomic Job Claim Implementation` (+4 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 171 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **13 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `SyncJob` connect `Sync Job Processing` to `Repository Mappings`, `Database Migrations`, `ZZap Messages and Fingerprints`, `Attachments and Outbound Webhooks`, `Worker Locks and Retries`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **Why does `ChatwootClient` connect `Chatwoot API Client` to `Health and Webhooks`, `ZZap Messages and Fingerprints`?**
  _High betweenness centrality (0.062) - this node is a cross-community bridge._
- **Why does `Settings` connect `Health and Webhooks` to `ZZap Messages and Fingerprints`?**
  _High betweenness centrality (0.044) - this node is a cross-community bridge._
- **Are the 41 inferred relationships involving `SyncJob` (e.g. with `build_claim_job_statement()` and `claim_next_job()`) actually correct?**
  _`SyncJob` has 41 INFERRED edges - model-reasoned connections that need verification._
- **Are the 29 inferred relationships involving `JobStatus` (e.g. with `build_claim_job_statement()` and `claim_next_job()`) actually correct?**
  _`JobStatus` has 29 INFERRED edges - model-reasoned connections that need verification._
- **Are the 27 inferred relationships involving `JobType` (e.g. with `create_inbound_sync_job()` and `create_outbound_sync_job()`) actually correct?**
  _`JobType` has 27 INFERRED edges - model-reasoned connections that need verification._
- **Are the 18 inferred relationships involving `MessageMapping` (e.g. with `get_message_mapping_by_id()` and `has_chatwoot_message_mapping()`) actually correct?**
  _`MessageMapping` has 18 INFERRED edges - model-reasoned connections that need verification._