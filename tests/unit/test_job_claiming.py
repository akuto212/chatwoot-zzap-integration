from __future__ import annotations

from sqlalchemy.dialects import postgresql

from app.db.repositories import build_claim_job_statement


def _compile_statement() -> str:
    statement = build_claim_job_statement(worker_id="worker-1")
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ),
    )


def test_claim_job_statement_uses_skip_locked() -> None:
    compiled = _compile_statement()

    assert "FOR UPDATE" in compiled
    assert "SKIP LOCKED" in compiled
    assert "sync_jobs" in compiled


def test_claim_job_statement_selects_one_due_pending_job_in_fifo_order() -> None:
    compiled = _compile_statement()

    assert "sync_jobs.status = 'pending'" in compiled
    assert "sync_jobs.next_attempt_at IS NULL OR sync_jobs.next_attempt_at <=" in compiled
    assert (
        "ORDER BY sync_jobs.next_attempt_at ASC NULLS FIRST, sync_jobs.created_at ASC"
        in compiled
    )
    assert "LIMIT 1" in compiled
