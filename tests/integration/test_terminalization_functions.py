"""The database-owned terminalization operations, against a real server.

Every outcome the wire contract defines is produced here by putting a row into
the state that produces it, rather than by asserting that a function returns
what its body says. The refusals matter more than the successes: a transition
that quietly does nothing and a transition that correctly reports it lost a
race look identical from the caller's side unless the outcome says which.

The equivalence-class cases are the ones with real consequences. Five
operations write CANCELLED and two write COMPLETED, so "already terminal at my
target status" cannot distinguish this operation's own committed work from
somebody else's — and a caller told its work was already done will not repeat
the coupled write that should have accompanied it.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Final

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.elements import TextClause

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from tests.attempt_snapshot_support import encode_attempt_snapshot
from tests.integration.history_seeding import read_attempt_history

from horsies.core.lifecycle.commands import (
    AbandonNodesOfPausedWorkflows,
    AbandonOwnedNode,
    AbandonOwnedNodes,
    CancelNodesOfCancelledWorkflow,
    CancelOrphanedTasks,
    CancelOwnedNode,
    CancelOwnedNodes,
    CancelOwnedOrphan,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
)
from horsies.core.lifecycle.fences import (
    OwnedClaim,
    OwnedClaimBatch,
    WorkerOwned,
)
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedClaim,
    ObservedDeadline,
    ObservedForeignTerminalization,
    ObservedStaleness,
    ObservedWorkflowLink,
    SourceStateConflict,
    TaskAbsent,
    TerminalizationOutcome,
    decode_outcome_row,
)
from horsies.core.lifecycle.persistence import (
    apply_async,
    apply_batch_async,
    apply_sync,
    batch_call_for,
    call_for,
)
from horsies.core.types.status import TaskStatus
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

WORKER = 'terminalization-test-worker'
GENERATION = datetime(2026, 8, 4, 9, 0, tzinfo=timezone.utc)

@pytest_asyncio.fixture(autouse=True)
async def _schema(broker: PostgresBroker) -> None:
    """The operations ship with the schema, so the schema has to be applied.

    Depending on the broker fixture is what installs them: these functions are
    part of a migration, not a fixture of their own.
    """
    return None


# Every baseline column is named. A sent task carries a command fingerprint;
# a workflow task carries none. A CLAIMED row carries a lease. A row with a
# worker carries the worker identity that the start write records.
_SEED_SQL = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at,
        command_fingerprint_version, command_fingerprint,
        idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname,
        worker_process_name, retry_count, max_retries, next_retry_at,
        good_until, is_workflow_task, updated_at, retain_rerun_input,
        rerun_of_task_id, rerun_root_task_id
    )
    VALUES (
        CAST(:id AS uuid), 'terminalization.test', 'default', 100, '{}', NULL,
        'ISOLATED',
        :retention_class_key, NOW(), NOW(),
        CASE WHEN :is_workflow_task THEN NULL ELSE 1 END,
        CASE
            WHEN :is_workflow_task THEN NULL
            ELSE sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8'))
        END,
        NULL, CAST(:status AS text), NOW(), :claimed_at,
        CAST(:worker_id AS text),
        CASE
            WHEN CAST(:status AS text) = 'CLAIMED'
            THEN NOW() + INTERVAL '5 minutes'
        END,
        NOW(), NULL,
        NULL,
        CASE WHEN CAST(:worker_id AS text) IS NOT NULL THEN 4242 END,
        CASE
            WHEN CAST(:worker_id AS text) IS NOT NULL
            THEN 'terminalization-host'
        END,
        CASE
            WHEN CAST(:worker_id AS text) IS NOT NULL
            THEN 'terminalization-proc'
        END,
        0, 0, NULL,
        NULL, :is_workflow_task, NOW(), FALSE,
        NULL, NULL
    )
""")


async def _seed(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    worker_id: str | None = WORKER,
    claimed_at: datetime | None = GENERATION,
    is_workflow_task: bool = False,
) -> str:
    task_id = str(uuid.uuid4())
    await session.execute(
        _SEED_SQL,
        {
            'id': task_id,
            'status': status,
            'worker_id': worker_id,
            'claimed_at': claimed_at,
            'is_workflow_task': is_workflow_task,
            'retention_class_key': DEFAULT_RETENTION_CLASS_KEY,
        },
    )
    await session.commit()
    return task_id


async def _requeued_after_failed_attempt(
    session: AsyncSession,
    task_id: str,
    *,
    failed_reason: str,
) -> None:
    """Leave the row and history where an automatic retry leaves them.

    The retry path records attempt 1 as FAILED with will_retry and advances
    retry_count. The attempt row keeps its failed_reason; the terminal
    writers must not carry it into the terminal record. The live row
    stores no failed reason.
    """
    await session.execute(
        text("""
            INSERT INTO horsies_task_attempts (
                task_id, attempt, outcome, will_retry,
                started_at, finished_at,
                error_code, error_message, failed_reason, worker_id,
                worker_hostname, worker_pid, worker_process_name
            )
            VALUES (
                :id, 1, 'FAILED', TRUE,
                NOW() - INTERVAL '2 minutes', NOW() - INTERVAL '1 minute',
                'TASK_EXCEPTION', 'attempt one raised', :reason, :worker,
                'terminalization-test-host', 4242, 'terminalization-test-proc'
            )
        """),
        {'id': task_id, 'reason': failed_reason, 'worker': WORKER},
    )
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET retry_count = 1
            WHERE id = :id
        """),
        {'id': task_id},
    )
    await session.commit()


# Attempts purge with the move; `read_attempt_history` follows them
# into the history row's snapshot.


async def _force_terminal(
    session: AsyncSession,
    task_id: str,
    *,
    status: str,
    kind: str,
) -> None:
    """Put the task where another operation would have left it.

    That is a history row carrying the operation's kind, with no live row
    and no live attempt rows.
    """
    snapshot = encode_attempt_snapshot(())
    await session.execute(
        text("""
            INSERT INTO horsies_task_history (
                task_id, task_name, queue_name, priority,
                command_fingerprint_version, command_fingerprint,
                status, terminalization_kind, terminal_at,
                retention_class_key, sent_at,
                enqueued_at, claimed_at, started_at, created_at,
                retry_count, max_retries,
                last_claimed_worker_id, last_worker_hostname,
                result_envelope_version,
                error_code, final_failed_reason,
                is_workflow_task, history_schema_version,
                attempt_archive_version, attempt_snapshot,
                attempt_snapshot_digest, rerun_input_disposition
            )
            SELECT id, task_name, queue_name, priority,
                   command_fingerprint_version, command_fingerprint,
                   :status, :kind, NOW(),
                   retention_class_key, sent_at,
                   enqueued_at, claimed_at, started_at, created_at,
                   retry_count, max_retries,
                   claimed_by_worker_id, worker_hostname,
                   1,
                   NULL, NULL,
                   is_workflow_task, 1,
                   :snapshot_version, :snapshot_payload,
                   :snapshot_digest, 'NEVER_ELIGIBLE'
            FROM horsies_tasks WHERE id = CAST(:id AS uuid)
        """),
        {
            'status': status,
            'kind': kind,
            'id': task_id,
            'snapshot_version': snapshot.version,
            'snapshot_payload': snapshot.payload,
            'snapshot_digest': snapshot.digest,
        },
    )
    await session.execute(
        text(
            'DELETE FROM horsies_task_attempts '
            'WHERE task_id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    await session.execute(
        text('DELETE FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
        {'id': task_id},
    )
    await session.commit()


_OUTCOME_COLUMNS: Final = (
    'task_id, ordinality, outcome, terminal_at, terminalization_kind, '
    'observed_status, observed_worker_id, observed_claimed_at, guard_kind, '
    'observed_guard'
)
"""The ten outcome columns that the writers of 0004_lifecycle_functions.sql
return first; ``decode_outcome_row`` takes exactly these."""

_WORKFLOW_SUCCESS_SQL: Final = text(f"""
    SELECT {_OUTCOME_COLUMNS} FROM horsies_finish_workflow_success(
        CAST(:task_id AS uuid), :worker_id, CAST(:claimed_at AS TIMESTAMPTZ),
        :result
    )
""")

_FAILED_ATTEMPT_SQL: Final = text(f"""
    SELECT {_OUTCOME_COLUMNS} FROM horsies_finish_failed_attempt(
        CAST(:task_id AS uuid), :worker_id, CAST(:claimed_at AS TIMESTAMPTZ),
        'NONE', NULL, NULL, NULL, :result, :error_code, :failed_reason,
        'FAILED', NULL, :failed_reason, 'REFUSE', TRUE
    )
""")

_STALE_ATTEMPT_SQL: Final = text(f"""
    SELECT {_OUTCOME_COLUMNS} FROM horsies_finish_stale_attempt(
        CAST(:task_id AS uuid), CAST(:stale_after_ms AS INTEGER),
        CAST(:finalizing_stale_after_ms AS INTEGER), 'NONE', NULL, NULL,
        :result, :error_code, :failed_reason, :failed_reason, :failed_reason
    )
""")

_CANCEL_TASK_SQL: Final = text(f"""
    SELECT {_OUTCOME_COLUMNS} FROM horsies_cancel_task(
        CAST(:task_id AS uuid), CAST(:include_running AS BOOLEAN)
    )
""")


async def _outcome_of(
    session: AsyncSession,
    statement: TextClause,
    parameters: dict[str, object],
) -> TerminalizationOutcome:
    """Run one writer on the session's connection and decode its one row."""
    rows = (
        await (await session.connection()).execute(statement, parameters)
    ).mappings().all()
    assert len(rows) == 1, rows
    return decode_outcome_row({str(key): value for key, value in rows[0].items()})


async def _locked(
    session: AsyncSession,
    task_id: str,
    *,
    claimed_at: datetime = GENERATION,
) -> TerminalizationOutcome:
    """The writer of COMPLETE_LOCKED: phase 1 of a workflow task's success
    (``horsies_finish_workflow_success``) under ``WORKER`` at ``claimed_at``.
    A miss is classified before the function asks whether the task is a
    workflow task."""
    return await _outcome_of(
        session,
        _WORKFLOW_SUCCESS_SQL,
        {
            'task_id': task_id,
            'worker_id': WORKER,
            'claimed_at': claimed_at,
            'result': '{"ok": 1}',
        },
    )


async def _workflow_task(session: AsyncSession) -> str:
    """A RUNNING workflow task of ``WORKER`` with its RUNNING node."""
    task_id = await _seed(session, is_workflow_task=True)
    _ = await _link_workflow_task(session, task_id, node_status='RUNNING')
    return task_id


def _fused(task_id: str, claimed_at: datetime = GENERATION) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
        result_json='{"ok": 2}',
    )


class TestLiveStatusInvariant:
    async def test_the_live_table_refuses_a_terminal_status(
        self,
        session: AsyncSession,
    ) -> None:
        """A terminal task leaves the live table; the live row cannot hold a
        terminal status (CHECK horsies_tasks_status_live)."""
        task_id = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
        )

        with pytest.raises(IntegrityError, match='horsies_tasks_status_live'):
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = 'CANCELLED'
                    WHERE id = :id
                """),
                {'id': task_id},
            )
            await session.commit()
        await session.rollback()

        row = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'PENDING'


class TestAppliedTransitions:
    async def test_locked_completion_applies_and_stamps_its_kind(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _workflow_task(session)
        outcome = await _locked(session, task_id)
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.COMPLETE_LOCKED
        assert outcome.terminal_at is not None

    async def test_applied_reports_the_image_it_matched(
        self,
        session: AsyncSession,
    ) -> None:
        """Not decoration: the pre-transition image is what the guard saw."""
        task_id = await _workflow_task(session)
        outcome = await _locked(session, task_id)
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.observed.status is TaskStatus.RUNNING
        assert outcome.observed.worker_id == WORKER
        assert outcome.observed.claimed_at == GENERATION

    async def test_fused_completion_writes_its_attempt_row(
        self,
        session: AsyncSession,
    ) -> None:
        """The fusion exists to do this in one statement; it still does.

        The move purges live attempt rows into the history snapshot, so
        the fused attempt's evidence is read from there.
        """
        task_id = await _seed(session)
        outcome = await apply_async(await session.connection(), _fused(task_id))
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.COMPLETE_FUSED
        assert await read_attempt_history(session, task_id) == [
            (1, 'COMPLETED', None),
        ]

    async def test_transition_leaves_the_row_terminal_and_attributed(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _workflow_task(session)
        await _locked(session, task_id)
        await session.commit()

        row = (
            await session.execute(
                text("""
                    SELECT status, terminal_at, terminalization_kind, result
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'COMPLETED'
        assert row.terminal_at is not None
        assert row.terminalization_kind == 'COMPLETE_LOCKED'
        assert row.result == '{"ok": 1}'

    async def test_completion_clears_an_earlier_attempts_failure_summary_fused(
        self,
        session: AsyncSession,
    ) -> None:
        """Attempt one failed and requeued; attempt two completes.

        Both halves of the invariant: the terminal row contains only the
        final disposition — the failed_reason the automatic requeue left on
        the row is cleared beside COMPLETED — and attempt one's history row
        keeps its reason untouched. The fused operation records the
        completion as attempt two after attempt one's failure.
        """
        task_id = await _seed(session)
        await _requeued_after_failed_attempt(
            session, task_id, failed_reason='attempt one crashed',
        )

        outcome = await apply_async(await session.connection(), _fused(task_id))
        await session.commit()
        assert isinstance(outcome, Applied)

        await _assert_completed_after_attempt_one(session, task_id)

    async def test_completion_clears_an_earlier_attempts_failure_summary_locked(
        self,
        session: AsyncSession,
    ) -> None:
        """The same invariant for the writer of COMPLETE_LOCKED, phase 1 of a
        workflow task's success: it records the completion as attempt two
        too."""
        task_id = await _workflow_task(session)
        await _requeued_after_failed_attempt(
            session, task_id, failed_reason='attempt one crashed',
        )

        outcome = await _locked(session, task_id)
        await session.commit()
        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.COMPLETE_LOCKED

        await _assert_completed_after_attempt_one(session, task_id)


async def _assert_completed_after_attempt_one(
    session: AsyncSession,
    task_id: str,
) -> None:
    """The terminal row is COMPLETED with no failure summary; attempt one
    keeps its reason and the completion is attempt two."""
    row = (
        await session.execute(
            text("""
                SELECT status, error_code, failed_reason
                FROM itest_task_rows WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one()
    assert row.status == 'COMPLETED'
    assert row.error_code is None
    assert row.failed_reason is None

    attempts = await read_attempt_history(session, task_id)
    assert attempts == [
        (1, 'FAILED', 'attempt one crashed'),
        (2, 'COMPLETED', None),
    ]


class TestAlreadyApplied:
    async def test_replay_of_the_same_operation(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _workflow_task(session)
        await _locked(session, task_id)
        await session.commit()

        replay = await _locked(session, task_id)
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.COMPLETE_LOCKED

    async def test_the_other_completion_operation_counts_as_the_same_work(
        self,
        session: AsyncSession,
    ) -> None:
        """Fused and locked completion are one class: same effect, same row."""
        task_id = await _seed(session)
        await apply_async(await session.connection(), _fused(task_id))
        await session.commit()

        replay = await _locked(session, task_id)
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.COMPLETE_FUSED

    async def test_a_foreign_kind_is_a_conflict_not_a_replay(
        self,
        session: AsyncSession,
    ) -> None:
        """Someone else ended this task, and the caller has to hear that.

        Reported as already-applied, a workflow cancellation's row would let a
        completion replay assume work it never did.
        """
        task_id = await _seed(session)
        await _force_terminal(
            session, task_id, status='CANCELLED', kind='CANCEL_ADMIN',
        )

        outcome = await _locked(session, task_id)
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedForeignTerminalization)
        assert evidence.observed_status is TaskStatus.CANCELLED
        assert evidence.committed_kind is TerminalizationKind.CANCEL_ADMIN
        assert evidence.terminal_at is not None


class TestRefusals:
    async def test_another_worker_holds_the_claim(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, worker_id='someone-else')
        outcome = await _locked(session, task_id)
        await session.commit()

        assert isinstance(outcome, LostClaim)
        assert outcome.observed.worker_id == 'someone-else'

    async def test_the_claim_generation_moved_on(
        self,
        session: AsyncSession,
    ) -> None:
        """The worker matches; the generation does not.

        This is the case a worker id alone cannot catch — a lease lapsed, the
        row was requeued, and the same worker claimed it again.
        """
        task_id = await _seed(session)
        stale = GENERATION - timedelta(hours=1)
        outcome = await apply_async(
            await session.connection(), _fused(task_id, claimed_at=stale),
        )
        await session.commit()

        assert isinstance(outcome, LostClaim)
        assert outcome.observed.claimed_at == GENERATION

    async def test_a_requeued_row_reports_a_lost_claim(
        self,
        session: AsyncSession,
    ) -> None:
        """PENDING with the claim cleared is the requeue case.

        Fenced commands classify it as a lost claim rather than a state
        conflict: the generation that held it is gone, which is precisely what
        the caller must act on.
        """
        task_id = await _seed(
            session, status='PENDING', worker_id='someone-else', claimed_at=None,
        )
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET claimed_by_worker_id = NULL
                WHERE id = :id
            """),
            {'id': task_id},
        )
        await session.commit()

        outcome = await _locked(session, task_id)
        await session.commit()
        assert isinstance(outcome, LostClaim)
        assert outcome.observed.status is TaskStatus.PENDING

    async def test_the_source_status_is_wrong(
        self,
        session: AsyncSession,
    ) -> None:
        """Owned by this worker at this generation, but not yet running."""
        task_id = await _seed(session, status='CLAIMED')
        outcome = await _locked(session, task_id)
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.CLAIMED
        assert outcome.evidence == ObservedClaim(
            worker_id=WORKER, claimed_at=GENERATION,
        )

    async def test_a_task_that_does_not_exist(
        self,
        session: AsyncSession,
    ) -> None:
        """Absence is an outcome, not an empty result."""
        outcome = await _locked(session, str(uuid.uuid4()))
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


STALE_AFTER_MS = 60_000
FINALIZING_STALE_AFTER_MS = 30_000
STALE_AFTER_SECONDS = STALE_AFTER_MS // 1000
FINALIZING_STALE_AFTER_SECONDS = FINALIZING_STALE_AFTER_MS // 1000


async def _fail_locked(
    session: AsyncSession,
    task_id: str,
    failed_reason: str | None = None,
) -> TerminalizationOutcome:
    """The writer of FAIL_RUNNING: a failed attempt with no retry
    (``horsies_finish_failed_attempt``) under ``WORKER`` at ``GENERATION``.
    The attempt row records ``failed_reason`` too."""
    return await _outcome_of(
        session,
        _FAILED_ATTEMPT_SQL,
        {
            'task_id': task_id,
            'worker_id': WORKER,
            'claimed_at': GENERATION,
            'result': '{"err": 1}',
            'error_code': 'TASK_EXCEPTION',
            'failed_reason': failed_reason,
        },
    )


async def _fail_stale(
    session: AsyncSession,
    task_id: str,
    *,
    stale_after_ms: int = STALE_AFTER_MS,
    finalizing_stale_after_ms: int = FINALIZING_STALE_AFTER_MS,
) -> TerminalizationOutcome:
    """The writer of FAIL_STALE: the stale RUNNING recovery with no retry
    (``horsies_finish_stale_attempt``)."""
    return await _outcome_of(
        session,
        _STALE_ATTEMPT_SQL,
        {
            'task_id': task_id,
            'stale_after_ms': stale_after_ms,
            'finalizing_stale_after_ms': finalizing_stale_after_ms,
            'result': '{"err": 2}',
            'error_code': 'WORKER_CRASHED',
            'failed_reason': 'Worker crashed',
        },
    )


async def _age(
    session: AsyncSession,
    task_id: str,
    *,
    started_seconds_ago: int | None,
    finalizing_seconds_ago: int | None = None,
) -> None:
    """Put the row's liveness columns where a staleness case needs them."""
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET started_at = CASE
                    WHEN CAST(:started AS INTEGER) IS NULL THEN NULL
                    ELSE NOW() - make_interval(secs => :started)
                END,
                finalizing_at = CASE
                    WHEN CAST(:finalizing AS INTEGER) IS NULL THEN NULL
                    ELSE NOW() - make_interval(secs => :finalizing)
                END
            WHERE id = :id
        """),
        {
            'id': task_id,
            'started': started_seconds_ago,
            'finalizing': finalizing_seconds_ago,
        },
    )
    await session.commit()


async def _heartbeat(
    session: AsyncSession,
    task_id: str,
    *,
    seconds_ago: int,
    role: str = 'runner',
) -> None:
    await session.execute(
        text("""
            INSERT INTO horsies_heartbeats (
                task_id, sender_id, role, sent_at, hostname, pid
            )
            VALUES (
                :id, 'staleness-test', :role, NOW() - make_interval(secs => :ago),
                'terminalization-test-host', 4242
            )
        """),
        {'id': task_id, 'role': role, 'ago': seconds_ago},
    )
    await session.commit()


class TestFailLocked:
    """The writer of FAIL_RUNNING, ``horsies_finish_failed_attempt``."""

    async def test_applies_and_stamps_its_kind(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        outcome = await _fail_locked(session, task_id, 'worker failure')
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.FAIL_RUNNING
        row = (
            await session.execute(
                text("""
                    SELECT status, failed_at, failed_reason, error_code, result
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'FAILED'
        assert row.failed_at is not None
        assert row.failed_reason == 'worker failure'
        assert row.error_code == 'TASK_EXCEPTION'
        assert row.result == '{"err": 1}'

    async def test_a_null_reason_clears_an_earlier_attempts_reason(
        self,
        session: AsyncSession,
    ) -> None:
        """The terminal writer owns the complete final-attempt summary.

        Attempt one failed with a reason and requeued; the final attempt
        fails with none. A NULL reason means this final attempt has no
        failure reason: the requeued attempt's residue on the row is cleared,
        while attempt one's history row keeps its reason untouched.
        """
        task_id = await _seed(session)
        await _requeued_after_failed_attempt(
            session, task_id, failed_reason='attempt one crashed',
        )

        outcome = await _fail_locked(session, task_id, None)
        await session.commit()
        assert isinstance(outcome, Applied)

        reason = (
            await session.execute(
                text('SELECT failed_reason FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert reason is None

        attempts = await read_attempt_history(session, task_id)
        assert attempts == [
            (1, 'FAILED', 'attempt one crashed'),
            (2, 'FAILED', None),
        ]

    async def test_a_supplied_reason_overwrites(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)

        await _fail_locked(session, task_id, 'attempt two crashed')
        await session.commit()

        reason = (
            await session.execute(
                text('SELECT failed_reason FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert reason == 'attempt two crashed'

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _fail_locked(session, task_id)
        await session.commit()

        replay = await _fail_locked(session, task_id)
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.FAIL_RUNNING

    async def test_a_stale_failure_is_a_conflict_not_a_replay(
        self,
        session: AsyncSession,
    ) -> None:
        """Both write FAILED; they are deliberately not one class.

        Distinctness is the conservative direction: a conflict makes the
        caller look, an already-applied lets it walk away.
        """
        task_id = await _seed(session)
        await _force_terminal(session, task_id, status='FAILED', kind='FAIL_STALE')

        outcome = await _fail_locked(session, task_id)
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is TerminalizationKind.FAIL_STALE

    async def test_another_worker_holds_the_claim(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, worker_id='someone-else')
        outcome = await _fail_locked(session, task_id)
        await session.commit()

        assert isinstance(outcome, LostClaim)
        assert outcome.observed.worker_id == 'someone-else'

    async def test_the_source_status_is_wrong(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        outcome = await _fail_locked(session, task_id)
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.CLAIMED
        assert outcome.evidence == ObservedClaim(
            worker_id=WORKER, claimed_at=GENERATION,
        )

    async def test_a_task_that_does_not_exist(
        self,
        session: AsyncSession,
    ) -> None:
        outcome = await _fail_locked(session, str(uuid.uuid4()))
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


class TestFailStale:
    """The writer of FAIL_STALE, ``horsies_finish_stale_attempt``."""

    async def test_a_silent_runner_is_failed(
        self,
        session: AsyncSession,
    ) -> None:
        """No heartbeat at all: staleness is judged from started_at."""
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 2)

        outcome = await _fail_stale(session, task_id)
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.FAIL_STALE
        assert outcome.observed.worker_id == WORKER

    async def test_a_stale_heartbeat_is_failed(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 4)
        await _heartbeat(session, task_id, seconds_ago=STALE_AFTER_SECONDS * 2)

        outcome = await _fail_stale(session, task_id)
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_a_recent_heartbeat_refuses_with_the_evidence(
        self,
        session: AsyncSession,
    ) -> None:
        """The refusal carries what the guard judged, not just that it refused."""
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 2)
        await _heartbeat(session, task_id, seconds_ago=1)

        outcome = await _fail_stale(session, task_id)
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.RUNNING
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedStaleness)
        assert evidence.last_heartbeat_at is not None
        assert evidence.started_at is not None
        assert evidence.finalizing_at is None
        assert evidence.stale_after_ms == STALE_AFTER_MS
        assert evidence.finalizing_stale_after_ms == FINALIZING_STALE_AFTER_MS
        # The refusal is reconstructible from its own evidence: the heartbeat
        # sits inside the freshness window the guard judged it against.
        assert evidence.last_heartbeat_at >= evidence.evaluated_at - timedelta(
            milliseconds=STALE_AFTER_MS,
        )

    async def test_fractional_second_threshold_is_not_truncated(
        self,
        session: AsyncSession,
    ) -> None:
        """The public millisecond contract reaches the SQL guard unchanged."""
        task_id = await _seed(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET started_at = NOW() - INTERVAL '10 seconds'
                WHERE id = :id
            """),
            {'id': task_id},
        )
        await session.execute(
            text("""
                INSERT INTO horsies_heartbeats (
                    task_id, sender_id, role, sent_at, hostname, pid
                ) VALUES (
                    :id, 'fractional-threshold-test', 'runner',
                    NOW() - INTERVAL '1.25 seconds',
                    'terminalization-test-host', 4242
                )
            """),
            {'id': task_id},
        )

        outcome = await _fail_stale(
            session,
            task_id,
            stale_after_ms=1_500,
            finalizing_stale_after_ms=2_750,
        )
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedStaleness)
        assert evidence.stale_after_ms == 1_500
        assert evidence.finalizing_stale_after_ms == 2_750
        assert evidence.last_heartbeat_at is not None
        assert evidence.last_heartbeat_at == (
            evidence.evaluated_at - timedelta(milliseconds=1_250)
        )

    async def test_a_heartbeat_from_another_role_does_not_count(
        self,
        session: AsyncSession,
    ) -> None:
        """Only the runner's heartbeats prove the runner is alive."""
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 2)
        await _heartbeat(session, task_id, seconds_ago=1, role='claimer')

        outcome = await _fail_stale(session, task_id)
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_a_row_that_never_started_refuses(
        self,
        session: AsyncSession,
    ) -> None:
        """RUNNING with no started_at is not judgeable as stale.

        The evidence says why: a null started_at, so the guard had no instant
        to measure silence from.
        """
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=None)

        outcome = await _fail_stale(session, task_id)
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedStaleness)
        assert evidence.started_at is None
        assert evidence.last_heartbeat_at is None
        assert evidence.evaluated_at is not None

    async def test_an_active_finalizer_defers(
        self,
        session: AsyncSession,
    ) -> None:
        """A finalizer inside its window holds the row against stale-failure.

        The evidence must explain this refusal on its own: the runner is
        silent past its threshold, so the only arm that can have refused is
        the finalizer's — and the finalizing instant it carries sits inside
        the window it was judged against.
        """
        task_id = await _seed(session)
        await _age(
            session,
            task_id,
            started_seconds_ago=STALE_AFTER_SECONDS * 2,
            finalizing_seconds_ago=1,
        )

        outcome = await _fail_stale(session, task_id)
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedStaleness)
        assert evidence.finalizing_at is not None
        assert evidence.finalizing_at >= evidence.evaluated_at - timedelta(
            milliseconds=FINALIZING_STALE_AFTER_MS,
        )
        assert evidence.started_at is not None
        assert evidence.started_at < evidence.evaluated_at - timedelta(
            milliseconds=STALE_AFTER_MS,
        )
        assert evidence.last_heartbeat_at is None

    async def test_a_stale_finalizer_does_not_defer(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _age(
            session,
            task_id,
            started_seconds_ago=STALE_AFTER_SECONDS * 2,
            finalizing_seconds_ago=FINALIZING_STALE_AFTER_SECONDS * 2,
        )

        outcome = await _fail_stale(session, task_id)
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 2)
        await _fail_stale(session, task_id)
        await session.commit()

        replay = await _fail_stale(session, task_id)
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.FAIL_STALE

    async def test_a_locked_failure_is_a_conflict_not_a_replay(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _force_terminal(session, task_id, status='FAILED', kind='FAIL_RUNNING')

        outcome = await _fail_stale(session, task_id)
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is TerminalizationKind.FAIL_RUNNING

    async def test_the_source_status_is_wrong(
        self,
        session: AsyncSession,
    ) -> None:
        """Not RUNNING, so no staleness was judged and no evidence is claimed."""
        task_id = await _seed(session, status='CLAIMED')
        outcome = await _fail_stale(session, task_id)
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.CLAIMED
        assert outcome.evidence == ObservedClaim(
            worker_id=WORKER, claimed_at=GENERATION,
        )

    async def test_a_task_that_does_not_exist(
        self,
        session: AsyncSession,
    ) -> None:
        outcome = await _fail_stale(session, str(uuid.uuid4()))
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


def _expire_claim(task_id: str) -> ExpireOwnedClaim:
    return ExpireOwnedClaim(
        task_id=task_id,
        fence=WorkerOwned(worker_id=WORKER),
        result_json='{"err": 3}',
        error_code='TASK_EXPIRED',
    )


def _expire_pending(batch_size: int = 10) -> ExpirePendingTasks:
    return ExpirePendingTasks(
        batch_size=batch_size,
        result_json='{"err": 4}',
        error_code='TASK_EXPIRED',
    )


async def _deadline(
    session: AsyncSession,
    task_id: str,
    *,
    seconds_ago: int | None,
) -> None:
    """Set the row's good_until relative to now; None removes the deadline."""
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET good_until = CASE
                    WHEN CAST(:ago AS INTEGER) IS NULL THEN NULL
                    ELSE NOW() - make_interval(secs => :ago)
                END
            WHERE id = :id
        """),
        {'id': task_id, 'ago': seconds_ago},
    )
    await session.commit()


class TestExpireOwnedClaim:
    async def test_applies_when_the_deadline_passed(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        await _deadline(session, task_id, seconds_ago=60)

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.EXPIRE_CLAIMED
        assert outcome.observed.status is TaskStatus.CLAIMED
        row = (
            await session.execute(
                text("""
                    SELECT status, claimed, claim_expires_at, failed_at, error_code
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'EXPIRED'
        assert row.claimed is False
        assert row.claim_expires_at is None
        assert row.failed_at is not None
        assert row.error_code == 'TASK_EXPIRED'

    async def test_expiry_clears_an_earlier_attempts_reason(
        self,
        session: AsyncSession,
    ) -> None:
        """A requeued row's stale reason does not sit beside TASK_EXPIRED.

        Attempt one failed with a reason and requeued; the reclaimed row then
        expires. Expiry has no failure reason of its own, so the requeued
        attempt's residue on the row is cleared with the transition, while
        attempt one's history row keeps its reason untouched.
        """
        task_id = await _seed(session, status='CLAIMED')
        await _deadline(session, task_id, seconds_ago=60)
        await _requeued_after_failed_attempt(
            session, task_id, failed_reason='attempt one crashed',
        )

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()
        assert isinstance(outcome, Applied)

        row = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.error_code == 'TASK_EXPIRED'
        assert row.failed_reason is None

        attempts = await read_attempt_history(session, task_id)
        assert attempts == [
            (1, 'FAILED', 'attempt one crashed'),
        ]

    async def test_deadline_change_while_waiting_on_the_row_is_rechecked(
        self,
        engine: AsyncEngine,
        session: AsyncSession,
    ) -> None:
        """A newly eligible row is not misclassified after lock contention.

        The operation's first guarded update sees the committed future
        deadline. A concurrent transaction already holds the row with a past
        deadline; the refusal capture waits for that transaction, then the
        operation must apply from the locked post-commit image.
        """
        task_id = await _seed(session, status='CLAIMED')
        await _deadline(session, task_id, seconds_ago=-3600)

        async with (
            AsyncSession(engine, expire_on_commit=False) as mutator,
            AsyncSession(engine, expire_on_commit=False) as applier,
            AsyncSession(engine, expire_on_commit=False) as observer,
        ):
            mutator_pid = int(
                (await mutator.execute(text('SELECT pg_backend_pid()'))).scalar_one()
            )
            await mutator.execute(
                text("""
                    UPDATE horsies_tasks
                    SET good_until = NOW() - INTERVAL '1 hour'
                    WHERE id = :id
                """),
                {'id': task_id},
            )

            applier_pid = int(
                (await applier.execute(text('SELECT pg_backend_pid()'))).scalar_one()
            )
            application = asyncio.create_task(
                apply_async(await applier.connection(), _expire_claim(task_id))
            )
            for _ in range(100):
                blocked_by_mutator = bool(
                    (
                        await observer.execute(
                            text(
                                'SELECT :mutator = ANY('
                                'pg_blocking_pids(:applier))'
                            ),
                            {'mutator': mutator_pid, 'applier': applier_pid},
                        )
                    ).scalar_one()
                )
                if blocked_by_mutator:
                    break
                await asyncio.sleep(0.01)
            else:
                await mutator.rollback()
                await asyncio.wait_for(application, timeout=5)
                raise AssertionError('expiry operation never waited on the row lock')

            await mutator.commit()
            outcome = await asyncio.wait_for(application, timeout=5)
            await applier.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.EXPIRE_CLAIMED

    async def test_a_live_deadline_refuses_with_the_evidence(
        self,
        session: AsyncSession,
    ) -> None:
        """The refusal names the deadline it judged and when it judged it."""
        task_id = await _seed(session, status='CLAIMED')
        await _deadline(session, task_id, seconds_ago=-3600)

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedDeadline)
        assert evidence.good_until is not None
        assert evidence.evaluated_at is not None

    async def test_a_row_with_no_deadline_refuses(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        await _deadline(session, task_id, seconds_ago=None)

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedDeadline)
        assert evidence.good_until is None

    async def test_another_workers_claim_is_lost(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', worker_id='someone-else')
        await _deadline(session, task_id, seconds_ago=60)

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()
        assert isinstance(outcome, LostClaim)
        assert outcome.observed.worker_id == 'someone-else'

    async def test_the_pending_expiry_counts_as_the_same_work(
        self,
        session: AsyncSession,
    ) -> None:
        """Claimed and pending expiry are one class: the same end, either door."""
        task_id = await _seed(session, status='CLAIMED')
        await _force_terminal(session, task_id, status='EXPIRED', kind='EXPIRE_PENDING')

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()
        assert isinstance(outcome, AlreadyApplied)
        assert outcome.kind is TerminalizationKind.EXPIRE_PENDING

    async def test_a_foreign_kind_is_a_conflict_not_a_replay(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        await _force_terminal(session, task_id, status='FAILED', kind='FAIL_STALE')

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is TerminalizationKind.FAIL_STALE

    async def test_the_source_status_is_wrong(
        self,
        session: AsyncSession,
    ) -> None:
        """RUNNING is past this operation's window even with the deadline gone."""
        task_id = await _seed(session)
        await _deadline(session, task_id, seconds_ago=60)

        outcome = await apply_async(await session.connection(), _expire_claim(task_id))
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.RUNNING
        assert outcome.evidence == ObservedClaim(
            worker_id=WORKER, claimed_at=GENERATION,
        )

    async def test_a_task_that_does_not_exist(
        self,
        session: AsyncSession,
    ) -> None:
        outcome = await apply_async(
            await session.connection(), _expire_claim(str(uuid.uuid4())),
        )
        await session.commit()
        assert isinstance(outcome, TaskAbsent)

    async def test_the_sync_driver_round_trips(
        self,
        session: AsyncSession,
    ) -> None:
        """The child paths call this operation on psycopg, so prove that path.

        Same function, same decoder; only the parameter rendering differs,
        and this is the test that would catch it rendering wrongly.
        """
        import psycopg

        from tests.integration.conftest import DB_URL

        task_id = await _seed(session, status='CLAIMED')
        await _deadline(session, task_id, seconds_ago=60)

        conninfo = DB_URL
        with psycopg.connect(conninfo) as connection:
            with connection.cursor() as cursor:
                outcome = apply_sync(cursor, _expire_claim(task_id))
            connection.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.EXPIRE_CLAIMED

        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'EXPIRED'


class TestExpirePendingTasks:
    async def test_expires_every_eligible_row_and_reports_each(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        eligible = [await _seed(session, status='PENDING') for _ in range(3)]
        for task_id in eligible:
            await _deadline(session, task_id, seconds_ago=60)
        fresh = await _seed(session, status='PENDING')
        await _deadline(session, fresh, seconds_ago=-3600)
        undated = await _seed(session, status='PENDING')

        outcomes = await apply_batch_async(
            await session.connection(), _expire_pending(),
        )
        await session.commit()

        assert {o.task_id for o in outcomes} == set(eligible)
        for outcome in outcomes:
            assert isinstance(outcome, Applied)
            assert outcome.kind is TerminalizationKind.EXPIRE_PENDING
            assert outcome.ordinality is None
            assert outcome.observed.status is TaskStatus.PENDING

        rows = (
            await session.execute(
                text("""
                    SELECT id, status FROM itest_task_rows
                    WHERE id = ANY(CAST(:ids AS uuid[]))
                """),
                {'ids': eligible + [fresh, undated]},
            )
        ).all()
        statuses = {row.id: row.status for row in rows}
        assert all(statuses[task_id] == 'EXPIRED' for task_id in eligible)
        assert statuses[fresh] == 'PENDING'
        assert statuses[undated] == 'PENDING'

    async def test_the_sweep_clears_an_earlier_attempts_reason(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        """A pending row that failed and requeued expires without its old reason.

        Attempt one failed with a reason and the task requeued to PENDING —
        exactly the state the sweep discovers. The sweep writes TASK_EXPIRED
        and clears the requeued attempt's residue, while attempt one's
        history row keeps its reason untouched.
        """
        task_id = await _seed(session, status='PENDING')
        await _deadline(session, task_id, seconds_ago=60)
        await _requeued_after_failed_attempt(
            session, task_id, failed_reason='attempt one crashed',
        )

        outcomes = await apply_batch_async(
            await session.connection(), _expire_pending(),
        )
        await session.commit()
        assert [o.task_id for o in outcomes] == [task_id]

        row = (
            await session.execute(
                text("""
                    SELECT status, error_code, failed_reason
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'EXPIRED'
        assert row.error_code == 'TASK_EXPIRED'
        assert row.failed_reason is None

        attempts = await read_attempt_history(session, task_id)
        assert attempts == [
            (1, 'FAILED', 'attempt one crashed'),
        ]

    async def test_the_batch_size_bounds_one_pass(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        """Oldest deadlines first, and the remainder waits for the next pass."""
        oldest = await _seed(session, status='PENDING')
        await _deadline(session, oldest, seconds_ago=300)
        middle = await _seed(session, status='PENDING')
        await _deadline(session, middle, seconds_ago=200)
        newest = await _seed(session, status='PENDING')
        await _deadline(session, newest, seconds_ago=100)

        outcomes = await apply_batch_async(
            await session.connection(), _expire_pending(batch_size=2),
        )
        await session.commit()

        assert {o.task_id for o in outcomes} == {oldest, middle}
        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': newest},
            )
        ).scalar_one()
        assert status == 'PENDING'

    async def test_nothing_eligible_is_an_empty_answer(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        task_id = await _seed(session, status='PENDING')
        await _deadline(session, task_id, seconds_ago=-3600)

        outcomes = await apply_batch_async(
            await session.connection(), _expire_pending(),
        )
        await session.commit()
        assert outcomes == []

    @pytest.mark.parametrize('batch_size', ['NULL', '0', '-1'])
    async def test_an_unbounded_or_absurd_batch_mutates_nothing(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
        batch_size: str,
    ) -> None:
        """LIMIT NULL means no limit at all, so NULL must be an error.

        The bound exists to keep one pass from committing an unbounded
        notification burst; a call that disables it is refused before any
        row is touched.
        """
        task_id = await _seed(session, status='PENDING')
        await _deadline(session, task_id, seconds_ago=60)

        with pytest.raises(Exception, match='positive integer'):
            await session.execute(
                text(f"""
                    SELECT * FROM horsies_expire_pending_tasks(
                        CAST({batch_size} AS INTEGER), '{{}}', 'TASK_EXPIRED'
                    )
                """),
            )
        await session.rollback()

        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'PENDING'

    async def test_a_row_a_concurrent_claim_holds_is_stepped_around(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        clean_workflow_tables: None,
    ) -> None:
        """SKIP LOCKED: the claim re-checks the deadline, so neither side waits."""
        held = await _seed(session, status='PENDING')
        await _deadline(session, held, seconds_ago=60)
        free = await _seed(session, status='PENDING')
        await _deadline(session, free, seconds_ago=60)

        async with current_resources(broker).engine.connect() as holder:
            await holder.execute(
                text('SELECT id FROM horsies_tasks WHERE id = CAST(:id AS uuid) FOR UPDATE'),
                {'id': held},
            )
            outcomes = await apply_batch_async(
                await session.connection(), _expire_pending(),
            )
            await session.commit()
            await holder.rollback()

        assert {o.task_id for o in outcomes} == {free}


async def _cancel_task(
    session: AsyncSession,
    task_id: str,
    *,
    include_running: bool = False,
) -> TerminalizationOutcome:
    """The writer of CANCEL_ADMIN: the administrative cancel of a plain task
    (``horsies_cancel_task``); a RUNNING row only with ``include_running``."""
    return await _outcome_of(
        session,
        _CANCEL_TASK_SQL,
        {'task_id': task_id, 'include_running': include_running},
    )


def _cancel_owned_orphan(
    task_id: str,
    claimed_at: datetime = GENERATION,
) -> CancelOwnedOrphan:
    return CancelOwnedOrphan(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
    )


async def _link_workflow_task(
    session: AsyncSession,
    task_id: str,
    *,
    node_status: str,
    workflow_status: str = 'RUNNING',
) -> str:
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows (
                id, name, definition_key, status, on_error, output_task_index,
                success_policy, result, error, parent_workflow_id,
                parent_task_index, root_workflow_id, depth, sent_at,
                created_at, started_at, completed_at, updated_at
            )
            VALUES (
                :id, 'terminalization.test', NULL, :workflow_status, 'fail',
                NULL, NULL, NULL, NULL, NULL,
                NULL, :id, 0, NOW(),
                NOW(), NOW(), NULL, NOW()
            )
        """),
        {'id': workflow_id, 'workflow_status': workflow_status},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
                id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, args_from,
                workflow_ctx_from, allow_failed_deps, join_type, min_success,
                task_options, max_retries, execution, good_until,
                retention_class_key, status, task_id, is_subworkflow,
                sub_workflow_id, sub_workflow_name, sub_definition_key,
                sub_workflow_params, sub_workflow_summary, result, error,
                created_at, started_at, completed_at
            )
            VALUES ('NONE',
                :id, :workflow_id, 0, 'node_0', 'terminalization.test', '{}',
                'default', 100, '{}', NULL,
                NULL, FALSE, 'all', NULL,
                NULL, 0, 'ISOLATED', NULL,
                :retention_class_key, :node_status, :task_id, FALSE,
                NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NOW(), NULL, NULL
            )
        """),
        {
            'id': str(uuid.uuid4()),
            'workflow_id': workflow_id,
            'node_status': node_status,
            'task_id': task_id,
            'retention_class_key': DEFAULT_RETENTION_CLASS_KEY,
        },
    )
    await session.commit()
    return workflow_id


class TestCancelTask:
    """The writer of CANCEL_ADMIN, ``horsies_cancel_task``."""

    async def test_applies_with_the_operator_owned_literals_and_pre_image(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')

        outcome = await _cancel_task(session, task_id)
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.CANCEL_ADMIN
        assert outcome.observed.status is TaskStatus.CLAIMED
        assert outcome.observed.worker_id == WORKER
        assert outcome.observed.claimed_at == GENERATION
        row = (
            await session.execute(
                text("""
                    SELECT status, error_code, failed_reason, failed_at,
                           terminal_at, claimed, claimed_by_worker_id,
                           claimed_at, finalizing_at,
                           finalizing_by_worker_id
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'CANCELLED'
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Cancelled via monitoring API'
        assert row.failed_at is not None
        assert row.terminal_at == outcome.terminal_at
        # No claim state survives the move; the claim's timestamp is
        # preserved on the history record as provenance.
        assert row.claimed is False
        assert row.claimed_by_worker_id is None
        assert row.claimed_at == GENERATION
        assert row.finalizing_at is None
        assert row.finalizing_by_worker_id is None

    async def test_running_requires_explicit_permission(
        self,
        session: AsyncSession,
    ) -> None:
        refused_id = await _seed(session)
        refused = await _cancel_task(session, refused_id)
        await session.commit()
        assert isinstance(refused, SourceStateConflict)
        assert refused.observed.status is TaskStatus.RUNNING

        allowed_id = await _seed(session)
        allowed = await _cancel_task(session, allowed_id, include_running=True)
        await session.commit()
        assert isinstance(allowed, Applied)
        assert allowed.observed.status is TaskStatus.RUNNING

    async def test_a_workflow_backing_task_is_never_administratively_cancelled(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )

        outcome = await _cancel_task(session, task_id)
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        await _cancel_task(session, task_id)
        await session.commit()

        replay = await _cancel_task(session, task_id)
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.CANCEL_ADMIN

    async def test_a_finished_task_of_another_kind_is_never_overwritten(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        await _force_terminal(
            session,
            task_id,
            status='CANCELLED',
            kind='CANCEL_ORPHAN',
        )

        outcome = await _cancel_task(session, task_id, include_running=True)
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is TerminalizationKind.CANCEL_ORPHAN

    async def test_absence_is_an_outcome(
        self,
        session: AsyncSession,
    ) -> None:
        outcome = await _cancel_task(session, str(uuid.uuid4()))
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


class TestCancelOwnedOrphan:
    async def test_applies_when_no_runnable_workflow_link_exists(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(task_id),
        )
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.CANCEL_ORPHAN
        assert outcome.observed.status is TaskStatus.CLAIMED
        row = (
            await session.execute(
                text("""
                    SELECT status, error_code, failed_reason, failed_at,
                           terminal_at, claimed, claimed_by_worker_id,
                           claimed_at
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'CANCELLED'
        assert row.error_code == 'WORKFLOW_CHECK_FAILED'
        assert row.failed_reason == (
            'Workflow task orphaned: no live workflow_task linkage'
        )
        # The terminal instant lands in failed_at beside CANCELLED, and
        # the claim's timestamp survives on the record as provenance
        # while no claim state does.
        assert row.failed_at is not None
        assert row.terminal_at == outcome.terminal_at
        assert row.claimed is False
        assert row.claimed_by_worker_id is None
        assert row.claimed_at == GENERATION

    async def test_a_terminal_link_still_leaves_the_backing_task_orphaned(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _link_workflow_task(session, task_id, node_status='COMPLETED')

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(task_id),
        )
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_a_runnable_link_refuses_with_its_locked_evidence(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _link_workflow_task(session, task_id, node_status='RUNNING')

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(task_id),
        )
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedWorkflowLink(node_status='RUNNING')
        assert outcome.observed.status is TaskStatus.CLAIMED

    async def test_a_stale_generation_is_a_lost_claim_even_with_a_live_link(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _link_workflow_task(session, task_id, node_status='ENQUEUED')

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(task_id, GENERATION - timedelta(minutes=1)),
        )
        await session.commit()

        assert isinstance(outcome, LostClaim)
        assert outcome.observed.claimed_at == GENERATION

    async def test_the_sweep_kind_counts_as_the_same_work(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            task_id,
            status='CANCELLED',
            kind='CANCEL_ORPHAN_SWEEP',
        )

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(task_id),
        )
        await session.commit()
        assert isinstance(outcome, AlreadyApplied)
        assert outcome.kind is TerminalizationKind.CANCEL_ORPHAN_SWEEP

    async def test_an_administrative_cancellation_is_a_conflict(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            task_id,
            status='CANCELLED',
            kind='CANCEL_ADMIN',
        )

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(task_id),
        )
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)

    async def test_absence_is_an_outcome(
        self,
        session: AsyncSession,
    ) -> None:
        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_orphan(str(uuid.uuid4())),
        )
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


class TestCancelOrphanedTasks:
    async def test_transitions_only_orphans_in_the_effective_source_set(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        claimed = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        pending = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
            is_workflow_task=True,
        )
        linked = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _link_workflow_task(session, linked, node_status='READY')
        plain = await _seed(session, status='CLAIMED')
        running = await _seed(
            session,
            status='RUNNING',
            is_workflow_task=True,
        )

        outcomes = await apply_batch_async(
            await session.connection(),
            CancelOrphanedTasks(batch_size=10),
        )
        await session.commit()

        assert {outcome.task_id for outcome in outcomes} == {claimed, pending}
        for outcome in outcomes:
            assert isinstance(outcome, Applied)
            assert outcome.kind is TerminalizationKind.CANCEL_ORPHAN_SWEEP
            assert outcome.ordinality is None

        rows = (
            await session.execute(
                text('SELECT id, status FROM itest_task_rows WHERE id = ANY(CAST(:ids AS uuid[]))'),
                {'ids': [claimed, pending, linked, plain, running]},
            )
        ).all()
        statuses = {row.id: row.status for row in rows}
        assert statuses[claimed] == 'CANCELLED'
        assert statuses[pending] == 'CANCELLED'
        assert statuses[linked] == 'CLAIMED'
        assert statuses[plain] == 'CLAIMED'
        assert statuses[running] == 'RUNNING'

    async def test_the_batch_size_bounds_one_pass(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        ids = [
            await _seed(
                session,
                status='CLAIMED',
                is_workflow_task=True,
            )
            for _ in range(3)
        ]
        outcomes = await apply_batch_async(
            await session.connection(),
            CancelOrphanedTasks(batch_size=2),
        )
        await session.commit()

        assert len(outcomes) == 2
        assert {outcome.task_id for outcome in outcomes} <= set(ids)
        remaining = (
            await session.execute(
                text("""
                    SELECT COUNT(*) FROM itest_task_rows
                    WHERE id = ANY(CAST(:ids AS uuid[])) AND status = 'CLAIMED'
                """),
                {'ids': ids},
            )
        ).scalar_one()
        assert remaining == 1

    async def test_no_orphan_is_an_empty_answer(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _link_workflow_task(session, task_id, node_status='PENDING')

        outcomes = await apply_batch_async(
            await session.connection(),
            CancelOrphanedTasks(batch_size=10),
        )
        await session.commit()
        assert outcomes == []

    @pytest.mark.parametrize('batch_size', ['NULL', '0', '-1'])
    async def test_an_invalid_bound_raises_before_mutation(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
        batch_size: str,
    ) -> None:
        task_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )

        with pytest.raises(Exception, match='positive integer'):
            await session.execute(
                text(f"""
                    SELECT * FROM horsies_cancel_orphaned_tasks(
                        CAST({batch_size} AS INTEGER)
                    )
                """),
            )
        await session.rollback()

        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    async def test_a_locked_orphan_is_stepped_around(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        clean_workflow_tables: None,
    ) -> None:
        held = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        free = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )

        async with current_resources(broker).engine.connect() as holder:
            await holder.execute(
                text('SELECT id FROM horsies_tasks WHERE id = CAST(:id AS uuid) FOR UPDATE'),
                {'id': held},
            )
            outcomes = await apply_batch_async(
                await session.connection(),
                CancelOrphanedTasks(batch_size=10),
            )
            await session.commit()
            await holder.rollback()

        assert {outcome.task_id for outcome in outcomes} == {free}


def _abandon_owned(
    task_id: str,
    claimed_at: datetime = GENERATION,
) -> AbandonOwnedNode:
    return AbandonOwnedNode(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
    )


def _cancel_owned_node(
    task_id: str,
    claimed_at: datetime = GENERATION,
    *,
    accepts_requeued_pending: bool = True,
) -> CancelOwnedNode:
    return CancelOwnedNode(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
        accepts_requeued_pending=accepts_requeued_pending,
    )


class TestAbandonOwnedNode:
    async def test_applies_pause_literals_and_reports_the_claim_pre_image(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)

        outcome = await apply_async(
            await session.connection(),
            _abandon_owned(task_id),
        )
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.PAUSE_ABANDON_CLAIM
        assert outcome.observed.status is TaskStatus.CLAIMED
        assert outcome.observed.worker_id == WORKER
        assert outcome.observed.claimed_at == GENERATION
        row = (
            await session.execute(
                text("""
                    SELECT status, claimed, claimed_at,
                           claimed_by_worker_id, claim_expires_at,
                           finalizing_at, finalizing_by_worker_id,
                           error_code, failed_reason, failed_at,
                           terminal_at, terminalization_kind
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'CANCELLED'
        # No claim state survives the move; the claim's timestamp is
        # preserved on the history record as provenance, and the
        # terminal instant lands in failed_at beside CANCELLED.
        assert row.claimed is False
        assert row.claimed_at == GENERATION
        assert row.claimed_by_worker_id is None
        assert row.claim_expires_at is None
        assert row.finalizing_at is None
        assert row.finalizing_by_worker_id is None
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Workflow paused before task start'
        assert row.failed_at is not None
        assert row.terminal_at == outcome.terminal_at
        assert row.terminalization_kind == 'PAUSE_ABANDON_CLAIM'

    async def test_a_stale_generation_is_a_lost_claim(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)

        outcome = await apply_async(
            await session.connection(),
            _abandon_owned(task_id, GENERATION - timedelta(seconds=1)),
        )
        await session.commit()

        assert isinstance(outcome, LostClaim)
        assert outcome.observed.claimed_at == GENERATION

    async def test_a_null_generation_is_refused(
        self,
        session: AsyncSession,
    ) -> None:
        """The claim generation is required: a NULL claimed_at is refused
        (22023) before any lock, and the row stays CLAIMED. The worker never
        sends one (``OwnedClaim`` requires a claim time), so the statement
        runs with NULL in its place."""
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        await session.commit()
        statement, parameters = call_for(_abandon_owned(task_id))
        with pytest.raises(DBAPIError) as raised:
            await session.execute(statement, {**parameters, 'claimed_at': None})
        await session.rollback()
        original = raised.value.orig
        assert isinstance(original, psycopg.Error)
        assert original.sqlstate == '22023'
        status = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    async def test_pause_kinds_are_equivalent_but_workflow_cancel_is_not(
        self,
        session: AsyncSession,
    ) -> None:
        same_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            same_id,
            status='CANCELLED',
            kind='PAUSE_ABANDON_WORKFLOW',
        )
        same = await apply_async(
            await session.connection(),
            _abandon_owned(same_id),
        )
        await session.commit()
        assert isinstance(same, AlreadyApplied)

        foreign_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            foreign_id,
            status='CANCELLED',
            kind='WORKFLOW_CANCEL_CLAIM',
        )
        foreign = await apply_async(
            await session.connection(),
            _abandon_owned(foreign_id),
        )
        await session.commit()
        assert isinstance(foreign, SourceStateConflict)
        assert isinstance(foreign.evidence, ObservedForeignTerminalization)

    async def test_absence_is_an_outcome(self, session: AsyncSession) -> None:
        outcome = await apply_async(
            await session.connection(),
            _abandon_owned(str(uuid.uuid4())),
        )
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


class TestCancelOwnedNode:
    async def test_claimed_and_requeued_pending_are_the_two_apply_arms(
        self,
        session: AsyncSession,
    ) -> None:
        claimed_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        claimed = await apply_async(
            await session.connection(),
            _cancel_owned_node(claimed_id),
        )
        await session.commit()
        assert isinstance(claimed, Applied)
        assert claimed.kind is TerminalizationKind.WORKFLOW_CANCEL_CLAIM
        assert claimed.observed.status is TaskStatus.CLAIMED

        pending_id = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
            is_workflow_task=True,
        )
        pending = await apply_async(
            await session.connection(),
            _cancel_owned_node(pending_id),
        )
        await session.commit()
        assert isinstance(pending, Applied)
        assert pending.observed.status is TaskStatus.PENDING

    async def test_pending_requires_the_explicit_carveout(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
            is_workflow_task=True,
        )

        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_node(task_id, accepts_requeued_pending=False),
        )
        await session.commit()

        assert isinstance(outcome, LostClaim)
        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'PENDING'

    async def test_the_terminal_record_carries_its_own_summary(
        self,
        session: AsyncSession,
    ) -> None:
        """The move writes the operation's own error summary.

        A worker-side node cancellation has none, so the record's
        error_code and failed_reason are NULL. The terminal instant lands
        in failed_at beside CANCELLED.
        """
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        await session.commit()

        await apply_async(
            await session.connection(),
            _cancel_owned_node(task_id),
        )
        await session.commit()
        row = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason, failed_at
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one()
        assert row.error_code is None
        assert row.failed_reason is None
        assert row.failed_at is not None

    async def test_a_stale_claim_is_lost(self, session: AsyncSession) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        outcome = await apply_async(
            await session.connection(),
            _cancel_owned_node(
                task_id,
                GENERATION - timedelta(seconds=1),
            ),
        )
        await session.commit()
        assert isinstance(outcome, LostClaim)

    async def test_replay_foreign_kind_wrong_source_and_absence_are_distinct(
        self,
        session: AsyncSession,
    ) -> None:
        replay_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            replay_id,
            status='CANCELLED',
            kind='WORKFLOW_CANCEL_WORKFLOW',
        )
        replay = await apply_async(
            await session.connection(),
            _cancel_owned_node(replay_id),
        )
        await session.commit()
        assert isinstance(replay, AlreadyApplied)

        foreign_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            foreign_id,
            status='CANCELLED',
            kind='PAUSE_ABANDON_CLAIM',
        )
        foreign = await apply_async(
            await session.connection(),
            _cancel_owned_node(foreign_id),
        )
        await session.commit()
        assert isinstance(foreign, SourceStateConflict)
        assert isinstance(foreign.evidence, ObservedForeignTerminalization)

        running_id = await _seed(
            session,
            status='RUNNING',
            is_workflow_task=True,
        )
        wrong_source = await apply_async(
            await session.connection(),
            _cancel_owned_node(running_id),
        )
        await session.commit()
        assert isinstance(wrong_source, SourceStateConflict)
        assert wrong_source.observed.status is TaskStatus.RUNNING

        absent = await apply_async(
            await session.connection(),
            _cancel_owned_node(str(uuid.uuid4())),
        )
        await session.commit()
        assert isinstance(absent, TaskAbsent)

    async def test_the_sync_driver_carries_the_pending_carveout(
        self,
        session: AsyncSession,
    ) -> None:
        import psycopg

        from tests.integration.conftest import DB_URL

        task_id = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
            is_workflow_task=True,
        )
        conninfo = DB_URL
        with psycopg.connect(conninfo) as connection:
            with connection.cursor() as cursor:
                outcome = apply_sync(cursor, _cancel_owned_node(task_id))
            connection.commit()

        assert isinstance(outcome, Applied)
        assert outcome.observed.status is TaskStatus.PENDING


class TestIdKeyedWorkflowBatches:
    @pytest.mark.parametrize(
        ('command_type', 'same_kind', 'foreign_kind', 'applied_kind'),
        [
            (
                AbandonOwnedNodes,
                'PAUSE_ABANDON_WORKFLOW',
                'WORKFLOW_CANCEL_WORKFLOW',
                TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
            ),
            (
                CancelOwnedNodes,
                'WORKFLOW_CANCEL_WORKFLOW',
                'PAUSE_ABANDON_WORKFLOW',
                TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH,
            ),
        ],
    )
    async def test_one_ordered_outcome_per_input_across_every_outcome(
        self,
        session: AsyncSession,
        command_type: type[AbandonOwnedNodes] | type[CancelOwnedNodes],
        same_kind: str,
        foreign_kind: str,
        applied_kind: TerminalizationKind,
    ) -> None:
        applied_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        lost_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        conflict_id = await _seed(
            session,
            status='RUNNING',
            is_workflow_task=True,
        )
        same_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            same_id,
            status='CANCELLED',
            kind=same_kind,
        )
        foreign_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            foreign_id,
            status='CANCELLED',
            kind=foreign_kind,
        )
        absent_id = str(uuid.uuid4())
        await session.commit()
        command = command_type(
            fence=OwnedClaimBatch(
                worker_id=WORKER,
                claim_generations=(
                    (applied_id, GENERATION),
                    (lost_id, GENERATION - timedelta(seconds=1)),
                    (conflict_id, GENERATION),
                    (same_id, GENERATION),
                    (foreign_id, GENERATION),
                    (absent_id, GENERATION),
                ),
            ),
        )

        outcomes = await apply_batch_async(await session.connection(), command)
        await session.commit()

        assert [outcome.task_id for outcome in outcomes] == [
            applied_id,
            lost_id,
            conflict_id,
            same_id,
            foreign_id,
            absent_id,
        ]
        assert [outcome.ordinality for outcome in outcomes] == list(range(1, 7))
        assert isinstance(outcomes[0], Applied)
        assert outcomes[0].kind is applied_kind
        assert isinstance(outcomes[1], LostClaim)
        assert isinstance(outcomes[2], SourceStateConflict)
        assert isinstance(outcomes[3], AlreadyApplied)
        assert isinstance(outcomes[4], SourceStateConflict)
        assert isinstance(outcomes[4].evidence, ObservedForeignTerminalization)
        assert isinstance(outcomes[5], TaskAbsent)

        summary = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': applied_id},
            )
        ).one()
        # The record's error fields are projected from the operation.
        # Pause-abandon states its cause; a workflow cancellation has none
        # to state, and its kind says so.
        if command_type is AbandonOwnedNodes:
            assert summary.error_code == 'TASK_CANCELLED'
            assert summary.failed_reason == 'Workflow paused before task start'
        else:
            assert summary.error_code is None
            assert summary.failed_reason is None

    @pytest.mark.parametrize(
        'function_name',
        ['horsies_abandon_owned_nodes', 'horsies_cancel_owned_nodes'],
    )
    @pytest.mark.parametrize(
        ('ids_sql', 'generations_sql', 'message'),
        [
            ('NULL', "ARRAY['2026-08-04 09:00:00+00']", 'non-NULL'),
            ("ARRAY['one']", 'NULL', 'non-NULL'),
            (
                "ARRAY['one', '00000000-0000-4000-8000-000000000002']",
                "ARRAY['2026-08-04 09:00:00+00']",
                'lengths differ',
            ),
            (
                'ARRAY[NULL]::uuid[]',
                "ARRAY['2026-08-04 09:00:00+00']",
                'non-NULL',
            ),
            (
                "ARRAY['one', 'one']",
                "ARRAY['2026-08-04 09:00:00+00', " "'2026-08-04 09:00:01+00']",
                'distinct',
            ),
        ],
    )
    async def test_invalid_arrays_raise_before_mutation(
        self,
        session: AsyncSession,
        function_name: str,
        ids_sql: str,
        generations_sql: str,
        message: str,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        ids = ids_sql.replace('one', task_id)

        with pytest.raises(Exception, match=message):
            await session.execute(
                text(f"""
                    SELECT * FROM {function_name}(
                        CAST({ids} AS uuid[]),
                        CAST({generations_sql} AS TIMESTAMPTZ[]),
                        :worker_id
                    )
                """),
                {'worker_id': WORKER},
            )
        await session.rollback()

        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    @pytest.mark.parametrize('command_type', [AbandonOwnedNodes, CancelOwnedNodes])
    async def test_a_null_generation_is_refused(
        self,
        session: AsyncSession,
        command_type: type[AbandonOwnedNodes] | type[CancelOwnedNodes],
    ) -> None:
        """Every entry of the generations array is required: a NULL entry
        refuses the statement (22023) before any lock, and the row stays
        CLAIMED. The worker never sends one, so the statement runs with NULL
        in its place."""
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        await session.commit()
        command = command_type(
            fence=OwnedClaimBatch(
                worker_id=WORKER,
                claim_generations=((task_id, GENERATION),),
            ),
        )
        statement, parameters = batch_call_for(command)
        with pytest.raises(DBAPIError) as raised:
            await session.execute(statement, {**parameters, 'claimed_ats': [None]})
        await session.rollback()
        original = raised.value.orig
        assert isinstance(original, psycopg.Error)
        assert original.sqlstate == '22023'
        status = (
            await session.execute(
                text('SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    @pytest.mark.parametrize('command_type', [AbandonOwnedNodes, CancelOwnedNodes])
    async def test_empty_input_is_valid(
        self,
        session: AsyncSession,
        command_type: type[AbandonOwnedNodes] | type[CancelOwnedNodes],
    ) -> None:
        command = command_type(
            fence=OwnedClaimBatch(worker_id=WORKER, claim_generations=()),
        )
        outcomes = await apply_batch_async(await session.connection(), command)
        await session.commit()
        assert outcomes == []


class TestWorkflowScopedBatches:
    async def test_pause_applies_only_under_both_live_guards(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        eligible = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        eligible_workflow = await _link_workflow_task(
            session,
            eligible,
            node_status='RUNNING',
            workflow_status='PAUSED',
        )
        eligible_enqueued = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        eligible_enqueued_workflow = await _link_workflow_task(
            session,
            eligible_enqueued,
            node_status='ENQUEUED',
            workflow_status='PAUSED',
        )
        wrong_workflow = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        wrong_workflow_id = await _link_workflow_task(
            session,
            wrong_workflow,
            node_status='RUNNING',
            workflow_status='RUNNING',
        )
        wrong_link = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        wrong_link_workflow = await _link_workflow_task(
            session,
            wrong_link,
            node_status='READY',
            workflow_status='PAUSED',
        )
        wrong_task_source = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
            is_workflow_task=True,
        )
        wrong_task_workflow = await _link_workflow_task(
            session,
            wrong_task_source,
            node_status='ENQUEUED',
            workflow_status='PAUSED',
        )

        outcomes = await apply_batch_async(
            await session.connection(),
            AbandonNodesOfPausedWorkflows(
                workflow_ids=(
                    eligible_workflow,
                    eligible_enqueued_workflow,
                    wrong_workflow_id,
                    wrong_link_workflow,
                    wrong_task_workflow,
                ),
            ),
        )
        await session.commit()

        assert {outcome.task_id for outcome in outcomes} == {
            eligible,
            eligible_enqueued,
        }
        outcome = next(outcome for outcome in outcomes if outcome.task_id == eligible)
        assert isinstance(outcome, Applied)
        assert outcome.ordinality is None
        assert outcome.kind is TerminalizationKind.PAUSE_ABANDON_WORKFLOW
        row = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason, failed_at
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': eligible},
            )
        ).one()
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Workflow paused before task start'
        assert row.failed_at is not None
        persisted = (
            await session.execute(
                text("""
                    SELECT status, terminalization_kind,
                           claimed_by_worker_id, claimed_at
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': eligible},
            )
        ).one()
        assert persisted.status == 'CANCELLED'
        assert persisted.terminalization_kind == 'PAUSE_ABANDON_WORKFLOW'
        assert persisted.claimed_by_worker_id is None
        # The claim's timestamp is preserved on the record as provenance.
        assert persisted.claimed_at == GENERATION
        refused_rows = (
            await session.execute(
                text('SELECT id, status FROM itest_task_rows WHERE id = ANY(CAST(:ids AS uuid[]))'),
                {'ids': [wrong_workflow, wrong_link, wrong_task_source]},
            )
        ).all()
        assert {row.id: row.status for row in refused_rows} == {
            wrong_workflow: 'CLAIMED',
            wrong_link: 'CLAIMED',
            wrong_task_source: 'PENDING',
        }

    async def test_cancel_accepts_all_three_legacy_task_source_states(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        task_ids: list[str] = []
        workflow_ids: list[str] = []
        for status in ('PENDING', 'CLAIMED', 'RUNNING'):
            worker_id = None if status == 'PENDING' else WORKER
            claimed_at = None if status == 'PENDING' else GENERATION
            task_id = await _seed(
                session,
                status=status,
                worker_id=worker_id,
                claimed_at=claimed_at,
                is_workflow_task=True,
            )
            task_ids.append(task_id)
            workflow_ids.append(
                await _link_workflow_task(
                    session,
                    task_id,
                    node_status='ENQUEUED',
                    workflow_status='CANCELLED',
                )
            )

        await session.commit()

        outcomes = await apply_batch_async(
            await session.connection(),
            CancelNodesOfCancelledWorkflow(workflow_ids=tuple(workflow_ids)),
        )
        await session.commit()

        assert {outcome.task_id for outcome in outcomes} == set(task_ids)
        assert all(isinstance(outcome, Applied) for outcome in outcomes)
        assert all(
            outcome.kind is TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW
            for outcome in outcomes
            if isinstance(outcome, Applied)
        )
        assert all(outcome.ordinality is None for outcome in outcomes)
        # The workflow-cancel sweep carries no error summary of its own,
        # and the terminal instant lands in failed_at beside CANCELLED.
        summary = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason, failed_at
                    FROM itest_task_rows WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_ids[0]},
            )
        ).one()
        assert summary.error_code is None
        assert summary.failed_reason is None
        assert summary.failed_at is not None

    async def test_cancel_refuses_a_live_workflow_or_non_enqueued_link(
        self,
        session: AsyncSession,
        clean_workflow_tables: None,
    ) -> None:
        live_workflow_task = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        live_workflow_id = await _link_workflow_task(
            session,
            live_workflow_task,
            node_status='ENQUEUED',
            workflow_status='RUNNING',
        )
        ready_link_task = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        ready_link_workflow = await _link_workflow_task(
            session,
            ready_link_task,
            node_status='READY',
            workflow_status='CANCELLED',
        )

        outcomes = await apply_batch_async(
            await session.connection(),
            CancelNodesOfCancelledWorkflow(
                workflow_ids=(live_workflow_id, ready_link_workflow),
            ),
        )
        await session.commit()

        assert outcomes == []
        rows = (
            await session.execute(
                text('SELECT id, status FROM itest_task_rows WHERE id = ANY(CAST(:ids AS uuid[]))'),
                {'ids': [live_workflow_task, ready_link_task]},
            )
        ).all()
        assert {row.status for row in rows} == {'CLAIMED'}
