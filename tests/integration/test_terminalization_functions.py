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

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.brokers.postgres import PostgresBroker

from horsies.core.lifecycle.commands import (
    AbandonNodesOfPausedWorkflows,
    AbandonOwnedNode,
    AbandonOwnedNodes,
    CancelLockedTask,
    CancelNodesOfCancelledWorkflow,
    CancelOrphanedTasks,
    CancelOwnedNode,
    CancelOwnedNodes,
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    FailLockedTask,
    FailStaleTask,
)
from horsies.core.lifecycle.fences import (
    CallerHoldsRowLock,
    OwnedClaim,
    OwnedClaimBatch,
    PriorLockedRead,
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
)
from horsies.core.lifecycle.persistence import (
    apply_async,
    apply_batch_async,
    apply_sync,
)
from horsies.core.types.status import TaskStatus

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


_SEED_SQL = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, status, args, kwargs, enqueue_sha,
        is_workflow_task, claimed, claimed_by_worker_id, claimed_at, started_at
    )
    VALUES (
        :id, 'terminalization.test', 'default', :status, '[]', '{}',
        repeat('0', 64), :is_workflow_task, :claimed,
        :worker_id, :claimed_at, NOW()
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
            'claimed': worker_id is not None,
        },
    )
    await session.commit()
    return task_id


async def _force_terminal(
    session: AsyncSession,
    task_id: str,
    *,
    status: str,
    kind: str | None,
) -> None:
    """Put a row where another operation would have left it."""
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = :status,
                terminal_at = NOW(),
                terminalization_kind = :kind,
                claimed = FALSE,
                claimed_by_worker_id = NULL,
                claimed_at = NULL
            WHERE id = :id
        """),
        {'status': status, 'kind': kind, 'id': task_id},
    )
    await session.commit()


def _locked(task_id: str) -> CompleteLockedTask:
    return CompleteLockedTask(
        task_id=task_id,
        fence=PriorLockedRead(worker_id=WORKER),
        result_json='{"ok": 1}',
    )


def _fused(task_id: str, claimed_at: datetime | None = GENERATION) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
        result_json='{"ok": 2}',
        notify_channel='task_queue_default',
        notify_payload=f'capacity:{task_id}',
    )


class TestTerminalAtInvariant:
    @pytest.mark.parametrize(
        ('status', 'terminal_at'),
        [
            pytest.param('CANCELLED', None, id='terminal-requires-timestamp'),
            pytest.param('PENDING', GENERATION, id='live-rejects-timestamp'),
        ],
    )
    async def test_status_and_terminal_at_must_agree(
        self,
        session: AsyncSession,
        status: str,
        terminal_at: datetime | None,
    ) -> None:
        task_id = await _seed(
            session,
            status='PENDING',
            worker_id=None,
            claimed_at=None,
        )

        with pytest.raises(
            IntegrityError,
            match='ck_horsies_tasks_terminal_at_terminal_only',
        ):
            await session.execute(
                text("""
                    UPDATE horsies_tasks
                    SET status = :status, terminal_at = :terminal_at
                    WHERE id = :id
                """),
                {
                    'id': task_id,
                    'status': status,
                    'terminal_at': terminal_at,
                },
            )
            await session.commit()
        await session.rollback()

        row = (
            await session.execute(
                text(
                    'SELECT status, terminal_at FROM horsies_tasks '
                    'WHERE id = :id'
                ),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'PENDING'
        assert row.terminal_at is None


class TestAppliedTransitions:
    async def test_locked_completion_applies_and_stamps_its_kind(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        outcome = await apply_async(await session.connection(), _locked(task_id))
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.COMPLETE_LOCKED
        assert outcome.terminal_at is not None

    async def test_applied_reports_the_image_it_matched(
        self,
        session: AsyncSession,
    ) -> None:
        """Not decoration: the pre-transition image is what the guard saw."""
        task_id = await _seed(session)
        outcome = await apply_async(await session.connection(), _locked(task_id))
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.observed.status is TaskStatus.RUNNING
        assert outcome.observed.worker_id == WORKER
        assert outcome.observed.claimed_at == GENERATION

    async def test_fused_completion_writes_its_attempt_row(
        self,
        session: AsyncSession,
    ) -> None:
        """The fusion exists to do this in one statement; it still does."""
        task_id = await _seed(session)
        outcome = await apply_async(await session.connection(), _fused(task_id))
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.COMPLETE_FUSED
        row = (
            await session.execute(
                text("""
                    SELECT outcome, will_retry FROM horsies_task_attempts
                    WHERE task_id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.outcome == 'COMPLETED'
        assert row.will_retry is False

    async def test_transition_leaves_the_row_terminal_and_attributed(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await apply_async(await session.connection(), _locked(task_id))
        await session.commit()

        row = (
            await session.execute(
                text("""
                    SELECT status, terminal_at, terminalization_kind, result
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'COMPLETED'
        assert row.terminal_at is not None
        assert row.terminalization_kind == 'COMPLETE_LOCKED'
        assert row.result == '{"ok": 1}'


class TestAlreadyApplied:
    async def test_replay_of_the_same_operation(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await apply_async(await session.connection(), _locked(task_id))
        await session.commit()

        replay = await apply_async(await session.connection(), _locked(task_id))
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

        replay = await apply_async(await session.connection(), _locked(task_id))
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

        outcome = await apply_async(await session.connection(), _locked(task_id))
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedForeignTerminalization)
        assert evidence.observed_status is TaskStatus.CANCELLED
        assert evidence.committed_kind is TerminalizationKind.CANCEL_ADMIN
        assert evidence.terminal_at is not None

    async def test_a_row_with_no_kind_is_never_a_replay(
        self,
        session: AsyncSession,
    ) -> None:
        """Rows terminalized before the column existed prove nothing.

        Their provenance is unknown, and unknown provenance classifies
        conservatively rather than being inferred from the status alone.
        """
        task_id = await _seed(session)
        await _force_terminal(session, task_id, status='COMPLETED', kind=None)

        outcome = await apply_async(await session.connection(), _locked(task_id))
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is None


class TestRefusals:
    async def test_another_worker_holds_the_claim(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, worker_id='someone-else')
        outcome = await apply_async(await session.connection(), _locked(task_id))
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
                SET claimed_by_worker_id = NULL, claimed = FALSE
                WHERE id = :id
            """),
            {'id': task_id},
        )
        await session.commit()

        outcome = await apply_async(await session.connection(), _locked(task_id))
        await session.commit()
        assert isinstance(outcome, LostClaim)
        assert outcome.observed.status is TaskStatus.PENDING

    async def test_the_source_status_is_wrong(
        self,
        session: AsyncSession,
    ) -> None:
        """Owned by this worker at this generation, but not yet running."""
        task_id = await _seed(session, status='CLAIMED')
        outcome = await apply_async(await session.connection(), _locked(task_id))
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
        outcome = await apply_async(await session.connection(), _locked(str(uuid.uuid4())))
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


class TestKindVocabularyIsEnforced:
    async def test_the_database_rejects_a_kind_outside_the_vocabulary(
        self,
        session: AsyncSession,
    ) -> None:
        """The value domain is a constraint, so a typo cannot become history."""
        task_id = await _seed(session)
        with pytest.raises(Exception, match='ck_horsies_tasks_terminalization_kind'):
            await session.execute(
                text("""
                    UPDATE horsies_tasks SET terminalization_kind = 'COMPLETE_ISH'
                    WHERE id = :id
                """),
                {'id': task_id},
            )
            await session.commit()
        await session.rollback()

    async def test_a_null_kind_is_accepted(
        self,
        session: AsyncSession,
    ) -> None:
        """Legacy rows and live rows both carry NULL, so NULL has to pass.

        This is what makes the constraint safe to install during a rolling
        upgrade: a worker that predates the column supplies nothing.
        """
        task_id = await _seed(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks SET terminalization_kind = NULL
                WHERE id = :id
            """),
            {'id': task_id},
        )
        await session.commit()


STALE_AFTER_MS = 60_000
FINALIZING_STALE_AFTER_MS = 30_000
STALE_AFTER_SECONDS = STALE_AFTER_MS // 1000
FINALIZING_STALE_AFTER_SECONDS = FINALIZING_STALE_AFTER_MS // 1000


def _fail_locked(task_id: str, failed_reason: str | None = None) -> FailLockedTask:
    return FailLockedTask(
        task_id=task_id,
        fence=PriorLockedRead(worker_id=WORKER),
        result_json='{"err": 1}',
        error_code='TASK_EXCEPTION',
        failed_reason=failed_reason,
    )


def _fail_stale(task_id: str) -> FailStaleTask:
    return FailStaleTask(
        task_id=task_id,
        stale_after_ms=STALE_AFTER_MS,
        finalizing_stale_after_ms=FINALIZING_STALE_AFTER_MS,
        result_json='{"err": 2}',
        error_code='WORKER_CRASHED',
        failed_reason='Worker crashed',
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
            INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at)
            VALUES (:id, 'staleness-test', :role, NOW() - make_interval(secs => :ago))
        """),
        {'id': task_id, 'role': role, 'ago': seconds_ago},
    )
    await session.commit()


class TestFailLocked:
    async def test_applies_and_stamps_its_kind(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        outcome = await apply_async(
            await session.connection(), _fail_locked(task_id, 'worker failure'),
        )
        await session.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.FAIL_RUNNING
        row = (
            await session.execute(
                text("""
                    SELECT status, failed_at, failed_reason, error_code, result
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'FAILED'
        assert row.failed_at is not None
        assert row.failed_reason == 'worker failure'
        assert row.error_code == 'TASK_EXCEPTION'
        assert row.result == '{"err": 1}'

    async def test_a_null_reason_leaves_an_earlier_attempts_reason(
        self,
        session: AsyncSession,
    ) -> None:
        """NULL means "leave the column as it is", not "clear it".

        No requeue path clears failed_reason, so a row can carry one from an
        earlier attempt that this transition never owned.
        """
        task_id = await _seed(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks SET failed_reason = 'attempt one crashed'
                WHERE id = :id
            """),
            {'id': task_id},
        )
        await session.commit()

        outcome = await apply_async(
            await session.connection(), _fail_locked(task_id, None),
        )
        await session.commit()
        assert isinstance(outcome, Applied)

        reason = (
            await session.execute(
                text('SELECT failed_reason FROM horsies_tasks WHERE id = :id'),
                {'id': task_id},
            )
        ).scalar_one()
        assert reason == 'attempt one crashed'

    async def test_a_supplied_reason_overwrites(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await session.execute(
            text("""
                UPDATE horsies_tasks SET failed_reason = 'attempt one crashed'
                WHERE id = :id
            """),
            {'id': task_id},
        )
        await session.commit()

        await apply_async(
            await session.connection(), _fail_locked(task_id, 'attempt two crashed'),
        )
        await session.commit()

        reason = (
            await session.execute(
                text('SELECT failed_reason FROM horsies_tasks WHERE id = :id'),
                {'id': task_id},
            )
        ).scalar_one()
        assert reason == 'attempt two crashed'

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await apply_async(await session.connection(), _fail_locked(task_id))
        await session.commit()

        replay = await apply_async(await session.connection(), _fail_locked(task_id))
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

        outcome = await apply_async(await session.connection(), _fail_locked(task_id))
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is TerminalizationKind.FAIL_STALE

    async def test_another_worker_holds_the_claim(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, worker_id='someone-else')
        outcome = await apply_async(await session.connection(), _fail_locked(task_id))
        await session.commit()

        assert isinstance(outcome, LostClaim)
        assert outcome.observed.worker_id == 'someone-else'

    async def test_the_source_status_is_wrong(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        outcome = await apply_async(await session.connection(), _fail_locked(task_id))
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
        outcome = await apply_async(
            await session.connection(), _fail_locked(str(uuid.uuid4())),
        )
        await session.commit()
        assert isinstance(outcome, TaskAbsent)


class TestFailStale:
    async def test_a_silent_runner_is_failed(
        self,
        session: AsyncSession,
    ) -> None:
        """No heartbeat at all: staleness is judged from started_at."""
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 2)

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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
                    task_id, sender_id, role, sent_at
                ) VALUES (
                    :id, 'fractional-threshold-test', 'runner',
                    NOW() - INTERVAL '1.25 seconds'
                )
            """),
            {'id': task_id},
        )

        outcome = await apply_async(
            await session.connection(),
            FailStaleTask(
                task_id=task_id,
                stale_after_ms=1_500,
                finalizing_stale_after_ms=2_750,
                result_json='{"err": 2}',
                error_code='WORKER_CRASHED',
                failed_reason='Worker crashed',
            ),
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
        await _heartbeat(session, task_id, seconds_ago=1, role='worker')

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER_SECONDS * 2)
        await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()

        replay = await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.FAIL_STALE

    async def test_a_locked_failure_is_a_conflict_not_a_replay(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _force_terminal(session, task_id, status='FAILED', kind='FAIL_RUNNING')

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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
        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
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
        outcome = await apply_async(
            await session.connection(), _fail_stale(str(uuid.uuid4())),
        )
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
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'EXPIRED'
        assert row.claimed is False
        assert row.claim_expires_at is None
        assert row.failed_at is not None
        assert row.error_code == 'TASK_EXPIRED'

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

        conninfo = DB_URL.replace('postgresql+psycopg://', 'postgresql://')
        with psycopg.connect(conninfo) as connection:
            with connection.cursor() as cursor:
                outcome = apply_sync(cursor, _expire_claim(task_id))
            connection.commit()

        assert isinstance(outcome, Applied)
        assert outcome.kind is TerminalizationKind.EXPIRE_CLAIMED

        status = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
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
                    SELECT id, status FROM horsies_tasks
                    WHERE id = ANY(:ids)
                """),
                {'ids': eligible + [fresh, undated]},
            )
        ).all()
        statuses = {row.id: row.status for row in rows}
        assert all(statuses[task_id] == 'EXPIRED' for task_id in eligible)
        assert statuses[fresh] == 'PENDING'
        assert statuses[undated] == 'PENDING'

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
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
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
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
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

        async with broker.async_engine.connect() as holder:
            await holder.execute(
                text('SELECT id FROM horsies_tasks WHERE id = :id FOR UPDATE'),
                {'id': held},
            )
            outcomes = await apply_batch_async(
                await session.connection(), _expire_pending(),
            )
            await session.commit()
            await holder.rollback()

        assert {o.task_id for o in outcomes} == {free}


def _cancel_locked(
    task_id: str,
    *permitted: TaskStatus,
) -> CancelLockedTask:
    return CancelLockedTask(
        task_id=task_id,
        fence=CallerHoldsRowLock(),
        permitted_source_statuses=(
            permitted or (TaskStatus.PENDING, TaskStatus.CLAIMED)
        ),
    )


def _cancel_owned_orphan(
    task_id: str,
    claimed_at: datetime | None = GENERATION,
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
                id, name, status, on_error, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            )
            VALUES (
                :id, 'terminalization.test', :workflow_status, 'FAIL', 0, :id,
                NOW(), NOW(), NOW(), NOW()
            )
        """),
        {'id': workflow_id, 'workflow_status': workflow_status},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks (
                id, workflow_id, task_index, node_id, task_name, task_args,
                task_kwargs, queue_name, priority, dependencies,
                allow_failed_deps, join_type, is_subworkflow, status,
                task_id, created_at
            )
            VALUES (
                :id, :workflow_id, 0, 'node_0', 'terminalization.test',
                '[]', '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
                :node_status, :task_id, NOW()
            )
        """),
        {
            'id': str(uuid.uuid4()),
            'workflow_id': workflow_id,
            'node_status': node_status,
            'task_id': task_id,
        },
    )
    await session.commit()
    return workflow_id


class TestCancelLockedTask:
    async def test_applies_with_the_operator_owned_literals_and_pre_image(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')

        outcome = await apply_async(
            await session.connection(),
            _cancel_locked(task_id),
        )
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
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'CANCELLED'
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Cancelled via monitoring API'
        assert row.failed_at is not None
        assert row.terminal_at == outcome.terminal_at
        assert row.claimed is False
        assert row.claimed_by_worker_id is None
        assert row.claimed_at is None
        assert row.finalizing_at is None
        assert row.finalizing_by_worker_id is None

    async def test_running_requires_explicit_permission(
        self,
        session: AsyncSession,
    ) -> None:
        refused_id = await _seed(session)
        refused = await apply_async(
            await session.connection(),
            _cancel_locked(refused_id),
        )
        await session.commit()
        assert isinstance(refused, SourceStateConflict)
        assert refused.observed.status is TaskStatus.RUNNING

        allowed_id = await _seed(session)
        allowed = await apply_async(
            await session.connection(),
            _cancel_locked(
                allowed_id,
                TaskStatus.PENDING,
                TaskStatus.CLAIMED,
                TaskStatus.RUNNING,
            ),
        )
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

        outcome = await apply_async(
            await session.connection(),
            _cancel_locked(task_id),
        )
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        status = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED')
        command = _cancel_locked(task_id)
        await apply_async(await session.connection(), command)
        await session.commit()

        replay = await apply_async(await session.connection(), command)
        await session.commit()
        assert isinstance(replay, AlreadyApplied)
        assert replay.kind is TerminalizationKind.CANCEL_ADMIN

    async def test_a_terminal_status_in_the_array_cannot_overwrite_another_kind(
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

        outcome = await apply_async(
            await session.connection(),
            _cancel_locked(task_id, TaskStatus.CANCELLED),
        )
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedForeignTerminalization)
        assert outcome.evidence.committed_kind is TerminalizationKind.CANCEL_ORPHAN

    async def test_absence_is_an_outcome(
        self,
        session: AsyncSession,
    ) -> None:
        outcome = await apply_async(
            await session.connection(),
            _cancel_locked(str(uuid.uuid4())),
        )
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
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'CANCELLED'
        assert row.error_code == 'WORKFLOW_CHECK_FAILED'
        assert row.failed_reason == (
            'Workflow task orphaned: no live workflow_task linkage'
        )
        assert row.failed_at is None
        assert row.terminal_at == outcome.terminal_at
        assert row.claimed is False
        assert row.claimed_by_worker_id is None
        assert row.claimed_at is None

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
                text('SELECT id, status FROM horsies_tasks WHERE id = ANY(:ids)'),
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
                    SELECT COUNT(*) FROM horsies_tasks
                    WHERE id = ANY(:ids) AND status = 'CLAIMED'
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
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
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

        async with broker.async_engine.connect() as holder:
            await holder.execute(
                text('SELECT id FROM horsies_tasks WHERE id = :id FOR UPDATE'),
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
    claimed_at: datetime | None = GENERATION,
) -> AbandonOwnedNode:
    return AbandonOwnedNode(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER, claimed_at=claimed_at),
    )


def _cancel_owned_node(
    task_id: str,
    claimed_at: datetime | None = GENERATION,
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
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.status == 'CANCELLED'
        assert row.claimed is False
        assert row.claimed_at is None
        assert row.claimed_by_worker_id is None
        assert row.claim_expires_at is None
        assert row.finalizing_at is None
        assert row.finalizing_by_worker_id is None
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Workflow paused before task start'
        assert row.failed_at is None
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

    async def test_a_null_generation_keeps_the_worker_fence(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        outcome = await apply_async(
            await session.connection(),
            _abandon_owned(task_id, None),
        )
        await session.commit()
        assert isinstance(outcome, Applied)
        assert outcome.observed.worker_id == WORKER
        assert outcome.observed.claimed_at == GENERATION

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
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'PENDING'

    async def test_preserves_error_summary_columns_it_does_not_own(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET error_code = 'OLD_CODE', failed_reason = 'old reason'
                WHERE id = :id
            """),
            {'id': task_id},
        )
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
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
        ).one()
        assert row.error_code == 'OLD_CODE'
        assert row.failed_reason == 'old reason'
        assert row.failed_at is None

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
        conninfo = DB_URL.replace('postgresql+psycopg://', 'postgresql://')
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
        legacy_id = await _seed(
            session,
            status='CLAIMED',
            is_workflow_task=True,
        )
        await _force_terminal(
            session,
            legacy_id,
            status='CANCELLED',
            kind=None,
        )
        absent_id = str(uuid.uuid4())
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET error_code = 'OLD_CODE', failed_reason = 'old reason'
                WHERE id = :id
            """),
            {'id': applied_id},
        )
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
                    (legacy_id, GENERATION),
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
            legacy_id,
            absent_id,
        ]
        assert [outcome.ordinality for outcome in outcomes] == list(range(1, 8))
        assert isinstance(outcomes[0], Applied)
        assert outcomes[0].kind is applied_kind
        assert isinstance(outcomes[1], LostClaim)
        assert isinstance(outcomes[2], SourceStateConflict)
        assert isinstance(outcomes[3], AlreadyApplied)
        assert isinstance(outcomes[4], SourceStateConflict)
        assert isinstance(outcomes[4].evidence, ObservedForeignTerminalization)
        assert isinstance(outcomes[5], SourceStateConflict)
        assert isinstance(outcomes[5].evidence, ObservedForeignTerminalization)
        assert outcomes[5].evidence.committed_kind is None
        assert isinstance(outcomes[6], TaskAbsent)

        summary = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': applied_id},
            )
        ).one()
        if command_type is AbandonOwnedNodes:
            assert summary.error_code == 'TASK_CANCELLED'
            assert summary.failed_reason == 'Workflow paused before task start'
        else:
            assert summary.error_code == 'OLD_CODE'
            assert summary.failed_reason == 'old reason'

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
                "ARRAY['one', 'two']",
                "ARRAY['2026-08-04 09:00:00+00']",
                'lengths differ',
            ),
            (
                'ARRAY[NULL]::varchar[]',
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
                        CAST({ids} AS VARCHAR[]),
                        CAST({generations_sql} AS TIMESTAMPTZ[]),
                        :worker_id
                    )
                """),
                {'worker_id': WORKER},
            )
        await session.rollback()

        status = (
            await session.execute(
                text('SELECT status FROM horsies_tasks WHERE id = :id'),
                {'id': task_id},
            )
        ).scalar_one()
        assert status == 'CLAIMED'

    @pytest.mark.parametrize('command_type', [AbandonOwnedNodes, CancelOwnedNodes])
    async def test_a_null_generation_keeps_the_per_task_worker_fence(
        self,
        session: AsyncSession,
        command_type: type[AbandonOwnedNodes] | type[CancelOwnedNodes],
    ) -> None:
        task_id = await _seed(session, status='CLAIMED', is_workflow_task=True)
        command = command_type(
            fence=OwnedClaimBatch(
                worker_id=WORKER,
                claim_generations=((task_id, None),),
            ),
        )
        outcomes = await apply_batch_async(await session.connection(), command)
        await session.commit()
        assert len(outcomes) == 1
        assert isinstance(outcomes[0], Applied)
        assert outcomes[0].observed.claimed_at == GENERATION

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
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': eligible},
            )
        ).one()
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Workflow paused before task start'
        assert row.failed_at is None
        persisted = (
            await session.execute(
                text("""
                    SELECT status, terminalization_kind,
                           claimed_by_worker_id, claimed_at
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': eligible},
            )
        ).one()
        assert persisted.status == 'CANCELLED'
        assert persisted.terminalization_kind == 'PAUSE_ABANDON_WORKFLOW'
        assert persisted.claimed_by_worker_id is None
        assert persisted.claimed_at is None
        refused_rows = (
            await session.execute(
                text('SELECT id, status FROM horsies_tasks WHERE id = ANY(:ids)'),
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

        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET error_code = 'OLD_CODE', failed_reason = 'old reason'
                WHERE id = :id
            """),
            {'id': task_ids[0]},
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
        summary = (
            await session.execute(
                text("""
                    SELECT error_code, failed_reason, failed_at
                    FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_ids[0]},
            )
        ).one()
        assert summary.error_code == 'OLD_CODE'
        assert summary.failed_reason == 'old reason'
        assert summary.failed_at is None

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
                text('SELECT id, status FROM horsies_tasks WHERE id = ANY(:ids)'),
                {'ids': [live_workflow_task, ready_link_task]},
            )
        ).all()
        assert {row.status for row in rows} == {'CLAIMED'}
