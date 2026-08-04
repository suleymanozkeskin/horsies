"""The database-owned completion operations, against a real server.

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

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker

from horsies.core.lifecycle.commands import (
    CompleteLockedTask,
    CompleteTaskFused,
    FailLockedTask,
    FailStaleTask,
)
from horsies.core.lifecycle.fences import OwnedClaim, PriorLockedRead
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedClaim,
    ObservedForeignTerminalization,
    ObservedStaleness,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_async
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
        repeat('0', 64), FALSE, TRUE, :worker_id, :claimed_at, NOW()
    )
""")


async def _seed(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    worker_id: str = WORKER,
    claimed_at: datetime | None = GENERATION,
) -> str:
    task_id = str(uuid.uuid4())
    await session.execute(
        _SEED_SQL,
        {
            'id': task_id,
            'status': status,
            'worker_id': worker_id,
            'claimed_at': claimed_at,
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


STALE_AFTER = 60
FINALIZING_STALE_AFTER = 30


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
        stale_after_seconds=STALE_AFTER,
        finalizing_stale_after_seconds=FINALIZING_STALE_AFTER,
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
        await _age(session, task_id, started_seconds_ago=STALE_AFTER * 2)

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
        await _age(session, task_id, started_seconds_ago=STALE_AFTER * 4)
        await _heartbeat(session, task_id, seconds_ago=STALE_AFTER * 2)

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_a_recent_heartbeat_refuses_with_the_evidence(
        self,
        session: AsyncSession,
    ) -> None:
        """The refusal carries what the guard judged, not just that it refused."""
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER * 2)
        await _heartbeat(session, task_id, seconds_ago=1)

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.RUNNING
        evidence = outcome.evidence
        assert isinstance(evidence, ObservedStaleness)
        assert evidence.last_heartbeat_at is not None
        assert evidence.started_at is not None
        assert evidence.stale_after_seconds == STALE_AFTER
        assert evidence.finalizing_stale_after_seconds == FINALIZING_STALE_AFTER

    async def test_a_heartbeat_from_another_role_does_not_count(
        self,
        session: AsyncSession,
    ) -> None:
        """Only the runner's heartbeats prove the runner is alive."""
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER * 2)
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

    async def test_an_active_finalizer_defers(
        self,
        session: AsyncSession,
    ) -> None:
        """A finalizer inside its window holds the row against stale-failure."""
        task_id = await _seed(session)
        await _age(
            session,
            task_id,
            started_seconds_ago=STALE_AFTER * 2,
            finalizing_seconds_ago=1,
        )

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()
        assert isinstance(outcome, SourceStateConflict)
        assert isinstance(outcome.evidence, ObservedStaleness)

    async def test_a_stale_finalizer_does_not_defer(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _age(
            session,
            task_id,
            started_seconds_ago=STALE_AFTER * 2,
            finalizing_seconds_ago=FINALIZING_STALE_AFTER * 2,
        )

        outcome = await apply_async(await session.connection(), _fail_stale(task_id))
        await session.commit()
        assert isinstance(outcome, Applied)

    async def test_replay_is_already_applied(
        self,
        session: AsyncSession,
    ) -> None:
        task_id = await _seed(session)
        await _age(session, task_id, started_seconds_ago=STALE_AFTER * 2)
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
