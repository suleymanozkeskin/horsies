"""Integration tests for finalize-path status and ownership guards.

Proves that a late-returning worker cannot overwrite a task's status after the
reaper has already reclaimed it, and that a stale worker cannot clobber an
attempt that was re-claimed by another worker (row RUNNING again under a new
owner).  Each test inserts a task in RUNNING state, simulates the competing
transition, then fires the guarded statement and asserts it writes nothing.
The failure and retry writes go through ``horsies_finish_failed_attempt``,
whose fence is the one the locked read and the retry update carried.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.commands import (
    CompleteLockedTask,
    CompleteTaskFused,
)
from horsies.core.lifecycle.fences import OwnedClaim, PriorLockedRead
from horsies.core.lifecycle.outcomes import (
    Applied,
    FailedAttemptOutcome,
    LostClaim,
    RetryScheduled,
    SourceStateConflict,
    SuccessApplied,
    TerminalizationOutcome,
    WorkflowSuccessOutcome,
    decode_workflow_success_row,
)
from horsies.core.lifecycle.persistence import apply_async
from horsies.core.worker.child_runner import _mark_task_finalizing
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    FinishFailedAttempt,
    NoRetry,
    RetryAfter,
    RetryRequest,
    finish_failed_attempt,
)
from horsies.core.worker.shared_stop import RetryPlacement
from horsies.core.worker.sql import READ_IN_FLIGHT_ATTEMPT_SQL
from horsies.core.worker.workflow_success import (
    FinishWorkflowSuccess,
    success_statement,
)
from horsies.core.worker.worker import (
    _confirm_ownership_and_set_running,
    _initialize_worker_pool,
    UNCLAIM_CLAIMED_TASK_SQL,
)
from tests.integration.conftest import compute_test_enqueue_sha
from tests.integration.history_seeding import force_terminal
from tests.broker_resources_support import listener_of

pytestmark = [pytest.mark.integration]

# Stable worker ids for ownership-guard scenarios.
OWNER_WORKER_ID = 'worker-owner-aaaa'
OTHER_WORKER_ID = 'worker-other-bbbb'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_running_task(
    session: AsyncSession,
    *,
    claimed_by_worker_id: str = OWNER_WORKER_ID,
    claimed_at: datetime | None = None,
) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state and return its id."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='guard_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id,
                 claimed_at,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'guard_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 3, NOW(), :enqueue_sha, :claimed_by_worker_id,
                 :claimed_at,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'claimed_by_worker_id': claimed_by_worker_id,
            'claimed_at': claimed_at,
        },
    )
    await session.flush()
    return task_id


async def _insert_running_task_with_retry(
    session: AsyncSession,
    *,
    good_until: datetime,
    retry_count: int = 0,
    max_retries: int = 3,
    intervals: list[int] | None = None,
) -> str:
    """Insert RUNNING task row with retry policy metadata."""
    if intervals is None:
        intervals = [1, 1, 1]

    task_id = str(uuid.uuid4())
    task_options = json.dumps({
        'retry_policy': {
            'max_retries': max_retries,
            'intervals': intervals,
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['TRANSIENT'],
        },
        'good_until': good_until.isoformat(),
    })

    sent_at, sha = compute_test_enqueue_sha(
        task_name='guard_test_retry',
        good_until=good_until,
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs, status, sent_at,
                 created_at, updated_at, claimed, retry_count, max_retries, started_at,
                 good_until, task_options, enqueue_sha,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'guard_test_retry', 'default', 100, '[]', '{}', 'RUNNING', :sent_at,
                 NOW(), NOW(), FALSE, :retry_count, :max_retries, NOW(),
                 :good_until, :task_options, :enqueue_sha,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'good_until': good_until,
            'task_options': task_options,
        },
    )
    await session.flush()
    return task_id


async def _get_task_status(session: AsyncSession, task_id: str) -> str:
    """Read current status of a task."""
    result = await session.execute(
        text("SELECT status FROM itest_task_rows WHERE id = CAST(:id AS uuid)"),
        {'id': task_id},
    )
    row = result.fetchone()
    assert row is not None, f'Task {task_id} not found'
    return str(row[0])


async def _set_task_status(
    session: AsyncSession,
    task_id: str,
    status: str,
) -> None:
    """Force-set a task's status (simulating reaper intervention).

    A terminal status is forced the way terminalization delivers it —
    the row moves to history — while a non-terminal status is a live
    transition. Production holds the same split, so a fixture that wrote
    a terminal status into the live table would be simulating a state
    the database cannot hold.
    """
    if status in ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'):
        await force_terminal(session, task_id, status=status)
        await session.flush()
        return
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = :status,
                terminal_at = NULL
            WHERE id = CAST(:id AS uuid)
        """),
        {'status': status, 'id': task_id},
    )
    await session.flush()


async def _complete_locked(
    session: AsyncSession,
    task_id: str,
    *,
    worker_id: str = OWNER_WORKER_ID,
    result_json: str = '{"ok": "result"}',
) -> TerminalizationOutcome:
    """``horsies_complete_locked_task``, which classifies its own miss."""
    command = CompleteLockedTask(
        task_id=task_id,
        fence=PriorLockedRead(worker_id=worker_id),
        result_json=result_json,
    )
    return await apply_async(await session.connection(), command)


async def _link_workflow_node(session: AsyncSession, task_id: str) -> None:
    """Make ``task_id`` the task of a RUNNING node of a RUNNING workflow."""
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            UPDATE horsies_tasks SET is_workflow_task = TRUE
            WHERE id = CAST(:id AS uuid)
        """),
        {'id': task_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:wf, 'guard_test_wf', 'RUNNING', 'FAIL', 0, :wf,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'wf': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (id, workflow_id, task_index, node_id, task_name, task_args,
                 task_kwargs, queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at)
            VALUES (:id, :wf, 0, 'node-0', 'guard_test', '[]', '{}',
                    'default', 100, '{}', FALSE, 'all', FALSE, 'RUNNING',
                    CAST(:task_id AS uuid), NOW())
        """),
        {'id': str(uuid.uuid4()), 'wf': workflow_id, 'task_id': task_id},
    )
    await session.flush()


async def _finish_workflow_success(
    session: AsyncSession,
    task_id: str,
    *,
    worker_id: str,
    claimed_at: datetime | None,
) -> WorkflowSuccessOutcome:
    """The success of a workflow task, in the session's open transaction."""
    statement = success_statement(
        FinishWorkflowSuccess(
            task_id=task_id,
            fence=OwnedClaim(worker_id=worker_id, claimed_at=claimed_at),
            result_json='{"ok": "result"}',
        ),
    )
    result = await session.execute(statement.statement, dict(statement.parameters))
    return decode_workflow_success_row(
        {str(key): value for key, value in result.mappings().one().items()},
    )


async def _fail_attempt(
    session: AsyncSession,
    task_id: str,
    *,
    worker_id: str = OWNER_WORKER_ID,
    failed_reason: str | None = None,
    retry: RetryRequest = NoRetry(),
) -> FailedAttemptOutcome:
    """The failure or retry write, in the session's open transaction."""
    return await finish_failed_attempt(
        await session.connection(),
        FinishFailedAttempt(
            task_id=task_id,
            fence=OwnedClaim(worker_id=worker_id, claimed_at=None),
            retry=retry,
            result_json='{"err": {"error_code": "LATE"}}',
            error_code='LATE',
            failed_reason=failed_reason,
            attempt=AttemptFailure(
                kind=AttemptFailureKind.FAILED,
                error_message='late',
                failed_reason=failed_reason,
            ),
        ),
    )


def _retry_after(retry_count: int) -> RetryAfter:
    return RetryAfter(
        retry_count=retry_count,
        delay_s=1.0,
        placement=RetryPlacement.SAME_CLASS,
    )


async def _complete_fused(
    session: AsyncSession,
    task_id: str,
    *,
    claimed_at: datetime | None,
    result_json: str,
) -> TerminalizationOutcome:
    return await apply_async(
        await session.connection(),
        CompleteTaskFused(
            task_id=task_id,
            fence=OwnedClaim(
                worker_id=OWNER_WORKER_ID,
                claimed_at=claimed_at,
            ),
            result_json=result_json,
            notify_channel='task_queue_default',
            notify_payload=f'capacity:{task_id}',
        ),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_complete_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Locked completion refuses a task that is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper marking task as FAILED
    await _set_task_status(session, task_id, 'FAILED')

    outcome = await _complete_locked(
        session,
        task_id,
        result_json='{"ok": "late_result"}',
    )

    assert isinstance(outcome, SourceStateConflict)
    assert await _get_task_status(session, task_id) == 'FAILED', (
        'Task status must remain FAILED after blocked finalize'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_complete_guard_succeeds_when_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Locked completion applies when the task is RUNNING."""
    task_id = await _insert_running_task(session)

    outcome = await _complete_locked(session, task_id)

    assert isinstance(outcome, Applied)
    assert await _get_task_status(session, task_id) == 'COMPLETED'


@pytest.mark.asyncio(loop_scope='function')
async def test_fail_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Locked failure refuses a task that is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper rescheduling a retry (RUNNING → PENDING)
    await _set_task_status(session, task_id, 'PENDING')

    outcome = await _fail_attempt(session, task_id)

    assert isinstance(outcome, SourceStateConflict)
    assert await _get_task_status(session, task_id) == 'PENDING', (
        'Task status must remain PENDING after blocked finalize'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_fail_worker_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Worker-level failure refuses a task that is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper intervention
    await _set_task_status(session, task_id, 'FAILED')

    outcome = await _fail_attempt(
        session,
        task_id,
        failed_reason='late worker failure',
    )

    assert isinstance(outcome, SourceStateConflict)


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_guard_blocks_when_not_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The retry write is a no-op when the task is no longer RUNNING."""
    task_id = await _insert_running_task(session)

    # Simulate reaper already marking FAILED
    await _set_task_status(session, task_id, 'FAILED')

    outcome = await _fail_attempt(session, task_id, retry=_retry_after(0))

    assert isinstance(
        outcome, SourceStateConflict
    ), 'the retry write must refuse a task that is not RUNNING'
    assert (
        await _get_task_status(session, task_id) == 'FAILED'
    ), 'Task status must remain FAILED after blocked retry'


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_guard_succeeds_when_running(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The retry write applies when the task IS running."""
    task_id = await _insert_running_task(session)

    outcome = await _fail_attempt(session, task_id, retry=_retry_after(0))

    assert isinstance(
        outcome, RetryScheduled
    ), 'the retry write must apply when status == RUNNING'
    assert await _get_task_status(session, task_id) == 'PENDING'


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_then_complete_race_sequence(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Full race scenario: reaper marks FAILED, late worker tries COMPLETED.

    Timeline:
      T=0  Task is RUNNING
      T=1  Reaper marks FAILED (simulated via MARK_STALE_TASK_FAILED_SQL pattern)
      T=2  Late worker completion is refused
      T=3  Assert: task is FAILED, result is reaper's WORKER_CRASHED, not worker's
    """
    task_id = await _insert_running_task(session)

    # T=1: Reaper marks FAILED with WORKER_CRASHED result
    reaper_result = '{"err": {"error_code": "WORKER_CRASHED", "message": "stale"}}'
    await force_terminal(
        session,
        task_id,
        status='FAILED',
        result_json=reaper_result,
        error_code='WORKER_CRASHED',
    )
    await session.flush()

    # T=2: Late worker finalize fires — should be blocked
    late_result = '{"ok": "I completed successfully!"}'
    outcome = await _complete_locked(
        session,
        task_id,
        result_json=late_result,
    )
    assert isinstance(outcome, SourceStateConflict)

    # T=3: Verify reaper's result is preserved
    row = (
        await session.execute(
            text("SELECT status, result FROM itest_task_rows WHERE id = CAST(:id AS uuid)"),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'FAILED', f'Expected FAILED, got {row[0]}'
    assert row[1] == reaper_result, (
        f'Reaper result must be preserved, got: {row[1]}'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_reaper_then_retry_race_sequence(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Race scenario: reaper marks FAILED, late worker tries to schedule retry.

    Timeline:
      T=0  Task is RUNNING
      T=1  Reaper marks FAILED
      T=2  Late worker tries the retry write → blocked
      T=3  Assert: task is FAILED, not PENDING (retry was blocked)
    """
    task_id = await _insert_running_task(session)

    # T=1: Reaper marks FAILED
    await _set_task_status(session, task_id, 'FAILED')

    # T=2: Late worker tries to schedule a retry
    retry_outcome = await _fail_attempt(session, task_id, retry=_retry_after(0))
    assert isinstance(retry_outcome, SourceStateConflict), 'Late retry must be blocked'

    # T=3: Task must remain FAILED
    assert await _get_task_status(session, task_id) == 'FAILED'


@pytest.mark.asyncio(loop_scope='function')
async def test_in_flight_read_returns_the_retry_facts(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """READ_IN_FLIGHT_ATTEMPT_SQL returns the status and the retry facts."""
    good_until = datetime.now(timezone.utc) + timedelta(seconds=60)
    task_id = await _insert_running_task_with_retry(
        session, good_until=good_until, retry_count=1
    )
    await session.execute(
        text(
            'UPDATE horsies_tasks SET claimed_by_worker_id = :wid '
            'WHERE id = CAST(:id AS uuid)'
        ),
        {'id': task_id, 'wid': OWNER_WORKER_ID},
    )

    row = (
        await session.execute(
            READ_IN_FLIGHT_ATTEMPT_SQL,
            {'id': task_id, 'wid': OWNER_WORKER_ID, 'claimed_at': None},
        )
    ).one()

    assert (row.status, row.retry_count, row.max_retries) == ('RUNNING', 1, 3)
    assert json.loads(row.task_options)['retry_policy']['auto_retry_for'] == [
        'TRANSIENT',
    ]


# ---------------------------------------------------------------------------
# Ownership guards: a stale worker must not clobber a re-claimed attempt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_success_blocks_on_ownership_mismatch(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The success of a workflow task refuses a RUNNING row that another
    worker owns, and writes nothing."""
    task_id = await _insert_running_task(
        session, claimed_by_worker_id=OTHER_WORKER_ID
    )
    await _link_workflow_node(session, task_id)

    outcome = await _finish_workflow_success(
        session, task_id, worker_id=OWNER_WORKER_ID, claimed_at=None
    )
    assert isinstance(outcome, LostClaim)
    assert await _get_task_status(session, task_id) == 'RUNNING'

    # Sanity: the actual owner still matches.
    owner = await _finish_workflow_success(
        session, task_id, worker_id=OTHER_WORKER_ID, claimed_at=None
    )
    assert isinstance(owner, SuccessApplied)


@pytest.mark.asyncio(loop_scope='function')
async def test_complete_guard_blocks_on_ownership_mismatch(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Locked completion refuses a row owned by another worker."""
    task_id = await _insert_running_task(
        session, claimed_by_worker_id=OTHER_WORKER_ID
    )

    outcome = await _complete_locked(
        session,
        task_id,
        result_json='{"ok": "stale_result"}',
    )
    assert isinstance(outcome, LostClaim)
    assert await _get_task_status(session, task_id) == 'RUNNING', (
        'Re-claimed RUNNING attempt must be left untouched'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_fail_guard_blocks_on_ownership_mismatch(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The failure write refuses a row owned by another worker."""
    task_id = await _insert_running_task(
        session, claimed_by_worker_id=OTHER_WORKER_ID
    )

    outcome = await _fail_attempt(session, task_id)
    assert isinstance(outcome, LostClaim)
    assert await _get_task_status(session, task_id) == 'RUNNING'


@pytest.mark.asyncio(loop_scope='function')
async def test_retry_guard_blocks_on_ownership_mismatch(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The retry write must not flip another worker's RUNNING row to PENDING."""
    task_id = await _insert_running_task(
        session, claimed_by_worker_id=OTHER_WORKER_ID
    )

    outcome = await _fail_attempt(session, task_id, retry=_retry_after(0))
    assert isinstance(
        outcome, LostClaim
    ), 'the retry write must be a no-op on ownership mismatch'
    assert (
        await _get_task_status(session, task_id) == 'RUNNING'
    ), 'In-flight attempt of the new owner must not be requeued by a stale worker'


@pytest.mark.asyncio(loop_scope='function')
async def test_stale_finalizer_after_reclaim_race_sequence(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Full race: reaper requeues, another worker re-claims and runs, stale
    finalizer of the original worker fires.

    Timeline:
      T=0  Worker A runs task (RUNNING, owner=A); its heartbeats stall.
      T=1  Reaper requeues the stale task (PENDING, owner cleared, retry+1).
      T=2  Worker B claims and starts it (RUNNING, owner=B).
      T=3  A's child finishes; A's finalize fires MARK_TASK_COMPLETED with wid=A.
      T=4  Assert: blocked — task stays RUNNING under B with no result written.
    """
    task_id = await _insert_running_task(
        session, claimed_by_worker_id=OWNER_WORKER_ID
    )

    # T=1: Reaper requeue (status-only transition, ownership cleared)
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'PENDING',
                retry_count = retry_count + 1,
                claimed_by_worker_id = NULL,
                started_at = NULL,
                updated_at = NOW()
            WHERE id = :id
        """),
        {'id': task_id},
    )

    # T=2: Worker B claims and starts running
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'RUNNING',
                claimed_by_worker_id = :wid,
                started_at = NOW(),
                updated_at = NOW()
            WHERE id = :id
        """),
        {'id': task_id, 'wid': OTHER_WORKER_ID},
    )
    await session.flush()

    # T=3: Worker A's stale finalizer fires — must be blocked
    outcome = await _complete_locked(
        session,
        task_id,
        result_json='{"ok": "stale A result"}',
    )
    assert isinstance(outcome, LostClaim)

    # T=4: B's in-flight attempt is untouched
    row = (
        await session.execute(
            text("""
                SELECT status, claimed_by_worker_id, result
                FROM itest_task_rows WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'RUNNING'
    assert row[1] == OTHER_WORKER_ID
    assert row[2] is None, 'Stale result must not be written'


# ---------------------------------------------------------------------------
# Claim-generation fence (C10)
#
# (status, worker_id) alone cannot reject a stale finalize when the SAME
# worker re-claimed its own reaper-requeued task: worker_id matches and the
# row is RUNNING again. The :claimed_at fence pins every owned-row statement
# to the claim generation the dispatch was born from; a requeue clears
# claimed_at and a re-claim stamps a new one, so a stale actor no longer
# matches. NULL disables the fence (pre-fence caller behavior).
# ---------------------------------------------------------------------------

# Fixed, deterministic claim generations. GEN_STALE simulates the dispatch
# born from the first claim; GEN_CURRENT is the row's generation after a
# requeue + re-claim by the same worker.
GEN_CURRENT = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
GEN_STALE = GEN_CURRENT - timedelta(minutes=1)


@pytest.mark.asyncio(loop_scope='function')
async def test_fused_finalize_rejects_stale_claim_generation(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Fused finalize is a no-op for a stale generation, applies for the live one."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)

    stale = await _complete_fused(
        session,
        task_id,
        claimed_at=GEN_STALE,
        result_json='{"ok": "stale_attempt_result"}',
    )
    assert isinstance(stale, LostClaim), (
        'Same worker id + RUNNING status must not be enough: a stale '
        'generation must be fenced out'
    )
    assert await _get_task_status(session, task_id) == 'RUNNING'

    live = await _complete_fused(
        session,
        task_id,
        claimed_at=GEN_CURRENT,
        result_json='{"ok": "live_attempt_result"}',
    )
    assert isinstance(live, Applied)
    assert await _get_task_status(session, task_id) == 'COMPLETED'


@pytest.mark.asyncio(loop_scope='function')
async def test_fused_finalize_null_fence_matches_any_generation(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """NULL fence preserves the (status, worker_id)-only behavior."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)

    outcome = await _complete_fused(
        session,
        task_id,
        claimed_at=None,
        result_json='{"ok": 1}',
    )
    assert isinstance(outcome, Applied)
    assert await _get_task_status(session, task_id) == 'COMPLETED'


@pytest.mark.asyncio(loop_scope='function')
async def test_workflow_success_rejects_stale_claim_generation(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The success of a workflow task refuses a row of a different claim
    generation, and applies under the current one."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)
    await _link_workflow_node(session, task_id)

    stale = await _finish_workflow_success(
        session, task_id, worker_id=OWNER_WORKER_ID, claimed_at=GEN_STALE
    )
    assert isinstance(stale, LostClaim)
    assert await _get_task_status(session, task_id) == 'RUNNING'

    live = await _finish_workflow_success(
        session, task_id, worker_id=OWNER_WORKER_ID, claimed_at=GEN_CURRENT
    )
    assert isinstance(live, SuccessApplied)


@pytest.mark.asyncio(loop_scope='function')
async def test_in_flight_read_rejects_stale_claim_generation(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The in-flight read of the failure paths skips a row from a different
    generation."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)

    stale_row = (
        await session.execute(
            READ_IN_FLIGHT_ATTEMPT_SQL,
            {'id': task_id, 'wid': OWNER_WORKER_ID, 'claimed_at': GEN_STALE},
        )
    ).fetchone()
    assert stale_row is None

    live_row = (
        await session.execute(
            READ_IN_FLIGHT_ATTEMPT_SQL,
            {'id': task_id, 'wid': OWNER_WORKER_ID, 'claimed_at': GEN_CURRENT},
        )
    ).fetchone()
    assert live_row is not None


@pytest.mark.asyncio(loop_scope='function')
async def test_unclaim_rejects_stale_claim_generation(
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Standalone requeue cannot release a CLAIMED row it did not claim."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)
    await _set_task_status(session, task_id, 'CLAIMED')

    stale = await session.execute(
        UNCLAIM_CLAIMED_TASK_SQL,
        {'id': task_id, 'wid': OWNER_WORKER_ID, 'claimed_at': GEN_STALE},
    )
    assert (getattr(stale, 'rowcount', 0) or 0) == 0
    assert await _get_task_status(session, task_id) == 'CLAIMED'

    live = await session.execute(
        UNCLAIM_CLAIMED_TASK_SQL,
        {'id': task_id, 'wid': OWNER_WORKER_ID, 'claimed_at': GEN_CURRENT},
    )
    assert (getattr(live, 'rowcount', 0) or 0) == 1
    assert await _get_task_status(session, task_id) == 'PENDING'


@pytest.mark.asyncio(loop_scope='function')
async def test_confirm_ownership_rejects_stale_claim_generation(
    session: AsyncSession,
    broker: 'PostgresBroker',
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The child's CLAIMED->RUNNING confirm aborts CLAIM_LOST for a stale
    generation and succeeds for the live one.

    This closes the soft-cap double-dispatch route: a buffered stale child
    whose row was re-claimed (same worker, expired lease) must not pass the
    confirm on the strength of (status, worker_id) alone.
    """
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)
    await _set_task_status(session, task_id, 'CLAIMED')
    await session.commit()  # the child confirm runs on its own connection

    _initialize_worker_pool(listener_of(broker).database_url)

    stale_result = _confirm_ownership_and_set_running(
        task_id, OWNER_WORKER_ID, False, GEN_STALE,
    )
    assert stale_result == (False, '', 'CLAIM_LOST')
    assert await _get_task_status(session, task_id) == 'CLAIMED'

    live_result = _confirm_ownership_and_set_running(
        task_id, OWNER_WORKER_ID, False, GEN_CURRENT,
    )
    assert live_result is None
    assert await _get_task_status(session, task_id) == 'RUNNING'


async def _get_finalizing_worker(session: AsyncSession, task_id: str) -> str | None:
    """Read the worker id of the task's finalizing marker."""
    result = await session.execute(
        text(
            'SELECT finalizing_by_worker_id FROM horsies_tasks '
            'WHERE id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    row = result.fetchone()
    assert row is not None, f'Task {task_id} not found'
    return None if row[0] is None else str(row[0])


@pytest.mark.asyncio(loop_scope='function')
async def test_finalizing_marker_rejects_stale_claim_generation(
    session: AsyncSession,
    broker: 'PostgresBroker',
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A stale attempt cannot mark a later claim of the same task finalizing."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)
    await session.commit()  # the child marker runs on its own connection

    _initialize_worker_pool(listener_of(broker).database_url)

    _mark_task_finalizing(task_id, OWNER_WORKER_ID, GEN_STALE)
    assert await _get_finalizing_worker(session, task_id) is None

    _mark_task_finalizing(task_id, OWNER_WORKER_ID, GEN_CURRENT)
    assert await _get_finalizing_worker(session, task_id) == OWNER_WORKER_ID


@pytest.mark.asyncio(loop_scope='function')
async def test_finalizing_marker_null_fence_matches_any_generation(
    session: AsyncSession,
    broker: 'PostgresBroker',
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A NULL claimed_at disables the generation half of the marker fence."""
    task_id = await _insert_running_task(session, claimed_at=GEN_CURRENT)
    await session.commit()

    _initialize_worker_pool(listener_of(broker).database_url)

    _mark_task_finalizing(task_id, OWNER_WORKER_ID, None)
    assert await _get_finalizing_worker(session, task_id) == OWNER_WORKER_ID
