"""Integration tests for the horsies.monitoring cancel action.

The action mutates real rows through the ``broker`` fixture. Coverage:

1. cancel_task — every eligible status, every diagnosis code, idempotence
2. cancel_task side effects — cleared claim fields, end timestamp, task_done
3. cancel of a RUNNING task — a later finalize cannot overwrite it and
   writes no attempt row
4. concurrent claim vs cancel — exactly one effect lands
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.lifecycle.commands import CompleteTaskFused
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    Applied,
    SourceStateConflict,
    TerminalizationOutcome,
)
from horsies.core.lifecycle.persistence import apply_async
from horsies.core.models.tasks import TaskResult
from horsies._core import CoreConnectionError
from horsies.core.types.result import is_err, is_ok
from horsies.core.types.status import TaskStatus
from horsies.core.utils.url import to_psycopg_url
from horsies.core.worker.sql import HORSIES_CLAIM_SQL
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.monitoring import (
    TaskActionDbOperationFailed,
    TaskActionErrorCode,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionStoredDataInvalid,
    TaskActionWorkflowTask,
    cancel_task,
)
from tests.integration.conftest import DB_URL
from tests.integration.history_seeding import stored_history_status
from tests.integration.monitoring_seeding import (
    FIXTURE_WORKER_PID,
    FIXTURE_WORKER_PROCESS_NAME,
    AttemptRow,
    NodeRow,
    SeedRow,
    TaskNode,
    TaskRow,
    WorkflowRow,
    insert_rows,
    task_row,
)
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

UTC = timezone.utc

WORKER_ID = 'worker-actions-aaaa'
OTHER_WORKER_ID = 'worker-actions-bbbb'


# --------------------------------------------------------------------------- #
# Fixtures and helpers
# --------------------------------------------------------------------------- #
@pytest_asyncio.fixture
async def clean_task_tables(
    session: AsyncSession,
    broker: PostgresBroker,  # noqa: ARG001 - ensures migrations are applied
) -> AsyncGenerator[None, None]:
    """Empty the task and workflow tables so claim caps and counts are exact."""
    await session.execute(
        text(
            'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks, '
            'horsies_task_history CASCADE'
        )
    )
    await session.commit()
    yield


@pytest_asyncio.fixture
async def unreachable_broker() -> AsyncGenerator[PostgresBroker, None]:
    """A broker pointed at a port nothing listens on."""
    broken = PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(
                'postgresql://postgres:none@127.0.0.1:1/none'
            )
        ),
        assume_initialized=True,
    )
    yield broken
    await broken.close_async()


def ago(seconds: int) -> datetime:
    """A timestamp ``seconds`` in the past, aware UTC."""
    return datetime.now(UTC) - timedelta(seconds=seconds)


def ahead(seconds: int) -> datetime:
    """A timestamp ``seconds`` in the future, aware UTC."""
    return datetime.now(UTC) + timedelta(seconds=seconds)


def make_task(
    *,
    task_name: str = 'action_task',
    queue_name: str = 'default',
    status: TaskStatus = TaskStatus.PENDING,
    is_workflow_task: bool = False,
    retry_count: int = 0,
    max_retries: int = 3,
    good_until: datetime | None = None,
    worker_id: str | None = None,
    claimed_at: datetime | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    failed_at: datetime | None = None,
    error_code: str | None = None,
    failed_reason: str | None = None,
) -> TaskRow:
    """Build a task row in an arbitrary lifecycle state.

    A claimed row holds its lease for another minute; a terminal row is
    dated by whichever end timestamp it carries, else now.
    """
    return task_row(
        task_id=str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        status=status,
        priority=100,
        retry_count=retry_count,
        max_retries=max_retries,
        is_workflow_task=is_workflow_task,
        enqueued_at=ago(120),
        claimed_at=claimed_at,
        started_at=started_at,
        completed_at=completed_at,
        failed_at=failed_at,
        good_until=good_until,
        error_code=error_code,
        failed_reason=failed_reason,
        worker_id=worker_id,
        worker_hostname='host-a' if worker_id is not None else None,
        worker_pid=4242 if worker_id is not None else None,
        worker_process_name='proc-a' if worker_id is not None else None,
        claim_expires_at=(
            ahead(60)
            if status is TaskStatus.CLAIMED and worker_id is not None
            else None
        ),
    )


def make_attempt(
    *,
    task_id: str,
    attempt: int,
    outcome: str = 'FAILED',
    error_message: str | None = None,
) -> AttemptRow:
    """Build a recorded attempt for a task."""
    return AttemptRow(
        task_id=task_id,
        attempt=attempt,
        outcome=outcome,
        will_retry=False,
        started_at=ago(100 - attempt),
        finished_at=ago(90 - attempt),
        error_code=None,
        error_message=error_message,
        failed_reason=None,
        worker_id=WORKER_ID,
        worker_hostname='host-a',
        worker_pid=FIXTURE_WORKER_PID,
        worker_process_name=FIXTURE_WORKER_PROCESS_NAME,
    )


async def persist(session: AsyncSession, *rows: SeedRow) -> None:
    """Persist rows on their lifecycle side and commit.

    Terminal-status tasks are seeded as history rows
    (``tests/integration/monitoring_seeding.py``).
    """
    await insert_rows(session, *rows)


async def read_task(session: AsyncSession, task_id: str) -> Any:
    """Re-read a task's action-relevant columns, bypassing identity map.

    A live row has no terminal columns: its error, failed reason, end
    times, result and kind read NULL, and ``claimed`` reads FALSE. A
    terminal task lives in history — the move deleted its live row — so
    the read falls through to the history row, presented with the live
    column names the assertions use (the terminal instant lands in
    ``completed_at`` for COMPLETED and ``failed_at`` otherwise; claim and
    finalize columns are structurally absent on a history row).
    """
    row = (
        await session.execute(
            text("""
                SELECT status,
                       NULL::text AS error_code,
                       NULL::text AS failed_reason,
                       NULL::timestamptz AS failed_at,
                       NULL::timestamptz AS completed_at,
                       started_at,
                       NULL::text AS result,
                       FALSE AS claimed,
                       claimed_at,
                       claimed_by_worker_id, claim_expires_at, finalizing_at,
                       finalizing_by_worker_id, retry_count, max_retries,
                       good_until, enqueued_at, next_retry_at, worker_pid,
                       worker_hostname, worker_process_name, queue_name,
                       NULL::text AS terminalization_kind
                FROM horsies_tasks WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).first()
    if row is not None:
        return row
    history = (
        await session.execute(
            text("""
                SELECT status,
                       error_code,
                       final_failed_reason AS failed_reason,
                       CASE WHEN status <> 'COMPLETED' THEN terminal_at END
                           AS failed_at,
                       CASE WHEN status = 'COMPLETED' THEN terminal_at END
                           AS completed_at,
                       started_at,
                       convert_from(result_payload, 'UTF8') AS result,
                       FALSE AS claimed,
                       claimed_at,
                       NULL AS claimed_by_worker_id,
                       NULL AS claim_expires_at,
                       NULL AS finalizing_at,
                       NULL AS finalizing_by_worker_id,
                       retry_count, max_retries,
                       good_until, enqueued_at,
                       NULL AS next_retry_at,
                       last_worker_pid AS worker_pid,
                       last_worker_hostname AS worker_hostname,
                       last_worker_process_name AS worker_process_name,
                       queue_name,
                       terminalization_kind
                FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).first()
    assert history is not None
    return history


async def attempt_rows(session: AsyncSession, task_id: str) -> list[Any]:
    """All recorded attempts for a task, attempt ascending."""
    return list(
        (
            await session.execute(
                text("""
                    SELECT attempt, outcome, error_message, finished_at
                    FROM horsies_task_attempts WHERE task_id = :id
                    ORDER BY attempt
                """),
                {'id': task_id},
            )
        ).all()
    )


def serialize_ok(value: object) -> str:
    """Build the strict task-result envelope for a seeded ok value."""
    encoded = dumps_json(encode_task_result(TaskResult(ok=value), JsonValue))
    assert is_ok(encoded)
    return encoded.ok_value


async def complete_fused(
    session: AsyncSession,
    *,
    task_id: str,
    claimed_at: datetime,
    result_json: str,
) -> TerminalizationOutcome:
    return await apply_async(
        await session.connection(),
        CompleteTaskFused(
            task_id=task_id,
            fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=claimed_at),
            result_json=result_json,
        ),
    )


async def next_notification(
    channel: str,
    trigger: Any,
    timeout_s: float = 5.0,
) -> psycopg.Notify | None:
    """LISTEN on ``channel``, run ``trigger``, return the first notification.

    The LISTEN is registered before the trigger runs, so a notification the
    trigger causes cannot be missed.
    """
    conn = await psycopg.AsyncConnection.connect(
        to_psycopg_url(DB_URL), autocommit=True
    )
    try:
        await conn.execute(sql.SQL('LISTEN {}').format(sql.Identifier(channel)))
        await trigger()
        notifications = conn.notifies()
        try:
            return await asyncio.wait_for(anext(notifications), timeout=timeout_s)
        except TimeoutError:
            return None
        finally:
            await notifications.aclose()
    finally:
        await conn.close()


async def claim_once(broker: PostgresBroker, worker_id: str) -> list[str]:
    """Run one real claim pass for ``worker_id`` and return the ids it took."""
    params: dict[str, Any] = {
        'p_worker_id': worker_id,
        'p_queues': json.dumps(['default']),
        'p_queue_priority': json.dumps({'default': 100}),
        'p_queue_max_concurrency': json.dumps({}),
        'p_hard_cap_mode': False,
        'p_budgets': json.dumps({'ISOLATED': 4}),
        'p_routes': json.dumps(
            {
                'ISOLATED': 'ISOLATED',
                'SHARED_ASYNC': 'ISOLATED',
                'SHARED_THREAD': 'ISOLATED',
            },
        ),
        'p_prefetch_buffer': 2,
        'p_max_claim_per_worker': 10,
        'p_max_claim_batch': 10,
        'p_cluster_wide_cap': None,
        'p_lease_ms': 60_000,
        'p_lock_keys': json.dumps([987654321]),
    }
    async with current_resources(broker).session_factory() as session:
        rows = (await session.execute(HORSIES_CLAIM_SQL, params)).fetchall()
        await session.commit()
    return [str(row.id) for row in rows if row.row_kind == 'CLAIMED']


# --------------------------------------------------------------------------- #
# cancel_task — happy paths
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_task_tables')
class TestCancelTaskSucceeds:
    """Every status that has not begun user code cancels cleanly."""

    async def test_pending_task_is_cancelled(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        result = await cancel_task(broker, uuid.UUID(task.id))

        assert is_ok(result)
        assert result.ok_value.was_status is TaskStatus.PENDING
        assert result.ok_value.task_id == uuid.UUID(task.id)
        row = await read_task(session, task.id)
        assert row.status == 'CANCELLED'
        assert row.error_code == 'TASK_CANCELLED'
        assert row.failed_reason == 'Cancelled via monitoring API'
        assert row.terminalization_kind == 'CANCEL_ADMIN'

    async def test_claimed_task_is_cancelled_and_claim_released(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(
            status=TaskStatus.CLAIMED, worker_id=WORKER_ID, claimed_at=ago(5)
        )
        await persist(session, task)

        result = await cancel_task(broker, uuid.UUID(task.id))

        assert is_ok(result)
        assert result.ok_value.was_status is TaskStatus.CLAIMED
        row = await read_task(session, task.id)
        assert row.status == 'CANCELLED'
        # The claim is released by the move itself: no live row remains
        # to claim. The history row preserves the claim's timestamp as
        # provenance while carrying no claim state.
        assert row.claimed is False
        assert row.claimed_at is not None
        assert row.claimed_by_worker_id is None
        assert row.claim_expires_at is None
        assert row.finalizing_at is None
        assert row.finalizing_by_worker_id is None

    async def test_running_task_requires_the_explicit_opt_in(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(
            status=TaskStatus.RUNNING,
            worker_id=WORKER_ID,
            claimed_at=ago(20),
            started_at=ago(15),
        )
        await persist(session, task)

        refused = await cancel_task(broker, uuid.UUID(task.id))

        assert is_err(refused)
        assert type(refused.err_value) is TaskActionNotCancellable
        assert refused.err_value == TaskActionNotCancellable(
            message=f'Task {uuid.UUID(task.id)} cannot be cancelled from status RUNNING.',
            task_id=uuid.UUID(task.id),
            current_status=TaskStatus.RUNNING,
        )
        assert refused.err_value.current_status is TaskStatus.RUNNING
        assert (await read_task(session, task.id)).status == 'RUNNING'

        allowed = await cancel_task(broker, uuid.UUID(task.id), include_running=True)

        assert is_ok(allowed)
        assert allowed.ok_value.was_status is TaskStatus.RUNNING
        assert (await read_task(session, task.id)).status == 'CANCELLED'

    async def test_cancel_writes_an_end_timestamp(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """Without failed_at the row would have no measurable end."""
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        result = await cancel_task(broker, uuid.UUID(task.id))

        assert is_ok(result)
        row = await read_task(session, task.id)
        assert row.failed_at is not None
        assert row.completed_at is None

    async def test_cancel_records_no_attempt(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(
            status=TaskStatus.RUNNING,
            worker_id=WORKER_ID,
            claimed_at=ago(20),
            started_at=ago(15),
        )
        await persist(session, task)

        result = await cancel_task(broker, uuid.UUID(task.id), include_running=True)

        assert is_ok(result)
        assert await attempt_rows(session, task.id) == []

    async def test_cancel_fires_the_terminal_status_notification(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """Waiting result handles unblock on the trigger, not on a manual notify."""
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        async def do_cancel() -> None:
            result = await cancel_task(broker, uuid.UUID(task.id))
            assert is_ok(result)

        notification = await next_notification('task_done', do_cancel)

        assert notification is not None
        assert notification.payload == task.id


# --------------------------------------------------------------------------- #
# cancel_task — diagnosis
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_task_tables')
class TestCancelTaskRefuses:
    """Each refusal names its cause and, for conflicts, the observed status."""

    async def test_missing_task(self, broker: PostgresBroker) -> None:
        missing_id = uuid.uuid4()

        result = await cancel_task(broker, missing_id)

        assert is_err(result)
        assert type(result.err_value) is TaskActionNotFound
        assert result.err_value == TaskActionNotFound(
            message=f'Task {missing_id} does not exist.', task_id=missing_id,
        )
        assert result.err_value.code is TaskActionErrorCode.TASK_NOT_FOUND
        assert result.err_value.task_id == missing_id
        assert result.err_value.retryable is False

    async def test_workflow_bound_task(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING, is_workflow_task=True)
        await persist(session, task)

        result = await cancel_task(broker, uuid.UUID(task.id))

        assert is_err(result)
        assert type(result.err_value) is TaskActionWorkflowTask
        assert result.err_value == TaskActionWorkflowTask(
            message=(
                f'Task {uuid.UUID(task.id)} belongs to a workflow; '
                'use the workflow actions instead.'
            ),
            task_id=uuid.UUID(task.id),
            current_status=TaskStatus.PENDING,
        )
        assert result.err_value.current_status is TaskStatus.PENDING
        assert (await read_task(session, task.id)).status == 'PENDING'

    @pytest.mark.parametrize(
        'status',
        [
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
            TaskStatus.EXPIRED,
        ],
    )
    async def test_terminal_task(
        self, broker: PostgresBroker, session: AsyncSession, status: TaskStatus
    ) -> None:
        task = make_task(status=status)
        await persist(session, task)

        result = await cancel_task(broker, uuid.UUID(task.id), include_running=True)

        assert is_err(result)
        assert type(result.err_value) is TaskActionNotCancellable
        assert result.err_value == TaskActionNotCancellable(
            message=(
                f'Task {uuid.UUID(task.id)} cannot be cancelled from status {status.value}.'
            ),
            task_id=uuid.UUID(task.id),
            current_status=status,
        )
        assert result.err_value.current_status is status

    async def test_second_cancel_is_a_conflict_not_a_second_effect(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)
        first = await cancel_task(broker, uuid.UUID(task.id))
        assert is_ok(first)
        first_failed_at = (await read_task(session, task.id)).failed_at

        second = await cancel_task(broker, uuid.UUID(task.id))

        assert is_err(second)
        assert isinstance(second.err_value, TaskActionNotCancellable)
        assert second.err_value.current_status is TaskStatus.CANCELLED
        assert (await read_task(session, task.id)).failed_at == first_failed_at

    async def test_history_row_with_an_unknown_status_is_stored_data_invalid(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.FAILED)
        await persist(session, task)

        async with stored_history_status(session, task.id, 'NOT_KNOWN'):
            result = await cancel_task(broker, uuid.UUID(task.id))

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionStoredDataInvalid)
        assert isinstance(error.exception, OutcomeDecodeError)
        assert type(error.exception) is OutcomeDecodeError
        assert error.task_id == uuid.UUID(task.id)
        assert error.retryable is False
        assert "'NOT_KNOWN'" in error.message
        # Python's texts: the message holds the decode error's text, and the
        # exception's text is that text.
        decode_text = str(error.exception)
        assert decode_text.startswith('terminalization outcome decode failed: ')
        assert "'NOT_KNOWN'" in decode_text
        assert error.exception.args == (decode_text,)
        assert error.message == f'Task cancel failed: stored data does not parse: {decode_text}'
        assert (await read_task(session, task.id)).status == 'FAILED'


# --------------------------------------------------------------------------- #
# cancel_task — the RUNNING/finalize interaction
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_task_tables')
class TestCancelRunningVersusFinalize:
    """A cancelled row survives the worker's later finalize attempt."""

    async def test_finalize_succeeds_when_the_task_was_not_cancelled(
        self, session: AsyncSession
    ) -> None:
        """Control: the same finalize call lands on an untouched RUNNING row.

        Without this, the no-op assertion below would also hold if the
        finalize arguments were simply wrong.
        """
        claimed_at = ago(20)
        task = make_task(
            status=TaskStatus.RUNNING,
            worker_id=WORKER_ID,
            claimed_at=claimed_at,
            started_at=ago(15),
        )
        await persist(session, task)

        finalized = await complete_fused(
            session,
            task_id=task.id,
            claimed_at=claimed_at,
            result_json=serialize_ok(99),
        )
        await session.commit()

        assert isinstance(finalized, Applied)
        assert (await read_task(session, task.id)).status == 'COMPLETED'
        # Attempts purge with the move; the history row's snapshot is
        # their only home once the task is terminal.
        assert await attempt_rows(session, task.id) == []

    async def test_finalize_after_cancel_is_a_no_op(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        claimed_at = ago(20)
        task = make_task(
            status=TaskStatus.RUNNING,
            worker_id=WORKER_ID,
            claimed_at=claimed_at,
            started_at=ago(15),
        )
        await persist(session, task)
        cancelled = await cancel_task(broker, uuid.UUID(task.id), include_running=True)
        assert is_ok(cancelled)

        finalized = await complete_fused(
            session,
            task_id=task.id,
            claimed_at=claimed_at,
            result_json=serialize_ok(99),
        )
        await session.commit()

        assert isinstance(finalized, SourceStateConflict)
        row = await read_task(session, task.id)
        assert row.status == 'CANCELLED'
        assert row.result is None
        assert row.completed_at is None
        assert await attempt_rows(session, task.id) == []


# --------------------------------------------------------------------------- #
# Races
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_task_tables')
class TestClaimCancelRace:
    """A claim and a cancel on one row cannot both take effect."""

    async def test_cancel_of_a_freshly_claimed_row_still_wins(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """Serialized: the claim lands first, then the cancel takes the row."""
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)
        assert await claim_once(broker, WORKER_ID) == [task.id]

        result = await cancel_task(broker, uuid.UUID(task.id))

        assert is_ok(result)
        assert result.ok_value.was_status is TaskStatus.CLAIMED
        row = await read_task(session, task.id)
        assert row.status == 'CANCELLED'
        assert row.claimed_by_worker_id is None

    async def test_concurrent_claim_and_cancel_leave_one_consistent_outcome(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        cancelled, claimed_ids = await asyncio.gather(
            cancel_task(broker, uuid.UUID(task.id)),
            claim_once(broker, OTHER_WORKER_ID),
        )

        row = await read_task(session, task.id)
        if is_ok(cancelled):
            # The cancel committed: the row is terminal and stays claimable by
            # nobody, whether or not the claim statement touched it first.
            assert cancelled.ok_value.was_status in (
                TaskStatus.PENDING,
                TaskStatus.CLAIMED,
            )
            assert row.status == 'CANCELLED'
            assert row.claimed_by_worker_id is None
        else:
            # The only way to lose is the row having moved past CLAIMED.
            assert isinstance(cancelled.err_value, TaskActionNotCancellable)
            assert cancelled.err_value.current_status not in (
                TaskStatus.PENDING,
                TaskStatus.CLAIMED,
            )
            assert row.status != 'CANCELLED'
        assert claimed_ids in ([], [task.id])
        assert await attempt_rows(session, task.id) == []


# --------------------------------------------------------------------------- #
# Workflow-bound rows
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures('clean_task_tables')
class TestWorkflowBoundRowsAreUntouched:
    """The cancel action never edits a row a workflow owns."""

    async def test_cancel_does_not_mutate_a_real_workflow_node_task(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        workflow = WorkflowRow(
            id=str(uuid.uuid4()),
            name='alpha_flow',
            status='RUNNING',
            on_error='fail',
            definition_key=None,
            parent=None,
            sent_at=ago(120),
            created_at=ago(120),
            completed_at=None,
        )
        await persist(session, workflow)
        task = make_task(status=TaskStatus.FAILED, is_workflow_task=True)
        await persist(session, task)
        await persist(
            session,
            NodeRow(
                id=str(uuid.uuid4()),
                workflow_id=workflow.id,
                task_index=0,
                node_id=None,
                task_name='action_task',
                status='FAILED',
                dependencies=(),
                allow_failed_deps=False,
                kind=TaskNode(task_id=task.id),
                result=None,
                created_at=ago(120),
                started_at=None,
                completed_at=None,
            ),
        )

        cancelled = await cancel_task(broker, uuid.UUID(task.id), include_running=True)

        assert is_err(cancelled)
        assert isinstance(cancelled.err_value, TaskActionWorkflowTask)
        assert cancelled.err_value.current_status is TaskStatus.FAILED
        row = await read_task(session, task.id)
        assert row.status == 'FAILED'
        assert row.retry_count == 0


# --------------------------------------------------------------------------- #
# Database failure
# --------------------------------------------------------------------------- #
class TestDatabaseFailure:
    """An unreachable database is an Err, never an exception."""

    async def test_cancel_reports_a_retryable_db_failure(
        self, unreachable_broker: PostgresBroker
    ) -> None:
        task_id = uuid.uuid4()
        result = await cancel_task(unreachable_broker, task_id)

        assert is_err(result)
        error = result.err_value
        assert type(error) is TaskActionDbOperationFailed
        assert error.retryable is True
        assert error.task_id == task_id
        assert isinstance(error.exception, CoreConnectionError)
        assert error.exception.retryable is True
        # Python's texts: the message holds the cause's text, and the
        # exception is that text with the subject of the cancel.
        assert error.message.startswith('Task cancel failed: ')
        cause_text = error.message.removeprefix('Task cancel failed: ')
        assert cause_text != ''
        assert error.exception.subject == 'cancel_task'
        assert error.exception.message == cause_text
        assert str(error.exception) == f'cancel_task: {cause_text}'
