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
from horsies.core.models.task_pg import TaskAttemptModel, TaskModel
from horsies.core.models.tasks import TaskResult
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel
from horsies.core.types.result import is_err, is_ok
from horsies.core.types.status import TaskStatus
from horsies.core.utils.url import to_psycopg_url
from horsies.core.worker.sql import HORSIES_CLAIM_SQL
from horsies.monitoring import (
    TaskActionErrorCode,
    cancel_task,
)
from tests.integration.conftest import DB_URL, compute_test_enqueue_sha

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
            'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks '
            'CASCADE'
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
                'postgresql+psycopg://postgres:none@127.0.0.1:1/none'
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


def _terminal_instant(
    status: TaskStatus,
    completed_at: datetime | None,
    failed_at: datetime | None,
) -> datetime | None:
    """The instant a terminal row is dated by, mirroring what production writes.

    Terminal exactly when dated is a database constraint, so a fixture that
    sets a terminal status without one is not a lighter fixture — it is a row
    that cannot exist. The instant is taken from whichever end timestamp the
    row carries, which keeps aged fixtures aged: a row completed sixty days ago
    is terminal sixty days ago, not now.
    """
    if not status.is_terminal:
        return None
    return completed_at or failed_at or datetime.now(timezone.utc)


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
    result: str | None = None,
    error_code: str | None = None,
    failed_reason: str | None = None,
) -> TaskModel:
    """Build a task row in an arbitrary lifecycle state."""
    sent_at, enqueue_sha = compute_test_enqueue_sha(
        task_name=task_name,
        queue_name=queue_name,
    )
    return TaskModel(
        id=str(uuid.uuid4()),
        task_name=task_name,
        queue_name=queue_name,
        priority=100,
        args='[]',
        kwargs='{}',
        status=status,
        sent_at=sent_at,
        enqueued_at=ago(120),
        claimed=worker_id is not None,
        claimed_at=claimed_at,
        claimed_by_worker_id=worker_id,
        claim_expires_at=ahead(60) if worker_id is not None else None,
        started_at=started_at,
        completed_at=completed_at,
        failed_at=failed_at,
        terminal_at=_terminal_instant(status, completed_at, failed_at),
        result=result,
        error_code=error_code,
        failed_reason=failed_reason,
        good_until=good_until,
        retry_count=retry_count,
        max_retries=max_retries,
        is_workflow_task=is_workflow_task,
        worker_pid=4242 if worker_id is not None else None,
        worker_hostname='host-a' if worker_id is not None else None,
        worker_process_name='proc-a' if worker_id is not None else None,
        enqueue_sha=enqueue_sha,
    )


def make_attempt(
    *,
    task_id: str,
    attempt: int,
    outcome: str = 'FAILED',
    error_message: str | None = None,
) -> TaskAttemptModel:
    """Build a recorded attempt for a task."""
    return TaskAttemptModel(
        task_id=task_id,
        attempt=attempt,
        outcome=outcome,
        will_retry=False,
        started_at=ago(100 - attempt),
        finished_at=ago(90 - attempt),
        error_message=error_message,
        worker_hostname='host-a',
    )


async def persist(session: AsyncSession, *rows: Any) -> None:
    """Persist rows and commit so other sessions observe them."""
    session.add_all(list(rows))
    await session.commit()


async def read_task(session: AsyncSession, task_id: str) -> Any:
    """Re-read a task row's action-relevant columns, bypassing identity map."""
    row = (
        await session.execute(
            text("""
                SELECT status, error_code, failed_reason, failed_at, completed_at,
                       started_at, result, claimed, claimed_at,
                       claimed_by_worker_id, claim_expires_at, finalizing_at,
                       finalizing_by_worker_id, retry_count, max_retries,
                       good_until, enqueued_at, next_retry_at, worker_pid,
                       worker_hostname, worker_process_name, queue_name,
                       terminalization_kind
                FROM horsies_tasks WHERE id = :id
            """),
            {'id': task_id},
        )
    ).first()
    assert row is not None
    return row


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
            notify_channel='task_queue_default',
            notify_payload=task_id,
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
        'p_processes': 4,
        'p_prefetch_buffer': 2,
        'p_max_claim_per_worker': 10,
        'p_max_claim_batch': 10,
        'p_cluster_wide_cap': None,
        'p_lease_ms': 60_000,
        'p_lock_keys': json.dumps([987654321]),
    }
    async with broker.session_factory() as session:
        rows = (await session.execute(HORSIES_CLAIM_SQL, params)).fetchall()
        await session.commit()
    return [str(row[0]) for row in rows]


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

        result = await cancel_task(broker, task.id)

        assert is_ok(result)
        assert result.ok_value.was_status is TaskStatus.PENDING
        assert result.ok_value.task_id == task.id
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

        result = await cancel_task(broker, task.id)

        assert is_ok(result)
        assert result.ok_value.was_status is TaskStatus.CLAIMED
        row = await read_task(session, task.id)
        assert row.status == 'CANCELLED'
        assert row.claimed is False
        assert row.claimed_at is None
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

        refused = await cancel_task(broker, task.id)

        assert is_err(refused)
        assert refused.err_value.code is TaskActionErrorCode.TASK_NOT_CANCELLABLE
        assert refused.err_value.current_status is TaskStatus.RUNNING
        assert (await read_task(session, task.id)).status == 'RUNNING'

        allowed = await cancel_task(broker, task.id, include_running=True)

        assert is_ok(allowed)
        assert allowed.ok_value.was_status is TaskStatus.RUNNING
        assert (await read_task(session, task.id)).status == 'CANCELLED'

    async def test_cancel_writes_an_end_timestamp(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """Without failed_at the row would have no measurable end."""
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        result = await cancel_task(broker, task.id)

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

        result = await cancel_task(broker, task.id, include_running=True)

        assert is_ok(result)
        assert await attempt_rows(session, task.id) == []

    async def test_cancel_fires_the_terminal_status_notification(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        """Waiting result handles unblock on the trigger, not on a manual notify."""
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)

        async def do_cancel() -> None:
            result = await cancel_task(broker, task.id)
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
        missing_id = str(uuid.uuid4())

        result = await cancel_task(broker, missing_id)

        assert is_err(result)
        assert result.err_value.code is TaskActionErrorCode.TASK_NOT_FOUND
        assert result.err_value.task_id == missing_id
        assert result.err_value.current_status is None
        assert result.err_value.retryable is False

    async def test_workflow_bound_task(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING, is_workflow_task=True)
        await persist(session, task)

        result = await cancel_task(broker, task.id)

        assert is_err(result)
        assert result.err_value.code is TaskActionErrorCode.TASK_IS_WORKFLOW_TASK
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

        result = await cancel_task(broker, task.id, include_running=True)

        assert is_err(result)
        assert result.err_value.code is TaskActionErrorCode.TASK_NOT_CANCELLABLE
        assert result.err_value.current_status is status

    async def test_second_cancel_is_a_conflict_not_a_second_effect(
        self, broker: PostgresBroker, session: AsyncSession
    ) -> None:
        task = make_task(status=TaskStatus.PENDING)
        await persist(session, task)
        first = await cancel_task(broker, task.id)
        assert is_ok(first)
        first_failed_at = (await read_task(session, task.id)).failed_at

        second = await cancel_task(broker, task.id)

        assert is_err(second)
        assert second.err_value.code is TaskActionErrorCode.TASK_NOT_CANCELLABLE
        assert second.err_value.current_status is TaskStatus.CANCELLED
        assert (await read_task(session, task.id)).failed_at == first_failed_at


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
        assert [row.attempt for row in await attempt_rows(session, task.id)] == [1]

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
        cancelled = await cancel_task(broker, task.id, include_running=True)
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

        result = await cancel_task(broker, task.id)

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
            cancel_task(broker, task.id),
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
            assert cancelled.err_value.code is TaskActionErrorCode.TASK_NOT_CANCELLABLE
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
        workflow = WorkflowModel(
            id=str(uuid.uuid4()),
            name='alpha_flow',
            status='RUNNING',
            on_error='fail',
            depth=0,
        )
        await persist(session, workflow)
        task = make_task(status=TaskStatus.FAILED, is_workflow_task=True)
        await persist(session, task)
        await persist(
            session,
            WorkflowTaskModel(
                id=str(uuid.uuid4()),
                workflow_id=workflow.id,
                task_index=0,
                task_name='action_task',
                queue_name='default',
                priority=100,
                dependencies=[],
                allow_failed_deps=False,
                join_type='all',
                status='FAILED',
                task_id=task.id,
                is_subworkflow=False,
            ),
        )

        cancelled = await cancel_task(broker, task.id, include_running=True)

        assert is_err(cancelled)
        assert cancelled.err_value.code is TaskActionErrorCode.TASK_IS_WORKFLOW_TASK
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
        result = await cancel_task(unreachable_broker, str(uuid.uuid4()))

        assert is_err(result)
        assert result.err_value.code is TaskActionErrorCode.DB_OPERATION_FAILED
        assert result.err_value.retryable is True
