# pyright: reportPrivateUsage=false
"""Integration tests for per-task timeout enforcement.

Covers Worker._handle_task_timeout: the parent-side handler invoked when a
dispatched task outruns TaskOptions.timeout_ms. The kill goes through the
task's slot handle. A handle here holds a child double that records the kill
and whose exit is seen at once, unless a test says otherwise, or its slot has
already answered.
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.task_options import serialize_task_options
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.tasks import OutcomeCode, TaskOptions
from horsies.core.types.result import Ok, is_ok
from horsies.core.worker.config import WorkerConfig
from tests.dualrun.reference.worker_parent.worker.finalize import _NodeFinishOwed
from tests.dualrun.reference.worker_parent.worker.slots import ChildExit, ExitUnknown, KilledBySignal, SlotHandle
from tests.dualrun.reference.worker_parent.worker.stored_timeout import NoTimeout, TaskTimeout, read_stored_timeout
from tests.dualrun.reference.worker_parent.worker.worker import Worker
from horsies.core.workflows.node_finish import FactsUnknown
from tests.fixtures.reference_worker_support import claim_at, row_claim
from tests.fixtures.slots.handles import (
    HANDLE_CLAIMED_AT,
    task_finished_handle,
    unbound_handle,
)
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.history_seeding import read_attempts

pytestmark = [pytest.mark.integration]

# Owner id shared by the insert helper and the worker under test so the
# ownership guards match.
TEST_WORKER_ID = 'w-timeout-test'
OTHER_WORKER_ID = 'w-timeout-other'

# A pid that cannot exist: pid_max is far below this on macOS and Linux.
FAKE_DEAD_PID = 2**22 - 1


def _handle_with_child(
    child_exit: ChildExit = KilledBySignal(9),
) -> tuple[SlotHandle, MagicMock]:
    handle = unbound_handle()
    child = MagicMock()
    child.pid = FAKE_DEAD_PID
    child.wait_exit = AsyncMock(return_value=child_exit)
    handle.bind_child(child)
    return handle, child


def _make_worker(
    engine: AsyncEngine,
    recovery: RecoveryConfig | None = None,
) -> Worker:
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
        recovery_config=recovery,
    )
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)
    worker.worker_instance_id = TEST_WORKER_ID
    return worker


async def _insert_task(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    owner: str = TEST_WORKER_ID,
    max_retries: int = 0,
    retry_count: int = 0,
    task_options: str | None = None,
    is_workflow_task: bool = False,
) -> str:
    """A task of ``owner``; a workflow task gets its RUNNING node."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name='timeout_test',
        task_options=task_options,
    )
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    created_at, updated_at, retry_count, max_retries, started_at,
                    claimed_by_worker_id, claimed_at, worker_pid, task_options,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution, enqueued_at,
                    claim_expires_at, worker_hostname, worker_process_name
                ) VALUES (
                    :id, 'timeout_test', 'default', 100, '{}', :status, :sent_at,
                    NOW(), NOW(), :retry_count, :max_retries, NOW(), :owner,
                    :claimed_at, :worker_pid, :task_options, :is_workflow_task,
                    'standard_30d',
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE 1 END,
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE :command_fingerprint END,
                    FALSE, 'ISOLATED', NOW(),
                    CASE WHEN CAST(:status AS varchar) = 'CLAIMED' THEN NOW() + INTERVAL '5 minutes' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'host-1' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'child-1' END
                )
        """),
        {
            'id': task_id,
            'status': status,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'owner': owner,
            'claimed_at': HANDLE_CLAIMED_AT,
            'retry_count': retry_count,
            'max_retries': max_retries,
            'worker_pid': FAKE_DEAD_PID,
            'task_options': task_options,
            'is_workflow_task': is_workflow_task,
        },
    )
    if is_workflow_task:
        workflow_id = str(uuid.uuid4())
        await session.execute(
            text("""
                INSERT INTO horsies_workflows
                    (id, name, status, on_error, depth, root_workflow_id,
                     sent_at, created_at, started_at, updated_at)
                VALUES (:wf, 'timeout_wf', 'RUNNING', 'fail', 0, :wf,
                        NOW(), NOW(), NOW(), NOW())
            """),
            {'wf': workflow_id},
        )
        await session.execute(
            text("""
                INSERT INTO horsies_workflow_tasks
                    (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name,
                     input, queue_name, priority,
                     dependencies, allow_failed_deps, join_type,
                     is_subworkflow, status, task_id, created_at,
                     max_retries, execution, retention_class_key)
                VALUES ('NONE', :id, :wf, 0, 'node-0', 'timeout_test', '{}',
                        'default', 100, '{}', FALSE, 'all', FALSE,
                        'RUNNING', CAST(:task_id AS uuid), NOW(),
                        0, 'ISOLATED', 'standard_30d')
            """),
            {'id': str(uuid.uuid4()), 'wf': workflow_id, 'task_id': task_id},
        )
    await session.commit()
    return task_id


async def _get_row(session: AsyncSession, task_id: str) -> Row[Any]:
    row = (
        await session.execute(
            text("""
                SELECT status, error_code, retry_count, claimed_by_worker_id,
                       terminalization_kind
                FROM itest_task_rows WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    return row


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_marks_running_task_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RUNNING + ours → FAILED with TASK_TIMEOUT and an attempt row."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)

    handle, child = _handle_with_child()
    await worker._handle_task_timeout(handle, task_id, 5_000, claim_at(HANDLE_CLAIMED_AT))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'FAILED'
    assert row.error_code == OutcomeCode.TASK_TIMEOUT.value
    assert row.terminalization_kind == 'FAIL_RUNNING'

    # Terminalization purges the live attempt row into the record's
    # snapshot, which is where the attempt now lives.
    attempts = await read_attempts(session, task_id)
    assert len(attempts) == 1
    assert attempts[0].outcome == 'FAILED'
    assert attempts[0].will_retry is False
    assert attempts[0].error_code == OutcomeCode.TASK_TIMEOUT.value
    child.kill_for_caller.assert_called_once_with()


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_replay_uses_committed_result_without_duplicate_attempt(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A repeated timeout handler replays phase 2 of a workflow task from
    the terminal row."""
    task_id = await _insert_task(session, is_workflow_task=True)
    worker = _make_worker(engine)
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )

    await worker._handle_task_timeout(
        _handle_with_child()[0],
        task_id,
        5_000,
        claim_at(HANDLE_CLAIMED_AT),
    )
    worker._finalize_workflow_phase.reset_mock()
    await worker._handle_task_timeout(
        _handle_with_child()[0],
        task_id,
        5_000,
        claim_at(HANDLE_CLAIMED_AT),
    )

    # The replay has no phase-1 node facts: phase 2 runs in the transaction
    # form, and reads the TASK_TIMEOUT result from the phase-2 record.
    worker._finalize_workflow_phase.assert_awaited_once_with(
        task_id,
        _NodeFinishOwed(queue_name='default', facts=FactsUnknown()),
    )
    # The replay adds no attempt to the one the first handler
    # committed, counted wherever the move left it.
    assert len(await read_attempts(session, task_id)) == 1


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_replay_of_a_plain_task_runs_no_phase_2(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The committed write sent the plain task's capacity wake: neither the
    first handler nor its replay runs phase 2, and the replay adds no
    attempt."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )

    await worker._handle_task_timeout(
        _handle_with_child()[0],
        task_id,
        5_000,
        claim_at(HANDLE_CLAIMED_AT),
    )
    await worker._handle_task_timeout(
        _handle_with_child()[0],
        task_id,
        5_000,
        claim_at(HANDLE_CLAIMED_AT),
    )

    worker._finalize_workflow_phase.assert_not_awaited()
    assert len(await read_attempts(session, task_id)) == 1


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_respects_ownership(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A row re-claimed by another worker is left untouched."""
    task_id = await _insert_task(session, owner=OTHER_WORKER_ID)
    worker = _make_worker(engine)

    handle, child = _handle_with_child()
    await worker._handle_task_timeout(handle, task_id, 5_000, claim_at(HANDLE_CLAIMED_AT))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'RUNNING'
    assert row.claimed_by_worker_id == OTHER_WORKER_ID
    child.kill_for_caller.assert_called_once_with()


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_requeues_claimed_task(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """CLAIMED (user code never started) is requeued, not failed."""
    task_id = await _insert_task(session, status='CLAIMED')
    worker = _make_worker(engine)

    handle, child = _handle_with_child()
    await worker._handle_task_timeout(handle, task_id, 5_000, claim_at(HANDLE_CLAIMED_AT))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'PENDING'
    assert row.claimed_by_worker_id is None
    child.kill_for_caller.assert_called_once_with()


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_retries_when_opted_in(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """TASK_TIMEOUT in auto_retry_for → retry scheduled, not terminal."""
    options = json.dumps(
        {
            'retry_policy': {
                'max_retries': 3,
                'intervals': [1, 1, 1],
                'backoff_strategy': 'fixed',
                'jitter': False,
                'auto_retry_for': ['TASK_TIMEOUT'],
            },
            'timeout_ms': 5_000,
        }
    )
    task_id = await _insert_task(
        session,
        max_retries=3,
        task_options=options,
    )
    worker = _make_worker(engine)

    handle, child = _handle_with_child()
    await worker._handle_task_timeout(handle, task_id, 5_000, await row_claim(session, task_id))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'PENDING'
    assert row.retry_count == 1

    attempt = (
        await session.execute(
            text("""
                SELECT will_retry, error_code
                FROM horsies_task_attempts WHERE task_id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()
    assert attempt is not None
    assert attempt.will_retry is True
    assert attempt.error_code == OutcomeCode.TASK_TIMEOUT.value
    child.kill_for_caller.assert_called_once_with()


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_without_a_seen_exit_leaves_the_row_running(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """No exit seen after SIGKILL → no write; with stale RUNNING recovery on,
    stale-task recovery owns the row."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine, RecoveryConfig())

    handle, child = _handle_with_child(ExitUnknown())
    await worker._handle_task_timeout(handle, task_id, 5_000, claim_at(HANDLE_CLAIMED_AT))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'RUNNING'
    assert row.error_code is None
    assert row.claimed_by_worker_id == TEST_WORKER_ID
    assert await read_attempts(session, task_id) == ()
    child.kill_for_caller.assert_called_once_with()


@pytest.mark.asyncio(loop_scope='function')
async def test_with_stale_recovery_off_a_late_exit_writes_the_timeout(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """With stale RUNNING recovery off nothing else moves the row: the worker
    waits past its bounded wait and writes TASK_TIMEOUT once it sees the exit."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)
    handle, child = _handle_with_child(ExitUnknown())
    child.wait_exit_seen = AsyncMock(return_value=KilledBySignal(9))

    await worker._handle_task_timeout(handle, task_id, 5_000, claim_at(HANDLE_CLAIMED_AT))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'FAILED'
    assert row.error_code == OutcomeCode.TASK_TIMEOUT.value
    child.wait_exit_seen.assert_awaited_once_with()


@pytest.mark.asyncio(loop_scope='function')
async def test_timeout_after_the_slot_answered_is_still_a_timeout(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A result that reached the slot after the deadline fired is dropped."""
    task_id = await _insert_task(session)
    worker = _make_worker(engine)

    handle = task_finished_handle(True, '{}', None)
    await worker._handle_task_timeout(handle, task_id, 5_000, claim_at(HANDLE_CLAIMED_AT))
    session.expire_all()

    row = await _get_row(session, task_id)
    assert row.status == 'FAILED'
    assert row.error_code == OutcomeCode.TASK_TIMEOUT.value
    attempts = await read_attempts(session, task_id)
    assert [attempt.error_code for attempt in attempts] == [
        OutcomeCode.TASK_TIMEOUT.value,
    ]


class TestTimeoutWireRoundTrip:
    """serialize_task_options must carry timeout_ms to the claim-side reader.

    Regression: the serializer originally emitted only retry_policy and
    good_until, silently dropping timeout_ms before it reached the DB —
    decorator-declared timeouts never fired. Each stored form of the reader
    is tested in tests/unit/test_stored_timeout.py.
    """

    def test_timeout_round_trips(self) -> None:
        serialized = serialize_task_options(
            TaskOptions(task_name='t', timeout_ms=5_000),
        )
        assert is_ok(serialized)
        assert read_stored_timeout(serialized.ok_value) == TaskTimeout(ms=5_000)

    def test_absent_timeout_round_trips_as_no_timeout(self) -> None:
        serialized = serialize_task_options(TaskOptions(task_name='t'))
        assert is_ok(serialized)
        assert read_stored_timeout(serialized.ok_value) == NoTimeout()
