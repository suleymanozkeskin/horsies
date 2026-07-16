"""Integration tests for Worker._finalize_after().

Covers the orchestration logic that:
1. Awaits the child-process future
2. Runs phase-1 (persist terminal state)
3. Runs phase-2 (workflow advancement + NOTIFY)
4. Propagates errors and clears retry attempts

The raw SQL guards are tested in test_finalize_status_guard.py.
Phase-1 branching is tested in test_worker_persist_terminal_state.py.
"""

# Tests drive Worker's private finalize seams directly.
# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import uuid
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from datetime import datetime

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.current import set_current_app
from horsies.core.worker.worker import (
    Worker,
    _FinalizeError,
    _FINALIZE_STAGE_PHASE1,
    _FINALIZE_STAGE_PHASE2,
    _FINALIZE_STAGE_FUTURE,
    _initialize_worker_pool,
    _run_task_entry,
)
from tests.integration.conftest import (
    compute_test_enqueue_sha,
    get_task_status as get_workflow_task_status,
    get_workflow_status,
    make_simple_task,
    make_workflow_spec,
    start_ok,
)

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_TEST_APP: Horsies | None = None


def _test_app() -> Horsies:
    """Lazy-built Horsies app registering the task names this file inserts.

    Production workers wire ``_app`` via ``_locate_app(cfg.app_locator)``;
    these tests bypass preload and build ``Worker`` directly, so we
    attach a minimal app here. Strict-serde phase 6 worker finalize
    looks up ``task_ok_type`` via ``self._app.tasks[task_name]``.
    """
    global _TEST_APP
    if _TEST_APP is None:
        cfg = AppConfig(broker=PostgresConfig(
            database_url='postgresql+psycopg://u:p@localhost/db',
        ))
        _TEST_APP = Horsies(cfg)

        @_TEST_APP.task(task_name='finalize_after_test')
        def _finalize_after_test() -> TaskResult[JsonValue, TaskError]:
            return TaskResult(ok=None)

        @_TEST_APP.task(task_name='finalize_after_owned_test')
        def _finalize_after_owned_test() -> TaskResult[JsonValue, TaskError]:
            return TaskResult(ok=None)

    return _TEST_APP


# Owner id shared by the insert helper and the worker under test so the
# finalize-path ownership guard matches.
TEST_WORKER_ID = 'w-finalize-test'


def _make_worker(engine: AsyncEngine) -> Worker:
    """Construct a Worker wired to the test DB."""
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)
    worker._app = _test_app()
    worker.worker_instance_id = TEST_WORKER_ID
    return worker


async def _insert_running_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='finalize_after_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id)
            VALUES
                (:id, 'finalize_after_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :enqueue_sha, :claimed_by_worker_id)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'claimed_by_worker_id': TEST_WORKER_ID,
        },
    )
    await session.commit()
    return task_id


async def _insert_owned_running_task(session: AsyncSession, worker_id: str) -> str:
    """Insert a RUNNING task owned by a specific worker."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='finalize_after_owned_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id,
                 worker_hostname, worker_pid, worker_process_name)
            VALUES
                (:id, 'finalize_after_owned_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :enqueue_sha, :worker_id,
                 'stale-host', 1234, 'stale-process')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'worker_id': worker_id,
        },
    )
    await session.commit()
    return task_id


async def _get_task_status(session: AsyncSession, task_id: str) -> str:
    """Read current status of a task."""
    row = (
        await session.execute(
            text('SELECT status FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None, f'Task {task_id} not found'
    return str(row[0])


def _make_resolved_future(
    ok: bool,
    result_json: str,
    failed_reason: str | None,
) -> asyncio.Future[tuple[bool, str, str | None]]:
    """Create an already-resolved Future with the given (ok, json, reason) tuple."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[tuple[bool, str, str | None]] = loop.create_future()
    fut.set_result((ok, result_json, failed_reason))
    return fut


def _make_failed_future(
    exc: BaseException,
) -> asyncio.Future[tuple[bool, str, str | None]]:
    """Create a Future that raises the given exception on await."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[tuple[bool, str, str | None]] = loop.create_future()
    fut.set_exception(exc)
    return fut


def _serialize_ok(value: object) -> str:
    """Build the strict ``__h_task_result__`` envelope for a seeded ok value.

    Mirrors production worker writes so the load / phase-2 replay paths
    decode cleanly via the registered task's ``task_ok_type``.
    ``JsonValue`` accepts the mixed ok payloads these tests use.
    """
    envelope = encode_task_result(TaskResult(ok=value), JsonValue)
    r = dumps_json(envelope)
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


def _make_finalize_error(task_id: str, stage: str) -> _FinalizeError:
    """Create a _FinalizeError for testing."""
    return _FinalizeError(
        error_code='TEST_ERROR',
        message='test error',
        stage=stage,
        task_id=task_id,
        retryable=False,
    )


# ---------------------------------------------------------------------------
# Test 1: Happy path — success completes and clears retry attempts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_happy_path_success_completes_and_clears(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Future resolves ok → phase-1 COMPLETED, phase-2 runs, retry attempts cleared."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('hello')
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Seed retry attempts so we can verify they get cleared
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 2
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] = 1

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_ok(result)
    assert result.ok_value is None
    assert await _get_task_status(session, task_id) == 'COMPLETED'
    # Both retry stages must be cleared
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts
    assert (task_id, _FINALIZE_STAGE_PHASE2) not in worker._finalize_retry_attempts


# ---------------------------------------------------------------------------
# Test 2: Phase-1 skip (CLAIM_LOST) → Ok(None), DB stays RUNNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase1_skip_returns_ok_none(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """CLAIM_LOST → phase-1 returns Ok(None) → _finalize_after returns Ok(None)."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_resolved_future(ok=False, result_json='', failed_reason='CLAIM_LOST')

    # Seed phase-1 retry attempts to verify they get cleared on skip
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 1

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_ok(result)
    assert result.ok_value is None
    assert await _get_task_status(session, task_id) == 'RUNNING'
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts


# ---------------------------------------------------------------------------
# Test 3: Phase-1 Err → propagated, no phase-2 attempted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase1_err_propagated_with_finalize_context(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase-1 Err carries replay context; phase-2 never runs."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('value')
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Inject phase-1 failure
    expected_err = _make_finalize_error(task_id, _FINALIZE_STAGE_PHASE1)
    worker._persist_task_terminal_state = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(expected_err),
    )
    # Phase-2 should NOT be called
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_err(result)
    assert result.err_value.error_code == expected_err.error_code
    assert result.err_value.stage == expected_err.stage
    assert result.err_value.task_id == expected_err.task_id
    assert result.err_value.data == {
        'queue_name': 'default',
        'is_workflow_task': True,
        'task_name': 'finalize_after_test',
        'claimed_at': None,
    }
    worker._finalize_workflow_phase.assert_not_awaited()


# ---------------------------------------------------------------------------
# Test 4: Phase-2 Err propagated but DB stays terminal (durability)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase2_err_propagated_but_db_stays_terminal(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase-2 failure → Err returned, but DB row remains COMPLETED (phase-1 durable)."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok('durable-value')
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Let phase-1 run against real DB, but inject phase-2 failure
    phase2_err = _make_finalize_error(task_id, _FINALIZE_STAGE_PHASE2)
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(phase2_err),
    )

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_err(result)
    assert result.err_value is phase2_err
    # Phase-1 committed: DB must reflect COMPLETED
    assert await _get_task_status(session, task_id) == 'COMPLETED'


# ---------------------------------------------------------------------------
# Test 5: BrokenProcessPool → non-retryable Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_broken_process_pool_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """BrokenProcessPool → _handle_broken_pool called, Err returned."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_failed_future(BrokenProcessPool('pool is dead'))

    # Mock _handle_broken_pool to avoid executor restart side-effects
    worker._handle_broken_pool = AsyncMock()  # type: ignore[method-assign]

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_FUTURE
    assert err.retryable is False
    assert 'Broken process pool' in err.message
    worker._handle_broken_pool.assert_awaited_once()


@pytest.mark.asyncio(loop_scope='function')
async def test_broken_process_pool_marks_non_retryable_running_task_failed(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """BrokenProcessPool treats owned RUNNING tasks as possible execution."""
    worker = _make_worker(engine)
    worker._restart_executor = AsyncMock()  # type: ignore[method-assign]
    task_id = await _insert_owned_running_task(session, worker.worker_instance_id)

    await worker._handle_broken_pool(task_id, BrokenProcessPool('pool is dead'))

    row = (
        await session.execute(
            text("""
                SELECT status, claimed, claimed_by_worker_id, started_at,
                       worker_hostname, worker_pid, worker_process_name, error_code
                FROM horsies_tasks
                WHERE id = :id
            """),
            {'id': task_id},
        )
    ).fetchone()

    assert row is not None
    assert row.status == 'FAILED'
    assert row.claimed is False
    assert row.claimed_by_worker_id == worker.worker_instance_id
    assert row.started_at is not None
    assert row.worker_hostname == 'stale-host'
    assert row.worker_pid == 1234
    assert row.worker_process_name == 'stale-process'
    assert row.error_code == 'WORKER_CRASHED'
    worker._restart_executor.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test 6: Generic future exception → requeue attempted, Err returned
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_generic_future_exception_requeues_and_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RuntimeError from future → crash recovery helper called, Err returned."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_failed_future(RuntimeError('child process exploded'))

    worker._recover_worker_future_failure = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(value='NOT_OWNER_OR_NOT_CLAIMED'),
    )

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_FUTURE
    assert 'child process exploded' in err.message
    worker._recover_worker_future_failure.assert_awaited_once()


# ---------------------------------------------------------------------------
# Test 7: Phase-1 Ok(None) clears only phase-1 retry attempts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase1_ok_none_clears_only_phase1_attempts(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Skip path clears phase-1 attempts but leaves phase-2 attempts intact."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_resolved_future(
        ok=False, result_json='', failed_reason='OWNERSHIP_UNCONFIRMED',
    )

    # Seed both retry stages
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 3
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] = 2

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_ok(result)
    # Phase-1 cleared
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts
    # Phase-2 untouched
    assert worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] == 2


# ---------------------------------------------------------------------------
# TASK_EXPIRED finalize-skip gap (regression)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_task_expired_workflow_task_advances_workflow_without_reaper(
    engine: AsyncEngine,
    session: AsyncSession,
    broker: PostgresBroker,
    app: Horsies,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A claimed workflow task that expires before child start resolves the
    workflow through parent finalize alone — no reaper/recovery call.

    Regression for the TASK_EXPIRED finalize-skip gap: the child marks the
    row EXPIRED itself and returns (False, '', 'TASK_EXPIRED'); the parent
    used to skip finalization entirely, so the workflow node stayed
    ENQUEUED against a terminal task row until reaper recovery case 1.7.
    """
    task_a = make_simple_task(app, 'expired_gap_a')
    task_b = make_simple_task(app, 'expired_gap_b')
    node_a = TaskNode(fn=task_a, kwargs={'value': 1})
    node_b = TaskNode(fn=task_b, kwargs={'value': 2}, waits_for=[node_a])
    spec = make_workflow_spec(
        broker=broker, name='expired_gap', tasks=[node_a, node_b],
    )
    handle = await start_ok(spec, broker)

    task_row = (
        await session.execute(
            text("""
                SELECT t.id, t.task_name, t.args, t.kwargs
                FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON t.id = wt.task_id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
    ).fetchone()
    assert task_row is not None
    task_id, task_name, args_json, kwargs_json = task_row

    # Claim node 0's backing task with a good_until about to pass.
    await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = 'CLAIMED',
                claimed = TRUE,
                claimed_at = NOW(),
                claimed_by_worker_id = :wid,
                claim_expires_at = NULL,
                good_until = NOW() + interval '1 second'
            WHERE id = :tid
        """),
        {'tid': task_id, 'wid': TEST_WORKER_ID},
    )
    await session.commit()
    claimed_at = (
        await session.execute(
            text('SELECT claimed_at FROM horsies_tasks WHERE id = :tid'),
            {'tid': task_id},
        )
    ).scalar_one()
    assert isinstance(claimed_at, datetime)

    await asyncio.sleep(1.3)  # cross good_until before the child starts

    set_current_app(app)
    database_url = broker.listener.database_url
    _initialize_worker_pool(database_url)
    wire = _run_task_entry(
        task_name=task_name,
        args_json=args_json,
        kwargs_json=kwargs_json,
        task_id=task_id,
        database_url=database_url,
        master_worker_id=TEST_WORKER_ID,
        claimed_at=claimed_at,
    )
    assert wire == (False, '', 'TASK_EXPIRED')
    # The child self-expired the row before the parent finalizes.
    assert await _get_task_status(session, task_id) == 'EXPIRED'

    worker = _make_worker(engine)
    worker._app = app
    worker.broker = broker
    fut = _make_resolved_future(wire[0], wire[1], wire[2])

    result = await worker._finalize_after(
        fut,
        task_id,
        is_workflow_task=True,
        task_name=task_name,
        claimed_at=claimed_at,
    )

    assert is_ok(result)
    # No reaper/recovery ran: finalize alone resolved the workflow per
    # on_error (node FAILED with TASK_EXPIRED, sibling SKIPPED, wf FAILED).
    assert await get_workflow_task_status(session, handle.workflow_id, 0) == 'FAILED'
    assert await get_workflow_task_status(session, handle.workflow_id, 1) == 'SKIPPED'
    assert await get_workflow_status(session, handle.workflow_id) == 'FAILED'
    node_result = (
        await session.execute(
            text("""
                SELECT result FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
    ).scalar_one()
    assert 'TASK_EXPIRED' in node_result


@pytest.mark.asyncio(loop_scope='function')
async def test_task_expired_plain_task_skips_finalization(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Plain-task TASK_EXPIRED keeps the skip: Ok(None), no DB write,
    no phase 2, no persisted-result load."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_resolved_future(
        ok=False, result_json='', failed_reason='TASK_EXPIRED',
    )
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )
    worker._load_persisted_task_result = AsyncMock()  # type: ignore[method-assign]

    result = await worker._finalize_after(
        fut, task_id, is_workflow_task=False, task_name='finalize_after_test',
    )

    assert is_ok(result)
    assert result.ok_value is None
    assert await _get_task_status(session, task_id) == 'RUNNING'
    worker._finalize_workflow_phase.assert_not_awaited()
    worker._load_persisted_task_result.assert_not_awaited()


@pytest.mark.asyncio(loop_scope='function')
async def test_task_expired_workflow_task_loader_error_propagates(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Workflow-task TASK_EXPIRED with a failing persisted-result load
    returns that finalize error (phase-2 stage) instead of skipping."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    fut = _make_resolved_future(
        ok=False, result_json='', failed_reason='TASK_EXPIRED',
    )
    loader_err = _make_finalize_error(task_id, _FINALIZE_STAGE_PHASE2)
    worker._load_persisted_task_result = AsyncMock(  # type: ignore[method-assign]
        return_value=Err(loader_err),
    )
    worker._finalize_workflow_phase = AsyncMock(  # type: ignore[method-assign]
        return_value=Ok(None),
    )

    result = await worker._finalize_after(
        fut, task_id, is_workflow_task=True, task_name='finalize_after_test',
    )

    assert is_err(result)
    assert result.err_value.stage == _FINALIZE_STAGE_PHASE2
    assert result.err_value.task_id == task_id
    worker._finalize_workflow_phase.assert_not_awaited()


# ---------------------------------------------------------------------------
# Test 8: Full success clears both retry stages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_full_success_clears_both_retry_stages(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Full success path (phase-1 Ok(tr) + phase-2 Ok) clears both stages."""
    task_id = await _insert_running_task(session)
    worker = _make_worker(engine)
    result_json = _serialize_ok(42)
    fut = _make_resolved_future(ok=True, result_json=result_json, failed_reason=None)

    # Seed both stages
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE1)] = 1
    worker._finalize_retry_attempts[(task_id, _FINALIZE_STAGE_PHASE2)] = 1

    result = await worker._finalize_after(fut, task_id, task_name='finalize_after_test')

    assert is_ok(result)
    assert (task_id, _FINALIZE_STAGE_PHASE1) not in worker._finalize_retry_attempts
    assert (task_id, _FINALIZE_STAGE_PHASE2) not in worker._finalize_retry_attempts
    assert await _get_task_status(session, task_id) == 'COMPLETED'
