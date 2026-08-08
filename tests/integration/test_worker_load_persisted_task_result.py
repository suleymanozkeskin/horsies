"""Integration tests for Worker._load_persisted_task_result().

Covers the phase-2 replay method that reads a terminal task's persisted
result from DB: not-found, non-terminal, null result, corrupt JSON,
invalid TaskResult structure, and valid success/error results.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import _FINALIZE_STAGE_PHASE2, Worker
from tests.integration.conftest import compute_test_enqueue_sha
from tests.integration.history_seeding import force_terminal

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_TEST_APP: Horsies | None = None


def _test_app() -> Horsies:
    """Lazy-built Horsies app registering ``load_result_test`` so the
    worker's strict-serde finalize/load decode path can resolve
    ``task_ok_type``. Production workers wire ``_app`` via
    ``_locate_app(cfg.app_locator)``; these tests bypass that and build
    ``Worker`` directly.
    """
    global _TEST_APP
    if _TEST_APP is None:
        cfg = AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://u:p@localhost/db',
            )
        )
        _TEST_APP = Horsies(cfg)

        @_TEST_APP.task(task_name='load_result_test')
        def _load_result_test() -> TaskResult[JsonValue, TaskError]:
            return TaskResult(ok=None)

    return _TEST_APP


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
    return worker


async def _insert_task(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    result: str | None = None,
) -> str:
    """Seed a task in the given status, holding the given stored result.

    A terminal status is reached the way a worker reaches it: the row is
    born running and then terminalized, which moves it to history with
    the result as its payload. A non-terminal status stays live.
    """
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='load_result_test')
    is_terminal = status in ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, result, enqueue_sha)
            VALUES
                (:id, 'load_result_test', 'default', 100, '[]', '{}',
                 :status, :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :result, :enqueue_sha)
        """),
        {
            'id': task_id,
            'status': 'RUNNING' if is_terminal else status,
            'result': None if is_terminal else result,
            'sent_at': sent_at,
            'enqueue_sha': sha,
        },
    )
    if is_terminal:
        await force_terminal(
            session,
            task_id,
            status=status,
            result_json=result,
        )
    await session.commit()
    return task_id


def _serialize_ok(value: object) -> str:
    """Build the strict ``__h_task_result__`` envelope for a seeded ok value.

    Mirrors production worker writes so the load path reads via
    ``decode_task_result(_, task_ok_type)``. ``JsonValue`` is the fence
    type that accepts the heterogeneous ok payloads these tests use.
    """
    envelope = encode_task_result(TaskResult(ok=value), JsonValue)
    r = dumps_json(envelope)
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


def _serialize_err(error_code: str, message: str) -> str:
    """Build the strict ``__h_task_result__`` envelope for a seeded err."""
    envelope = encode_task_result(
        TaskResult(err=TaskError(error_code=error_code, message=message)),
        JsonValue,
    )
    r = dumps_json(envelope)
    assert not is_err(r), f'Serialization failed: {r}'
    return r.ok_value


# ---------------------------------------------------------------------------
# Test 1: Task row not found → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_task_not_found_returns_err(
    engine: AsyncEngine,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Nonexistent task_id → Err with 'not found' message."""
    worker = _make_worker(engine)
    fake_id = str(uuid.uuid4())

    result = await worker._load_persisted_task_result(fake_id)

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_PHASE2
    assert 'not found' in err.message
    assert err.retryable is False


# ---------------------------------------------------------------------------
# Test 2: Task exists but not terminal (RUNNING) → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_not_terminal_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """RUNNING task → Err with 'unavailable' message."""
    task_id = await _insert_task(session, status='RUNNING', result=None)
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'unavailable' in err.message
    assert err.data is not None
    assert err.data['status'] == 'RUNNING'


# ---------------------------------------------------------------------------
# Test 3: Terminal but result column is NULL → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_terminal_null_result_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """COMPLETED task with result=NULL → Err with 'unavailable' message."""
    task_id = await _insert_task(session, status='COMPLETED', result=None)
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'unavailable' in err.message


# ---------------------------------------------------------------------------
# Test 4: Corrupt JSON in result column → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_corrupt_json_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """COMPLETED task with corrupt JSON in result → Err with 'corrupt' message."""
    task_id = await _insert_task(
        session,
        status='COMPLETED',
        result='not json {',
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'corrupt' in err.message


# ---------------------------------------------------------------------------
# Test 5: Valid JSON but not a TaskResult structure → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_invalid_task_result_structure_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Valid JSON but missing ``__h_task_result__`` marker → Err carrying
    the strict-serde envelope-shape rejection."""
    task_id = await _insert_task(
        session,
        status='COMPLETED',
        result='{"random": 1}',
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'stored result envelope invalid' in err.message
    assert '__h_task_result__' in err.message


# ---------------------------------------------------------------------------
# Test 6: COMPLETED with valid ok result → Ok(TaskResult) with is_ok()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_completed_ok_result_returns_task_result(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """COMPLETED task with valid ok result → Ok(TaskResult(ok='hello'))."""
    result_json = _serialize_ok('hello')
    task_id = await _insert_task(
        session,
        status='COMPLETED',
        result=result_json,
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_ok(result)
    persisted = result.ok_value
    assert persisted.result.is_ok()
    assert persisted.result.unwrap() == 'hello'
    assert persisted.task_name == 'load_result_test'
    assert persisted.queue_name == 'default'
    assert persisted.is_workflow_task is False


# ---------------------------------------------------------------------------
# Test 7: FAILED with valid error result → Ok(TaskResult) with is_err()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_failed_err_result_returns_task_result(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """FAILED task with valid error result → Ok(TaskResult(err=TaskError(...)))."""
    result_json = _serialize_err('DELIBERATE_FAIL', 'test failure')
    task_id = await _insert_task(
        session,
        status='FAILED',
        result=result_json,
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_ok(result)
    persisted = result.ok_value
    assert persisted.result.is_err()
    task_error = persisted.result.unwrap_err()
    assert task_error.error_code == 'DELIBERATE_FAIL'
    assert task_error.message == 'test failure'


# ---------------------------------------------------------------------------
# Test 8: FAILED with err result for task NOT registered locally → Ok(TaskResult)
#         via err-fast-path (regression: replay used to require task_ok_type
#         lookup before reading the err slot, which masked the original
#         error_code with WORKER_SERIALIZATION_ERROR).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_unknown_task_err_result_uses_fast_path(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Err-slot decode must NOT require task_ok_type lookup.

    Regression: ``_load_persisted_task_result`` previously looked up
    ``task_ok_type`` from the local registry before branching on
    ok-vs-err, so a row whose worker wrote
    ``WORKER_RESOLUTION_ERROR`` (unknown task name, by definition not
    in the local registry) was re-wrapped here as
    ``WORKER_SERIALIZATION_ERROR: ...missing task_ok_type...``.
    The err slot is fixed-schema (TaskError) and decodes without OkT;
    the fix branches on the err slot first.
    """
    task_id = str(uuid.uuid4())
    # Insert a row pretending the worker terminalized 'unknown_task_xyz'
    # with WORKER_RESOLUTION_ERROR — using the strict envelope shape.
    envelope = encode_task_result(
        TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.WORKER_RESOLUTION_ERROR,
                message='task not found in registry',
            )
        ),
        JsonValue,
    )
    err_result_json = dumps_json(envelope).unwrap()
    sent_at, sha = compute_test_enqueue_sha(task_name='unknown_task_xyz')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha)
            VALUES
                (:id, 'unknown_task_xyz', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 0, NOW(), :enqueue_sha)
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
        },
    )
    await force_terminal(
        session,
        task_id,
        status='FAILED',
        result_json=err_result_json,
    )
    await session.commit()

    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_ok(result), (
        f'expected Ok(TaskResult(err)) via err-fast-path, got Err: '
        f'{result.err_value if not is_ok(result) else None}'
    )
    persisted = result.ok_value
    assert persisted.result.is_err()
    task_error = persisted.result.unwrap_err()
    assert persisted.task_name == 'unknown_task_xyz'
    assert task_error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR
    assert task_error.message == 'task not found in registry'


# ---------------------------------------------------------------------------
# Test 9: EXPIRED with err result → Ok(TaskResult) with is_err()
#         (C9 regression: expired-before-start tasks write a TASK_EXPIRED err
#         result and a terminal EXPIRED status. The in-process phase-2 replay
#         reload must accept EXPIRED alongside COMPLETED/FAILED; before the fix
#         it rejected the row as 'terminal task result unavailable', leaving
#         in-process replay dead for the expired case.)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_expired_err_result_returns_task_result(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """EXPIRED task with valid err result → Ok(TaskResult(err=TaskError(...)))."""
    result_json = _serialize_err(
        OutcomeCode.TASK_EXPIRED,
        'Task expired: good_until deadline passed before execution started',
    )
    task_id = await _insert_task(
        session,
        status='EXPIRED',
        result=result_json,
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_task_result(task_id)

    assert is_ok(result), (
        f'expected Ok(TaskResult(err)) for EXPIRED row, got Err: '
        f'{result.err_value if not is_ok(result) else None}'
    )
    persisted = result.ok_value
    assert persisted.result.is_err()
    task_error = persisted.result.unwrap_err()
    assert task_error.error_code == OutcomeCode.TASK_EXPIRED
    assert persisted.task_name == 'load_result_test'
