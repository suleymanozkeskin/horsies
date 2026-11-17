# pyright: reportPrivateUsage=false
"""Integration tests for Worker._load_persisted_terminal().

Covers the phase-2 replay method that reads what a terminal task owes: a
task that is not found or not terminal (an error), a terminal workflow task
(phase 2 in the transaction form, whatever its stored result: the node
finish reads and checks the result from the phase-2 record), and a plain
task, which owes no phase 2.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Ok, is_err
from horsies.core.worker.finalize import _NodeFinishOwed, _PlainTaskDone
from horsies.core.workflows.node_finish import FactsUnknown
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import _FINALIZE_STAGE_PHASE2, Worker
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.history_seeding import force_terminal

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(engine: AsyncEngine) -> Worker:
    """Construct a Worker wired to the test DB."""
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    return Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)


async def _insert_task(
    session: AsyncSession,
    *,
    status: str = 'RUNNING',
    result: str | None = None,
    is_workflow_task: bool = True,
) -> str:
    """Seed a task in the given status, holding the given stored result.

    A terminal status is reached the way a worker reaches it: the row is
    born running and then terminalized, which moves it to history with
    the result as its payload. A non-terminal status stays live. A
    workflow task by default: only a workflow task owes a phase 2.
    """
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='load_result_test')
    is_terminal = status in ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    created_at, updated_at, retry_count, max_retries, started_at,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution, enqueued_at,
                    claim_expires_at, worker_pid, worker_hostname, worker_process_name
                ) VALUES (
                    :id, 'load_result_test', 'default', 100, '{}', :status, :sent_at,
                    NOW(), NOW(), 0, 0, NOW(), :is_workflow_task, 'standard_30d',
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE 1 END,
                    CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE :command_fingerprint END,
                    FALSE, 'ISOLATED', NOW(),
                    CASE WHEN CAST(:status AS varchar) = 'CLAIMED' THEN NOW() + INTERVAL '5 minutes' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 4242 END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'host-1' END,
                    CASE WHEN CAST(:status AS varchar) = 'RUNNING' THEN 'child-1' END
                )
        """),
        {
            'id': task_id,
            'is_workflow_task': is_workflow_task,
            'status': 'RUNNING' if is_terminal else status,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
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

    Mirrors production worker writes. ``JsonValue`` is the fence type that
    accepts the heterogeneous ok payloads these tests use.
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
# A task that is not found or not terminal → Err
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_task_not_found_returns_err(
    engine: AsyncEngine,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Nonexistent task_id → Err with 'not found' message."""
    worker = _make_worker(engine)
    fake_id = str(uuid.uuid4())

    result = await worker._load_persisted_terminal(fake_id)

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_PHASE2
    assert 'not found' in err.message
    assert err.retryable is False


@pytest.mark.asyncio(loop_scope='function')
async def test_not_terminal_returns_err(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A live RUNNING workflow task owes no replay → Err naming its status."""
    task_id = await _insert_task(session, status='RUNNING', result=None)
    worker = _make_worker(engine)

    result = await worker._load_persisted_terminal(task_id)

    assert is_err(result)
    err = result.err_value
    assert 'not terminal' in err.message
    assert err.retryable is False
    assert err.data is not None
    assert err.data['status'] == 'RUNNING'


# ---------------------------------------------------------------------------
# A terminal workflow task → phase 2 in the transaction form
# ---------------------------------------------------------------------------


_REPLAY = Ok(_NodeFinishOwed(queue_name='default', facts=FactsUnknown()))
"""What a replay owes: phase 2 with no phase-1 node facts."""


@pytest.mark.parametrize(
    ('status', 'stored'),
    [
        ('COMPLETED', _serialize_ok('hello')),
        ('FAILED', _serialize_err('DELIBERATE_FAIL', 'test failure')),
        (
            'EXPIRED',
            _serialize_err(
                OutcomeCode.TASK_EXPIRED,
                'Task expired: good_until deadline passed before execution started',
            ),
        ),
        ('COMPLETED', None),
        ('COMPLETED', 'not json {'),
        ('COMPLETED', '{"random": 1}'),
    ],
    ids=[
        'completed',
        'failed',
        'expired',
        'no-stored-result',
        'stored-result-not-json',
        'stored-result-not-an-envelope',
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_a_terminal_workflow_task_owes_phase_two_in_the_transaction_form(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    status: str,
    stored: str | None,
) -> None:
    """The load decodes no result: the node finish reads the result from
    the phase-2 record and keeps a record whose result fails its check."""
    task_id = await _insert_task(session, status=status, result=stored)
    worker = _make_worker(engine)

    result = await worker._load_persisted_terminal(task_id)

    assert result == _REPLAY


@pytest.mark.asyncio(loop_scope='function')
async def test_a_task_that_no_local_app_registers_owes_phase_two(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The load needs no registry entry of the task: a workflow task named
    ``unknown_task_xyz`` owes phase 2 as any other."""
    task_id = str(uuid.uuid4())
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
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='unknown_task_xyz')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    created_at, updated_at, retry_count, max_retries, started_at,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution, enqueued_at,
                    worker_pid, worker_hostname, worker_process_name
                ) VALUES (
                    :id, 'unknown_task_xyz', 'default', 100, '{}', 'RUNNING', :sent_at,
                    NOW(), NOW(), 0, 0, NOW(), TRUE, 'standard_30d', NULL, NULL, FALSE,
                    'ISOLATED', NOW(), 4242, 'host-1', 'child-1'
                )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
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

    result = await worker._load_persisted_terminal(task_id)

    assert result == _REPLAY


@pytest.mark.asyncio(loop_scope='function')
async def test_a_plain_task_owes_no_phase_two(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A plain task's terminal row: the statement that wrote it sent its
    capacity wake, so the load reports nothing owed."""
    task_id = await _insert_task(
        session,
        status='COMPLETED',
        result='{not json',
        is_workflow_task=False,
    )
    worker = _make_worker(engine)

    result = await worker._load_persisted_terminal(task_id)

    assert result == Ok(_PlainTaskDone())
