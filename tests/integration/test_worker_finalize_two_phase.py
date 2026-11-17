"""Integration regression test for two-phase worker finalization."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err
from horsies.core.worker.config import WorkerConfig
from tests.dualrun.reference.worker_parent.worker.worker import Worker
from tests.fixtures.reference_worker_support import claim_at
from tests.phase2_support import failing_phase2
from tests.fixtures.slots.handles import HANDLE_CLAIMED_AT, task_finished_handle
from tests.integration.conftest import compute_test_command_fingerprint

_TEST_APP: Horsies | None = None


def _test_app() -> Horsies:
    """Lazy-built Horsies app registering ``two_phase_finalize_test``.

    Production workers wire ``_app`` via ``_locate_app(cfg.app_locator)``;
    this test bypasses preload and builds ``Worker`` directly, so the
    strict-serde phase-2 replay path needs the task registered to
    resolve ``task_ok_type``.
    """
    global _TEST_APP
    if _TEST_APP is None:
        cfg = AppConfig(
            broker=PostgresConfig(
                database_url='postgresql://u:p@localhost/db',
            )
        )
        _TEST_APP = Horsies(cfg)

        @_TEST_APP.task(task_name='two_phase_finalize_test')
        def _two_phase_finalize_test() -> TaskResult[JsonValue, TaskError]:
            return TaskResult(ok=None)

    return _TEST_APP


pytestmark = [pytest.mark.integration]

# Owner id shared by the insert helper and the worker under test so the
# finalize-path ownership guard matches.
TEST_WORKER_ID = 'w-two-phase-test'


async def _insert_running_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state and return its id."""
    task_id = str(uuid.uuid4())
    sent_at, fingerprint = compute_test_command_fingerprint(task_name='two_phase_finalize_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    created_at, updated_at, retry_count, max_retries, started_at,
                    claimed_by_worker_id, claimed_at, is_workflow_task,
                    retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution, enqueued_at,
                    worker_pid, worker_hostname, worker_process_name
                ) VALUES (
                    :id, 'two_phase_finalize_test', 'default', 100, '{}', 'RUNNING',
                    :sent_at, NOW(), NOW(), 0, 3, NOW(), :claimed_by_worker_id,
                    :claimed_at, TRUE, 'standard_30d', NULL, NULL, FALSE, 'ISOLATED',
                    NOW(), 4242, 'host-1', 'child-1'
                )
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'claimed_by_worker_id': TEST_WORKER_ID,
            'claimed_at': HANDLE_CLAIMED_AT,
        },
    )
    # The workflow and its node. A workflow task whose node row is
    # missing is a state the engine never produces, and phase 1 reads
    # that row with INTO STRICT: without it the first phase errs and the
    # phase-2 failure this test is about is never reached.
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES
                (:wf_id, 'two_phase_finalize_wf', 'RUNNING', 'fail', 0,
                 :wf_id, NOW(), NOW(), NOW(), NOW())
        """),
        {'wf_id': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name,
                 input, queue_name, priority,
                 dependencies, allow_failed_deps, join_type,
                 is_subworkflow, status, task_id, created_at,
                 max_retries, execution, retention_class_key)
            VALUES
                ('NONE', :id, :wf_id, 0, 'node-0', 'two_phase_finalize_test',
                 '{}', 'default', 100,
                 '{}', FALSE, 'all',
                 FALSE, 'RUNNING', CAST(:task_id AS uuid), NOW(),
                 0, 'ISOLATED', 'standard_30d')
        """),
        {
            'id': str(uuid.uuid4()),
            'wf_id': workflow_id,
            'task_id': task_id,
        },
    )
    await session.commit()
    return task_id


@pytest.mark.asyncio(loop_scope='function')
async def test_finalize_phase2_failure_keeps_terminal_task_result_durable(
    engine: AsyncEngine,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase-2 workflow error must not roll back phase-1 task terminal persistence."""
    task_id = await _insert_running_task(session)
    worker = _worker(engine, broker)

    # Simulate workflow advancement failure after phase-1 task status persistence.
    phase2_call = failing_phase2(broker, monkeypatch, 'phase-2 boom')

    envelope = encode_task_result(
        TaskResult(ok='phase-1-result'),
        JsonValue,
    )
    serialized = dumps_json(envelope)
    assert not is_err(serialized)

    handle = task_finished_handle(True, serialized.ok_value, None)

    # Should return Err (for callback-driven handling) but keep phase-1 durability.
    finalize_r = await worker._finalize_after(
        handle,
        task_id,
        task_name='two_phase_finalize_test',
        claim=claim_at(HANDLE_CLAIMED_AT),
    )
    assert is_err(finalize_r)

    row = (
        await session.execute(
            text(
                'SELECT status, result FROM itest_task_rows '
                'WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'COMPLETED'
    assert row[1] == serialized.ok_value

    # Ensure phase-2 was actually attempted.
    phase2_call.assert_awaited()


def _worker(engine: AsyncEngine, broker: PostgresBroker) -> Worker:
    """A worker with the broker, through which phase 2 builds child starts."""
    cfg = WorkerConfig(
        dsn='postgresql://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    worker = Worker(
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        listener=MagicMock(),
        cfg=cfg,
        broker=broker,
    )
    worker._app = _test_app()
    worker.worker_instance_id = TEST_WORKER_ID
    return worker


async def _outbox_rows(session: AsyncSession, task_id: str) -> int:
    return (
        await session.execute(
            text(
                'SELECT count(*) FROM horsies_workflow_phase2_pending '
                'WHERE task_id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).scalar_one()


async def _node_status(session: AsyncSession, task_id: str) -> str:
    return (
        await session.execute(
            text(
                'SELECT status FROM horsies_workflow_tasks '
                'WHERE task_id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).scalar_one()


@pytest.mark.parametrize('ok', [True, False], ids=['success', 'failure'])
@pytest.mark.asyncio(loop_scope='function')
async def test_phase2_deletes_the_outbox_row_of_the_node(
    engine: AsyncEngine,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    ok: bool,
) -> None:
    """Phase 1 writes the node's outbox row; the phase 2 that advances the
    node deletes it in the same transaction, so no recovery pass has it
    left to consume."""
    task_id = await _insert_running_task(session)
    worker = _worker(engine, broker)
    result: TaskResult[JsonValue, TaskError] = (
        TaskResult(ok='done')
        if ok
        else TaskResult(err=TaskError(error_code='BOOM', message='boom'))
    )
    wire = dumps_json(encode_task_result(result, JsonValue))
    assert not is_err(wire)

    finalize_r = await worker._finalize_after(
        task_finished_handle(True, wire.ok_value, None),
        task_id,
        task_name='two_phase_finalize_test',
        claim=claim_at(HANDLE_CLAIMED_AT),
    )

    assert not is_err(finalize_r)
    assert await _node_status(session, task_id) == ('COMPLETED' if ok else 'FAILED')
    assert await _outbox_rows(session, task_id) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failed_phase2_keeps_the_outbox_row(
    engine: AsyncEngine,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The delete commits with the node write, or not at all: a phase 2
    that fails leaves the row for the recovery pass."""
    task_id = await _insert_running_task(session)
    worker = _worker(engine, broker)
    phase2_call = failing_phase2(broker, monkeypatch, 'phase-2 boom')
    wire = dumps_json(encode_task_result(TaskResult(ok='done'), JsonValue))
    assert not is_err(wire)

    finalize_r = await worker._finalize_after(
        task_finished_handle(True, wire.ok_value, None),
        task_id,
        task_name='two_phase_finalize_test',
        claim=claim_at(HANDLE_CLAIMED_AT),
    )

    assert is_err(finalize_r)
    phase2_call.assert_awaited()
    assert await _outbox_rows(session, task_id) == 1
