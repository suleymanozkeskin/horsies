# pyright: reportPrivateUsage=false
"""Integration tests for Worker._finalize_workflow_phase().

Phase 2 of a workflow task is ``horsies_workflow_node_finish``
(``node_finish.finish_node``) in the form of the node facts. Covers: the
node write and the promotion, the capacity wake on the queue of the node,
delivered once at the commit, a task with no phase-2 record (no write, no
wake), a phase 2 that fails after its wake (no wake), a raise and a kept
record as phase-2 errors, and a worker with no broker.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

import psycopg
import psycopg.sql
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.outcomes import PhaseTwoStep
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Ok, is_err, is_ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.finalize import _NodeFinishOwed
from horsies.core.worker.worker import Worker, _FINALIZE_STAGE_PHASE2
from horsies.core.workflows.node_finish import FactsUnknown
from tests.phase2_support import failing_phase2
from tests.integration.conftest import DB_URL, PhaseOneDone, finish_phase_one, start_ok

pytestmark = [pytest.mark.integration]

_QUEUE = 'default'
_WAKE_WAIT_S = 0.5
"""How long a test listens for wakes after phase 2 returned."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(engine: AsyncEngine, broker: PostgresBroker | None) -> Worker:
    """A Worker wired to the test DB, with ``broker`` (``None``: a worker
    built without one)."""
    sf = async_sessionmaker(engine, expire_on_commit=False)
    cfg = WorkerConfig(
        dsn='postgresql://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=[_QUEUE],
    )
    return Worker(session_factory=sf, listener=MagicMock(), cfg=cfg, broker=broker)


async def _chain_after_phase_one(
    app: Horsies, broker: PostgresBroker, session: AsyncSession,
) -> tuple[str, PhaseOneDone]:
    """A -> B started, and phase 1 of A's success committed. Returns the
    workflow and what phase 2 of A takes."""

    @app.task(task_name=f'wf_phase_{uuid.uuid4().hex[:8]}')
    def fn(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    a = TaskNode(fn=fn, kwargs={'value': 1})
    b = TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[a])
    spec = app.workflow(
        f'wf_phase_{uuid.uuid4().hex[:6]}', [a, b],
        definition_key=f'wf-phase-{uuid.uuid4().hex[:6]}',
    )
    handle = await start_ok(spec, broker)
    done = await finish_phase_one(session, broker, handle.workflow_id, 0, TaskResult(ok=1))
    assert done.facts is PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW
    return handle.workflow_id, done


def _owed(done: PhaseOneDone) -> _NodeFinishOwed:
    return _NodeFinishOwed(queue_name=_QUEUE, facts=done.facts)


async def _node_statuses(session: AsyncSession, workflow_id: str) -> dict[int, str]:
    await session.commit()
    rows = (
        await session.execute(
            text("""
                SELECT task_index, status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf ORDER BY task_index
            """),
            {'wf': workflow_id},
        )
    ).fetchall()
    return {row.task_index: row.status for row in rows}


async def _records(session: AsyncSession, task_id: str) -> int:
    await session.commit()
    return (
        await session.execute(
            text(
                'SELECT count(*) FROM horsies_workflow_phase2_pending '
                'WHERE task_id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).scalar_one()


async def _listen(channel: str) -> psycopg.AsyncConnection[Any]:
    connection = await psycopg.AsyncConnection.connect(DB_URL, autocommit=True)
    await connection.execute(
        psycopg.sql.SQL('LISTEN {}').format(psycopg.sql.Identifier(channel)),
    )
    return connection


async def _wakes(listener: psycopg.AsyncConnection[Any]) -> list[str]:
    try:
        return [notify.payload async for notify in listener.notifies(timeout=_WAKE_WAIT_S)]
    finally:
        await listener.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
async def test_phase_two_writes_the_node_and_promotes_the_dependent(
    engine: AsyncEngine,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The node is written from the phase-2 record, the record is gone, and
    the dependent is enqueued."""
    workflow_id, done = await _chain_after_phase_one(app, broker, session)
    worker = _make_worker(engine, broker)

    result = await worker._finalize_workflow_phase(done.task_id, _owed(done))

    assert result == Ok(None)
    assert await _node_statuses(session, workflow_id) == {0: 'COMPLETED', 1: 'ENQUEUED'}
    assert await _records(session, done.task_id) == 0


@pytest.mark.asyncio(loop_scope='function')
async def test_phase_two_wakes_the_queue_of_the_node_once(
    engine: AsyncEngine,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """One capacity wake on ``task_queue_<queue of the node>``, delivered at
    the commit. The insert of B's task row sends ``new`` on the same
    channel."""
    _, done = await _chain_after_phase_one(app, broker, session)
    worker = _make_worker(engine, broker)
    listener = await _listen(f'task_queue_{_QUEUE}')

    result = await worker._finalize_workflow_phase(done.task_id, _owed(done))
    payloads = await _wakes(listener)

    assert is_ok(result)
    assert sorted(payloads) == ['capacity', 'new']


@pytest.mark.asyncio(loop_scope='function')
async def test_a_task_with_no_phase_two_record_writes_nothing_and_sends_no_wake(
    engine: AsyncEngine,
    broker: PostgresBroker,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """No node links the task and no record names it: PENDING_ABSENT, Ok,
    no wake."""
    worker = _make_worker(engine, broker)
    listener = await _listen(f'task_queue_{_QUEUE}')

    result = await worker._finalize_workflow_phase(
        str(uuid.uuid4()), _NodeFinishOwed(queue_name=_QUEUE, facts=FactsUnknown()),
    )
    payloads = await _wakes(listener)

    assert result == Ok(None)
    assert payloads == []


@pytest.mark.asyncio(loop_scope='function')
async def test_phase_two_that_fails_after_its_wake_sends_no_wake(
    engine: AsyncEngine,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """In the transaction form the step queues the wake and the COMMIT
    delivers it: a phase 2 that fails after the step rolls back the wake
    with the node write and keeps the record.

    Phase 2 runs in the core. A deferred constraint trigger on the node rows
    of the workflow raises at the COMMIT, after the step wrote the nodes and
    queued the wake (SQLSTATE P0001: not retryable)."""
    workflow_id, done = await _chain_after_phase_one(app, broker, session)
    worker = _make_worker(engine, broker)
    await session.execute(
        text("""
            CREATE FUNCTION it_fail_after_the_step() RETURNS trigger
            LANGUAGE plpgsql AS $body$
            BEGIN
                RAISE EXCEPTION 'failed after the node step';
            END
            $body$
        """),
    )
    await session.execute(
        text(f"""
            CREATE CONSTRAINT TRIGGER it_fail_after_the_step
            AFTER UPDATE ON horsies_workflow_tasks
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            WHEN (NEW.workflow_id = '{workflow_id}'::uuid)
            EXECUTE FUNCTION it_fail_after_the_step()
        """),
    )
    await session.commit()
    listener = await _listen(f'task_queue_{_QUEUE}')

    try:
        result = await worker._finalize_workflow_phase(
            done.task_id, _NodeFinishOwed(queue_name=_QUEUE, facts=FactsUnknown()),
        )
    finally:
        await session.execute(
            text('DROP TRIGGER it_fail_after_the_step ON horsies_workflow_tasks'),
        )
        await session.execute(text('DROP FUNCTION it_fail_after_the_step()'))
        await session.commit()
    payloads = await _wakes(listener)

    assert is_err(result)
    assert result.err_value.stage == _FINALIZE_STAGE_PHASE2
    assert payloads == []
    assert await _node_statuses(session, workflow_id) == {0: 'ENQUEUED', 1: 'PENDING'}
    assert await _records(session, done.task_id) == 1


@pytest.mark.asyncio(loop_scope='function')
async def test_a_failed_phase_two_is_a_phase2_error(
    engine: AsyncEngine,
    broker: PostgresBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure record of the phase 2 → Err(_FinalizeError), stage phase 2,
    retryable as the record says."""
    worker = _make_worker(engine, broker)
    _ = failing_phase2(broker, monkeypatch, 'workflow engine exploded')

    result = await worker._finalize_workflow_phase(
        str(uuid.uuid4()),
        _NodeFinishOwed(queue_name=_QUEUE, facts=PhaseTwoStep.NO_DEPENDENTS),
    )

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_PHASE2
    assert err.retryable is False
    assert err.data is not None
    assert 'workflow engine exploded' in err.data['exception']


@pytest.mark.asyncio(loop_scope='function')
async def test_a_kept_record_is_a_phase2_error_that_is_not_retried(
    engine: AsyncEngine,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A record whose stored result fails its digest check is kept, the node
    is not written, and phase 2 reports the disposition: phase-2 recovery
    counts the record."""
    workflow_id, done = await _chain_after_phase_one(app, broker, session)
    await session.execute(
        text("""
            UPDATE horsies_workflow_phase2_pending
            SET result_digest = sha256(convert_to('another result', 'UTF8'))
            WHERE task_id = CAST(:id AS uuid)
        """),
        {'id': done.task_id},
    )
    await session.commit()
    worker = _make_worker(engine, broker)

    result = await worker._finalize_workflow_phase(done.task_id, _owed(done))

    assert is_err(result)
    err = result.err_value
    assert err.stage == _FINALIZE_STAGE_PHASE2
    assert err.retryable is False
    assert err.data is not None
    assert err.data['disposition'] == 'SOURCE_DIGEST_MISMATCH'
    assert await _node_statuses(session, workflow_id) == {0: 'ENQUEUED', 1: 'PENDING'}
    assert await _records(session, done.task_id) == 1


@pytest.mark.asyncio(loop_scope='function')
async def test_a_worker_without_a_broker_runs_no_phase_two(
    engine: AsyncEngine,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase 2 builds child starts through the broker: a worker without one
    reports a phase-2 error that is not retryable and writes nothing."""
    workflow_id, done = await _chain_after_phase_one(app, broker, session)
    worker = _make_worker(engine, None)

    result = await worker._finalize_workflow_phase(done.task_id, _owed(done))

    assert is_err(result)
    assert result.err_value.stage == _FINALIZE_STAGE_PHASE2
    assert result.err_value.retryable is False
    assert await _records(session, done.task_id) == 1
    assert await _node_statuses(session, workflow_id) == {0: 'ENQUEUED', 1: 'PENDING'}
