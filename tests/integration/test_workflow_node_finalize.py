# pyright: reportPrivateUsage=false
"""The finalize of a workflow node's success, from the worker's entry point.

Phase 1 is one statement (``horsies_finish_workflow_success``) in
autocommit; phase 2 is the workflow transaction. Each test starts a real
workflow A -> B, runs node A as the worker's claim would, and drives
``_finalize_task_result`` or the phase-1 retry:

- a success owes phase 2, and phase 2 enqueues B;
- a replay of a committed success runs phase 2 from the persisted result;
- a lost reply is a retryable phase-1 error, and its retry runs phase 2;
- a lost claim generation writes nothing and owes nothing;
- a node whose workflow was cancelled or failed meanwhile completes its
  task, and phase 2 enqueues nothing.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import SuccessApplied
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode, WorkflowHandle
from horsies.core.types.result import Err, Ok, Result, is_ok
from horsies.core.worker.runtime import FinalizeStage, _FinalizeError
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.worker.workflow_success import (
    FinishWorkflowSuccess,
    finish_workflow_success,
)
from tests.integration.autocommit_support import (
    WORKER_ID,
    ProxiedWorker,
    open_proxied_worker,
    proxied_url,
    upstream_of,
)
from tests.integration.round_trip_proxy import RoundTripProxy

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

REPLY_DROP_WAIT_S: Final = 5.0


@dataclass(frozen=True, slots=True)
class _RunningNode:
    workflow_id: str
    task_id: str
    task_name: str
    queue_name: str
    claimed_at: datetime


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


@pytest_asyncio.fixture(loop_scope='function')
async def worker_of(
    proxy: RoundTripProxy,
    db_url: str,
    app: Horsies,
    broker: PostgresBroker,  # noqa: ARG001 - the schema and history leaves
) -> AsyncGenerator[ProxiedWorker]:
    """A worker on the proxy, as the CLI builds it, that knows ``app``."""
    opened = await open_proxied_worker(proxied_url(db_url, proxy))
    opened.worker._app = app
    opened.broker.app = app
    yield opened
    await opened.close()


def _task(app: Horsies) -> Any:
    @app.task(task_name=f'node_finalize_{uuid.uuid4().hex[:8]}')
    def node_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    return node_task


def _ok_json(value: int) -> str:
    return dumps_json(encode_task_result(TaskResult(ok=value), JsonValue)).unwrap()


async def _start_chain(app: Horsies, broker: PostgresBroker) -> str:
    """A workflow A -> B; A is enqueued, B waits for it."""
    fn = _task(app)
    a = TaskNode(fn=fn, kwargs={'value': 1})
    b = TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[a])
    spec = app.workflow(
        f'node_finalize_{uuid.uuid4().hex[:6]}',
        [a, b],
        definition_key=f'node-finalize-{uuid.uuid4().hex[:6]}',
    )
    started = await start_workflow_async(spec, broker)
    assert is_ok(started), started
    return started.ok_value.workflow_id


async def _run_node_a(session: AsyncSession, workflow_id: str) -> _RunningNode:
    """Node A RUNNING and its task RUNNING under ``WORKER_ID``, as a claim
    and the child's start leave them."""
    claimed_at = datetime.now(timezone.utc)
    row = (
        await session.execute(
            text("""
                UPDATE horsies_tasks t
                SET status = 'RUNNING', claimed = FALSE,
                    claimed_by_worker_id = :worker, claimed_at = :claimed_at,
                    started_at = NOW()
                FROM horsies_workflow_tasks wt
                WHERE wt.workflow_id = :wf AND wt.task_index = 0
                  AND t.id = wt.task_id
                RETURNING t.id, t.task_name, t.queue_name
            """),
            {'wf': workflow_id, 'worker': WORKER_ID, 'claimed_at': claimed_at},
        )
    ).one()
    await session.execute(
        text("""
            UPDATE horsies_workflow_tasks
            SET status = 'RUNNING', started_at = NOW()
            WHERE workflow_id = :wf AND task_index = 0
        """),
        {'wf': workflow_id},
    )
    await session.commit()
    return _RunningNode(
        workflow_id=workflow_id,
        task_id=str(row.id),
        task_name=str(row.task_name),
        queue_name=str(row.queue_name),
        claimed_at=claimed_at,
    )


async def _finalize(
    opened: ProxiedWorker,
    node: _RunningNode,
    *,
    claimed_at: datetime | None = None,
) -> Result[None, _FinalizeError]:
    return await opened.worker._finalize_task_result(
        node.task_id,
        True,
        _ok_json(1),
        None,
        queue_name=node.queue_name,
        is_workflow_task=True,
        task_name=node.task_name,
        claimed_at=node.claimed_at if claimed_at is None else claimed_at,
    )


async def _nodes(
    session: AsyncSession, workflow_id: str
) -> list[tuple[int, str, bool]]:
    rows = (
        await session.execute(
            text("""
                SELECT task_index, status, task_id IS NOT NULL AS linked
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf ORDER BY task_index
            """),
            {'wf': workflow_id},
        )
    ).all()
    await session.commit()
    return [(int(row[0]), str(row[1]), bool(row[2])) for row in rows]


async def _workflow_status(session: AsyncSession, workflow_id: str) -> str:
    status = await session.scalar(
        text('SELECT status FROM horsies_workflows WHERE id = :wf'),
        {'wf': workflow_id},
    )
    await session.commit()
    return str(status)


async def _history(session: AsyncSession, task_id: str) -> list[tuple[str, str]]:
    rows = (
        await session.execute(
            text(
                'SELECT status, terminalization_kind FROM horsies_task_history '
                'WHERE task_id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).all()
    await session.commit()
    return [(str(row[0]), str(row[1])) for row in rows]


async def _pending_rows(session: AsyncSession, task_id: str) -> int:
    count = await session.scalar(
        text(
            'SELECT count(*) FROM horsies_workflow_phase2_pending '
            'WHERE task_id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    await session.commit()
    assert isinstance(count, int)
    return count


async def _one_pooled_connection(opened: ProxiedWorker) -> None:
    """Use the worker's pool once, so its next statement needs no connect."""
    async with opened.worker.sf() as worker_session:
        _ = await worker_session.execute(text('SELECT 1'))
        await worker_session.commit()


def _retryable_phase1_error(result: Result[None, _FinalizeError]) -> _FinalizeError:
    match result:
        case Err(error):
            assert error.stage is FinalizeStage.PHASE1
            assert error.retryable is True, error
            return error
        case Ok():
            raise AssertionError('finalize succeeded on a reply that was lost')


async def test_a_success_owes_phase_two_and_phase_two_enqueues_the_dependent(
    worker_of: ProxiedWorker,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    node = await _run_node_a(session, await _start_chain(app, broker))

    result = await _finalize(worker_of, node)

    assert result == Ok(None)
    assert await _history(session, node.task_id) == [('COMPLETED', 'COMPLETE_LOCKED')]
    assert await _nodes(session, node.workflow_id) == [
        (0, 'COMPLETED', True),
        (1, 'ENQUEUED', True),
    ]
    assert await _pending_rows(session, node.task_id) == 0
    assert await _workflow_status(session, node.workflow_id) == 'RUNNING'


async def test_a_replay_of_a_committed_success_runs_phase_two(
    worker_of: ProxiedWorker,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase 1 committed and phase 2 never ran: the worker's finalize gets
    AlreadyApplied, reloads the stored result and runs phase 2."""
    node = await _run_node_a(session, await _start_chain(app, broker))
    committed = await finish_workflow_success(
        worker_of.worker.sf,
        FinishWorkflowSuccess(
            task_id=node.task_id,
            fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=node.claimed_at),
            result_json=_ok_json(1),
        ),
    )
    assert isinstance(committed, SuccessApplied)
    assert await _pending_rows(session, node.task_id) == 1
    assert (await _nodes(session, node.workflow_id))[1] == (1, 'PENDING', False)

    result = await _finalize(worker_of, node)

    assert result == Ok(None)
    assert await _history(session, node.task_id) == [('COMPLETED', 'COMPLETE_LOCKED')]
    assert await _nodes(session, node.workflow_id) == [
        (0, 'COMPLETED', True),
        (1, 'ENQUEUED', True),
    ]
    assert await _pending_rows(session, node.task_id) == 0


async def test_a_lost_reply_is_retried_and_the_replay_runs_phase_two(
    proxy: RoundTripProxy,
    worker_of: ProxiedWorker,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The success commits and its reply never reaches the worker."""
    node = await _run_node_a(session, await _start_chain(app, broker))
    await _one_pooled_connection(worker_of)
    proxy.drop_next_execute_reply()

    first = await _finalize(worker_of, node)

    assert proxy.reply_was_dropped(REPLY_DROP_WAIT_S)
    error = _retryable_phase1_error(first)
    # The premise of the replay: phase 1 committed, phase 2 did not run.
    assert await _history(session, node.task_id) == [('COMPLETED', 'COMPLETE_LOCKED')]
    assert await _pending_rows(session, node.task_id) == 1

    await worker_of.worker._retry_finalize_phase1(error, 0.0)

    assert await _nodes(session, node.workflow_id) == [
        (0, 'COMPLETED', True),
        (1, 'ENQUEUED', True),
    ]
    assert await _pending_rows(session, node.task_id) == 0


async def test_a_lost_claim_generation_writes_nothing_and_owes_nothing(
    worker_of: ProxiedWorker,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    node = await _run_node_a(session, await _start_chain(app, broker))

    result = await _finalize(
        worker_of,
        node,
        claimed_at=node.claimed_at + timedelta(seconds=1),
    )

    assert result == Ok(None)
    assert await _history(session, node.task_id) == []
    assert await _nodes(session, node.workflow_id) == [
        (0, 'RUNNING', True),
        (1, 'PENDING', False),
    ]


async def _cancel(broker: PostgresBroker, workflow_id: str) -> None:
    cancelled = await WorkflowHandle(
        workflow_id=workflow_id, broker=broker
    ).cancel_async()
    assert is_ok(cancelled), cancelled


async def _fail(session: AsyncSession, workflow_id: str) -> None:
    """The workflow row FAILED, as the workflow recovery limit leaves it."""
    await session.execute(
        text("""
            UPDATE horsies_workflows
            SET status = 'FAILED', completed_at = NOW(), updated_at = NOW()
            WHERE id = :wf
        """),
        {'wf': workflow_id},
    )
    await session.commit()


@pytest.mark.parametrize('ended_as', ['CANCELLED', 'FAILED'])
async def test_a_node_of_an_ended_workflow_completes_and_enqueues_nothing(
    worker_of: ProxiedWorker,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    ended_as: str,
) -> None:
    """The workflow ended while node A ran. Phase 1 completes the task;
    phase 2 writes the node and consumes the phase-2 record, and the ended
    workflow promotes no dependent: B keeps the status the end left it
    (a cancel skips a PENDING node; a failure leaves it)."""
    node = await _run_node_a(session, await _start_chain(app, broker))
    match ended_as:
        case 'CANCELLED':
            await _cancel(broker, node.workflow_id)
        case _:
            await _fail(session, node.workflow_id)
    dependent_before = (await _nodes(session, node.workflow_id))[1]

    result = await _finalize(worker_of, node)

    assert result == Ok(None)
    assert await _history(session, node.task_id) == [('COMPLETED', 'COMPLETE_LOCKED')]
    assert await _workflow_status(session, node.workflow_id) == ended_as
    assert await _nodes(session, node.workflow_id) == [
        (0, 'COMPLETED', True),
        dependent_before,
    ]
    assert dependent_before[1] in ('PENDING', 'SKIPPED')
    assert dependent_before[2] is False
    assert await _pending_rows(session, node.task_id) == 0
