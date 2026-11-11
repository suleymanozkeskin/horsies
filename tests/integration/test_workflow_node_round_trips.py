# pyright: reportPrivateUsage=false
"""Round trips of a workflow node's finalize, counted at the wire.

Phase 1 and phase 2 are counted apart: ``_persist_task_terminal_state``,
then ``_finalize_workflow_phase``, which are the two halves of
``_finalize_task_result``. Each case starts a real workflow and runs node A
as the claim and the child's start leave it. The count is taken on the
``WARM_RUNS``-th run, in the steady state of psycopg's prepared statements;
the statements are named from an earlier run, whose turns still carry the
SQL text. The worker's engine is the one the CLI builds, with
``worker_pool_pre_ping`` off unless a test turns it on.

Shapes: ``chain`` is A -> B (phase 2 of A promotes B), ``fanout`` is A with
``FANOUT`` dependents (phase 2 of A promotes all of them with the statements
of ``chain``), ``single`` is A alone (phase 2 of A completes the workflow).
"""

from __future__ import annotations

import uuid
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import OnError, TaskNode
from horsies.core.types.result import Ok, is_ok
from horsies.core.worker.worker import Worker
from horsies.core.workflows.lifecycle import start_workflow_async
from tests.integration.autocommit_support import (
    WARM_RUNS,
    WORKER_ID,
    open_proxied_worker,
    proxied_url,
    upstream_of,
)
from tests.integration.round_trip_proxy import RoundTripProxy, Turn

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

PING: Final = 1
"""The pool pre-ping of a checkout, when ``worker_pool_pre_ping`` is on."""
FANOUT: Final = 5
LABEL_RUN: Final = 2
"""A run whose statements are not prepared yet, so each turn shows its SQL."""

PRE_PING = pytest.mark.parametrize(
    'pre_ping', [False, True], ids=['pre_ping_off', 'pre_ping_on']
)

# The statements of each phase, in order, by the start of their SQL text.
SUCCESS_PHASE1: Final = ('SELECT * FROM horsies_finish_workflow_success(',)
FAILURE_PHASE1: Final = (
    'SELECT CAST(status AS text) AS status, retry_count',
    'SELECT * FROM horsies_finish_failed_attempt(',
)
NODE_WRITE: Final = 'WITH woken AS ( SELECT pg_notify('
"""The node write, which sends the capacity wake of the task's queue."""
DEPENDENTS: Final = 'SELECT d.task_index, d.dependencies'
COMPLETION_CHECK: Final = 'SELECT w.status, w.completed_at, w.error'
FIRST_FAILURE: Final = 'SELECT task_name, result FROM horsies_workflow_tasks'
PHASE2: Final[dict[tuple[str, bool], tuple[str, ...]]] = {
    ('chain', True): (
        'BEGIN',
        NODE_WRITE,
        DEPENDENTS,
        "UPDATE horsies_workflow_tasks SET status = 'READY'",
        'SELECT task_index, task_name, status, result',
        "UPDATE horsies_workflow_tasks wt SET status = 'ENQUEUED'",
        'WITH inserted AS ( INSERT INTO horsies_tasks',
        COMPLETION_CHECK,
        'COMMIT',
    ),
    ('fanout', True): (
        'BEGIN',
        NODE_WRITE,
        DEPENDENTS,
        "UPDATE horsies_workflow_tasks SET status = 'READY'",
        'SELECT task_index, task_name, status, result',
        "UPDATE horsies_workflow_tasks wt SET status = 'ENQUEUED'",
        'WITH inserted AS ( INSERT INTO horsies_tasks',
        COMPLETION_CHECK,
        'COMMIT',
    ),
    ('single', True): (
        'BEGIN',
        NODE_WRITE,
        DEPENDENTS,
        COMPLETION_CHECK,
        'SELECT task_index, dependencies FROM horsies_workflow_tasks',
        'SELECT node_id, task_index, task_name, status, result',
        "UPDATE horsies_workflows SET status = 'COMPLETED'",
        'COMMIT',
    ),
    ('chain', False): (
        'BEGIN',
        NODE_WRITE,
        FIRST_FAILURE,
        'UPDATE horsies_workflows SET error',
        DEPENDENTS,
        "UPDATE horsies_workflow_tasks SET status = 'READY'",
        "UPDATE horsies_workflow_tasks SET status = 'SKIPPED'",
        DEPENDENTS,
        COMPLETION_CHECK,
        FIRST_FAILURE,
        "UPDATE horsies_workflows SET status = 'FAILED'",
        'COMMIT',
    ),
    ('single', False): (
        'BEGIN',
        NODE_WRITE,
        FIRST_FAILURE,
        'UPDATE horsies_workflows SET error',
        DEPENDENTS,
        COMPLETION_CHECK,
        FIRST_FAILURE,
        "UPDATE horsies_workflows SET status = 'FAILED'",
        'COMMIT',
    ),
}


@dataclass(frozen=True, slots=True)
class _Measured:
    """The turns of one finalize: phase 1, then phase 2."""

    phase1: tuple[Turn, ...]
    phase2: tuple[Turn, ...]


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


def _report(ok: bool) -> str:
    result: TaskResult[Any, TaskError] = (
        TaskResult(ok=1)
        if ok
        else TaskResult(err=TaskError(error_code='BOOM', message='boom'))
    )
    return dumps_json(encode_task_result(result, JsonValue)).unwrap()


async def _start(app: Horsies, broker: PostgresBroker, fn: Any, shape: str) -> str:
    a = TaskNode(fn=fn, kwargs={'value': 1})
    nodes: list[Any] = [a]
    match shape:
        case 'chain':
            nodes.append(TaskNode(fn=fn, kwargs={'value': 2}, waits_for=[a]))
        case 'fanout':
            nodes.extend(
                TaskNode(fn=fn, kwargs={'value': value}, waits_for=[a])
                for value in range(FANOUT)
            )
        case _:
            pass
    spec = app.workflow(
        f'node_rt_{uuid.uuid4().hex[:6]}',
        nodes,
        definition_key=f'node-rt-{uuid.uuid4().hex[:6]}',
        on_error=OnError.FAIL,
    )
    started = await start_workflow_async(spec, broker)
    assert is_ok(started), started
    return started.ok_value.workflow_id


async def _run_node_a(
    session: AsyncSession, workflow_id: str
) -> tuple[str, str, str, datetime]:
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
            UPDATE horsies_workflow_tasks SET status = 'RUNNING', started_at = NOW()
            WHERE workflow_id = :wf AND task_index = 0
        """),
        {'wf': workflow_id},
    )
    await session.commit()
    return str(row.id), str(row.task_name), str(row.queue_name), claimed_at


async def _finalize_once(
    proxy: RoundTripProxy,
    worker: Worker,
    session: AsyncSession,
    workflow_id: str,
    ok: bool,
) -> _Measured:
    task_id, task_name, queue_name, claimed_at = await _run_node_a(session, workflow_id)
    proxy.start_recording()
    phase1_r = await worker._persist_task_terminal_state(
        task_id=task_id,
        now=datetime.now(timezone.utc),
        ok=True,
        result_json_str=_report(ok),
        failed_reason=None,
        task_name=task_name,
        queue_name=queue_name,
        is_workflow_task=True,
        claimed_at=claimed_at,
    )
    phase1 = proxy.stop_recording()
    assert is_ok(phase1_r), phase1_r
    persisted = phase1_r.ok_value
    assert persisted is not None
    proxy.start_recording()
    phase2_r = await worker._finalize_workflow_phase(
        task_id,
        persisted.result,
        queue_name=persisted.queue_name,
        task_name=persisted.task_name,
    )
    phase2 = proxy.stop_recording()
    assert phase2_r == Ok(None)
    return _Measured(phase1=phase1, phase2=phase2)


def _statements(turns: tuple[Turn, ...]) -> list[str]:
    """The statement each turn sent: a simple query or a parsed text."""
    named: list[str] = []
    for turn in turns:
        texts = [
            message.split(':', 1)[1]
            for message in turn.messages
            if message.startswith(('Q:', 'P:'))
        ]
        assert len(texts) == 1, turn
        named.append(texts[0].removesuffix(' ISOLATION LEVEL READ COMMITTED'))
    return named


def _assert_statements(turns: tuple[Turn, ...], expected: tuple[str, ...]) -> None:
    named = _statements(turns)
    assert len(named) == len(expected), named
    for sent, start in zip(named, expected, strict=True):
        assert sent.startswith(start), (sent, start)


async def _measure(
    proxy: RoundTripProxy,
    db_url: str,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    *,
    shape: str,
    ok: bool,
    pre_ping: bool,
) -> tuple[_Measured, _Measured]:
    """The labelled run and the steady-state run of one case."""

    @app.task(task_name=f'node_rt_{uuid.uuid4().hex[:8]}')
    def node_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    opened = await open_proxied_worker(proxied_url(db_url, proxy), pre_ping=pre_ping)
    opened.worker._app = app
    opened.broker.app = app
    labelled: _Measured | None = None
    steady: _Measured | None = None
    try:
        for run in range(1, WARM_RUNS + 1):
            workflow_id = await _start(app, broker, node_task, shape)
            measured = await _finalize_once(
                proxy, opened.worker, session, workflow_id, ok
            )
            if run == LABEL_RUN:
                labelled = measured
            steady = measured
    finally:
        _ = await opened.broker.close_async()
    assert labelled is not None and steady is not None
    return labelled, steady


@PRE_PING
@pytest.mark.parametrize('shape', ['chain', 'single'])
async def test_phase_one_of_a_success_is_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    shape: str,
    pre_ping: bool,
) -> None:
    labelled, steady = await _measure(
        proxy, db_url, app, broker, session, shape=shape, ok=True, pre_ping=pre_ping
    )

    assert len(steady.phase1) == len(SUCCESS_PHASE1) + (PING if pre_ping else 0)
    if not pre_ping:
        _assert_statements(labelled.phase1, SUCCESS_PHASE1)


@PRE_PING
@pytest.mark.parametrize('shape', ['chain', 'single'])
async def test_phase_one_of_a_failure_is_one_read_and_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    shape: str,
    pre_ping: bool,
) -> None:
    labelled, steady = await _measure(
        proxy, db_url, app, broker, session, shape=shape, ok=False, pre_ping=pre_ping
    )

    assert len(steady.phase1) == len(FAILURE_PHASE1) + (PING if pre_ping else 0)
    if not pre_ping:
        _assert_statements(labelled.phase1, FAILURE_PHASE1)


@PRE_PING
@pytest.mark.parametrize(
    ('shape', 'ok'),
    [
        ('chain', True),
        ('fanout', True),
        ('single', True),
        ('chain', False),
        ('single', False),
    ],
    ids=[
        'chain-success',
        'fanout-success',
        'single-success',
        'chain-failure',
        'single-failure',
    ],
)
async def test_phase_two_statements(
    proxy: RoundTripProxy,
    db_url: str,
    app: Horsies,
    broker: PostgresBroker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    shape: str,
    ok: bool,
    pre_ping: bool,
) -> None:
    """Phase 2 is one transaction that holds the workflow row lock from its
    first statement to its COMMIT."""
    labelled, steady = await _measure(
        proxy, db_url, app, broker, session, shape=shape, ok=ok, pre_ping=pre_ping
    )
    expected = PHASE2[(shape, ok)]

    assert len(steady.phase2) == len(expected) + (PING if pre_ping else 0)
    # The insert of a fan-out runs FANOUT times a run, so it is prepared, and
    # sends no text, before the labelled run; chain names the same statements.
    if not pre_ping and shape != 'fanout':
        _assert_statements(labelled.phase2, expected)
