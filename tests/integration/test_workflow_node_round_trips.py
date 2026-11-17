# pyright: reportPrivateUsage=false, reportUnusedFunction=false
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
of ``chain``), ``single`` is A alone (phase 2 of A completes the workflow;
A has no dependents, so phase 2 does not read them), ``subworkflow`` is A ->
a sub-workflow node (phase 2 of A owes the child start).

Phase 2 is ``horsies_workflow_node_finish`` in the form of the node facts
that phase 1 read: one statement in autocommit when no sub-workflow node is
below A, else a transaction with the owed child start.
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
from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.types.result import Ok, is_ok
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.worker import Worker
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.registry import register_workflow_definition
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
"""The pre-ping of a checkout, when ``worker_pool_pre_ping`` is on. These
tests set the idle limit to 0, so every reused connection is pinged."""


@pytest.fixture(autouse=True)
def _ping_every_reused_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """With ``worker_pool_pre_ping`` on, the count does not depend on how
    long a connection waited in the pool."""
    monkeypatch.setattr(postgres_module, 'PRE_PING_AFTER_IDLE_S', 0.0)
FANOUT: Final = 5
LABEL_RUN: Final = 2
"""A run whose statements are not prepared yet, so each turn shows its SQL."""

PRE_PING = pytest.mark.parametrize(
    'pre_ping', [False, True], ids=['pre_ping_off', 'pre_ping_on']
)

# The statements of each phase, in order, by the start of their SQL text.
SUCCESS_PHASE1: Final = ('SELECT * FROM horsies_finish_workflow_success(',)
FAILURE_PHASE1: Final = ('SELECT * FROM horsies_finish_failed_attempt(',)
STEP: Final = 'SELECT r.row_kind, r.disposition, r.workflow_id'
"""A step statement (``steps._step_statement``): the phase-2 call
(``horsies_workflow_node_finish``: the node write, the capacity wake of the
node's queue, the failure policy, promotion and the completion check), or
the child start that it owes (``horsies_workflow_child_start``). The proxy
keeps the first 90 characters of a statement, which do not reach the
function's name; the order tells them apart."""
ONE_STATEMENT: Final = (STEP,)
PHASE2: Final[dict[tuple[str, bool], tuple[str, ...]]] = {
    ('chain', True): ONE_STATEMENT,
    ('fanout', True): ONE_STATEMENT,
    ('single', True): ONE_STATEMENT,
    ('subworkflow', True): ('BEGIN', STEP, STEP, 'COMMIT'),
    ('chain', False): ONE_STATEMENT,
    ('single', False): ONE_STATEMENT,
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


def _child_definition(fn: Any) -> type[WorkflowDefinition[int]]:
    """A registered child definition of one task node of ``fn``."""
    suffix = uuid.uuid4().hex[:8]

    class RoundTripChild(WorkflowDefinition[int]):
        name = f'node_rt_child_{suffix}'
        definition_key = f'node-rt-child-{suffix}'

        @classmethod
        def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
            _ = (args, params)
            node = TaskNode(fn=fn, kwargs={'value': 3})
            return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)

    register_workflow_definition(RoundTripChild)
    return RoundTripChild


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
        case 'subworkflow':
            nodes.append(SubWorkflowNode(workflow_def=_child_definition(fn), waits_for=[a]))
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
                SET status = 'RUNNING',
                    claimed_by_worker_id = :worker, claimed_at = :claimed_at,
                    claim_expires_at = NULL, started_at = NOW(),
                    worker_pid = 4242, worker_hostname = 'host-1',
                    worker_process_name = 'child-1'
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
        claim=DispatchClaim(
            claimed_at=claimed_at, retry_count=0, max_retries=0, task_options=None,
        ),
    )
    phase1 = proxy.stop_recording()
    assert is_ok(phase1_r), phase1_r
    persisted = phase1_r.ok_value
    assert persisted is not None
    proxy.start_recording()
    phase2_r = await worker._finalize_workflow_phase(task_id, persisted)
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
        await opened.close()
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
async def test_phase_one_of_a_failure_is_one_statement(
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
        ('subworkflow', True),
        ('chain', False),
        ('single', False),
    ],
    ids=[
        'chain-success',
        'fanout-success',
        'single-success',
        'subworkflow-success',
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
    """Phase 2 is one statement in autocommit when no sub-workflow node is
    below the node, and a transaction with the owed child start when one
    is: the workflow row lock is held from the step to the COMMIT."""
    labelled, steady = await _measure(
        proxy, db_url, app, broker, session, shape=shape, ok=ok, pre_ping=pre_ping
    )
    expected = PHASE2[(shape, ok)]

    assert len(steady.phase2) == len(expected) + (PING if pre_ping else 0)
    # The insert of a fan-out runs FANOUT times a run, so it is prepared, and
    # sends no text, before the labelled run; chain names the same statements.
    if not pre_ping and shape != 'fanout':
        _assert_statements(labelled.phase2, expected)
