"""Dual-run of the phase-2 switch: the session statements of the worker's
phase 2 against ``horsies_workflow_node_finish``, one test per form of the
call.

Reference: ``tests/dualrun/reference/workflow_node_finish.py``. Candidate:
``node_finish.finish_node`` in the form of the node facts, then
``steps.advance_parents``. Each side publishes the history readers, then for
each task of the test writes phase 1 through the production phase-1 function
(``horsies_finish_workflow_success``, or ``horsies_finish_failed_attempt``
with no retry) and runs phase 2 of that task:

- one statement, no dependents: the last node of a workflow, and the last
  node of a child workflow (its parent node advances after the commit and
  promotes a task node);
- one statement, dependents read: task dependents (one receives the result
  through ``args_from``), a failure under ``on_error`` fail (the dependent
  is skipped, the workflow fails), the node of a workflow cancelled after
  the task started (written; nothing promoted);
- a transaction with the owed child start: a sub-workflow dependent next to
  a task dependent, and a failure under ``on_error`` pause with a
  sub-workflow node below it;
- a transaction with no phase-1 facts: a phase-2 retry, then a replay of the
  applied phase 2.

The caller pair runs the same seeds with the Python steps that the worker
called before the switch (``finish_node`` and ``advance_parents``) against the
core operations that it calls now (``CoreHandle.finish_workflow_node_async``
and ``advance_workflow_parents_async``), whose owed child starts build their
children through the broker's child spec source.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import FailureApplied, PhaseTwoOwed, PhaseTwoStep, SuccessApplied
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode, WorkflowDefinition, WorkflowSpec
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.utils.db import autocommit_connection
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    FinishFailedAttempt,
    NoRetry,
    finish_failed_attempt,
)
from horsies.core.worker.workflow_success import FinishWorkflowSuccess, finish_workflow_success
from horsies.core.workflows.node_finish import CapacityWake, FactsUnknown, finish_node
from horsies.core.workflows.registry import register_workflow_definition
from horsies.core.workflows.steps import advance_parents
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_node_finish as reference
from tests.dualrun.workflow_seeds import (
    ChildOf,
    OnError,
    TopLevel,
    WorkflowSeed,
    args_from_node,
    cover_history,
    end_workflow,
    run_node_task,
    run_or_fail,
    start_tree,
    subworkflow_node,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER = 'worker-dualrun-node-finish'
_VALUE = 'dualrun.node_finish.value'
_CONSUME = 'dualrun.node_finish.consume'
_QUEUE = 'default'

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://dualrun@127.0.0.1:1/unused'),
        ),
    ),
)


@APP.task(task_name=_VALUE)
def value_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok=7)


@APP.task(task_name=_CONSUME)
def consume_task(*, value: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value.unwrap() + 1)


class NodeFinishChild(WorkflowDefinition[int]):
    """The child that a promoted sub-workflow node starts."""

    name = 'dualrun.node_finish.child'
    definition_key = 'dualrun.node_finish.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(fn=value_task)
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The child definition, registered again: other tests clear the registry."""
    register_workflow_definition(NodeFinishChild)


class Ending(Enum):
    """How the task of a node ends."""

    SUCCESS = 'SUCCESS'
    """``TaskResult(ok=7)``."""

    FAILURE = 'FAILURE'
    """A task error; the node has no retry."""


class PhaseTwoRun(Enum):
    """How the candidate runs phase 2 of a task."""

    AFTER_PHASE_ONE = 'AFTER_PHASE_ONE'
    """Phase 1, then phase 2 in the form of the node facts that phase 1 read."""

    RETRY = 'RETRY'
    """Phase 1, then phase 2 with no facts, as a phase-2 retry runs it."""

    REPLAY = 'REPLAY'
    """Phase 2 alone with no facts, after an earlier phase 2 of the task."""


@dataclass(frozen=True, slots=True)
class Finish:
    """The end of the task of node ``index`` of ``workflow_id``."""

    workflow_id: uuid.UUID
    index: int
    ending: Ending
    run: PhaseTwoRun


@dataclass(frozen=True, slots=True)
class FinishReport:
    """What a side learned of one finish: the node facts of phase 1 (``None``
    for a replay, which writes no phase 1) and, on the candidate side, the
    disposition of phase 2 (``None`` on the reference side)."""

    step: PhaseTwoStep | None
    disposition: str | None


_ERROR = TaskError(error_code='NODE_FINISH_FAILED', message='the task failed', data={'k': 1})


def _result(ending: Ending) -> TaskResult[Any, TaskError]:
    match ending:
        case Ending.SUCCESS:
            return TaskResult(ok=7)
        case Ending.FAILURE:
            return TaskResult(err=_ERROR)


def _result_json(ending: Ending) -> str:
    """The result text that a child sends for ``ending``, as the worker encodes it."""
    encoded = dumps_json(encode_task_result(_result(ending), int))
    assert not is_err(encoded), encoded
    return encoded.ok_value


_CLAIMED_AT_SQL = text('SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:id AS uuid)')
_NODE_TASK_SQL = text("""
    SELECT task_id, task_name FROM horsies_workflow_tasks
    WHERE workflow_id = CAST(:wf AS uuid) AND task_index = :idx
""")


@dataclass(frozen=True, slots=True)
class _NodeTask:
    task_id: str
    task_name: str


async def _task_of(
    session_factory: async_sessionmaker[AsyncSession], finish: Finish,
) -> _NodeTask:
    """The task of the node and its name, as the worker reads them from the task row."""
    async with session_factory() as session:
        row = (
            await session.execute(
                _NODE_TASK_SQL, {'wf': str(finish.workflow_id), 'idx': finish.index},
            )
        ).one()
    return _NodeTask(task_id=str(row.task_id), task_name=str(row.task_name))


async def _phase_one(
    session_factory: async_sessionmaker[AsyncSession], task_id: str, ending: Ending,
) -> PhaseTwoStep:
    """Phase 1 of a RUNNING task through the production function; the node facts it read."""
    async with session_factory() as session:
        claimed_at = (await session.execute(_CLAIMED_AT_SQL, {'id': task_id})).scalar_one()
    fence = OwnedClaim(worker_id=_WORKER, claimed_at=claimed_at)
    match ending:
        case Ending.SUCCESS:
            outcome = await finish_workflow_success(
                session_factory,
                FinishWorkflowSuccess(
                    task_id=task_id, fence=fence, result_json=_result_json(ending),
                ),
            )
            assert isinstance(outcome, SuccessApplied), outcome
            return outcome.phase_two.step
        case Ending.FAILURE:
            async with session_factory() as session:
                failed = await finish_failed_attempt(
                    await autocommit_connection(session),
                    FinishFailedAttempt(
                        task_id=task_id,
                        fence=fence,
                        retry=NoRetry(),
                        result_json=_result_json(ending),
                        error_code='NODE_FINISH_FAILED',
                        failed_reason=None,
                        attempt=AttemptFailure(
                            kind=AttemptFailureKind.FAILED,
                            error_message=_ERROR.message,
                            failed_reason=None,
                        ),
                    ),
                )
            assert isinstance(failed, FailureApplied), failed
            assert isinstance(failed.after, PhaseTwoOwed), failed.after
            return failed.after.step


def _attach(broker: PostgresBroker) -> None:
    broker.app = APP
    APP._broker = broker  # pyright: ignore[reportPrivateUsage]


async def _resources(
    broker: PostgresBroker,
) -> async_sessionmaker[AsyncSession]:
    match await broker.select_resources():
        case Err(limit):
            raise AssertionError(f'no resources: {limit}')
        case Ok(resources):
            return resources.session_factory


async def _reference_async(
    database: DualRunDatabase, finishes: Sequence[Finish],
) -> tuple[FinishReport, ...]:
    reports: list[FinishReport] = []
    async with open_broker(database) as broker:
        _attach(broker)
        session_factory = await _resources(broker)
        for finish in finishes:
            task = await _task_of(session_factory, finish)
            match finish.run:
                case PhaseTwoRun.AFTER_PHASE_ONE | PhaseTwoRun.RETRY:
                    step: PhaseTwoStep | None = await _phase_one(
                        session_factory, task.task_id, finish.ending,
                    )
                case PhaseTwoRun.REPLAY:
                    step = None
            await reference.finish_node(
                session_factory,
                broker,
                task_id=task.task_id,
                task_name=task.task_name,
                queue_name=_QUEUE,
                result=_result(finish.ending),
            )
            reports.append(FinishReport(step=step, disposition=None))
    return tuple(reports)


async def _candidate_async(
    database: DualRunDatabase, finishes: Sequence[Finish],
) -> tuple[FinishReport, ...]:
    reports: list[FinishReport] = []
    async with open_broker(database) as broker:
        _attach(broker)
        session_factory = await _resources(broker)
        for finish in finishes:
            task = await _task_of(session_factory, finish)
            match finish.run:
                case PhaseTwoRun.AFTER_PHASE_ONE:
                    phase_one = await _phase_one(session_factory, task.task_id, finish.ending)
                    step: PhaseTwoStep | None = phase_one
                    facts: PhaseTwoStep | FactsUnknown = phase_one
                case PhaseTwoRun.RETRY:
                    step = await _phase_one(session_factory, task.task_id, finish.ending)
                    facts = FactsUnknown()
                case PhaseTwoRun.REPLAY:
                    step = None
                    facts = FactsUnknown()
            committed = await finish_node(
                session_factory, broker, task.task_id, facts, CapacityWake.SEND,
            )
            failure = await advance_parents(broker, session_factory, committed.parent_advances)
            assert failure is None, failure
            reports.append(
                FinishReport(step=step, disposition=committed.outcome.disposition.value),
            )
    return tuple(reports)


def _side(
    finishes: Callable[[Any], Sequence[Finish]],
    run: Callable[[DualRunDatabase, Sequence[Finish]], Any],
) -> Callable[[DualRunDatabase, Any], tuple[FinishReport, ...]]:
    def act(database: DualRunDatabase, seed: Any) -> tuple[FinishReport, ...]:
        cover_history(database)
        return asyncio.run(run(database, finishes(seed)))

    return act


def _run(
    empty_database: MakeDatabase,
    seed: Callable[[Connection], Any],
    finishes: Callable[[Any], Sequence[Finish]],
) -> tuple[tuple[FinishReport, ...], tuple[FinishReport, ...]]:
    run = run_or_fail(
        dual_run(
            empty_database,
            seed,
            OperationSide(name='session statements', act=_side(finishes, _reference_async)),
            OperationSide(
                name='horsies_workflow_node_finish', act=_side(finishes, _candidate_async),
            ),
        )
    )
    assert run.diff.entries == (), render_diff(run.diff)
    return run.reference.report, run.candidate.report


def _top(workflow_id: uuid.UUID, name: str, on_error: OnError, *nodes: Any) -> WorkflowSeed:
    return WorkflowSeed(
        workflow_id=workflow_id, name=name, on_error=on_error, parent=TopLevel(), nodes=nodes,
    )


def _child_start(index: int, *dependencies: int) -> Any:
    return subworkflow_node(
        index, NodeFinishChild.name, *dependencies, definition_key=NodeFinishChild.definition_key,
    )


# ---------------------------------------------------------------------------
# One statement, no dependents
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _NoDependents:
    top: uuid.UUID
    parent: uuid.UUID
    child: uuid.UUID


def _seed_no_dependents(connection: Connection) -> _NoDependents:
    top, parent, child = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    start_tree(connection, [_top(top, 'nf.none.top', OnError.FAIL, task_node(0, _VALUE))])
    start_tree(
        connection,
        [
            _top(
                parent, 'nf.none.parent', OnError.FAIL,
                subworkflow_node(0, 'nf.none.child'), task_node(1, _VALUE, 0),
            ),
            WorkflowSeed(
                workflow_id=child,
                name='nf.none.child',
                on_error=OnError.FAIL,
                parent=ChildOf(parent=parent, parent_task_index=0, depth=1),
                nodes=(task_node(0, _VALUE),),
            ),
        ],
    )
    _ = run_node_task(connection, top, 0, _WORKER)
    _ = run_node_task(connection, child, 0, _WORKER)
    return _NoDependents(top=top, parent=parent, child=child)


def _finishes_no_dependents(seed: _NoDependents) -> Sequence[Finish]:
    return (
        Finish(seed.top, 0, Ending.SUCCESS, PhaseTwoRun.AFTER_PHASE_ONE),
        Finish(seed.child, 0, Ending.SUCCESS, PhaseTwoRun.AFTER_PHASE_ONE),
    )


def test_one_statement_without_dependents(empty_database: MakeDatabase) -> None:
    reference_report, candidate_report = _run(
        empty_database, _seed_no_dependents, _finishes_no_dependents,
    )

    assert [entry.step for entry in reference_report] == [PhaseTwoStep.NO_DEPENDENTS] * 2
    assert candidate_report == (
        FinishReport(step=PhaseTwoStep.NO_DEPENDENTS, disposition='APPLIED'),
        FinishReport(step=PhaseTwoStep.NO_DEPENDENTS, disposition='APPLIED'),
    )


# ---------------------------------------------------------------------------
# One statement, dependents read
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Dependents:
    promote: uuid.UUID
    failed: uuid.UUID
    ended: uuid.UUID


def _seed_dependents(connection: Connection) -> _Dependents:
    promote, failed, ended = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            _top(
                promote, 'nf.read.promote', OnError.FAIL,
                task_node(0, _VALUE),
                args_from_node(1, _CONSUME, (('value', 0),), 0),
                task_node(2, _VALUE, 0),
            ),
        ],
    )
    start_tree(
        connection,
        [_top(failed, 'nf.read.failed', OnError.FAIL, task_node(0, _VALUE), task_node(1, _VALUE, 0))],
    )
    start_tree(
        connection,
        [_top(ended, 'nf.read.ended', OnError.FAIL, task_node(0, _VALUE), task_node(1, _VALUE, 0))],
    )
    for workflow_id in (promote, failed, ended):
        _ = run_node_task(connection, workflow_id, 0, _WORKER)
    end_workflow(connection, ended, 'CANCELLED')
    return _Dependents(promote=promote, failed=failed, ended=ended)


def _finishes_dependents(seed: _Dependents) -> Sequence[Finish]:
    return (
        Finish(seed.promote, 0, Ending.SUCCESS, PhaseTwoRun.AFTER_PHASE_ONE),
        Finish(seed.failed, 0, Ending.FAILURE, PhaseTwoRun.AFTER_PHASE_ONE),
        Finish(seed.ended, 0, Ending.SUCCESS, PhaseTwoRun.AFTER_PHASE_ONE),
    )


def test_one_statement_with_dependents(empty_database: MakeDatabase) -> None:
    reference_report, candidate_report = _run(
        empty_database, _seed_dependents, _finishes_dependents,
    )

    steps = [PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW] * 3
    assert [entry.step for entry in reference_report] == steps
    assert candidate_report == (
        FinishReport(step=steps[0], disposition='APPLIED'),
        FinishReport(step=steps[1], disposition='APPLIED'),
        FinishReport(step=steps[2], disposition='APPLIED_WORKFLOW_ENDED'),
    )


# ---------------------------------------------------------------------------
# A transaction with the owed child start
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Reachable:
    started: uuid.UUID
    paused: uuid.UUID


def _seed_reachable(connection: Connection) -> _Reachable:
    started, paused = uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            _top(
                started, 'nf.sub.started', OnError.FAIL,
                task_node(0, _VALUE), _child_start(1, 0), task_node(2, _VALUE, 0),
            ),
        ],
    )
    start_tree(
        connection,
        [_top(paused, 'nf.sub.paused', OnError.PAUSE, task_node(0, _VALUE), _child_start(1, 0))],
    )
    for workflow_id in (started, paused):
        _ = run_node_task(connection, workflow_id, 0, _WORKER)
    return _Reachable(started=started, paused=paused)


def _finishes_reachable(seed: _Reachable) -> Sequence[Finish]:
    return (
        Finish(seed.started, 0, Ending.SUCCESS, PhaseTwoRun.AFTER_PHASE_ONE),
        Finish(seed.paused, 0, Ending.FAILURE, PhaseTwoRun.AFTER_PHASE_ONE),
    )


def test_transaction_with_an_owed_child_start(empty_database: MakeDatabase) -> None:
    reference_report, candidate_report = _run(
        empty_database, _seed_reachable, _finishes_reachable,
    )

    steps = [PhaseTwoStep.SUBWORKFLOW_REACHABLE] * 2
    assert [entry.step for entry in reference_report] == steps
    assert candidate_report == (
        FinishReport(step=steps[0], disposition='APPLIED'),
        FinishReport(step=steps[1], disposition='APPLIED'),
    )


# ---------------------------------------------------------------------------
# A transaction with no phase-1 facts
# ---------------------------------------------------------------------------


def _seed_retry(connection: Connection) -> uuid.UUID:
    retried = uuid.uuid4()
    start_tree(
        connection,
        [
            _top(
                retried, 'nf.retry', OnError.FAIL,
                task_node(0, _VALUE), task_node(1, _VALUE, 0), _child_start(2, 0),
            ),
        ],
    )
    _ = run_node_task(connection, retried, 0, _WORKER)
    return retried


def _finishes_retry(seed: uuid.UUID) -> Sequence[Finish]:
    return (
        Finish(seed, 0, Ending.SUCCESS, PhaseTwoRun.RETRY),
        Finish(seed, 0, Ending.SUCCESS, PhaseTwoRun.REPLAY),
    )


def test_transaction_without_phase_one_facts(empty_database: MakeDatabase) -> None:
    reference_report, candidate_report = _run(empty_database, _seed_retry, _finishes_retry)

    assert [entry.step for entry in reference_report] == [
        PhaseTwoStep.SUBWORKFLOW_REACHABLE, None,
    ]
    assert candidate_report == (
        FinishReport(step=PhaseTwoStep.SUBWORKFLOW_REACHABLE, disposition='APPLIED'),
        FinishReport(step=None, disposition='ALREADY_APPLIED'),
    )



# ---------------------------------------------------------------------------
# The caller pair: the Python steps against the core operations
# ---------------------------------------------------------------------------


def _facts_text(
    facts: PhaseTwoStep | FactsUnknown,
) -> Literal['NO_DEPENDENTS', 'DEPENDENTS_WITHOUT_SUBWORKFLOW', 'SUBWORKFLOW_REACHABLE', 'UNKNOWN']:
    match facts:
        case PhaseTwoStep.NO_DEPENDENTS:
            return 'NO_DEPENDENTS'
        case PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW:
            return 'DEPENDENTS_WITHOUT_SUBWORKFLOW'
        case PhaseTwoStep.SUBWORKFLOW_REACHABLE:
            return 'SUBWORKFLOW_REACHABLE'
        case FactsUnknown():
            return 'UNKNOWN'


async def _core_async(
    database: DualRunDatabase, finishes: Sequence[Finish],
) -> tuple[FinishReport, ...]:
    reports: list[FinishReport] = []
    async with open_broker(database) as broker:
        _attach(broker)
        session_factory = await _resources(broker)
        handle = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        for finish in finishes:
            task = await _task_of(session_factory, finish)
            match finish.run:
                case PhaseTwoRun.AFTER_PHASE_ONE:
                    phase_one = await _phase_one(session_factory, task.task_id, finish.ending)
                    step: PhaseTwoStep | None = phase_one
                    facts: PhaseTwoStep | FactsUnknown = phase_one
                case PhaseTwoRun.RETRY:
                    step = await _phase_one(session_factory, task.task_id, finish.ending)
                    facts = FactsUnknown()
                case PhaseTwoRun.REPLAY:
                    step = None
                    facts = FactsUnknown()
            finished = await handle.finish_workflow_node_async(
                core.NodeFinishRequest(
                    task_id=uuid.UUID(task.task_id), facts=_facts_text(facts), wake='SEND',
                ),
            )
            assert isinstance(finished, core.NodeFinished), finished
            advanced = await handle.advance_workflow_parents_async(finished.parent_advances)
            assert isinstance(advanced, core.ParentsAdvanced), advanced
            reports.append(FinishReport(step=step, disposition=finished.disposition))
    return tuple(reports)


@pytest.mark.parametrize(
    ('seed', 'finishes'),
    [
        (_seed_no_dependents, _finishes_no_dependents),
        (_seed_dependents, _finishes_dependents),
        (_seed_reachable, _finishes_reachable),
        (_seed_retry, _finishes_retry),
    ],
)
def test_phase_two_on_the_core_writes_what_the_python_steps_wrote(
    empty_database: MakeDatabase,
    seed: Callable[[Connection], Any],
    finishes: Callable[[Any], Sequence[Finish]],
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            seed,
            OperationSide(name='python steps', act=_side(finishes, _candidate_async)),
            OperationSide(name='core operations', act=_side(finishes, _core_async)),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.candidate.report == run.reference.report
