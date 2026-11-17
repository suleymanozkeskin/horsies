"""Dual-run of the resume switch: the session statements against ``horsies_workflow_resume``.

Reference: ``tests/dualrun/reference/workflow_resume.py``. Candidate:
``resume_workflow`` (``core/workflows/lifecycle.py``). The seed pauses a
RUNNING tree (each side pauses it and finishes one node through the SQL
functions, after the history readers are published) whose root holds: a
node that finished while the tree was paused, its PENDING task dependent, a READY node whose claimed task the
pause abandoned, a PENDING sub-workflow dependent (its child start is owed
at the resume), and a sub-workflow root whose PAUSED child holds a READY
node. A RUNNING bystander and an unknown id complete the targets. Each side
resumes the tree, the bystander and the unknown id.

The caller pair runs the same seed with the Python caller before the switch
(``tests/dualrun/reference/workflow_resume_caller.py``) against the caller on
the core (``CoreHandle.resume_workflow_async``), whose owed child start
builds its child through the broker's child spec source.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, assert_never

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode, WorkflowDefinition, WorkflowSpec
from horsies.core.models.workflow.handle_types import HandleOperationError, HandleWorkflowNotFound
from horsies.core.types.result import Err, Ok, Result
from horsies.core.workflows.lifecycle import resume_workflow
from tests.dualrun.reference import workflow_resume_caller as caller_reference
from horsies.core.workflows.registry import register_workflow_definition
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_resume as reference
from tests.dualrun.workflow_seeds import (
    ChildOf,
    OnError,
    TopLevel,
    WorkflowSeed,
    claim_node_task,
    cover_history,
    finish_node_task,
    pause_tree,
    run_node_task,
    run_or_fail,
    start_tree,
    subworkflow_node,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER = 'worker-dualrun-resume'
_TASK = 'dualrun.resume.value'
_RESULT = '{"__h_task_result__":true,"ok":5,"err":null}'

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://dualrun@127.0.0.1:1/unused'),
        ),
    ),
)


@APP.task(task_name=_TASK)
def value_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok=5)


class ResumeChild(WorkflowDefinition[int]):
    """The child that the resume starts for the promoted sub-workflow node."""

    name = 'dualrun.resume.child'
    definition_key = 'dualrun.resume.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(fn=value_task)
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The child definition, registered again: other tests clear the registry."""
    register_workflow_definition(ResumeChild)


@dataclass(frozen=True, slots=True)
class _Seed:
    tree: uuid.UUID
    running: uuid.UUID
    unknown: uuid.UUID


def _seed(connection: Connection) -> _Seed:
    root, child = uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=root,
                name='resume.root',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, _TASK),
                    task_node(1, _TASK, 0),
                    task_node(2, _TASK),
                    subworkflow_node(
                        3, ResumeChild.name, 0, definition_key=ResumeChild.definition_key,
                    ),
                    subworkflow_node(4, 'resume.child'),
                ),
            ),
            WorkflowSeed(
                workflow_id=child,
                name='resume.child',
                on_error=OnError.FAIL,
                parent=ChildOf(parent=root, parent_task_index=4, depth=1),
                nodes=(task_node(0, _TASK), task_node(1, _TASK, 0)),
            ),
        ],
    )
    _ = run_node_task(connection, root, 0, _WORKER)
    _ = claim_node_task(connection, root, 2, _WORKER)
    _ = claim_node_task(connection, child, 0, _WORKER)

    running = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=running,
                name='resume.running',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, _TASK),),
            ),
        ],
    )
    return _Seed(tree=root, running=running, unknown=uuid.uuid4())


def _pause_and_finish(database: DualRunDatabase, seed: _Seed) -> None:
    """The rest of the seed state, written after the history readers are
    published (the pause abandons the claimed tasks, and the finish of node 0
    moves its task to history): the tree paused, then node 0 finished while
    the tree is paused."""
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            pause_tree(connection, seed.tree)
            _ = finish_node_task(connection, seed.tree, 0, _WORKER, _RESULT)


def _targets(seed: _Seed) -> tuple[uuid.UUID, ...]:
    return (seed.tree, seed.running, seed.unknown)


def _attach(broker: PostgresBroker) -> None:
    broker.app = APP
    APP._broker = broker  # pyright: ignore[reportPrivateUsage]


async def _reference_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.ResumeReport, ...]:
    async with open_broker(database) as broker:
        _attach(broker)
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        return tuple([
            await reference.resume(resources.session_factory, broker, str(target))
            for target in _targets(seed)
        ])


def _reference(database: DualRunDatabase, seed: _Seed) -> tuple[reference.ResumeReport, ...]:
    _pause_and_finish(database, seed)
    return asyncio.run(_reference_async(database, seed))


def _report(result: Result[bool, HandleOperationError]) -> reference.ResumeReport:
    match result:
        case Ok(resumed):
            match resumed:
                case True:
                    return reference.Resumed()
                case False:
                    return reference.NotPaused()
        case Err(HandleWorkflowNotFound()):
            return reference.NotFound()
        case Err(other):
            raise AssertionError(f'the resume answered {other!r}')
        case _ as unreachable:
            assert_never(unreachable)


async def _candidate_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.ResumeReport, ...]:
    async with open_broker(database) as broker:
        _attach(broker)
        return tuple([
            _report(await resume_workflow(broker, str(target))) for target in _targets(seed)
        ])


def _candidate(database: DualRunDatabase, seed: _Seed) -> tuple[reference.ResumeReport, ...]:
    _pause_and_finish(database, seed)
    return asyncio.run(_candidate_async(database, seed))


def test_resume_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_resume', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    expected = (reference.Resumed(), reference.NotPaused(), reference.NotFound())
    assert run.reference.report == expected
    assert run.candidate.report == expected


async def _caller_reference_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.ResumeReport, ...]:
    async with open_broker(database) as broker:
        _attach(broker)
        return tuple([
            _report(await caller_reference.resume_workflow(broker, str(target)))
            for target in _targets(seed)
        ])


def _caller_reference(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.ResumeReport, ...]:
    _pause_and_finish(database, seed)
    return asyncio.run(_caller_reference_async(database, seed))


def test_the_resume_on_the_core_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='python caller', act=_caller_reference),
            OperationSide(name='core caller', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    expected = (reference.Resumed(), reference.NotPaused(), reference.NotFound())
    assert run.reference.report == expected
    assert run.candidate.report == expected
