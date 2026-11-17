"""Dual-run of the recovery pass redesign: the session engine against the 0005 recovery functions.

Reference: ``tests/dualrun/reference/workflow_recovery_pass.py`` (the case
queries and the session engine, one SAVEPOINT per candidate). Candidate:
``recover_stuck_workflows_global`` (``core/workflows/recovery.py``, one
transaction per candidate on the step functions). The seed holds one workflow
per recovery case:

- case 0: a PENDING node whose one dependency is COMPLETED;
- case 1: a READY task node with no task;
- case 1.5: a READY sub-workflow node with no child, whose definition is
  registered (the pass builds and starts the child);
- case 1.6: a child workflow that finished while its parent node stayed
  RUNNING;
- cases 2/3: a top-level workflow and a child workflow whose nodes all
  finished while the workflow row stayed RUNNING (the child's parent node
  advances after its completion).

Each side publishes the history readers, finishes the nodes that the seed
started, and runs one global pass.

The orphan pair holds a RUNNING workflow with no node (case 4). Its stored
error is the one named difference: the catalog code
``WORKFLOW_START_INCOMPLETE`` in place of Python's ``E400``, which is not a
catalog error (approved rule for the stored error of recovery case 4).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, LiteralString, cast

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
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.recovery import recover_stuck_workflows_global
from horsies.core.workflows.registry import register_workflow_definition
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import ColumnDifference, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_recovery_pass as reference
from tests.dualrun.workflow_seeds import (
    ChildOf,
    OnError,
    TopLevel,
    WorkflowSeed,
    cover_history,
    finish_node_task,
    node_task_id,
    run_node_task,
    run_or_fail,
    start_tree,
    subworkflow_node,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER: Final = 'worker-dualrun-recovery'
_TASK: Final = 'dualrun.recovery.value'
_RESULT: Final = '{"__h_task_result__":true,"ok":5,"err":null}'

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


class RecoveryChild(WorkflowDefinition[int]):
    """The child that case 1.5 builds and starts."""

    name = 'dualrun.recovery.child'
    definition_key = 'dualrun.recovery.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(fn=value_task)
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The child definition, registered again: other tests clear the registry."""
    register_workflow_definition(RecoveryChild)


_COMPLETE_ENQUEUED_NODE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'COMPLETED', result = %(result)s, started_at = now(), completed_at = now()
    WHERE workflow_id = %(wf)s AND task_index = %(idx)s AND status = 'ENQUEUED'
"""

_DROP_PENDING_TASK_SQL: Final[LiteralString] = """
    DELETE FROM horsies_tasks WHERE id = %(task)s AND status = 'PENDING'
"""

_READY_WITHOUT_TASK_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks SET status = 'READY', task_id = NULL
    WHERE workflow_id = %(wf)s AND task_index = %(idx)s AND status = 'ENQUEUED'
"""

_READY_PENDING_NODE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks SET status = 'READY'
    WHERE workflow_id = %(wf)s AND task_index = %(idx)s AND status = 'PENDING'
"""

_REOPEN_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET status = 'RUNNING', result = NULL, completed_at = NULL, updated_at = now()
    WHERE id = %(wf)s AND status = 'COMPLETED'
"""

_ORPHAN_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflows (
        id, name, status, on_error, depth, root_workflow_id, sent_at, created_at, updated_at
    ) VALUES (%(wf)s, 'recovery.orphan', 'RUNNING', 'fail', 0, %(wf)s, now(), now(), now())
"""


def _one_row(connection: Connection, statement: LiteralString, parameters: dict[str, Any]) -> None:
    cursor = connection.execute(statement, parameters)
    assert cursor.rowcount == 1, f'{statement.split()[0]} wrote {cursor.rowcount} rows'


def _top(workflow_id: uuid.UUID, name: str, *nodes: Any) -> WorkflowSeed:
    return WorkflowSeed(
        workflow_id=workflow_id,
        name=name,
        on_error=OnError.FAIL,
        parent=TopLevel(),
        nodes=tuple(nodes),
    )


def _child(workflow_id: uuid.UUID, name: str, parent: uuid.UUID) -> WorkflowSeed:
    return WorkflowSeed(
        workflow_id=workflow_id,
        name=name,
        on_error=OnError.FAIL,
        parent=ChildOf(parent=parent, parent_task_index=0, depth=1),
        nodes=(task_node(0, _TASK),),
    )


@dataclass(frozen=True, slots=True)
class _Seed:
    finished_child: uuid.UUID
    """Case 1.6: its node finishes at the act; its parent node stays RUNNING."""
    stuck_top: uuid.UUID
    """Cases 2/3: its node finishes at the act; the row is set RUNNING again."""
    stuck_child: uuid.UUID
    """Cases 2/3 of a child: as ``stuck_top``, under a RUNNING parent node."""


def _seed_case0(connection: Connection) -> None:
    workflow = uuid.uuid4()
    start_tree(
        connection,
        [_top(workflow, 'recovery.case0', task_node(0, _TASK), task_node(1, _TASK, 0))],
    )
    task = node_task_id(connection, workflow, 0)
    _one_row(connection, _COMPLETE_ENQUEUED_NODE_SQL, {'wf': workflow, 'idx': 0, 'result': _RESULT})
    _one_row(connection, _DROP_PENDING_TASK_SQL, {'task': task})


def _seed_case1(connection: Connection) -> None:
    workflow = uuid.uuid4()
    start_tree(connection, [_top(workflow, 'recovery.case1', task_node(0, _TASK))])
    task = node_task_id(connection, workflow, 0)
    _one_row(connection, _DROP_PENDING_TASK_SQL, {'task': task})
    _one_row(connection, _READY_WITHOUT_TASK_SQL, {'wf': workflow, 'idx': 0})


def _seed_case1_5(connection: Connection) -> None:
    workflow = uuid.uuid4()
    start_tree(
        connection,
        [
            _top(
                workflow,
                'recovery.case1_5',
                task_node(0, _TASK),
                subworkflow_node(
                    1, RecoveryChild.name, 0, definition_key=RecoveryChild.definition_key,
                ),
            ),
        ],
    )
    _one_row(connection, _READY_PENDING_NODE_SQL, {'wf': workflow, 'idx': 1})


def _seed_parent_and_child(connection: Connection, name: str) -> uuid.UUID:
    parent, child = uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            _top(parent, name, subworkflow_node(0, f'{name}.child')),
            _child(child, f'{name}.child', parent),
        ],
    )
    _ = run_node_task(connection, child, 0, _WORKER)
    return child


def _seed(connection: Connection) -> _Seed:
    _seed_case0(connection)
    _seed_case1(connection)
    _seed_case1_5(connection)
    finished_child = _seed_parent_and_child(connection, 'recovery.case1_6')
    stuck_top = uuid.uuid4()
    start_tree(connection, [_top(stuck_top, 'recovery.case2_3', task_node(0, _TASK))])
    _ = run_node_task(connection, stuck_top, 0, _WORKER)
    stuck_child = _seed_parent_and_child(connection, 'recovery.case2_3_parent')
    return _Seed(finished_child=finished_child, stuck_top=stuck_top, stuck_child=stuck_child)


def _finish_seeded_nodes(database: DualRunDatabase, seed: _Seed) -> None:
    """The rest of the seed state, written after the history readers are
    published (a finish moves its task to history): each started node
    finishes through the SQL functions, and the two stuck workflows are set
    RUNNING again."""
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            for workflow in (seed.finished_child, seed.stuck_top, seed.stuck_child):
                _ = finish_node_task(connection, workflow, 0, _WORKER, _RESULT)
            for workflow in (seed.stuck_top, seed.stuck_child):
                _one_row(connection, _REOPEN_WORKFLOW_SQL, {'wf': workflow})


def _attach(broker: PostgresBroker) -> None:
    broker.app = APP
    APP._broker = broker  # pyright: ignore[reportPrivateUsage]


def _health_without_durations(health: dict[str, object]) -> dict[str, object]:
    """The health of a pass with each case's duration left out: the one field
    that depends on the clock."""
    cases = cast(dict[str, dict[str, object]], health['cases'])
    return {
        **health,
        'cases': {
            name: {key: value for key, value in metrics.items() if key != 'duration_ms'}
            for name, metrics in cases.items()
        },
    }


async def _reference_async(database: DualRunDatabase) -> dict[str, object]:
    async with open_broker(database) as broker:
        _attach(broker)
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        report = await reference.recover_stuck_workflows_global(
            resources.session_factory, broker,
        )
        return _health_without_durations(report.health())


async def _candidate_async(database: DualRunDatabase) -> dict[str, object]:
    async with open_broker(database) as broker:
        _attach(broker)
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        report = await recover_stuck_workflows_global(resources.session_factory, broker)
        return _health_without_durations(report.health())


def _reference(database: DualRunDatabase, seed: _Seed) -> dict[str, object]:
    _finish_seeded_nodes(database, seed)
    return asyncio.run(_reference_async(database))


def _candidate(database: DualRunDatabase, seed: _Seed) -> dict[str, object]:
    _finish_seeded_nodes(database, seed)
    return asyncio.run(_candidate_async(database))


def test_the_recovery_pass_writes_what_the_session_engine_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session engine', act=_reference),
            OperationSide(name='0005 recovery functions', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == run.candidate.report
    assert run.candidate.report['recovered'] == 6
    assert run.candidate.report['errors'] == 0


@dataclass(frozen=True, slots=True)
class _OrphanSeed:
    orphan: uuid.UUID


def _orphan_seed(connection: Connection) -> _OrphanSeed:
    orphan = uuid.uuid4()
    _one_row(connection, _ORPHAN_SQL, {'wf': orphan})
    return _OrphanSeed(orphan=orphan)


def _orphan_reference(database: DualRunDatabase, _seed: _OrphanSeed) -> dict[str, object]:
    cover_history(database)
    return asyncio.run(_reference_async(database))


def _orphan_candidate(database: DualRunDatabase, _seed: _OrphanSeed) -> dict[str, object]:
    cover_history(database)
    return asyncio.run(_candidate_async(database))


def test_an_orphan_fails_with_the_catalog_code_in_place_of_e400(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _orphan_seed,
            OperationSide(name='session engine', act=_orphan_reference),
            OperationSide(name='0005 recovery functions', act=_orphan_candidate),
        )
    )

    match run.diff.entries:
        case (ColumnDifference(table='horsies_workflows', column='error') as difference,):
            assert 'E400' in str(difference.reference), render_diff(run.diff)
            assert 'WORKFLOW_START_INCOMPLETE' in str(difference.candidate), render_diff(run.diff)
        case _:
            raise AssertionError(render_diff(run.diff))
    assert run.reference.report == run.candidate.report
    assert run.candidate.report['recovered'] == 1
