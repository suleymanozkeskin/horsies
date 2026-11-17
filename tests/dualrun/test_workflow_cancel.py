"""Dual-run of the cancel switch: the session statements against ``horsies_workflow_cancel``.

Reference: ``tests/dualrun/reference/workflow_cancel.py``. Candidate:
``WorkflowHandle.cancel_async`` (``core/models/workflow/handle.py``). The
seed holds a RUNNING tree of three levels (the grandchild PAUSED) with
PENDING, CLAIMED and RUNNING tasks, a COMPLETED workflow and a RUNNING
bystander with a claimed task. Each side cancels the tree, the COMPLETED
workflow and an unknown id.

The caller pair runs the same seed with the Python caller before the switch
(``tests/dualrun/reference/workflow_cancel_caller.py``) against the caller on
the core (``CoreHandle.cancel_workflow_async``).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import assert_never

import psycopg
import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import HandleOperationError, HandleWorkflowNotFound
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_cancel as reference
from tests.dualrun.reference import workflow_cancel_caller as caller_reference
from tests.dualrun.workflow_seeds import (
    ChildOf,
    OnError,
    TopLevel,
    WorkflowSeed,
    claim_node_task,
    cover_history,
    end_workflow,
    pause_workflow_row,
    run_node_task,
    run_or_fail,
    start_tree,
    subworkflow_node,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER = 'worker-dualrun-cancel'


@dataclass(frozen=True, slots=True)
class _Seed:
    tree: uuid.UUID
    completed: uuid.UUID
    unknown: uuid.UUID


def _seed(connection: Connection) -> _Seed:
    """A RUNNING tree with a PAUSED grandchild, a COMPLETED workflow, a RUNNING bystander."""
    root, child, grandchild = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=root,
                name='cancel.root',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, 'cancel.root.pending'),
                    task_node(1, 'cancel.root.claimed'),
                    task_node(2, 'cancel.root.running'),
                    task_node(3, 'cancel.root.waiting', 0),
                    subworkflow_node(4, 'cancel.child'),
                ),
            ),
            WorkflowSeed(
                workflow_id=child,
                name='cancel.child',
                on_error=OnError.FAIL,
                parent=ChildOf(parent=root, parent_task_index=4, depth=1),
                nodes=(
                    task_node(0, 'cancel.child.claimed'),
                    task_node(1, 'cancel.child.waiting', 0),
                    subworkflow_node(2, 'cancel.grandchild'),
                ),
            ),
            WorkflowSeed(
                workflow_id=grandchild,
                name='cancel.grandchild',
                on_error=OnError.PAUSE,
                parent=ChildOf(parent=child, parent_task_index=2, depth=2),
                nodes=(
                    task_node(0, 'cancel.grandchild.pending'),
                    task_node(1, 'cancel.grandchild.waiting', 0),
                ),
            ),
        ],
    )
    _ = claim_node_task(connection, root, 1, _WORKER)
    _ = run_node_task(connection, root, 2, _WORKER)
    _ = claim_node_task(connection, child, 0, _WORKER)
    pause_workflow_row(connection, grandchild)

    completed = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=completed,
                name='cancel.completed',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'cancel.completed.pending'),),
            ),
        ],
    )
    end_workflow(connection, completed, 'COMPLETED')

    bystander = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=bystander,
                name='cancel.bystander',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'cancel.bystander.claimed'),),
            ),
        ],
    )
    _ = claim_node_task(connection, bystander, 0, _WORKER)
    return _Seed(tree=root, completed=completed, unknown=uuid.uuid4())


def _targets(seed: _Seed) -> tuple[uuid.UUID, ...]:
    return (seed.tree, seed.completed, seed.unknown)


def _reference(database: DualRunDatabase, seed: _Seed) -> tuple[reference.CancelReport, ...]:
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        return tuple(reference.cancel(connection, target) for target in _targets(seed))


def _report(result: Result[None, HandleOperationError]) -> reference.CancelReport:
    match result:
        case Ok():
            return reference.Cancelled()
        case Err(HandleWorkflowNotFound()):
            return reference.NotFound()
        case Err(other):
            raise AssertionError(f'the cancel answered {other!r}')
        case _ as unreachable:
            assert_never(unreachable)


async def _cancel_all(
    broker: PostgresBroker, seed: _Seed,
) -> tuple[reference.CancelReport, ...]:
    reports: list[reference.CancelReport] = []
    for target in _targets(seed):
        handle: WorkflowHandle[object] = WorkflowHandle(workflow_id=str(target), broker=broker)
        reports.append(_report(await handle.cancel_async()))
    return tuple(reports)


async def _candidate_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.CancelReport, ...]:
    async with open_broker(database) as broker:
        return await _cancel_all(broker, seed)


def _candidate(database: DualRunDatabase, seed: _Seed) -> tuple[reference.CancelReport, ...]:
    cover_history(database)
    return asyncio.run(_candidate_async(database, seed))


def test_cancel_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_cancel', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    expected = (reference.Cancelled(), reference.Cancelled(), reference.NotFound())
    assert run.reference.report == expected
    assert run.candidate.report == expected


async def _caller_reference_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.CancelReport, ...]:
    async with open_broker(database) as broker:
        reports: list[reference.CancelReport] = []
        for target in _targets(seed):
            handle: WorkflowHandle[object] = WorkflowHandle(workflow_id=str(target), broker=broker)
            reports.append(_report(await caller_reference.cancel_async(handle)))
        return tuple(reports)


def _caller_reference(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.CancelReport, ...]:
    cover_history(database)
    return asyncio.run(_caller_reference_async(database, seed))


def test_the_cancel_on_the_core_writes_what_the_python_caller_wrote(
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
    expected = (reference.Cancelled(), reference.Cancelled(), reference.NotFound())
    assert run.reference.report == expected
    assert run.candidate.report == expected
