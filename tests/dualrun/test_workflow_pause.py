"""Dual-run of the pause switch: the session statements against ``horsies_workflow_pause``.

Reference: ``tests/dualrun/reference/workflow_pause.py``. Candidate:
``pause_workflow`` (``core/workflows/lifecycle.py``). The seed holds a
RUNNING tree of three levels with a PENDING, a CLAIMED and a RUNNING task
in each level, a PAUSED workflow and a RUNNING bystander with a claimed
task. Each side pauses the tree, the PAUSED workflow and an unknown id.

The caller pair runs the same seed with the Python caller before the switch
(``tests/dualrun/reference/workflow_pause_caller.py``) against the caller on
the core (``CoreHandle.pause_workflow_async``).
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
from horsies.core.models.workflow.handle_types import HandleOperationError, HandleWorkflowNotFound
from horsies.core.types.result import Err, Ok, Result
from horsies.core.workflows.lifecycle import pause_workflow
from tests.dualrun.capture import Connection
from tests.dualrun.clients import install_as_a_client, open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_pause as reference
from tests.dualrun.reference import workflow_pause_caller as caller_reference
from tests.dualrun.workflow_seeds import (
    ChildOf,
    OnError,
    TopLevel,
    WorkflowSeed,
    claim_node_task,
    cover_history,
    pause_workflow_row,
    run_node_task,
    run_or_fail,
    start_tree,
    subworkflow_node,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER = 'worker-dualrun-pause'


@dataclass(frozen=True, slots=True)
class _Seed:
    tree: uuid.UUID
    paused: uuid.UUID
    unknown: uuid.UUID


def _seed(connection: Connection) -> _Seed:
    """A RUNNING tree (root, child, grandchild), a PAUSED workflow, a RUNNING bystander."""
    root, child, grandchild = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=root,
                name='pause.root',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, 'pause.root.pending'),
                    task_node(1, 'pause.root.claimed'),
                    task_node(2, 'pause.root.running'),
                    task_node(3, 'pause.root.waiting', 0),
                    subworkflow_node(4, 'pause.child'),
                ),
            ),
            WorkflowSeed(
                workflow_id=child,
                name='pause.child',
                on_error=OnError.FAIL,
                parent=ChildOf(parent=root, parent_task_index=4, depth=1),
                nodes=(
                    task_node(0, 'pause.child.claimed'),
                    task_node(1, 'pause.child.waiting', 0),
                    subworkflow_node(2, 'pause.grandchild'),
                ),
            ),
            WorkflowSeed(
                workflow_id=grandchild,
                name='pause.grandchild',
                on_error=OnError.PAUSE,
                parent=ChildOf(parent=child, parent_task_index=2, depth=2),
                nodes=(
                    task_node(0, 'pause.grandchild.claimed'),
                    task_node(1, 'pause.grandchild.running'),
                ),
            ),
        ],
    )
    _ = claim_node_task(connection, root, 1, _WORKER)
    _ = run_node_task(connection, root, 2, _WORKER)
    _ = claim_node_task(connection, child, 0, _WORKER)
    _ = claim_node_task(connection, grandchild, 0, _WORKER)
    _ = run_node_task(connection, grandchild, 1, _WORKER)

    paused = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=paused,
                name='pause.already_paused',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'pause.already_paused.claimed'),),
            ),
        ],
    )
    _ = claim_node_task(connection, paused, 0, _WORKER)
    pause_workflow_row(connection, paused)

    bystander = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=bystander,
                name='pause.bystander',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'pause.bystander.claimed'),),
            ),
        ],
    )
    _ = claim_node_task(connection, bystander, 0, _WORKER)
    return _Seed(tree=root, paused=paused, unknown=uuid.uuid4())


def _targets(seed: _Seed) -> tuple[uuid.UUID, ...]:
    return (seed.tree, seed.paused, seed.unknown)


def _reference(database: DualRunDatabase, seed: _Seed) -> tuple[reference.PauseReport, ...]:
    cover_history(database)
    install_as_a_client(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        return tuple(reference.pause(connection, target) for target in _targets(seed))


def _report(result: Result[bool, HandleOperationError]) -> reference.PauseReport:
    match result:
        case Ok(paused):
            match paused:
                case True:
                    return reference.Paused()
                case False:
                    return reference.NotRunning()
        case Err(HandleWorkflowNotFound()):
            return reference.NotFound()
        case Err(other):
            raise AssertionError(f'the pause answered {other!r}')
        case _ as unreachable:
            assert_never(unreachable)


async def _pause_all(broker: PostgresBroker, seed: _Seed) -> tuple[reference.PauseReport, ...]:
    return tuple([
        _report(await pause_workflow(broker, str(target))) for target in _targets(seed)
    ])


async def _candidate_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.PauseReport, ...]:
    async with open_broker(database) as broker:
        return await _pause_all(broker, seed)


def _candidate(database: DualRunDatabase, seed: _Seed) -> tuple[reference.PauseReport, ...]:
    cover_history(database)
    return asyncio.run(_candidate_async(database, seed))


def test_pause_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_pause', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    expected = (reference.Paused(), reference.NotRunning(), reference.NotFound())
    assert run.reference.report == expected
    assert run.candidate.report == expected


async def _caller_reference_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.PauseReport, ...]:
    async with open_broker(database) as broker:
        return tuple([
            _report(await caller_reference.pause_workflow(broker, str(target)))
            for target in _targets(seed)
        ])


def _caller_reference(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.PauseReport, ...]:
    cover_history(database)
    return asyncio.run(_caller_reference_async(database, seed))


def test_the_pause_on_the_core_writes_what_the_python_caller_wrote(
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
    expected = (reference.Paused(), reference.NotRunning(), reference.NotFound())
    assert run.reference.report == expected
    assert run.candidate.report == expected
