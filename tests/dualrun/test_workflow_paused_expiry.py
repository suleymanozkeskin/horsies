"""Dual-run of the paused expiry redesign: the session statements against ``horsies_workflow_expire_paused``.

Reference: ``tests/dualrun/reference/workflow_paused_expiry.py``. Candidate:
``expire_paused_workflows`` (``core/workflows/lifecycle.py``). The seed holds
a PAUSED tree paused two hours ago (a PENDING, a CLAIMED and a RUNNING task
and a waiting node in the root; a CLAIMED task in the child), a workflow
PAUSED ten minutes ago, and a RUNNING bystander with a claimed task. Each
side expires the workflows PAUSED for more than one hour.

The caller pair runs the same seed with the reaper's call before the switch
(``tests/dualrun/reference/paused_expiry_caller.py``) against the reaper on
the core (``CoreHandle.expire_paused_workflows_async``).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, LiteralString

import psycopg
import pytest

from horsies.core.types.result import Err, Ok
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.workflows.lifecycle import expire_paused_workflows
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import paused_expiry_caller as caller_reference
from tests.dualrun.reference import workflow_paused_expiry as reference
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

_WORKER: Final = 'worker-dualrun-expiry'
_OLDER_THAN: Final = timedelta(hours=1)
_BATCH_SIZE: Final = 50

_AGE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows SET updated_at = now() - %(age)s WHERE id = %(wf)s
"""


@dataclass(frozen=True, slots=True)
class _Seed:
    root: uuid.UUID
    child: uuid.UUID
    young: uuid.UUID


def _age(connection: Connection, workflow_id: uuid.UUID, age: timedelta) -> None:
    cursor = connection.execute(_AGE_SQL, {'wf': workflow_id, 'age': age})
    assert cursor.rowcount == 1, f'workflow {workflow_id} is absent'


def _seed(connection: Connection) -> _Seed:
    """An old PAUSED tree, a young PAUSED workflow, a RUNNING bystander."""
    root, child = uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=root,
                name='expiry.root',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, 'expiry.root.pending'),
                    task_node(1, 'expiry.root.claimed'),
                    task_node(2, 'expiry.root.running'),
                    task_node(3, 'expiry.root.waiting', 0),
                    subworkflow_node(4, 'expiry.child'),
                ),
            ),
            WorkflowSeed(
                workflow_id=child,
                name='expiry.child',
                on_error=OnError.FAIL,
                parent=ChildOf(parent=root, parent_task_index=4, depth=1),
                nodes=(
                    task_node(0, 'expiry.child.claimed'),
                    task_node(1, 'expiry.child.waiting', 0),
                ),
            ),
        ],
    )
    _ = claim_node_task(connection, root, 1, _WORKER)
    _ = run_node_task(connection, root, 2, _WORKER)
    _ = claim_node_task(connection, child, 0, _WORKER)
    pause_workflow_row(connection, root)
    pause_workflow_row(connection, child)
    _age(connection, root, timedelta(hours=2))
    _age(connection, child, timedelta(hours=2))

    young = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=young,
                name='expiry.young',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'expiry.young.pending'),),
            ),
        ],
    )
    pause_workflow_row(connection, young)
    _age(connection, young, timedelta(minutes=10))

    bystander = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=bystander,
                name='expiry.bystander',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'expiry.bystander.claimed'),),
            ),
        ],
    )
    _ = claim_node_task(connection, bystander, 0, _WORKER)
    _age(connection, bystander, timedelta(hours=2))
    return _Seed(root=root, child=child, young=young)


def _reference(database: DualRunDatabase, _seed_facts: _Seed) -> int:
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        return reference.expire_paused(
            connection,
            age_us=_OLDER_THAN // timedelta(microseconds=1),
            batch_size=_BATCH_SIZE,
            error_json=reference.expiry_error_json(_OLDER_THAN),
        )


async def _candidate_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        return await expire_paused_workflows(
            resources.session_factory,
            older_than=_OLDER_THAN,
            batch_size=_BATCH_SIZE,
        )


def _candidate(database: DualRunDatabase, _seed_facts: _Seed) -> int:
    cover_history(database)
    return asyncio.run(_candidate_async(database))


def test_paused_expiry_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_expire_paused', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == 2
    assert run.candidate.report == 2


class _Reaper(ReaperMixin):
    """The reaper mixin alone: the paused expiry reads no worker state."""


async def _caller_reference_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        return await caller_reference.expire_paused_workflows(
            _Reaper(), broker, older_than=_OLDER_THAN,
        )


def _caller_reference(database: DualRunDatabase, _seed_facts: _Seed) -> int:
    cover_history(database)
    return asyncio.run(_caller_reference_async(database))


async def _core_caller_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        return await _Reaper()._expire_paused_workflows(  # pyright: ignore[reportPrivateUsage]
            broker, older_than=_OLDER_THAN,
        )


def _core_caller(database: DualRunDatabase, _seed_facts: _Seed) -> int:
    cover_history(database)
    return asyncio.run(_core_caller_async(database))


def test_the_core_paused_expiry_writes_what_the_python_reaper_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='python reaper', act=_caller_reference),
            OperationSide(name='core reaper', act=_core_caller),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == 2
    assert run.candidate.report == 2
