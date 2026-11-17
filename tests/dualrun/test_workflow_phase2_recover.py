"""Dual-run of the phase-2 recovery switch: the session statements against
``horsies_workflow_phase2_recover``.

Reference: ``tests/dualrun/reference/workflow_phase2_recover.py``. Candidate:
``drive_phase2_recovery`` (``core/workflows/phase2_recovery.py``). Each side
writes the phase-2 records after the history readers are published, then
runs one pass (bound 3, no grace window):

- a record of a node with a task dependent and a sub-workflow dependent
  (a task row is minted; the child start is owed in the record's
  transaction);
- a record of a node of a workflow that ended after phase 1;
- a record at the attempt bound whose history row is absent (kept, then
  quarantined).

The snapshots compare equal except for one decided difference: the node of
an ended workflow is written (decision on a node of an ended workflow,
"Write the node on both the normal and the recovery path (one body);
nothing is promoted"); the session statements dropped the record and left
the node.

The caller pair runs the same seed with the Python pass that the reaper
called before the switch (``drive_phase2_recovery``) against the pass in the
core (``CoreHandle.recover_workflow_phase2_async``, through
``ReaperMixin._recover_phase2``'s mapping): empty diff, equal reports.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

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
import horsies._core as core
from horsies.core.workflows.phase2_recovery import drive_phase2_recovery
from horsies.core.workflows.registry import register_workflow_definition
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import ColumnDifference, DiffEntry, SnapshotDiff, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_phase2_recover as reference
from tests.dualrun.values import ExactValue, NullValue
from tests.dualrun.workflow_seeds import (
    OnError,
    TopLevel,
    WorkflowSeed,
    cover_history,
    end_workflow,
    finish_phase_one,
    insert_phase2_pending,
    run_node_task,
    run_or_fail,
    start_tree,
    subworkflow_node,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER = 'worker-dualrun-phase2-recover'
_TASK = 'dualrun.recover.value'
_RESULT = '{"__h_task_result__":true,"ok":7,"err":null}'
_BOUND = 3

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
    return TaskResult(ok=7)


class RecoverChild(WorkflowDefinition[int]):
    """The child that the recovered progression starts."""

    name = 'dualrun.recover.child'
    definition_key = 'dualrun.recover.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(fn=value_task)
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The child definition, registered again: other tests clear the registry."""
    register_workflow_definition(RecoverChild)


@dataclass(frozen=True, slots=True)
class _Seed:
    applied: uuid.UUID
    ended: uuid.UUID
    kept: uuid.UUID


def _one_node(workflow_id: uuid.UUID, name: str) -> WorkflowSeed:
    return WorkflowSeed(
        workflow_id=workflow_id,
        name=name,
        on_error=OnError.FAIL,
        parent=TopLevel(),
        nodes=(task_node(0, _TASK), task_node(1, _TASK, 0)),
    )


def _seed(connection: Connection) -> _Seed:
    applied, ended, kept = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=applied,
                name='recover.applied',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, _TASK),
                    task_node(1, _TASK, 0),
                    subworkflow_node(
                        2, RecoverChild.name, 0, definition_key=RecoverChild.definition_key,
                    ),
                ),
            ),
        ],
    )
    start_tree(connection, [_one_node(ended, 'recover.ended')])
    start_tree(connection, [_one_node(kept, 'recover.kept')])
    _ = run_node_task(connection, applied, 0, _WORKER)
    _ = run_node_task(connection, ended, 0, _WORKER)
    return _Seed(applied=applied, ended=ended, kept=kept)


def _records(database: DualRunDatabase, seed: _Seed) -> None:
    """The phase-2 records, written after the history readers are published:
    phase 1 of two running tasks (one workflow then ends), and a record at
    one attempt under the bound whose history row is absent."""
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            _ = finish_phase_one(connection, seed.applied, 0, _WORKER, _RESULT)
            _ = finish_phase_one(connection, seed.ended, 0, _WORKER, _RESULT)
            end_workflow(connection, seed.ended, 'CANCELLED')
            _ = insert_phase2_pending(connection, seed.kept, 0, _BOUND - 1)


def _attach(broker: PostgresBroker) -> None:
    broker.app = APP
    APP._broker = broker  # pyright: ignore[reportPrivateUsage]


async def _reference_async(database: DualRunDatabase) -> reference.RecoverReport:
    async with open_broker(database) as broker:
        _attach(broker)
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        return await reference.recover(
            resources.session_factory,
            broker,
            grace_ms=0,
            max_rows=100,
            quarantine_after_attempts=_BOUND,
        )


def _reference(database: DualRunDatabase, seed: _Seed) -> reference.RecoverReport:
    _records(database, seed)
    return asyncio.run(_reference_async(database))


async def _candidate_async(database: DualRunDatabase) -> reference.RecoverReport:
    async with open_broker(database) as broker:
        _attach(broker)
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        summary = await drive_phase2_recovery(
            resources.session_factory,
            broker,
            grace_ms=0,
            max_rows=100,
            quarantine_after_attempts=_BOUND,
        )
        return reference.RecoverReport(
            applied=summary.applied,
            already_applied=summary.already_applied,
            superseded=summary.superseded,
            retained=summary.retained,
            failed=summary.failed,
            quarantined=summary.quarantined,
        )


def _candidate(database: DualRunDatabase, seed: _Seed) -> reference.RecoverReport:
    _records(database, seed)
    return asyncio.run(_candidate_async(database))


def _ended_workflow_node(entry: DiffEntry) -> bool:
    """The decided difference: the node of the ended workflow is written."""
    match entry:
        case ColumnDifference(table='horsies_workflow_tasks', column='status'):
            return entry.reference == ExactValue(text='RUNNING') and entry.candidate == ExactValue(
                text='COMPLETED',
            )
        case ColumnDifference(table='horsies_workflow_tasks', column='result' | 'completed_at'):
            return isinstance(entry.reference, NullValue)
        case _:
            return False


def test_phase2_recovery_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_phase2_recover', act=_candidate),
        )
    )

    decided = [entry for entry in run.diff.entries if _ended_workflow_node(entry)]
    rest = SnapshotDiff(
        entries=tuple(entry for entry in run.diff.entries if not _ended_workflow_node(entry)),
    )
    assert rest.entries == (), render_diff(rest)
    assert len(decided) == 3, render_diff(run.diff)
    # The kept record's history row is absent, so its quarantine copy is
    # refused: no record is counted as quarantined.
    assert run.reference.report == reference.RecoverReport(
        applied=1, already_applied=0, superseded=1, retained=1, failed=0, quarantined=0,
    )
    assert run.candidate.report == run.reference.report


async def _core_async(database: DualRunDatabase) -> reference.RecoverReport:
    async with open_broker(database) as broker:
        _attach(broker)
        record = await broker._core_handle.recover_workflow_phase2_async(  # pyright: ignore[reportPrivateUsage]
            grace_ms=0, max_rows=100, quarantine_after_attempts=_BOUND,
        )
        assert isinstance(record, core.Phase2RecoveryRecord), record
        return reference.RecoverReport(
            applied=record.applied,
            already_applied=record.already_applied + record.pending_absent,
            superseded=record.superseded,
            retained=record.retained,
            failed=record.failed,
            quarantined=record.quarantined,
        )


def _core(database: DualRunDatabase, seed: _Seed) -> reference.RecoverReport:
    _records(database, seed)
    return asyncio.run(_core_async(database))


def test_phase2_recovery_on_the_core_writes_what_the_python_pass_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='python pass', act=_candidate),
            OperationSide(name='core pass', act=_core),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.candidate.report == run.reference.report
