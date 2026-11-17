"""Dual-run of the handle reads switch: the session reads against the core reads.

Reference: ``tests/dualrun/reference/workflow_handle_reads.py`` (the bodies of
``WorkflowHandle.status_async``, ``get_async``, ``result_for_async``,
``results_async`` and ``tasks_async`` before the switch). Candidate: the
methods on the core (``CoreHandle.workflow_status_async``,
``wait_workflow_result_async``, ``workflow_node_result_async``,
``workflow_node_results_async``, ``workflow_nodes_async``).

The seed holds a COMPLETED workflow (its one node finished with a result), a
FAILED workflow with a stored error, a PAUSED workflow, a RUNNING workflow and
an unknown id. Each side runs every read on every target, with no wait
(``get(timeout_ms=0)``). The reads write nothing, so the snapshots are equal;
the reports, with each workflow id replaced by its role, must be equal.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, LiteralString

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow.handle import WorkflowHandle, WorkflowTaskInfo
from horsies.core.models.workflow.handle_types import HandleResult
from horsies.core.models.workflow.nodes import NodeKey
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_handle_reads as reference
from tests.dualrun.workflow_seeds import (
    OnError,
    TopLevel,
    WorkflowSeed,
    cover_history,
    end_workflow,
    finish_node_task,
    pause_workflow_row,
    run_node_task,
    run_or_fail,
    start_tree,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WORKER: Final = 'worker-dualrun-handle-reads'
_TASK: Final = 'dualrun.handle_reads.value'
_RESULT: Final = '{"__h_task_result__":true,"ok":5,"err":null}'
_STORED_ERROR: Final = '{"error_code":"TASK_EXCEPTION","message":"node failed"}'

_STORE_ERROR_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows SET error = %(error)s WHERE id = %(wf)s
"""

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://dualrun@127.0.0.1:1/unused'),
        ),
    ),
)
"""The app of both sides' broker: the reads decode a node result with the ok
type of its task, which only a registered task gives."""


@APP.task(task_name=_TASK)
def value_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok=5)


@dataclass(frozen=True, slots=True)
class _Seed:
    completed: uuid.UUID
    failed: uuid.UUID
    paused: uuid.UUID
    running: uuid.UUID
    unknown: uuid.UUID

    def roles(self) -> dict[str, str]:
        return {
            str(self.completed): '<completed>',
            str(self.failed): '<failed>',
            str(self.paused): '<paused>',
            str(self.running): '<running>',
            str(self.unknown): '<unknown>',
        }


def _top(workflow_id: uuid.UUID, name: str) -> WorkflowSeed:
    return WorkflowSeed(
        workflow_id=workflow_id,
        name=name,
        on_error=OnError.FAIL,
        parent=TopLevel(),
        nodes=(task_node(0, _TASK),),
    )


def _seed(connection: Connection) -> _Seed:
    seed = _Seed(
        completed=uuid.uuid4(),
        failed=uuid.uuid4(),
        paused=uuid.uuid4(),
        running=uuid.uuid4(),
        unknown=uuid.uuid4(),
    )
    start_tree(connection, [_top(seed.completed, 'reads.completed')])
    _ = run_node_task(connection, seed.completed, 0, _WORKER)
    start_tree(connection, [_top(seed.failed, 'reads.failed')])
    end_workflow(connection, seed.failed, 'FAILED')
    cursor = connection.execute(_STORE_ERROR_SQL, {'wf': seed.failed, 'error': _STORED_ERROR})
    assert cursor.rowcount == 1
    start_tree(connection, [_top(seed.paused, 'reads.paused')])
    pause_workflow_row(connection, seed.paused)
    start_tree(connection, [_top(seed.running, 'reads.running')])
    return seed


def _finish_completed(database: DualRunDatabase, seed: _Seed) -> None:
    """The finish of the completed workflow's node, after the history readers are published."""
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            _ = finish_node_task(connection, seed.completed, 0, _WORKER, _RESULT)


def _id_free(text: str | None, roles: dict[str, str]) -> str | None:
    match text:
        case None:
            return None
        case value:
            for workflow_id, role in roles.items():
                value = value.replace(workflow_id, role)
            return value


def _task_result(result: TaskResult[Any, TaskError] | None, roles: dict[str, str]) -> object:
    match result:
        case None:
            return None
        case _ if result.err is not None:
            return ('err', str(result.err.error_code), _id_free(result.err.message, roles))
        case _:
            return ('ok', result.ok)


def _handle_result(result: HandleResult[Any], roles: dict[str, str]) -> object:
    match result:
        case Ok(value):
            return ('ok', value)
        case Err(error):
            return ('err', str(error.code), _id_free(error.message, roles))


def _task_info(info: WorkflowTaskInfo, roles: dict[str, str]) -> object:
    return (
        info.node_id,
        info.index,
        info.name,
        info.status,
        _task_result(info.result, roles),
        info.sub_workflow_id is None,
        info.sub_workflow_summary,
    )


@dataclass(frozen=True, slots=True)
class _Reads:
    """The id-free answers of the five reads on one workflow."""

    status: object
    get: object
    result_for: object
    results: object
    tasks: object


class _ReadFunctions:
    """The five reads of one side."""

    def __init__(self, reads: dict[str, Callable[..., Any]]) -> None:
        self.reads = reads

    async def run(
        self, handle: WorkflowHandle[Any], node: NodeKey[Any], roles: dict[str, str],
    ) -> _Reads:
        tasks: HandleResult[list[WorkflowTaskInfo]] = await self.reads['tasks'](handle)
        match tasks:
            case Ok(infos):
                tasks_answer: object = ('ok', [_task_info(info, roles) for info in infos])
            case Err():
                tasks_answer = _handle_result(tasks, roles)
        return _Reads(
            status=_handle_result(await self.reads['status'](handle), roles),
            get=_task_result(await self.reads['get'](handle, timeout_ms=0), roles),
            result_for=_task_result(await self.reads['result_for'](handle, node), roles),
            results=_handle_result(await self.reads['results'](handle), roles),
            tasks=tasks_answer,
        )


def _reference_reads() -> _ReadFunctions:
    return _ReadFunctions({
        'status': reference.status_async,
        'get': reference.get_async,
        'result_for': reference.result_for_async,
        'results': reference.results_async,
        'tasks': reference.tasks_async,
    })


def _core_reads() -> _ReadFunctions:
    handle_class: Any = WorkflowHandle
    return _ReadFunctions({
        'status': handle_class.status_async,
        'get': handle_class.get_async,
        'result_for': handle_class.result_for_async,
        'results': handle_class.results_async,
        'tasks': handle_class.tasks_async,
    })


async def _read_all(
    database: DualRunDatabase, seed: _Seed, reads: _ReadFunctions,
) -> dict[str, _Reads]:
    roles = seed.roles()
    targets = {
        'completed': (seed.completed, 'reads.completed'),
        'failed': (seed.failed, 'reads.failed'),
        'paused': (seed.paused, 'reads.paused'),
        'running': (seed.running, 'reads.running'),
        'unknown': (seed.unknown, 'reads.unknown'),
    }
    async with open_broker(database) as broker:
        broker.app = APP
        answers: dict[str, _Reads] = {}
        for role, (workflow_id, name) in targets.items():
            handle: WorkflowHandle[Any] = WorkflowHandle(
                workflow_id=str(workflow_id), broker=broker, out_type=int,
            )
            answers[role] = await reads.run(handle, NodeKey(f'{name}.n0'), roles)
        return answers


def _reference(database: DualRunDatabase, seed: _Seed) -> dict[str, _Reads]:
    _finish_completed(database, seed)
    return asyncio.run(_read_all(database, seed, _reference_reads()))


def _candidate(database: DualRunDatabase, seed: _Seed) -> dict[str, _Reads]:
    _finish_completed(database, seed)
    return asyncio.run(_read_all(database, seed, _core_reads()))


def test_the_core_handle_reads_answer_what_the_session_reads_answered(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session reads', act=_reference),
            OperationSide(name='core reads', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == run.candidate.report
    completed = run.candidate.report['completed']
    assert completed.get == ('ok', 5)
    assert completed.result_for == ('ok', 5)
    unknown_status = run.candidate.report['unknown'].status
    assert isinstance(unknown_status, tuple) and unknown_status[0] == 'err'
