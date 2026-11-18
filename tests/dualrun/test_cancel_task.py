"""Dual-runs of the monitoring cancel.

Two pairs, with the same seed (one task per case, live and finished):

1. The SQL redesign: the locked-row cancel against ``horsies_cancel_task``.
   Reference: ``tests/dualrun/reference/cancel.py``. Candidate:
   ``horsies.monitoring.task_actions.cancel_task``. Both sides must cancel
   the same rows, and the candidate's answer must be the answer the old
   diagnosis gave.
2. The caller switch: the Python caller of the function against the core.
   Reference: ``tests/dualrun/reference/cancel_caller.py``, the function's
   body before the switch (one statement on the broker's SQLAlchemy engine,
   decoded in Python). Candidate: ``cancel_task`` as shipped, which calls
   ``CoreHandle.cancel_task_async``. The snapshots and the answers must be
   equal.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Final, LiteralString

from horsies.core.brokers.postgres import PostgresBroker

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.monitoring.task_actions import (
    TaskActionError,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionWorkflowTask,
    TaskCancelled,
    cancel_task,
)
from tests.dualrun.capture import Connection
from tests.dualrun.clients import install_as_a_client, open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import cancel as reference
from tests.dualrun.reference import cancel_caller
from tests.dualrun.reference.interim_terminal_writes import (
    install_interim_terminal_writes,
)
from tests.dualrun.seeds import (
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    insert_task,
    insert_workflow_task,
)
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_AGO: Final = timedelta(minutes=1)

_MOVE_SQL: Final[LiteralString] = """
    SELECT horsies_move_task_to_history(
        (SELECT t FROM horsies_tasks AS t WHERE t.id = CAST(%(task_id)s AS uuid) FOR UPDATE),
        %(status)s, %(kind)s, now(), %(result)s, %(error_code)s, %(reason)s
    )
"""


@dataclass(frozen=True, slots=True)
class _Case:
    name: str
    task_id: str
    include_running: bool


def _move(
    connection: Connection,
    task_id: uuid.UUID,
    status: str,
    kind: str,
    result: str | None,
    error_code: str | None,
) -> None:
    _ = connection.execute(
        _MOVE_SQL,
        {
            'task_id': task_id,
            'status': status,
            'kind': kind,
            'result': result,
            'error_code': error_code,
            'reason': None,
        },
    )


def _seed(connection: Connection) -> tuple[_Case, ...]:
    worker_id = f'worker-{uuid.uuid4().hex}'

    def running(name: str) -> TaskSeed:
        return TaskSeed(
            task_name=name,
            status=SeedStatus.RUNNING,
            worker_id=worker_id,
            claimed_ago=_AGO,
            started_ago=_AGO,
        )

    pending = insert_task(connection, TaskSeed(task_name='cancel.pending', status=SeedStatus.PENDING))
    claimed = insert_task(
        connection,
        TaskSeed(
            task_name='cancel.claimed',
            status=SeedStatus.CLAIMED,
            worker_id=worker_id,
            claimed_ago=_AGO,
        ),
    )
    running_refused = insert_task(connection, running('cancel.running_refused'))
    running_included = insert_task(connection, running('cancel.running_included'))
    workflow_live = insert_workflow_task(
        connection,
        TaskSeed(task_name='cancel.workflow_live', status=SeedStatus.PENDING),
        WorkflowLink(workflow_status='RUNNING', node_status='ENQUEUED'),
    )
    completed = insert_task(connection, running('cancel.completed'))
    _move(connection, completed.task_id, 'COMPLETED', 'COMPLETE_FUSED', '{"ok":1}', None)
    cancelled = insert_task(
        connection, TaskSeed(task_name='cancel.cancelled_before', status=SeedStatus.PENDING)
    )
    _move(connection, cancelled.task_id, 'CANCELLED', 'CANCEL_ADMIN', None, 'TASK_CANCELLED')
    workflow_failed = insert_workflow_task(
        connection,
        running('cancel.workflow_failed'),
        WorkflowLink(workflow_status='RUNNING', node_status='RUNNING'),
    )
    _move(connection, workflow_failed.task_id, 'FAILED', 'FAIL_RUNNING', '{"err":1}', 'E1')
    return (
        _Case('pending', str(pending.task_id), False),
        _Case('claimed', str(claimed.task_id), False),
        _Case('running_refused', str(running_refused.task_id), False),
        _Case('running_included', str(running_included.task_id), True),
        _Case('workflow_live', str(workflow_live.task_id), False),
        _Case('completed', str(completed.task_id), False),
        _Case('cancelled_before', str(cancelled.task_id), False),
        _Case('workflow_failed', str(workflow_failed.task_id), False),
        _Case('absent', str(uuid.uuid4()), False),
    )


def _reference_cancel(
    database: DualRunDatabase, cases: tuple[_Case, ...]
) -> dict[str, reference.LiveMiss | reference.CancelCalled]:
    install_as_a_client(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        install_interim_terminal_writes(connection)
        return {
            case.name: reference.cancel(connection, case.task_id, case.include_running)
            for case in cases
        }


type _Answer = (
    TaskCancelled
    | TaskActionNotFound
    | TaskActionWorkflowTask
    | TaskActionNotCancellable
)


async def _candidate_cancel_async(
    database: DualRunDatabase, cases: tuple[_Case, ...]
) -> dict[str, _Answer]:
    answers: dict[str, _Answer] = {}
    async with open_broker(database) as broker:
        for case in cases:
            match await cancel_task(
                broker, uuid.UUID(case.task_id), include_running=case.include_running,
            ):
                case Ok(cancelled):
                    answers[case.name] = cancelled
                case Err(TaskActionNotFound() | TaskActionWorkflowTask() | TaskActionNotCancellable() as refused):
                    answers[case.name] = refused
                case Err(failed):
                    raise AssertionError(f'cancel of {case.name} failed: {failed}')
    return answers


def _candidate_cancel(
    database: DualRunDatabase, cases: tuple[_Case, ...]
) -> dict[str, _Answer]:
    return asyncio.run(_candidate_cancel_async(database, cases))


def _ok(
    result: Result[
        OperationDualRun[dict[str, reference.LiveMiss | reference.CancelCalled], dict[str, _Answer]],
        SelfCheckStepFailed,
    ],
) -> OperationDualRun[dict[str, reference.LiveMiss | reference.CancelCalled], dict[str, _Answer]]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def _answer_kind(answer: _Answer) -> tuple[str, TaskStatus | None]:
    match answer:
        case TaskCancelled(was_status=status):
            return ('cancelled', status)
        case TaskActionNotCancellable(current_status=status):
            return ('not cancellable', status)
        case TaskActionWorkflowTask(current_status=status):
            return ('workflow task', status)
        case TaskActionNotFound():
            return ('not found', None)


def test_cancel_writes_and_answers_what_the_locked_row_cancel_did(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='locked-row cancel', act=_reference_cancel),
            OperationSide(name='horsies_cancel_task', act=_candidate_cancel),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {
        'pending': reference.CancelCalled(False, 'APPLIED', 'PENDING'),
        'claimed': reference.CancelCalled(False, 'APPLIED', 'CLAIMED'),
        'running_refused': reference.CancelCalled(False, 'SOURCE_STATE_CONFLICT', 'RUNNING'),
        'running_included': reference.CancelCalled(False, 'APPLIED', 'RUNNING'),
        'workflow_live': reference.CancelCalled(True, 'SOURCE_STATE_CONFLICT', 'PENDING'),
        'completed': reference.LiveMiss(),
        'cancelled_before': reference.LiveMiss(),
        'workflow_failed': reference.LiveMiss(),
        'absent': reference.LiveMiss(),
    }
    assert {name: _answer_kind(answer) for name, answer in run.candidate.report.items()} == {
        'pending': ('cancelled', TaskStatus.PENDING),
        'claimed': ('cancelled', TaskStatus.CLAIMED),
        'running_refused': ('not cancellable', TaskStatus.RUNNING),
        'running_included': ('cancelled', TaskStatus.RUNNING),
        'workflow_live': ('workflow task', TaskStatus.PENDING),
        'completed': ('not cancellable', TaskStatus.COMPLETED),
        'cancelled_before': ('not cancellable', TaskStatus.CANCELLED),
        'workflow_failed': ('workflow task', TaskStatus.FAILED),
        'absent': ('not found', None),
    }


# ---- The caller switch: the Python caller against the core ----

type _CancelCall = Callable[
    [PostgresBroker, uuid.UUID, bool],
    Awaitable[Result[TaskCancelled, TaskActionError]],
]


async def _cancel_each_async(
    database: DualRunDatabase, cases: tuple[_Case, ...], call: _CancelCall
) -> dict[str, Result[TaskCancelled, TaskActionError]]:
    async with open_broker(database) as broker:
        return {
            case.name: await call(broker, uuid.UUID(case.task_id), case.include_running)
            for case in cases
        }


async def _python_caller(
    broker: PostgresBroker, task_id: uuid.UUID, include_running: bool
) -> Result[TaskCancelled, TaskActionError]:
    return await cancel_caller.cancel_task(broker, task_id, include_running=include_running)


async def _core_caller(
    broker: PostgresBroker, task_id: uuid.UUID, include_running: bool
) -> Result[TaskCancelled, TaskActionError]:
    return await cancel_task(broker, task_id, include_running=include_running)


def _caller_side(
    name: str, call: _CancelCall
) -> OperationSide[tuple[_Case, ...], dict[str, Result[TaskCancelled, TaskActionError]]]:
    def act(
        database: DualRunDatabase, cases: tuple[_Case, ...]
    ) -> dict[str, Result[TaskCancelled, TaskActionError]]:
        return asyncio.run(_cancel_each_async(database, cases, call))

    return OperationSide(name=name, act=act)


_SEEDED_ID: Final = uuid.UUID(int=0)
"""Stands in for the task id of a compared answer: each side seeds its own ids."""


def _answer_without_id(
    answer: Result[TaskCancelled, TaskActionError],
) -> Result[TaskCancelled, TaskActionError]:
    """``answer`` with its task id replaced by ``_SEEDED_ID``, also inside an
    error message."""
    match answer:
        case Ok(cancelled):
            return Ok(replace(cancelled, task_id=_SEEDED_ID))
        case Err(error):
            return Err(
                replace(
                    error,
                    task_id=_SEEDED_ID,
                    message=error.message.replace(str(error.task_id), str(_SEEDED_ID)),
                )
            )


def _answers_by_case(
    report: dict[str, Result[TaskCancelled, TaskActionError]],
) -> dict[str, Result[TaskCancelled, TaskActionError]]:
    return {name: _answer_without_id(answer) for name, answer in report.items()}


def test_the_core_cancel_writes_and_answers_what_the_python_caller_did(
    empty_database: MakeDatabase,
) -> None:
    match dual_run(
        empty_database,
        _seed,
        _caller_side('Python caller on SQLAlchemy', _python_caller),
        _caller_side('CoreHandle.cancel_task_async', _core_caller),
        SeedCoverage.HISTORY_AND_BEATS,
    ):
        case Ok(run):
            pass
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')

    assert run.diff.entries == (), render_diff(run.diff)
    assert _answers_by_case(run.candidate.report) == _answers_by_case(run.reference.report)
    assert {
        name: answer.is_ok() for name, answer in run.reference.report.items()
    } == {
        'pending': True,
        'claimed': True,
        'running_refused': False,
        'running_included': True,
        'workflow_live': False,
        'completed': False,
        'cancelled_before': False,
        'workflow_failed': False,
        'absent': False,
    }
