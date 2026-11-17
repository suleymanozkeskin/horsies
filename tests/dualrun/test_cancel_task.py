"""Dual-run of the monitoring cancel switch: the locked-row cancel against ``horsies_cancel_task``.

Reference: ``tests/dualrun/reference/cancel.py``. Candidate:
``horsies.monitoring.task_actions.cancel_task``. The seed holds one task
per case, live and finished; both sides must cancel the same rows, and the
candidate's answer must be the answer the old diagnosis gave.
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

from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.monitoring.task_actions import (
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionWorkflowTask,
    TaskCancelled,
    cancel_task,
)
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import cancel as reference
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
    with psycopg.connect(database.url, autocommit=True) as connection:
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
