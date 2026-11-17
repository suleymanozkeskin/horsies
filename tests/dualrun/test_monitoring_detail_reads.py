"""Dual-run of the detail switch: the client statements against the detail and node functions.

Reference: ``tests/dualrun/monitoring_detail_reference.py`` (a REPEATABLE
READ session: live read, staged history read, absence rule, attempt rows,
workflow link). Candidate: one call of ``horsies_monitoring_task_detail``
and of ``horsies_monitoring_workflow_node``, Python's bodies before the
monitoring switch (``tests/dualrun/reference/monitoring_queries.py``).
The seed has live tasks with and without attempts, history tasks with an
attempt snapshot, workflow-bound tasks, absent ids of both kinds, and the
nodes of one run: a task node per location, a sub-workflow node, a node
whose task is in neither place, a node with a stored error and one with a
stored value. Each read must give equal answers.

The caller switch pair of the monitoring switch: on the same seed, Python's
``get_task_detail`` and ``get_workflow_node`` against the production reads,
which the core runs. The reports must be equal; the open span of a live
task may be up to ``COUNTING_SPAN_TOLERANCE_S`` longer on the core, which
reads second.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import get_task_detail, get_workflow_node
from horsies.monitoring.models import TaskDetail, TaskDetailNotFound, WorkflowTaskDetail
from tests.dualrun import monitoring_detail_reference as reference
from tests.dualrun.monitoring_reads import (
    MakeDatabase,
    ReadPair,
    differing,
    differing_reports,
    read_dual_run,
)
from tests.dualrun.reference import monitoring_queries as python_reads
from tests.integration.monitoring_seeding import (
    AttemptRow,
    NodeRow,
    SeedRow,
    SubworkflowNode,
    TaskNode,
    TaskRow,
    WorkflowRow,
    task_row,
)

pytestmark = [pytest.mark.integration]

_NOW: Final = datetime.now(UTC)
_RUN_ID: Final = str(uuid.uuid4())
_NODE_INDEXES: Final = range(8)
"""Indexes 0..6 are seeded nodes; 7 is a node the run does not have."""


def _uuid7_born_at(birth: datetime) -> str:
    milliseconds = int(birth.timestamp() * 1000)
    value = (milliseconds << 80) | int.from_bytes(os.urandom(10), 'big')
    value = (value & ~(0xF << 76)) | (0x7 << 76)
    value = (value & ~(0x3 << 62)) | (0x2 << 62)
    return str(uuid.UUID(int=value))


def _task(
    task_id: str,
    *,
    status: TaskStatus,
    is_workflow_task: bool = False,
    worker_id: str | None = None,
    error_code: str | None = None,
    retry_count: int = 0,
) -> TaskRow:
    terminal = status.is_terminal
    claimed = worker_id is not None
    return task_row(
        task_id=task_id,
        task_name='detail.task',
        queue_name='default',
        status=status,
        priority=100,
        retry_count=retry_count,
        max_retries=3,
        is_workflow_task=is_workflow_task,
        enqueued_at=_NOW - timedelta(seconds=90),
        claimed_at=_NOW - timedelta(seconds=80) if claimed else None,
        started_at=_NOW - timedelta(seconds=70) if status is TaskStatus.RUNNING or terminal and claimed else None,
        completed_at=_NOW - timedelta(seconds=10) if status is TaskStatus.COMPLETED else None,
        failed_at=_NOW - timedelta(seconds=10) if terminal and status is not TaskStatus.COMPLETED else None,
        good_until=_NOW + timedelta(hours=1) if not terminal else None,
        error_code=error_code,
        failed_reason='the worker reported a failure' if error_code is not None else None,
        worker_id=worker_id,
        worker_hostname='host-1' if claimed else None,
        worker_pid=4321 if claimed else None,
        worker_process_name='child-1' if claimed else None,
        claim_expires_at=None,
    )


def _attempt(task_id: str, attempt: int, *, will_retry: bool, message: str | None) -> AttemptRow:
    return AttemptRow(
        task_id=task_id,
        attempt=attempt,
        outcome='FAILED',
        will_retry=will_retry,
        started_at=_NOW - timedelta(seconds=60 - attempt),
        finished_at=_NOW - timedelta(seconds=50 - attempt),
        error_code='TASK_EXCEPTION',
        error_message=message,
        failed_reason='' if message is None else 'raised',
        worker_id='worker-1',
        worker_hostname='host-1',
        worker_pid=4321,
        worker_process_name='child-1',
    )


def _stored(result: TaskResult[int, TaskError]) -> str:
    match dumps_json(encode_task_result(result, int)):
        case Ok(text):
            return text
        case Err(cause):
            raise AssertionError(f'the result does not encode: {cause}')


def _node(
    task_index: int,
    kind: TaskNode | SubworkflowNode,
    *,
    status: str,
    result: str | None = None,
) -> NodeRow:
    return NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=_RUN_ID,
        task_index=task_index,
        node_id=f'step-{task_index}',
        task_name='detail.task',
        status=status,
        dependencies=(),
        allow_failed_deps=False,
        kind=kind,
        result=result,
        created_at=_NOW - timedelta(seconds=100),
        started_at=None,
        completed_at=None,
    )


# The task ids, by what they hold.
_PENDING: Final = _uuid7_born_at(_NOW - timedelta(seconds=90))
_RUNNING: Final = _uuid7_born_at(_NOW - timedelta(seconds=90))
_LIVE_NODE_TASK: Final = _uuid7_born_at(_NOW - timedelta(seconds=90))
_FAILED: Final = _uuid7_born_at(_NOW - timedelta(seconds=90))
_COMPLETED_NODE_TASK: Final = _uuid7_born_at(_NOW - timedelta(seconds=90))
_CANCELLED: Final = _uuid7_born_at(_NOW - timedelta(seconds=90))
_ABSENT_V7: Final = _uuid7_born_at(_NOW - timedelta(days=3))
_ABSENT_V4: Final = str(uuid.uuid4())
_ABSENT_NODE_TASK: Final = _uuid7_born_at(_NOW - timedelta(days=3))
_TASK_IDS: Final = (
    _PENDING,
    _RUNNING,
    _LIVE_NODE_TASK,
    _FAILED,
    _COMPLETED_NODE_TASK,
    _CANCELLED,
    _ABSENT_V7,
    _ABSENT_V4,
)


def _seed() -> tuple[SeedRow, ...]:
    run = WorkflowRow(
        id=_RUN_ID,
        name='detail.flow',
        status='RUNNING',
        on_error='fail',
        definition_key='detail.flow.v1',
        parent=None,
        sent_at=_NOW - timedelta(seconds=100),
        created_at=_NOW - timedelta(seconds=100),
        completed_at=None,
    )
    return (
        run,
        _task(_PENDING, status=TaskStatus.PENDING),
        _task(_RUNNING, status=TaskStatus.RUNNING, worker_id='worker-1', retry_count=1),
        _attempt(_RUNNING, 1, will_retry=True, message='first try'),
        _task(_LIVE_NODE_TASK, status=TaskStatus.CLAIMED, worker_id='worker-1', is_workflow_task=True),
        _task(_FAILED, status=TaskStatus.FAILED, worker_id='worker-1', error_code='TASK_EXCEPTION', retry_count=1),
        _attempt(_FAILED, 1, will_retry=True, message='first try'),
        _attempt(_FAILED, 2, will_retry=False, message=None),
        _task(_COMPLETED_NODE_TASK, status=TaskStatus.COMPLETED, worker_id='worker-1', is_workflow_task=True),
        _task(_CANCELLED, status=TaskStatus.CANCELLED, error_code='TASK_CANCELLED'),
        _node(0, TaskNode(task_id=_LIVE_NODE_TASK), status='ENQUEUED'),
        _node(
            1,
            TaskNode(task_id=_COMPLETED_NODE_TASK),
            status='COMPLETED',
            result=_stored(TaskResult(ok=7)),
        ),
        _node(2, SubworkflowNode(sub_workflow_id=None), status='PENDING'),
        _node(3, TaskNode(task_id=_ABSENT_NODE_TASK), status='RUNNING'),
        _node(
            4,
            TaskNode(task_id=None),
            status='FAILED',
            result=_stored(TaskResult(err=TaskError(error_code='ORDER_REJECTED', message='node blew up'))),
        ),
        _node(5, TaskNode(task_id=None), status='PENDING'),
        _node(
            6,
            TaskNode(task_id=None),
            status='FAILED',
            result=_stored(TaskResult(err=TaskError(error_code='ORDER_REJECTED', message=''))),
        ),
    )


type _Read = Callable[[PostgresBroker], Awaitable[object]]


def _detail_pair(task_id: str) -> ReadPair[object]:
    async def candidate(broker: PostgresBroker) -> object:
        return await python_reads.get_task_detail(broker, uuid.UUID(task_id))

    async def reference_read(broker: PostgresBroker) -> object:
        return await reference.get_task_detail(broker, uuid.UUID(task_id))

    return ReadPair(name=f'detail {task_id}', reference=reference_read, candidate=candidate)


def _node_pair(task_index: int) -> ReadPair[object]:
    async def candidate(broker: PostgresBroker) -> object:
        return await python_reads.get_workflow_node(broker, _RUN_ID, task_index)

    async def reference_read(broker: PostgresBroker) -> object:
        return await reference.get_workflow_node(broker, _RUN_ID, task_index)

    return ReadPair(name=f'node {task_index}', reference=reference_read, candidate=candidate)


def _kind(answer: object) -> str:
    """What one answer is: the found task, the absence, the node, no node, or an error."""
    match answer:
        case Ok(TaskDetail()):
            return 'task'
        case Ok(TaskDetailNotFound(reason=reason)):
            return f'absent {reason.value}'
        case Ok(WorkflowTaskDetail(leaf=None)):
            return 'node without task'
        case Ok(WorkflowTaskDetail()):
            return 'node with task'
        case Ok(None):
            return 'no node'
        case _:
            return f'other {answer!r}'


def test_the_functions_answer_what_the_client_statements_answered(
    empty_database: MakeDatabase,
) -> None:
    pairs = [_detail_pair(task_id) for task_id in _TASK_IDS]
    pairs += [_node_pair(task_index) for task_index in _NODE_INDEXES]

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert differing(answers) == []
    assert [_kind(each.candidate) for each in answers] == [
        'task',
        'task',
        'task',
        'task',
        'task',
        'task',
        'absent WITHIN_RETAINED_HISTORY',
        'absent NOT_CLASSIFIED',
        'node with task',
        'node with task',
        'node without task',
        'node without task',
        'node without task',
        'node without task',
        'node without task',
        'no node',
    ]


def test_the_comparison_sees_a_candidate_that_reads_another_task(
    empty_database: MakeDatabase,
) -> None:
    """The candidate reads the RUNNING task in place of the FAILED one; the answers must differ."""
    pair = _detail_pair(_FAILED)

    async def wrong(broker: PostgresBroker) -> object:
        return await get_task_detail(broker, uuid.UUID(_RUNNING))

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=wrong)],
    )

    assert differing(answers) == [pair.name]


def _core_detail_pair(task_id: str) -> ReadPair[object]:
    async def python_read(broker: PostgresBroker) -> object:
        return await python_reads.get_task_detail(broker, uuid.UUID(task_id))

    async def core_read(broker: PostgresBroker) -> object:
        return await get_task_detail(broker, uuid.UUID(task_id))

    return ReadPair(name=f'detail {task_id}', reference=python_read, candidate=core_read)


def _core_node_pair(task_index: int) -> ReadPair[object]:
    async def python_read(broker: PostgresBroker) -> object:
        return await python_reads.get_workflow_node(broker, _RUN_ID, task_index)

    async def core_read(broker: PostgresBroker) -> object:
        return await get_workflow_node(broker, _RUN_ID, task_index)

    return ReadPair(name=f'node {task_index}', reference=python_read, candidate=core_read)


def test_the_core_answers_what_python_answered(empty_database: MakeDatabase) -> None:
    """The caller switch: Python's detail and node reads against the production reads on the core."""
    pairs = [_core_detail_pair(task_id) for task_id in _TASK_IDS]
    pairs += [_core_node_pair(task_index) for task_index in _NODE_INDEXES]

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert differing_reports(answers) == {}
    assert [_kind(each.candidate) for each in answers] == [
        'task',
        'task',
        'task',
        'task',
        'task',
        'task',
        'absent WITHIN_RETAINED_HISTORY',
        'absent NOT_CLASSIFIED',
        'node with task',
        'node with task',
        'node without task',
        'node without task',
        'node without task',
        'node without task',
        'node without task',
        'no node',
    ]


def test_the_core_comparison_sees_a_candidate_that_reads_another_task(
    empty_database: MakeDatabase,
) -> None:
    """The core candidate reads the RUNNING task in place of the FAILED one."""
    pair = _core_detail_pair(_FAILED)

    async def wrong(broker: PostgresBroker) -> object:
        return await get_task_detail(broker, uuid.UUID(_RUNNING))

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=wrong)],
    )

    assert list(differing_reports(answers)) == [pair.name]
