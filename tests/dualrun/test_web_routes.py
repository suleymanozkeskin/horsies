"""Dual-run of the monitoring web app, per route group.

Reference: the Python app before the switch
(``tests/dualrun/reference/web_app.py``: Python's routers, schema probe,
read caches and event broadcaster). Candidate: ``create_monitoring_app`` as
shipped, which mounts the core's handler (``horsies._core.MonitoringHandler``).
Both apps answer the same requests; each app is new for its request, so the
read caches start empty on both sides.

- Read groups (tasks, workflows, workers, meta): one seeded database, read by
  both apps through one broker. A read writes nothing: the snapshot after
  the reads equals the one before. A record is the status, the JSON body,
  and the ``content-type`` and ``cache-control`` headers; the records must be
  equal, with an open span of the candidate (which reads second) up to
  ``COUNTING_SPAN_TOLERANCE_S`` longer. A worker's ages are measured to the
  database clock by the core and to the web host clock by Python (decided:
  the database clock); on one host the two agree within that tolerance.
- Actions: two fresh databases with the same seed, one per app; the
  snapshots and the records must be equal. The seed includes a resume that
  owes a sub-workflow child start, which both apps build through the
  broker's child spec source (``build_with``).
- Events: both apps subscribe on one database; the test sends one fixed set
  of notifications (each channel, a duplicate, and a burst over
  ``MAX_IDS_PER_EVENT``). Per topic, the data frames must be equal; a frame
  of one topic may come before or after a frame of another, as each side
  flushes its topics in its own order. A listener drop is not compared:
  Python goes on with a silent gap, the core sends one event with no ids per
  topic (decided).
"""

from __future__ import annotations

import asyncio
import json
import time
import typing
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, LiteralString

import psycopg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode as SpecTaskNode
from horsies.core.models.workflow import WorkflowDefinition, WorkflowSpec
from horsies.core.types.status import TaskStatus
from horsies.core.workflows.registry import register_workflow_definition
from horsies.web import INTENT_HEADER, INTENT_VALUE, AllowAll
from horsies.web.app import create_monitoring_app
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.monitoring_reads import (
    ReadAnswers,
    ReadPair,
    read_dual_run,
    report_difference,
)
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference.web_app import (
    create_monitoring_app as reference_monitoring_app,
)
from tests.dualrun.seeds import SeedStatus, TaskSeed, insert_task
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
from tests.integration.monitoring_seeding import (
    LastRun,
    NodeRow,
    ScheduleRow,
    SeedRow,
    TaskNode,
    WorkflowRow,
    task_row,
)
from tests.web_event_stream import AsgiEventStream

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]
type AppFactory = Callable[..., FastAPI]
type Record = dict[str, object]

_ACT: Final = {INTENT_HEADER: INTENT_VALUE}
_NOW: Final = datetime.now(UTC)
_FRAME_TIMEOUT_S: Final = 10.0
"""The longest wait for the next frame of an event stream."""
_QUIET_S: Final = 1.5
"""A wait without a frame after which an event stream is read to its end."""
_FRAMES_BOUND: Final = 64
"""The most frames one stream of the event pair is read for."""


# --------------------------------------------------------------------------- #
# Requests and records
# --------------------------------------------------------------------------- #
def _web_app_of(broker: PostgresBroker, app: Horsies | None = None) -> Horsies:
    """A horsies app whose broker is ``broker``."""
    built = app or Horsies(AppConfig(broker=broker.config))
    built._broker = broker  # pyright: ignore[reportPrivateUsage]
    broker.app = built
    return built


def _record(response: Response) -> Record:
    """The status, the body (JSON, or the text of another body) and the
    headers that the contract names."""
    content_type = response.headers.get('content-type', '')
    body: object = (
        response.json() if content_type.startswith('application/json') else response.text
    )
    return {
        'status': response.status_code,
        'body': body,
        'content-type': content_type,
        'cache-control': response.headers.get('cache-control'),
    }


async def _ask(
    factory: AppFactory,
    app: Horsies,
    method: str,
    path: str,
    *,
    mount: str | None = None,
) -> Record:
    """One request to a new app of ``factory``, mounted under ``mount`` when given."""
    built = factory(app, auth_policy=AllowAll())
    target: FastAPI = built
    if mount is not None:
        target = FastAPI()
        target.mount(mount, built)
    try:
        async with AsyncClient(transport=ASGITransport(app=target), base_url='http://test') as client:
            response = await client.request(method, path, headers=_ACT)
    finally:
        await built.state.events.close()
    return _record(response)


def _measured(record: Record, keys: tuple[str, ...]) -> Record:
    """``record`` with each top-level body value of ``keys`` replaced by
    whether it was measured: a read time or a latency differs per read."""
    body = record['body']
    match body:
        case dict():
            fields = typing.cast(dict[str, object], body)
            return {
                **record,
                'body': {
                    key: ('measured' if value is not None else None) if key in keys else value
                    for key, value in fields.items()
                },
            }
        case _:
            return record


def _read_pair(
    method: str,
    path: str,
    *,
    mount: str | None = None,
    measured: tuple[str, ...] = (),
) -> ReadPair[Record]:
    """One request, as the Python app answers it and as the core answers it.
    The body values of ``measured`` are compared by presence only."""

    def reading(factory: AppFactory) -> Callable[[PostgresBroker], Awaitable[Record]]:
        async def read(broker: PostgresBroker) -> Record:
            record = await _ask(factory, _web_app_of(broker), method, path, mount=mount)
            return _measured(record, measured)

        return read

    return ReadPair(
        name=f'{method} {mount or ""}{path}',
        reference=reading(reference_monitoring_app),
        candidate=reading(create_monitoring_app),
    )


def _differences(answers: tuple[ReadAnswers[Record], ...]) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for answer in answers:
        differences = report_difference(answer.reference, answer.candidate)
        if differences:
            found[answer.name] = differences
    return found


# --------------------------------------------------------------------------- #
# Read groups
# --------------------------------------------------------------------------- #
_PENDING: Final = str(uuid.uuid4())
_FAILED: Final = str(uuid.uuid4())
_COMPLETED: Final = str(uuid.uuid4())


def _task(task_id: str, name: str, status: TaskStatus, *, ended_ago: int | None) -> SeedRow:
    ended = None if ended_ago is None else _NOW - timedelta(seconds=ended_ago)
    return task_row(
        task_id=task_id,
        task_name=name,
        queue_name='default',
        status=status,
        priority=100,
        retry_count=0,
        max_retries=3,
        is_workflow_task=False,
        enqueued_at=_NOW - timedelta(seconds=120),
        claimed_at=None,
        started_at=None,
        completed_at=ended if status is TaskStatus.COMPLETED else None,
        failed_at=ended if status is TaskStatus.FAILED else None,
        good_until=None,
        error_code='TASK_EXCEPTION' if status is TaskStatus.FAILED else None,
        failed_reason=None,
        worker_id=None,
        worker_hostname=None,
        worker_pid=None,
        worker_process_name=None,
        claim_expires_at=None,
    )


def _task_rows() -> tuple[SeedRow, ...]:
    return (
        _task(_PENDING, 'web.pending', TaskStatus.PENDING, ended_ago=None),
        _task(_FAILED, 'web.failed', TaskStatus.FAILED, ended_ago=30),
        _task(_COMPLETED, 'web.completed', TaskStatus.COMPLETED, ended_ago=20),
    )


def test_the_task_reads_answer_as_python_did(empty_database: MakeDatabase) -> None:
    pairs = [
        _read_pair('GET', '/api/tasks'),
        _read_pair('GET', '/api/tasks?status=FAILED&status=COMPLETED&sort_by=task_name&sort_dir=asc'),
        _read_pair('GET', '/api/tasks?limit=1&offset=1'),
        _read_pair('GET', '/api/tasks/stats'),
        _read_pair('GET', '/api/tasks/facets'),
        _read_pair('GET', '/api/tasks/breakdown?group_by=task_name'),
        _read_pair('GET', '/api/tasks/breakdown?group_by=queue&status=FAILED'),
        _read_pair('GET', f'/api/tasks/{_FAILED}'),
        _read_pair('GET', f'/api/tasks/{_PENDING}'),
        _read_pair('GET', f'/api/tasks/{uuid.uuid4()}'),
        _read_pair('GET', '/api/tasks/not-a-uuid'),
        _read_pair('GET', '/api/tasks?status=NOT_A_STATUS'),
        _read_pair('GET', '/api/tasks?limit=0'),
        _read_pair('GET', '/api/tasks?offset=500&limit=1'),
        _read_pair('GET', '/api/tasks?error_category=NOPE'),
        _read_pair('GET', '/api/tasks?since=2026-01-01T00:00:00'),
    ]

    answers = read_dual_run(empty_database, _task_rows(), pairs)

    assert _differences(answers) == {}


_ROOT: Final = str(uuid.uuid4())


def _workflow_rows() -> tuple[SeedRow, ...]:
    run = WorkflowRow(
        id=_ROOT,
        name='web.flow',
        status='RUNNING',
        on_error='fail',
        definition_key='web.flow.v1',
        parent=None,
        sent_at=_NOW - timedelta(seconds=300),
        created_at=_NOW - timedelta(seconds=300),
        completed_at=None,
    )
    done = WorkflowRow(
        id=str(uuid.uuid4()),
        name='web.other',
        status='COMPLETED',
        on_error='fail',
        definition_key=None,
        parent=None,
        sent_at=_NOW - timedelta(seconds=200),
        created_at=_NOW - timedelta(seconds=200),
        completed_at=_NOW - timedelta(seconds=100),
    )
    nodes = tuple(
        NodeRow(
            id=str(uuid.uuid4()),
            workflow_id=_ROOT,
            task_index=index,
            node_id=f'node-{index}',
            task_name='web.step',
            status=status,
            dependencies=dependencies,
            allow_failed_deps=False,
            kind=TaskNode(task_id=None),
            result=None,
            created_at=_NOW - timedelta(seconds=300),
            started_at=started,
            completed_at=completed,
        )
        for index, status, dependencies, started, completed in (
            (0, 'COMPLETED', (), _NOW - timedelta(seconds=290), _NOW - timedelta(seconds=280)),
            (1, 'PENDING', (0,), None, None),
        )
    )
    return (run, done, *nodes)


def test_the_workflow_reads_answer_as_python_did(empty_database: MakeDatabase) -> None:
    pairs = [
        _read_pair('GET', '/api/workflows'),
        _read_pair('GET', '/api/workflows?name=web.flow&limit=5'),
        _read_pair('GET', '/api/workflows?status=COMPLETED'),
        _read_pair('GET', '/api/workflows/names'),
        _read_pair('GET', f'/api/workflows/{_ROOT}'),
        _read_pair('GET', f'/api/workflows/{_ROOT}/tasks/0'),
        _read_pair('GET', f'/api/workflows/{_ROOT}/tasks/9'),
        _read_pair('GET', f'/api/workflows/{_ROOT}/tasks/x'),
        _read_pair('GET', f'/api/workflows/{uuid.uuid4()}'),
        _read_pair('GET', '/api/workflows/not-a-uuid'),
        _read_pair('GET', '/api/workflows?limit=201'),
    ]

    answers = read_dual_run(empty_database, _workflow_rows(), pairs)

    assert _differences(answers) == {}


_ZERO_LED_RUN: Final = str(uuid.UUID('00' + uuid.uuid4().hex[2:]))
"""A run id whose first two hex digits are 0: ``int(text, 16)`` reads the
same value with ``0x``, a sign or a space in their place."""


def _id_form_rows() -> tuple[SeedRow, ...]:
    run = WorkflowRow(
        id=_ZERO_LED_RUN,
        name='web.forms',
        status='RUNNING',
        on_error='fail',
        definition_key=None,
        parent=None,
        sent_at=_NOW - timedelta(seconds=60),
        created_at=_NOW - timedelta(seconds=60),
        completed_at=None,
    )
    node = NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=_ZERO_LED_RUN,
        task_index=0,
        node_id='node-0',
        task_name='web.step',
        status='PENDING',
        dependencies=(),
        allow_failed_deps=False,
        kind=TaskNode(task_id=None),
        result=None,
        created_at=_NOW - timedelta(seconds=60),
        started_at=None,
        completed_at=None,
    )
    return (run, node)


def _id_forms_read(run_id: str) -> tuple[str, ...]:
    """Texts of ``run_id`` that Python's ``uuid.UUID(text)`` reads as it. The
    ``uuid`` crate refuses each one but the first two."""
    digits = uuid.UUID(run_id).hex
    return (
        run_id,
        f'urn:uuid:{run_id}',
        f'uuid:{run_id}',
        f'{{{digits}}}',
        f'{digits[:2]}-{digits[2:4]}-{digits[4:]}',
        f'0x{digits[2:]}',
        f'+{digits[1:]}',
        f' {digits[1:]}',
        f'{digits[1:16]}_{digits[16:]}',
        f'٠{digits[1:]}',
    )


def _id_forms_refused(run_id: str) -> tuple[str, ...]:
    """Texts of ``run_id`` that ``uuid.UUID(text)`` refuses. The ``uuid``
    crate reads the first one."""
    digits = uuid.UUID(run_id).hex
    return (f'URN:UUID:{run_id}', f'{digits[1:]}_')


def test_the_workflow_reads_take_the_id_forms_that_python_took(
    empty_database: MakeDatabase,
) -> None:
    """``get_workflow_run`` and ``get_workflow_node`` read the path text with
    ``uuid.UUID(text)``: a text that it reads names the run, any other names
    none (404)."""
    read = _id_forms_read(_ZERO_LED_RUN)
    refused = _id_forms_refused(_ZERO_LED_RUN)
    paths = [
        path
        for form in (*read, *refused)
        for path in (f'/api/workflows/{form}', f'/api/workflows/{form}/tasks/0')
    ]

    answers = read_dual_run(
        empty_database, _id_form_rows(), [_read_pair('GET', path) for path in paths],
    )

    statuses = {each.name: each.reference['status'] for each in answers}
    assert statuses == {
        f'GET {path}': 200 if form in read else 404
        for form in (*read, *refused)
        for path in (f'/api/workflows/{form}', f'/api/workflows/{form}/tasks/0')
    }, 'the reference reads each form that uuid.UUID reads'
    assert _differences(answers) == {}


_WORKER_STATES: Final[LiteralString] = """
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes,
        max_claim_batch, max_claim_per_worker, queues,
        queue_priorities, queue_max_concurrency, recovery_config,
        tasks_running, tasks_claimed, worker_started_at,
        slots_down, class_capacity
    ) VALUES
    ('web-worker', now() - interval '5 seconds', 'web-host', 4242, 2, 0, 2,
     ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
     1, 0, now() - interval '1 hour', '[]'::jsonb,
     '{"SHARED_ASYNC": {"capacity": 8, "serves": ["SHARED_ASYNC"]}}'),
    ('web-worker', now() - interval '65 seconds', 'web-host', 4242, 2, 0, 2,
     ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
     0, 0, now() - interval '1 hour', '[]'::jsonb, '{}'::jsonb)
"""


def _schedule_rows() -> tuple[SeedRow, ...]:
    return (
        ScheduleRow(schedule_name='nightly', next_run_at=_NOW + timedelta(hours=8), last_run=None),
        ScheduleRow(
            schedule_name='hourly',
            next_run_at=_NOW + timedelta(minutes=5),
            last_run=LastRun(at=_NOW - timedelta(minutes=55), task_id=str(uuid.uuid4()), run_count=3),
        ),
    )


def test_the_worker_reads_answer_as_python_did(empty_database: MakeDatabase) -> None:
    pairs = [
        _read_pair('GET', '/api/workers'),
        _read_pair('GET', '/api/workers/web-worker/history'),
        _read_pair('GET', '/api/workers/web-worker/history?limit=1'),
        _read_pair('GET', '/api/workers/never-reported/history'),
        _read_pair('GET', '/api/workers/schedules'),
        _read_pair('GET', '/api/workers/classes', measured=('checked_at',)),
        _read_pair('GET', '/api/workers/ping?timeout_seconds=0.1', measured=('db_latency_ms',)),
        _read_pair('GET', '/api/workers/ping?timeout_seconds=99'),
        _read_pair('GET', '/api/workers/web-worker/history?limit=1001'),
    ]

    answers = read_dual_run(
        empty_database, _schedule_rows(), pairs, seed_statements=(_WORKER_STATES,),
    )

    assert _differences(answers) == {}


# --------------------------------------------------------------------------- #
# Time text
# --------------------------------------------------------------------------- #
_SECOND: Final = _NOW.replace(microsecond=0)
"""A whole second near the seed time."""
_TIMED_TASK: Final = str(uuid.uuid4())
_TIMED_RUN: Final = str(uuid.uuid4())
_TIMED_WORKER: Final = 'web-timed-worker'


def _at(seconds_before: int, microsecond: int) -> datetime:
    """``seconds_before`` whole seconds before ``_SECOND``, at ``microsecond``."""
    return _SECOND - timedelta(seconds=seconds_before) + timedelta(microseconds=microsecond)


def _timed_rows() -> tuple[SeedRow, ...]:
    """Rows whose times have a microsecond that is a multiple of 1,000, or 0.

    pydantic writes 6 fraction digits for a microsecond other than 0 and none
    for 0 (``...:14.500000Z``, ``...:14Z``). A writer that drops the trailing
    zero digits (``...:14.500Z``) differs on these times only.
    """
    task = task_row(
        task_id=_TIMED_TASK,
        task_name='web.timed',
        queue_name='default',
        status=TaskStatus.COMPLETED,
        priority=100,
        retry_count=0,
        max_retries=3,
        is_workflow_task=False,
        enqueued_at=_at(120, 500_000),
        claimed_at=None,
        started_at=None,
        completed_at=_at(20, 120_000),
        failed_at=None,
        good_until=None,
        error_code=None,
        failed_reason=None,
        worker_id=None,
        worker_hostname=None,
        worker_pid=None,
        worker_process_name=None,
        claim_expires_at=None,
    )
    run = WorkflowRow(
        id=_TIMED_RUN,
        name='web.timed.flow',
        status='COMPLETED',
        on_error='fail',
        definition_key=None,
        parent=None,
        sent_at=_at(300, 0),
        created_at=_at(300, 0),
        completed_at=_at(100, 250_000),
    )
    node = NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=_TIMED_RUN,
        task_index=0,
        node_id='node-0',
        task_name='web.step',
        status='COMPLETED',
        dependencies=(),
        allow_failed_deps=False,
        kind=TaskNode(task_id=None),
        result=None,
        created_at=_at(300, 0),
        started_at=_at(290, 500_000),
        completed_at=_at(280, 1_000),
    )
    schedule = ScheduleRow(
        schedule_name='timed',
        next_run_at=_SECOND + timedelta(minutes=5, milliseconds=500),
        last_run=LastRun(at=_at(55, 750_000), task_id=str(uuid.uuid4()), run_count=1),
    )
    return (task, run, node, schedule)


_TIMED_WORKER_STATE: Final[LiteralString] = """
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes,
        max_claim_batch, max_claim_per_worker, queues,
        queue_priorities, queue_max_concurrency, recovery_config,
        tasks_running, tasks_claimed, worker_started_at,
        slots_down, class_capacity
    ) VALUES
    ('web-timed-worker', date_trunc('second', now()) - interval '4.5 seconds', 'web-host', 4243,
     2, 0, 2, ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
     0, 0, date_trunc('second', now()) - interval '1 hour' + interval '120 milliseconds',
     '[]'::jsonb, '{}'::jsonb)
"""


def _body_value(record: Record, *keys: str) -> object:
    """The value at ``keys`` in the JSON body of ``record``."""
    value: object = record['body']
    for key in keys:
        match value:
            case dict():
                value = typing.cast(dict[str, object], value)[key]
            case _:
                raise AssertionError(f'no object holds {key!r}: {value!r}')
    return value


def test_the_times_of_an_answer_are_written_as_python_wrote_them(
    empty_database: MakeDatabase,
) -> None:
    pairs = [
        _read_pair('GET', '/api/tasks'),
        _read_pair('GET', f'/api/tasks/{_TIMED_TASK}'),
        _read_pair('GET', '/api/workflows'),
        _read_pair('GET', f'/api/workflows/{_TIMED_RUN}'),
        _read_pair('GET', f'/api/workflows/{_TIMED_RUN}/tasks/0'),
        _read_pair('GET', '/api/workers'),
        _read_pair('GET', f'/api/workers/{_TIMED_WORKER}/history'),
        _read_pair('GET', '/api/workers/schedules'),
    ]

    answers = read_dual_run(
        empty_database, _timed_rows(), pairs, seed_statements=(_TIMED_WORKER_STATE,),
    )

    detail = next(each for each in answers if each.name == f'GET /api/tasks/{_TIMED_TASK}')
    enqueued_at = _body_value(detail.reference, 'leaf', 'enqueued_at')
    assert enqueued_at == _at(120, 500_000).strftime('%Y-%m-%dT%H:%M:%S.500000Z'), (
        'the seed holds a time whose microsecond is a multiple of 1,000'
    )
    assert _differences(answers) == {}


def test_meta_and_the_api_paths_answer_as_python_did(empty_database: MakeDatabase) -> None:
    pairs = [
        _read_pair('GET', '/api/meta'),
        _read_pair('GET', '/monitoring/api/meta', mount='/monitoring'),
        _read_pair('GET', '/api/unknown'),
        _read_pair('GET', '/api'),
        _read_pair('GET', '/api/tasks/'),
    ]

    answers = read_dual_run(empty_database, (), pairs)

    assert _differences(answers) == {}


# --------------------------------------------------------------------------- #
# Actions
# --------------------------------------------------------------------------- #
_TASK: Final = 'dualrun.web.value'
_WORKER: Final = 'worker-dualrun-web'
_RESULT: Final = '{"__h_task_result__":true,"ok":5,"err":null}'

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=SecretStr('postgresql://dualrun@127.0.0.1:1/unused')),
    ),
)


@APP.task(task_name=_TASK)
def value_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok=5)


class WebResumeChild(WorkflowDefinition[int]):
    """The child that a resume through the web starts for the promoted
    sub-workflow node."""

    name = 'dualrun.web.child'
    definition_key = 'dualrun.web.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = SpecTaskNode(fn=value_task)
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The child definition, registered again: other tests clear the registry."""
    register_workflow_definition(WebResumeChild)


_MISSING_TASK: Final = uuid.UUID('7b0c2f4e-5a61-4d8e-9f13-2c6a8b1d4e70')
"""A task id that no seed writes: both sides ask for the same absent task."""
_MISSING_WORKFLOW: Final = uuid.UUID('3e9d1a72-8c4b-4f05-a6e2-91b7d0c5f838')
"""A workflow id that no seed writes."""


@dataclass(frozen=True, slots=True)
class _ActionSeed:
    pending: uuid.UUID
    running: uuid.UUID
    tree: uuid.UUID


def _action_seed(connection: Connection) -> _ActionSeed:
    pending = insert_task(connection, TaskSeed(task_name='web.cancel.pending', status=SeedStatus.PENDING))
    running = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=running,
                name='web.running',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, _TASK),),
            ),
        ],
    )
    root, child = uuid.uuid4(), uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=root,
                name='web.root',
                on_error=OnError.FAIL,
                parent=TopLevel(),
                nodes=(
                    task_node(0, _TASK),
                    subworkflow_node(
                        1, WebResumeChild.name, 0, definition_key=WebResumeChild.definition_key,
                    ),
                    subworkflow_node(2, 'web.child'),
                ),
            ),
            WorkflowSeed(
                workflow_id=child,
                name='web.child',
                on_error=OnError.FAIL,
                parent=ChildOf(parent=root, parent_task_index=2, depth=1),
                nodes=(task_node(0, _TASK),),
            ),
        ],
    )
    _ = run_node_task(connection, root, 0, _WORKER)
    _ = claim_node_task(connection, child, 0, _WORKER)
    return _ActionSeed(pending=pending.task_id, running=running, tree=root)


def _pause_and_finish(database: DualRunDatabase, seed: _ActionSeed) -> None:
    """Pause the tree, then finish its node 0 while it is paused, so that the
    resume owes the start of the sub-workflow child of node 1."""
    cover_history(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            pause_tree(connection, seed.tree)
            _ = finish_node_task(connection, seed.tree, 0, _WORKER, _RESULT)


def _action_requests(seed: _ActionSeed) -> tuple[tuple[str, str], ...]:
    return (
        ('POST', f'/api/tasks/{seed.pending}/cancel'),
        ('POST', f'/api/tasks/{seed.pending}/cancel'),
        ('POST', f'/api/tasks/{_MISSING_TASK}/cancel'),
        ('POST', '/api/tasks/not-a-uuid/cancel'),
        ('POST', f'/api/workflows/{seed.running}/pause'),
        ('POST', f'/api/workflows/{seed.running}/pause'),
        ('POST', f'/api/workflows/{seed.running}/resume'),
        ('POST', f'/api/workflows/{seed.running}/cancel'),
        ('POST', f'/api/workflows/{seed.running}/cancel'),
        ('POST', f'/api/workflows/{seed.tree}/resume'),
        ('POST', f'/api/workflows/{_MISSING_WORKFLOW}/resume'),
        ('POST', '/api/workflows/not-a-uuid/pause'),
    )


def _acting(factory: AppFactory) -> Callable[[DualRunDatabase, _ActionSeed], tuple[Record, ...]]:
    async def act(database: DualRunDatabase, seed: _ActionSeed) -> tuple[Record, ...]:
        async with open_broker(database) as broker:
            app = _web_app_of(broker, APP)
            return tuple([
                await _ask(factory, app, method, path) for method, path in _action_requests(seed)
            ])

    def side(database: DualRunDatabase, seed: _ActionSeed) -> tuple[Record, ...]:
        _pause_and_finish(database, seed)
        return asyncio.run(act(database, seed))

    return side


def test_the_actions_write_what_python_wrote(empty_database: MakeDatabase) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _action_seed,
            OperationSide(name='python routes', act=_acting(reference_monitoring_app)),
            OperationSide(name='core handler', act=_acting(create_monitoring_app)),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert report_difference(list(run.reference.report), list(run.candidate.report)) == []
    statuses = [record['status'] for record in run.reference.report]
    assert statuses == [200, 409, 404, 400, 200, 409, 200, 200, 200, 200, 404, 400]


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #
_BURST: Final = 101
"""One more id than an event carries: the window overflows."""
_BURST_GAP_S: Final = 1.0
"""The wait between the first notifications and the burst."""


def _notify(database: DualRunDatabase) -> None:
    """The fixed notifications: one transaction per channel set, then the burst
    in one transaction of its own."""
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            for channel, payload in (
                ('horsies_task_status', 'task-a'),
                ('horsies_task_status', 'task-a'),
                ('horsies_task_status', 'task-b'),
                ('horsies_workflow_status', 'workflow-a'),
                ('horsies_worker_state', 'worker-a'),
            ):
                _ = connection.execute('SELECT pg_notify(%s, %s)', (channel, payload))
    # Past the debounce of both sides: the burst opens windows of its own.
    time.sleep(_BURST_GAP_S)
    with psycopg.connect(database.url, autocommit=True) as connection:
        with connection.transaction():
            for index in range(_BURST):
                _ = connection.execute(
                    'SELECT pg_notify(%s, %s)', ('horsies_workflow_status', f'burst-{index}'),
                )


async def _frames_by_topic(stream: AsgiEventStream) -> dict[str, list[list[str]]]:
    """The data frames of a stream until it is quiet, ids per frame by topic;
    heartbeats aside."""
    by_topic: dict[str, list[list[str]]] = {}
    for _ in range(_FRAMES_BOUND):
        try:
            frame = await stream.next_frame(timeout_s=_QUIET_S)
        except TimeoutError:
            return by_topic
        if not frame.startswith('data: '):
            continue
        event: dict[str, Any] = json.loads(frame.removeprefix('data: ').strip())
        by_topic.setdefault(str(event['topic']), []).append(
            [str(each) for each in event.get('ids', [])],
        )
    raise AssertionError(f'the stream sent more than {_FRAMES_BOUND} frames')


async def _both_streams(database: DualRunDatabase) -> tuple[dict[str, list[list[str]]], ...]:
    async with open_broker(database) as broker:
        app = _web_app_of(broker)
        python = reference_monitoring_app(app, auth_policy=AllowAll())
        core = create_monitoring_app(app, auth_policy=AllowAll())
        try:
            async with (
                AsgiEventStream(python, '/api/events') as python_stream,
                AsgiEventStream(core, '/api/events') as core_stream,
            ):
                # Both listeners subscribe at the first frame wait; a heartbeat
                # comes after the wait, so give both a moment to LISTEN.
                await asyncio.sleep(1.0)
                await asyncio.to_thread(_notify, database)
                return (
                    await _frames_by_topic(python_stream),
                    await _frames_by_topic(core_stream),
                )
        finally:
            await python.state.events.close()
            await core.state.events.close()


def test_the_event_frames_are_pythons(empty_database: MakeDatabase) -> None:
    database = empty_database()

    async def install() -> None:
        async with open_broker(database):
            pass

    asyncio.run(install())
    python, core = asyncio.run(_both_streams(database))

    assert python == {
        'tasks': [['task-a', 'task-b']],
        'workflows': [['workflow-a'], []],
        'workers': [['worker-a']],
    }
    assert core == python
