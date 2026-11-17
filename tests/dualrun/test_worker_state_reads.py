"""Dual-run of the worker state and class read switch.

Reference: Python's bodies before the switch
(``tests/dualrun/reference/broker_monitoring.py``: ``list_worker_states_async``,
``get_worker_state_async``, ``get_worker_state_history_async``,
``get_class_coverage_async``, ``get_class_report_async``) on the loop's
SQLAlchemy pool. Candidate: the ``PostgresBroker`` methods, which the core
runs. Both read one seeded database, and a read writes nothing, so the
snapshot of the database cannot change; the reports are the comparison.

The snapshots, the coverage and the report compare as frozen dataclasses.
The class reads take ``checked_at`` from each statement's own clock, so
``checked_at`` and the wait of a stream compare within the time between
the two reads. A failure compares by code and ``retryable``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, LiteralString

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.class_coverage import (
    ClassCoverage,
    ClassReport,
    NoneDue,
    OldestWaiting,
    PendingStream,
)
from horsies.core.models.health import WorkerStateSnapshot
from horsies.core.types.result import Err, Ok
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.reference import broker_monitoring as python_reads

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]
Seed = Callable[[psycopg.Connection[tuple[object, ...]]], None]

_READ_GAP: Final = timedelta(seconds=30)
"""More than the time between the reference read and the candidate read."""

_HISTORY_LIMITS: Final[tuple[int | None, ...]] = (None, 1, 2, 0, -3, 2**70)
"""Every retained row, two bounds, two refused limits and one above the
``bigint`` range."""

_SEED_STATE: Final[LiteralString] = """
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid, processes, max_claim_batch,
        max_claim_per_worker, cluster_wide_cap, queues, queue_priorities,
        queue_max_concurrency, recovery_config, tasks_running, tasks_claimed,
        memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
        worker_started_at, slots_down, class_capacity
    ) VALUES (
        %(worker_id)s::text, now() - make_interval(secs => %(age_s)s),
        'host-' || %(worker_id)s::text,
        %(pid)s, 4, 8, 4, %(cluster_wide_cap)s, %(queues)s, %(queue_priorities)s::jsonb,
        %(queue_max_concurrency)s::jsonb, %(recovery_config)s::jsonb, %(tasks_running)s, 1,
        %(memory_usage_mb)s, NULL, 12.5, NULL, now() - interval '1 day',
        %(slots_down)s::jsonb, %(class_capacity)s::jsonb
    )
"""

_SEED_TASK: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at, command_fingerprint_version,
        command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT horsies_uuidv7(), 'reads.task', %(queue)s::text, %(priority)s, '{}', NULL, %(execution)s::text,
           'standard_30d', now() - interval '1 hour', now() - interval '1 hour', 1,
           sha256(convert_to(gen_random_uuid()::text, 'UTF8')), NULL, %(status)s::text,
           now() + make_interval(secs => %(due_in_s)s),
           CASE WHEN %(owner)s::text IS NULL THEN NULL ELSE now() END,
           %(owner)s::text,
           CASE WHEN %(status)s::text = 'CLAIMED' THEN now() + interval '5 minutes' END,
           CASE WHEN %(status)s::text = 'RUNNING' THEN now() END,
           NULL, NULL,
           CASE WHEN %(status)s::text = 'RUNNING' THEN 4242 END,
           CASE WHEN %(status)s::text = 'RUNNING' THEN 'host-1' END,
           CASE WHEN %(status)s::text = 'RUNNING' THEN 'child-1' END,
           0, 0, NULL, NULL, false, now(), false, NULL, NULL
    FROM generate_series(1, %(count)s)
"""

_FLEET_CAPACITY: Final[dict[str, object]] = {
    'ISOLATED': {'capacity': 2, 'serves': ['ISOLATED']},
    'SHARED_ASYNC': {'capacity': 8, 'serves': ['SHARED_ASYNC']},
}

_DOWN_SLOTS: Final[list[dict[str, object]]] = [
    {
        'slot': 1,
        'cause': {'kind': 'resource_shortage', 'errno': 'EMFILE'},
        'detail': 'OSError: [Errno 24] Too many open files',
        'down_since': '2026-11-17T09:30:00+00:00',
        'failed_boots': 2,
    },
    {
        'slot': 3,
        'cause': {'kind': 'thread_start_refused'},
        'detail': "RuntimeError: can't start new thread",
        'down_since': '2026-11-17T09:31:00.250000+00:00',
        'failed_boots': 1,
    },
]


def _state(
    connection: psycopg.Connection[tuple[object, ...]],
    worker_id: str,
    *,
    age_s: int,
    tasks_running: int = 0,
    queues: tuple[str, ...] = ('a', 'b'),
    slots_down: list[dict[str, object]] | None = None,
    class_capacity: dict[str, object] | None = None,
) -> None:
    """One snapshot of ``worker_id``, ``age_s`` seconds old in database time."""
    _ = connection.execute(
        _SEED_STATE,
        {
            'worker_id': worker_id,
            'age_s': age_s,
            'pid': 4000 + age_s,
            'cluster_wide_cap': 10 if worker_id.endswith('a') else None,
            'queues': list(queues),
            'queue_priorities': json.dumps({'a': 10, 'b': 90}),
            'queue_max_concurrency': json.dumps({'b': 2}),
            'recovery_config': json.dumps(
                {
                    'auto_requeue_stale_claimed': True,
                    'check_interval_ms': 30_000,
                    'passes': {'stale_running': {'last_rows': [1, 2.5, None]}},
                },
            ),
            'tasks_running': tasks_running,
            'memory_usage_mb': 120.25 if tasks_running else None,
            'slots_down': json.dumps(slots_down or []),
            'class_capacity': json.dumps(_FLEET_CAPACITY if class_capacity is None else class_capacity),
        },
    )


def _tasks(
    connection: psycopg.Connection[tuple[object, ...]],
    queue: str,
    execution: str,
    *,
    count: int = 1,
    priority: int = 50,
    status: str = 'PENDING',
    due_in_s: int = -60,
    owner: str | None = None,
) -> None:
    _ = connection.execute(
        _SEED_TASK,
        {
            'queue': queue,
            'execution': execution,
            'count': count,
            'priority': priority,
            'status': status,
            'due_in_s': due_in_s,
            'owner': owner,
        },
    )


def _fleet_seed(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    # w-a: three snapshots; the newest is the latest.
    for age_s, running in ((50, 1), (30, 2), (5, 3)):
        _state(connection, 'w-a', age_s=age_s, tasks_running=running)
    # w-b: two slots down, and a budget of shared threads only.
    _state(
        connection,
        'w-b',
        age_s=8,
        queues=('b',),
        slots_down=_DOWN_SLOTS,
        class_capacity={'SHARED_THREAD': {'capacity': 0, 'serves': ['SHARED_THREAD']}},
    )
    # w-old: not live (two days old); listed, but it serves nothing.
    _state(connection, 'w-old', age_s=172_800, queues=('c',))
    # Streams: served, without capacity, unserved, and one due later.
    _tasks(connection, 'a', 'ISOLATED', count=3, priority=10)
    _tasks(connection, 'a', 'ISOLATED', count=1, priority=90, due_in_s=-600)
    _tasks(connection, 'b', 'SHARED_THREAD', count=2)
    _tasks(connection, 'c', 'ISOLATED', count=1)
    _tasks(connection, 'a', 'SHARED_ASYNC', count=2, due_in_s=3_600)
    # In flight on w-a: one RUNNING isolated row and two claimed async rows.
    _tasks(connection, 'a', 'ISOLATED', status='RUNNING', owner='w-a')
    _tasks(connection, 'a', 'SHARED_ASYNC', count=2, status='CLAIMED', owner='w-a')


@dataclass(frozen=True, slots=True)
class _Failed:
    code: BrokerErrorCode
    retryable: bool


type _Answer[T] = T | _Failed


def _answer[T](result: BrokerResult[T]) -> _Answer[T]:
    match result:
        case Ok(value):
            return value
        case Err(error):
            return _Failed(code=error.code, retryable=error.retryable)


@dataclass(frozen=True, slots=True)
class _Reads:
    states: _Answer[list[WorkerStateSnapshot]]
    each: tuple[_Answer[WorkerStateSnapshot | None], ...]
    histories: tuple[_Answer[list[WorkerStateSnapshot]], ...]
    coverage: _Answer[ClassCoverage]
    report: _Answer[ClassReport]


_WORKERS: Final = ('w-a', 'w-b', 'w-old', 'w-missing')


async def _reads(database: DualRunDatabase, *, on_core: bool) -> _Reads:
    """Every read of the switch: Python's bodies, or the broker's on the core."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        if on_core:
            states = await broker.list_worker_states_async()
            each = [await broker.get_worker_state_async(worker) for worker in _WORKERS]
            histories = [
                await broker.get_worker_state_history_async('w-a', limit=limit)
                for limit in _HISTORY_LIMITS
            ]
            coverage = await broker.get_class_coverage_async()
            report = await broker.get_class_report_async()
        else:
            states = await python_reads.list_worker_states_async(broker)
            each = [await python_reads.get_worker_state_async(broker, worker) for worker in _WORKERS]
            histories = [
                await python_reads.get_worker_state_history_async(broker, 'w-a', limit=limit)
                for limit in _HISTORY_LIMITS
            ]
            coverage = await python_reads.get_class_coverage_async(broker)
            report = await python_reads.get_class_report_async(broker)
        return _Reads(
            states=_answer(states),
            each=tuple(_answer(state) for state in each),
            histories=tuple(_answer(history) for history in histories),
            coverage=_answer(coverage),
            report=_answer(report),
        )
    finally:
        await broker.close_async()


async def _install(database: DualRunDatabase) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        installed = await broker.ensure_schema_initialized()
        assert installed.is_ok(), installed
    finally:
        await broker.close_async()


def _dual_run(database: DualRunDatabase, seed: Seed) -> tuple[_Reads, _Reads]:
    """Install and seed; Python's reads, then the broker's on the core."""
    asyncio.run(_install(database))
    with psycopg.connect(database.url, autocommit=True) as connection:
        seed(connection)
    return asyncio.run(_reads(database, on_core=False)), asyncio.run(_reads(database, on_core=True))


def _assert_close(earlier: datetime | timedelta, later: datetime | timedelta) -> None:
    match earlier, later:
        case datetime(), datetime():
            gap = later - earlier
        case timedelta(), timedelta():
            gap = later - earlier
        case _:
            raise AssertionError(f'{earlier!r} and {later!r} are not of one kind')
    assert timedelta(0) <= gap <= _READ_GAP, (earlier, later)


def _assert_coverage_equal(python: _Answer[ClassCoverage], core: _Answer[ClassCoverage]) -> None:
    match python, core:
        case ClassCoverage() as before, ClassCoverage() as after:
            _assert_close(before.checked_at, after.checked_at)
            assert dataclasses.replace(after, checked_at=before.checked_at) == before
        case _:
            assert core == python


def _without_clock(stream: PendingStream) -> tuple[PendingStream, timedelta | None]:
    match stream.wait:
        case OldestWaiting(enqueued_at=enqueued_at, wait=wait):
            return (
                dataclasses.replace(
                    stream, wait=OldestWaiting(enqueued_at=enqueued_at, wait=timedelta(0)),
                ),
                wait,
            )
        case NoneDue():
            return stream, None


def _assert_report_equal(python: _Answer[ClassReport], core: _Answer[ClassReport]) -> None:
    match python, core:
        case ClassReport() as before, ClassReport() as after:
            _assert_close(before.checked_at, after.checked_at)
            assert after.workers == before.workers
            assert len(after.streams) == len(before.streams)
            for earlier, later in zip(before.streams, after.streams, strict=True):
                earlier_stream, earlier_wait = _without_clock(earlier)
                later_stream, later_wait = _without_clock(later)
                assert later_stream == earlier_stream
                match earlier_wait, later_wait:
                    case timedelta(), timedelta():
                        _assert_close(earlier_wait, later_wait)
                    case _:
                        assert later_wait == earlier_wait
        case _:
            assert core == python


def test_the_core_answers_what_python_answered(empty_database: MakeDatabase) -> None:
    python, core = _dual_run(empty_database(), _fleet_seed)

    assert core.states == python.states
    assert core.each == python.each
    assert core.histories == python.histories
    _assert_coverage_equal(python.coverage, core.coverage)
    _assert_report_equal(python.report, core.report)
    # The seed reaches every arm: three workers, an unknown one, every
    # history answer, and every coverage state.
    match core.states:
        case list() as states:
            assert [state.worker_id for state in states] == ['w-a', 'w-b', 'w-old']
            assert states[0].tasks_running == 3
        case _Failed() as failed:
            raise AssertionError(f'the worker list failed: {failed}')
    assert core.each[3] is None
    assert [
        len(history) if isinstance(history, list) else history for history in core.histories
    ] == [
        3,
        1,
        2,
        _Failed(code=BrokerErrorCode.INVALID_INPUT, retryable=False),
        _Failed(code=BrokerErrorCode.INVALID_INPUT, retryable=False),
        _Failed(code=BrokerErrorCode.MONITORING_QUERY_FAILED, retryable=False),
    ]


def _unreadable_seed(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    _state(connection, 'w-a', age_s=5)
    # A live snapshot whose slots_down names a slot twice, and whose budgets
    # put one class in two budgets.
    _state(
        connection,
        'w-bad',
        age_s=5,
        slots_down=[_DOWN_SLOTS[0], _DOWN_SLOTS[0]],
        class_capacity={
            'ISOLATED': {'capacity': 1, 'serves': ['ISOLATED']},
            'SHARED_ASYNC': {'capacity': 1, 'serves': ['SHARED_ASYNC', 'ISOLATED']},
        },
    )
    _tasks(connection, 'a', 'ISOLATED')


def test_an_unreadable_stored_snapshot_fails_both_reads_alike(
    empty_database: MakeDatabase,
) -> None:
    python, core = _dual_run(empty_database(), _unreadable_seed)

    refused = _Failed(code=BrokerErrorCode.MONITORING_QUERY_FAILED, retryable=False)
    assert python.states == refused
    assert core.states == python.states
    assert core.each == python.each
    assert core.coverage == python.coverage == refused
    assert core.report == python.report == refused


def test_the_comparison_sees_a_candidate_that_reads_another_worker(
    empty_database: MakeDatabase,
) -> None:
    """The reference reads w-a and the candidate w-b; the snapshots must differ."""
    database = empty_database()
    python, core = _dual_run(database, _fleet_seed)

    assert python.each[0] != core.each[1]
