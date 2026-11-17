# pyright: reportPrivateUsage=false
"""Dual-runs of the claim release.

Two pairs per placement, with the same seed (one row per fence case):

1. The SQL redesign: the per-row updates against ``horsies_release_claims``.
   Reference: ``tests/dualrun/reference/release.py``. Candidate: the worker's
   ``_requeue_claimed_task`` (placement SAME_CLASS) and
   ``_return_rows_of_another_class`` (placement ISOLATED). Both sides must
   leave the same rows PENDING and the others as they were.
2. The caller switch: the worker's callers on SQLAlchemy against the core.
   Reference: ``tests/dualrun/reference/release_caller.py``, the two method
   bodies before the switch. Candidate: the same methods as shipped, which
   call ``CoreHandle.release_claims_async``. The snapshots, the outcomes and
   the queue wakes must be equal.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies._core import CoreHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.claiming import ClaimMixin
from horsies.core.worker.dispatch import DispatchMixin
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.row_class import RowClassMismatch
from horsies.core.worker.runtime import _RequeueOutcome
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import release as reference
from tests.dualrun.reference import release_caller
from tests.dualrun.seeds import SeedStatus, TaskSeed, insert_task
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_CLAIMED_AGO: Final = timedelta(seconds=5)
_OTHER_GENERATION: Final = timedelta(seconds=1)


@dataclass(frozen=True, slots=True)
class _Dispatch:
    """A row as the worker's dispatch knows it: its id, queue, name and claim generation."""

    task_id: str
    task_name: str
    queue_name: str
    claimed_at: datetime


@dataclass(frozen=True, slots=True)
class _Seed:
    worker_id: str
    dispatches: tuple[_Dispatch, ...]


def _seed(connection: Connection) -> _Seed:
    """One row per fence case. Only ``held`` and ``held_reports`` are held by the dispatch."""
    worker_id = f'worker-{uuid.uuid4().hex}'
    other_worker = f'worker-{uuid.uuid4().hex}'

    def claimed(name: str, owner: str, queue: str = 'default') -> TaskSeed:
        return TaskSeed(
            task_name=name,
            status=SeedStatus.CLAIMED,
            worker_id=owner,
            claimed_ago=_CLAIMED_AGO,
            queue_name=queue,
        )

    held = insert_task(connection, claimed('release.held', worker_id))
    held_reports = insert_task(connection, claimed('release.held_reports', worker_id, 'reports'))
    reclaimed = insert_task(connection, claimed('release.reclaimed', worker_id))
    other = insert_task(connection, claimed('release.other_worker', other_worker))
    running = insert_task(
        connection,
        TaskSeed(
            task_name='release.running',
            status=SeedStatus.RUNNING,
            worker_id=worker_id,
            claimed_ago=_CLAIMED_AGO,
            started_ago=_CLAIMED_AGO,
        ),
    )
    pending = insert_task(
        connection, TaskSeed(task_name='release.pending', status=SeedStatus.PENDING)
    )
    generation = held.claimed_at
    assert generation is not None, 'a CLAIMED seed has a claim time'

    def dispatch(task_id: uuid.UUID, name: str, at: datetime, queue: str = 'default') -> _Dispatch:
        return _Dispatch(task_id=str(task_id), task_name=name, queue_name=queue, claimed_at=at)

    return _Seed(
        worker_id=worker_id,
        dispatches=(
            dispatch(held.task_id, 'release.held', generation),
            dispatch(held_reports.task_id, 'release.held_reports', generation, 'reports'),
            dispatch(reclaimed.task_id, 'release.reclaimed', generation - _OTHER_GENERATION),
            dispatch(other.task_id, 'release.other_worker', generation),
            dispatch(running.task_id, 'release.running', generation),
            dispatch(pending.task_id, 'release.pending', generation),
        ),
    )


@dataclass(frozen=True, slots=True)
class _WorkerParts:
    """What the worker's callers use: its session factory and its broker's core handle."""

    sf: async_sessionmaker[AsyncSession]
    broker: PostgresBroker


@asynccontextmanager
async def _worker_parts(database: DualRunDatabase) -> AsyncGenerator[_WorkerParts, None]:
    """A session factory and a broker on ``database``; both closed at exit."""
    engine = create_async_engine(
        database.url.replace('postgresql://', 'postgresql+psycopg://', 1)
    )
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        yield _WorkerParts(sf=async_sessionmaker(engine, expire_on_commit=False), broker=broker)
    finally:
        _ = await broker.close_async()
        await engine.dispose()


class _DispatchProbe(DispatchMixin):
    def __init__(self, parts: _WorkerParts, worker_id: str) -> None:
        self.sf = parts.sf
        self.worker_instance_id = worker_id
        self._broker = parts.broker

    @property
    def core_handle(self) -> CoreHandle:
        return self._broker._core_handle


class _ClaimProbe(ClaimMixin):
    def __init__(
        self,
        parts: _WorkerParts,
        worker_id: str,
        registered: Mapping[str, Execution],
    ) -> None:
        self.sf = parts.sf
        self.worker_instance_id = worker_id
        self._registered_classes = registered
        self._broker = parts.broker

    @property
    def core_handle(self) -> CoreHandle:
        return self._broker._core_handle


# ---- SAME_CLASS ----


def _reference_unclaim(database: DualRunDatabase, seed: _Seed) -> tuple[bool, ...]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        return tuple(
            reference.unclaim(connection, each.task_id, seed.worker_id, each.claimed_at)
            for each in seed.dispatches
        )


def _dispatch_claim(each: _Dispatch) -> DispatchClaim:
    return DispatchClaim(
        claimed_at=each.claimed_at,
        retry_count=0,
        max_retries=0,
        task_options=None,
    )


async def _candidate_requeue_async(
    database: DualRunDatabase, seed: _Seed
) -> tuple[_RequeueOutcome, ...]:
    async with _worker_parts(database) as parts:
        probe = _DispatchProbe(parts, seed.worker_id)
        return tuple(
            [
                await probe._requeue_claimed_task(
                    each.task_id, 'dual-run', _dispatch_claim(each)
                )
                for each in seed.dispatches
            ]
        )


def _candidate_requeue(database: DualRunDatabase, seed: _Seed) -> tuple[_RequeueOutcome, ...]:
    return asyncio.run(_candidate_requeue_async(database, seed))


def _requeued(outcome: _RequeueOutcome) -> bool:
    match outcome:
        case _RequeueOutcome.REQUEUED:
            return True
        case _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED:
            return False
        case _RequeueOutcome.TERMINAL_REPLAYED | _RequeueOutcome.DB_ERROR:
            raise AssertionError(f'the release answered {outcome}')


def _ok[RR, CR](
    result: Result[OperationDualRun[RR, CR], SelfCheckStepFailed],
) -> OperationDualRun[RR, CR]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_release_same_class_writes_what_the_per_row_unclaim_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='per-row unclaim', act=_reference_unclaim),
            OperationSide(name='horsies_release_claims', act=_candidate_requeue),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == (True, True, False, False, False, False)
    assert tuple(_requeued(each) for each in run.candidate.report) == run.reference.report


# ---- ISOLATED ----


def _reference_return_as_isolated(database: DualRunDatabase, seed: _Seed) -> tuple[bool, ...]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        return tuple(
            reference.return_as_isolated(
                connection, each.task_id, each.queue_name, seed.worker_id, each.claimed_at
            )
            for each in seed.dispatches
        )


def _claimed_row(each: _Dispatch) -> dict[str, Any]:
    """The row of ``each`` as the claim gives it: SHARED_ASYNC on the row."""
    return {
        'id': each.task_id,
        'task_name': each.task_name,
        'queue_name': each.queue_name,
        'execution': Execution.SHARED_ASYNC.value,
        'claimed_at': each.claimed_at,
    }


def _registered_isolated(seed: _Seed) -> dict[str, Execution]:
    return {each.task_name: Execution.ISOLATED for each in seed.dispatches}


async def _candidate_return_async(database: DualRunDatabase, seed: _Seed) -> int:
    async with _worker_parts(database) as parts:
        probe = _ClaimProbe(parts, seed.worker_id, _registered_isolated(seed))
        fitting = await probe._return_rows_of_another_class(
            [_claimed_row(each) for each in seed.dispatches]
        )
        return len(fitting)


def _candidate_return(database: DualRunDatabase, seed: _Seed) -> int:
    return asyncio.run(_candidate_return_async(database, seed))


def test_return_as_isolated_writes_what_the_per_row_return_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='per-row return as ISOLATED', act=_reference_return_as_isolated),
            OperationSide(name='horsies_release_claims', act=_candidate_return),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == (True, True, False, False, False, False)
    assert run.candidate.report == 0


# ---- The caller switch: the worker's callers on SQLAlchemy against the core ----

NOTIFY_WAIT_S: Final = 5.0
"""Bound of the wait for the sentinel wake after the release."""

_WOKEN_QUEUES: Final = ('default', 'reports')
"""The queues of the seed, whose ``task_queue_<queue>`` channels the pairs listen on."""

type _Wake = tuple[str, str]
"""One notification: (channel, payload)."""


def _wakes_after(listener: psycopg.Connection[Any], database: DualRunDatabase) -> tuple[_Wake, ...]:
    """Every wake that ``listener`` got before a sentinel this call sends, sorted.

    PostgreSQL delivers the notifications of one listener in commit order, so
    the wakes of the release come before the sentinel.
    """
    sentinel = f'sentinel:{uuid.uuid4()}'
    channel = f'task_queue_{_WOKEN_QUEUES[0]}'
    with psycopg.connect(database.url, autocommit=True) as sender:
        _ = sender.execute('SELECT pg_notify(%s, %s)', (channel, sentinel))
    wakes: list[_Wake] = []
    for notify in listener.notifies(timeout=NOTIFY_WAIT_S):
        if notify.payload == sentinel:
            return tuple(sorted(wakes))
        wakes.append((notify.channel, notify.payload))
    raise AssertionError(f'the sentinel wake did not arrive; wakes {wakes}')


def _listener(database: DualRunDatabase) -> psycopg.Connection[Any]:
    listener = psycopg.connect(database.url, autocommit=True)
    for queue in _WOKEN_QUEUES:
        _ = listener.execute(f'LISTEN task_queue_{queue}')
    return listener


@dataclass(frozen=True, slots=True)
class _ReleaseReport:
    """What a caller answered, and the wakes it sent."""

    outcomes: tuple[_RequeueOutcome, ...]
    wakes: tuple[_Wake, ...]


async def _requeue_each_async(
    database: DualRunDatabase, seed: _Seed, use_reference: bool
) -> tuple[_RequeueOutcome, ...]:
    async with _worker_parts(database) as parts:
        probe = _DispatchProbe(parts, seed.worker_id)
        outcomes: list[_RequeueOutcome] = []
        for each in seed.dispatches:
            match use_reference:
                case True:
                    outcome = await release_caller.requeue_claimed_task(
                        probe, each.task_id, 'dual-run', _dispatch_claim(each)
                    )
                case False:
                    outcome = await probe._requeue_claimed_task(
                        each.task_id, 'dual-run', _dispatch_claim(each)
                    )
            outcomes.append(outcome)
        return tuple(outcomes)


def _same_class_side(use_reference: bool) -> OperationSide[_Seed, _ReleaseReport]:
    def act(database: DualRunDatabase, seed: _Seed) -> _ReleaseReport:
        with _listener(database) as listener:
            outcomes = asyncio.run(_requeue_each_async(database, seed, use_reference))
            return _ReleaseReport(outcomes=outcomes, wakes=_wakes_after(listener, database))

    name = 'Python caller on SQLAlchemy' if use_reference else 'CoreHandle.release_claims_async'
    return OperationSide(name=name, act=act)


def _mismatched(seed: _Seed) -> list[tuple[dict[str, Any], RowClassMismatch]]:
    return [
        (
            _claimed_row(each),
            RowClassMismatch(
                row_class=Execution.SHARED_ASYNC,
                registered_class=Execution.ISOLATED,
            ),
        )
        for each in seed.dispatches
    ]


async def _return_all_async(
    database: DualRunDatabase, seed: _Seed, use_reference: bool
) -> None:
    async with _worker_parts(database) as parts:
        probe = _ClaimProbe(parts, seed.worker_id, _registered_isolated(seed))
        match use_reference:
            case True:
                await release_caller.return_rows_as_isolated(probe, _mismatched(seed))
            case False:
                await probe._return_rows_as_isolated(_mismatched(seed))


def _isolated_side(use_reference: bool) -> OperationSide[_Seed, _ReleaseReport]:
    def act(database: DualRunDatabase, seed: _Seed) -> _ReleaseReport:
        with _listener(database) as listener:
            asyncio.run(_return_all_async(database, seed, use_reference))
            return _ReleaseReport(outcomes=(), wakes=_wakes_after(listener, database))

    name = 'Python caller on SQLAlchemy' if use_reference else 'CoreHandle.release_claims_async'
    return OperationSide(name=name, act=act)


def test_the_core_release_same_class_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(dual_run(empty_database, _seed, _same_class_side(True), _same_class_side(False)))

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report.outcomes == (
        _RequeueOutcome.REQUEUED,
        _RequeueOutcome.REQUEUED,
        _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
        _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
        _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
        _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED,
    )
    assert run.reference.report.wakes == ()
    assert run.candidate.report == run.reference.report


def test_the_core_release_as_isolated_writes_and_wakes_what_the_python_caller_did(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(dual_run(empty_database, _seed, _isolated_side(True), _isolated_side(False)))

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report.wakes == (
        ('task_queue_default', 'class'),
        ('task_queue_reports', 'class'),
    )
    assert run.candidate.report == run.reference.report
