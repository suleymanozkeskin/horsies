# pyright: reportPrivateUsage=false
"""Dual-run of the release switch: the per-row updates against ``horsies_release_claims``.

Reference: ``tests/dualrun/reference/release.py``. Candidate: the worker's
``_requeue_claimed_task`` (placement SAME_CLASS) and
``_return_rows_of_another_class`` (placement ISOLATED). The seed holds one row
per fence case; both sides must leave the same rows PENDING and the others as
they were.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

import psycopg
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.claiming import ClaimMixin
from horsies.core.worker.dispatch import DispatchMixin
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.runtime import _RequeueOutcome
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import release as reference
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


def _session_factory(database: DualRunDatabase) -> tuple[Any, async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(
        database.url.replace('postgresql://', 'postgresql+psycopg://', 1)
    )
    return engine, async_sessionmaker(engine, expire_on_commit=False)


class _DispatchProbe(DispatchMixin):
    def __init__(self, sf: async_sessionmaker[AsyncSession], worker_id: str) -> None:
        self.sf = sf
        self.worker_instance_id = worker_id


class _ClaimProbe(ClaimMixin):
    def __init__(
        self,
        sf: async_sessionmaker[AsyncSession],
        worker_id: str,
        registered: Mapping[str, Execution],
    ) -> None:
        self.sf = sf
        self.worker_instance_id = worker_id
        self._registered_classes = registered


# ---- SAME_CLASS ----


def _reference_unclaim(database: DualRunDatabase, seed: _Seed) -> tuple[bool, ...]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        return tuple(
            reference.unclaim(connection, each.task_id, seed.worker_id, each.claimed_at)
            for each in seed.dispatches
        )


async def _candidate_requeue_async(
    database: DualRunDatabase, seed: _Seed
) -> tuple[_RequeueOutcome, ...]:
    engine, sf = _session_factory(database)
    try:
        probe = _DispatchProbe(sf, seed.worker_id)
        return tuple(
            [
                await probe._requeue_claimed_task(
                    each.task_id,
                    'dual-run',
                    DispatchClaim(
                        claimed_at=each.claimed_at,
                        retry_count=0,
                        max_retries=0,
                        task_options=None,
                    ),
                )
                for each in seed.dispatches
            ]
        )
    finally:
        await engine.dispose()


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


async def _candidate_return_async(database: DualRunDatabase, seed: _Seed) -> int:
    engine, sf = _session_factory(database)
    try:
        probe = _ClaimProbe(
            sf,
            seed.worker_id,
            {each.task_name: Execution.ISOLATED for each in seed.dispatches},
        )
        fitting = await probe._return_rows_of_another_class(
            [
                {
                    'id': each.task_id,
                    'task_name': each.task_name,
                    'queue_name': each.queue_name,
                    'execution': Execution.SHARED_ASYNC.value,
                    'claimed_at': each.claimed_at,
                }
                for each in seed.dispatches
            ]
        )
        return len(fitting)
    finally:
        await engine.dispose()


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
