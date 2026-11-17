"""Dual-run of the claim loop's switch: the claim pass and the soft-cap start count.

Reference: ``tests/dualrun/reference/claim_caller.py``, the bodies of
``ClaimMixin._claim_rows`` and ``ClaimMixin._count_queue_starts`` before the
switch (``HORSIES_CLAIM_SQL`` and ``COUNT_QUEUE_STARTS_SQL`` on the claim
loop's held connection). Candidate: the two methods, which call
``CoreHandle.claim_pass_async`` and ``count_queue_starts_async``. A claim
driver (``_Driver``) runs them with a worker config and no slot pool.

- The claim pass: ISOLATED rows of two queues, the second capped and holding
  a RUNNING row of another worker, and a SHARED_ASYNC row that the driver
  does not serve. The snapshots and the reports must be equal.
- The start count: a capped queue with a RUNNING row of another worker and
  CLAIMED rows of the driver. With the claim generations of the started
  rows, both count the same. A started row whose claim generation differs
  from the row's is the one known difference (#1330): Python counts any
  CLAIMED row of the worker with a started id, the core counts it only at
  its claim generation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

import pytest

from horsies._core import CoreHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import Execution
from horsies.core.utils.db import PooledStatements
from horsies.core.worker.claiming import ClaimMixin, ClaimRows
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.queue_cap_hold import QueueCapHold, StartedClaim, start_check_caps
from horsies.core.types.result import Err, Ok, Result
from tests.broker_resources_support import current_resources
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import claim_caller
from tests.dualrun.seeds import SeedCoverage, SeedStatus, TaskSeed, insert_task
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

DRIVER_WORKER: Final = 'claim-driver'
OTHER_WORKER: Final = 'other-worker'
QUEUE_A: Final = 'claim.a'
QUEUE_B: Final = 'claim.b'


class _Driver(ClaimMixin):
    """The claim mixin with a worker config and no slot pool."""

    def __init__(self, broker: PostgresBroker, cfg: WorkerConfig) -> None:
        self.sf = current_resources(broker).session_factory
        self._claim_runner = PooledStatements(self.sf)
        self.cfg = cfg
        self.worker_instance_id = DRIVER_WORKER
        self._registered_classes: Mapping[str, Execution] = {}
        self._queue_cap_hold = QueueCapHold(
            start_check_caps(cfg.prefetch_buffer, cfg.queue_max_concurrency),
        )
        self._broker = broker

    @property
    def core_handle(self) -> CoreHandle:
        return self._broker._core_handle  # pyright: ignore[reportPrivateUsage]

    def _claim_slot_count(self) -> int:
        return self.cfg.processes

    def _shared_async_seats_in_service(self) -> int:
        return 0

    def _shared_thread_seats_in_service(self) -> int:
        return 0


def _config(*, prefetch_buffer: int, caps: Mapping[str, int]) -> WorkerConfig:
    return WorkerConfig(
        dsn='unused',
        psycopg_dsn='unused',
        queues=[QUEUE_A, QUEUE_B],
        processes=4,
        queue_priorities={QUEUE_A: 1, QUEUE_B: 2},
        queue_max_concurrency=dict(caps),
        prefetch_buffer=prefetch_buffer,
        claim_lease_ms=60_000 if prefetch_buffer > 0 else None,
    )


def _pending(name: str, queue: str, execution: str = 'ISOLATED') -> TaskSeed:
    return TaskSeed(task_name=name, status=SeedStatus.PENDING, queue_name=queue, execution=execution)


def _running_elsewhere(name: str, queue: str) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.RUNNING,
        worker_id=OTHER_WORKER,
        claimed_ago=timedelta(minutes=2),
        started_ago=timedelta(minutes=1),
        queue_name=queue,
        execution='ISOLATED',
    )


def _claimed_by_driver(name: str, queue: str) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.CLAIMED,
        worker_id=DRIVER_WORKER,
        claimed_ago=timedelta(seconds=10),
        lease=timedelta(minutes=5),
        queue_name=queue,
        execution='ISOLATED',
    )


# ---- The claim pass ----


def _seed_claim(connection: Connection) -> None:
    for name in ('claim.a1', 'claim.a2', 'claim.a3'):
        _ = insert_task(connection, _pending(name, QUEUE_A))
    _ = insert_task(connection, _pending('claim.a_shared', QUEUE_A, 'SHARED_ASYNC'))
    for name in ('claim.b1', 'claim.b2'):
        _ = insert_task(connection, _pending(name, QUEUE_B))
    _ = insert_task(connection, _running_elsewhere('claim.b_running', QUEUE_B))


@dataclass(frozen=True, slots=True)
class _ClaimedRow:
    """The facts of one claimed row that both databases share."""

    task_name: str
    queue_name: str
    execution: str
    retry_count: int
    max_retries: int
    is_workflow_task: bool
    task_options: str | None
    input: str
    claimed_at_known: bool


@dataclass(frozen=True, slots=True)
class _ClaimReport:
    claimed: tuple[_ClaimedRow, ...]
    """In dispatch order."""
    short_of_plan: int


def _claim_report(rows: ClaimRows, names: Mapping[str, str]) -> _ClaimReport:
    return _ClaimReport(
        claimed=tuple(
            _ClaimedRow(
                task_name=names.get(str(row['id']), str(row['task_name'])),
                queue_name=str(row['queue_name']),
                execution=str(row['execution']),
                retry_count=int(row['retry_count']),
                max_retries=int(row['max_retries']),
                is_workflow_task=bool(row['is_workflow_task']),
                task_options=row['task_options'],
                input=str(row['input']),
                claimed_at_known=isinstance(row['claimed_at'], datetime),
            )
            for row in rows.tasks
        ),
        short_of_plan=rows.short_of_plan,
    )


type _ClaimCall = Callable[[_Driver], Awaitable[ClaimRows]]


def _claim_side(name: str, call: _ClaimCall) -> OperationSide[None, _ClaimReport]:
    async def claim(database: DualRunDatabase) -> _ClaimReport:
        async with open_broker(database) as broker:
            driver = _Driver(broker, _config(prefetch_buffer=0, caps={QUEUE_B: 2}))
            return _claim_report(await call(driver), {})

    def act(database: DualRunDatabase, _seed: None) -> _ClaimReport:
        return asyncio.run(claim(database))

    return OperationSide(name=name, act=act)


async def _core_claim(driver: _Driver) -> ClaimRows:
    return await driver._claim_rows()  # pyright: ignore[reportPrivateUsage]


def _ok[R](
    result: Result[OperationDualRun[R, R], SelfCheckStepFailed],
) -> OperationDualRun[R, R]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_claim_pass_takes_what_the_python_pass_took(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed_claim,
            _claim_side('Python claim statement', claim_caller.claim_rows),
            _claim_side('CoreHandle.claim_pass_async', _core_claim),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    claimed_names = [row.task_name for row in run.reference.report.claimed]
    assert claimed_names, 'the reference pass claimed nothing'
    assert 'claim.a_shared' not in claimed_names
    assert 'claim.b_running' not in claimed_names
    assert run.candidate.report == run.reference.report


# ---- The soft-cap start count ----


@dataclass(frozen=True, slots=True)
class _StartSeed:
    started: tuple[StartedClaim, ...]


def _seed_starts(stale_generation: bool) -> Callable[[Connection], _StartSeed]:
    def seed(connection: Connection) -> _StartSeed:
        _ = insert_task(connection, _running_elsewhere('start.running', QUEUE_B))
        started = insert_task(connection, _claimed_by_driver('start.started', QUEUE_B))
        _ = insert_task(connection, _claimed_by_driver('start.held', QUEUE_B))
        assert started.claimed_at is not None, 'a CLAIMED seed has a claim time'
        claimed_at = started.claimed_at - timedelta(seconds=1) if stale_generation else started.claimed_at
        return _StartSeed(started=(StartedClaim(task_id=str(started.task_id), claimed_at=claimed_at),))

    return seed


type _CountCall = Callable[[_Driver, Sequence[str], Sequence[StartedClaim]], Awaitable[dict[str, int]]]


def _count_side(name: str, call: _CountCall) -> OperationSide[_StartSeed, dict[str, int]]:
    async def count(database: DualRunDatabase, seed: _StartSeed) -> dict[str, int]:
        async with open_broker(database) as broker:
            driver = _Driver(broker, _config(prefetch_buffer=1, caps={QUEUE_B: 3}))
            return await call(driver, [QUEUE_B], seed.started)

    def act(database: DualRunDatabase, seed: _StartSeed) -> dict[str, int]:
        return asyncio.run(count(database, seed))

    return OperationSide(name=name, act=act)


async def _core_count(
    driver: _Driver, queues: Sequence[str], started: Sequence[StartedClaim],
) -> dict[str, int]:
    return await driver._count_queue_starts(queues, started)  # pyright: ignore[reportPrivateUsage]


def _count_pair(
    empty_database: MakeDatabase, stale_generation: bool,
) -> OperationDualRun[dict[str, int], dict[str, int]]:
    return _ok(
        dual_run(
            empty_database,
            _seed_starts(stale_generation),
            _count_side('Python count statement', claim_caller.count_queue_starts),
            _count_side('CoreHandle.count_queue_starts_async', _core_count),
        )
    )


def test_the_core_start_count_counts_what_the_python_count_counted(
    empty_database: MakeDatabase,
) -> None:
    run = _count_pair(empty_database, stale_generation=False)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {QUEUE_B: 2}
    assert run.candidate.report == run.reference.report


def test_a_started_row_at_another_claim_generation_counts_only_in_python(
    empty_database: MakeDatabase,
) -> None:
    run = _count_pair(empty_database, stale_generation=True)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {QUEUE_B: 2}
    assert run.candidate.report == {QUEUE_B: 1}

