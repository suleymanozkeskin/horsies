# pyright: reportPrivateUsage=false
"""Dual-run of the plain-success batch caller switch.

Reference: ``tests/dualrun/reference/success_batch_caller.py``, the body of
``FinalizeMixin._write_fused_batch`` before the switch (``apply_fused_batch``:
``horsies_complete_task_fused`` for one row, ``horsies_complete_tasks_fused``
for several, on the worker's SQLAlchemy engine). Candidate: the method as
shipped, which calls ``CoreHandle.complete_successes_async``
(``horsies_complete_tasks_fused`` for every batch). Two batches: one row, and
64 rows, among them rows of an older claim generation and of another
worker. Both sides must write the same attempt and history rows, and answer
the same outcome, with the same position, for each row.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal

import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies._core import CoreHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.commands import CompleteTaskFused
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import TerminalizationOutcome
from horsies.core.lifecycle.persistence import (
    FUSED_BATCH_MAX_ROWS,
    BatchWriteError,
    FusedCompletionBatch,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskResult
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.finalize import FinalizeMixin
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import success_batch_caller
from tests.dualrun.seeds import SeedCoverage, SeedStatus, TaskSeed, insert_task
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_AGO: Final = timedelta(minutes=5)
_OTHER_GENERATION: Final = timedelta(seconds=1)
_OLDER_GENERATION_ROWS: Final = 2
_OTHER_WORKER_ROWS: Final = 2

type _Implementation = Literal['shipped', 'python']
"""``shipped``: the method as shipped, on the core. ``python``: the method
body before the switch, on SQLAlchemy."""

type _Report = dict[str, list[tuple[str, int | None]]]
"""Per batch, the outcome class and the position of each row, in order."""


@dataclass(frozen=True, slots=True)
class _Seed:
    worker_id: str
    batches: dict[str, FusedCompletionBatch]


def _ok_result_json() -> str:
    match dumps_json(encode_task_result(TaskResult(ok=None), Any)):
        case Ok(value):
            return value
        case Err(error):
            raise AssertionError(error)


def _seed(connection: Connection) -> _Seed:
    worker_id = f'worker-{uuid.uuid4().hex}'
    other_worker = f'worker-{uuid.uuid4().hex}'
    result_json = _ok_result_json()

    def running(name: str, owner: str) -> tuple[str, datetime]:
        task = insert_task(
            connection,
            TaskSeed(
                task_name=name,
                status=SeedStatus.RUNNING,
                worker_id=owner,
                claimed_ago=_AGO,
                started_ago=_AGO,
            ),
        )
        assert task.claimed_at is not None, 'a RUNNING seed has a claim time'
        return str(task.task_id), task.claimed_at

    def command(task_id: str, claimed_at: datetime) -> CompleteTaskFused:
        return CompleteTaskFused(
            task_id=task_id,
            fence=OwnedClaim(worker_id=worker_id, claimed_at=claimed_at),
            result_json=result_json,
        )

    lone_id, lone_at = running('batch.lone', worker_id)
    held = FUSED_BATCH_MAX_ROWS - _OLDER_GENERATION_ROWS - _OTHER_WORKER_ROWS
    commands: list[CompleteTaskFused] = []
    for index in range(held):
        commands.append(command(*running(f'batch.held_{index:02d}', worker_id)))
    for index in range(_OLDER_GENERATION_ROWS):
        task_id, claimed_at = running(f'batch.older_generation_{index}', worker_id)
        commands.append(command(task_id, claimed_at - _OTHER_GENERATION))
    for index in range(_OTHER_WORKER_ROWS):
        commands.append(command(*running(f'batch.other_worker_{index}', other_worker)))
    return _Seed(
        worker_id=worker_id,
        batches={
            'one row': FusedCompletionBatch(commands=(command(lone_id, lone_at),)),
            f'{FUSED_BATCH_MAX_ROWS} rows': FusedCompletionBatch(commands=tuple(commands)),
        },
    )


class _Probe(FinalizeMixin):
    def __init__(
        self,
        sf: async_sessionmaker[AsyncSession],
        worker_id: str,
        broker: PostgresBroker,
    ) -> None:
        self.sf = sf
        self.worker_instance_id = worker_id
        self._broker = broker

    @property
    def core_handle(self) -> CoreHandle:
        return self._broker._core_handle


def _positions(
    written: Result[tuple[TerminalizationOutcome, ...], BatchWriteError],
) -> list[tuple[str, int | None]]:
    match written:
        case Ok(outcomes):
            return [(type(outcome).__name__, outcome.ordinality) for outcome in outcomes]
        case Err(error):
            raise AssertionError(f'the batch was not written: {error!r}')


async def _write(
    probe: _Probe, batch: FusedCompletionBatch, implementation: _Implementation,
) -> Result[tuple[TerminalizationOutcome, ...], BatchWriteError]:
    match implementation:
        case 'shipped':
            return await probe._write_fused_batch(batch)
        case 'python':
            return await success_batch_caller.write_fused_batch(probe, batch)


async def _side_async(
    database: DualRunDatabase, seed: _Seed, implementation: _Implementation,
) -> _Report:
    engine = create_async_engine(sqlalchemy_url(database))
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        probe = _Probe(
            async_sessionmaker(engine, expire_on_commit=False), seed.worker_id, broker,
        )
        return {
            name: _positions(await _write(probe, batch, implementation))
            for name, batch in seed.batches.items()
        }
    finally:
        _ = await broker.close_async()
        await engine.dispose()


def _side(implementation: _Implementation) -> OperationSide[_Seed, _Report]:
    def act(database: DualRunDatabase, seed: _Seed) -> _Report:
        return asyncio.run(_side_async(database, seed, implementation))

    name = (
        'Python caller on SQLAlchemy'
        if implementation == 'python'
        else 'CoreHandle.complete_successes_async'
    )
    return OperationSide(name=name, act=act)


def _ok(
    result: Result[OperationDualRun[_Report, _Report], SelfCheckStepFailed],
) -> OperationDualRun[_Report, _Report]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_batch_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            _side('python'),
            _side('shipped'),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.candidate.report == run.reference.report
    held = FUSED_BATCH_MAX_ROWS - _OLDER_GENERATION_ROWS - _OTHER_WORKER_ROWS
    assert run.reference.report == {
        'one row': [('Applied', None)],
        f'{FUSED_BATCH_MAX_ROWS} rows': [
            *(('Applied', position) for position in range(1, held + 1)),
            *(
                ('LostClaim', position)
                for position in range(held + 1, FUSED_BATCH_MAX_ROWS + 1)
            ),
        ],
    }
