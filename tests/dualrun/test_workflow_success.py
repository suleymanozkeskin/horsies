# pyright: reportPrivateUsage=false
"""Dual-run of the workflow success caller switch.

Reference: ``tests/dualrun/reference/workflow_success_caller.py``, the body of
``FinalizeMixin._finish_workflow_success`` before the switch (one
``horsies_finish_workflow_success`` statement on the worker's SQLAlchemy
engine). Candidate: the method as shipped, which calls
``CoreHandle.finish_workflow_success_async``. The seed holds RUNNING
workflow tasks: one this claim holds, one of an older claim generation, and
one of another worker. Both sides must write the same attempt, history and
phase-2 rows, and route phase 2 the same way.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final, Literal

import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies._core import CoreHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.commands import CompleteLockedTask
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskResult
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.finalize import FinalizeMixin, _NodeFinishOwed, _Phase1Input
from horsies.core.worker.runtime import _FinalizeError
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import workflow_success_caller
from tests.dualrun.seeds import (
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    insert_workflow_task,
)
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_AGO: Final = timedelta(minutes=5)
_OTHER_GENERATION: Final = timedelta(seconds=1)

type _Implementation = Literal['shipped', 'python']
"""``shipped``: the method as shipped, on the core. ``python``: the method
body before the switch, on SQLAlchemy."""


@dataclass(frozen=True, slots=True)
class _Case:
    name: str
    task_id: str
    claim: DispatchClaim


@dataclass(frozen=True, slots=True)
class _Seed:
    worker_id: str
    cases: tuple[_Case, ...]


def _ok_result_json() -> str:
    match dumps_json(encode_task_result(TaskResult(ok=None), Any)):
        case Ok(value):
            return value
        case Err(error):
            raise AssertionError(error)


def _seed(connection: Connection) -> _Seed:
    worker_id = f'worker-{uuid.uuid4().hex}'
    other_worker = f'worker-{uuid.uuid4().hex}'

    def running(name: str, owner: str) -> TaskSeed:
        return TaskSeed(
            task_name=name,
            status=SeedStatus.RUNNING,
            worker_id=owner,
            claimed_ago=_AGO,
            started_ago=_AGO,
        )

    def case(seed: TaskSeed, generation_offset: timedelta = timedelta(0)) -> _Case:
        task = insert_workflow_task(connection, seed, WorkflowLink('RUNNING', 'RUNNING'))
        assert task.claimed_at is not None, 'a RUNNING seed has a claim time'
        return _Case(
            name=seed.task_name,
            task_id=str(task.task_id),
            claim=DispatchClaim(
                claimed_at=task.claimed_at - generation_offset,
                retry_count=seed.retry_count,
                max_retries=seed.max_retries,
                task_options=seed.task_options,
            ),
        )

    return _Seed(
        worker_id=worker_id,
        cases=(
            case(running('success.held', worker_id)),
            case(running('success.older_generation', worker_id), _OTHER_GENERATION),
            case(running('success.other_worker', other_worker)),
        ),
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


def _route(result: Result[_NodeFinishOwed | None, _FinalizeError]) -> str:
    """How phase 1 routed phase 2: owed with its form, nothing owed, or the error stage."""
    match result:
        case Ok(owed):
            match owed:
                case _NodeFinishOwed(facts=facts):
                    return f'PHASE_2:{facts}'
                case None:
                    return 'NOTHING_OWED'
        case Err(error):
            return f'ERROR:{error.stage}'


async def _finish(
    probe: _Probe, case: _Case, worker_id: str, implementation: _Implementation,
) -> Result[_NodeFinishOwed | None, _FinalizeError]:
    completion = CompleteLockedTask(
        task_id=case.task_id,
        fence=OwnedClaim(worker_id=worker_id, claimed_at=case.claim.claimed_at),
        result_json=_ok_result_json(),
    )
    child = _Phase1Input(ok=True, result_json_str=completion.result_json, failed_reason=None)
    match implementation:
        case 'shipped':
            return await probe._finish_workflow_success(completion, child=child)
        case 'python':
            return await workflow_success_caller.finish_workflow_success(
                probe, completion, child=child,
            )


async def _side_async(
    database: DualRunDatabase, seed: _Seed, implementation: _Implementation,
) -> dict[str, str]:
    engine = create_async_engine(sqlalchemy_url(database))
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        probe = _Probe(
            async_sessionmaker(engine, expire_on_commit=False), seed.worker_id, broker,
        )
        return {
            case.name: _route(await _finish(probe, case, seed.worker_id, implementation))
            for case in seed.cases
        }
    finally:
        _ = await broker.close_async()
        await engine.dispose()


def _side(implementation: _Implementation) -> OperationSide[_Seed, dict[str, str]]:
    def act(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
        return asyncio.run(_side_async(database, seed, implementation))

    name = (
        'Python caller on SQLAlchemy'
        if implementation == 'python'
        else 'CoreHandle.finish_workflow_success_async'
    )
    return OperationSide(name=name, act=act)


def _ok(
    result: Result[OperationDualRun[dict[str, str], dict[str, str]], SelfCheckStepFailed],
) -> OperationDualRun[dict[str, str], dict[str, str]]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_workflow_success_writes_what_the_python_caller_wrote(
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
    assert run.reference.report['success.held'].startswith('PHASE_2:')
    assert run.reference.report['success.older_generation'] == 'NOTHING_OWED'
    assert run.reference.report['success.other_worker'] == 'NOTHING_OWED'
