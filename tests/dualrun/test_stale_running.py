"""Dual-runs of the stale RUNNING recovery.

The seed holds one RUNNING row per staleness case, crash retries (scheduled,
past the deadline, spent), a workflow task, and rows the recovery must not
touch. Two pairs, with the same seed:

1. The SQL redesign: the scan and per-task transaction against the two
   functions. Reference: ``tests/dualrun/reference/stale_running.py``.
   Candidate: ``PostgresBroker.mark_stale_tasks_as_failed``. Both sides must
   write the same attempts, retries and history rows.
2. The caller switch: the Python caller of the two functions against the
   core. Reference: ``tests/dualrun/reference/stale_running_caller.py``, the
   method's body before the switch (the crash result built in Python).
   Candidate: the method as shipped, which runs the pass in the core with
   the crash result that the core builds. The snapshots (the stored results
   compared as JSON) and the results must be equal.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Final

import psycopg
import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import install_as_a_client, open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import stale_running as reference
from tests.dualrun.reference import stale_running_caller
from tests.dualrun.reference.interim_terminal_writes import (
    install_interim_terminal_writes,
)
from tests.dualrun.seeds import (
    BeatRole,
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    insert_beat,
    insert_task,
    insert_workflow_task,
)
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

STALE_AFTER_MS: Final = 30_000
FINALIZING_STALE_AFTER_MS: Final = 30_000
_OLD: Final = timedelta(minutes=10)
_OLD_BEAT: Final = timedelta(minutes=5)
_FRESH: Final = timedelta(seconds=5)
_CRASH_RETRY_OPTIONS: Final = json.dumps(
    {
        'retry_policy': {
            'max_retries': 2,
            'intervals': [60, 60],
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['WORKER_CRASHED'],
        },
    },
)


def _running(name: str, worker_id: str) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.RUNNING,
        worker_id=worker_id,
        claimed_ago=_OLD,
        started_ago=_OLD,
    )


def _seed(connection: Connection) -> None:
    worker_id = f'worker-{uuid.uuid4().hex}'
    _ = insert_task(connection, _running('stale.no_beat', worker_id))
    old_beat = insert_task(connection, _running('stale.old_beat', worker_id))
    insert_beat(connection, old_beat.task_id, worker_id, BeatRole.RUNNER, _OLD_BEAT)
    fresh_beat = insert_task(connection, _running('stale.fresh_beat', worker_id))
    insert_beat(connection, fresh_beat.task_id, worker_id, BeatRole.RUNNER, _FRESH)
    claimer_beat = insert_task(connection, _running('stale.fresh_claimer_beat_only', worker_id))
    insert_beat(connection, claimer_beat.task_id, worker_id, BeatRole.CLAIMER, _FRESH)
    _ = insert_task(
        connection, replace(_running('stale.finalizing_fresh', worker_id), finalizing_ago=_FRESH)
    )
    _ = insert_task(
        connection, replace(_running('stale.finalizing_old', worker_id), finalizing_ago=_OLD)
    )
    _ = insert_task(
        connection,
        replace(
            _running('stale.retry', worker_id),
            max_retries=2,
            task_options=_CRASH_RETRY_OPTIONS,
        ),
    )
    _ = insert_task(
        connection,
        replace(
            _running('stale.retry_past_deadline', worker_id),
            max_retries=2,
            task_options=_CRASH_RETRY_OPTIONS,
            good_until_in=timedelta(seconds=30),
        ),
    )
    _ = insert_task(
        connection,
        replace(
            _running('stale.retry_spent', worker_id),
            retry_count=2,
            max_retries=2,
            task_options=_CRASH_RETRY_OPTIONS,
        ),
    )
    _ = insert_workflow_task(
        connection,
        _running('stale.workflow', worker_id),
        WorkflowLink(workflow_status='RUNNING', node_status='RUNNING'),
    )
    _ = insert_task(
        connection,
        TaskSeed(
            task_name='stale.claimed',
            status=SeedStatus.CLAIMED,
            worker_id=worker_id,
            claimed_ago=_OLD,
        ),
    )
    _ = insert_task(connection, TaskSeed(task_name='stale.pending', status=SeedStatus.PENDING))


def _reference_pass(database: DualRunDatabase, _seed: None) -> int:
    install_as_a_client(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        install_interim_terminal_writes(connection)
        return reference.mark_stale_tasks_as_failed(
            connection, STALE_AFTER_MS, FINALIZING_STALE_AFTER_MS
        )


async def _candidate_pass_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        match await broker.mark_stale_tasks_as_failed(
            stale_threshold_ms=STALE_AFTER_MS,
            finalizing_stale_threshold_ms=FINALIZING_STALE_AFTER_MS,
        ):
            case Ok(recovered):
                return recovered
            case Err(failed):
                raise AssertionError(f'mark_stale_tasks_as_failed failed: {failed}')


def _candidate_pass(database: DualRunDatabase, _seed: None) -> int:
    return asyncio.run(_candidate_pass_async(database))


def _ok(
    result: Result[OperationDualRun[int, int], SelfCheckStepFailed],
) -> OperationDualRun[int, int]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_stale_recovery_writes_what_the_per_task_transaction_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='scan and per-task transaction', act=_reference_pass),
            OperationSide(name='horsies_finish_stale_attempt', act=_candidate_pass),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == 8
    assert run.candidate.report == run.reference.report


# ---- The caller switch: the Python caller against the core ----

REFUSED_STALE_AFTER_MS: Final = -1
"""A threshold that ``horsies_stale_running_scan`` and the core both refuse."""


@dataclass(frozen=True, slots=True)
class _PassRecovered:
    recovered: int


@dataclass(frozen=True, slots=True)
class _PassFailed:
    code: BrokerErrorCode
    retryable: bool


type _PassReport = _PassRecovered | _PassFailed

type _StaleCall = Callable[[PostgresBroker, int, int], Awaitable[BrokerResult[int]]]


async def _python_caller(
    broker: PostgresBroker, stale_after_ms: int, finalizing_stale_after_ms: int
) -> BrokerResult[int]:
    return await stale_running_caller.mark_stale_tasks_as_failed(
        broker, stale_after_ms, finalizing_stale_after_ms
    )


async def _core_caller(
    broker: PostgresBroker, stale_after_ms: int, finalizing_stale_after_ms: int
) -> BrokerResult[int]:
    return await broker.mark_stale_tasks_as_failed(
        stale_threshold_ms=stale_after_ms,
        finalizing_stale_threshold_ms=finalizing_stale_after_ms,
    )


async def _caller_pass_async(
    database: DualRunDatabase, call: _StaleCall, stale_after_ms: int
) -> _PassReport:
    async with open_broker(database) as broker:
        match await call(broker, stale_after_ms, FINALIZING_STALE_AFTER_MS):
            case Ok(recovered):
                return _PassRecovered(recovered=recovered)
            case Err(failed):
                return _PassFailed(code=failed.code, retryable=failed.retryable)


def _caller_side(
    name: str, call: _StaleCall, stale_after_ms: int
) -> OperationSide[None, _PassReport]:
    def act(database: DualRunDatabase, _seed: None) -> _PassReport:
        return asyncio.run(_caller_pass_async(database, call, stale_after_ms))

    return OperationSide(name=name, act=act)


def _caller_pair(
    empty_database: MakeDatabase, stale_after_ms: int
) -> OperationDualRun[_PassReport, _PassReport]:
    match dual_run(
        empty_database,
        _seed,
        _caller_side('Python caller on SQLAlchemy', _python_caller, stale_after_ms),
        _caller_side('CoreHandle.mark_stale_running_async', _core_caller, stale_after_ms),
        SeedCoverage.HISTORY_AND_BEATS,
    ):
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_recovery_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _caller_pair(empty_database, STALE_AFTER_MS)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _PassRecovered(recovered=8)
    assert run.candidate.report == run.reference.report


def test_a_refused_threshold_fails_alike_and_writes_nothing(
    empty_database: MakeDatabase,
) -> None:
    run = _caller_pair(empty_database, REFUSED_STALE_AFTER_MS)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _PassFailed(
        code=BrokerErrorCode.CLEANUP_FAILED, retryable=False,
    )
    assert run.candidate.report == run.reference.report
