"""Dual-runs of the stale CLAIMED requeue.

Two pairs, with the same seeds:

1. The SQL redesign: the one-pass UPDATE against ``horsies_requeue_stale_claims``.
   Reference: ``tests/dualrun/reference/requeue.py``. Candidate:
   ``PostgresBroker.requeue_stale_claimed``, which runs the bounded function
   in batches until one is short. The two rules agree on every row but one
   kind: a fresh claim whose earlier claim generation left claimer beats
   older than the threshold. The old statement read only the latest beat and
   requeued it; the function also requires the claim itself to be older than
   the threshold, and keeps it. ``test_a_fresh_claim_with_old_beats_is_kept``
   pins that difference.
2. The caller switch: the Python caller of the function against the core.
   Reference: ``tests/dualrun/reference/requeue_caller.py``, the method's body
   before the switch (the batches on the broker's SQLAlchemy engine).
   Candidate: ``PostgresBroker.requeue_stale_claimed``, which runs the pass in
   the core through the broker's ``CoreHandle``. The snapshots and the
   results must be equal.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

import psycopg
import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import install_as_a_client, open_broker
from tests.dualrun.compare import ColumnDifference, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import requeue as reference
from tests.dualrun.reference import requeue_caller
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
from tests.dualrun.values import ExactValue

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

STALE_AFTER_MS: Final = 30_000
_OLD: Final = timedelta(minutes=10)
_OLD_BEAT: Final = timedelta(minutes=5)
_FRESH: Final = timedelta(seconds=5)


def _claimed(name: str, worker_id: str, claimed_ago: timedelta) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.CLAIMED,
        worker_id=worker_id,
        claimed_ago=claimed_ago,
        lease=timedelta(hours=1),
    )


def _seed_agreed_cases(connection: Connection) -> None:
    """One row per case both rules decide the same way."""
    worker_id = f'worker-{uuid.uuid4().hex}'
    _ = insert_task(connection, _claimed('requeue.old_claim_no_beat', worker_id, _OLD))
    old_beat = insert_task(connection, _claimed('requeue.old_claim_old_beat', worker_id, _OLD))
    insert_beat(connection, old_beat.task_id, worker_id, BeatRole.CLAIMER, _OLD_BEAT)
    fresh_beat = insert_task(connection, _claimed('requeue.old_claim_fresh_beat', worker_id, _OLD))
    insert_beat(connection, fresh_beat.task_id, worker_id, BeatRole.CLAIMER, _OLD_BEAT)
    insert_beat(connection, fresh_beat.task_id, worker_id, BeatRole.CLAIMER, _FRESH)
    _ = insert_task(connection, _claimed('requeue.fresh_claim_no_beat', worker_id, _FRESH))
    runner_beat = insert_task(
        connection, _claimed('requeue.old_claim_old_runner_beat', worker_id, _OLD)
    )
    insert_beat(connection, runner_beat.task_id, worker_id, BeatRole.RUNNER, _FRESH)
    _ = insert_task(
        connection,
        TaskSeed(
            task_name='requeue.running',
            status=SeedStatus.RUNNING,
            worker_id=worker_id,
            claimed_ago=_OLD,
            started_ago=_OLD,
        ),
    )
    _ = insert_task(connection, TaskSeed(task_name='requeue.pending', status=SeedStatus.PENDING))
    _ = insert_workflow_task(
        connection,
        _claimed('requeue.workflow_linked', worker_id, _OLD),
        WorkflowLink(workflow_status='RUNNING', node_status='ENQUEUED'),
    )
    _ = insert_workflow_task(
        connection,
        _claimed('requeue.workflow_node_done', worker_id, _OLD),
        WorkflowLink(workflow_status='RUNNING', node_status='COMPLETED'),
    )
    _ = insert_workflow_task(
        connection,
        _claimed('requeue.workflow_no_node', worker_id, _OLD),
        WorkflowLink(workflow_status='RUNNING', node_status=None),
    )


def _seed_fresh_claim_old_beats(connection: Connection) -> None:
    """A claim taken now on a row whose earlier claim generation left old claimer beats."""
    worker_id = f'worker-{uuid.uuid4().hex}'
    task = insert_task(connection, _claimed('requeue.fresh_claim_old_beat', worker_id, _FRESH))
    insert_beat(connection, task.task_id, worker_id, BeatRole.CLAIMER, _OLD_BEAT)


def _reference_pass(database: DualRunDatabase, _seed: None) -> int:
    install_as_a_client(database)
    with psycopg.connect(database.url, autocommit=True) as connection:
        return reference.requeue_stale_claimed(connection, STALE_AFTER_MS)


async def _candidate_pass_async(database: DualRunDatabase) -> int:
    async with open_broker(database) as broker:
        match await broker.requeue_stale_claimed(stale_threshold_ms=STALE_AFTER_MS):
            case Ok(requeued):
                return requeued
            case Err(failed):
                raise AssertionError(f'requeue_stale_claimed failed: {failed}')


def _candidate_pass(database: DualRunDatabase, _seed: None) -> int:
    return asyncio.run(_candidate_pass_async(database))


_REFERENCE: Final = OperationSide(name='one-pass UPDATE', act=_reference_pass)
_CANDIDATE: Final = OperationSide(name='horsies_requeue_stale_claims', act=_candidate_pass)


def _ok(
    result: Result[OperationDualRun[int, int], SelfCheckStepFailed],
) -> OperationDualRun[int, int]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_requeue_writes_what_the_one_pass_update_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed_agreed_cases,
            _REFERENCE,
            _CANDIDATE,
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == 4
    assert run.candidate.report == run.reference.report


def test_a_fresh_claim_with_old_beats_is_kept(empty_database: MakeDatabase) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed_fresh_claim_old_beats,
            _REFERENCE,
            _CANDIDATE,
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert (run.reference.report, run.candidate.report) == (1, 0)
    differences = {
        (entry.table, entry.column)
        for entry in run.diff.entries
        if isinstance(entry, ColumnDifference)
    }
    assert len(differences) == len(run.diff.entries), render_diff(run.diff)
    assert differences == {
        ('horsies_tasks', 'status'),
        ('horsies_tasks', 'claimed_at'),
        ('horsies_tasks', 'claimed_by_worker_id'),
        ('horsies_tasks', 'claim_expires_at'),
    }, render_diff(run.diff)
    (status,) = (
        entry
        for entry in run.diff.entries
        if isinstance(entry, ColumnDifference) and entry.column == 'status'
    )
    assert (status.reference, status.candidate) == (
        ExactValue(text='PENDING'),
        ExactValue(text='CLAIMED'),
    )


# ---- The caller switch: the Python caller against the core ----

REFUSED_STALE_AFTER_MS: Final = -1
"""A threshold that ``horsies_requeue_stale_claims`` and the core both refuse."""


@dataclass(frozen=True, slots=True)
class _PassRequeued:
    """The pass ended; ``requeued`` rows went back to PENDING."""

    requeued: int


@dataclass(frozen=True, slots=True)
class _PassFailed:
    """The pass failed; the facts of the broker error that both callers share."""

    code: BrokerErrorCode
    retryable: bool
    counted_prefix: bool
    """The message starts with the count of the batches before the failure."""


type _PassReport = _PassRequeued | _PassFailed

_ZERO_REQUEUED_PREFIX: Final = 'requeue_stale_claimed failed after requeueing 0 task(s): '

type _RequeueCall = Callable[[PostgresBroker, int], Awaitable[BrokerResult[int]]]


def _report(result: BrokerResult[int]) -> _PassReport:
    match result:
        case Ok(requeued):
            return _PassRequeued(requeued=requeued)
        case Err(failed):
            return _PassFailed(
                code=failed.code,
                retryable=failed.retryable,
                counted_prefix=failed.message.startswith(_ZERO_REQUEUED_PREFIX),
            )


async def _caller_pass_async(
    database: DualRunDatabase, call: _RequeueCall, stale_after_ms: int
) -> _PassReport:
    async with open_broker(database) as broker:
        return _report(await call(broker, stale_after_ms))


def _python_caller(stale_after_ms: int) -> OperationSide[None, _PassReport]:
    def act(database: DualRunDatabase, _seed: None) -> _PassReport:
        return asyncio.run(
            _caller_pass_async(
                database, requeue_caller.requeue_stale_claimed, stale_after_ms
            )
        )

    return OperationSide(name='Python caller on SQLAlchemy', act=act)


def _core_caller(stale_after_ms: int) -> OperationSide[None, _PassReport]:
    async def call(broker: PostgresBroker, threshold_ms: int) -> BrokerResult[int]:
        return await broker.requeue_stale_claimed(stale_threshold_ms=threshold_ms)

    def act(database: DualRunDatabase, _seed: None) -> _PassReport:
        return asyncio.run(_caller_pass_async(database, call, stale_after_ms))

    return OperationSide(name='CoreHandle.requeue_stale_claimed_async', act=act)


def _caller_pair(
    empty_database: MakeDatabase,
    seed: Callable[[Connection], None],
    stale_after_ms: int,
) -> OperationDualRun[_PassReport, _PassReport]:
    match dual_run(
        empty_database,
        seed,
        _python_caller(stale_after_ms),
        _core_caller(stale_after_ms),
        SeedCoverage.HISTORY_AND_BEATS,
    ):
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_caller_requeues_what_the_python_caller_requeued(
    empty_database: MakeDatabase,
) -> None:
    run = _caller_pair(empty_database, _seed_agreed_cases, STALE_AFTER_MS)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _PassRequeued(requeued=4)
    assert run.candidate.report == run.reference.report


def test_the_core_caller_keeps_a_fresh_claim_with_old_beats(
    empty_database: MakeDatabase,
) -> None:
    run = _caller_pair(empty_database, _seed_fresh_claim_old_beats, STALE_AFTER_MS)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _PassRequeued(requeued=0)
    assert run.candidate.report == run.reference.report


def test_a_refused_threshold_fails_alike_and_writes_nothing(
    empty_database: MakeDatabase,
) -> None:
    run = _caller_pair(empty_database, _seed_agreed_cases, REFUSED_STALE_AFTER_MS)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _PassFailed(
        code=BrokerErrorCode.CLEANUP_FAILED,
        retryable=False,
        counted_prefix=True,
    )
    assert run.candidate.report == run.reference.report
