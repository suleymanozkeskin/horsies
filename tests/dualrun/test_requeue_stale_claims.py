"""Dual-run of the stale CLAIMED requeue switch: the one-pass UPDATE against ``horsies_requeue_stale_claims``.

Reference: ``tests/dualrun/reference/requeue.py``. Candidate:
``PostgresBroker.requeue_stale_claimed``, which repeats the bounded function
until a batch is short.

The two rules agree on every row but one kind: a fresh claim whose earlier
claim generation left claimer beats older than the threshold. The old
statement read only the latest beat and requeued it; the function also
requires the claim itself to be older than the threshold, and keeps it.
``test_a_fresh_claim_with_old_beats_is_kept`` pins that difference.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Final

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import ColumnDifference, render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import requeue as reference
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
