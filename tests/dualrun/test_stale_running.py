"""Dual-run of the stale RUNNING recovery switch: the scan and per-task transaction against the two functions.

Reference: ``tests/dualrun/reference/stale_running.py``. Candidate:
``PostgresBroker.mark_stale_tasks_as_failed``. The seed holds one RUNNING
row per staleness case, crash retries (scheduled, past the deadline, spent),
a workflow task, and rows the recovery must not touch. Both sides must write
the same attempts, retries and history rows.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from typing import Final

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import stale_running as reference
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
    with psycopg.connect(database.url, autocommit=True) as connection:
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
