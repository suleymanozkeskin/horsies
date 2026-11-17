# pyright: reportPrivateUsage=false
"""Dual-run of the finalizing marker: the fenced update against ``horsies_mark_finalizing``.

Reference: ``tests/dualrun/reference/finalizing.py``. Candidate: the
child's ``mark_run_finalizing`` through the core handle of the child
(``CoreChildHandle.mark_finalizing``). The seed holds
one row per marker case; both sides must mark the same rows, leave the
others as they were, and report the same outcome. The candidate's outcome is
what its log records say: the debug record of a changed claim is
``NOT_HELD``, no record is ``MARKED``, and a warning fails the test.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Final

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import child_runner
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import finalizing as reference
from tests.dualrun.seeds import SeedStatus, TaskSeed, insert_task
from tests.dualrun.selfcheck import SelfCheckStepFailed
from tests.fixtures.child_core import child_core_handle_for

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_CLAIMED_AGO: Final = timedelta(seconds=5)
_STARTED_AGO: Final = timedelta(seconds=4)


@dataclass(frozen=True, slots=True)
class _Mark:
    name: str
    task_id: str
    worker_id: str
    claimed_at: datetime


def _seed(connection: Connection) -> tuple[_Mark, ...]:
    worker_id = f'worker-{uuid.uuid4().hex}'
    running = TaskSeed(
        task_name='marker.held',
        status=SeedStatus.RUNNING,
        worker_id=worker_id,
        claimed_ago=_CLAIMED_AGO,
        started_ago=_STARTED_AGO,
    )
    claimed = TaskSeed(
        task_name='marker.claimed',
        status=SeedStatus.CLAIMED,
        worker_id=worker_id,
        claimed_ago=_CLAIMED_AGO,
    )
    cases = (
        ('marker.held', running, worker_id, timedelta(0)),
        (
            'marker.older_generation',
            replace(running, task_name='marker.older_generation'),
            worker_id,
            timedelta(seconds=1),
        ),
        (
            'marker.other_worker',
            replace(running, task_name='marker.other_worker'),
            f'worker-{uuid.uuid4().hex}',
            timedelta(0),
        ),
        (
            'marker.refreshed',
            replace(
                running,
                task_name='marker.refreshed',
                finalizing_ago=timedelta(seconds=3),
            ),
            worker_id,
            timedelta(0),
        ),
        ('marker.claimed', claimed, worker_id, timedelta(0)),
    )
    marks: list[_Mark] = []
    for name, seed, marking_worker, older in cases:
        task = insert_task(connection, seed)
        assert task.claimed_at is not None, 'a CLAIMED or RUNNING seed has a claim time'
        marks.append(
            _Mark(name, str(task.task_id), marking_worker, task.claimed_at - older)
        )
    first = marks[0]
    marks.append(
        _Mark('marker.absent', str(uuid.uuid4()), first.worker_id, first.claimed_at)
    )
    return tuple(marks)


def _reference_mark(
    database: DualRunDatabase, marks: tuple[_Mark, ...]
) -> dict[str, str]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        return {
            each.name: reference.mark(
                connection, each.task_id, each.worker_id, each.claimed_at
            )
            for each in marks
        }


def _logged_outcome(records: list[logging.LogRecord], task_id: str) -> str:
    messages = [record.getMessage() for record in records]
    failed = f'Failed to mark task {task_id} finalizing'
    skipped = f'Finalizing marker for task {task_id} skipped: the claim changed'
    match [message for message in messages if failed in message], skipped in messages:
        case [[], True]:
            return 'NOT_HELD'
        case [[], False]:
            return 'MARKED'
        case failures, _:
            raise AssertionError(f'the marker of task {task_id} failed: {failures}')


def _candidate_mark(
    caplog: pytest.LogCaptureFixture,
) -> Callable[[DualRunDatabase, tuple[_Mark, ...]], dict[str, str]]:
    def act(database: DualRunDatabase, marks: tuple[_Mark, ...]) -> dict[str, str]:
        with child_core_handle_for(database.url):
            report: dict[str, str] = {}
            for each in marks:
                caplog.clear()
                child_runner.mark_run_finalizing(
                    uuid.UUID(each.task_id), each.worker_id, each.claimed_at
                )
                report[each.name] = _logged_outcome(caplog.records, each.task_id)
            return report

    return act


def _ok(
    result: Result[
        OperationDualRun[dict[str, str], dict[str, str]], SelfCheckStepFailed
    ],
) -> OperationDualRun[dict[str, str], dict[str, str]]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_marker_writes_and_answers_what_the_fenced_update_did(
    empty_database: MakeDatabase,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger='horsies.worker')

    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='fenced finalizing update', act=_reference_mark),
            OperationSide(name='horsies_mark_finalizing', act=_candidate_mark(caplog)),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {
        'marker.held': 'MARKED',
        'marker.older_generation': 'NOT_HELD',
        'marker.other_worker': 'NOT_HELD',
        'marker.refreshed': 'MARKED',
        'marker.claimed': 'NOT_HELD',
        'marker.absent': 'NOT_HELD',
    }
    assert run.candidate.report == run.reference.report
