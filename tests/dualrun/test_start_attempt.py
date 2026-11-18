# pyright: reportPrivateUsage=false
"""Dual-run of the start switch: the RUNNING update and its miss path against ``horsies_start_attempt``.

Reference: ``tests/dualrun/reference/start.py``. Candidate: the isolated
child's ``_confirm_until_not_resumed`` on its core handle, with the orphan
self-heal off: the reference leaves an unlinked task CLAIMED, as the child
did before the parent's finalize ran (the self-heal is the pair of
``test_stop_before_start.py``). The seed holds one claimed row per start
case, plain and workflow; both sides must start, expire, stop or leave the
same rows, and report the same wire reason.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Final

import psycopg
import pytest

from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import child_pool, child_runner
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import start as reference
from tests.dualrun.seeds import (
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    insert_task,
    insert_workflow_task,
)
from tests.dualrun.selfcheck import SelfCheckStepFailed
from tests.fixtures.child_core import child_core_handle_for

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_CLAIMED_AGO: Final = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class _Dispatch:
    name: str
    task_id: str
    is_workflow_task: bool
    claimed_at: datetime


@dataclass(frozen=True, slots=True)
class _Seed:
    worker_id: str
    dispatches: tuple[_Dispatch, ...]


def _seed(connection: Connection) -> _Seed:
    worker_id = f'worker-{uuid.uuid4().hex}'

    def claimed(name: str) -> TaskSeed:
        return TaskSeed(
            task_name=name,
            status=SeedStatus.CLAIMED,
            worker_id=worker_id,
            claimed_ago=_CLAIMED_AGO,
        )

    plain_cases = (
        ('start.plain', claimed('start.plain'), timedelta(0)),
        ('start.older_generation', claimed('start.older_generation'), timedelta(seconds=1)),
        ('start.lease_expired', replace(claimed('start.lease_expired'), lease=timedelta(seconds=1)), timedelta(0)),
        (
            'start.deadline_passed',
            replace(claimed('start.deadline_passed'), good_until_in=timedelta(seconds=-1)),
            timedelta(0),
        ),
    )
    workflow_cases = (
        ('start.workflow', 'RUNNING', 'ENQUEUED'),
        ('start.workflow_retry', 'RUNNING', 'RUNNING'),
        ('start.workflow_paused', 'PAUSED', 'ENQUEUED'),
        ('start.workflow_cancelled', 'CANCELLED', 'ENQUEUED'),
        ('start.workflow_node_done', 'RUNNING', 'COMPLETED'),
    )
    dispatches: list[_Dispatch] = []
    for name, seed, older in plain_cases:
        task = insert_task(connection, seed)
        assert task.claimed_at is not None, 'a CLAIMED seed has a claim time'
        dispatches.append(_Dispatch(name, str(task.task_id), False, task.claimed_at - older))
    for name, workflow_status, node_status in workflow_cases:
        task = insert_workflow_task(
            connection, claimed(name), WorkflowLink(workflow_status, node_status)
        )
        assert task.claimed_at is not None, 'a CLAIMED seed has a claim time'
        dispatches.append(_Dispatch(name, str(task.task_id), True, task.claimed_at))
    return _Seed(worker_id=worker_id, dispatches=tuple(dispatches))


def _reference_start(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        return {
            each.name: reference.start(
                connection, each.task_id, seed.worker_id, each.is_workflow_task, each.claimed_at
            )
            for each in seed.dispatches
        }


def _wire_reason(result: object) -> str:
    match result:
        case None:
            return 'STARTED'
        case (False, str(), str() as reason):
            return reason
        case _:
            raise AssertionError(f'the start returned {result!r}')


def _candidate_start(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    with child_core_handle_for(database.url):
        return {
            each.name: _wire_reason(
                child_runner._confirm_until_not_resumed(
                    each.task_id,
                    seed.worker_id,
                    each.is_workflow_task,
                    each.claimed_at,
                    orphan_self_heal=False,
                )
            )
            for each in seed.dispatches
        }


def _ok(
    result: Result[OperationDualRun[dict[str, str], dict[str, str]], SelfCheckStepFailed],
) -> OperationDualRun[dict[str, str], dict[str, str]]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_start_writes_and_answers_what_the_running_update_did(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='RUNNING update and miss path', act=_reference_start),
            OperationSide(name='horsies_start_attempt', act=_candidate_start),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {
        'start.plain': 'STARTED',
        'start.older_generation': 'CLAIM_LOST',
        'start.lease_expired': 'CLAIM_LOST',
        'start.deadline_passed': 'TASK_EXPIRED',
        'start.workflow': 'STARTED',
        'start.workflow_retry': 'STARTED',
        'start.workflow_paused': 'WORKFLOW_STOPPED',
        'start.workflow_cancelled': 'WORKFLOW_STOPPED',
        'start.workflow_node_done': 'WORKFLOW_CHECK_FAILED',
    }
    assert run.candidate.report == run.reference.report


def test_the_candidate_starts_on_its_own_database_while_another_handle_is_in_place(
    empty_database: MakeDatabase,
) -> None:
    """The process state that integration tests leave (a child core handle
    of another database) does not move the candidate's statements to that
    database: ``child_core_handle_for`` puts its own handle in place."""
    other = empty_database()

    with child_core_handle_for(other.url) as in_place:
        run = _ok(
            dual_run(
                empty_database,
                _seed,
                OperationSide(name='RUNNING update and miss path', act=_reference_start),
                OperationSide(name='horsies_start_attempt', act=_candidate_start),
                SeedCoverage.HISTORY_AND_BEATS,
            )
        )
        after = child_pool._get_child_core_handle()

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.candidate.report == run.reference.report
    assert after is in_place, 'the handle in place before the pair is back'
