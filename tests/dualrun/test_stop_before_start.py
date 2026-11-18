# pyright: reportPrivateUsage=false
"""Dual-run of the stop before start of a workflow task: the miss path of the
child and the orphan cancel of the worker's finalize against
``horsies_workflow_task_stop_before_start``.

Reference: ``tests/dualrun/reference/stop_before_start_caller.py``, the start
and its miss path before the switch (on a psycopg pool of its own), with the
cancel of the finalize folded in. Candidate: the isolated child's
``_confirm_until_not_resumed`` on its core handle, which sends the stop for
each workflow miss. The seed holds one claimed workflow task per case, with
the orphan self-heal of its run; both sides must stop, expire, cancel or
leave the same rows, and report the same stop reason.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import child_runner
from horsies.core.worker.child_runner import DeadlineBeforeStart
from horsies.core.worker.run_return import StoppedBeforeRunning
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import stop_before_start_caller as reference
from tests.dualrun.seeds import (
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    insert_workflow_task,
)
from tests.dualrun.selfcheck import SelfCheckStepFailed
from tests.fixtures.child_core import child_core_handle_for

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_CLAIMED_AGO: Final = timedelta(seconds=5)
_OTHER_GENERATION: Final = timedelta(seconds=1)


@dataclass(frozen=True, slots=True)
class _Dispatch:
    name: str
    task_id: str
    claimed_at: datetime
    orphan_self_heal: bool


@dataclass(frozen=True, slots=True)
class _Seed:
    worker_id: str
    dispatches: tuple[_Dispatch, ...]


@dataclass(frozen=True, slots=True)
class _Case:
    name: str
    link: WorkflowLink
    orphan_self_heal: bool
    good_until_in: timedelta | None = None
    older_generation: bool = False


_CASES: Final = (
    _Case('stop.paused', WorkflowLink('PAUSED', 'ENQUEUED'), orphan_self_heal=True),
    _Case('stop.paused_running_node', WorkflowLink('PAUSED', 'RUNNING'), orphan_self_heal=True),
    _Case('stop.paused_done_node', WorkflowLink('PAUSED', 'COMPLETED'), orphan_self_heal=True),
    _Case('stop.cancelled', WorkflowLink('CANCELLED', 'ENQUEUED'), orphan_self_heal=True),
    _Case('stop.cancelled_ready_node', WorkflowLink('CANCELLED', 'READY'), orphan_self_heal=True),
    _Case(
        'stop.paused_older_generation',
        WorkflowLink('PAUSED', 'ENQUEUED'),
        orphan_self_heal=True,
        older_generation=True,
    ),
    _Case(
        'stop.deadline_passed',
        WorkflowLink('PAUSED', 'ENQUEUED'),
        orphan_self_heal=True,
        good_until_in=timedelta(seconds=-1),
    ),
    _Case('stop.orphan_unlinked', WorkflowLink('RUNNING', None), orphan_self_heal=True),
    _Case('stop.orphan_done_node', WorkflowLink('RUNNING', 'COMPLETED'), orphan_self_heal=True),
    _Case('stop.orphan_self_heal_off', WorkflowLink('RUNNING', 'COMPLETED'), orphan_self_heal=False),
    _Case('stop.orphan_unlinked_self_heal_off', WorkflowLink('RUNNING', None), orphan_self_heal=False),
)


def _seed(connection: Connection) -> _Seed:
    worker_id = f'worker-{uuid.uuid4().hex}'
    dispatches: list[_Dispatch] = []
    for case in _CASES:
        task = insert_workflow_task(
            connection,
            TaskSeed(
                task_name=case.name,
                status=SeedStatus.CLAIMED,
                worker_id=worker_id,
                claimed_ago=_CLAIMED_AGO,
                good_until_in=case.good_until_in,
            ),
            case.link,
        )
        assert task.claimed_at is not None, 'a CLAIMED seed has a claim time'
        older = _OTHER_GENERATION if case.older_generation else timedelta(0)
        dispatches.append(
            _Dispatch(
                name=case.name,
                task_id=str(task.task_id),
                claimed_at=task.claimed_at - older,
                orphan_self_heal=case.orphan_self_heal,
            ),
        )
    return _Seed(worker_id=worker_id, dispatches=tuple(dispatches))


def _stop_reason(result: StoppedBeforeRunning | DeadlineBeforeStart | None) -> str:
    match result:
        case None:
            return 'STARTED'
        case StoppedBeforeRunning(reason=reason):
            return reason
        case DeadlineBeforeStart():
            raise AssertionError('a start with no deadline ended at its deadline')


def _reference_stop(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    """The reference runs on a pool of its own on the database of the core
    handle in place (``child_core_handle_for``)."""
    with child_core_handle_for(database.url):
        return {
            each.name: _stop_reason(
                reference._confirm_until_not_resumed(
                    each.task_id,
                    seed.worker_id,
                    each.claimed_at,
                    orphan_self_heal=each.orphan_self_heal,
                ),
            )
            for each in seed.dispatches
        }


def _candidate_stop(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    with child_core_handle_for(database.url):
        return {
            each.name: _stop_reason(
                child_runner._confirm_until_not_resumed(
                    each.task_id,
                    seed.worker_id,
                    each.claimed_at,
                    orphan_self_heal=each.orphan_self_heal,
                ),
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


def test_the_stop_before_start_writes_and_answers_what_the_miss_path_and_finalize_did(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='miss path and finalize orphan cancel', act=_reference_stop),
            OperationSide(name='horsies_workflow_task_stop_before_start', act=_candidate_stop),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {
        'stop.paused': 'WORKFLOW_STOPPED',
        'stop.paused_running_node': 'WORKFLOW_STOPPED',
        'stop.paused_done_node': 'WORKFLOW_STOPPED',
        'stop.cancelled': 'WORKFLOW_STOPPED',
        'stop.cancelled_ready_node': 'WORKFLOW_STOPPED',
        'stop.paused_older_generation': 'CLAIM_LOST',
        'stop.deadline_passed': 'TASK_EXPIRED',
        'stop.orphan_unlinked': 'ORPHAN_CANCELLED',
        'stop.orphan_done_node': 'ORPHAN_CANCELLED',
        'stop.orphan_self_heal_off': 'NODE_NOT_LINKED',
        'stop.orphan_unlinked_self_heal_off': 'NODE_NOT_LINKED',
    }
    assert run.candidate.report == run.reference.report

