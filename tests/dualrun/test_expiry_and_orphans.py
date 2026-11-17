# pyright: reportPrivateUsage=false
"""Dual-runs of the caller switch of the expiry and orphan audit writes.

Reference: ``tests/dualrun/reference/expiry_and_orphans_caller.py``, the
bodies before the switch, on SQLAlchemy. Candidate: the callers as shipped,
which call the core:

- ``PostgresBroker.expire_pending_tasks`` (``expire_pending_tasks_async``);
- ``PostgresBroker.audit_orphaned_workflow_tasks``
  (``audit_orphaned_workflow_tasks_async``), with the page refused at once
  while another session holds the orphan cursor.

The orphan self-heal moved from the worker's finalize to the child's stop
before start: its pair is ``tests/dualrun/test_stop_before_start.py``.

Each pair runs on two fresh databases with the same seed. The snapshots
(the stored results compared as JSON) and the reports must be equal.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, Literal

import psycopg
import pytest

from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationError,
    BrokerResult,
    broker_error_cause,
)
from horsies.core.models.recovery import OrphanTaskAuditReport
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import expiry_and_orphans_caller
from tests.dualrun.seeds import (
    SeedCoverage,
    SeedStatus,
    TaskSeed,
    WorkflowLink,
    insert_task,
    insert_workflow_task,
)
from tests.dualrun.selfcheck import SelfCheckStepFailed

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

type _Implementation = Literal['shipped', 'python']
"""``shipped``: the caller as shipped, on the core. ``python``: the body
before the switch, on SQLAlchemy."""

_PASSED: Final = timedelta(minutes=-1)
_AHEAD: Final = timedelta(hours=1)
_CLAIMED_AGO: Final = timedelta(seconds=30)
_LOCK_NOT_AVAILABLE: Final = '55P03'


def _ok[R, C](
    result: Result[OperationDualRun[R, C], SelfCheckStepFailed],
) -> OperationDualRun[R, C]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


# ---- Reports ----


@dataclass(frozen=True, slots=True)
class _Counted:
    """A pass or page that returned ``Ok``."""

    counts: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _Failed:
    """A pass or page that returned ``Err``: its code, the retry rule and the
    SQLSTATE of its cause, where the cause has one."""

    code: BrokerErrorCode
    retryable: bool
    sqlstate: str | None


type _Report = _Counted | _Failed


def _failed(error: BrokerOperationError) -> _Failed:
    match broker_error_cause(error):
        case NoCause():
            sqlstate = None
        case cause:
            original: object = getattr(cause, 'orig', cause)
            sqlstate = getattr(original, 'sqlstate', None)
    return _Failed(
        code=error.code,
        retryable=error.retryable,
        sqlstate=sqlstate if isinstance(sqlstate, str) else None,
    )


# ---- Pending expiry ----


def _pending(name: str, good_until_in: timedelta | None) -> TaskSeed:
    return TaskSeed(task_name=name, status=SeedStatus.PENDING, good_until_in=good_until_in)


def _expiry_seed(connection: Connection) -> None:
    for position in range(3):
        _ = insert_task(connection, _pending(f'expiry.due.{position}', good_until_in=_PASSED))
    _ = insert_task(connection, _pending('expiry.not_due', good_until_in=_AHEAD))
    _ = insert_task(connection, _pending('expiry.no_deadline', None))
    _ = insert_task(
        connection,
        TaskSeed(
            task_name='expiry.claimed',
            status=SeedStatus.CLAIMED,
            worker_id='worker-expiry',
            claimed_ago=_CLAIMED_AGO,
            good_until_in=_PASSED,
        ),
    )
    _ = insert_workflow_task(
        connection,
        _pending('expiry.workflow_due', good_until_in=_PASSED),
        WorkflowLink(workflow_status='RUNNING', node_status='ENQUEUED'),
    )


async def _expire_async(database: DualRunDatabase, implementation: _Implementation) -> _Report:
    async with open_broker(database) as broker:
        result: BrokerResult[int]
        match implementation:
            case 'shipped':
                result = await broker.expire_pending_tasks()
            case 'python':
                result = await expiry_and_orphans_caller.expire_pending_tasks(broker)
        match result:
            case Ok(expired):
                return _Counted(counts=(expired,))
            case Err(failed):
                return _failed(failed)


def _expiry_side(name: str, implementation: _Implementation) -> OperationSide[None, _Report]:
    def act(database: DualRunDatabase, _seed: None) -> _Report:
        return asyncio.run(_expire_async(database, implementation))

    return OperationSide(name=name, act=act)


def test_the_core_pending_expiry_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _expiry_seed,
            _expiry_side('Python caller on SQLAlchemy', 'python'),
            _expiry_side('CoreHandle.expire_pending_tasks_async', 'shipped'),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Counted(counts=(4,))
    assert run.candidate.report == run.reference.report


# ---- Orphan audit ----


def _workflow_task(name: str, status: SeedStatus) -> TaskSeed:
    match status:
        case SeedStatus.PENDING:
            return TaskSeed(task_name=name, status=status)
        case SeedStatus.CLAIMED:
            return TaskSeed(
                task_name=name,
                status=status,
                worker_id='worker-orphans',
                claimed_ago=_CLAIMED_AGO,
            )
        case SeedStatus.RUNNING:
            raise AssertionError('the orphan seeds hold PENDING and CLAIMED rows')
        case _ as unreachable:
            assert_never(unreachable)


def _orphan_seed(connection: Connection) -> None:
    for status in (SeedStatus.PENDING, SeedStatus.CLAIMED):
        _ = insert_workflow_task(
            connection,
            _workflow_task(f'orphan.unlinked.{status.value}', status),
            WorkflowLink(workflow_status='RUNNING', node_status=None),
        )
        _ = insert_workflow_task(
            connection,
            _workflow_task(f'orphan.terminal_link.{status.value}', status),
            WorkflowLink(workflow_status='RUNNING', node_status='COMPLETED'),
        )
        _ = insert_workflow_task(
            connection,
            _workflow_task(f'orphan.runnable_link.{status.value}', status),
            WorkflowLink(workflow_status='RUNNING', node_status='ENQUEUED'),
        )
    _ = insert_task(connection, TaskSeed(task_name='orphan.plain', status=SeedStatus.PENDING))


def _audit_report(result: BrokerResult[OrphanTaskAuditReport]) -> _Report:
    match result:
        case Ok(report):
            return _Counted(
                counts=(report.rows_selected, report.candidates_returned, report.cancelled),
            )
        case Err(failed):
            return _failed(failed)


async def _audit_async(database: DualRunDatabase, implementation: _Implementation) -> _Report:
    async with open_broker(database) as broker:
        match implementation:
            case 'shipped':
                return _audit_report(await broker.audit_orphaned_workflow_tasks())
            case 'python':
                return _audit_report(
                    await expiry_and_orphans_caller.audit_orphaned_workflow_tasks(broker),
                )


_HOLD_ORPHAN_CURSOR_SQL: Final = """
    SELECT 1 FROM horsies_recovery_scan_cursors
    WHERE scan_name = 'orphan_workflow_tasks'
    FOR UPDATE
"""


def _audit_side(
    name: str, implementation: _Implementation, *, cursor_held: bool
) -> OperationSide[None, _Report]:
    def act(database: DualRunDatabase, _seed: None) -> _Report:
        match cursor_held:
            case False:
                return asyncio.run(_audit_async(database, implementation))
            case True:
                with psycopg.connect(database.url) as blocker:
                    _ = blocker.execute(_HOLD_ORPHAN_CURSOR_SQL)
                    report = asyncio.run(_audit_async(database, implementation))
                    blocker.rollback()
                    return report

    return OperationSide(name=name, act=act)


def test_the_core_orphan_audit_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _orphan_seed,
            _audit_side('Python caller on SQLAlchemy', 'python', cursor_held=False),
            _audit_side(
                'CoreHandle.audit_orphaned_workflow_tasks_async', 'shipped', cursor_held=False,
            ),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Counted(counts=(6, 4, 4))
    assert run.candidate.report == run.reference.report


def test_a_held_orphan_cursor_refuses_the_page_alike(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _orphan_seed,
            _audit_side('Python caller on SQLAlchemy', 'python', cursor_held=True),
            _audit_side(
                'CoreHandle.audit_orphaned_workflow_tasks_async', 'shipped', cursor_held=True,
            ),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Failed(
        code=BrokerErrorCode.CLEANUP_FAILED,
        retryable=True,
        sqlstate=_LOCK_NOT_AVAILABLE,
    )
    assert run.candidate.report == run.reference.report
