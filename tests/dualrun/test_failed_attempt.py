# pyright: reportPrivateUsage=false
"""Dual-runs of the failed attempt.

The seed holds one row per case: RUNNING rows whose policy retries, does not
retry, is spent or would retry past the deadline; a CLAIMED row this claim
holds; rows of another claim; and a workflow task. Two pairs, with the same
seed:

1. The SQL redesign: the read before the finish against the retry facts of
   the claim. Reference: ``tests/dualrun/reference/failed_attempt.py``.
   Candidates: the finalize of a reported failure
   (``FinalizeMixin._persist_failed_attempt``) and the end of an attempt
   whose child ended without a result (``DispatchMixin._write_ended_attempt``),
   each with the ``DispatchClaim`` that the claim returned. Both sides must
   write the same attempts, retries, releases and history rows, and report
   the same outcome.
2. The caller switch: the same two callers on SQLAlchemy against the core.
   Reference: ``tests/dualrun/reference/failed_attempt_caller.py``, the method
   bodies before the switch. Candidate: the methods as shipped, which call
   ``CoreHandle.finish_failed_attempt_async`` and
   ``finish_ended_attempt_async``. The snapshots and the outcomes must be
   equal.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Final, Literal, assert_never

import psycopg
import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies._core import CoreHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.classify import ErrorProduced, WorkerLevelFailure
from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    FailureApplied,
    LostClaim,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.dispatch import DispatchMixin, _AttemptFinished, _AttemptUnclaimed
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.failed_attempt import RetryRequest
from horsies.core.worker.finalize import FinalizeMixin, _Phase1Input
from horsies.core.worker.shared_stop import RetryPlacement
from tests.dualrun.capture import Connection
from tests.dualrun.clients import sqlalchemy_url
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import failed_attempt as reference
from tests.dualrun.reference import failed_attempt_caller
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

_AGO: Final = timedelta(minutes=5)
_OTHER_GENERATION: Final = timedelta(seconds=1)
_TASK_ERROR_CODE: Final = 'TRANSIENT'


def _options(code: str) -> str:
    return json.dumps(
        {
            'retry_policy': {
                'max_retries': 2,
                'intervals': [60, 60],
                'backoff_strategy': 'fixed',
                'jitter': False,
                'auto_retry_for': [code],
            },
        },
    )


@dataclass(frozen=True, slots=True)
class _Case:
    """One failed attempt: the row's claim as the claim returned it, and how it ended."""

    name: str
    task_id: str
    claim: DispatchClaim
    task_error: bool
    placement: RetryPlacement


@dataclass(frozen=True, slots=True)
class _Seed:
    worker_id: str
    finalized: tuple[_Case, ...]
    ended: tuple[_Case, ...]


def _row(name: str, worker_id: str, status: SeedStatus, **fields: Any) -> TaskSeed:
    started = _AGO if status is SeedStatus.RUNNING else None
    return replace(
        TaskSeed(
            task_name=name,
            status=status,
            worker_id=worker_id,
            claimed_ago=_AGO,
            started_ago=started,
        ),
        **fields,
    )


def _case(
    connection: Connection,
    seed: TaskSeed,
    *,
    workflow: bool = False,
    task_error: bool = True,
    placement: RetryPlacement = RetryPlacement.SAME_CLASS,
    generation_offset: timedelta = timedelta(0),
) -> _Case:
    task = (
        insert_workflow_task(connection, seed, WorkflowLink('RUNNING', 'RUNNING'))
        if workflow
        else insert_task(connection, seed)
    )
    assert task.claimed_at is not None, 'a claimed seed has a claim time'
    return _Case(
        name=seed.task_name,
        task_id=str(task.task_id),
        claim=DispatchClaim(
            claimed_at=task.claimed_at - generation_offset,
            retry_count=seed.retry_count,
            max_retries=seed.max_retries,
            task_options=seed.task_options,
        ),
        task_error=task_error,
        placement=placement,
    )


def _seed(connection: Connection) -> _Seed:
    worker_id = f'worker-{uuid.uuid4().hex}'
    other_worker = f'worker-{uuid.uuid4().hex}'
    running, claimed = SeedStatus.RUNNING, SeedStatus.CLAIMED
    error_retry = {'max_retries': 2, 'task_options': _options(_TASK_ERROR_CODE)}
    crash_retry = {'max_retries': 2, 'task_options': _options('WORKER_CRASHED')}
    finalized = (
        _case(connection, _row('finalize.retry', worker_id, running, **error_retry)),
        _case(connection, _row('finalize.no_policy', worker_id, running)),
        _case(
            connection,
            _row('finalize.worker_failure', worker_id, running, **error_retry),
            task_error=False,
        ),
        _case(connection, _row('finalize.claimed', worker_id, claimed, **error_retry)),
        _case(
            connection,
            _row('finalize.older_generation', worker_id, running, **error_retry),
            generation_offset=_OTHER_GENERATION,
        ),
        _case(connection, _row('finalize.workflow', worker_id, running), workflow=True),
    )
    ended = (
        _case(connection, _row('ended.retry', worker_id, running, **crash_retry)),
        _case(
            connection,
            _row('ended.retry_isolated', worker_id, running, **crash_retry),
            placement=RetryPlacement.ISOLATED,
        ),
        _case(connection, _row('ended.no_policy', worker_id, running)),
        _case(connection, _row('ended.spent', worker_id, running, retry_count=2, **crash_retry)),
        _case(
            connection,
            _row(
                'ended.past_deadline', worker_id, running,
                good_until_in=timedelta(seconds=30), **crash_retry,
            ),
        ),
        _case(connection, _row('ended.claimed', worker_id, claimed, **crash_retry)),
        _case(connection, _row('ended.other_worker', other_worker, claimed, **crash_retry)),
        _case(
            connection,
            _row('ended.older_generation', worker_id, running, **crash_retry),
            generation_offset=_OTHER_GENERATION,
        ),
        _case(connection, _row('ended.workflow', worker_id, running), workflow=True),
    )
    return _Seed(worker_id=worker_id, finalized=finalized, ended=ended)


# ---- the failures both sides write ----


def _task_failure(case: _Case) -> TaskError:
    return TaskError(error_code=_TASK_ERROR_CODE, message=f'{case.name} failed')


def _crash(case: _Case) -> TaskError:
    return TaskError(error_code=OperationalErrorCode.WORKER_CRASHED, message=f'{case.name} crashed')


def _finalize_failure(case: _Case, worker_id: str) -> reference.Failure:
    error = _task_failure(case)
    return reference.Failure(
        task_id=case.task_id,
        worker_id=worker_id,
        claimed_at=case.claim.claimed_at,
        result_json=serialize_error_payload(TaskResult(err=error)),
        error_code=_TASK_ERROR_CODE,
        failed_reason='task failed',
        attempt_outcome='FAILED' if case.task_error else 'WORKER_FAILURE',
        attempt_message=error.message if case.task_error else 'worker failed',
        attempt_reason=None if case.task_error else 'worker failed',
    )


def _ended_failure(case: _Case, worker_id: str) -> reference.Failure:
    error = _crash(case)
    return reference.Failure(
        task_id=case.task_id,
        worker_id=worker_id,
        claimed_at=case.claim.claimed_at,
        result_json=serialize_error_payload(TaskResult(err=error)),
        error_code='WORKER_CRASHED',
        failed_reason=None,
        attempt_outcome='FAILED',
        attempt_message=error.message,
        attempt_reason=error.message,
    )


# ---- reference ----


def _reference_side(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        report = {
            case.name: reference.persist_failed_attempt(
                connection, _finalize_failure(case, seed.worker_id), task_error=case.task_error
            )
            for case in seed.finalized
        }
        report |= {
            case.name: reference.write_ended_attempt(
                connection, _ended_failure(case, seed.worker_id), case.placement.name
            )
            for case in seed.ended
        }
        return report


# ---- candidate ----


class _Probe(DispatchMixin, FinalizeMixin):
    def __init__(
        self,
        sf: async_sessionmaker[AsyncSession],
        worker_id: str,
        broker: PostgresBroker,
    ) -> None:
        self.sf = sf
        self.worker_instance_id = worker_id
        self.wakes: list[str] = []
        self._broker = broker

    @property
    def core_handle(self) -> CoreHandle:
        return self._broker._core_handle

    def _wake_when_retry_due(self, scheduled: RetryScheduled, request: RetryRequest) -> None:
        self.wakes.append(scheduled.task_id)


def _outcome_name(
    outcome: FailureApplied | RetryScheduled | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent,
) -> str:
    match outcome:
        case FailureApplied():
            return 'APPLIED'
        case RetryScheduled():
            return 'RETRY_SCHEDULED'
        case AlreadyApplied():
            return 'ALREADY_APPLIED'
        case LostClaim():
            return 'LOST_CLAIM'
        case SourceStateConflict():
            return 'SOURCE_STATE_CONFLICT'
        case TaskAbsent():
            return 'TASK_ABSENT'
        case _ as unreachable:
            assert_never(unreachable)


async def _persist(
    probe: _Probe, case: _Case, worker_id: str, implementation: _Implementation = 'shipped',
) -> None:
    failure = _finalize_failure(case, worker_id)
    report = (
        ErrorProduced(error=_task_failure(case))
        if case.task_error
        else WorkerLevelFailure(detail='worker failed')
    )
    command = FailLockedTask(
        task_id=case.task_id,
        fence=OwnedClaim(worker_id=worker_id, claimed_at=case.claim.claimed_at),
        result_json=failure.result_json,
        error_code=failure.error_code,
        failed_reason=failure.failed_reason,
    )
    child = _Phase1Input(ok=True, result_json_str=failure.result_json, failed_reason=None)
    match implementation:
        case 'shipped':
            result = await probe._persist_failed_attempt(
                command, report, claim=case.claim, child=child,
            )
        case 'python':
            result = await failed_attempt_caller.persist_failed_attempt(
                probe, command, report, claim=case.claim, child=child,
            )
    match result:
        case Ok():
            pass
        case Err(error):
            raise AssertionError(f'the finalize of {case.name} failed: {error}')


type _Implementation = Literal['shipped', 'python']
"""``shipped``: the methods as shipped, on the core. ``python``: the method
bodies before the switch, on SQLAlchemy."""


async def _write_ended(
    probe: _Probe, case: _Case, implementation: _Implementation,
) -> _AttemptUnclaimed | _AttemptFinished:
    match implementation:
        case 'shipped':
            return await probe._write_ended_attempt(
                case.task_id, _crash(case), case.claim, case.placement
            )
        case 'python':
            return await failed_attempt_caller.write_ended_attempt(
                probe, case.task_id, _crash(case), case.claim, case.placement
            )


async def _candidate_async(
    database: DualRunDatabase, seed: _Seed, implementation: _Implementation = 'shipped',
) -> dict[str, str]:
    engine = create_async_engine(sqlalchemy_url(database))
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        probe = _Probe(
            async_sessionmaker(engine, expire_on_commit=False), seed.worker_id, broker,
        )
        outcomes: dict[str, str] = {}
        for case in seed.finalized:
            await _persist(probe, case, seed.worker_id, implementation)
        for case in seed.ended:
            match await _write_ended(probe, case, implementation):
                case _AttemptUnclaimed():
                    outcomes[case.name] = 'RELEASED'
                case _AttemptFinished(outcome=outcome):
                    outcomes[case.name] = _outcome_name(outcome)
        outcomes |= {
            case.name: 'RETRY_SCHEDULED' if case.task_id in probe.wakes else 'WRITTEN'
            for case in seed.finalized
        }
        return outcomes
    finally:
        _ = await broker.close_async()
        await engine.dispose()


def _candidate_side(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    return asyncio.run(_candidate_async(database, seed))


def _python_caller_side(database: DualRunDatabase, seed: _Seed) -> dict[str, str]:
    return asyncio.run(_candidate_async(database, seed, 'python'))


def _finalize_report(outcome: str) -> str:
    """The finalize reports a scheduled retry by its wake; every other outcome only by the row."""
    return 'RETRY_SCHEDULED' if outcome == 'RETRY_SCHEDULED' else 'WRITTEN'


def _ok(
    result: Result[OperationDualRun[dict[str, str], dict[str, str]], SelfCheckStepFailed],
) -> OperationDualRun[dict[str, str], dict[str, str]]:
    match result:
        case Ok(value):
            return value
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_failed_attempts_write_what_the_read_and_finish_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='read, unclaim and finish', act=_reference_side),
            OperationSide(name='finish with the claim facts', act=_candidate_side),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == {
        'finalize.retry': 'RETRY_SCHEDULED',
        'finalize.no_policy': 'APPLIED',
        'finalize.worker_failure': 'APPLIED',
        'finalize.claimed': 'SOURCE_STATE_CONFLICT',
        'finalize.older_generation': 'LOST_CLAIM',
        'finalize.workflow': 'APPLIED',
        'ended.retry': 'RETRY_SCHEDULED',
        'ended.retry_isolated': 'RETRY_SCHEDULED',
        'ended.no_policy': 'APPLIED',
        'ended.spent': 'APPLIED',
        'ended.past_deadline': 'APPLIED',
        'ended.claimed': 'RELEASED',
        'ended.other_worker': 'LOST_CLAIM',
        'ended.older_generation': 'LOST_CLAIM',
        'ended.workflow': 'APPLIED',
    }
    assert run.candidate.report == {
        name: _finalize_report(outcome) if name.startswith('finalize.') else outcome
        for name, outcome in run.reference.report.items()
    }


def test_the_core_failed_attempts_write_what_the_python_callers_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = _ok(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='Python callers on SQLAlchemy', act=_python_caller_side),
            OperationSide(name='CoreHandle failed-attempt writes', act=_candidate_side),
            SeedCoverage.HISTORY_AND_BEATS,
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.candidate.report == run.reference.report
    assert run.reference.report['ended.claimed'] == 'RELEASED'
    assert run.reference.report['finalize.retry'] == 'RETRY_SCHEDULED'
