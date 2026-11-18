"""Stale RUNNING recovery: one bounded scan, then one statement per task.

``scan_stale_running`` reads the candidates of a pass with
``horsies_stale_running_scan``: RUNNING rows whose runner stopped beating,
with the facts the retry decision and the crash result need. It takes no
lock; a candidate is only a hint. ``finish_stale_attempt`` sends
``horsies_finish_stale_attempt`` for one candidate: the function judges
staleness again under the task advisory lock and the row lock, writes the
FAILED attempt, and then the crash retry or the move to history. A retry
request carries the retry count it was computed from; a row whose count
changed is refused with guard ``RETRY_COUNT`` and nothing is written.

Each statement runs alone on an autocommit connection and commits when it
returns. The retry decision is made here, from the scan row, by the task's
retry policy for ``WORKER_CRASHED``; ``good_until`` is judged by the
function under the lock.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.codec.json_io import SerializationError, dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.outcomes import (
    StaleAttemptOutcome,
    decode_stale_attempt_row,
)
from horsies.core.models.tasks import (
    OperationalErrorCode,
    RetryPolicy,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import Result
from horsies.core.utils.retry import admitted_retry_policy, calculate_retry_delay

STALE_RUNNING_SCAN_BATCH_SIZE: Final = 1000
"""Most candidates of one pass (the function accepts 1 to 10000). A pass that
reads a full batch leaves the rest to the next pass."""

STALE_RUNNING_SCAN_SQL = text("""
    SELECT * FROM horsies_stale_running_scan(
        'BEAT_AGE',
        CAST(:stale_after_ms AS INTEGER),
        CAST(:finalizing_stale_after_ms AS INTEGER),
        NULL, NULL,
        CAST(:batch_size AS INTEGER)
    )
""")

FINISH_STALE_ATTEMPT_SQL = text("""
    SELECT * FROM horsies_finish_stale_attempt(
        CAST(:task_id AS uuid),
        'BEAT_AGE',
        CAST(:stale_after_ms AS INTEGER),
        CAST(:finalizing_stale_after_ms AS INTEGER),
        NULL, NULL,
        :retry_kind,
        CAST(:expected_retry_count AS INTEGER),
        CAST(:retry_delay_s AS DOUBLE PRECISION),
        :result, :error_code, :failed_reason,
        :attempt_message, :attempt_reason
    )
""")


@dataclass(frozen=True, slots=True)
class StaleThresholds:
    """How long a runner may stay silent, and a finalizing marker may stand, in ms."""

    stale_after_ms: int
    finalizing_stale_after_ms: int


@dataclass(frozen=True, slots=True)
class StaleCandidate:
    """One row of the scan: the facts of the retry decision and of the crash result."""

    task_id: str
    retry_count: int
    max_retries: int
    task_options: str | None
    good_until: datetime | None
    claimed_by_worker_id: str | None
    worker_pid: int | None
    worker_hostname: str | None
    worker_process_name: str | None
    started_at: datetime
    db_now: datetime


@dataclass(frozen=True, slots=True)
class NoCrashRetry:
    """The policy does not retry this crash: the function writes the failure."""


@dataclass(frozen=True, slots=True)
class CrashRetry:
    """The policy retries this crash after ``delay_s``, unless the next attempt
    would start at or after ``good_until``. ``retry_count`` is the scan's
    count, and part of the function's fence."""

    retry_count: int
    delay_s: float


type CrashRetryRequest = NoCrashRetry | CrashRetry


def read_stale_candidate(row: Mapping[str, Any]) -> StaleCandidate:
    """One scan row as a candidate. A value outside the function's contract
    is a defect of the function (``AssertionError``). Pure."""
    match row:
        case {
            'task_id': task_id,
            'retry_count': int() as retry_count,
            'max_retries': int() as max_retries,
            'task_options': str() | None as task_options,
            'good_until': datetime() | None as good_until,
            'claimed_by_worker_id': str() | None as worker_id,
            'worker_pid': int() | None as worker_pid,
            'worker_hostname': str() | None as worker_hostname,
            'worker_process_name': str() | None as worker_process_name,
            'started_at': datetime() as started_at,
            'db_now': datetime() as db_now,
        } if task_id is not None:
            return StaleCandidate(
                task_id=str(task_id),
                retry_count=retry_count,
                max_retries=max_retries,
                task_options=task_options,
                good_until=good_until,
                claimed_by_worker_id=worker_id,
                worker_pid=worker_pid,
                worker_hostname=worker_hostname,
                worker_process_name=worker_process_name,
                started_at=started_at,
                db_now=db_now,
            )
        case _:
            raise AssertionError(
                f'horsies_stale_running_scan returned a row outside its contract: {dict(row)!r}'
            )


def crash_retry_request(candidate: StaleCandidate) -> CrashRetryRequest:
    """The retry that the task's policy asks for after a crash. Pure."""
    policy = admitted_retry_policy(
        retry_count=candidate.retry_count,
        max_retries=candidate.max_retries,
        task_options_json=candidate.task_options,
        error_code=OperationalErrorCode.WORKER_CRASHED,
    )
    match policy:
        case None:
            return NoCrashRetry()
        case RetryPolicy():
            return CrashRetry(
                retry_count=candidate.retry_count,
                delay_s=calculate_retry_delay(candidate.retry_count + 1, policy),
            )
        case _ as unreachable:
            assert_never(unreachable)


def crash_reason(thresholds: StaleThresholds) -> str:
    """The failure reason of a stale attempt. Pure."""
    stale_after_ms = thresholds.stale_after_ms
    return (
        f'Worker process crashed (no runner heartbeat '
        f'for {stale_after_ms}ms = {stale_after_ms / 1000:.1f}s)'
    )


def crash_result_json(
    candidate: StaleCandidate,
    thresholds: StaleThresholds,
) -> Result[str, SerializationError]:
    """The terminal result of a stale attempt: a ``WORKER_CRASHED`` error with
    the facts of the silent attempt. ``detected_at`` is the scan's clock. Pure."""
    task_error = TaskError(
        error_code=OperationalErrorCode.WORKER_CRASHED,
        message=crash_reason(thresholds),
        data={
            'stale_threshold_ms': thresholds.stale_after_ms,
            'stale_threshold_seconds': thresholds.stale_after_ms / 1000.0,
            'worker_pid': candidate.worker_pid,
            'worker_hostname': candidate.worker_hostname,
            'worker_id': candidate.claimed_by_worker_id,
            'started_at': candidate.started_at.isoformat(),
            'detected_at': candidate.db_now.isoformat(),
        },
    )
    task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
    return dumps_json(encode_task_result(task_result, type(None)))


def finish_stale_parameters(
    candidate: StaleCandidate,
    thresholds: StaleThresholds,
    retry: CrashRetryRequest,
    result_json: str,
) -> dict[str, object]:
    """The parameters of ``FINISH_STALE_ATTEMPT_SQL``. The attempt row and the
    terminal failure carry the same reason. Pure."""
    match retry:
        case NoCrashRetry():
            retry_parameters: dict[str, object] = {
                'retry_kind': 'NONE',
                'expected_retry_count': None,
                'retry_delay_s': None,
            }
        case CrashRetry(retry_count=count, delay_s=delay_s):
            retry_parameters = {
                'retry_kind': 'RETRY',
                'expected_retry_count': count,
                'retry_delay_s': delay_s,
            }
        case _ as unreachable:
            assert_never(unreachable)
    reason = crash_reason(thresholds)
    return {
        'task_id': candidate.task_id,
        'stale_after_ms': thresholds.stale_after_ms,
        'finalizing_stale_after_ms': thresholds.finalizing_stale_after_ms,
        **retry_parameters,
        'result': result_json,
        'error_code': OperationalErrorCode.WORKER_CRASHED.value,
        'failed_reason': reason,
        'attempt_message': reason,
        'attempt_reason': reason,
    }


async def scan_stale_running(
    connection: AsyncConnection,
    thresholds: StaleThresholds,
    batch_size: int,
) -> tuple[StaleCandidate, ...]:
    """The candidates of one pass, at most ``batch_size``. Takes no lock.

    Raises:
        Exception: database errors, to the caller. The scan writes nothing.
        AssertionError: a row outside the function's contract.
    """
    result = await connection.execute(
        STALE_RUNNING_SCAN_SQL,
        {
            'stale_after_ms': thresholds.stale_after_ms,
            'finalizing_stale_after_ms': thresholds.finalizing_stale_after_ms,
            'batch_size': batch_size,
        },
    )
    return tuple(
        read_stale_candidate({str(key): value for key, value in row.items()})
        for row in result.mappings().all()
    )


async def finish_stale_attempt(
    connection: AsyncConnection,
    candidate: StaleCandidate,
    thresholds: StaleThresholds,
    result_json: str,
) -> StaleAttemptOutcome:
    """Recover one candidate: the retry its policy asks for, or the failure.

    ``connection`` must be in autocommit: the statement commits when it
    returns. A refusal writes nothing. A lost reply is an unknown outcome: a
    second call classifies a committed failure as ``AlreadyApplied`` and a
    committed retry as a refusal (the row is not RUNNING).

    Raises:
        Exception: database errors, to the caller.
        OutcomeDecodeError: the row does not satisfy the wire contract.
        AssertionError: the function returned other than one row.
    """
    result = await connection.execute(
        FINISH_STALE_ATTEMPT_SQL,
        finish_stale_parameters(
            candidate, thresholds, crash_retry_request(candidate), result_json
        ),
    )
    rows: Sequence[Any] = result.mappings().all()
    assert len(rows) == 1, (
        f'horsies_finish_stale_attempt returned {len(rows)} rows for task '
        f'{candidate.task_id}; it reports exactly one outcome'
    )
    return decode_stale_attempt_row({str(key): value for key, value in rows[0].items()})

