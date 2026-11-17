"""Failed attempt, before the retry facts travelled with the claim.

``python/horsies/core/worker/failed_attempt.py`` read the row of the claim
generation first (``READ_IN_FLIGHT_ATTEMPT_SQL``, no lock), and the retry
request came from the facts that the read returned. Then
``horsies_finish_failed_attempt`` ran with the ``REFUSE`` rule, on the same
autocommit connection:

- the finalize of a reported failure (``_persist_failed_attempt``,
  ``python/horsies/core/worker/finalize.py``) read only for a task error; a
  worker-level failure sent no retry;
- the end of an attempt whose child ended without a result
  (``_write_ended_attempt``, ``python/horsies/core/worker/dispatch.py``) returned a
  CLAIMED row to the queue with ``UNCLAIM_CLAIMED_TASK_SQL``, and sent no
  retry for a row that the claim did not hold.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, LiteralString, assert_never

from horsies.core.utils.retry import RetryPolicy, admitted_retry_policy, calculate_retry_delay
from tests.dualrun.capture import Connection

READ_IN_FLIGHT_ATTEMPT_SQL: Final[LiteralString] = """
    SELECT CAST(status AS text) AS status, retry_count, max_retries,
           task_options
    FROM horsies_tasks
    WHERE id = %(id)s
      AND claimed_by_worker_id = CAST(%(wid)s AS VARCHAR)
      AND status IN ('CLAIMED', 'RUNNING')
      AND claimed_at = CAST(%(claimed_at)s AS TIMESTAMPTZ)
"""

UNCLAIM_CLAIMED_TASK_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    WHERE id = %(id)s
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(%(wid)s AS VARCHAR)
      AND claimed_at = CAST(%(claimed_at)s AS TIMESTAMPTZ)
"""

FINISH_FAILED_ATTEMPT_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_failed_attempt(
        CAST(%(task_id)s AS uuid), %(worker_id)s, CAST(%(claimed_at)s AS TIMESTAMPTZ),
        %(retry_kind)s,
        CAST(%(expected_retry_count)s AS INTEGER),
        CAST(%(retry_delay_s)s AS DOUBLE PRECISION),
        %(retry_placement)s,
        %(result)s, %(error_code)s, %(failed_reason)s,
        %(attempt_outcome)s, %(attempt_error_message)s, %(attempt_failed_reason)s,
        'REFUSE', TRUE
    )
"""


@dataclass(frozen=True, slots=True)
class Failure:
    """The terminal values and the attempt row of one failed attempt."""

    task_id: str
    worker_id: str
    claimed_at: datetime
    result_json: str
    error_code: str
    failed_reason: str | None
    attempt_outcome: str
    attempt_message: str | None
    attempt_reason: str | None


@dataclass(frozen=True, slots=True)
class _Running:
    retry_count: int
    max_retries: int
    task_options: str | None


@dataclass(frozen=True, slots=True)
class _Claimed:
    pass


@dataclass(frozen=True, slots=True)
class _NotHeld:
    pass


_NO_RETRY: Final[dict[str, Any]] = {
    'retry_kind': 'NONE',
    'expected_retry_count': None,
    'retry_delay_s': None,
    'retry_placement': None,
}


def _read(connection: Connection, failure: Failure) -> _Running | _Claimed | _NotHeld:
    row = connection.execute(
        READ_IN_FLIGHT_ATTEMPT_SQL,
        {'id': failure.task_id, 'wid': failure.worker_id, 'claimed_at': failure.claimed_at},
    ).fetchone()
    match row:
        case None:
            return _NotHeld()
        case ('RUNNING', int() as retry_count, int() as max_retries, str() | None as options):
            return _Running(retry_count, max_retries, options)
        case ('CLAIMED', *_):
            return _Claimed()
        case _:
            raise AssertionError(f'the in-flight read returned {row!r}')


def _retry(attempt: _Running, error_code: str, placement: str) -> dict[str, Any]:
    policy = admitted_retry_policy(
        retry_count=attempt.retry_count,
        max_retries=attempt.max_retries,
        task_options_json=attempt.task_options,
        error_code=error_code,
    )
    match policy:
        case None:
            return _NO_RETRY
        case RetryPolicy():
            return {
                'retry_kind': 'RETRY',
                'expected_retry_count': attempt.retry_count,
                'retry_delay_s': calculate_retry_delay(attempt.retry_count + 1, policy),
                'retry_placement': placement,
            }
        case _ as unreachable:
            assert_never(unreachable)


def _finish(connection: Connection, failure: Failure, retry: dict[str, Any]) -> str:
    row = connection.execute(
        FINISH_FAILED_ATTEMPT_SQL,
        {
            'task_id': failure.task_id,
            'worker_id': failure.worker_id,
            'claimed_at': failure.claimed_at,
            **retry,
            'result': failure.result_json,
            'error_code': failure.error_code,
            'failed_reason': failure.failed_reason,
            'attempt_outcome': failure.attempt_outcome,
            'attempt_error_message': failure.attempt_message,
            'attempt_failed_reason': failure.attempt_reason,
        },
    ).fetchone()
    match row:
        case (str() as outcome,):
            return outcome
        case _:
            raise AssertionError(f'the finish returned {row!r}')


def persist_failed_attempt(
    connection: Connection, failure: Failure, *, task_error: bool
) -> str:
    """The finalize of a reported failure: the read for a task error, then the finish."""
    retry = _NO_RETRY
    if task_error:
        match _read(connection, failure):
            case _Running() as attempt:
                retry = _retry(attempt, failure.error_code, 'SAME_CLASS')
            case _Claimed() | _NotHeld():
                pass
    return _finish(connection, failure, retry)


def write_ended_attempt(connection: Connection, failure: Failure, placement: str) -> str:
    """The end of an attempt with no result: the read, then the unclaim or the finish."""
    match _read(connection, failure):
        case _Claimed():
            cursor = connection.execute(
                UNCLAIM_CLAIMED_TASK_SQL,
                {'id': failure.task_id, 'wid': failure.worker_id, 'claimed_at': failure.claimed_at},
            )
            return 'RELEASED' if cursor.rowcount > 0 else 'UNCLAIM_MISSED'
        case _Running() as attempt:
            retry = _retry(attempt, failure.error_code, placement)
        case _NotHeld():
            retry = _NO_RETRY
    return _finish(connection, failure, retry)
