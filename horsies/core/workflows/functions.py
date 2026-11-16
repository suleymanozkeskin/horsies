"""Calls of the workflow SQL functions of the baseline, and the typed read of their rows.

Each function of ``0005_workflow_functions.sql`` takes every lock, writes
every status and reads every fact that decides its step. The Python caller
sends one statement and reads the outcome from the returned row. A row
outside the function's contract raises ``OutcomeDecodeError``: one side runs
against a contract that the other does not implement.

A pause, a cancel and the record of a phase-2 raise are each complete in
one statement, so their callers send them on an autocommit connection: the
statement commits when it returns, and its locks end with it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

from sqlalchemy import text

from horsies.core.lifecycle.outcomes import OutcomeDecodeError

PAUSE_WORKFLOW_FUNCTION_SQL: Final = text("""
    SELECT outcome FROM horsies_workflow_pause(CAST(:wf_id AS uuid))
""")
"""Pause of a RUNNING workflow and its RUNNING descendants; the claimed
tasks of the paused tree are abandoned and their nodes go back to READY."""

CANCEL_WORKFLOW_FUNCTION_SQL: Final = text("""
    SELECT outcome FROM horsies_workflow_cancel(CAST(:wf_id AS uuid))
""")
"""Cancel of a workflow tree: every PENDING, RUNNING or PAUSED workflow of
the tree, the tasks behind ENQUEUED nodes, and the nodes that did not start."""


PHASE2_RECORD_RAISE_FUNCTION_SQL: Final = text("""
    SELECT outcome, attempt_count, limit_outcome, quarantine_verdict
    FROM horsies_workflow_phase2_record_raise(
        CAST(:task_id AS uuid),
        CAST(:attempt_bound AS integer),
        CAST(:limit_error AS text),
        CAST(:limit_node_result AS text)
    )
""")
"""One persistent raise of a phase-2 record: the attempt count, and at the
attempt bound the recovery limit of the workflow and the quarantine of the
record."""


class PauseOutcome(Enum):
    """What ``horsies_workflow_pause`` did."""

    PAUSED = 'PAUSED'
    """The workflow was RUNNING; it and its RUNNING descendants are PAUSED."""

    NOT_RUNNING = 'NOT_RUNNING'
    """The workflow exists and is not RUNNING. Nothing was written."""

    NOT_FOUND = 'NOT_FOUND'
    """No workflow has this id. Nothing was written."""


class CancelOutcome(Enum):
    """What ``horsies_workflow_cancel`` did."""

    CANCELLED = 'CANCELLED'
    """The workflow was PENDING, RUNNING or PAUSED; its tree is CANCELLED."""

    NOT_CANCELLABLE = 'NOT_CANCELLABLE'
    """The workflow is in a terminal status. Nothing was written."""

    NOT_FOUND = 'NOT_FOUND'
    """No workflow has this id. Nothing was written."""


def _one_outcome(rows: Sequence[Any], function: str) -> object:
    """The ``outcome`` column of the one row of a one-row function. Pure.

    Raises:
        OutcomeDecodeError: the function returned no row or several rows.
    """
    match rows:
        case [row]:
            return row.outcome
        case _:
            raise OutcomeDecodeError(
                f'{function} returned {len(rows)} rows; expected one row',
            )


def decode_pause_outcome(rows: Sequence[Any]) -> PauseOutcome:
    """The outcome of a ``horsies_workflow_pause`` call from its rows. Pure.

    Raises:
        OutcomeDecodeError: not one row, or an outcome outside ``PauseOutcome``.
    """
    value = _one_outcome(rows, 'horsies_workflow_pause')
    match value:
        case 'PAUSED':
            return PauseOutcome.PAUSED
        case 'NOT_RUNNING':
            return PauseOutcome.NOT_RUNNING
        case 'NOT_FOUND':
            return PauseOutcome.NOT_FOUND
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_pause returned outcome {value!r}; '
                'expected PAUSED, NOT_RUNNING or NOT_FOUND',
            )


def decode_cancel_outcome(rows: Sequence[Any]) -> CancelOutcome:
    """The outcome of a ``horsies_workflow_cancel`` call from its rows. Pure.

    Raises:
        OutcomeDecodeError: not one row, or an outcome outside ``CancelOutcome``.
    """
    value = _one_outcome(rows, 'horsies_workflow_cancel')
    match value:
        case 'CANCELLED':
            return CancelOutcome.CANCELLED
        case 'NOT_CANCELLABLE':
            return CancelOutcome.NOT_CANCELLABLE
        case 'NOT_FOUND':
            return CancelOutcome.NOT_FOUND
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_cancel returned outcome {value!r}; '
                'expected CANCELLED, NOT_CANCELLABLE or NOT_FOUND',
            )


class RecoveryLimitWrite(Enum):
    """What the recovery limit of a raise wrote."""

    WORKFLOW_FAILED = 'LIMIT_REACHED_FAILED'
    """No node was live: the workflow is FAILED."""

    LIVE_NODES_FINISH = 'LIMIT_REACHED_LIVE_NODES'
    """A node is live: the workflow stays RUNNING with its error stored."""


@dataclass(frozen=True, slots=True)
class RaisePendingAbsent:
    """The record is gone. Nothing was written."""


@dataclass(frozen=True, slots=True)
class RaiseNotRecorded:
    """The raise reached the bound and the workflow is not RUNNING.

    Nothing was written; ``attempt_count`` is the stored count.
    """

    attempt_count: int


@dataclass(frozen=True, slots=True)
class RaiseCounted:
    """The raise is counted and stays under the bound."""

    attempt_count: int


@dataclass(frozen=True, slots=True)
class RaiseLimitReached:
    """The raise is counted at the bound: the limit applied and the record
    went to the quarantine function.

    ``quarantine_verdict`` is the verdict of ``horsies_phase2_quarantine_one``;
    ``quarantine_detail`` is its detail text, or ``None`` when it has none.
    """

    attempt_count: int
    limit: RecoveryLimitWrite
    quarantine_verdict: str
    quarantine_detail: str | None


type Phase2RaiseOutcome = (
    RaisePendingAbsent | RaiseNotRecorded | RaiseCounted | RaiseLimitReached
)


def _attempt_count(value: object) -> int:
    match value:
        case int() as count:
            return count
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_phase2_record_raise returned attempt_count '
                f'{value!r}; expected an integer',
            )


def _limit_write(value: object) -> RecoveryLimitWrite:
    match value:
        case 'LIMIT_REACHED_FAILED':
            return RecoveryLimitWrite.WORKFLOW_FAILED
        case 'LIMIT_REACHED_LIVE_NODES':
            return RecoveryLimitWrite.LIVE_NODES_FINISH
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_phase2_record_raise returned limit_outcome '
                f'{value!r}; expected LIMIT_REACHED_FAILED or '
                'LIMIT_REACHED_LIVE_NODES',
            )


def _quarantine_parts(value: object) -> tuple[str, str | None]:
    """The verdict and the detail of ``quarantine_verdict``.

    The function writes the verdict, then ``': '`` and the detail when the
    verdict has one. A verdict is a token of capitals and underscores, so the
    first ``': '`` ends it.
    """
    match value:
        case str() as text_value:
            verdict, separator, detail = text_value.partition(': ')
            match separator:
                case '':
                    return verdict, None
                case _:
                    return verdict, detail
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_phase2_record_raise returned '
                f'quarantine_verdict {value!r}; expected a text',
            )


def decode_phase2_raise(rows: Sequence[Any]) -> Phase2RaiseOutcome:
    """The outcome of a ``horsies_workflow_phase2_record_raise`` call. Pure.

    Raises:
        OutcomeDecodeError: not one row, an outcome outside the function's
            catalog, or a column that its outcome needs and does not hold.
    """
    match rows:
        case [row]:
            pass
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_phase2_record_raise returned {len(rows)} '
                'rows; expected one row',
            )
    match row.outcome:
        case 'PENDING_ABSENT':
            return RaisePendingAbsent()
        case 'NOT_RECORDED':
            return RaiseNotRecorded(attempt_count=_attempt_count(row.attempt_count))
        case 'COUNTED':
            return RaiseCounted(attempt_count=_attempt_count(row.attempt_count))
        case 'LIMIT_REACHED':
            verdict, detail = _quarantine_parts(row.quarantine_verdict)
            return RaiseLimitReached(
                attempt_count=_attempt_count(row.attempt_count),
                limit=_limit_write(row.limit_outcome),
                quarantine_verdict=verdict,
                quarantine_detail=detail,
            )
        case other:
            raise OutcomeDecodeError(
                f'horsies_workflow_phase2_record_raise returned outcome '
                f'{other!r}; expected PENDING_ABSENT, NOT_RECORDED, COUNTED '
                'or LIMIT_REACHED',
            )
