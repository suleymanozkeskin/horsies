"""Calls of the workflow SQL functions of the baseline, and the typed read of their rows.

Each function of ``0005_workflow_functions.sql`` takes every lock, writes
every status and reads every fact that decides its step. The Python caller
sends one statement and reads the outcome from the returned row. A row
outside the function's contract raises ``OutcomeDecodeError``: one side runs
against a contract that the other does not implement.

A pause is complete in one statement, so its caller sends it on an
autocommit connection: the statement commits when it returns, and its
locks end with it.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Any, Final

from sqlalchemy import text

from horsies.core.lifecycle.outcomes import OutcomeDecodeError

PAUSE_WORKFLOW_FUNCTION_SQL: Final = text("""
    SELECT outcome FROM horsies_workflow_pause(CAST(:wf_id AS uuid))
""")
"""Pause of a RUNNING workflow and its RUNNING descendants; the claimed
tasks of the paused tree are abandoned and their nodes go back to READY."""


class PauseOutcome(Enum):
    """What ``horsies_workflow_pause`` did."""

    PAUSED = 'PAUSED'
    """The workflow was RUNNING; it and its RUNNING descendants are PAUSED."""

    NOT_RUNNING = 'NOT_RUNNING'
    """The workflow exists and is not RUNNING. Nothing was written."""

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
