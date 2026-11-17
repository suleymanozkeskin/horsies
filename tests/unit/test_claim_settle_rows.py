"""The read of the rows of ``horsies_workflow_claimed_tasks_settle`` (``blocked_claims_of``)."""

from __future__ import annotations

import uuid

import pytest

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.worker.claiming import blocked_claims_of

_FIRST = str(uuid.UUID(int=1))
_SECOND = str(uuid.UUID(int=2))


@pytest.mark.unit
class TestBlockedClaimsOf:
    """One row per input task, in input order, with DISPATCH or BLOCKED."""

    def test_dispatched_tasks_are_not_blocked(self) -> None:
        rows = [
            {'task_id': uuid.UUID(_FIRST), 'disposition': 'DISPATCH'},
            {'task_id': uuid.UUID(_SECOND), 'disposition': 'DISPATCH'},
        ]
        assert blocked_claims_of([_FIRST, _SECOND], rows) == set()

    def test_a_blocked_task_is_named(self) -> None:
        rows = [
            {'task_id': uuid.UUID(_FIRST), 'disposition': 'BLOCKED'},
            {'task_id': uuid.UUID(_SECOND), 'disposition': 'DISPATCH'},
        ]
        assert blocked_claims_of([_FIRST, _SECOND], rows) == {_FIRST}

    def test_another_row_count_is_refused(self) -> None:
        rows = [{'task_id': uuid.UUID(_FIRST), 'disposition': 'DISPATCH'}]
        with pytest.raises(OutcomeDecodeError, match='returned 1 rows for 2 claims'):
            _ = blocked_claims_of([_FIRST, _SECOND], rows)

    def test_a_row_of_another_task_is_refused(self) -> None:
        rows = [
            {'task_id': uuid.UUID(_SECOND), 'disposition': 'DISPATCH'},
            {'task_id': uuid.UUID(_FIRST), 'disposition': 'DISPATCH'},
        ]
        with pytest.raises(OutcomeDecodeError, match='row 1 names task'):
            _ = blocked_claims_of([_FIRST, _SECOND], rows)

    def test_an_unknown_disposition_is_refused(self) -> None:
        rows = [{'task_id': uuid.UUID(_FIRST), 'disposition': 'SKIPPED'}]
        with pytest.raises(OutcomeDecodeError, match="'SKIPPED'"):
            _ = blocked_claims_of([_FIRST], rows)
