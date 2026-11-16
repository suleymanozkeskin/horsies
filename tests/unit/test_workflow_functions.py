"""The typed read of the rows of the workflow SQL functions (``core/workflows/functions.py``)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.workflows.functions import (
    CancelOutcome,
    PauseOutcome,
    decode_cancel_outcome,
    decode_pause_outcome,
)


def _row(outcome: object) -> SimpleNamespace:
    return SimpleNamespace(outcome=outcome)


@pytest.mark.unit
class TestDecodePauseOutcome:
    """One row with an outcome of ``PauseOutcome``; anything else is a defect."""

    @pytest.mark.parametrize('outcome', list(PauseOutcome), ids=lambda o: o.value)
    def test_each_outcome_reads_as_its_member(self, outcome: PauseOutcome) -> None:
        assert decode_pause_outcome([_row(outcome.value)]) is outcome

    def test_no_row_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 0 rows'):
            decode_pause_outcome([])

    def test_two_rows_are_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 2 rows'):
            decode_pause_outcome([_row('PAUSED'), _row('PAUSED')])

    def test_an_unknown_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="'STOPPED'"):
            decode_pause_outcome([_row('STOPPED')])


@pytest.mark.unit
class TestDecodeCancelOutcome:
    """One row with an outcome of ``CancelOutcome``; anything else is a defect."""

    @pytest.mark.parametrize('outcome', list(CancelOutcome), ids=lambda o: o.value)
    def test_each_outcome_reads_as_its_member(self, outcome: CancelOutcome) -> None:
        assert decode_cancel_outcome([_row(outcome.value)]) is outcome

    def test_no_row_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 0 rows'):
            decode_cancel_outcome([])

    def test_two_rows_are_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 2 rows'):
            decode_cancel_outcome([_row('CANCELLED'), _row('CANCELLED')])

    def test_an_unknown_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="'PAUSED'"):
            decode_cancel_outcome([_row('PAUSED')])
