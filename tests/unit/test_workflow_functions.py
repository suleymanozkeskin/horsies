"""The typed read of the rows of the workflow SQL functions (``core/workflows/functions.py``)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.workflows.functions import (
    CancelOutcome,
    PausedExpiryOutcome,
    PauseOutcome,
    RaiseCounted,
    RaiseLimitReached,
    RaiseNotRecorded,
    RaisePendingAbsent,
    RecoveryLimitWrite,
    decode_cancel_outcome,
    decode_pause_outcome,
    decode_paused_expiry_outcome,
    decode_phase2_raise,
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


@pytest.mark.unit
class TestDecodePausedExpiryOutcome:
    """One row with an outcome of ``PausedExpiryOutcome``; anything else is a defect."""

    @pytest.mark.parametrize('outcome', list(PausedExpiryOutcome), ids=lambda o: o.value)
    def test_each_outcome_reads_as_its_member(self, outcome: PausedExpiryOutcome) -> None:
        assert decode_paused_expiry_outcome([_row(outcome.value)]) is outcome

    def test_no_row_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 0 rows'):
            decode_paused_expiry_outcome([])

    def test_two_rows_are_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 2 rows'):
            decode_paused_expiry_outcome([_row('EXPIRED'), _row('EXPIRED')])

    def test_an_unknown_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="'CANCELLED'"):
            decode_paused_expiry_outcome([_row('CANCELLED')])


def _raise_row(
    outcome: object,
    attempt_count: object = None,
    limit_outcome: object = None,
    quarantine_verdict: object = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        outcome=outcome,
        attempt_count=attempt_count,
        limit_outcome=limit_outcome,
        quarantine_verdict=quarantine_verdict,
    )


@pytest.mark.unit
class TestDecodePhase2Raise:
    """One row per call; each outcome reads the columns that it fills."""

    def test_pending_absent(self) -> None:
        assert decode_phase2_raise([_raise_row('PENDING_ABSENT')]) == RaisePendingAbsent()

    def test_not_recorded_keeps_the_stored_count(self) -> None:
        assert decode_phase2_raise([_raise_row('NOT_RECORDED', 2)]) == RaiseNotRecorded(
            attempt_count=2,
        )

    def test_counted(self) -> None:
        assert decode_phase2_raise([_raise_row('COUNTED', 1)]) == RaiseCounted(attempt_count=1)

    @pytest.mark.parametrize('limit', list(RecoveryLimitWrite), ids=lambda w: w.value)
    def test_limit_reached_with_a_verdict_and_no_detail(
        self, limit: RecoveryLimitWrite,
    ) -> None:
        decoded = decode_phase2_raise(
            [_raise_row('LIMIT_REACHED', 3, limit.value, 'REPOINTED')],
        )

        assert decoded == RaiseLimitReached(
            attempt_count=3,
            limit=limit,
            quarantine_verdict='REPOINTED',
            quarantine_detail=None,
        )

    def test_limit_reached_with_a_verdict_and_its_detail(self) -> None:
        decoded = decode_phase2_raise(
            [_raise_row('LIMIT_REACHED', 3, 'LIMIT_REACHED_FAILED', 'SOURCE_ABSENT: no row: x')],
        )

        assert decoded == RaiseLimitReached(
            attempt_count=3,
            limit=RecoveryLimitWrite.WORKFLOW_FAILED,
            quarantine_verdict='SOURCE_ABSENT',
            quarantine_detail='no row: x',
        )

    def test_no_row_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='returned 0 rows'):
            decode_phase2_raise([])

    def test_an_unknown_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="'QUARANTINED'"):
            decode_phase2_raise([_raise_row('QUARANTINED', 3)])

    def test_a_count_that_is_not_an_integer_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='attempt_count None'):
            decode_phase2_raise([_raise_row('COUNTED')])

    def test_an_unknown_limit_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="'REFUSED'"):
            decode_phase2_raise([_raise_row('LIMIT_REACHED', 3, 'REFUSED', 'REPOINTED')])

    def test_a_verdict_that_is_not_a_text_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='quarantine verdict None'):
            decode_phase2_raise([_raise_row('LIMIT_REACHED', 3, 'LIMIT_REACHED_FAILED')])
