"""Ladder hooks: the refit and the ceiling as a stop condition."""

from __future__ import annotations

import pytest

from horsies.core.history.cutover.ladder import (
    LADDER,
    LadderRung,
    MeasuredRun,
    RungBusted,
    RungPassed,
    evaluate_rung,
    fit_coefficients,
)
from horsies.core.history.cutover.preflight import RelocationCoefficients

pytestmark = [pytest.mark.unit]


class TestLadderShape:
    def test_three_rungs_and_only_the_last_is_contingent(self) -> None:
        assert [rung.rows for rung in LADDER] == [
            1_000_000, 11_000_000, 100_000_000,
        ]
        assert [rung.contingent for rung in LADDER] == [
            False, False, True,
        ]


class TestRefit:
    def test_two_points_recover_the_exact_line(self) -> None:
        fitted = fit_coefficients(
            (
                MeasuredRun(rows=1_000_000, seconds=150.0),
                MeasuredRun(rows=11_000_000, seconds=1350.0),
            )
        )
        assert fitted.seconds_per_million_rows == pytest.approx(120.0)
        assert fitted.fixed_seconds == pytest.approx(30.0)

    def test_single_point_pins_the_slope(self) -> None:
        fitted = fit_coefficients(
            (MeasuredRun(rows=2_000_000, seconds=240.0),)
        )
        assert fitted.seconds_per_million_rows == pytest.approx(120.0)
        assert fitted.fixed_seconds == 0.0

    def test_zero_runs_and_identical_rows_are_typed_refusals(self) -> None:
        with pytest.raises(ValueError, match='zero runs'):
            fit_coefficients(())
        with pytest.raises(ValueError, match='identical row counts'):
            fit_coefficients(
                (
                    MeasuredRun(rows=1_000_000, seconds=100.0),
                    MeasuredRun(rows=1_000_000, seconds=110.0),
                )
            )


class TestCeilingStopsTheLadder:
    def test_a_bust_is_a_stop_not_an_override(self) -> None:
        coefficients = RelocationCoefficients(
            seconds_per_million_rows=120.0, fixed_seconds=30.0
        )
        rung = LadderRung(name='one-million', rows=1_000_000, contingent=False)
        # Estimate 150 s, ceiling 187.5 s: 200 s busts it.
        busted = evaluate_rung(
            rung,
            coefficients=coefficients,
            measured=MeasuredRun(rows=1_000_000, seconds=200.0),
        )
        assert isinstance(busted, RungBusted)
        # 180 s passes and refits.
        passed = evaluate_rung(
            rung,
            coefficients=coefficients,
            measured=MeasuredRun(rows=1_000_000, seconds=180.0),
        )
        assert isinstance(passed, RungPassed)
        assert passed.refit.seconds_per_million_rows == pytest.approx(180.0)
