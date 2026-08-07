"""Ladder hooks: both bounds as stop conditions, the fit's two terms
kept apart by measurement, and the decomposition's seam reported."""

from __future__ import annotations

import pytest

from horsies.core.history.cutover.ladder import (
    LADDER,
    BatchCommit,
    LadderRung,
    MeasuredRun,
    RungBusted,
    RungOverpredicted,
    RungPassed,
    evaluate_rung,
    fit_run,
)
from horsies.core.history.cutover.preflight import RelocationCoefficients

pytestmark = [pytest.mark.unit]


def linear_commits(
    *,
    batches: int,
    batch_rows: int,
    slope_per_million: float,
    intercept: float,
) -> tuple[BatchCommit, ...]:
    return tuple(
        BatchCommit(
            cumulative_rows=batch_rows * i,
            elapsed_seconds=(
                intercept
                + slope_per_million * (batch_rows * i) / 1_000_000
            ),
        )
        for i in range(1, batches + 1)
    )


class TestLadderShape:
    def test_three_rungs_and_only_the_last_is_contingent(self) -> None:
        assert [rung.rows for rung in LADDER] == [
            1_000_000, 10_000_000, 100_000_000,
        ]
        assert [rung.contingent for rung in LADDER] == [
            False, False, True,
        ]


class TestFit:
    def test_slope_from_the_trajectory_fixed_as_measured(self) -> None:
        """A 120s-fixed / 30s-per-million workload: the one-total fit
        inferred slope 150 and fixed 0; this fit recovers slope 30 from
        the intra-run trajectory and carries the measured 120 fixed."""
        commits = linear_commits(
            batches=10, batch_rows=100_000,
            slope_per_million=30.0, intercept=0.0,
        )
        fitted = fit_run(
            MeasuredRun(
                rows=1_000_000, seconds=150.0,
                fixed_seconds=120.0, commits=commits,
            )
        )
        assert fitted.coefficients.seconds_per_million_rows == (
            pytest.approx(30.0)
        )
        assert fitted.coefficients.fixed_seconds == 120.0

    def test_the_regression_intercept_is_reported_beside(self) -> None:
        """The seam: the regression's own intercept estimates the same
        quantity as the measured fixed term by an independent route —
        agreement validates the decomposition, disagreement exposes
        it."""
        commits = linear_commits(
            batches=8, batch_rows=125_000,
            slope_per_million=30.0, intercept=7.5,
        )
        fitted = fit_run(
            MeasuredRun(
                rows=1_000_000, seconds=157.5,
                fixed_seconds=120.0, commits=commits,
            )
        )
        assert fitted.regression_intercept_seconds == pytest.approx(7.5)
        assert fitted.coefficients.fixed_seconds == 120.0

    def test_fewer_than_two_distinct_points_is_refused(self) -> None:
        single = (
            BatchCommit(cumulative_rows=100_000, elapsed_seconds=3.0),
        )
        with pytest.raises(ValueError, match='two distinct'):
            fit_run(
                MeasuredRun(
                    rows=100_000, seconds=3.0,
                    fixed_seconds=0.0, commits=single,
                )
            )
        repeated = (
            BatchCommit(cumulative_rows=100_000, elapsed_seconds=3.0),
            BatchCommit(cumulative_rows=100_000, elapsed_seconds=3.5),
        )
        with pytest.raises(ValueError, match='two distinct'):
            fit_run(
                MeasuredRun(
                    rows=100_000, seconds=3.5,
                    fixed_seconds=0.0, commits=repeated,
                )
            )


class TestBothBoundsStopTheLadder:
    def test_ceiling_bust_stops(self) -> None:
        coefficients = RelocationCoefficients(
            seconds_per_million_rows=120.0, fixed_seconds=30.0
        )
        rung = LadderRung(
            name='one-million', rows=1_000_000, contingent=False
        )
        # Estimate 150 s, ceiling 187.5 s: 200 s busts it.
        busted = evaluate_rung(
            rung,
            coefficients=coefficients,
            measured=MeasuredRun(
                rows=1_000_000, seconds=200.0,
                fixed_seconds=30.0, commits=(),
            ),
        )
        assert isinstance(busted, RungBusted)

    def test_floor_bust_catches_the_silent_over_prediction(self) -> None:
        """The demonstrated interaction: a 120s-fixed / 30s-per-million
        workload measured 150 s at one million; the one-total fit gave
        slope 150 / fixed 0, predicting 1500 s at ten million where
        the true time is 420 s — ratio 0.28, sailing under the ceiling
        forever. The floor names it disproven."""
        inflated = RelocationCoefficients(
            seconds_per_million_rows=150.0, fixed_seconds=0.0
        )
        rung = LadderRung(
            name='ten-million', rows=10_000_000, contingent=False
        )
        outcome = evaluate_rung(
            rung,
            coefficients=inflated,
            measured=MeasuredRun(
                rows=10_000_000, seconds=420.0,
                fixed_seconds=120.0, commits=(),
            ),
        )
        assert isinstance(outcome, RungOverpredicted)

    def test_in_bounds_passes_and_refits_from_the_trajectory(self) -> None:
        coefficients = RelocationCoefficients(
            seconds_per_million_rows=30.0, fixed_seconds=120.0
        )
        rung = LadderRung(
            name='one-million', rows=1_000_000, contingent=False
        )
        commits = linear_commits(
            batches=10, batch_rows=100_000,
            slope_per_million=32.0, intercept=1.0,
        )
        passed = evaluate_rung(
            rung,
            coefficients=coefficients,
            measured=MeasuredRun(
                rows=1_000_000, seconds=153.0,
                fixed_seconds=119.0, commits=commits,
            ),
        )
        assert isinstance(passed, RungPassed)
        assert passed.refit.coefficients.seconds_per_million_rows == (
            pytest.approx(32.0)
        )
        assert passed.refit.coefficients.fixed_seconds == 119.0
        assert passed.refit.regression_intercept_seconds == (
            pytest.approx(1.0)
        )
