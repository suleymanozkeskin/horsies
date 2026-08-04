"""The verdict rule, checked on distributions whose answer is known.

These are the assertions that make a measurement trustworthy: that an equal
comparison reports no difference, that a clear regression is called a failure,
and — the one that matters most — that too few samples produce "inconclusive"
rather than a confident answer nobody earned.

Everything here is deterministic. The bootstrap draws from a seeded generator,
so a failure is a real change in behavior rather than an unlucky run.
"""

from __future__ import annotations

import pytest

from tests.perf.statistics import (
    Budget,
    Verdict,
    compare,
    percentile_ms,
    worst,
)

pytestmark = [pytest.mark.unit]

_BUDGET = Budget(fraction=0.10, floor_ms=0.5)


def _steady(center: float, count: int = 400) -> list[float]:
    """A tight distribution with a modest tail, as a fast query produces."""
    return [center + (index % 20) * 0.01 for index in range(count)]


class TestPercentile:
    def test_nearest_rank_returns_an_observed_value(self) -> None:
        """An interpolated tail invents a number nothing measured."""
        samples = [1.0, 2.0, 3.0, 100.0]
        assert percentile_ms(samples, 99) in samples

    def test_median_of_known_samples(self) -> None:
        assert percentile_ms([5.0, 1.0, 3.0], 50) == 3.0

    def test_percentile_of_nothing_is_an_error(self) -> None:
        with pytest.raises(ValueError, match='no samples'):
            percentile_ms([], 50)


class TestBudget:
    def test_floor_applies_to_fast_operations(self) -> None:
        """A percentage of a sub-millisecond operation measures scheduling."""
        assert Budget(fraction=0.10, floor_ms=0.5).limit_ms(1.0) == 0.5

    def test_fraction_applies_to_slow_operations(self) -> None:
        assert Budget(fraction=0.10, floor_ms=0.5).limit_ms(100.0) == 10.0


class TestVerdicts:
    def test_equal_distributions_pass_with_an_interval_around_zero(self) -> None:
        """The control property: no difference must read as no difference."""
        samples = _steady(2.0)
        result = compare(
            baseline=samples,
            candidate=list(samples),
            percentile=50,
            budget=_BUDGET,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.PASS
        assert result.ci_low_ms <= 0.0 <= result.ci_high_ms

    def test_clear_regression_fails(self) -> None:
        """Well past the budget, with samples tight enough to prove it."""
        result = compare(
            baseline=_steady(2.0),
            candidate=_steady(5.0),
            percentile=50,
            budget=_BUDGET,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.FAIL
        assert result.ci_low_ms > result.limit_ms

    def test_small_improvement_passes(self) -> None:
        result = compare(
            baseline=_steady(2.0),
            candidate=_steady(1.9),
            percentile=50,
            budget=_BUDGET,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.PASS

    def test_straddling_the_budget_is_inconclusive(self) -> None:
        """Absent evidence must not clear a gate.

        The candidate here is noisy enough that the interval covers both sides
        of the budget: the samples cannot say which side the truth is on, and
        the honest report of that is neither pass nor fail.
        """
        baseline = _steady(2.0)
        candidate = [2.0 + (0.0 if index % 2 else 1.2) for index in range(400)]
        result = compare(
            baseline=baseline,
            candidate=candidate,
            percentile=50,
            budget=_BUDGET,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.INCONCLUSIVE
        assert result.ci_low_ms <= result.limit_ms <= result.ci_high_ms

    def test_bootstrap_is_reproducible_for_a_seed(self) -> None:
        """A recorded interval must be recomputable from the recorded seed."""
        arguments = {
            'baseline': _steady(2.0),
            'candidate': _steady(2.1),
            'percentile': 99.0,
            'budget': _BUDGET,
            'resamples': 200,
            'seed': 11,
        }
        assert compare(**arguments) == compare(**arguments)  # type: ignore[arg-type]

    def test_resamples_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match='at least one resample'):
            compare(
                baseline=_steady(2.0),
                candidate=_steady(2.0),
                percentile=50,
                budget=_BUDGET,
                resamples=0,
                seed=7,
            )


class TestOverallVerdict:
    def test_a_failure_anywhere_fails_the_run(self) -> None:
        assert worst([Verdict.PASS, Verdict.INCONCLUSIVE, Verdict.FAIL]) is (
            Verdict.FAIL
        )

    def test_an_inconclusive_leaves_the_run_inconclusive(self) -> None:
        """The run did not establish what it set out to establish."""
        assert worst([Verdict.PASS, Verdict.INCONCLUSIVE]) is Verdict.INCONCLUSIVE

    def test_all_passing_passes(self) -> None:
        assert worst([Verdict.PASS, Verdict.PASS]) is Verdict.PASS
