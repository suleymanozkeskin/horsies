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

from crossclient.perf.statistics import (
    Budget,
    UnitCost,
    Verdict,
    compare,
    compare_cost,
    compare_throughput,
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


class TestThroughputVerdicts:
    def test_equal_batch_throughput_passes(self) -> None:
        samples = _steady(10.0)
        result = compare_throughput(
            baseline_ms=samples,
            candidate_ms=list(samples),
            rows_per_operation=500,
            minimum_ratio=0.90,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.PASS
        assert result.ci_low_ratio <= 1.0 <= result.ci_high_ratio

    def test_clear_batch_throughput_regression_fails(self) -> None:
        result = compare_throughput(
            baseline_ms=_steady(10.0),
            candidate_ms=_steady(20.0),
            rows_per_operation=500,
            minimum_ratio=0.90,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.FAIL
        assert result.ci_high_ratio < result.minimum_ratio

    def test_crossing_batch_throughput_interval_is_inconclusive(self) -> None:
        baseline = _steady(10.0)
        candidate = [10.0 if index % 2 else 12.4 for index in range(400)]
        result = compare_throughput(
            baseline_ms=baseline,
            candidate_ms=candidate,
            rows_per_operation=500,
            minimum_ratio=0.90,
            resamples=200,
            seed=7,
        )
        assert result.verdict is Verdict.INCONCLUSIVE
        assert result.ci_low_ratio < 0.90 <= result.ci_high_ratio

    @pytest.mark.parametrize(
        ('samples', 'message'),
        [([], 'no samples'), ([1.0, 0.0], 'must be positive')],
    )
    def test_invalid_durations_are_rejected(
        self,
        samples: list[float],
        message: str,
    ) -> None:
        with pytest.raises(ValueError, match=message):
            compare_throughput(
                baseline_ms=samples,
                candidate_ms=[1.0],
                rows_per_operation=500,
                minimum_ratio=0.90,
                resamples=200,
                seed=7,
            )


class TestCostComparison:
    """The cost per operation of each side and their ratio, with no verdict."""

    @staticmethod
    def _units(seconds: float, count: int = 10) -> list[UnitCost]:
        return [
            UnitCost(operations=1_000, seconds=seconds + index * 0.001)
            for index in range(count)
        ]

    def test_equal_costs_have_a_ratio_of_one_inside_the_interval(self) -> None:
        compared = compare_cost(
            baseline=self._units(1.0),
            candidate=self._units(1.0),
            resamples=500,
            seed=7,
        )
        assert compared.ratio == pytest.approx(1.0)
        assert compared.ci_low_ratio <= 1.0 <= compared.ci_high_ratio

    def test_the_cost_per_operation_is_the_total_over_the_operations(self) -> None:
        compared = compare_cost(
            baseline=[UnitCost(operations=100, seconds=1.0)],
            candidate=[
                UnitCost(operations=100, seconds=1.0),
                UnitCost(operations=300, seconds=5.0),
            ],
            resamples=10,
            seed=7,
        )
        assert compared.baseline_per_operation_s == pytest.approx(0.01)
        assert compared.candidate_per_operation_s == pytest.approx(0.015)
        assert compared.ratio == pytest.approx(1.5)

    def test_a_doubled_cost_has_an_interval_above_one(self) -> None:
        compared = compare_cost(
            baseline=self._units(1.0),
            candidate=self._units(2.0),
            resamples=500,
            seed=7,
        )
        assert compared.ci_low_ratio > 1.0

    @pytest.mark.parametrize(
        ('baseline', 'candidate'),
        [
            ([], [UnitCost(operations=1, seconds=1.0)]),
            (
                [UnitCost(operations=0, seconds=1.0)],
                [UnitCost(operations=1, seconds=1.0)],
            ),
            (
                [UnitCost(operations=1, seconds=1.0)],
                [UnitCost(operations=1, seconds=-1.0)],
            ),
            (
                [UnitCost(operations=1, seconds=0.0)],
                [UnitCost(operations=1, seconds=1.0)],
            ),
        ],
        ids=['no-unit', 'no-operation', 'negative-cost', 'free-baseline'],
    )
    def test_a_cost_that_cannot_be_compared_is_refused(
        self,
        baseline: list[UnitCost],
        candidate: list[UnitCost],
    ) -> None:
        with pytest.raises(ValueError):
            _ = compare_cost(
                baseline=baseline, candidate=candidate, resamples=10, seed=7
            )
