"""A row must say which arm decided it, and no row is quotable on one pass."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_cell import SampleUnit
from tests.task_history_prototypes.paired_limit import (
    WIDTH_THRESHOLD_RATIO,
    BootstrapInterval,
    ConfirmedRow,
    ControlKind,
    IntervalVerdict,
    LimitArm,
    LimitError,
    PassReading,
    RowOutcome,
    TwoArmedLimit,
    judge_containment,
    judge_magnitude,
    median,
    percentile,
)

P50 = TwoArmedLimit(
    statistic='p50',
    relative=0.05,
    absolute=0.20,
    unit=SampleUnit.MILLISECONDS,
    control=ControlKind.BASELINE_BUILD,
)
KEYED = TwoArmedLimit(
    statistic='p50',
    relative=0.25,
    absolute=1.00,
    unit=SampleUnit.MILLISECONDS,
    control=ControlKind.SIBLING_OPERATION,
)


def _reading(
    index: int, control: float, candidate: float, limit: TwoArmedLimit = P50
) -> PassReading:
    return PassReading(
        pass_index=index,
        control_value=control,
        candidate_value=candidate,
        limit=limit,
    )


def test_the_absolute_arm_governs_a_fast_operation() -> None:
    """Five per cent of 1.6 ms is 0.082 ms, well under the 0.20 ms arm."""
    reading = _reading(0, 1.578, 1.648)
    assert reading.governing_arm is LimitArm.ABSOLUTE
    assert reading.allowance == pytest.approx(0.20)
    assert reading.within_limit


def test_the_relative_arm_governs_a_slow_operation() -> None:
    """The same limit, a slower control, and the other arm decides."""
    reading = _reading(0, 40.0, 41.0)
    assert reading.governing_arm is LimitArm.RELATIVE
    assert reading.allowance == pytest.approx(2.0)
    assert reading.within_limit


def test_both_arms_are_recorded_not_only_the_governing_one() -> None:
    """A reader can see how far from the crossover the row sat."""
    conditions = _reading(0, 1.578, 1.648).as_conditions()
    assert conditions['relative_arm_allowance'] == pytest.approx(0.0789)
    assert conditions['absolute_arm_allowance'] == pytest.approx(0.20)
    assert conditions['governing_arm'] == 'absolute'


def test_a_delta_beyond_the_allowance_is_outside_the_limit() -> None:
    assert not _reading(0, 1.578, 1.900).within_limit


def test_a_faster_candidate_is_within_the_limit() -> None:
    """The limits bound a regression, not a difference."""
    assert _reading(0, 2.454, 2.277).within_limit


def test_one_pass_is_a_reading_not_a_row() -> None:
    """Between-pass movement here is comparable to the relative arm."""
    with pytest.raises(LimitError, match='at least two'):
        ConfirmedRow(
            name='ordinary-enqueue-p50',
            limit=P50,
            readings=(_reading(0, 1.578, 1.648),),
        )


def test_two_agreeing_passes_confirm_a_row() -> None:
    row = ConfirmedRow(
        name='ordinary-enqueue-p50',
        limit=P50,
        readings=(_reading(0, 1.578, 1.648), _reading(1, 1.579, 1.667)),
    )
    assert row.outcome is RowOutcome.CONFIRMED_WITHIN
    assert row.quotable


def test_two_agreeing_failures_confirm_a_row_too() -> None:
    row = ConfirmedRow(
        name='r',
        limit=P50,
        readings=(_reading(0, 1.5, 2.0), _reading(1, 1.5, 2.1)),
    )
    assert row.outcome is RowOutcome.CONFIRMED_EXCEEDED
    assert row.quotable


def test_passes_that_disagree_are_recorded_as_a_disagreement() -> None:
    """Not resolved by preferring one; a disagreement is about the instrument."""
    row = ConfirmedRow(
        name='r',
        limit=P50,
        readings=(_reading(0, 1.5, 1.6), _reading(1, 1.5, 2.0)),
    )
    assert row.outcome is RowOutcome.DISAGREED
    assert not row.quotable


def test_a_sign_flip_between_passes_is_recorded() -> None:
    """A recorded fact, not a discarded one."""
    row = ConfirmedRow(
        name='ordinary-enqueue-p99',
        limit=P50,
        readings=(_reading(0, 2.454, 2.277), _reading(1, 2.309, 2.389)),
    )
    assert row.as_conditions()['sign_flipped'] is True


def test_every_pass_survives_into_the_conditions() -> None:
    """A first reading is never erased by a second."""
    row = ConfirmedRow(
        name='r',
        limit=P50,
        readings=(_reading(0, 1.5, 1.6), _reading(1, 1.6, 1.7)),
    )
    passes = row.as_conditions()['passes']
    assert [entry['pass'] for entry in passes] == [0, 1]
    assert [entry['control'] for entry in passes] == [1.5, 1.6]


def test_passes_recorded_out_of_order_are_refused() -> None:
    with pytest.raises(LimitError, match='in the order they ran'):
        ConfirmedRow(
            name='r',
            limit=P50,
            readings=(_reading(1, 1.5, 1.6), _reading(0, 1.5, 1.6)),
        )


def test_a_pass_judged_against_another_limit_is_refused() -> None:
    with pytest.raises(LimitError, match='different limit'):
        ConfirmedRow(
            name='r',
            limit=P50,
            readings=(_reading(0, 1.5, 1.6), _reading(1, 1.5, 1.6, KEYED)),
        )


def test_the_keyed_row_names_its_control_as_a_sibling_operation() -> None:
    """Forced, not chosen: the released build takes no key at all."""
    assert KEYED.control is ControlKind.SIBLING_OPERATION
    assert KEYED.as_conditions()['control'] == 'sibling-operation'


@pytest.mark.parametrize('relative,absolute', [(0.0, 1.0), (-0.1, 1.0), (0.05, 0.0)])
def test_a_limit_with_a_non_positive_arm_is_refused(
    relative: float, absolute: float
) -> None:
    with pytest.raises(LimitError, match='must be positive'):
        TwoArmedLimit(
            statistic='p50',
            relative=relative,
            absolute=absolute,
            unit=SampleUnit.MILLISECONDS,
            control=ControlKind.BASELINE_BUILD,
        )


def test_a_percentile_that_would_be_the_maximum_is_refused() -> None:
    """A percentile read from a sample too small to contain it is the maximum."""
    with pytest.raises(LimitError, match='is the maximum of the sample'):
        percentile([float(value) for value in range(50)], 0.99)


def test_a_percentile_with_room_beneath_the_maximum_is_returned() -> None:
    # Nearest rank on a zero-based index: 0.99 x 999 rounds to 989.
    values = [float(value) for value in range(1000)]
    assert percentile(values, 0.99) == 989.0
    assert percentile(values, 0.5) < percentile(values, 0.99)
    assert percentile(values, 0.99) < max(values)


def test_a_percentile_of_nothing_is_refused() -> None:
    with pytest.raises(LimitError, match='at least one observation'):
        percentile([], 0.5)


@pytest.mark.parametrize('fraction', [0.0, 1.0, -0.1, 1.5])
def test_a_fraction_outside_the_open_unit_interval_is_refused(
    fraction: float,
) -> None:
    with pytest.raises(LimitError, match='must be in'):
        percentile([1.0, 2.0, 3.0], fraction)


def test_the_median_of_an_even_sample_averages_the_middle_pair() -> None:
    assert median([1.0, 2.0, 3.0, 4.0]) == 2.5
    assert median([1.0, 2.0, 3.0]) == 2.0


def test_a_median_of_nothing_is_refused() -> None:
    with pytest.raises(LimitError, match='at least one observation'):
        median([])


def test_a_relative_delta_against_a_zero_control_is_refused() -> None:
    with pytest.raises(LimitError, match='undefined'):
        _reading(0, 0.0, 1.0).relative_delta


def test_a_bootstrap_interval_is_reproducible_from_its_seed() -> None:
    """A reader with the samples and the seed gets the same interval back."""
    from tests.task_history_prototypes.paired_limit import bootstrap_delta

    control = [1.5 + (index % 7) * 0.01 for index in range(500)]
    candidate = [1.6 + (index % 7) * 0.01 for index in range(500)]
    first = bootstrap_delta(control, candidate, statistic='p50')
    second = bootstrap_delta(control, candidate, statistic='p50')
    assert (first.low, first.high) == (second.low, second.high)
    assert first.seed == second.seed


def test_a_different_seed_gives_a_different_interval() -> None:
    """Which is why the seed is recorded rather than left implicit.

    Uses samples with the variety a real run has: on low-cardinality data every
    resample returns the same median and the interval collapses regardless of
    seed, which would make this test pass for the wrong reason.
    """
    from tests.task_history_prototypes.paired_limit import bootstrap_delta

    control = [1.5 + index * 0.0007 for index in range(500)]
    candidate = [1.6 + index * 0.0009 for index in range(500)]
    first = bootstrap_delta(control, candidate, statistic='p50', seed=1)
    second = bootstrap_delta(control, candidate, statistic='p50', seed=2)
    assert (first.low, first.high) != (second.low, second.high)


def test_the_interval_brackets_its_point_estimate() -> None:
    from tests.task_history_prototypes.paired_limit import bootstrap_delta

    control = [1.5 + (index % 7) * 0.01 for index in range(500)]
    candidate = [1.6 + (index % 7) * 0.01 for index in range(500)]
    interval = bootstrap_delta(control, candidate, statistic='p50')
    assert interval.low <= interval.point <= interval.high
    assert interval.excludes_zero


def test_two_samples_from_one_population_do_not_exclude_zero() -> None:
    """The check that says a delta inside its limit is a delta at all."""
    from tests.task_history_prototypes.paired_limit import bootstrap_delta

    values = [1.5 + (index % 17) * 0.03 for index in range(600)]
    interval = bootstrap_delta(values, list(values), statistic='p50')
    assert not interval.excludes_zero


def test_too_few_resamples_are_refused() -> None:
    from tests.task_history_prototypes.paired_limit import bootstrap_delta

    with pytest.raises(LimitError, match='at least 1000'):
        bootstrap_delta([1.0, 2.0], [1.0, 2.0], statistic='p50', resamples=100)


def test_an_empty_side_cannot_be_resampled() -> None:
    from tests.task_history_prototypes.paired_limit import bootstrap_delta

    with pytest.raises(LimitError, match='both sides need observations'):
        bootstrap_delta([], [1.0], statistic='p50')


def test_the_conditions_carry_the_seed_and_the_resample_count() -> None:
    from tests.task_history_prototypes.paired_limit import (
        BOOTSTRAP_RESAMPLES,
        BOOTSTRAP_SEED,
        bootstrap_delta,
    )

    control = [1.5 + (index % 7) * 0.01 for index in range(500)]
    candidate = [1.6 + (index % 7) * 0.01 for index in range(500)]
    conditions = bootstrap_delta(
        control, candidate, statistic='p50'
    ).as_conditions()
    assert conditions['resamples'] == BOOTSTRAP_RESAMPLES
    assert conditions['seed'] == BOOTSTRAP_SEED
    assert conditions['confidence'] == 0.95


def _interval(low: float, high: float, statistic: str = 'p99') -> BootstrapInterval:
    return BootstrapInterval(
        statistic=statistic,
        point=(low + high) / 2.0,
        low=low,
        high=high,
        confidence=0.95,
        resamples=1_000,
        seed=20260808,
    )


# The four intervals below are the measured no-blocking rows: a paired leaf
# creation under an unrelated class parent while a detach waits behind a
# reader, judged against a 100 ms budget, at 100 and at 1,000 observations per
# arm.
NO_BLOCKING_INTERVALS = (
    ('p50 at 100 per arm', _interval(-6.8, 3.1, 'p50'), IntervalVerdict.WITHIN),
    ('p50 at 1000 per arm', _interval(14.3, 17.8, 'p50'), IntervalVerdict.WITHIN),
    ('p99 at 100 per arm', _interval(-126.8, 1692.5), IntervalVerdict.UNDECIDED),
    ('p99 at 1000 per arm', _interval(-35.5, 50.5), IntervalVerdict.WITHIN),
)


def test_an_interval_wholly_beneath_the_bound_is_within() -> None:
    verdict = judge_containment(_interval(-35.5, 50.5), bound=100.0)
    assert verdict is IntervalVerdict.WITHIN


def test_an_interval_wholly_above_the_bound_is_exceeded() -> None:
    verdict = judge_containment(_interval(140.0, 260.0), bound=100.0)
    assert verdict is IntervalVerdict.EXCEEDED


def test_an_interval_straddling_the_bound_decides_nothing() -> None:
    """The point estimate sits well inside the budget; the interval does not."""
    straddling = _interval(-126.8, 1692.5)
    assert straddling.point < 100.0 or straddling.high > 100.0
    assert judge_containment(straddling, bound=100.0) is IntervalVerdict.UNDECIDED


def test_containment_reproduces_the_measured_no_blocking_verdicts() -> None:
    """Regression: the gate must return the verdicts these rows were held at,
    including the p99 at 100 per arm that was withheld as unconstrained."""
    judged = {
        name: judge_containment(interval, bound=100.0)
        for name, interval, _ in NO_BLOCKING_INTERVALS
    }
    assert judged == {
        name: expected for name, _, expected in NO_BLOCKING_INTERVALS
    }


@pytest.mark.parametrize('bound', [0.0, -1.0])
def test_containment_against_a_non_positive_bound_is_refused(
    bound: float,
) -> None:
    with pytest.raises(LimitError, match='must be positive'):
        judge_containment(_interval(1.0, 2.0), bound=bound)


def test_a_decided_verdict_can_still_carry_an_unquotable_magnitude() -> None:
    """p99 at 1,000 per arm: within the budget, sign of the delta unresolved."""
    interval = _interval(-35.5, 50.5)
    assert judge_containment(interval, bound=100.0) is IntervalVerdict.WITHIN
    judgement = judge_magnitude(interval, bound=100.0)
    assert not judgement.sign_resolved
    assert not judgement.quotable


def test_a_magnitude_whose_sign_is_established_is_quotable() -> None:
    judgement = judge_magnitude(_interval(14.3, 17.8, 'p50'), bound=100.0)
    assert judgement.sign_resolved
    assert judgement.quotable


def test_the_shipped_width_gate_enforces_its_derived_threshold() -> None:
    """The threshold is derived and binding, not recorded and inert."""
    assert WIDTH_THRESHOLD_RATIO == 0.50
    judgement = judge_magnitude(_interval(-126.8, 1692.5), bound=100.0)
    assert judgement.width_ratio == pytest.approx(18.193, abs=0.001)
    assert judgement.width_threshold == 0.50
    assert not judgement.width_within_threshold
    assert judgement.as_conditions()['width_enforced'] is True


def test_the_shipped_threshold_refuses_the_specimen_that_motivated_it() -> None:
    """p99 at 1,000 per arm: decided WITHIN, sign of its own delta unresolved."""
    judgement = judge_magnitude(_interval(-35.5, 50.5), bound=100.0)
    assert judgement.width_ratio == pytest.approx(0.86)
    assert not judgement.width_within_threshold
    assert not judgement.quotable


# Every width ratio measured while siting the threshold, sorted. The gate must
# separate them where the empty band is — informative below, refused above —
# or 0.50 is sited on numbers it does not actually divide.
SITING_RATIOS_INFORMATIVE = (0.039, 0.096, 0.113, 0.147, 0.235, 0.266, 0.412)
SITING_RATIOS_REFUSED = (
    0.860, 0.902, 0.925, 1.186, 1.292, 2.635, 3.178, 12.388, 18.193,
)


@pytest.mark.parametrize('ratio', SITING_RATIOS_INFORMATIVE)
def test_every_ratio_below_the_empty_band_is_admitted(ratio: float) -> None:
    judgement = judge_magnitude(_interval(0.0, ratio * 100.0), bound=100.0)
    assert judgement.width_within_threshold


@pytest.mark.parametrize('ratio', SITING_RATIOS_REFUSED)
def test_every_ratio_above_the_empty_band_is_refused(ratio: float) -> None:
    judgement = judge_magnitude(_interval(0.0, ratio * 100.0), bound=100.0)
    assert not judgement.width_within_threshold


def test_the_threshold_sits_in_an_empty_band() -> None:
    """No observation lies near it, so re-measurement will not move the split."""
    below = max(SITING_RATIOS_INFORMATIVE)
    above = min(SITING_RATIOS_REFUSED)
    assert below < WIDTH_THRESHOLD_RATIO < above
    assert WIDTH_THRESHOLD_RATIO / below > 1.19
    assert above / WIDTH_THRESHOLD_RATIO > 1.19


def test_supplying_a_width_threshold_refuses_a_too_wide_magnitude() -> None:
    """The comparison answers to its threshold, not to a fixed width."""
    interval = _interval(-35.5, 50.5)
    # The same interval, admitted by a threshold wide enough to hold it.
    assert judge_magnitude(
        interval, bound=100.0, width_threshold=1.0
    ).width_within_threshold
    judgement = judge_magnitude(interval, bound=100.0, width_threshold=0.5)
    assert judgement.width_ratio == pytest.approx(0.86)
    assert not judgement.width_within_threshold
    assert not judgement.quotable
    assert judgement.as_conditions()['width_enforced'] is True


def test_a_width_threshold_a_narrow_interval_meets_leaves_it_quotable() -> None:
    judgement = judge_magnitude(
        _interval(14.3, 17.8, 'p50'), bound=100.0, width_threshold=0.5
    )
    assert judgement.width_ratio == pytest.approx(0.035)
    assert judgement.quotable


@pytest.mark.parametrize('threshold', [0.0, -0.25])
def test_a_non_positive_width_threshold_is_refused(threshold: float) -> None:
    with pytest.raises(LimitError, match='width threshold must be positive'):
        judge_magnitude(_interval(1.0, 2.0), bound=100.0, width_threshold=threshold)


def test_a_magnitude_against_a_non_positive_bound_is_refused() -> None:
    with pytest.raises(LimitError, match='must be positive'):
        judge_magnitude(_interval(1.0, 2.0), bound=0.0)


def test_an_established_sign_does_not_by_itself_make_a_magnitude_quotable() -> None:
    """Both conditions bind: the sign is settled here and the width still refuses."""
    judgement = judge_magnitude(
        _interval(10.0, 90.0), bound=100.0, width_threshold=0.5
    )
    assert judgement.sign_resolved
    assert not judgement.width_within_threshold
    assert not judgement.quotable


def test_an_explicitly_unenforced_width_records_without_refusing() -> None:
    """Passing None opts out of the width gate; the ratio is still recorded.

    The shipped threshold makes this branch unreachable by default, which is
    exactly why it needs a test of its own — an unreachable branch nobody
    exercises is where a defect waits.
    """
    judgement = judge_magnitude(
        _interval(-126.8, 1692.5), bound=100.0, width_threshold=None
    )
    assert judgement.width_ratio == pytest.approx(18.193, abs=0.001)
    assert judgement.width_within_threshold
    assert judgement.as_conditions()['width_enforced'] is False


def test_an_unresolved_sign_refuses_a_magnitude_the_width_gate_admits() -> None:
    """Sign binds on its own, where width has nothing to say.

    The interval is narrow enough for the width gate and still spans zero, so
    only the sign condition can refuse it. Without a case like this the sign
    half of the conjunction is untested whenever the width half already
    refuses.
    """
    judgement = judge_magnitude(_interval(-10.0, 20.0), bound=100.0)
    assert judgement.width_ratio == pytest.approx(0.30)
    assert judgement.width_within_threshold
    assert not judgement.sign_resolved
    assert not judgement.quotable
