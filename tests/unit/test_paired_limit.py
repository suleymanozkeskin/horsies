"""A row must say which arm decided it, and no row is quotable on one pass."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_cell import SampleUnit
from tests.task_history_prototypes.paired_limit import (
    ConfirmedRow,
    ControlKind,
    LimitArm,
    LimitError,
    PassReading,
    RowOutcome,
    TwoArmedLimit,
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
