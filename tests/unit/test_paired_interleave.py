"""The observation order must not hand the window's drift to the build."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_interleave import (
    BlockOrder,
    InterleaveError,
    InterleaveSpec,
    ScheduledObservation,
    assert_no_long_runs,
    assert_schedule_matches_its_model,
    block_sides,
    DRIFT_MODEL,
    drift_attribution_bound,
    expected_position_gap,
    interleave_conditions,
    interleave_schedule,
    mean_position_gap,
    measured,
    observed_drift_per_observation,
    permitted_run_observations,
    partition_samples,
)
from tests.task_history_prototypes.paired_sides import PairedSide

DRIFT_PER_OBSERVATION = 0.05


def _unit(order: BlockOrder) -> int:
    return 2 if order is BlockOrder.ALTERNATING else 4


def _sequential_schedule(
    *, per_side: int
) -> tuple[ScheduledObservation, ...]:
    """The schedule this module exists to refuse.

    Every baseline observation, then every candidate observation. It runs
    without complaint and attributes the whole window's drift to the build.
    """
    entries: list[ScheduledObservation] = []
    for side in (PairedSide.BASELINE, PairedSide.CANDIDATE):
        for _ in range(per_side):
            entries.append(
                ScheduledObservation(
                    global_index=len(entries),
                    block=0 if side is PairedSide.BASELINE else 1,
                    side=side,
                    warmup=False,
                )
            )
    return tuple(entries)


def _drifting_samples(
    schedule: tuple[ScheduledObservation, ...],
    *,
    true_value: float = 10.0,
    drift_per_observation: float = DRIFT_PER_OBSERVATION,
) -> tuple[float, ...]:
    """Both builds are identical; only the machine changes as the run proceeds.

    Any delta a schedule reports from these samples is drift wearing the
    build's name, because there is no build difference in them to find.
    """
    return tuple(
        true_value + drift_per_observation * entry.global_index
        for entry in schedule
    )


def _mean(values: tuple[float, ...]) -> float:
    return sum(values) / len(values)


def test_counterbalanced_blocks_mirror_each_pair() -> None:
    spec = InterleaveSpec(blocks=8, block_size=3, warmup_blocks=4)
    assert [side.value for side in block_sides(spec)] == [
        'baseline', 'candidate', 'candidate', 'baseline',
        'baseline', 'candidate', 'candidate', 'baseline',
    ]


def test_alternating_blocks_simply_take_turns() -> None:
    spec = InterleaveSpec(blocks=4, block_size=3, warmup_blocks=2, order=BlockOrder.ALTERNATING)
    assert [side.value for side in block_sides(spec)] == [
        'baseline', 'candidate', 'baseline', 'candidate',
    ]


def test_a_counterbalanced_schedule_has_no_position_gap() -> None:
    """Zero gap is what makes a monotonic drift unable to produce a delta."""
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    assert mean_position_gap(schedule) == 0.0
    assert expected_position_gap(spec) == 0.0


def test_an_alternating_schedule_leaves_exactly_one_block_of_gap() -> None:
    """The residual is stated rather than assumed away."""
    spec = InterleaveSpec(blocks=6, block_size=4, warmup_blocks=2, order=BlockOrder.ALTERNATING)
    schedule = interleave_schedule(spec)
    assert mean_position_gap(schedule) == 4.0
    assert expected_position_gap(spec) == 4.0


def test_a_built_schedule_matches_the_model_its_bound_came_from() -> None:
    for order in (BlockOrder.ALTERNATING, BlockOrder.COUNTERBALANCED):
        spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4, order=order)
        assert_schedule_matches_its_model(spec, interleave_schedule(spec))


def test_sequential_observation_reports_drift_as_a_build_difference() -> None:
    """The falsification: identical builds, a drifting machine, two orders.

    The samples carry no build difference at all. A sequential order still
    reports a large one, and it is exactly the drift rate multiplied by the
    distance between the two sides' mean positions.
    """
    sequential = _sequential_schedule(per_side=20)
    samples = _drifting_samples(sequential)
    baseline, candidate = partition_samples(sequential, samples)
    delta = _mean(candidate) - _mean(baseline)
    assert delta == pytest.approx(
        DRIFT_PER_OBSERVATION * mean_position_gap(sequential)
    )
    assert delta == pytest.approx(1.0)


def test_counterbalanced_observation_reports_no_difference_at_all() -> None:
    """Same identical builds, same drifting machine, an order that cancels it."""
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    baseline, candidate = partition_samples(schedule, _drifting_samples(schedule))
    assert _mean(candidate) - _mean(baseline) == pytest.approx(0.0)


def test_alternating_observation_leaves_the_residual_it_declares() -> None:
    """Its bound is honest: the delta it reports is the bound, not less."""
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=2, order=BlockOrder.ALTERNATING)
    schedule = interleave_schedule(spec)
    baseline, candidate = partition_samples(schedule, _drifting_samples(schedule))
    delta = _mean(candidate) - _mean(baseline)
    bound = drift_attribution_bound(
        spec, drift_per_observation=DRIFT_PER_OBSERVATION
    )
    assert delta == pytest.approx(bound.bound)
    assert bound.bound == pytest.approx(0.25)
    assert bound.model == DRIFT_MODEL
    assert 'warm-up transient' in bound.excludes


def test_the_sequential_schedule_is_refused_outright() -> None:
    """It is caught by its shape, before anyone reads its numbers."""
    spec = InterleaveSpec(blocks=8, block_size=3, warmup_blocks=4)
    assert permitted_run_observations(spec) == 6
    with pytest.raises(InterleaveError, match='consecutive observations'):
        assert_no_long_runs(spec, _sequential_schedule(per_side=12))


def test_a_run_written_as_one_oversized_block_is_still_refused() -> None:
    """Counting blocks would miss this; a block is whatever a schedule says.

    The sequential failure written as a single block per side has a block
    run-length of one, so any block-counting check waves it through.
    """
    spec = InterleaveSpec(blocks=8, block_size=3, warmup_blocks=4)
    sequential = _sequential_schedule(per_side=12)
    assert len({entry.block for entry in sequential}) == 2
    with pytest.raises(InterleaveError, match='consecutive observations'):
        assert_no_long_runs(spec, sequential)


def test_a_real_schedule_survives_the_run_length_check() -> None:
    for order in (BlockOrder.ALTERNATING, BlockOrder.COUNTERBALANCED):
        spec = InterleaveSpec(blocks=8, block_size=3, warmup_blocks=4, order=order)
        assert_no_long_runs(spec, interleave_schedule(spec))


def test_a_schedule_with_unequal_sides_is_refused() -> None:
    """Unequal sides mean one build was sampled more thoroughly than the other."""
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    with pytest.raises(InterleaveError, match='observations'):
        assert_schedule_matches_its_model(spec, _sequential_schedule(per_side=20))


def test_a_correctly_counted_but_wrongly_ordered_schedule_is_refused() -> None:
    """Equal counts are not enough; where the observations sit is the point.

    This schedule gives each side exactly the observations its spec calls for
    and still runs them sequentially, so its gap is nothing like the zero its
    ordering claims.
    """
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    real = interleave_schedule(spec)
    counted = measured(real)
    resequenced = tuple(
        ScheduledObservation(
            global_index=entry.global_index,
            block=entry.block,
            side=(
                PairedSide.BASELINE
                if position < len(counted) // 2
                else PairedSide.CANDIDATE
            ),
            warmup=False,
        )
        for position, entry in enumerate(counted)
    )
    with pytest.raises(InterleaveError, match='mean-position gap'):
        assert_schedule_matches_its_model(spec, resequenced)


def test_warmup_is_discarded_and_the_rest_stays_balanced() -> None:
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    assert len(schedule) == 40
    assert len(measured(schedule)) == 20
    assert mean_position_gap(schedule) == 0.0
    assert spec.measured_observations_per_side() == 10


def test_warmup_samples_never_reach_a_side() -> None:
    spec = InterleaveSpec(blocks=8, block_size=2, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    samples = tuple(float(index) for index in range(len(schedule)))
    baseline, candidate = partition_samples(schedule, samples)
    assert min(baseline + candidate) == 8.0
    assert len(baseline) == len(candidate) == 4


@pytest.mark.parametrize(
    'blocks,order',
    [
        (6, BlockOrder.COUNTERBALANCED),
        (3, BlockOrder.ALTERNATING),
        (2, BlockOrder.COUNTERBALANCED),
    ],
)
def test_a_partial_unit_of_blocks_is_refused(
    blocks: int, order: BlockOrder
) -> None:
    """A partial unit leaves one side sitting later in the run than the other."""
    with pytest.raises(InterleaveError, match='whole number'):
        InterleaveSpec(blocks=blocks, block_size=4, warmup_blocks=_unit(order), order=order)


def test_a_spec_with_no_warmup_is_refused() -> None:
    """Ordering and warm-up discard answer different failure shapes.

    A zero mean-position gap bounds only a drift that grows with position. A
    warm-up transient is neither fixed-length nor linear, so the ordering says
    nothing about it and the discard is not optional.
    """
    with pytest.raises(InterleaveError, match='at least one unit'):
        InterleaveSpec(blocks=8, block_size=5, warmup_blocks=0)


def test_a_partial_unit_of_warmup_is_refused() -> None:
    """Discarding half a unit puts the gap back into the measured half."""
    with pytest.raises(InterleaveError, match='whole number'):
        InterleaveSpec(blocks=8, block_size=4, warmup_blocks=2)


def test_discarding_everything_is_refused() -> None:
    with pytest.raises(InterleaveError, match='no measured blocks'):
        InterleaveSpec(blocks=4, block_size=4, warmup_blocks=4)


def test_an_empty_block_is_refused() -> None:
    with pytest.raises(InterleaveError, match='block_size'):
        InterleaveSpec(blocks=4, block_size=0, warmup_blocks=4)


def test_samples_that_do_not_line_up_with_the_schedule_are_refused() -> None:
    """A short sequence would pair each side with the other's numbers."""
    spec = InterleaveSpec(blocks=8, block_size=3, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    with pytest.raises(InterleaveError, match='cannot be placed'):
        partition_samples(schedule, (1.0, 2.0))


def test_a_schedule_with_one_side_missing_is_refused() -> None:
    """There is nothing to compare a lone side against."""
    lonely = tuple(
        ScheduledObservation(
            global_index=index,
            block=0,
            side=PairedSide.BASELINE,
            warmup=False,
        )
        for index in range(4)
    )
    with pytest.raises(InterleaveError, match='no candidate observations'):
        mean_position_gap(lonely)


def test_starting_with_the_candidate_mirrors_the_gap() -> None:
    """The residual has a direction, and the order decides which."""
    spec = InterleaveSpec(
        blocks=4,
        block_size=4,
        warmup_blocks=2,
        order=BlockOrder.ALTERNATING,
        first=PairedSide.CANDIDATE,
    )
    assert expected_position_gap(spec) == -4.0
    assert mean_position_gap(interleave_schedule(spec)) == -4.0


def test_the_run_reports_its_own_drift_rate() -> None:
    """The bound is derived from the window it is quoted with."""
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    rate = observed_drift_per_observation(schedule, _drifting_samples(schedule))
    assert rate == pytest.approx(DRIFT_PER_OBSERVATION)


def test_a_flat_run_reports_no_drift() -> None:
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    flat = tuple(10.0 for _ in schedule)
    assert observed_drift_per_observation(schedule, flat) == pytest.approx(0.0)


def test_the_drift_rate_ignores_the_warm_up_transient() -> None:
    """A transient in the discarded blocks must not enter the fit.

    The transient is the failure shape the ordering cannot bound. If it also
    tilted the fitted rate, the bound quoted for the run would be a bound
    computed from data the run threw away.
    """
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    samples = tuple(
        (500.0 if entry.warmup else 0.0)
        + 10.0
        + DRIFT_PER_OBSERVATION * entry.global_index
        for entry in schedule
    )
    assert observed_drift_per_observation(schedule, samples) == pytest.approx(
        DRIFT_PER_OBSERVATION
    )


def test_a_drift_rate_needs_samples_that_line_up() -> None:
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    with pytest.raises(InterleaveError, match='cannot be fitted'):
        observed_drift_per_observation(interleave_schedule(spec), (1.0,))


def test_conditions_carry_the_bound_with_the_model_it_holds_under() -> None:
    """A bare zero would read as a bound on every way a window can move."""
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    schedule = interleave_schedule(spec)
    conditions = interleave_conditions(
        spec,
        schedule,
        drift_per_observation=observed_drift_per_observation(
            schedule, _drifting_samples(schedule)
        ),
    )
    attribution = conditions['drift_attribution']
    assert attribution['model'] == DRIFT_MODEL
    assert attribution['bound'] == 0.0
    assert attribution['drift_per_observation'] == pytest.approx(
        DRIFT_PER_OBSERVATION
    )
    assert 'warm-up transient' in attribution['excludes']


def test_an_alternating_run_carries_a_non_zero_bound() -> None:
    """The residual reaches the artifact instead of being left implicit."""
    spec = InterleaveSpec(
        blocks=8, block_size=5, warmup_blocks=2, order=BlockOrder.ALTERNATING
    )
    schedule = interleave_schedule(spec)
    conditions = interleave_conditions(
        spec, schedule, drift_per_observation=DRIFT_PER_OBSERVATION
    )
    assert conditions['drift_attribution']['bound'] == pytest.approx(0.25)


def test_conditions_record_the_order_the_numbers_were_taken_in() -> None:
    spec = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
    conditions = interleave_conditions(
        spec, interleave_schedule(spec), drift_per_observation=0.0
    )
    assert conditions['total_observations'] == 40
    assert conditions['discarded_warmup_observations'] == 20
    assert conditions['measured_observations_per_side'] == 10
    assert conditions['mean_position_gap'] == 0.0
    assert conditions['block_sides'][:4] == [
        'baseline', 'candidate', 'candidate', 'baseline',
    ]
