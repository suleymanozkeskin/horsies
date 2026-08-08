"""Blocks must run where the schedule put them, and say what they ran."""

from __future__ import annotations

import json

import pytest

from tests.task_history_prototypes.paired_cell import SampleUnit
from tests.task_history_prototypes.paired_interleave import (
    BlockOrder,
    InterleaveError,
    InterleaveSpec,
)
from tests.task_history_prototypes.paired_measure import (
    SIDE_SAMPLES_MARKER,
    BlockResult,
    MeasurementError,
    MeasurementPlan,
    Operation,
    assert_blocks_ran_in_schedule_order,
    assert_configurations_held,
    assert_run_is_measurable,
    measure_source,
    observations_in_schedule_order,
    run_conditions,
    samples_from_output,
    samples_in_schedule_order,
)
from tests.task_history_prototypes.paired_seed import (
    ConfigEquivalenceError,
    SeedConfigSpec,
    SideConfig,
)
from tests.task_history_prototypes.paired_sides import (
    BASELINE_SCHEMA_VERSION,
    CANDIDATE_SCHEMA_VERSION,
    PairedSide,
    SideIdentity,
)

SPEC = InterleaveSpec(blocks=8, block_size=5, warmup_blocks=4)
PLAN = MeasurementPlan(
    task_name='probe',
    operation=Operation.ORDINARY_ENQUEUE,
    payload_bytes=200,
    payload_seed=1,
)
SHARED_EFFECTIVE: dict[str, object] = {
    'broker.pool_size': 10,
    'prefetch_buffer': 0,
}
CANDIDATE_ONLY_KEY = 'broker.retain_rerun_input_default'


def _config(
    side: PairedSide, *, effective: dict[str, object] | None = None
) -> SideConfig:
    base = dict(SHARED_EFFECTIVE)
    base['broker.database_url'] = f'**{side.value}**'
    if side is PairedSide.CANDIDATE:
        base[CANDIDATE_ONLY_KEY] = False
    if effective is not None:
        base.update(effective)
    return SideConfig(
        side=side, effective=base, defaults={CANDIDATE_ONLY_KEY: False}
    )


def _identity(side: PairedSide) -> SideIdentity:
    version = (
        BASELINE_SCHEMA_VERSION
        if side is PairedSide.BASELINE
        else CANDIDATE_SCHEMA_VERSION
    )
    return SideIdentity(
        side=side,
        interpreter=f'/{side.value}/bin/python',
        module_path=f'/{side.value}/horsies/__init__.py',
        schema_version=version,
        expected_root=f'/{side.value}',
        expected_schema_version=version,
    )


def _blocks(
    spec: InterleaveSpec = SPEC, *, swap_sides: tuple[int, ...] = ()
) -> list[BlockResult]:
    from tests.task_history_prototypes.paired_interleave import block_sides

    swap = swap_sides
    results: list[BlockResult] = []
    for index, side in enumerate(block_sides(spec)):
        actual = (
            side
            if index not in swap
            else (
                PairedSide.CANDIDATE
                if side is PairedSide.BASELINE
                else PairedSide.BASELINE
            )
        )
        results.append(
            BlockResult(
                block=index,
                side=actual,
                identity=_identity(actual),
                config=_config(actual),
                samples=tuple(
                    1.0 + index * 0.01 + position * 0.001
                    for position in range(spec.block_size)
                ),
                wall_clock_seconds=0.5,
            )
        )
    return results


def test_the_body_is_text_and_imports_nothing_from_this_checkout() -> None:
    body = measure_source(
        PLAN,
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    assert 'tests.task_history_prototypes' not in body
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith(('import ', 'from ')):
            assert stripped.split()[1].split('.')[0] in {
                'base64', 'json', 'random', 'time', 'pydantic', 'horsies',
            }


def test_the_body_does_not_migrate() -> None:
    """Seeding migrated each side; a block that migrates times a schema check."""
    body = measure_source(
        PLAN,
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    assert 'run_schema_migrations=False' in body
    assert 'run_schema_migrations=True' not in body


def test_the_body_primes_outside_the_timed_region() -> None:
    """Every block is a fresh process, so every block has a first send."""
    body = measure_source(
        PLAN,
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    priming = body.index('_primed = _measured.send(')
    timing = body.index('_time.perf_counter_ns')
    assert priming < timing
    # And the priming send is not itself timed.
    assert '_primed' not in body[timing:]


def test_the_two_enqueue_operations_differ_only_by_the_key() -> None:
    """The one group A item whose control is its neighbour, not the baseline."""
    ordinary = measure_source(
        PLAN,
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    keyed = measure_source(
        MeasurementPlan(
            task_name=PLAN.task_name,
            operation=Operation.KEYED_ENQUEUE,
            payload_bytes=PLAN.payload_bytes,
            payload_seed=PLAN.payload_seed,
        ),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    differences = [
        (left, right)
        for left, right in zip(
            ordinary.splitlines(), keyed.splitlines(), strict=True
        )
        if left != right
    ]
    # Exactly two lines differ, and only one of them is behaviour: the key
    # expression. The other is the plan literal, which records which operation
    # was asked for and must therefore differ.
    assert len(differences) == 2
    behavioural = [pair for pair in differences if '_key = ' in pair[0]]
    assert len(behavioural) == 1
    assert behavioural[0][0].strip() == '_key = None'
    assert behavioural[0][1].strip() == (
        "_key = _plan['key_prefix'] + str(_index)"
    )

    def _plan_literal(line: str) -> dict[str, object]:
        inner = line[line.index('(') + 1 : line.rindex(')')]
        return json.loads(json.loads(inner))

    recorded = [pair for pair in differences if pair not in behavioural]
    ordinary_plan = _plan_literal(recorded[0][0])
    keyed_plan = _plan_literal(recorded[0][1])
    differing_fields = {
        field
        for field in ordinary_plan.keys() | keyed_plan.keys()
        if ordinary_plan.get(field) != keyed_plan.get(field)
    }
    assert differing_fields == {'operation'}


def test_a_block_without_priming_is_refused() -> None:
    with pytest.raises(MeasurementError, match='priming sends'):
        MeasurementPlan(
            task_name='p',
            operation=Operation.ORDINARY_ENQUEUE,
            payload_bytes=200,
            payload_seed=1,
            priming_sends=0,
        )


def test_an_empty_block_is_refused() -> None:
    with pytest.raises(MeasurementError, match='at least one observation'):
        measure_source(
            PLAN,
            config_spec=SeedConfigSpec(),
            database_url='postgresql://x/y',
            observations=0,
            block=0,
        )


def test_samples_are_read_from_what_the_block_reported() -> None:
    output = (
        'noise\n'
        + SIDE_SAMPLES_MARKER
        + ' '
        + json.dumps({'samples': [1.0, 2.0, 3.0]})
        + '\n'
    )
    assert samples_from_output(
        output, side=PairedSide.BASELINE, expected=3
    ) == (1.0, 2.0, 3.0)


def test_a_short_block_is_refused() -> None:
    """It moved the position of every later observation with it."""
    output = (
        SIDE_SAMPLES_MARKER + ' ' + json.dumps({'samples': [1.0, 2.0]}) + '\n'
    )
    with pytest.raises(MeasurementError, match='not the schedule whose'):
        samples_from_output(output, side=PairedSide.BASELINE, expected=5)


def test_a_block_with_no_samples_line_is_refused() -> None:
    with pytest.raises(MeasurementError, match='no samples line'):
        samples_from_output('nothing\n', side=PairedSide.CANDIDATE, expected=1)


def test_an_unparseable_samples_line_is_refused() -> None:
    with pytest.raises(MeasurementError, match='unparseable'):
        samples_from_output(
            f'{SIDE_SAMPLES_MARKER} not-json\n',
            side=PairedSide.BASELINE,
            expected=1,
        )


def test_blocks_in_schedule_order_are_accepted() -> None:
    assert_blocks_ran_in_schedule_order(SPEC, _blocks())


def test_a_block_that_ran_on_the_wrong_side_is_refused() -> None:
    """The ordering's whole guarantee is positional."""
    with pytest.raises(MeasurementError, match='the schedule assigns it'):
        assert_blocks_ran_in_schedule_order(SPEC, _blocks(swap_sides=(2,)))


def test_a_missing_block_is_refused() -> None:
    with pytest.raises(MeasurementError, match='the schedule describes'):
        assert_blocks_ran_in_schedule_order(SPEC, _blocks()[:-1])


def test_blocks_out_of_order_are_refused() -> None:
    blocks = _blocks()
    blocks[3], blocks[4] = blocks[4], blocks[3]
    with pytest.raises(MeasurementError, match='did not run in schedule order'):
        assert_blocks_ran_in_schedule_order(SPEC, blocks)


def test_configurations_holding_across_the_run_are_accepted() -> None:
    assert_configurations_held(_blocks())


def test_a_configuration_that_changed_midway_is_refused() -> None:
    """Blocks are separate processes; a change would show only as a delta."""
    blocks = _blocks()
    blocks[4] = BlockResult(
        block=4,
        side=blocks[4].side,
        identity=blocks[4].identity,
        config=_config(blocks[4].side, effective={'broker.pool_size': 25}),
        samples=blocks[4].samples,
        wall_clock_seconds=blocks[4].wall_clock_seconds,
    )
    with pytest.raises(MeasurementError, match='ran a different configuration'):
        assert_configurations_held(blocks)


def test_the_two_sides_configurations_are_compared_across_the_run() -> None:
    blocks = _blocks()
    replaced = [
        BlockResult(
            block=result.block,
            side=result.side,
            identity=result.identity,
            config=(
                _config(result.side, effective={'broker.pool_size': 25})
                if result.side is PairedSide.CANDIDATE
                else result.config
            ),
            samples=result.samples,
            wall_clock_seconds=result.wall_clock_seconds,
        )
        for result in blocks
    ]
    with pytest.raises(ConfigEquivalenceError, match='broker.pool_size'):
        assert_configurations_held(replaced)


def test_a_run_with_one_side_missing_is_refused() -> None:
    spec = InterleaveSpec(
        blocks=8, block_size=5, warmup_blocks=4, order=BlockOrder.ALTERNATING
    )
    blocks = [
        BlockResult(
            block=result.block,
            side=PairedSide.BASELINE,
            identity=_identity(PairedSide.BASELINE),
            config=_config(PairedSide.BASELINE),
            samples=result.samples,
            wall_clock_seconds=result.wall_clock_seconds,
        )
        for result in _blocks(spec)
    ]
    with pytest.raises(MeasurementError, match='no blocks ran on'):
        assert_configurations_held(blocks)


def test_the_realised_schedule_marks_warmup_by_block() -> None:
    schedule = observations_in_schedule_order(SPEC, _blocks())
    assert len(schedule) == SPEC.blocks * SPEC.block_size
    assert sum(1 for entry in schedule if entry.warmup) == (
        SPEC.warmup_blocks * SPEC.block_size
    )
    assert schedule[0].global_index == 0
    assert schedule[-1].global_index == len(schedule) - 1


def test_a_complete_run_passes_every_check() -> None:
    schedule, samples = assert_run_is_measurable(SPEC, _blocks())
    assert len(schedule) == len(samples)
    assert samples == samples_in_schedule_order(_blocks())


def _resized(
    blocks: list[BlockResult], sizes: dict[PairedSide, int]
) -> list[BlockResult]:
    """Right blocks, right sides, right order — wrong number of observations."""
    return [
        BlockResult(
            block=result.block,
            side=result.side,
            identity=result.identity,
            config=result.config,
            samples=tuple(
                1.0 + position * 0.001 for position in range(sizes[result.side])
            ),
            wall_clock_seconds=result.wall_clock_seconds,
        )
        for result in blocks
    ]


def test_a_run_giving_one_side_more_observations_is_refused() -> None:
    """It passes the block checks and still samples one build more thoroughly.

    The block-level checks cannot see this: every block is where the schedule
    put it. Only the realised schedule shows it.
    """
    blocks = _resized(
        _blocks(), {PairedSide.BASELINE: 5, PairedSide.CANDIDATE: 3}
    )
    with pytest.raises(InterleaveError, match='observations'):
        assert_run_is_measurable(SPEC, blocks)


def test_a_run_with_an_overlong_warmup_stretch_is_refused() -> None:
    """Correct order, equal measured sides, zero gap — and still not interleaved.

    The counts and the gap are computed over the measured half, so a warm-up
    stretch that grew is invisible to both. The two adjacent candidate warm-up
    blocks here hold sixteen consecutive observations against the ten this
    ordering permits, and only the run-length check sees it.
    """
    sizes = [5, 8, 8, 5, 5, 5, 5, 5]
    blocks = [
        BlockResult(
            block=result.block,
            side=result.side,
            identity=result.identity,
            config=result.config,
            samples=tuple(
                1.0 + position * 0.001
                for position in range(sizes[result.block])
            ),
            wall_clock_seconds=result.wall_clock_seconds,
        )
        for result in _blocks()
    ]
    schedule = observations_in_schedule_order(SPEC, blocks)
    measured_per_side = {
        side: sum(
            1 for entry in schedule if not entry.warmup and entry.side is side
        )
        for side in (PairedSide.BASELINE, PairedSide.CANDIDATE)
    }
    assert set(measured_per_side.values()) == {10}
    with pytest.raises(InterleaveError, match='consecutive observations'):
        assert_run_is_measurable(SPEC, blocks)


def test_conditions_record_the_cost_of_every_block() -> None:
    conditions = run_conditions(PLAN, _blocks(), unit=SampleUnit.MILLISECONDS)
    assert conditions['unit'] == 'ms'
    assert conditions['plan']['priming_sends'] == 8
    assert len(conditions['blocks']) == SPEC.blocks
    assert conditions['wall_clock_seconds_total'] == pytest.approx(
        0.5 * SPEC.blocks
    )
