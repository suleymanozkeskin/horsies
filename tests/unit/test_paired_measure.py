"""Blocks must run where the schedule put them, and say what they ran."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.task_history_prototypes.paired_cell import SampleUnit
from tests.task_history_prototypes.paired_interleave import (
    InterleaveError,
    InterleaveSpec,
)
from tests.task_history_prototypes.paired_measure import (
    BLOCK_MEDIAN_SPREAD_BOUND,
    SIDE_SAMPLES_MARKER,
    STEADY_STATE_STATEMENTS,
    assert_block_medians_are_flat,
    block_median_spread,
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
                arm=actual,
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


def test_the_keyed_row_and_its_control_differ_only_by_the_key() -> None:
    """The control is the same options call with the key set to None.

    Comparing keyed against a plain send would fold the options call into the
    delta. This is the row whose control is a sibling operation rather than the
    baseline build, which is forced: the released build takes no key at all.
    """
    ordinary = measure_source(
        MeasurementPlan(
            task_name=PLAN.task_name,
            operation=Operation.OPTIONS_ORDINARY_ENQUEUE,
            payload_bytes=PLAN.payload_bytes,
            payload_seed=PLAN.payload_seed,
        ),
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
    behavioural = [pair for pair in differences if '_sent = ' in pair[0]]
    assert len(behavioural) == 1
    control_call, keyed_call = behavioural[0]
    assert 'idempotency_key=None' in control_call
    assert 'idempotency_key=_keys[_index]' in keyed_call
    assert control_call.replace(
        'idempotency_key=None', 'idempotency_key=_keys[_index]'
    ) == keyed_call

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


def test_a_block_that_ran_on_the_wrong_arm_is_refused() -> None:
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
        arm=blocks[4].arm,
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
            arm=result.side,
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


def test_a_run_with_no_blocks_at_all_is_refused() -> None:
    with pytest.raises(MeasurementError, match='no blocks ran at all'):
        assert_configurations_held([])


def test_all_blocks_on_one_build_is_allowed_when_the_arms_alternate() -> None:
    """An operation-paired row runs both arms on the candidate by design.

    The safety that used to come from requiring both builds now comes from the
    arm check: a cross-build row whose blocks all landed on one build fails
    because its arms no longer match the schedule.
    """
    blocks = [
        BlockResult(
            block=result.block,
            side=PairedSide.CANDIDATE,
            arm=result.arm,
            identity=_identity(PairedSide.CANDIDATE),
            config=_config(PairedSide.CANDIDATE),
            samples=result.samples,
            wall_clock_seconds=result.wall_clock_seconds,
        )
        for result in _blocks()
    ]
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
            arm=result.side,
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
            arm=result.side,
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


def test_the_baseline_cannot_be_asked_for_an_operation_it_lacks() -> None:
    """The released build takes no idempotency key, so the row is not a delta.

    Refused before the block runs. Left to fail inside the block it would
    surface as a send error and read as a product fault.
    """
    from tests.task_history_prototypes.paired_measure import (
        CANDIDATE_ONLY_OPERATIONS,
        SideRuntime,
        run_block,
    )

    assert Operation.KEYED_ENQUEUE in CANDIDATE_ONLY_OPERATIONS
    assert Operation.ORDINARY_ENQUEUE not in CANDIDATE_ONLY_OPERATIONS
    runtime = SideRuntime(
        side=PairedSide.BASELINE,
        interpreter=Path('/baseline/bin/python'),
        expected_root=Path('/baseline'),
        expected_schema_version=BASELINE_SCHEMA_VERSION,
        database_url='postgresql://h/base',
    )
    with pytest.raises(MeasurementError, match='does not have it'):
        run_block(
            runtime,
            block=0,
            observations=5,
            plan=MeasurementPlan(
                task_name='p',
                operation=Operation.KEYED_ENQUEUE,
                payload_bytes=200,
                payload_seed=1,
            ),
            config_spec=SeedConfigSpec(),
            cwd=Path('/'),
            clock=lambda: 0.0,
        )


def test_keys_are_unique_per_observation() -> None:
    """A repeated idempotency key is a deduplicated send, not this operation."""
    body = measure_source(
        MeasurementPlan(
            task_name='p',
            operation=Operation.KEYED_ENQUEUE,
            payload_bytes=200,
            payload_seed=1,
        ),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=3,
    )
    assert "str(_block) + '-' + str(_index)" in body


def test_an_operation_paired_row_interleaves_on_one_build() -> None:
    """Both arms on the candidate, still interleaved by the schedule.

    The keyed row's control is a sibling operation because the released build
    has no keyed enqueue at all. Without an arm distinct from the build, the
    two operations would be measured in separate windows and the drift between
    those windows would be attributed to the key.
    """
    from tests.task_history_prototypes.paired_interleave import block_sides

    blocks = [
        BlockResult(
            block=index,
            side=PairedSide.CANDIDATE,
            arm=arm,
            identity=_identity(PairedSide.CANDIDATE),
            config=_config(PairedSide.CANDIDATE),
            samples=tuple(
                1.0 + position * 0.001 for position in range(SPEC.block_size)
            ),
            wall_clock_seconds=0.5,
        )
        for index, arm in enumerate(block_sides(SPEC))
    ]
    schedule, samples = assert_run_is_measurable(SPEC, blocks)
    assert len(schedule) == len(samples)
    assert {entry.side for entry in schedule} == {
        PairedSide.BASELINE, PairedSide.CANDIDATE
    }
    assert {result.side for result in blocks} == {PairedSide.CANDIDATE}


def _blocks_with_medians(medians: list[float]) -> list[BlockResult]:
    """One block per median, in schedule order, each block flat internally."""
    from tests.task_history_prototypes.paired_interleave import block_sides

    return [
        BlockResult(
            block=index,
            side=arm,
            arm=arm,
            identity=_identity(arm),
            config=_config(arm),
            samples=(medians[index],) * SPEC.block_size,
            wall_clock_seconds=0.5,
        )
        for index, arm in enumerate(block_sides(SPEC))
    ]


def test_a_steady_run_passes_the_flatness_gate() -> None:
    """The control run's own shape: per-block medians 0.617-0.709 ms."""
    blocks = _blocks_with_medians(
        [0.6, 0.6, 0.6, 0.6, 0.617, 0.647, 0.709, 0.639]
    )
    assert block_median_spread(SPEC, blocks) == pytest.approx(0.149, abs=0.001)
    assert_block_medians_are_flat(SPEC, blocks)


def test_a_run_whose_measurand_moved_is_refused() -> None:
    """The defect's own shape: medians climbing 0.598 -> 2.144 ms.

    Counterbalancing holds the mean-position gap at zero, which cancels this in
    a location statistic and leaves it in the tail — so such a run looks stable
    at p50 and disagrees with itself at p99.
    """
    blocks = _blocks_with_medians(
        [0.5, 0.5, 0.5, 0.5, 0.598, 1.250, 1.958, 2.144]
    )
    assert block_median_spread(SPEC, blocks) > BLOCK_MEDIAN_SPREAD_BOUND
    with pytest.raises(MeasurementError, match='changed while it was being measured'):
        assert_block_medians_are_flat(SPEC, blocks)


def test_the_gate_reads_only_the_measured_half() -> None:
    """Warm-up blocks are discarded, so their shape cannot fail a good run."""
    blocks = _blocks_with_medians(
        [0.5, 5.0, 0.5, 9.0, 0.617, 0.647, 0.709, 0.639]
    )
    assert_block_medians_are_flat(SPEC, blocks)


def test_the_bound_separates_the_measured_noise_from_the_defect() -> None:
    """Derived, not chosen: above the control run, far below the defect."""
    steady = _blocks_with_medians(
        [0.6, 0.6, 0.6, 0.6, 0.617, 0.647, 0.709, 0.639]
    )
    moving = _blocks_with_medians(
        [0.5, 0.5, 0.5, 0.5, 0.598, 1.250, 1.958, 2.144]
    )
    assert block_median_spread(SPEC, steady) < BLOCK_MEDIAN_SPREAD_BOUND
    assert block_median_spread(SPEC, moving) > BLOCK_MEDIAN_SPREAD_BOUND
    assert BLOCK_MEDIAN_SPREAD_BOUND / block_median_spread(SPEC, steady) > 2.5
    assert block_median_spread(SPEC, moving) / BLOCK_MEDIAN_SPREAD_BOUND > 6.0


def test_a_run_with_no_measured_blocks_has_no_steady_state_to_check() -> None:
    with pytest.raises(MeasurementError, match='no measured blocks'):
        block_median_spread(SPEC, [])


def test_the_steady_state_step_returns_rows_before_it_vacuums() -> None:
    """Order matters: vacuuming first would leave the rows still claimed."""
    assert len(STEADY_STATE_STATEMENTS) == 2
    assert "status = 'PENDING'" in STEADY_STATE_STATEMENTS[0]
    assert STEADY_STATE_STATEMENTS[1] == 'VACUUM ANALYZE horsies_tasks'
    assert 'VACUUM' not in STEADY_STATE_STATEMENTS[0]


def test_the_steady_state_step_returns_a_row_whole() -> None:
    """A partial reset leaves an incoherent row, and the product refuses it.

    ck_horsies_tasks_terminal_at_terminal_only rejects a PENDING row whose
    terminal_at still carries a terminal timestamp, so every column the claim,
    the RUNNING setup and the terminalization write must be cleared.
    """
    from tests.task_history_prototypes.paired_measure import (
        RUNNING_SETUP_COLUMNS,
        STEADY_STATE_COLUMNS,
    )

    assert RUNNING_SETUP_COLUMNS <= STEADY_STATE_COLUMNS
    for column in ('terminal_at', 'terminalization_kind', 'result', 'completed_at'):
        assert column in STEADY_STATE_COLUMNS
    for column in STEADY_STATE_COLUMNS:
        assert f'{column} =' in STEADY_STATE_STATEMENTS[0]


def test_a_complete_run_is_gated_on_flatness_too() -> None:
    """The gate is part of what a run must discharge, not an optional check."""
    blocks = _blocks_with_medians(
        [0.5, 0.5, 0.5, 0.5, 0.598, 1.250, 1.958, 2.144]
    )
    with pytest.raises(MeasurementError, match='changed while it was being measured'):
        assert_run_is_measurable(SPEC, blocks)


def test_the_claim_body_refuses_an_empty_claim() -> None:
    """An empty claim does the lock and the scan but claims nothing.

    It is a different operation from the one this row judges, and it is what a
    drained queue produces — so a row that timed it would report a cheaper
    number the longer it ran.
    """
    from tests.task_history_prototypes.paired_measure import (
        ClaimPlan,
        claim_source,
    )

    body = claim_source(
        ClaimPlan(task_name='p'),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    assert 'if not _rows:' in body
    assert 'claim returned no rows' in body
    refusal = body.index('if not _rows:')
    recording = body.index('_samples.append')
    assert refusal < recording


def test_the_claim_body_uses_a_fresh_worker_per_observation() -> None:
    """One worker id would let the per-worker cap empty every later claim."""
    from tests.task_history_prototypes.paired_measure import (
        ClaimPlan,
        claim_source,
    )

    body = claim_source(
        ClaimPlan(task_name='p'),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=2,
    )
    assert "'qual-' + str(_block) + '-' + str(_index)" in body


def test_the_claim_body_takes_the_statement_from_the_build_under_test() -> None:
    """Copying its text here would run a statement neither build ships."""
    from tests.task_history_prototypes.paired_measure import (
        ClaimPlan,
        claim_source,
    )

    body = claim_source(
        ClaimPlan(task_name='p'),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    assert 'from horsies.core.worker.sql import HORSIES_CLAIM_SQL' in body
    assert 'horsies_claim(' not in body


BASELINE_CHILD_RUNNER = Path(
    'ignored-content/.throughput-venvs/horsies-0.4.7/lib/python3.13/'
    'site-packages/horsies/core/worker/child_runner.py'
)
CANDIDATE_CHILD_RUNNER = Path('horsies/core/worker/child_runner.py')


def test_the_setup_is_pinned_to_what_the_product_writes() -> None:
    """The harness authors this statement, so it is checked against the product.

    Read out of each build's own source rather than from what this harness
    remembers: a product change to the transition breaks the pin instead of
    silently leaving the harness establishing a different precondition.
    """
    from tests.task_history_prototypes.paired_measure import (
        RUNNING_SETUP_COLUMNS,
        running_transition_columns,
    )

    if not BASELINE_CHILD_RUNNER.exists():
        pytest.skip('the released build is not installed in this environment')
    baseline = running_transition_columns(BASELINE_CHILD_RUNNER.read_text())
    candidate = running_transition_columns(CANDIDATE_CHILD_RUNNER.read_text())
    assert RUNNING_SETUP_COLUMNS == baseline & candidate


def test_the_setup_writes_every_column_the_candidate_writes() -> None:
    """A RUNNING row missing a column would exercise paths production never runs."""
    from tests.task_history_prototypes.paired_measure import (
        RUNNING_SETUP_COLUMNS,
        RUNNING_SETUP_STATEMENT,
        running_transition_columns,
    )

    product = running_transition_columns(CANDIDATE_CHILD_RUNNER.read_text())
    assert RUNNING_SETUP_COLUMNS == product
    for column in product:
        assert f'{column} =' in RUNNING_SETUP_STATEMENT


def test_a_moved_transition_breaks_the_pin_rather_than_passing() -> None:
    from tests.task_history_prototypes.paired_measure import (
        running_transition_columns,
    )

    with pytest.raises(MeasurementError, match='no CLAIMED -> RUNNING'):
        running_transition_columns('def something_else(): pass\n')


def test_the_terminalization_body_checks_the_outcome() -> None:
    """A refused command still runs, and costs a fraction of applying."""
    from tests.task_history_prototypes.paired_measure import (
        TerminalizePlan,
        terminalize_source,
    )

    body = terminalize_source(
        TerminalizePlan(task_name='p', result_bytes=200, payload_seed=1),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    assert 'decode_outcome_row' in body
    assert "!= 'APPLIED'" in body
    assert 'terminalization did not apply' in body


def test_the_terminalization_body_establishes_running_before_timing() -> None:
    from tests.task_history_prototypes.paired_measure import (
        TerminalizePlan,
        terminalize_source,
    )

    body = terminalize_source(
        TerminalizePlan(task_name='p', result_bytes=200, payload_seed=1),
        config_spec=SeedConfigSpec(),
        database_url='postgresql://x/y',
        observations=5,
        block=0,
    )
    setup = body.index('_to_running(_conn, _row, _row_worker_id)')
    timing = body.index('_started = _time.perf_counter_ns()')
    assert setup < timing
    # The predicate, not only its message: a message left in place while the
    # check is gone would satisfy a test that only looked for the words.
    assert 'if len(_updated) != 1:' in body
    assert 'the CLAIMED to RUNNING setup updated' in body


def test_the_setup_declares_itself_as_harness_authored() -> None:
    """Conditions name it for what it is, and name the source it replicates."""
    from tests.task_history_prototypes.paired_measure import (
        RUNNING_SETUP_PROVENANCE,
        TerminalizePlan,
    )

    payload = TerminalizePlan(
        task_name='p', result_bytes=200, payload_seed=1
    ).as_payload()
    assert payload['running_setup_provenance'] == RUNNING_SETUP_PROVENANCE
    assert 'harness-authored' in RUNNING_SETUP_PROVENANCE
    assert 'child_runner.py' in RUNNING_SETUP_PROVENANCE
