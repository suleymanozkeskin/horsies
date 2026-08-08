"""A row's limits belong to a declared instrument, checked before the row runs."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_mode import (
    MIN_OBSERVATIONS_PER_SIDE,
    REQUIRED_RECORDED_PARAMETERS,
    REQUIRED_SETTINGS,
    RESTORE_COMMAND_BLOCK,
    BenchMode,
    ModeConformanceError,
    ServerMode,
    assert_mode_conformance,
    assert_structure_conformance,
    settings_from_rows,
)

MODE = BenchMode.PAIRED_MICRO


def _server(
    *, settings: dict[str, str] | None = None, active: int = 0
) -> ServerMode:
    effective = dict(REQUIRED_SETTINGS[MODE])
    if settings is not None:
        effective.update(settings)
    return ServerMode(
        mode=MODE, settings=effective, active_client_backends=active
    )


def test_a_conforming_instrument_is_accepted() -> None:
    assert_mode_conformance(_server())


@pytest.mark.parametrize(
    'name', sorted(REQUIRED_SETTINGS[BenchMode.PAIRED_MICRO])
)
def test_each_required_setting_is_checked(name: str) -> None:
    """Every one of them, not a representative sample.

    All four are at the opposite state by PostgreSQL default, so an
    unconfigured instance fails rather than passing by accident.
    """
    with pytest.raises(ModeConformanceError, match='not in paired-micro mode'):
        assert_mode_conformance(_server(settings={name: 'on'}))


def test_a_setting_the_server_never_reported_is_refused() -> None:
    """A setting nobody read cannot be shown to hold."""
    partial = dict(REQUIRED_SETTINGS[MODE])
    del partial['fsync']
    with pytest.raises(ModeConformanceError, match="did not report"):
        assert_mode_conformance(
            ServerMode(mode=MODE, settings=partial, active_client_backends=0)
        )


def test_a_busy_instance_is_refused() -> None:
    """The mode requires the surrounding units quiesced."""
    with pytest.raises(ModeConformanceError, match='quiesced'):
        assert_mode_conformance(_server(active=2))


def test_the_observation_minimum_is_enforced() -> None:
    with pytest.raises(ModeConformanceError, match='requires at least'):
        assert_structure_conformance(
            MODE, observations_per_side=4_000, block_size=100
        )


def _minimum(mode: BenchMode) -> int:
    value = MIN_OBSERVATIONS_PER_SIDE[mode]
    assert value is not None
    return value


def test_the_mandated_block_size_is_enforced() -> None:
    """Block size is what the ordering alternates over."""
    with pytest.raises(ModeConformanceError, match='alternates over'):
        assert_structure_conformance(
            MODE,
            observations_per_side=_minimum(MODE),
            block_size=1_000,
        )


def test_a_conforming_structure_is_accepted() -> None:
    assert_structure_conformance(
        MODE,
        observations_per_side=_minimum(MODE),
        block_size=100,
    )


def test_settings_are_read_from_the_rows_the_query_returned() -> None:
    assert settings_from_rows([['fsync', 'off'], ['autovacuum', 'off']]) == {
        'fsync': 'off',
        'autovacuum': 'off',
    }


def test_a_settings_row_of_the_wrong_shape_is_refused() -> None:
    with pytest.raises(ModeConformanceError, match='expected name and setting'):
        settings_from_rows([['fsync']])


def test_the_window_carries_its_own_restore() -> None:
    """Durability is relaxed instance-wide, so the way back is committed."""
    for name in REQUIRED_SETTINGS[MODE]:
        assert f'ALTER SYSTEM RESET {name}' in RESTORE_COMMAND_BLOCK
    assert 'pg_reload_conf()' in RESTORE_COMMAND_BLOCK


def test_conditions_carry_the_instrument() -> None:
    conditions = _server().as_conditions()
    assert conditions['mode'] == 'paired-micro'
    assert conditions['server_settings'] == dict(REQUIRED_SETTINGS[MODE])
    assert conditions['active_client_backends'] == 0


OPERATIONAL = BenchMode.OPERATIONAL


def _operational(
    *,
    settings: dict[str, str] | None = None,
    recorded: dict[str, str] | None = None,
    drop_recorded: tuple[str, ...] = (),
    active: int = 0,
    declared: tuple[str, ...] = (),
) -> ServerMode:
    effective = dict(REQUIRED_SETTINGS[OPERATIONAL])
    if settings is not None:
        effective.update(settings)
    parameters = {
        name: 'recorded'
        for name in REQUIRED_RECORDED_PARAMETERS[OPERATIONAL]
    }
    if recorded is not None:
        parameters.update(recorded)
    for name in drop_recorded:
        parameters.pop(name, None)
    return ServerMode(
        mode=OPERATIONAL,
        settings=effective,
        active_client_backends=active,
        recorded_parameters=parameters,
        declared_competing_processes=declared,
    )


def test_the_two_modes_require_opposite_durability() -> None:
    """They are not variations on one instrument."""
    micro = REQUIRED_SETTINGS[BenchMode.PAIRED_MICRO]
    operational = REQUIRED_SETTINGS[OPERATIONAL]
    assert set(micro) == set(operational)
    for name in micro:
        assert micro[name] != operational[name]


def test_a_conforming_operational_instrument_is_accepted() -> None:
    assert_mode_conformance(_operational())


def test_operational_refuses_the_paired_micro_instrument() -> None:
    """The window that is right for one row is wrong for the other."""
    with pytest.raises(ModeConformanceError, match='not in operational mode'):
        assert_mode_conformance(
            _operational(settings=dict(REQUIRED_SETTINGS[BenchMode.PAIRED_MICRO]))
        )


def test_operational_requires_its_parameters_reported() -> None:
    """The mode does not constrain their values; the report must state them."""
    with pytest.raises(ModeConformanceError, match='requires these reported'):
        assert_mode_conformance(
            _operational(drop_recorded=('autovacuum_vacuum_scale_factor',))
        )


def test_a_recorded_parameter_may_hold_any_value() -> None:
    """Any autovacuum tuning is permitted as long as it is declared."""
    assert_mode_conformance(
        _operational(recorded={'autovacuum_vacuum_scale_factor': '0.02'})
    )


def test_a_declared_competing_process_is_permitted() -> None:
    """Section 2.2's exception, as an instrument setting rather than a licence."""
    assert_mode_conformance(
        _operational(active=1, declared=('qual-long-reader',))
    )


def test_undeclared_activity_is_still_refused() -> None:
    """Naming one reader does not excuse a second nobody mentioned."""
    with pytest.raises(ModeConformanceError, match='undeclared activity'):
        assert_mode_conformance(
            _operational(active=2, declared=('qual-long-reader',))
        )


def test_operational_imposes_no_paired_structure() -> None:
    """An operational row reports intervals, so there is no per-side count."""
    assert MIN_OBSERVATIONS_PER_SIDE[OPERATIONAL] is None
    assert_structure_conformance(
        OPERATIONAL, observations_per_side=7, block_size=3
    )


def test_paired_micro_still_imposes_its_structure() -> None:
    with pytest.raises(ModeConformanceError, match='requires at least'):
        assert_structure_conformance(
            BenchMode.PAIRED_MICRO, observations_per_side=7, block_size=100
        )


def test_operational_conditions_carry_the_parameters_and_the_readers() -> None:
    conditions = _operational(
        active=1, declared=('qual-long-reader',)
    ).as_conditions()
    assert conditions['mode'] == 'operational'
    assert conditions['declared_competing_processes'] == ['qual-long-reader']
    for name in REQUIRED_RECORDED_PARAMETERS[OPERATIONAL]:
        assert name in conditions['recorded_parameters']
