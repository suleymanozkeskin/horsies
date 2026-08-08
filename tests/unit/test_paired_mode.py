"""A row's limits belong to a declared instrument, checked before the row runs."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_mode import (
    MIN_OBSERVATIONS_PER_SIDE,
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


def _server(**overrides: object) -> ServerMode:
    settings = dict(REQUIRED_SETTINGS[MODE])
    settings.update(overrides.pop('settings', {}))  # type: ignore[arg-type]
    return ServerMode(
        mode=MODE,
        settings=settings,
        active_client_backends=int(overrides.pop('active', 0)),  # type: ignore[call-overload]
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


def test_the_mandated_block_size_is_enforced() -> None:
    """Block size is what the ordering alternates over."""
    with pytest.raises(ModeConformanceError, match='alternates over'):
        assert_structure_conformance(
            MODE,
            observations_per_side=MIN_OBSERVATIONS_PER_SIDE[MODE],
            block_size=1_000,
        )


def test_a_conforming_structure_is_accepted() -> None:
    assert_structure_conformance(
        MODE,
        observations_per_side=MIN_OBSERVATIONS_PER_SIDE[MODE],
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
