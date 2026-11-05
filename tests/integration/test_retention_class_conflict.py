"""A declared class whose duration differs from its registry row.

A registered class never changes its duration. Each test registers a
class for 7 days, then declares the same key for 14 days, and checks one
consequence:

- the coverage pass still serves every other class and reports the
  conflict, through both entry points;
- a fleet broker refuses to initialize, and a matching or unregistered
  declaration does not;
- ``check(live=True)`` reports HRS-216.

Coverage is idempotent, so a pass proves it served a class only when the
pass owes the class leaves. Each test registers its own classes with no
leaves, and removes them at the end.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Literal

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.errors import ErrorCode
from horsies.core.history.ddl.classes import (
    ClassRegistered,
    register_finite_retention_class,
)
from horsies.core.history.maintenance.coverage import (
    CoverageEnsureFailed,
    CoverageOutcome,
    ensure_partition_coverage,
    maintain_partition_coverage,
)
from horsies.core.history.names import LEAF_CATALOG, RETENTION_CLASSES
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.retention import RetentionClassConfig, RetentionConfig
from horsies.core.types.result import Err, Ok, is_ok
from tests.broker_resources_support import current_resources
from tests.integration.conftest import DB_URL

# Sorts before `standard_30d` and before the survivor, so a pass that
# stops at the conflict denies leaves to both classes after it.
_CONFLICT_KEY = 'a_conflict_class'
_SURVIVOR_KEY = 'z_conflict_survivor'
_REGISTERED = timedelta(days=7)
_DECLARED = timedelta(days=14)
_HORIZON_DAYS = 3

type EntryPoint = Literal['one_transaction', 'maintenance_database']


async def _register(engine: AsyncEngine, key: str, duration: timedelta) -> None:
    async with engine.begin() as connection:
        registration = await register_finite_retention_class(
            connection, class_key=key, duration=duration,
        )
    assert isinstance(registration, ClassRegistered), registration


async def _forget(engine: AsyncEngine, key: str) -> None:
    """Remove the class and every leaf a pass built for it.

    The leaf catalog has a foreign key to the class, so the class row
    goes last.
    """
    async with engine.begin() as connection:
        leaves = (
            await connection.execute(
                text(f'SELECT leaf_name FROM {LEAF_CATALOG} WHERE class_key = :key'),
                {'key': key},
            )
        ).scalars().all()
        for leaf_name in leaves:
            await connection.execute(text(f'DROP TABLE IF EXISTS {leaf_name} CASCADE'))
        await connection.execute(
            text(f'DELETE FROM {LEAF_CATALOG} WHERE class_key = :key'),
            {'key': key},
        )
        await connection.execute(
            text(f'DROP TABLE IF EXISTS horsies_task_history_{key} CASCADE')
        )
        await connection.execute(
            text(f'DELETE FROM {RETENTION_CLASSES} WHERE class_key = :key'),
            {'key': key},
        )


async def _leaf_count(engine: AsyncEngine, key: str) -> int:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text(f'SELECT count(*) FROM {LEAF_CATALOG} WHERE class_key = :key'),
                {'key': key},
            )
        ).scalar_one()


async def _registered_duration(engine: AsyncEngine, key: str) -> timedelta | None:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    f'SELECT duration FROM {RETENTION_CLASSES} WHERE class_key = :key'
                ),
                {'key': key},
            )
        ).scalar_one_or_none()


async def _run_pass(
    broker: PostgresBroker,
    entry_point: EntryPoint,
    declared: tuple[tuple[str, timedelta], ...],
) -> CoverageOutcome:
    resources = current_resources(broker)
    match entry_point:
        case 'one_transaction':
            async with resources.engine.begin() as connection:
                return await ensure_partition_coverage(
                    connection,
                    history_horizon_days=_HORIZON_DAYS,
                    heartbeat_horizon_hours=6,
                    declared_classes=declared,
                )
        case 'maintenance_database':
            return await maintain_partition_coverage(
                resources.partition_maintenance,
                history_horizon_days=_HORIZON_DAYS,
                heartbeat_horizon_hours=6,
                declared_classes=declared,
            )


def _app_declaring(key: str, duration: timedelta) -> Horsies:
    return Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(DB_URL)),
            retention=RetentionConfig(
                retention_classes=(
                    RetentionClassConfig(key=key, duration=duration),
                ),
            ),
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize('entry_point', ['one_transaction', 'maintenance_database'])
async def test_a_declared_conflict_does_not_stop_the_other_classes(
    broker: PostgresBroker,
    entry_point: EntryPoint,
) -> None:
    engine = current_resources(broker).engine
    await _forget(engine, _CONFLICT_KEY)
    await _forget(engine, _SURVIVOR_KEY)
    await _register(engine, _CONFLICT_KEY, _REGISTERED)
    await _register(engine, _SURVIVOR_KEY, _REGISTERED)
    owed = _HORIZON_DAYS + 1
    assert await _leaf_count(engine, _SURVIVOR_KEY) == 0
    try:
        outcome = await _run_pass(
            broker,
            entry_point,
            ((_CONFLICT_KEY, _DECLARED), (_SURVIVOR_KEY, _REGISTERED)),
        )

        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert outcome.stage == 'register_declared_class', outcome
        assert outcome.class_key == _CONFLICT_KEY, outcome
        assert '7 days' in outcome.refusal and '14 days' in outcome.refusal, (
            f'the report must name both durations: {outcome.refusal}'
        )
        assert outcome.heartbeat_covered_now, outcome
        assert await _leaf_count(engine, _SURVIVOR_KEY) == owed, (
            'the conflict still stops the pass before the classes after it'
        )
        assert await _leaf_count(engine, _CONFLICT_KEY) == owed, (
            'rows already sent into the conflicting class need its leaves'
        )
        assert await _registered_duration(engine, _CONFLICT_KEY) == _REGISTERED, (
            'a conflict must not change the registered duration'
        )
    finally:
        await _forget(engine, _CONFLICT_KEY)
        await _forget(engine, _SURVIVOR_KEY)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('declared_key', 'declared_duration', 'refused'),
    [
        pytest.param(_CONFLICT_KEY, _DECLARED, True, id='different-duration'),
        pytest.param(_CONFLICT_KEY, _REGISTERED, False, id='same-duration'),
        pytest.param('a_never_registered', _DECLARED, False, id='not-registered'),
    ],
)
async def test_a_fleet_broker_refuses_a_conflicting_declaration(
    broker: PostgresBroker,
    declared_key: str,
    declared_duration: timedelta,
    refused: bool,
) -> None:
    engine = current_resources(broker).engine
    await _forget(engine, _CONFLICT_KEY)
    await _register(engine, _CONFLICT_KEY, _REGISTERED)
    fleet = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    fleet.app = _app_declaring(declared_key, declared_duration)
    try:
        initialized = await fleet.ensure_schema_initialized()
    finally:
        await fleet.close_async()
        await _forget(engine, _CONFLICT_KEY)

    match refused, initialized:
        case False, Ok():
            pass
        case True, Err(err_value=failure):
            assert failure.code is BrokerErrorCode.SCHEMA_INIT_FAILED, failure
            assert not failure.retryable, failure
            assert _CONFLICT_KEY in failure.message, failure.message
            assert '7 days' in failure.message, failure.message
            assert '14 days' in failure.message, failure.message
        case _:
            pytest.fail(f'refused={refused}, but initialization gave {initialized!r}')


async def _prepare_registered_class() -> None:
    schema_broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
    try:
        initialized = await schema_broker.ensure_schema_initialized()
        assert is_ok(initialized), initialized
    finally:
        await schema_broker.close_async()
    engine = create_async_engine(DB_URL)
    try:
        await _forget(engine, _CONFLICT_KEY)
        await _register(engine, _CONFLICT_KEY, _REGISTERED)
    finally:
        await engine.dispose()


async def _remove_registered_class() -> None:
    engine = create_async_engine(DB_URL)
    try:
        await _forget(engine, _CONFLICT_KEY)
    finally:
        await engine.dispose()


@pytest.mark.integration
def test_check_live_reports_the_conflict() -> None:
    """Sync: ``check(live=True)`` refuses to run inside an event loop."""
    asyncio.run(_prepare_registered_class())
    try:
        conflicting = _app_declaring(_CONFLICT_KEY, _DECLARED).check(live=True)
        matching = _app_declaring(_CONFLICT_KEY, _REGISTERED).check(live=True)
    finally:
        asyncio.run(_remove_registered_class())

    assert [error.code for error in conflicting] == [
        ErrorCode.CONFIG_INVALID_RETENTION
    ], conflicting
    assert _CONFLICT_KEY in conflicting[0].message, conflicting[0].message
    assert '14 days' in conflicting[0].message, conflicting[0].message
    assert matching == [], matching
