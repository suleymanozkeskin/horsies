"""Stage-0 preflight on the real migrated schema.

The typed plan inventories the work and carries the estimate WITH its
fitted coefficients and the ×1.25 planning ceiling as first-class
fields — never a bare duration. Readiness failures are typed refusals,
not partial reports.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.cutover.preflight import (
    CutoverPreflight,
    PreflightError,
    RelocationCoefficients,
    estimate_relocation,
    run_preflight,
)
from horsies.core.models.broker import PostgresConfig
from tests.integration.test_task_history_relocation import (
    install_program_state,
    insert_legacy_task,
)
from tests.integration.test_task_history_schema_emission import (
    MakeDatabase,
    make_database,
)

__all__ = ['make_database']

pytestmark = [pytest.mark.integration]

COEFFICIENTS = RelocationCoefficients(
    seconds_per_million_rows=120.0, fixed_seconds=30.0
)


def test_estimate_carries_coefficients_and_ceiling() -> None:
    estimate = estimate_relocation(COEFFICIENTS, rows=2_000_000)
    assert estimate.coefficients == COEFFICIENTS
    assert estimate.estimated_seconds == pytest.approx(270.0)
    assert estimate.ceiling_seconds == pytest.approx(270.0 * 1.25)


@pytest.mark.asyncio
async def test_preflight_inventories_the_work(
    make_database: MakeDatabase,
) -> None:
    url = await make_database()
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await install_program_state(connection)
            await insert_legacy_task(
                connection, status='COMPLETED', kind='COMPLETE_LOCKED'
            )
            await insert_legacy_task(
                connection, status='FAILED', kind=None, disposition=None
            )
            await insert_legacy_task(connection, status='PENDING', kind=None)
            plan = await run_preflight(
                connection, coefficients=COEFFICIENTS
            )
    finally:
        await engine.dispose()

    assert isinstance(plan, CutoverPreflight)
    assert plan.history_parent_present
    assert plan.terminal_live_rows == 2
    assert plan.unrecorded_kind_rows == 1
    assert plan.unprepared_envelope_rows == 1
    assert plan.class_day_pairs == 1
    assert plan.estimate.rows == 2
    assert plan.estimate.ceiling_seconds > plan.estimate.estimated_seconds


@pytest.mark.asyncio
async def test_preflight_refuses_a_stale_schema(
    make_database: MakeDatabase,
) -> None:
    url = await make_database()
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    'DELETE FROM horsies_schema_version WHERE version > 28'
                )
            )
            await connection.execute(
                text(
                    'INSERT INTO horsies_schema_version (version) '
                    'VALUES (28) ON CONFLICT DO NOTHING'
                )
            )
            with pytest.raises(PreflightError, match='predates'):
                await run_preflight(connection, coefficients=COEFFICIENTS)
    finally:
        await engine.dispose()
