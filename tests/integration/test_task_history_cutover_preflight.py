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
    seconds_per_million_rows=120.0,
    fixed_seconds=30.0,
    preparation_seconds_per_million_rows=0.0,
)


def test_estimate_presents_the_total_window() -> None:
    itemized = RelocationCoefficients(
        seconds_per_million_rows=120.0,
        fixed_seconds=30.0,
        preparation_seconds_per_million_rows=600.0,
    )
    estimate = estimate_relocation(
        itemized,
        rows=2_000_000,
        stage_seconds=(('drain-verify', 5.0), ('tighten', 20.0)),
    )
    assert estimate.coefficients == itemized
    assert estimate.preparation_seconds == pytest.approx(1200.0)
    assert estimate.relocation_seconds == pytest.approx(240.0)
    # Total = fixed + prep + relocation + itemized stages; the
    # planning ceiling applies to the TOTAL, never the model term.
    assert estimate.total_seconds == pytest.approx(1495.0)
    assert estimate.ceiling_seconds == pytest.approx(1495.0 * 1.25)


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
    assert plan.estimate.ceiling_seconds > plan.estimate.total_seconds


@pytest.mark.asyncio
async def test_preflight_reports_the_consequence_of_an_absent_class(
    make_database: MakeDatabase,
) -> None:
    """Class-less terminal rows are inventoried WITH what will happen.

    Counting them alone would leave the operator to work out that
    relocation routes them to forever and that nothing will ever age
    them; the advisory states it, which is what makes the number a
    decision surface. It is an advisory and not a refusal: the cutover
    completes either way.
    """
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
                connection,
                status='COMPLETED',
                kind='COMPLETE_LOCKED',
                class_key=None,
            )
            await insert_legacy_task(
                connection, status='COMPLETED', kind='COMPLETE_LOCKED'
            )
            plan = await run_preflight(connection, coefficients=COEFFICIENTS)
    finally:
        await engine.dispose()

    assert plan.unclassified_rows == 1
    # Measured, not assumed: the row's live size varies with its
    # payload, so the pin holds the wording and the format while the
    # figure comes from the plan — and a separate assertion holds that
    # the figure is a real measurement rather than a zero.
    assert plan.unclassified_live_bytes > 0
    megabytes = plan.unclassified_live_bytes / (1024 * 1024)
    assert plan.advisories == (
        f'1 terminal rows ({megabytes:.1f} MB live) carry no retention '
        "class; relocation will place them in the 'forever' class (no "
        'automatic aging); backfill a class before cutover to age them',
    )


@pytest.mark.asyncio
async def test_preflight_is_silent_when_every_row_is_classified(
    make_database: MakeDatabase,
) -> None:
    """No advisory where there is nothing for the operator to decide."""
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
            plan = await run_preflight(connection, coefficients=COEFFICIENTS)
    finally:
        await engine.dispose()

    assert plan.unclassified_rows == 0
    assert plan.advisories == ()


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
