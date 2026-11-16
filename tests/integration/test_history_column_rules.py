"""The history parent carries no single-column CHECK.

The move family copies values that the live table and the enqueue path have
already checked, so the history parent keeps only its two cross-column
CHECKs: the result domain and the rerun input shape.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CROSS_COLUMN_CHECKS = frozenset({
    'horsies_task_history_result_domain',
    'horsies_task_history_rerun_input_shape',
})

CHECKS_SQL = text("""
    SELECT conname::text, pg_get_constraintdef(oid),
           array_length(conkey, 1) = 1 AS single_column
    FROM pg_constraint
    WHERE conrelid = to_regclass('horsies_task_history')
      AND contype = 'c'
""")


async def _checks(connection: AsyncConnection) -> tuple[dict[str, str], set[str]]:
    """The single-column CHECKs with their definitions, and the
    cross-column CHECK names, of the visible history parent."""
    single: dict[str, str] = {}
    cross: set[str] = set()
    for name, definition, single_column in (
        await connection.execute(CHECKS_SQL)
    ).tuples():
        if single_column:
            single[name] = definition
        else:
            cross.add(name)
    return single, cross


async def test_an_initialized_database_has_no_single_column_history_check(
    broker: PostgresBroker,
    engine: AsyncEngine,
) -> None:
    _ = broker
    async with engine.connect() as connection:
        single, cross = await _checks(connection)
    assert single == {}
    assert cross == CROSS_COLUMN_CHECKS
