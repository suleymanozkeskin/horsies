"""The CHECKs of the history parent.

The move family copies values that the live table and the enqueue path have
already checked. The history parent keeps one single-column CHECK, the
terminalization kind, which every move writes as a literal, and two
cross-column CHECKs: the result domain and the rerun input shape.
"""

from __future__ import annotations

import re

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.operations import TerminalizationKind

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

KIND_CHECK = 'horsies_task_history_kind_known'

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


async def test_the_kind_is_the_one_single_column_history_check(
    broker: PostgresBroker,
    engine: AsyncEngine,
) -> None:
    """The one single-column CHECK holds the kinds of TerminalizationKind."""
    _ = broker
    async with engine.connect() as connection:
        single, cross = await _checks(connection)
    assert set(single) == {KIND_CHECK}
    assert set(re.findall(r"'(\w+)'::character varying", single[KIND_CHECK])) == {
        kind.value for kind in TerminalizationKind
    }
    assert cross == CROSS_COLUMN_CHECKS
