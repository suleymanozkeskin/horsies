"""The column-rule registry matches the history table's single-column CHECKs."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.column_rules import HISTORY_COLUMN_RULES

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

INSTALLED_SQL = text("""
    SELECT conname::text, pg_get_constraintdef(oid)
    FROM pg_constraint
    WHERE conrelid = 'horsies_task_history'::regclass
      AND contype = 'c'
      AND array_length(conkey, 1) = 1
""")


async def test_each_rule_renders_as_its_installed_constraint(
    broker: PostgresBroker,
    engine: AsyncEngine,
) -> None:
    _ = broker
    async with engine.connect() as connection:
        transaction = await connection.begin()
        try:
            installed = dict((await connection.execute(INSTALLED_SQL)).tuples().all())
            await connection.execute(text(
                'CREATE TEMP TABLE history_rule_probe '
                '(LIKE horsies_task_history)'
            ))
            rendered: dict[str, str] = {}
            for index, rule in enumerate(HISTORY_COLUMN_RULES):
                probe_name = f'history_rule_probe_{index}'
                await connection.execute(text(
                    f'ALTER TABLE history_rule_probe ADD CONSTRAINT {probe_name} '
                    f'CHECK ({rule.predicate}) NOT VALID'
                ))
                rendered[rule.constraint_name] = str((await connection.execute(
                    text(
                        'SELECT pg_get_constraintdef(oid) FROM pg_constraint '
                        'WHERE conname = :name'
                    ),
                    {'name': probe_name},
                )).scalar_one()).removesuffix(' NOT VALID')
        finally:
            await transaction.rollback()

    assert set(rendered) == set(installed)
    assert rendered == installed
