"""The task ORM model has the columns of ``horsies_tasks`` on the baseline schema.

The baseline SQL creates the table; the model is a query helper. A column
that the model names and the table lacks fails every full-entity read at
run time, so the two sets and their nullability must be equal.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.task_pg import TaskModel
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_task_model_columns_and_nullability_equal_the_table(
    broker: PostgresBroker,
) -> None:
    async with current_resources(broker).session_factory() as session:
        rows = (
            await session.execute(
                text(
                    'SELECT column_name, is_nullable = \'YES\' AS nullable '
                    'FROM information_schema.columns '
                    "WHERE table_schema = current_schema() "
                    "AND table_name = 'horsies_tasks'"
                ),
            )
        ).all()
    table = {str(row.column_name): bool(row.nullable) for row in rows}
    model = {
        column.name: bool(column.nullable) for column in TaskModel.__table__.columns
    }

    assert model == table
