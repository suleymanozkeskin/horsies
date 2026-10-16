"""horsies_phase2_consume plans its history lookup once per connection.

The function carries plan_cache_mode = force_generic_plan, so the lookup
through the partitioned history parent prunes leaves at run time instead
of planning over every leaf on each call. The setting is function-local:
the caller's own setting is back in force after the call.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio(loop_scope='function')
async def test_function_forces_a_generic_plan(broker: PostgresBroker) -> None:
    async with broker.async_engine.connect() as connection:
        setting = (
            await connection.execute(
                text("""
                SELECT array_to_string(proconfig, ',')
                FROM pg_proc
                WHERE oid = to_regprocedure('horsies_phase2_consume(uuid,text)')
            """)
            )
        ).scalar_one()

    assert setting == 'plan_cache_mode=force_generic_plan'


@pytest.mark.asyncio(loop_scope='function')
async def test_caller_plan_setting_survives_the_call(broker: PostgresBroker) -> None:
    async with broker.async_engine.connect() as connection:
        async with connection.begin():
            await connection.execute(
                text('SET LOCAL plan_cache_mode = force_custom_plan')
            )
            disposition = (
                await connection.execute(
                    text(
                        'SELECT (horsies_phase2_consume(CAST(:id AS uuid), :status))'
                        '.disposition'
                    ),
                    {'id': str(uuid.uuid4()), 'status': 'FAILED'},
                )
            ).scalar_one()
            caller_setting = (
                await connection.execute(text('SHOW plan_cache_mode'))
            ).scalar_one()

    assert disposition == 'PENDING_ABSENT'
    assert caller_setting == 'force_custom_plan'
