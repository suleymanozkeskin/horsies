"""One unusable class does not cost the other classes their coverage.

The pass reads registered classes from the database in key order. A class
whose name arithmetic fails raised out of the loop, so every class sorting
after it silently stopped getting leaves — and because the trigger is a
registered row rather than a declaration, removing the declaration could
not clear it.

The trigger is planted the only way it can now occur: written straight to
`horsies_retention_classes`, since configuration refuses the key. That is
also the shape a deployment would be left in by a version that accepted it.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.maintenance.coverage import (
    CoverageEnsureFailed,
    CoverageEnsured,
    ensure_partition_coverage,
)
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.names import (
    LEAF_CATALOG,
    MAX_RETENTION_CLASS_KEY_LENGTH,
    RETENTION_CLASSES,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# 35 characters: past the length at which `daily_leaf_name` raises, and
# below the length at which the parent name would raise first. Leading
# 'a' so it sorts BEFORE the default class, which is the whole point —
# the class denied coverage must be one the loop reaches afterwards.
_UNUSABLE_KEY = 'a' + 'k' * 34


async def _leaf_count(broker: PostgresBroker, class_key: str) -> int:
    async with broker.async_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    f'SELECT count(*) FROM {LEAF_CATALOG} '
                    'WHERE class_key = :key'
                ),
                {'key': class_key},
            )
        ).scalar_one()


async def _plant_unusable_class(broker: PostgresBroker) -> None:
    async with broker.async_engine.begin() as connection:
        await connection.execute(
            text(
                f"""
                INSERT INTO {RETENTION_CLASSES} (
                    class_key, duration, partition_interval,
                    finite_parent_name, created_at
                ) VALUES (
                    :key, :duration, :interval, :parent,
                    statement_timestamp()
                )
                ON CONFLICT (class_key) DO NOTHING
                """
            ),
            {
                'key': _UNUSABLE_KEY,
                'duration': timedelta(days=5),
                'interval': timedelta(days=1),
                'parent': f'horsies_task_history_{_UNUSABLE_KEY}',
            },
        )


async def _forget_unusable_class(broker: PostgresBroker) -> None:
    async with broker.async_engine.begin() as connection:
        await connection.execute(
            text(f'DELETE FROM {LEAF_CATALOG} WHERE class_key = :key'),
            {'key': _UNUSABLE_KEY},
        )
        await connection.execute(
            text(f'DELETE FROM {RETENTION_CLASSES} WHERE class_key = :key'),
            {'key': _UNUSABLE_KEY},
        )


async def test_a_class_that_cannot_be_named_does_not_deny_the_others() -> None:
    """Guard the premise the test rests on."""
    assert len(_UNUSABLE_KEY) > MAX_RETENTION_CLASS_KEY_LENGTH
    assert _UNUSABLE_KEY < DEFAULT_RETENTION_CLASS_KEY, (
        'the unusable key must sort first or the test proves nothing'
    )


async def test_coverage_contains_the_failure_and_keeps_going(
    broker: PostgresBroker,
) -> None:
    async with broker.async_engine.begin() as connection:
        baseline = await ensure_partition_coverage(
            connection, history_horizon_days=2, heartbeat_horizon_hours=3
        )
    assert isinstance(baseline, CoverageEnsured), baseline
    before = await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY)

    await _plant_unusable_class(broker)
    try:
        # Raising the horizon means the default class is OWED a new leaf.
        # It can only receive one if the loop survived the class sorting
        # ahead of it, so the count is the containment evidence.
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection, history_horizon_days=3, heartbeat_horizon_hours=3
            )

        assert isinstance(outcome, CoverageEnsureFailed), (
            'the pass must report the failure rather than raise through '
            f'its caller: {outcome!r}'
        )
        assert _UNUSABLE_KEY in outcome.refusal, (
            f'the refusal must name the class that failed: {outcome!r}'
        )
        assert await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY) > before, (
            'a class sorting after the unusable one was denied its leaf; '
            'the failure is still taking the rest of the pass with it'
        )
    finally:
        await _forget_unusable_class(broker)
