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
from typing import Any

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.maintenance.coverage import (
    CoverageEnsureFailed,
    CoverageEnsured,
    ensure_partition_coverage,
)
from horsies.core.history.ddl.classes import (
    ClassAlreadyRegistered,
    ClassRegistered,
    DEFAULT_RETENTION_CLASS_KEY,
    finite_class_parent_name,
    register_finite_retention_class,
)
from horsies.core.history.names import (
    LEAF_CATALOG,
    MAX_RETENTION_CLASS_KEY_LENGTH,
    RETENTION_CLASSES,
)
from horsies.core.history.heartbeats.partitioning import HEARTBEAT_CLASS_KEY
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    database_now,
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

    # Its own horizon, lower than the returned-refusal test's, because
    # coverage is idempotent: two tests sharing a horizon means whichever
    # runs second is owed nothing and its count proves nothing. The
    # premise is asserted rather than assumed, so a database left at a
    # higher horizon by an earlier run fails here saying so instead of
    # looking like a containment regression.
    horizon = 3
    owed = horizon + 1
    before = await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY)
    assert before < owed, (
        f'premise broken: the default class already holds {before} leaves '
        f'against a horizon of {horizon}, so this pass owes it nothing'
    )

    await _plant_unusable_class(broker)
    try:
        # The default class is OWED a leaf and can only receive it if the
        # loop survived the class sorting ahead of it, so the count is
        # the containment evidence.
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection,
                history_horizon_days=horizon,
                heartbeat_horizon_hours=3,
            )

        assert isinstance(outcome, CoverageEnsureFailed), (
            'the pass must report the failure rather than raise through '
            f'its caller: {outcome!r}'
        )
        assert _UNUSABLE_KEY in outcome.refusal, (
            f'the refusal must name the class that failed: {outcome!r}'
        )
        assert await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY) == owed, (
            'a class sorting after the unusable one was denied its leaf; '
            'the failure is still taking the rest of the pass with it'
        )
    finally:
        await _forget_unusable_class(broker)


# A key that sorts BEFORE the default class, as every queue-derived key
# does: `q_` < `standard_30d` in byte order. The ordering is no longer
# incidental — the feature guarantees the adverse case.
_REFUSING_KEY = 'q_broken_7d'


async def _plant_class_row(connection: Any) -> None:
    """Register `_REFUSING_KEY` without creating anything for it."""
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
            'key': _REFUSING_KEY,
            'duration': timedelta(days=7),
            'interval': timedelta(days=1),
            'parent': f'horsies_task_history_{_REFUSING_KEY}',
        },
    )


async def _plant_class_without_relation(broker: PostgresBroker) -> None:
    """A registered class whose parent relation does not exist.

    This is the divergence the teardown-ordering fix exists to prevent,
    and it is reachable on a real deployment by an operator DROP. Leaf
    coverage RETURNS a refusal for it rather than raising, which is the
    escape a try/except cannot see.
    """
    async with broker.async_engine.begin() as connection:
        await _plant_class_row(connection)


async def _forget_refusing_class(broker: PostgresBroker) -> None:
    async with broker.async_engine.begin() as connection:
        await connection.execute(
            text(f'DELETE FROM {LEAF_CATALOG} WHERE class_key = :key'),
            {'key': _REFUSING_KEY},
        )
        await connection.execute(
            text(f'DELETE FROM {RETENTION_CLASSES} WHERE class_key = :key'),
            {'key': _REFUSING_KEY},
        )


async def _heartbeat_leaf_count(broker: PostgresBroker) -> int:
    async with broker.async_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    f'SELECT count(*) FROM {LEAF_CATALOG} '
                    'WHERE class_key = :key'
                ),
                {'key': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()


async def test_a_returned_refusal_is_contained_like_a_raise(
    broker: PostgresBroker,
) -> None:
    """The escape a try/except cannot catch.

    `create_daily_leaf` RETURNS its refusals. They never reach an
    exception handler, so the pass used to return on the first one and
    deny coverage to every class sorting after it.
    """
    # A horizon no other test in this file reaches. Coverage is
    # idempotent, so "the count went up" is only evidence when this pass
    # is the one that owes a leaf; sharing a horizon with the test above
    # meant it had already been created and this one proved nothing.
    horizon = 5
    owed = horizon + 1

    before = await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY)
    assert before < owed, (
        f'premise broken: the default class already holds {before} leaves '
        f'against a horizon of {horizon}, so this pass owes it nothing and '
        'the count cannot show whether it was served'
    )

    await _plant_class_without_relation(broker)
    try:
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection,
                history_horizon_days=horizon,
                heartbeat_horizon_hours=3,
            )

        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert _REFUSING_KEY in outcome.refusal, outcome
        assert await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY) == owed, (
            'a returned refusal still denied coverage to the class after '
            f'it: the default class holds '
            f'{await _leaf_count(broker, DEFAULT_RETENTION_CLASS_KEY)} '
            f'leaves, not the {owed} a horizon of {horizon} owes it'
        )
    finally:
        await _forget_refusing_class(broker)


async def test_heartbeat_coverage_survives_a_failing_history_class(
    broker: PostgresBroker,
) -> None:
    """Heartbeat leaves gate worker startup.

    Stopping the pass before heartbeat coverage turned one poisoned
    class row into a fleet that cannot restart: within the heartbeat
    horizon no leaf covers the present instant, heartbeat writes fail,
    and startup refuses outright.
    """
    await _plant_class_without_relation(broker)
    try:
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection, history_horizon_days=3, heartbeat_horizon_hours=6
            )

        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert outcome.heartbeat_covered_now, (
            'the present instant lost heartbeat coverage while a history '
            'class was failing'
        )
        assert await _heartbeat_leaf_count(broker) > 0
    finally:
        await _forget_refusing_class(broker)


async def test_a_database_error_does_not_poison_the_pass(
    broker: PostgresBroker,
) -> None:
    """The variant a Python-level raise cannot reproduce.

    A DATABASE error aborts the caller's transaction. Without a
    savepoint, every later statement — including the health probe this
    function ends on — fails with InFailedSqlTransaction and the pass
    raises instead of reporting, which is the stale-health defect
    containment was introduced to fix.

    The trigger is a real database error: the class's parent exists and
    is partitioned, but the leaf name it will try to create is already
    taken by an ordinary table, so CREATE fails on a duplicate relation
    rather than returning a refusal.
    """
    async with broker.async_engine.begin() as connection:
        # Registered through the shipped function, not hand-written DDL:
        # the parent's partition key is the library's to choose, and a
        # guess at it would test the collision against a relation the
        # real registrar would never have built.
        registration = await register_finite_retention_class(
            connection, class_key=_REFUSING_KEY, duration=timedelta(days=7)
        )
        assert isinstance(
            registration, (ClassRegistered, ClassAlreadyRegistered)
        ), registration
        parent = finite_class_parent_name(_REFUSING_KEY)
        # The name the next pass will try to create, occupied by a table
        # that is not a partition of it. Derived exactly as the pass
        # derives it -- `current_date` renders in the SESSION timezone
        # while the pass normalizes `database_now()`, so on a non-UTC
        # session the two disagree near midnight and the squatter would
        # sit on a name nothing tries to create.
        now = await database_now(connection)
        day_lower = now.replace(hour=0, minute=0, second=0, microsecond=0)
        squatter = daily_leaf_name(parent, day_lower)
        await connection.execute(
            text(f'CREATE TABLE IF NOT EXISTS {squatter} (x int)')
        )

    try:
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection, history_horizon_days=3, heartbeat_horizon_hours=3
            )

        # Reaching this line at all is most of the assertion: without a
        # savepoint the aborted transaction makes the pass raise.
        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert _REFUSING_KEY in outcome.refusal, outcome
        assert isinstance(outcome.heartbeat_covered_now, bool), (
            'the health probe ran, so the transaction was still usable'
        )
    finally:
        async with broker.async_engine.begin() as connection:
            await connection.execute(
                text(f'DROP TABLE IF EXISTS {squatter} CASCADE')
            )
            await connection.execute(
                text(f'DROP TABLE IF EXISTS {parent} CASCADE')
            )
        await _forget_refusing_class(broker)
