"""One unusable class does not cost the other classes their coverage.

The pass reads registered classes from the database in key order. A class
whose name arithmetic fails raised out of the loop, so every class sorting
after it silently stopped getting leaves — and because the trigger is a
registered row rather than a declaration, removing the declaration could
not clear it.

The trigger is planted the only way it can now occur: written straight to
`horsies_retention_classes`, since configuration refuses the key. That is
also the shape a deployment would be left in by a version that accepted it.

Coverage is idempotent, so a pass proves it served a class only when that
pass owes the class a leaf. Each count-based test registers its own passing
class. The premise is therefore independent of definition order, earlier
suite runs against the same database, and parallel collection.
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
_RAISE_SURVIVOR_KEY = 'z_raise_survivor_7d'
_RETURN_SURVIVOR_KEY = 'z_return_survivor_7d'


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


async def _register_owned_class(broker: PostgresBroker, class_key: str) -> None:
    await _forget_owned_class(broker, class_key)
    async with broker.async_engine.begin() as connection:
        registration = await register_finite_retention_class(
            connection,
            class_key=class_key,
            duration=timedelta(days=7),
        )
    assert isinstance(registration, ClassRegistered), registration


async def _forget_owned_class(broker: PostgresBroker, class_key: str) -> None:
    parent = finite_class_parent_name(class_key)
    async with broker.async_engine.begin() as connection:
        await connection.execute(text(f'DROP TABLE IF EXISTS {parent} CASCADE'))
        await connection.execute(
            text(f'DELETE FROM {LEAF_CATALOG} WHERE class_key = :key'),
            {'key': class_key},
        )
        await connection.execute(
            text(f'DELETE FROM {RETENTION_CLASSES} WHERE class_key = :key'),
            {'key': class_key},
        )


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

    await _register_owned_class(broker, _RAISE_SURVIVOR_KEY)
    horizon = 3
    owed = horizon + 1
    assert await _leaf_count(broker, _RAISE_SURVIVOR_KEY) == 0

    await _plant_unusable_class(broker)
    try:
        # The owned class is OWED leaves and can only receive them if the
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
        assert await _leaf_count(broker, _RAISE_SURVIVOR_KEY) == owed, (
            'a class sorting after the unusable one was denied its leaf; '
            'the failure is still taking the rest of the pass with it'
        )
    finally:
        await _forget_unusable_class(broker)
        await _forget_owned_class(broker, _RAISE_SURVIVOR_KEY)


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
    and it is reachable on a real deployment by an operator DROP.

    Creating a leaf under a missing parent is a DATABASE error --
    `UndefinedTable` from the CREATE -- which is why this planter belongs
    to the transaction-poisoning test. It is the failure kind that aborts
    the caller's transaction.
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


async def _plant_class_with_squatted_leaf(broker: PostgresBroker) -> str:
    """A registered class whose next leaf name is already taken.

    Leaf creation probes for the relation and RETURNS
    `LeafCatalogConflict` rather than raising, so this is the escape a
    `try/except` cannot see -- which is why this planter belongs to the
    returned-refusal test.

    Registered through the shipped function, not hand-written DDL: the
    parent's partition key is the library's to choose, and a guess at it
    would test against a relation the real registrar would never build.
    The squatter's name is derived the way the pass derives it --
    `current_date` renders in the SESSION timezone while the pass
    normalizes `database_now()`, so on a non-UTC session near midnight
    the two disagree and the squatter would sit on a name nothing tries
    to create.

    Returns the squatter's relation name so the caller can drop it.
    """
    async with broker.async_engine.begin() as connection:
        registration = await register_finite_retention_class(
            connection, class_key=_REFUSING_KEY, duration=timedelta(days=7)
        )
        assert isinstance(
            registration, (ClassRegistered, ClassAlreadyRegistered)
        ), registration
        parent = finite_class_parent_name(_REFUSING_KEY)
        now = await database_now(connection)
        day_lower = now.replace(hour=0, minute=0, second=0, microsecond=0)
        squatter = daily_leaf_name(parent, day_lower)
        await connection.execute(
            text(f'CREATE TABLE IF NOT EXISTS {squatter} (x int)')
        )
    return squatter


async def test_a_returned_refusal_is_contained_like_a_raise(
    broker: PostgresBroker,
) -> None:
    """The escape a try/except cannot catch.

    Leaf creation probes for the relation and RETURNS
    `LeafCatalogConflict` when the name is taken. A returned refusal
    never reaches an exception handler, so the pass used to return on
    the first one and deny coverage to every class sorting after it.
    """
    await _register_owned_class(broker, _RETURN_SURVIVOR_KEY)
    horizon = 5
    owed = horizon + 1
    assert await _leaf_count(broker, _RETURN_SURVIVOR_KEY) == 0

    squatter = await _plant_class_with_squatted_leaf(broker)
    try:
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection,
                history_horizon_days=horizon,
                heartbeat_horizon_hours=3,
            )

        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert _REFUSING_KEY in outcome.refusal, outcome
        survivor_count = await _leaf_count(broker, _RETURN_SURVIVOR_KEY)
        assert survivor_count == owed, (
            'a returned refusal still denied coverage to the class after '
            f'it: the owned class holds {survivor_count} '
            f'leaves, not the {owed} a horizon of {horizon} owes it'
        )
    finally:
        await _drop_if_present(broker, squatter)
        await _drop_if_present(broker, finite_class_parent_name(_REFUSING_KEY))
        await _forget_refusing_class(broker)
        await _forget_owned_class(broker, _RETURN_SURVIVOR_KEY)


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

    The trigger is a real database error rather than a Python one: the
    class is registered but its parent relation does not exist, so the
    CREATE for its leaf fails with `UndefinedTable` inside the
    transaction the caller owns.
    """
    await _plant_class_without_relation(broker)

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
        await _forget_refusing_class(broker)


# --- The offset-1 survivor: the only shape that sees the scope fix --------
#
# Rolling a savepoint back and releasing it are observationally identical
# when the class created nothing before refusing. Every other trigger in
# this file refuses at offset 0 or raises first, so none of them can tell
# the two apart. This one makes the class CREATE and THEN refuse.

_SURVIVOR_KEY = 'q_survivor_7d'


async def _drop_if_present(broker: PostgresBroker, relation: str) -> None:
    async with broker.async_engine.begin() as connection:
        await connection.execute(
            text(f'DROP TABLE IF EXISTS {relation} CASCADE')
        )


async def _forget_survivor_class(broker: PostgresBroker) -> None:
    parent = finite_class_parent_name(_SURVIVOR_KEY)
    async with broker.async_engine.begin() as connection:
        leaves = (
            await connection.execute(
                text(
                    f'SELECT leaf_name FROM {LEAF_CATALOG} '
                    'WHERE class_key = :key'
                ),
                {'key': _SURVIVOR_KEY},
            )
        ).scalars().all()
        for leaf_name in leaves:
            await connection.execute(
                text(f'DROP TABLE IF EXISTS {leaf_name} CASCADE')
            )
        await connection.execute(
            text(f'DELETE FROM {LEAF_CATALOG} WHERE class_key = :key'),
            {'key': _SURVIVOR_KEY},
        )
        await connection.execute(
            text(f'DROP TABLE IF EXISTS {parent} CASCADE')
        )
        await connection.execute(
            text(f'DELETE FROM {RETENTION_CLASSES} WHERE class_key = :key'),
            {'key': _SURVIVOR_KEY},
        )


async def test_a_refusal_keeps_the_leaves_created_before_it(
    broker: PostgresBroker,
) -> None:
    """A refusing class keeps the days it finished first.

    Leaf coverage stops at its first refusal and returns what it built
    before it. Unwinding the savepoint to erase the refusal would erase
    those days too, so a permanently refusing day would starve every
    EARLIER day of its class forever -- re-doing and re-discarding the
    same work on every pass.

    The squatter sits one day PAST the first pass's last leaf, and that
    distance is the whole test: the second pass must CREATE a day and
    only then refuse. A squatter on the first day the second pass looks
    at would be met with nothing yet created, and rolling back would be
    indistinguishable from releasing -- the same blindness every other
    trigger in this file has.

    The arithmetic: `EnsureLeafCoverage` refuses a horizon below 2, so
    the first pass covers today plus two days (three leaves). The
    squatter takes day+4. The second pass at horizon 4 finds days 0-2
    conformant, CREATES day+3, then refuses at day+4 -- so four leaves
    survive here and three on a head that unwinds the savepoint.
    """
    await _forget_survivor_class(broker)
    parent = finite_class_parent_name(_SURVIVOR_KEY)
    async with broker.async_engine.begin() as connection:
        now = await database_now(connection)
    day_lower = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Day+2, deliberately: see the docstring.
    squatter = daily_leaf_name(parent, day_lower + timedelta(days=4))
    try:
        async with broker.async_engine.begin() as connection:
            registration = await register_finite_retention_class(
                connection,
                class_key=_SURVIVOR_KEY,
                duration=timedelta(days=7),
            )
            assert isinstance(
                registration, (ClassRegistered, ClassAlreadyRegistered)
            ), registration

        # Two is the floor: `EnsureLeafCoverage` refuses anything lower
        # as the health-contract red line. `range(horizon + 1)` makes
        # that today plus two future days.
        async with broker.async_engine.begin() as connection:
            first = await ensure_partition_coverage(
                connection, history_horizon_days=2, heartbeat_horizon_hours=3
            )
        assert isinstance(first, CoverageEnsured), first
        assert await _leaf_count(broker, _SURVIVOR_KEY) == 3, (
            'premise broken: the first pass must leave today and two days'
        )

        async with broker.async_engine.begin() as connection:
            await connection.execute(
                text(f'CREATE TABLE IF NOT EXISTS {squatter} (x int)')
            )

        # Offsets 0-2 are already conformant, offset 3 is CREATED, and
        # offset 4 meets the squatter and refuses.
        async with broker.async_engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection, history_horizon_days=4, heartbeat_horizon_hours=3
            )

        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert _SURVIVOR_KEY in outcome.refusal, outcome
        assert await _leaf_count(broker, _SURVIVOR_KEY) == 4, (
            'the day the pass finished before refusing was discarded; '
            'containment is unwinding completed work, so a permanently '
            'refusing day starves every earlier day of its class'
        )
    finally:
        await _drop_if_present(broker, squatter)
        await _forget_survivor_class(broker)
