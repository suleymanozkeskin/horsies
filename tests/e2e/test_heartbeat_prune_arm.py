"""A booted worker drops a leaf that is already past its horizon.

The pruning driver has integration coverage that calls it directly. This
enters where a deployment does: a leaf is planted overdue, a real worker
is booted through the CLI, and the leaf must be gone without anything in
the test asking for it.

The leaf is overdue because its BOUNDS are backdated, not because a
clock was moved. The sweep selects on `upper_anchor + duration <=
statement_timestamp()`, so a leaf anchored well before the horizon is
genuinely expired by the same arithmetic production uses.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    create_hourly_heartbeat_leaf,
    hourly_leaf_name,
    hourly_leaf_ref,
)
from horsies.core.history.partitions.manager import (
    LeafAlreadyConformant,
    LeafCreated,
)
from tests.e2e.helpers.worker import run_worker

pytestmark = [pytest.mark.e2e]

INSTANCE = 'tests.e2e.tasks.instance_queue_retention:app'

DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ.get("DB_PASSWORD", "")}'
    '@localhost:5432/horsies',
)

# Far enough back that no configured heartbeat horizon (ceiling 48h)
# could still consider the leaf live.
_MINIMUM_AGE = timedelta(hours=72)


def _unremembered_hour() -> datetime:
    """An hour the catalog has never held a leaf for.

    Stepping back from a fixed offset is not enough on a database that
    has been lived in: the catalog keeps dropped leaves as rows, and
    planting a leaf on an hour it already remembers collides with that
    memory instead of testing anything. Rows are checked regardless of
    `dropped_at` for exactly that reason.
    """
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as connection:
            candidate = datetime.now(UTC).replace(
                minute=0, second=0, microsecond=0
            ) - _MINIMUM_AGE
            for _ in range(240):
                remembered = (
                    connection.execute(
                        text(
                            'SELECT 1 FROM '
                            'horsies_task_history_leaf_catalog '
                            'WHERE leaf_name = :name'
                        ),
                        {'name': hourly_leaf_name(candidate)},
                    )
                ).scalar_one_or_none()
                if remembered is None:
                    return candidate
                candidate -= timedelta(hours=1)
            raise RuntimeError(
                'no unremembered hour within 240 hours of the offset'
            )
    finally:
        engine.dispose()


def _leaf_exists(leaf_name: str) -> bool:
    """Whether the relation itself is still present."""
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as connection:
            return (
                connection.execute(
                    text('SELECT to_regclass(:name)'),
                    {'name': leaf_name},
                )
            ).scalar_one() is not None
    finally:
        engine.dispose()


def _plant_overdue_leaf(lower: datetime) -> str:
    """Create one heartbeat leaf whose bounds are already past.

    Through the shipped creation path, not a hand-written INSERT: the
    catalog carries columns a test has no business inventing values for,
    and a leaf assembled by hand would be testing the drop against a row
    the real machinery would never have written.
    """

    async def _create() -> str:
        engine = create_async_engine(DB_URL)
        try:
            async with engine.begin() as connection:
                outcome = await create_hourly_heartbeat_leaf(
                    connection,
                    CreateHourlyHeartbeatLeaf(leaf=hourly_leaf_ref(lower)),
                )
            match outcome:
                case LeafCreated() | LeafAlreadyConformant():
                    return hourly_leaf_name(lower)
                case refusal:
                    raise RuntimeError(f'could not plant the leaf: {refusal!r}')
        finally:
            await engine.dispose()

    return asyncio.run(_create())


def _forget_leaf(leaf_name: str) -> None:
    engine = create_engine(DB_URL, isolation_level='AUTOCOMMIT')
    try:
        with engine.connect() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS {leaf_name}'))
            connection.execute(
                text(
                    'DELETE FROM horsies_task_history_leaf_catalog '
                    'WHERE leaf_name = :name'
                ),
                {'name': leaf_name},
            )
    finally:
        engine.dispose()


def test_a_booted_worker_drops_an_overdue_leaf() -> None:
    lower = _unremembered_hour()
    leaf_name = _plant_overdue_leaf(lower)
    try:
        assert _leaf_exists(leaf_name), (
            'setup failed: the overdue leaf was not created'
        )

        def _leaf_is_gone() -> bool:
            return not _leaf_exists(leaf_name)

        with run_worker(
            INSTANCE,
            processes=1,
            timeout=90.0,
            ready_check=_leaf_is_gone,
        ):
            dropped = _leaf_is_gone()

        assert dropped, (
            f'{leaf_name} was still present after a worker ran its '
            'maintenance; nothing in the deployment drops expired leaves'
        )
    finally:
        _forget_leaf(leaf_name)


def test_the_planted_leaf_is_genuinely_expired() -> None:
    """Guard the premise: an age below the horizon would prove nothing."""
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as connection:
            horizon = (
                connection.execute(
                    text(
                        'SELECT duration FROM horsies_retention_classes '
                        "WHERE class_key = 'heartbeats'"
                    )
                )
            ).scalar_one_or_none()
    finally:
        engine.dispose()

    if horizon is None:
        pytest.skip('the heartbeat class is not registered yet')
    assert _MINIMUM_AGE > horizon, (
        f'the planted leaf is {_MINIMUM_AGE} old against a {horizon} '
        'horizon; it would not be due for a drop'
    )
