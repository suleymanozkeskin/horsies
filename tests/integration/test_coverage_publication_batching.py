"""The coverage pass publishes the readers once, with one manifest write.

Leaves are created unpublished inside each class's savepoint, and the
publication after the class loop covers all of them. The manifest is
rewritten by one DELETE and one array INSERT, so every row of a
publication shares one timestamp and the probe positions are contiguous.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.classes import (
    ClassRegistered,
    register_finite_retention_class,
)
from horsies.core.history.maintenance import coverage as coverage_module
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    ensure_partition_coverage,
)
from horsies.core.history.names import LEAF_CATALOG, TASK_LOOKUP_MANIFEST
from horsies.core.history.partitions.publication import LoaderRepublished
from horsies.core.history.reads.publisher import (
    StagedLoaderPublisher,
    published_manifest_matches_catalog,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HORIZON_DAYS = 3


class CountingPublisher(StagedLoaderPublisher):
    """The real publisher; every instance adds to one shared count."""

    republished = 0

    async def republish(self, connection: AsyncConnection) -> LoaderRepublished:
        CountingPublisher.republished += 1
        return await super().republish(connection)


async def _register(connection: AsyncConnection) -> str:
    class_key = f'it_pub_{uuid.uuid4().hex[:8]}'
    registration = await register_finite_retention_class(
        connection, class_key=class_key, duration=timedelta(days=7),
    )
    assert isinstance(registration, ClassRegistered), registration
    return class_key


async def test_coverage_creating_leaves_in_two_classes_publishes_once(
    broker: PostgresBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with broker.async_engine.begin() as connection:
        classes = [await _register(connection), await _register(connection)]
    monkeypatch.setattr(coverage_module, 'StagedLoaderPublisher', CountingPublisher)
    CountingPublisher.republished = 0

    async with broker.async_engine.begin() as connection:
        outcome = await ensure_partition_coverage(
            connection,
            history_horizon_days=HORIZON_DAYS,
            heartbeat_horizon_hours=3,
        )
    assert isinstance(outcome, CoverageEnsured), outcome
    assert CountingPublisher.republished == 1

    async with broker.async_engine.connect() as connection:
        created = (await connection.execute(
            text(
                f'SELECT count(*) FROM {LEAF_CATALOG} '
                'WHERE class_key = ANY(:keys)'
            ),
            {'keys': classes},
        )).scalar_one()
        assert created == 2 * (HORIZON_DAYS + 1)
        manifest = (await connection.execute(text(
            f'SELECT probe_position, published_at FROM {TASK_LOOKUP_MANIFEST} '
            'ORDER BY probe_position'
        ))).all()
        assert [row.probe_position for row in manifest] == list(range(len(manifest)))
        assert len({row.published_at for row in manifest}) == 1
        assert await published_manifest_matches_catalog(connection)
