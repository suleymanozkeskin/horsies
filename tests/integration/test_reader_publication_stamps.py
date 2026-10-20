"""Every rendered reader carries the stamp of the manifest it was rendered from.

A publisher of release 0.5.3 renders the lookup, provenance and detail
functions and rewrites the manifest table, but it does not render the
duplicate guard. After such a publication the manifest equals the catalog,
so the manifest check alone reports the readers as current. The stamps show
that the guard is stale, and the next coverage pass renders it again.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import EnsureLeafCoverage
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    maintain_partition_coverage,
)
from horsies.core.history.maintenance.database import (
    PartitionMaintenanceDatabase,
)
from horsies.core.history.names import (
    TASK_DUPLICATE_GUARD_FUNCTION,
    TASK_LOOKUP_MANIFEST,
)
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.catalog import read_manifest_leaf_rows
from horsies.core.history.partitions.manager import ensure_leaf_coverage
from horsies.core.history.partitions.publication import LoaderRepublished
from horsies.core.history.reads import publisher as publisher_module
from horsies.core.history.reads.lookup_generation import (
    manifest_from_catalog,
    render_staged_detail_function,
    render_staged_lookup_function,
    render_staged_provenance_function,
)
from horsies.core.history.reads.publisher import (
    MANIFEST_STAMP_SQL,
    RENDERED_READER_SIGNATURES,
    StagedLoaderPublisher,
    published_manifest_matches_catalog,
    published_readers_stamped,
)
from tests.integration.task_history_harness import (
    HistorySchema,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

partition_schema = terminalization_schema_fixture(
    'task_history_it_reader_stamps',
    partitioned_heartbeats=True,
)


class PublisherOf053(StagedLoaderPublisher):
    """The publication of release 0.5.3: three readers and the manifest."""

    async def republish(self, connection: AsyncConnection) -> LoaderRepublished:
        selection = await read_manifest_leaf_rows(connection)
        manifest = manifest_from_catalog(
            selection.attached,
            absent_relations=selection.absent_relations,
        )
        for render in (
            render_staged_lookup_function,
            render_staged_provenance_function,
            render_staged_detail_function,
        ):
            await connection.execute(text(render(manifest)))
        rewrite = publisher_module._rewrite_manifest_table  # pyright: ignore[reportPrivateUsage]
        await rewrite(connection, manifest)
        return LoaderRepublished(absent_leaves=())


async def manifest_stamp(connection: AsyncConnection) -> str:
    return str(
        (await connection.execute(text(f'SELECT {MANIFEST_STAMP_SQL}'))).scalar_one()
    )


async def reader_comments(connection: AsyncConnection) -> dict[str, str | None]:
    rows = (
        await connection.execute(
            text(
                'SELECT signature, '
                "obj_description(to_regprocedure(signature), 'pg_proc') "
                'AS comment '
                'FROM unnest(CAST(:signatures AS text[])) AS reader(signature)'
            ),
            {'signatures': list(RENDERED_READER_SIGNATURES)},
        )
    ).all()
    return {str(row.signature): row.comment for row in rows}


async def manifest_leaves(connection: AsyncConnection) -> set[str]:
    rows = (
        await connection.execute(text(f'SELECT leaf_name FROM {TASK_LOOKUP_MANIFEST}'))
    ).scalars().all()
    return {str(row) for row in rows}


async def guard_source(connection: AsyncConnection) -> str:
    return str(
        (
            await connection.execute(
                text('SELECT pg_get_functiondef(to_regprocedure(:signature))'),
                {'signature': f'{TASK_DUPLICATE_GUARD_FUNCTION}(uuid)'},
            )
        ).scalar_one()
    )


def _database(schema: HistorySchema) -> PartitionMaintenanceDatabase:
    return PartitionMaintenanceDatabase(schema.engine, connection_capacity=2)


async def test_publication_stamps_every_rendered_reader(
    partition_schema: HistorySchema,
) -> None:
    first = await maintain_partition_coverage(
        _database(partition_schema),
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first

    async with partition_schema.engine.connect() as connection:
        stamp = await manifest_stamp(connection)
        assert stamp.startswith('horsies:manifest:')
        assert await reader_comments(connection) == {
            signature: stamp for signature in RENDERED_READER_SIGNATURES
        }
        assert await published_readers_stamped(connection)
        assert await StagedLoaderPublisher().is_current(connection)


async def test_a_manifest_rewritten_without_the_guard_republishes_the_guard(
    partition_schema: HistorySchema,
) -> None:
    database = _database(partition_schema)
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.begin() as connection:
        published_before = await manifest_leaves(connection)
        creations = await ensure_leaf_coverage(
            connection,
            EnsureLeafCoverage(
                class_key=DEFAULT_RETENTION_CLASS_KEY, horizon_days=3,
            ),
            PublisherOf053(),
        )
        assert any(isinstance(item, LeafCreated) for item in creations)

    async with partition_schema.engine.connect() as connection:
        added = await manifest_leaves(connection) - published_before
        assert added
        stale_guard = await guard_source(connection)
        assert not any(leaf in stale_guard for leaf in added)
        assert await published_manifest_matches_catalog(connection)
        assert not await published_readers_stamped(connection)
        assert not await StagedLoaderPublisher().is_current(connection)

    healed = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(healed, CoverageEnsured), healed
    assert healed.created_history_leaves == 0
    assert healed.republished
    async with partition_schema.engine.connect() as connection:
        rendered_guard = await guard_source(connection)
        assert all(leaf in rendered_guard for leaf in added)
        assert await published_readers_stamped(connection)


@pytest.mark.parametrize('time_zone', ['Etc/GMT+12', 'Asia/Tokyo'])
async def test_the_stamp_does_not_depend_on_the_session_time_zone(
    partition_schema: HistorySchema,
    time_zone: str,
) -> None:
    first = await maintain_partition_coverage(
        _database(partition_schema),
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.connect() as connection:
        default_stamp = await manifest_stamp(connection)
        await connection.execute(
            text("SELECT set_config('TimeZone', :zone, false)"),
            {'zone': time_zone},
        )
        assert await manifest_stamp(connection) == default_stamp
        assert await published_readers_stamped(connection)
