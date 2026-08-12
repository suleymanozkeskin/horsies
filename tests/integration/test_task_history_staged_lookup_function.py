"""The generated staged lookup against real PostgreSQL.

Covers the qualified probe order end to end — live short-circuits cataloged
history leaves, v7 likely/fallback probes and the legacy walk find retained
rows, absence is absence — and carries the reviewer-required
revert-proof regression for the false-absence seam: republishing from a
manifest scoped to one retention class must demonstrably lose the other
class's retained rows, and the publisher must demonstrably not.

The disable is verified before its result counts (AGENTS-TESTING 2.1.1):
the single-class body is asserted to omit the other class's leaf before
the behavioral absence is trusted, so a patch that failed to disable
anything cannot masquerade as a passing regression.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import CreateDailyHistoryLeaf, LeafBounds, LeafRef
from horsies.core.history.identity.uuid7 import MonotonicUuid7Generator
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    read_attached_leaf_rows,
)
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.reads.identity_lookup import (
    HistoryTaskIdentity,
    LiveTaskIdentity,
    TaskIdentityAbsent,
    lookup_task_identity,
)
from horsies.core.history.reads.lookup_generation import (
    manifest_from_catalog,
    render_staged_lookup_function,
)
from horsies.core.history.names import (
    TASK_DETAIL_FUNCTION,
    TASK_LOOKUP_FUNCTION,
    TASK_PROVENANCE_FUNCTION,
    TASK_PROVENANCE_TYPE,
)
from horsies.core.history.reads.publisher import StagedLoaderPublisher

from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    day_bounds,
    frozen_history_row,
    register_class,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_A = 'it_lookup_a'
CLASS_B = 'it_lookup_b'

history_schema = task_history_schema_fixture('task_history_it_lookup')


def v7_with_birth(birth: datetime) -> str:
    """Mint a v7 identifier whose embedded birth is the given instant."""
    milliseconds = int(birth.timestamp() * 1_000)
    generator = MonotonicUuid7Generator(clock_ms=lambda: milliseconds)
    return generator.mint()


async def seed_class_with_row(
    connection: AsyncConnection,
    class_key: str,
    *,
    day: datetime,
) -> str:
    """Register a class, create the day's leaf, insert one retained row."""
    parent = await register_class(connection, class_key)
    lower, upper = day_bounds(day)
    ref = LeafRef(
        leaf_name=daily_leaf_name(parent, lower),
        class_key=class_key,
        bounds=LeafBounds(lower=lower, upper=upper),
    )
    created = await create_daily_leaf(
        connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
    )
    assert isinstance(created, LeafCreated)
    terminal_at = lower + timedelta(hours=6)
    task_id = v7_with_birth(terminal_at - timedelta(seconds=30))
    await connection.execute(
        text(INSERT_HISTORY_ROW_SQL),
        frozen_history_row(
            task_id=task_id, class_key=class_key, terminal_at=terminal_at
        ),
    )
    return task_id


async def seed_two_classes(
    connection: AsyncConnection,
) -> tuple[str, str]:
    yesterday = datetime.now(UTC) - timedelta(days=1)
    row_a = await seed_class_with_row(connection, CLASS_A, day=yesterday)
    row_b = await seed_class_with_row(connection, CLASS_B, day=yesterday)
    await StagedLoaderPublisher().republish(connection)
    return row_a, row_b


class TestLookupOutcomes:
    @pytest.mark.asyncio
    async def test_live_forever_finite_and_absent(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            row_a, row_b = await seed_two_classes(connection)

            live_id = v7_with_birth(datetime.now(UTC))
            await connection.execute(
                text(
                    'INSERT INTO horsies_tasks (id, '
                    'command_fingerprint_version, command_fingerprint) '
                    'VALUES (CAST(:task_id AS uuid), 1, :fingerprint)'
                ),
                {'task_id': live_id, 'fingerprint': sha256(b'live').digest()},
            )
            forever_id = v7_with_birth(datetime.now(UTC) - timedelta(days=400))
            await connection.execute(
                text(INSERT_HISTORY_ROW_SQL),
                frozen_history_row(
                    task_id=forever_id,
                    class_key='forever',
                    terminal_at=datetime.now(UTC),
                ),
            )

            assert isinstance(
                await lookup_task_identity(connection, live_id),
                LiveTaskIdentity,
            )
            assert isinstance(
                await lookup_task_identity(connection, forever_id),
                HistoryTaskIdentity,
            )
            assert isinstance(
                await lookup_task_identity(connection, row_a),
                HistoryTaskIdentity,
            )
            assert isinstance(
                await lookup_task_identity(connection, row_b),
                HistoryTaskIdentity,
            )
            never_seen_v7 = v7_with_birth(datetime.now(UTC) - timedelta(days=1))
            assert (
                await lookup_task_identity(connection, never_seen_v7)
                == TaskIdentityAbsent()
            )

    @pytest.mark.asyncio
    async def test_future_birth_hint_cannot_hide_a_retained_row(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            row_a, _ = await seed_two_classes(connection)
            future_id = v7_with_birth(datetime.now(UTC) + timedelta(days=365))
            await connection.execute(
                text(
                    'UPDATE horsies_task_history '
                    'SET task_id = CAST(:future_id AS uuid) '
                    'WHERE task_id = CAST(:row_a AS uuid)'
                ),
                {'future_id': future_id, 'row_a': row_a},
            )
            await StagedLoaderPublisher().republish(connection)

            assert isinstance(
                await lookup_task_identity(connection, future_id),
                HistoryTaskIdentity,
            )
            assert (
                await lookup_task_identity(connection, str(uuid4()))
                == TaskIdentityAbsent()
            )

    @pytest.mark.asyncio
    async def test_manifest_table_matches_attached_leaves(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            await seed_two_classes(connection)
            manifest_names = {
                row.leaf_name
                for row in (
                    await connection.execute(
                        text(
                            'SELECT leaf_name FROM horsies_task_lookup_manifest'
                        )
                    )
                ).all()
            }
            expected_finite = {
                daily_leaf_name(
                    f'horsies_task_history_{class_key}',
                    day_bounds(datetime.now(UTC) - timedelta(days=1))[0],
                )
                for class_key in (CLASS_A, CLASS_B)
            }
            assert expected_finite <= manifest_names
            assert any(
                name.startswith('horsies_task_history_forever_')
                for name in manifest_names
            )
            publisher = StagedLoaderPublisher()
            for leaf_name in manifest_names:
                assert await publisher.references_leaf(connection, leaf_name)


class TestPublicationAtomicity:
    """Identity, provenance, and manifest regenerate as one publication.

    Same discipline as the false-absence regression: the disable — a
    provenance function regenerated alone from a stale single-class
    manifest — is verified statically before its behavioral divergence
    counts, and the publisher's one-transaction regeneration of both
    functions plus the manifest is what restores agreement.
    """

    @pytest.mark.asyncio
    async def test_publisher_keeps_both_functions_in_agreement(
        self, history_schema: HistorySchema
    ) -> None:
        from horsies.core.history.reads.lookup_generation import (
            render_staged_provenance_function,
        )

        async with history_schema.engine.begin() as connection:
            row_a, row_b = await seed_two_classes(connection)

            # Baseline: both functions see both classes' rows.
            provenance = (
                await connection.execute(
                    text(
                        'SELECT found, status FROM '
                        'horsies_task_provenance_staged('
                        'CAST(:task_id AS uuid))'
                    ),
                    {'task_id': row_b},
                )
            ).one()
            assert provenance.found and provenance.status == 'COMPLETED'

            # Disable: regenerate ONLY provenance from a single-class
            # manifest — the divergence a partial publication would cause.
            stale_manifest = manifest_from_catalog(
                await read_attached_leaf_rows(connection, CLASS_A)
            )
            stale_body = render_staged_provenance_function(stale_manifest)
            leaf_b = daily_leaf_name(
                f'horsies_task_history_{CLASS_B}',
                day_bounds(datetime.now(UTC) - timedelta(days=1))[0],
            )
            # Presence half first: the derived name must appear in the
            # full-manifest body, or the absence assert below is vacuous
            # and guards nothing.
            full_manifest = manifest_from_catalog(
                await read_attached_leaf_rows(connection, CLASS_A)
                + await read_attached_leaf_rows(connection, CLASS_B)
            )
            assert leaf_b in render_staged_provenance_function(full_manifest)
            assert leaf_b not in stale_body
            await connection.execute(text(stale_body))

            # Divergence: identity still finds B's row, provenance does not.
            assert isinstance(
                await lookup_task_identity(connection, row_b),
                HistoryTaskIdentity,
            )
            diverged = (
                await connection.execute(
                    text(
                        'SELECT found FROM horsies_task_provenance_staged('
                        'CAST(:task_id AS uuid))'
                    ),
                    {'task_id': row_b},
                )
            ).one()
            assert not diverged.found

            # Restore: one republish regenerates both plus the manifest.
            await StagedLoaderPublisher().republish(connection)
            restored = (
                await connection.execute(
                    text(
                        'SELECT found, terminalization_kind FROM '
                        'horsies_task_provenance_staged('
                        'CAST(:task_id AS uuid))'
                    ),
                    {'task_id': row_b},
                )
            ).one()
            assert restored.found
            assert restored.terminalization_kind == 'COMPLETE_FUSED'
            assert isinstance(
                await lookup_task_identity(connection, row_a),
                HistoryTaskIdentity,
            )

    @pytest.mark.asyncio
    async def test_publisher_keeps_the_detail_function_in_agreement(
        self, history_schema: HistorySchema
    ) -> None:
        """The atomicity discipline extended to the generated triple: a
        stale detail body diverges from identity, and one republish
        restores agreement across all three functions."""
        from horsies.core.history.reads.lookup_generation import (
            render_staged_detail_function,
        )

        async def detail_locations(
            connection: AsyncConnection, task_id: str
        ) -> list[str]:
            return list(
                (
                    await connection.execute(
                        text(
                            'SELECT location FROM horsies_task_detail_staged('
                            'CAST(:task_id AS uuid))'
                        ),
                        {'task_id': task_id},
                    )
                )
                .scalars()
                .all()
            )

        async with history_schema.engine.begin() as connection:
            _, row_b = await seed_two_classes(connection)
            assert await detail_locations(connection, row_b) == ['HISTORY']

            stale_manifest = manifest_from_catalog(
                await read_attached_leaf_rows(connection, CLASS_A)
            )
            stale_body = render_staged_detail_function(stale_manifest)
            leaf_b = daily_leaf_name(
                f'horsies_task_history_{CLASS_B}',
                day_bounds(datetime.now(UTC) - timedelta(days=1))[0],
            )
            full_manifest = manifest_from_catalog(
                await read_attached_leaf_rows(connection, CLASS_A)
                + await read_attached_leaf_rows(connection, CLASS_B)
            )
            # Presence half first, or the absence assert is vacuous.
            assert leaf_b in render_staged_detail_function(full_manifest)
            assert leaf_b not in stale_body
            await connection.execute(text(stale_body))

            assert await detail_locations(connection, row_b) == []
            assert isinstance(
                await lookup_task_identity(connection, row_b),
                HistoryTaskIdentity,
            )

            await StagedLoaderPublisher().republish(connection)
            assert await detail_locations(connection, row_b) == ['HISTORY']


class TestHeartbeatLeafExclusion:
    """A heartbeat leaf in the shared catalog never enters the manifest.

    Presence half first (the leaf name provably appears when the filter
    is bypassed), then the published bodies are asserted clean and the
    real lookups still resolve — the false-absence discipline applied to
    the cross-consumer hazard.
    """

    @pytest.mark.asyncio
    async def test_republish_excludes_heartbeat_leaves(
        self, history_schema: HistorySchema
    ) -> None:
        from horsies.core.history.partitions.catalog import (
            read_all_attached_leaf_rows,
        )
        from horsies.core.history.reads.lookup_generation import (
            LookupLeaf,
            LookupManifest,
            render_staged_provenance_function,
        )

        async with history_schema.engine.begin() as connection:
            row_a, row_b = await seed_two_classes(connection)

            heartbeat_leaf = 'horsies_heartbeats_2026_08_07_00'
            await connection.execute(
                text(
                    "INSERT INTO horsies_retention_classes "
                    "(class_key, duration, partition_interval, "
                    "finite_parent_name, created_at) VALUES "
                    "('heartbeats', NULL, NULL, NULL, statement_timestamp())"
                )
            )
            await connection.execute(
                text(
                    'INSERT INTO horsies_task_history_leaf_catalog ('
                    'leaf_name, parent_name, class_key, lower_anchor, '
                    'upper_anchor, index_schema_version, id_index_name, '
                    'partition_bound, min_birth_at, min_birth_verified, '
                    'created_at'
                    ") VALUES ("
                    ":leaf_name, 'horsies_heartbeats', 'heartbeats', "
                    "'2026-08-07T00:00:00Z', '2026-08-07T01:00:00Z', 1, "
                    ":index_name, 'FOR VALUES ...', NULL, TRUE, "
                    'statement_timestamp())'
                ),
                {
                    'leaf_name': heartbeat_leaf,
                    'index_name': f'{heartbeat_leaf}_task_idx',
                },
            )

            # Presence half: bypassing the filter provably includes the
            # heartbeat leaf, so the exclusion assert below is non-vacuous.
            rows = await read_all_attached_leaf_rows(connection)
            heartbeat_row = next(
                row for row in rows if row.leaf_name == heartbeat_leaf
            )
            unfiltered = LookupManifest(
                leaves=(
                    LookupLeaf(
                        relation_name=heartbeat_row.leaf_name,
                        lower_anchor=heartbeat_row.lower_anchor,
                        upper_anchor=heartbeat_row.upper_anchor,
                        min_birth_at=None,
                    ),
                ),
                birth_floor=None,
            )
            assert heartbeat_leaf in render_staged_provenance_function(
                unfiltered
            )

            await StagedLoaderPublisher().republish(connection)
            for function_name in (
                'horsies_task_lookup_staged',
                'horsies_task_provenance_staged',
            ):
                # Scoped to this schema and asserted to be a single
                # entry: an unqualified pg_proc read also matches the
                # same function published into another schema of the
                # shared test database, and a changed argument list
                # would leave a second overload beside the current one.
                # Both arrive as MultipleResultsFound, which names
                # neither.
                published = (
                    await connection.execute(
                        text(
                            'SELECT procedure.prosrc, '
                            'pg_get_function_identity_arguments('
                            'procedure.oid) AS identity_arguments '
                            'FROM pg_proc AS procedure '
                            'JOIN pg_namespace AS namespace '
                            '  ON namespace.oid = procedure.pronamespace '
                            'WHERE namespace.nspname = current_schema() '
                            '  AND procedure.proname = :name'
                        ),
                        {'name': function_name},
                    )
                ).all()
                assert len(published) == 1, (
                    f'{function_name} has {len(published)} signatures in '
                    f'this schema: '
                    f'{[row.identity_arguments for row in published]}'
                )
                assert heartbeat_leaf not in published[0].prosrc
            manifest_names = {
                row.leaf_name
                for row in (
                    await connection.execute(
                        text(
                            'SELECT leaf_name FROM '
                            'horsies_task_lookup_manifest'
                        )
                    )
                ).all()
            }
            assert heartbeat_leaf not in manifest_names
            assert isinstance(
                await lookup_task_identity(connection, row_a),
                HistoryTaskIdentity,
            )
            assert isinstance(
                await lookup_task_identity(connection, row_b),
                HistoryTaskIdentity,
            )


STAGED_SIGNATURES = {
    TASK_LOOKUP_FUNCTION: 'p_task_id uuid',
    TASK_PROVENANCE_FUNCTION: 'p_task_id uuid, p_include_live boolean',
    TASK_DETAIL_FUNCTION: 'p_task_id uuid',
}
"""The one signature each staged function may have after publication.

Names come from the program's own constants so a rename cannot skip the
pin; the argument lists are literals, because the change this guards
against is a changed argument list. A fourth staged function has to be
added here by hand — the authority belongs beside the renderers, which
publish the three individually and expose no manifest to iterate.
"""


class TestSupersededSignatures:
    """Republication replaces a staged function; it never accumulates one.

    PostgreSQL overloads by argument list, so CREATE OR REPLACE against a
    changed signature installs a second function beside the first and
    leaves both callable — and a call matching both is refused as
    ambiguous. The publisher drops the superseded provenance signature
    for exactly this reason.

    This guards the NEXT argument-list change. No shipped database
    carries the ambiguous pair: the history program is unreleased, so
    the stale overload has to be planted here to be observed at all.
    """

    @pytest.mark.asyncio
    async def test_republish_leaves_one_signature_per_staged_function(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            await seed_two_classes(connection)

            # Plant the superseded single-parameter provenance overload.
            await connection.execute(
                text(
                    f'CREATE FUNCTION {TASK_PROVENANCE_FUNCTION}'
                    '(p_task_id uuid) '
                    f'RETURNS {TASK_PROVENANCE_TYPE} '
                    'LANGUAGE sql STABLE AS '
                    f"$$ SELECT NULL::{TASK_PROVENANCE_TYPE} $$"
                )
            )

            # Presence half: the plant is really installed beside the
            # current signature, so the assertion below is non-vacuous.
            planted = await _staged_signatures(connection)
            assert planted[TASK_PROVENANCE_FUNCTION] == [
                'p_task_id uuid',
                'p_task_id uuid, p_include_live boolean',
            ]

            await StagedLoaderPublisher().republish(connection)

            published = await _staged_signatures(connection)
            assert published == {
                name: [arguments]
                for name, arguments in STAGED_SIGNATURES.items()
            }


async def _staged_signatures(
    connection: AsyncConnection,
) -> dict[str, list[str]]:
    """Installed argument lists per staged function, this schema only."""
    rows = (
        await connection.execute(
            text(
                'SELECT procedure.proname, '
                'pg_get_function_identity_arguments(procedure.oid) '
                '    AS identity_arguments '
                'FROM pg_proc AS procedure '
                'JOIN pg_namespace AS namespace '
                '  ON namespace.oid = procedure.pronamespace '
                'WHERE namespace.nspname = current_schema() '
                '  AND procedure.proname = ANY(CAST(:names AS text[]))'
            ),
            {'names': list(STAGED_SIGNATURES)},
        )
    ).all()
    installed: dict[str, list[str]] = {}
    for row in rows:
        installed.setdefault(row.proname, []).append(row.identity_arguments)
    return {name: sorted(arguments) for name, arguments in installed.items()}


class TestFalseAbsenceSeamRegression:
    """Revert-proof: a class-scoped manifest loses other classes' rows."""

    @pytest.mark.asyncio
    async def test_single_class_republish_creates_false_absence(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            row_a, row_b = await seed_two_classes(connection)

            # Baseline: the publisher's all-classes regeneration finds both.
            assert isinstance(
                await lookup_task_identity(connection, row_b),
                HistoryTaskIdentity,
            )

            # Disable: regenerate from a manifest scoped to class A only —
            # the exact pre-fix seam shape.
            single_class_manifest = manifest_from_catalog(
                await read_attached_leaf_rows(connection, CLASS_A)
            )
            single_class_body = render_staged_lookup_function(
                single_class_manifest
            )

            # Verify the disable before trusting its result: class B's leaf
            # must be absent from the scoped body and present in the
            # publisher's body, or the patch disabled nothing.
            leaf_b = daily_leaf_name(
                f'horsies_task_history_{CLASS_B}',
                day_bounds(datetime.now(UTC) - timedelta(days=1))[0],
            )
            assert leaf_b not in single_class_body
            all_manifest = manifest_from_catalog(
                await read_attached_leaf_rows(connection, CLASS_A)
                + await read_attached_leaf_rows(connection, CLASS_B)
            )
            assert leaf_b in render_staged_lookup_function(all_manifest)

            await connection.execute(text(single_class_body))

            # The defect: B's row is retained yet the lookup reports absence.
            retained = (
                await connection.execute(
                    text(
                        'SELECT EXISTS (SELECT 1 FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid))'
                    ),
                    {'task_id': row_b},
                )
            ).scalar_one()
            assert retained
            assert (
                await lookup_task_identity(connection, row_b)
                == TaskIdentityAbsent()
            )

            # Restore: the publisher regenerates from every class; the row
            # is found again.
            await StagedLoaderPublisher().republish(connection)
            assert isinstance(
                await lookup_task_identity(connection, row_b),
                HistoryTaskIdentity,
            )
            assert isinstance(
                await lookup_task_identity(connection, row_a),
                HistoryTaskIdentity,
            )
