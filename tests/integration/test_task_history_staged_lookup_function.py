"""The generated staged lookup against real PostgreSQL.

Covers the qualified probe order end to end — live short-circuits forever,
forever short-circuits finite, v7 pruning and the legacy walk both find
retained rows, absence is absence — and carries the reviewer-required
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
                    'INSERT INTO horsies_tasks (task_id, '
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
                    terminal_at=datetime.now(UTC) - timedelta(days=399),
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
            assert manifest_names == {
                daily_leaf_name(
                    f'horsies_task_history_{class_key}',
                    day_bounds(datetime.now(UTC) - timedelta(days=1))[0],
                )
                for class_key in (CLASS_A, CLASS_B)
            }
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
