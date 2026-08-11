"""A leaf the catalog names and the database no longer has.

PostgreSQL does not validate PL/pgSQL bodies at CREATE, so a staged
reader generated over a catalog row whose relation was dropped out of
band builds happily and dies at execution — with `undefined_table`,
inside the provenance probe that terminalization runs (`move.py` calls
`horsies_task_provenance_staged(p_task_id, FALSE)`). One such leaf
therefore fails every finalize, in every queue and every retention
class, until something regenerates the readers.

The fix filters the PROBE LIST by relation existence and leaves the
catalog untouched: `CatalogConflictKind` reserves catalog correction to
an operator, and stamping the row dropped would erase the evidence of
an accidental drop. The floor is deliberately NOT filtered — see
`test_absence_explanation_survives_the_exclusion`, which is the whole
reason the manifest has two jobs rather than one.

Every healing assertion here is preceded by its own disable
verification: the missing relation is proved to kill the probe before
the repair is trusted, so a test that healed nothing cannot pass by
finding a working function that was never broken.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import CreateDailyHistoryLeaf, LeafBounds, LeafRef
from horsies.core.history.errors import HistoryParentAbsent
from horsies.core.history.heartbeats.partitioning import (
    HEARTBEATS_PARTITIONED_DDL,
)
from horsies.core.history.identity.uuid7 import MonotonicUuid7Generator
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    ensure_partition_coverage,
)
from horsies.core.history.names import LEAF_CATALOG, TASK_PROVENANCE_FUNCTION
from horsies.core.history.outcomes import LeafCreated
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    database_now,
    read_manifest_leaf_rows,
)
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.reads.detail import TaskDetailAbsent, read_task_detail
from horsies.core.history.reads.publisher import (
    StagedLoaderPublisher,
    published_manifest_absent_leaves,
)

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
CLASS_KEY = 'it_missing_leaf'

history_schema = task_history_schema_fixture('task_history_it_missing_leaf')


def v7_with_birth(birth: datetime) -> str:
    """Mint a v7 identifier whose embedded birth is the given instant."""
    milliseconds = int(birth.timestamp() * 1_000)
    generator = MonotonicUuid7Generator(clock_ms=lambda: milliseconds)
    return generator.mint()


async def _create_leaf(
    connection: AsyncConnection,
    *,
    parent: str,
    day: datetime,
) -> str:
    """Create one daily leaf through the manager and return its name."""
    lower, upper = day_bounds(day)
    leaf_name = daily_leaf_name(parent, lower)
    outcome = await create_daily_leaf(
        connection,
        CreateDailyHistoryLeaf(
            leaf=LeafRef(
                leaf_name=leaf_name,
                class_key=CLASS_KEY,
                bounds=LeafBounds(lower=lower, upper=upper),
            )
        ),
        StagedLoaderPublisher(),
    )
    assert isinstance(outcome, LeafCreated), outcome
    return leaf_name


async def _set_min_birth(
    connection: AsyncConnection,
    leaf_name: str,
    birth: datetime,
) -> None:
    """Record a leaf's minimum birth, the catalog fact the floor reads."""
    await connection.execute(
        text(
            f'UPDATE {LEAF_CATALOG} SET min_birth_at = :birth '
            'WHERE leaf_name = :leaf_name'
        ),
        {'birth': birth, 'leaf_name': leaf_name},
    )


async def _probe_provenance(
    connection: AsyncConnection,
    task_id: str,
) -> object:
    """Call the provenance function exactly as terminalization calls it."""
    return (
        await connection.execute(
            text(
                f'SELECT found FROM {TASK_PROVENANCE_FUNCTION}('
                'CAST(:task_id AS uuid), FALSE)'
            ),
            {'task_id': task_id},
        )
    ).scalar_one()


class TestMissingLeafBreaksAndHeals:
    @pytest.mark.asyncio
    async def test_missing_relation_kills_the_provenance_probe(
        self, history_schema: HistorySchema
    ) -> None:
        """The disable verification: the wound is real before it is healed.

        Asserts the SIGNATURE matches the MECHANISM — `undefined_table`
        naming the dropped relation, raised from inside the generated
        function — rather than merely observing that something failed.
        """
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            now = await database_now(connection)
            leaf_name = await _create_leaf(connection, parent=parent, day=now)
            task_id = v7_with_birth(now - timedelta(hours=1))
            await connection.execute(
                text(INSERT_HISTORY_ROW_SQL),
                frozen_history_row(
                    task_id=task_id, class_key=CLASS_KEY, terminal_at=now
                ),
            )

        async with history_schema.engine.connect() as connection:
            assert await _probe_provenance(connection, task_id) is True

        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE {leaf_name}'))

        async with history_schema.engine.connect() as connection:
            with pytest.raises(DBAPIError) as raised:
                await _probe_provenance(connection, task_id)
        assert getattr(raised.value.orig, 'sqlstate', None) == '42P01', (
            f'expected undefined_table from the provenance probe, got '
            f'{raised.value.orig!r}'
        )
        assert leaf_name in str(raised.value.orig)

    @pytest.mark.asyncio
    async def test_republish_excludes_the_missing_leaf_and_names_it(
        self, history_schema: HistorySchema
    ) -> None:
        """One republication heals the readers and reports the loss."""
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            now = await database_now(connection)
            gone = await _create_leaf(connection, parent=parent, day=now)
            kept = await _create_leaf(
                connection, parent=parent, day=now + timedelta(days=1)
            )
            task_id = v7_with_birth(now - timedelta(hours=1))
            await connection.execute(
                text(INSERT_HISTORY_ROW_SQL),
                frozen_history_row(
                    task_id=task_id, class_key=CLASS_KEY, terminal_at=now
                ),
            )

        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE {gone}'))

        # The published readers still name it: the trigger the
        # maintenance pass consults must see the divergence.
        async with history_schema.engine.connect() as connection:
            assert await published_manifest_absent_leaves(connection) == (gone,)

        async with history_schema.engine.begin() as connection:
            republication = await StagedLoaderPublisher().republish(connection)
        assert republication.absent_leaves == (gone,)

        async with history_schema.engine.connect() as connection:
            # The catalog is untouched: the row is still attached, which
            # is the operator's evidence that a drop happened.
            selection = await read_manifest_leaf_rows(connection)
            assert gone in {row.leaf_name for row in selection.attached}
            assert selection.absent_relations == frozenset({gone})
            # The surviving leaf is still probed -- exclusion is scoped
            # to the missing relation, not to the class.
            assert kept not in selection.absent_relations
            # And the probe that died now answers.
            assert await _probe_provenance(connection, task_id) is False
            assert await published_manifest_absent_leaves(connection) == ()

    @pytest.mark.asyncio
    async def test_absence_explanation_survives_the_exclusion(
        self, history_schema: HistorySchema
    ) -> None:
        """A destroyed leaf's tasks did not predate retained history.

        The row is gone either way. What must stay true is the REASON:
        `predates_retained_floor` is a published value that reaches the
        rerun surface, and reporting True here would present an
        accidental drop as ordinary ageing. Fails if the birth floor is
        narrowed to the probe list.
        """
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            now = await database_now(connection)
            gone = await _create_leaf(connection, parent=parent, day=now)
            kept = await _create_leaf(
                connection, parent=parent, day=now + timedelta(days=1)
            )
            # The dropped leaf holds the OLDEST birth, so excluding it
            # from the floor would raise the floor over the task below.
            await _set_min_birth(connection, gone, now - timedelta(hours=6))
            await _set_min_birth(connection, kept, now + timedelta(hours=6))

        task_id = v7_with_birth(now - timedelta(hours=3))

        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE {gone}'))
            await StagedLoaderPublisher().republish(connection)

        async with history_schema.engine.connect() as connection:
            detail = await read_task_detail(connection, task_id=task_id)
        assert isinstance(detail, TaskDetailAbsent), detail
        assert detail.predates_retained_floor is False, (
            'a task whose leaf was destroyed out of band does not predate '
            'retained history; True would report the drop as ageing'
        )

    @pytest.mark.asyncio
    async def test_maintenance_pass_republishes_on_the_divergence_alone(
        self, history_schema: HistorySchema
    ) -> None:
        """The pass heals it without any leaf being created.

        Republication is otherwise driven by leaf creation and by an
        absent reader. A leaf vanishing does neither, so without the
        divergence trigger the broken function stays published and the
        pass reports a healthy fleet while every finalize fails.

        The disable verification is the probe assertion before the pass:
        the readers are proved to name the missing relation first.
        """
        async with history_schema.engine.begin() as connection:
            await connection.execute(text(HEARTBEATS_PARTITIONED_DDL))
            parent = await register_class(connection, CLASS_KEY)
            now = await database_now(connection)
            gone = await _create_leaf(connection, parent=parent, day=now)

        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE {gone}'))

        async with history_schema.engine.connect() as connection:
            assert await published_manifest_absent_leaves(connection) == (gone,)

        async with history_schema.engine.begin() as connection:
            outcome = await ensure_partition_coverage(
                connection,
                history_horizon_days=2,
                heartbeat_horizon_hours=2,
            )
        assert isinstance(outcome, CoverageEnsured), outcome
        assert outcome.republished is True
        assert outcome.absent_leaves == (gone,)

        async with history_schema.engine.connect() as connection:
            assert await published_manifest_absent_leaves(connection) == ()

    @pytest.mark.asyncio
    async def test_unresolvable_parent_raises_rather_than_emptying(
        self, history_schema: HistorySchema
    ) -> None:
        """A session that cannot see the schema must not publish absence.

        `to_regclass` resolves through `search_path`, so a connection
        that cannot see the history schema reports every leaf missing.
        Treating that as data would empty the manifest and turn all
        retained history into genuine false absence, so the parent is
        the discriminator and an unresolvable one fails closed.
        """
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            now = await database_now(connection)
            await _create_leaf(connection, parent=parent, day=now)
            lower, upper = day_bounds(now + timedelta(days=1))
            await connection.execute(
                text(
                    f'INSERT INTO {LEAF_CATALOG} ('
                    'leaf_name, parent_name, class_key, lower_anchor, '
                    'upper_anchor, index_schema_version, id_index_name, '
                    'partition_bound, min_birth_at, min_birth_verified, '
                    'created_at) VALUES ('
                    ':leaf_name, :parent_name, :class_key, :lower, :upper, '
                    "1, :index_name, 'FOR VALUES ...', NULL, TRUE, "
                    'statement_timestamp())'
                ),
                {
                    'leaf_name': 'horsies_task_history_unreachable_2026_01_01',
                    'parent_name': 'horsies_task_history_unreachable',
                    'class_key': CLASS_KEY,
                    'lower': lower,
                    'upper': upper,
                    'index_name': 'horsies_task_history_unreachable_task_idx',
                },
            )

        async with history_schema.engine.connect() as connection:
            with pytest.raises(HistoryParentAbsent, match='cannot see'):
                await read_manifest_leaf_rows(connection)
