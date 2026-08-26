"""The staged-loader publisher: the loader side of the publication seam.

Republication regenerates the lookup function and rewrites the published
manifest table in the caller's transaction, so the function and the record
of what it references become visible in one commit. The manifest table —
not the function source — answers `references_leaf`: text-searching
`pg_proc` for a relation name would confuse a leaf with any identifier that
embeds it, while the manifest is exact by construction.

The manifest is assembled inside this module from the complete attached
catalog set across every retention class. Accepting a caller-supplied
manifest was rejected: a caller scoped to one class would silently drop
every other class's leaves from the function, and the resulting false
absence would be undetectable at the call site.

The probe list additionally requires each relation to exist. A leaf the
catalog still calls attached whose relation was dropped out of band would
otherwise be rendered into the function body, which CREATE accepts and
execution rejects — on the finalize path, for every queue and class at
once. Excluding it is honest rather than false absence: a relation that
does not exist holds no rows to hide. The catalog row is left untouched;
`CatalogConflictKind` in `outcomes.py` reserves catalog correction to an
operator, and the exclusion is reported instead.

The manifest table therefore records exactly the probe list and nothing
else. `references_leaf` answers from it, and `drop_detached_leaf` refuses
a drop while it answers True — so an unprobed row written here would
block a legitimate drop on behalf of a reader that never looks at the
relation.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..names import HEARTBEAT_CLASS_KEY, LEAF_CATALOG, TASK_LOOKUP_MANIFEST
from ..partitions.catalog import read_manifest_leaf_rows
from ..partitions.publication import LoaderRepublished
from .lookup_generation import (
    LookupManifest,
    manifest_from_catalog,
    render_staged_detail_function,
    render_staged_lookup_function,
    render_staged_provenance_function,
)


class StagedLoaderPublisher:
    """Implements the partition manager's `LoaderPublication` protocol."""

    async def republish(
        self, connection: AsyncConnection
    ) -> LoaderRepublished:
        """Regenerate function and manifest from the full attached set.

        Leaves the manager still believes attached whose relation no
        longer exists are excluded from the probe list and returned, so
        the regeneration is self-correcting: republishing over a wound
        heals it in one pass instead of rebuilding the same broken
        function.
        """
        selection = await read_manifest_leaf_rows(connection)
        manifest = manifest_from_catalog(
            selection.attached,
            absent_relations=selection.absent_relations,
        )
        await connection.execute(text(render_staged_lookup_function(manifest)))
        # PostgreSQL overloads by signature: the provenance function's
        # include-live parameter changed its signature, and CREATE OR
        # REPLACE would leave a stale single-parameter overload callable
        # forever. Dropping the superseded signature is idempotent.
        await connection.execute(
            text(
                'DROP FUNCTION IF EXISTS '
                'horsies_task_provenance_staged(uuid)'
            )
        )
        await connection.execute(
            text(render_staged_provenance_function(manifest))
        )
        await connection.execute(
            text(render_staged_detail_function(manifest))
        )
        await _rewrite_manifest_table(connection, manifest)
        return LoaderRepublished(
            absent_leaves=tuple(sorted(selection.absent_relations))
        )

    async def references_leaf(
        self,
        connection: AsyncConnection,
        leaf_name: str,
    ) -> bool:
        """Whether the published function still probes `leaf_name`."""
        referenced = (
            await connection.execute(
                text(
                    f"""
                    SELECT EXISTS (
                        SELECT 1 FROM {TASK_LOOKUP_MANIFEST}
                        WHERE leaf_name = :leaf_name
                    )
                    """
                ),
                {'leaf_name': leaf_name},
            )
        ).scalar_one()
        return bool(referenced)


async def published_manifest_absent_leaves(
    connection: AsyncConnection,
) -> tuple[str, ...]:
    """Published probe entries whose relation no longer resolves.

    The divergence trigger for maintenance. Republication is otherwise
    driven by leaf creation, and a leaf vanishing creates nothing — so
    without this probe the broken function stays published until some
    unrelated change forces a regeneration.

    One statement, and empty on a healthy fleet. It answers a different
    question from `republish`'s own return: this names what the CURRENTLY
    PUBLISHED function probes and cannot reach, which includes leaves the
    catalog has since stamped dropped; that names catalog rows still
    called attached whose relation is gone, which is the operator-visible
    anomaly.
    """
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT leaf_name
                FROM {TASK_LOOKUP_MANIFEST}
                WHERE to_regclass(leaf_name) IS NULL
                ORDER BY leaf_name
                """
            )
        )
    ).all()
    return tuple(str(row.leaf_name) for row in rows)


async def published_manifest_matches_catalog(
    connection: AsyncConnection,
) -> bool:
    """Whether every published leaf and its probe metadata are current."""
    matches = (
        await connection.execute(
            text(
                f"""
                WITH expected AS (
                    SELECT
                        leaf_name,
                        row_number() OVER (
                            ORDER BY lower_anchor, leaf_name
                        ) - 1 AS probe_position,
                        lower_anchor,
                        upper_anchor,
                        min_birth_at
                    FROM {LEAF_CATALOG}
                    WHERE detached_at IS NULL
                      AND dropped_at IS NULL
                      AND class_key <> :heartbeat_class
                      AND to_regclass(leaf_name) IS NOT NULL
                ),
                difference AS (
                    (
                        SELECT leaf_name, probe_position,
                               lower_anchor, upper_anchor, min_birth_at
                        FROM expected
                        EXCEPT
                        SELECT leaf_name, probe_position::bigint,
                               lower_anchor, upper_anchor, min_birth_at
                        FROM {TASK_LOOKUP_MANIFEST}
                    )
                    UNION ALL
                    (
                        SELECT leaf_name, probe_position::bigint,
                               lower_anchor, upper_anchor, min_birth_at
                        FROM {TASK_LOOKUP_MANIFEST}
                        EXCEPT
                        SELECT leaf_name, probe_position,
                               lower_anchor, upper_anchor, min_birth_at
                        FROM expected
                    )
                )
                SELECT NOT EXISTS (SELECT 1 FROM difference)
                """
            ),
            {'heartbeat_class': HEARTBEAT_CLASS_KEY},
        )
    ).scalar_one()
    return bool(matches)


async def _rewrite_manifest_table(
    connection: AsyncConnection,
    manifest: LookupManifest,
) -> None:
    await connection.execute(text(f'DELETE FROM {TASK_LOOKUP_MANIFEST}'))
    for position, leaf in enumerate(manifest.leaves):
        await connection.execute(
            text(
                f"""
                INSERT INTO {TASK_LOOKUP_MANIFEST} (
                    leaf_name, probe_position,
                    lower_anchor, upper_anchor, min_birth_at,
                    published_at
                ) VALUES (
                    :leaf_name, :probe_position,
                    :lower_anchor, :upper_anchor, :min_birth_at,
                    statement_timestamp()
                )
                """
            ),
            {
                'leaf_name': leaf.relation_name,
                'probe_position': position,
                'lower_anchor': leaf.lower_anchor,
                'upper_anchor': leaf.upper_anchor,
                'min_birth_at': leaf.min_birth_at,
            },
        )
