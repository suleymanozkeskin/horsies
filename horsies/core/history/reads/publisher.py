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
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..names import TASK_LOOKUP_MANIFEST
from ..partitions.catalog import read_all_attached_leaf_rows
from .lookup_generation import (
    LookupManifest,
    manifest_from_catalog,
    render_staged_lookup_function,
)


class StagedLoaderPublisher:
    """Implements the partition manager's `LoaderPublication` protocol."""

    async def republish(self, connection: AsyncConnection) -> None:
        """Regenerate function and manifest from the full attached set."""
        manifest = manifest_from_catalog(
            await read_all_attached_leaf_rows(connection)
        )
        await connection.execute(text(render_staged_lookup_function(manifest)))
        await _rewrite_manifest_table(connection, manifest)

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
