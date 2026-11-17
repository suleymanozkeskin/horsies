"""The staged-reader publisher: the loader side of the publication seam.

The readers are rendered in the database by
``horsies_publish_history_readers()`` (0008_history_maintenance_functions.sql),
the one renderer of every client. One call, in the caller's transaction,
rewrites the manifest table from the leaf catalog, replaces the four readers
and stamps each with the manifest it was rendered from, so the readers and
the record of what they probe become visible in one commit. A call on
current readers changes nothing.

The manifest table, not the reader source, answers ``references_leaf``:
``horsies_drop_detached_leaf`` and the Python drop refuse a leaf while the
manifest names it.
"""

from __future__ import annotations

from typing import Final, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import HistoryContractError
from ..names import (
    TASK_DETAIL_FUNCTION,
    TASK_DUPLICATE_GUARD_FUNCTION,
    TASK_LOOKUP_MANIFEST,
    TASK_PROVENANCE_FUNCTION,
    TASK_RESULT_FUNCTION,
)
from ..partitions.publication import LoaderRepublished

PUBLICATION_OUTCOMES: Final = frozenset({'PUBLISHED', 'ALREADY_CURRENT'})
"""The outcomes of ``horsies_publish_history_readers``."""

PUBLISHED_READER_SIGNATURES: Final = (
    f'{TASK_DUPLICATE_GUARD_FUNCTION}(uuid)',
    f'{TASK_PROVENANCE_FUNCTION}(uuid)',
    f'{TASK_DETAIL_FUNCTION}(uuid)',
    f'{TASK_RESULT_FUNCTION}(uuid)',
)
"""The readers that ``horsies_publish_history_readers`` publishes, as the
rows of ``horsies_history_reader_signatures()`` name them. The install reads
their presence on a database that may not hold that function yet."""

_PUBLISH_READERS_SQL: Final = text(
    'SELECT outcome, absent_leaves FROM horsies_publish_history_readers()'
)
_READERS_CURRENT_SQL: Final = text('SELECT horsies_history_readers_current()')
_REFERENCES_LEAF_SQL: Final = text(
    f'SELECT EXISTS (SELECT 1 FROM {TASK_LOOKUP_MANIFEST} WHERE leaf_name = :leaf_name)'
)


def _leaf_names(value: object) -> tuple[str, ...]:
    """The absent-leaf column (text[], in name order) as a tuple; anything else raises."""
    if not isinstance(value, list):
        raise HistoryContractError(
            f'reader publication returned absent leaves {value!r}, not a text array'
        )
    names: list[str] = []
    for name in cast(list[object], value):
        match name:
            case str():
                names.append(name)
            case _:
                raise HistoryContractError(
                    f'reader publication returned absent leaf {name!r}, not a text value'
                )
    return tuple(names)


class StagedLoaderPublisher:
    """Implements the partition manager's ``LoaderPublication`` protocol."""

    async def republish(self, connection: AsyncConnection) -> LoaderRepublished:
        """Publish the readers for the attached history leaves: one statement.

        Leaves the catalog records as attached whose relation no longer exists
        are left out of the readers and returned. A database error propagates
        and aborts the caller's transaction: 25000 when the transaction is not
        READ COMMITTED, 42P01 when no attached leaf resolves its parent. A row
        with an unknown outcome or without an absent-leaf list raises
        ``HistoryContractError``.
        """
        row = (await connection.execute(_PUBLISH_READERS_SQL)).one()
        if row.outcome not in PUBLICATION_OUTCOMES:
            raise HistoryContractError(
                f'reader publication returned unknown outcome {row.outcome!r}'
            )
        return LoaderRepublished(absent_leaves=_leaf_names(row.absent_leaves))

    async def is_current(self, connection: AsyncConnection) -> bool:
        """Whether the published readers serve the current catalog
        (``horsies_history_readers_current``)."""
        current = (await connection.execute(_READERS_CURRENT_SQL)).scalar_one()
        if not isinstance(current, bool):
            raise HistoryContractError(
                f'reader freshness check returned {current!r}, not a boolean'
            )
        return current

    async def references_leaf(
        self,
        connection: AsyncConnection,
        leaf_name: str,
    ) -> bool:
        """Whether the published manifest still names ``leaf_name``."""
        referenced = (
            await connection.execute(_REFERENCES_LEAF_SQL, {'leaf_name': leaf_name})
        ).scalar_one()
        if not isinstance(referenced, bool):
            raise HistoryContractError(
                f'manifest lookup of {leaf_name} returned {referenced!r}, not a boolean'
            )
        return referenced


async def published_manifest_absent_leaves(
    connection: AsyncConnection,
) -> tuple[str, ...]:
    """Published manifest entries whose relation no longer resolves, in name order.

    It names what the published readers probe and cannot reach, which
    includes leaves the catalog has since recorded as dropped. Empty on a
    healthy database.
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
