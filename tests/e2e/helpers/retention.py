"""Remove a retention class a test created, without stranding readers.

The staged history readers are GENERATED from the leaf catalog, and
PL/pgSQL bodies are not validated at CREATE. A function that still names
a dropped leaf is therefore created happily and dies at execution — and
those readers sit on the finalize path, so one missing leaf fails every
terminalization, whatever queue or class it belongs to.

Order is the whole point:

  1. delete the catalog rows, so the class's leaves leave the manifest
  2. republish, regenerating the readers without them
  3. only then drop the relations

Between 1 and 2 the readers still name the leaves, which is harmless
because the relations still exist. Dropping first inverts that into the
window that breaks: published readers naming relations that are gone.
"""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.history.reads.publisher import StagedLoaderPublisher


async def _forget(database_url: str, class_key: str) -> None:
    engine = create_async_engine(database_url, isolation_level='AUTOCOMMIT')
    try:
        async with engine.connect() as connection:
            leaves = (
                await connection.execute(
                    text(
                        'SELECT leaf_name FROM '
                        'horsies_task_history_leaf_catalog '
                        'WHERE class_key = :key'
                    ),
                    {'key': class_key},
                )
            ).scalars().all()

            await connection.execute(
                text(
                    'DELETE FROM horsies_task_history_leaf_catalog '
                    'WHERE class_key = :key'
                ),
                {'key': class_key},
            )

        # Republish on its own transaction, after the catalog no longer
        # offers these leaves and before anything is dropped.
        async with engine.begin() as connection:
            await StagedLoaderPublisher().republish(connection)

        async with engine.connect() as connection:
            for leaf_name in leaves:
                await connection.execute(
                    text(f'DROP TABLE IF EXISTS {leaf_name} CASCADE')
                )
            await connection.execute(
                text(
                    'DROP TABLE IF EXISTS '
                    f'horsies_task_history_{class_key} CASCADE'
                )
            )
            await connection.execute(
                text(
                    'DELETE FROM horsies_retention_classes '
                    'WHERE class_key = :key'
                ),
                {'key': class_key},
            )
    finally:
        await engine.dispose()


def forget_retention_class(database_url: str, class_key: str) -> None:
    """Drop `class_key` and everything built for it, readers included."""
    asyncio.run(_forget(database_url, class_key))
