"""Schema-v35 conversion of the forever class to bounded RANGE leaves.

The top-level history table remains LIST-partitioned by retention class.
``forever`` changes from one unbounded leaf into a RANGE parent. Existing
rows older than the current UTC day stay in one bounded legacy leaf; rows
from the current day move into its daily leaf. The conversion therefore
does not rewrite the historical population, while every subsequent window
can prune the legacy leaf once its upper bound is outside the window.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import CreateDailyHistoryLeaf, LeafBounds, LeafRef
from ..ddl.tables import FOREVER_CLASS_KEY, TASK_HISTORY_FOREVER_DDL
from ..errors import HistoryContractError
from ..names import LEAF_CATALOG, TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT
from ..outcomes import LeafAlreadyConformant, LeafCreated, LeafIndexRepaired
from .catalog import (
    INDEX_SCHEMA_VERSION,
    capture_partition_bound_utc,
    daily_leaf_name,
    database_now,
    leaf_enqueued_index_name,
    leaf_id_index_name,
)
from .manager import create_daily_leaf
from .publication import UnpublishedLoader


FOREVER_LEGACY_LEAF = 'horsies_task_history_forever_before_v35'
_UTC = timezone.utc
_CATALOG_LOWER = datetime(1970, 1, 1, tzinfo=_UTC)


async def ensure_forever_range_partitioning(
    connection: AsyncConnection,
) -> int:
    """Ensure a RANGE parent and current daily leaf for ``forever``.

    Returns the number of rows moved from the old unbounded leaf into the
    current daily leaf. The caller owns the transaction. A v34 conversion is
    atomic: any refusal rolls the relation renames, row move, attachment, and
    catalog writes back together.
    """
    relkind = (
        await connection.execute(
            text('SELECT relkind FROM pg_class ' 'WHERE oid = to_regclass(:relation)'),
            {'relation': TASK_HISTORY_FOREVER},
        )
    ).scalar_one_or_none()
    if relkind not in ('r', 'p'):
        raise HistoryContractError(
            f'{TASK_HISTORY_FOREVER} must be a table or partitioned table, '
            f'found relkind {relkind!r}'
        )

    now = await database_now(connection)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    moved = 0
    if relkind == 'r':
        moved = await _convert_unbounded_leaf(connection, today=today)

    leaf = LeafRef(
        leaf_name=daily_leaf_name(TASK_HISTORY_FOREVER, today),
        class_key=FOREVER_CLASS_KEY,
        bounds=LeafBounds(lower=today, upper=tomorrow),
    )
    creation = await create_daily_leaf(
        connection,
        CreateDailyHistoryLeaf(leaf=leaf),
        UnpublishedLoader(),
    )
    match creation:
        case LeafCreated() | LeafAlreadyConformant() | LeafIndexRepaired():
            return moved
        case _:
            raise HistoryContractError(
                f'current forever leaf could not be ensured: {creation!r}'
            )


async def _convert_unbounded_leaf(
    connection: AsyncConnection,
    *,
    today: datetime,
) -> int:
    legacy_id_index = leaf_id_index_name(FOREVER_LEGACY_LEAF)
    legacy_ordering_index = leaf_enqueued_index_name(FOREVER_LEGACY_LEAF)
    today_leaf = daily_leaf_name(TASK_HISTORY_FOREVER, today)

    if any(
        len(identifier.encode()) > 63
        for identifier in (
            FOREVER_LEGACY_LEAF,
            legacy_id_index,
            legacy_ordering_index,
            today_leaf,
        )
    ):
        raise AssertionError('forever conversion identifier exceeds PostgreSQL limit')

    await connection.execute(
        text(
            f'ALTER TABLE {TASK_HISTORY_PARENT} '
            f'DETACH PARTITION {TASK_HISTORY_FOREVER}'
        )
    )
    await connection.execute(
        text(f'DROP INDEX IF EXISTS {TASK_HISTORY_FOREVER}_task_idx')
    )
    await connection.execute(
        text(f'DROP INDEX IF EXISTS {TASK_HISTORY_FOREVER}_enqueued_idx')
    )
    await connection.execute(
        text(f'ALTER TABLE {TASK_HISTORY_FOREVER} ' f'RENAME TO {FOREVER_LEGACY_LEAF}')
    )
    await connection.execute(text(TASK_HISTORY_FOREVER_DDL))

    # Create the current leaf without publishing while the legacy leaf is
    # detached. The caller republishes only after the conversion is complete.
    current = LeafRef(
        leaf_name=today_leaf,
        class_key=FOREVER_CLASS_KEY,
        bounds=LeafBounds(lower=today, upper=today + timedelta(days=1)),
    )
    creation = await create_daily_leaf(
        connection,
        CreateDailyHistoryLeaf(leaf=current),
        UnpublishedLoader(),
    )
    if not isinstance(creation, LeafCreated):
        raise HistoryContractError(
            f'forever conversion could not create current leaf: {creation!r}'
        )

    moved = int(
        (
            await connection.execute(
                text(
                    f"""
                    WITH moved AS (
                        DELETE FROM {FOREVER_LEGACY_LEAF}
                        WHERE retention_anchor_at >= :today
                        RETURNING *
                    ), inserted AS (
                        INSERT INTO {TASK_HISTORY_FOREVER}
                        SELECT * FROM moved
                        RETURNING 1
                    )
                    SELECT count(*) FROM inserted
                    """
                ),
                {'today': today},
            )
        ).scalar_one()
    )

    legacy_check = f'{FOREVER_LEGACY_LEAF}_anchor_check'
    await connection.execute(
        text(
            f'ALTER TABLE {FOREVER_LEGACY_LEAF} '
            f'ADD CONSTRAINT {legacy_check} '
            f"CHECK (retention_anchor_at < '{today.isoformat()}')"
        )
    )
    await connection.execute(
        text(
            f'ALTER TABLE {TASK_HISTORY_FOREVER} '
            f'ATTACH PARTITION {FOREVER_LEGACY_LEAF} '
            f"FOR VALUES FROM (MINVALUE) TO ('{today.isoformat()}')"
        )
    )
    await connection.execute(
        text(f'CREATE INDEX {legacy_id_index} ' f'ON {FOREVER_LEGACY_LEAF} (task_id)')
    )
    await connection.execute(
        text(
            f'CREATE INDEX {legacy_ordering_index} '
            f'ON {FOREVER_LEGACY_LEAF} (enqueued_at)'
        )
    )

    bound = await capture_partition_bound_utc(connection, FOREVER_LEGACY_LEAF)
    await connection.execute(
        text(
            f"""
            INSERT INTO {LEAF_CATALOG} (
                leaf_name, parent_name, class_key,
                lower_anchor, upper_anchor,
                index_schema_version, id_index_name, partition_bound,
                min_birth_at, min_birth_verified, created_at
            ) VALUES (
                :leaf_name, :parent_name, :class_key,
                :lower, :upper,
                :index_schema_version, :id_index_name, :partition_bound,
                NULL, FALSE, statement_timestamp()
            )
            """
        ),
        {
            'leaf_name': FOREVER_LEGACY_LEAF,
            'parent_name': TASK_HISTORY_FOREVER,
            'class_key': FOREVER_CLASS_KEY,
            'lower': _CATALOG_LOWER,
            'upper': today,
            'index_schema_version': INDEX_SCHEMA_VERSION,
            'id_index_name': legacy_id_index,
            'partition_bound': bound,
        },
    )
    await connection.execute(text(f'ANALYZE {FOREVER_LEGACY_LEAF}'))
    return moved
