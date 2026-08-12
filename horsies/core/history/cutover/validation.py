"""Stage 6: post-cutover validation — read-only, concrete, typed.

Every check names a fact the frozen posture requires: no terminal row
lives, the live-only status domain holds, every declared not-null
cutover column is not-null in the catalog, the identity columns are
uuid, the heartbeat shape is partitioned, and the history population
reconciles with the relocation ledger. Violations are sentences, not
booleans."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..names import TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT
from ..terminalization.live_cutover import CUTOVER_COLUMNS
from ..terminalization.move import LIVE_TASKS
from .relocation import RELOCATION_LEDGER
from .state import cutover_complete


@dataclass(frozen=True, slots=True)
class CutoverValidated:
    history_rows: int
    ledger_rows: int


@dataclass(frozen=True, slots=True)
class CutoverInvalid:
    violations: tuple[str, ...]


_UUID_COLUMNS: tuple[tuple[str, str], ...] = (
    (LIVE_TASKS, 'id'),
    ('horsies_task_attempts', 'task_id'),
    ('horsies_workflows', 'id'),
    ('horsies_workflows', 'parent_workflow_id'),
    ('horsies_workflows', 'root_workflow_id'),
    ('horsies_workflow_tasks', 'id'),
    ('horsies_workflow_tasks', 'workflow_id'),
    ('horsies_workflow_tasks', 'task_id'),
    ('horsies_heartbeats', 'task_id'),
)


async def validate_cutover(
    connection: AsyncConnection,
) -> CutoverValidated | CutoverInvalid:
    structural = await validate_cutover_structure(connection)
    if isinstance(structural, CutoverInvalid):
        return structural
    if not await cutover_complete(connection):
        return CutoverInvalid(
            violations=('the durable cutover completion marker is absent',)
        )
    return structural


async def validate_cutover_structure(
    connection: AsyncConnection,
) -> CutoverValidated | CutoverInvalid:
    """Validate the frozen posture without consulting its durable marker.

    Schema migration uses this before writing the marker for a fresh or
    already-cut-over database. The operator-facing validator additionally
    requires the marker through ``validate_cutover`` above.
    """
    violations: list[str] = []

    terminal = int(
        (
            await connection.execute(
                text(
                    f'SELECT count(*) FROM {LIVE_TASKS} '
                    "WHERE status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')"
                )
            )
        ).scalar_one()
    )
    if terminal:
        violations.append(f'{terminal} terminal rows remain live')

    status_domain = bool(
        (
            await connection.execute(
                text(
                    'SELECT EXISTS (SELECT 1 FROM pg_constraint '
                    'WHERE conrelid = CAST(:relation AS regclass) '
                    f"AND conname = '{LIVE_TASKS}_live_status_only')"
                ),
                {'relation': LIVE_TASKS},
            )
        ).scalar_one()
    )
    if not status_domain:
        violations.append('the live-only status domain is absent')

    not_null = {
        str(row.attname): bool(row.attnotnull)
        for row in (
            await connection.execute(
                text(
                    'SELECT attname, attnotnull FROM pg_attribute '
                    'WHERE attrelid = CAST(:relation AS regclass) '
                    'AND attnum > 0 AND NOT attisdropped'
                ),
                {'relation': LIVE_TASKS},
            )
        ).all()
    }
    for column in CUTOVER_COLUMNS:
        if column.not_null and not not_null.get(column.name, False):
            violations.append(
                f'declared not-null column {column.name} is nullable'
            )

    for relation, column_name in _UUID_COLUMNS:
        is_uuid = bool(
            (
                await connection.execute(
                    text(
                        "SELECT atttypid = 'uuid'::regtype "
                        'FROM pg_attribute '
                        'WHERE attrelid = CAST(:relation AS regclass) '
                        'AND attname = :column'
                    ),
                    {'relation': relation, 'column': column_name},
                )
            ).scalar_one()
        )
        if not is_uuid:
            violations.append(f'{relation}.{column_name} is not uuid')

    heartbeats_partitioned = bool(
        (
            await connection.execute(
                text(
                    "SELECT relkind = 'p' FROM pg_class "
                    "WHERE oid = 'horsies_heartbeats'::regclass"
                )
            )
        ).scalar_one()
    )
    if not heartbeats_partitioned:
        violations.append('the heartbeat shape is not partitioned')

    forever_partitioned = bool(
        (
            await connection.execute(
                text(
                    "SELECT relkind = 'p' FROM pg_class "
                    'WHERE oid = to_regclass(:relation)'
                ),
                {'relation': TASK_HISTORY_FOREVER},
            )
        ).scalar_one_or_none()
    )
    if not forever_partitioned:
        violations.append('the forever history class is not RANGE-partitioned')
    else:
        uncataloged_forever_leaves = int(
            (
                await connection.execute(
                    text(
                        """
                        SELECT count(*)
                        FROM pg_partition_tree(
                            CAST(:relation AS regclass)
                        ) AS tree
                        JOIN pg_class AS child ON child.oid = tree.relid
                        LEFT JOIN horsies_task_history_leaf_catalog AS catalog
                          ON catalog.leaf_name = child.relname
                         AND catalog.detached_at IS NULL
                         AND catalog.dropped_at IS NULL
                        WHERE tree.isleaf
                          AND catalog.leaf_name IS NULL
                        """
                    ),
                    {'relation': TASK_HISTORY_FOREVER},
                )
            ).scalar_one()
        )
        if uncataloged_forever_leaves:
            violations.append(
                f'{uncataloged_forever_leaves} forever history leaves '
                'are absent from the leaf catalog'
            )

    totals = (
        await connection.execute(
            text(
                f'SELECT (SELECT count(*) FROM {TASK_HISTORY_PARENT}) '
                'AS history_rows, '
                f'(SELECT COALESCE(sum(rows_relocated), 0) '
                f'FROM {RELOCATION_LEDGER}) AS ledger_rows'
            )
        )
    ).one()
    history_rows = int(totals.history_rows)
    ledger_rows = int(totals.ledger_rows)
    if history_rows < ledger_rows:
        violations.append(
            f'history holds {history_rows} rows but the ledger '
            f'recorded {ledger_rows} relocations'
        )

    if violations:
        return CutoverInvalid(violations=tuple(violations))
    return CutoverValidated(
        history_rows=history_rows, ledger_rows=ledger_rows
    )
