"""Durable state and exact indexes for bounded recovery scans."""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


CREATE_RECOVERY_SCAN_CURSORS_SQL = text("""
CREATE TABLE IF NOT EXISTS horsies_recovery_scan_cursors (
    scan_name varchar(64) PRIMARY KEY,
    last_created_at timestamptz,
    last_id uuid,
    cycle_upper_created_at timestamptz,
    cycle_upper_id uuid,
    claim_token uuid,
    claim_expires_at timestamptz,
    completed_cycles bigint NOT NULL DEFAULT 0,
    last_scan_rows integer NOT NULL DEFAULT 0,
    last_candidate_rows integer NOT NULL DEFAULT 0,
    last_scan_at timestamptz,
    CONSTRAINT horsies_recovery_cursor_last_pair CHECK (
        (last_created_at IS NULL) = (last_id IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_upper_pair CHECK (
        (cycle_upper_created_at IS NULL) = (cycle_upper_id IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_claim_pair CHECK (
        (claim_token IS NULL) = (claim_expires_at IS NULL)
    ),
    CONSTRAINT horsies_recovery_cursor_last_has_upper CHECK (
        last_id IS NULL OR cycle_upper_id IS NOT NULL
    )
)
""")

SEED_RECOVERY_SCAN_CURSORS_SQL = text("""
INSERT INTO horsies_recovery_scan_cursors (scan_name)
VALUES ('running_workflows'), ('orphan_workflow_tasks')
ON CONFLICT (scan_name) DO NOTHING
""")


@dataclass(frozen=True)
class RecoveryIndex:
    """One fixed recovery index and its expected owner."""

    name: str
    table: str
    create_sql: str


RECOVERY_INDEXES = (
    RecoveryIndex(
        name='idx_horsies_workflows_running_recovery_scan',
        table='horsies_workflows',
        create_sql=(
            'CREATE INDEX CONCURRENTLY '
            'idx_horsies_workflows_running_recovery_scan '
            'ON horsies_workflows (created_at, id) INCLUDE (name) '
            "WHERE status = 'RUNNING'"
        ),
    ),
    RecoveryIndex(
        name='idx_horsies_tasks_orphan_recovery_scan',
        table='horsies_tasks',
        create_sql=(
            'CREATE INDEX CONCURRENTLY '
            'idx_horsies_tasks_orphan_recovery_scan '
            'ON horsies_tasks (created_at, id) '
            'WHERE is_workflow_task = TRUE '
            "AND status IN ('CLAIMED', 'PENDING')"
        ),
    ),
)

_INDEX_NAME = re.compile(r'^[a-z][a-z0-9_]*$')
_CLAIM_PREFIX = 'horsies_recovery_index_claim_'
_index_inspection_pause: Callable[[RecoveryIndex], Awaitable[None]] | None = None

_INDEX_RELATION_STATE_SQL = text("""
SELECT i.indrelid = to_regclass(:table_name)
FROM pg_class AS relation
LEFT JOIN pg_index AS i ON i.indexrelid = relation.oid
WHERE relation.oid = to_regclass(:index_name)
""")

_ABANDONED_CLAIMS_SQL = text("""
SELECT relation.relname
FROM pg_class AS relation
JOIN pg_index AS index_state ON index_state.indexrelid = relation.oid
WHERE index_state.indrelid = to_regclass(:table_name)
  AND relation.relname LIKE 'horsies_recovery_index_claim\\_%' ESCAPE '\\'
""")


async def _relation_owner(
    connection: 'AsyncConnection',
    *,
    index_name: str,
    table_name: str,
) -> bool | None:
    """Return true for the expected owner, false for a conflict, or none."""
    result = await connection.execute(
        _INDEX_RELATION_STATE_SQL,
        {'index_name': index_name, 'table_name': table_name},
    )
    row = result.one_or_none()
    if row is None:
        return None
    owner = row[0]
    return bool(owner) if owner is not None else False


def _safe_index_name(name: str) -> str:
    if _INDEX_NAME.fullmatch(name) is None:
        raise RuntimeError(f'unsafe recovery index name: {name!r}')
    return name


async def _claim_existing_index(
    engine: 'AsyncEngine',
    index: RecoveryIndex,
) -> str | None:
    """Rename an expected canonical index and recheck its owner."""
    async with engine.begin() as connection:
        owner = await _relation_owner(
            connection,
            index_name=index.name,
            table_name=index.table,
        )
        if _index_inspection_pause is not None:
            await _index_inspection_pause(index)
        match owner:
            case None:
                return None
            case False:
                raise RuntimeError(
                    f'{index.name} belongs to a relation other than '
                    f'{index.table}'
                )
            case True:
                pass

        claim_name = f'{_CLAIM_PREFIX}{uuid.uuid4().hex}'
        await connection.execute(text(
            f'ALTER INDEX IF EXISTS {_safe_index_name(index.name)} '
            f'RENAME TO {_safe_index_name(claim_name)}'
        ))
        claimed_owner = await _relation_owner(
            connection,
            index_name=claim_name,
            table_name=index.table,
        )
        if claimed_owner is not True:
            raise RuntimeError(
                f'{index.name} changed owner while the migration claimed it'
            )
        return claim_name


async def _autocommit(connection: 'AsyncConnection') -> 'AsyncConnection':
    return await connection.execution_options(isolation_level='AUTOCOMMIT')


async def _drop_index_concurrently(
    engine: 'AsyncEngine',
    index_name: str,
) -> None:
    async with engine.connect() as connection:
        connection = await _autocommit(connection)
        await connection.execute(text(
            f'DROP INDEX CONCURRENTLY {_safe_index_name(index_name)}'
        ))


async def _create_index_concurrently(
    engine: 'AsyncEngine',
    index: RecoveryIndex,
) -> None:
    async with engine.connect() as connection:
        connection = await _autocommit(connection)
        await connection.execute(text(index.create_sql))


async def _remove_abandoned_claims(
    engine: 'AsyncEngine',
    index: RecoveryIndex,
) -> None:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                _ABANDONED_CLAIMS_SQL,
                {'table_name': index.table},
            )
        ).all()
    for row in rows:
        claim_name = str(row[0])
        if not claim_name.startswith(_CLAIM_PREFIX):
            raise RuntimeError(f'unexpected recovery index claim: {claim_name}')
        await _drop_index_concurrently(engine, claim_name)


async def install_recovery_indexes(engine: 'AsyncEngine') -> None:
    """Install both exact indexes outside a transaction."""
    for index in RECOVERY_INDEXES:
        claim_name = await _claim_existing_index(engine, index)
        if claim_name is not None:
            await _drop_index_concurrently(engine, claim_name)
        await _create_index_concurrently(engine, index)
        await _remove_abandoned_claims(engine, index)


VALIDATE_RECOVERY_INDEXES_SQL = text("""
DO $migration$
DECLARE
    v_actual jsonb;
    v_expected jsonb;
BEGIN
    CREATE TEMP TABLE horsies_expected_workflow_recovery_index
        ON COMMIT DROP
        AS SELECT created_at, id, name, status
           FROM horsies_workflows WITH NO DATA;
    CREATE INDEX horsies_expected_workflow_recovery_index_idx
        ON horsies_expected_workflow_recovery_index (created_at, id)
        INCLUDE (name)
        WHERE status = 'RUNNING';

    SELECT jsonb_build_object(
               'method', am.amname, 'valid', i.indisvalid,
               'ready', i.indisready, 'live', i.indislive,
               'unique', i.indisunique, 'exclusion', i.indisexclusion,
               'immediate', i.indimmediate, 'key_count', i.indnkeyatts,
               'attribute_count', i.indnatts,
               'has_expressions', i.indexprs IS NOT NULL,
               'columns', (
                   SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE)
                                    ORDER BY n)
                   FROM generate_series(1, i.indnatts) AS n
               ),
               'operator_classes', to_jsonb(i.indclass::oid[]),
               'collations', to_jsonb(i.indcollation::oid[]),
               'options', to_jsonb(i.indoption::smallint[]),
               'predicate', pg_get_expr(i.indpred, i.indrelid)
           )
    INTO v_expected
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass(
        'horsies_expected_workflow_recovery_index_idx'
    );

    SELECT jsonb_build_object(
               'method', am.amname, 'valid', i.indisvalid,
               'ready', i.indisready, 'live', i.indislive,
               'unique', i.indisunique, 'exclusion', i.indisexclusion,
               'immediate', i.indimmediate, 'key_count', i.indnkeyatts,
               'attribute_count', i.indnatts,
               'has_expressions', i.indexprs IS NOT NULL,
               'columns', (
                   SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE)
                                    ORDER BY n)
                   FROM generate_series(1, i.indnatts) AS n
               ),
               'operator_classes', to_jsonb(i.indclass::oid[]),
               'collations', to_jsonb(i.indcollation::oid[]),
               'options', to_jsonb(i.indoption::smallint[]),
               'predicate', pg_get_expr(i.indpred, i.indrelid)
           )
    INTO v_actual
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass(
        'idx_horsies_workflows_running_recovery_scan'
    )
      AND i.indrelid = 'horsies_workflows'::regclass;

    IF v_actual IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            'idx_horsies_workflows_running_recovery_scan is absent, invalid, or noncanonical'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    CREATE TEMP TABLE horsies_expected_task_recovery_index
        ON COMMIT DROP
        AS SELECT created_at, id, is_workflow_task, status
           FROM horsies_tasks WITH NO DATA;
    CREATE INDEX horsies_expected_task_recovery_index_idx
        ON horsies_expected_task_recovery_index (created_at, id)
        WHERE is_workflow_task = TRUE
          AND status IN ('CLAIMED', 'PENDING');

    SELECT jsonb_build_object(
               'method', am.amname, 'valid', i.indisvalid,
               'ready', i.indisready, 'live', i.indislive,
               'unique', i.indisunique, 'exclusion', i.indisexclusion,
               'immediate', i.indimmediate, 'key_count', i.indnkeyatts,
               'attribute_count', i.indnatts,
               'has_expressions', i.indexprs IS NOT NULL,
               'columns', (
                   SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE)
                                    ORDER BY n)
                   FROM generate_series(1, i.indnatts) AS n
               ),
               'operator_classes', to_jsonb(i.indclass::oid[]),
               'collations', to_jsonb(i.indcollation::oid[]),
               'options', to_jsonb(i.indoption::smallint[]),
               'predicate', pg_get_expr(i.indpred, i.indrelid)
           )
    INTO v_expected
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass('horsies_expected_task_recovery_index_idx');

    SELECT jsonb_build_object(
               'method', am.amname, 'valid', i.indisvalid,
               'ready', i.indisready, 'live', i.indislive,
               'unique', i.indisunique, 'exclusion', i.indisexclusion,
               'immediate', i.indimmediate, 'key_count', i.indnkeyatts,
               'attribute_count', i.indnatts,
               'has_expressions', i.indexprs IS NOT NULL,
               'columns', (
                   SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE)
                                    ORDER BY n)
                   FROM generate_series(1, i.indnatts) AS n
               ),
               'operator_classes', to_jsonb(i.indclass::oid[]),
               'collations', to_jsonb(i.indcollation::oid[]),
               'options', to_jsonb(i.indoption::smallint[]),
               'predicate', pg_get_expr(i.indpred, i.indrelid)
           )
    INTO v_actual
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass('idx_horsies_tasks_orphan_recovery_scan')
      AND i.indrelid = 'horsies_tasks'::regclass;

    IF v_actual IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            'idx_horsies_tasks_orphan_recovery_scan is absent, invalid, or noncanonical'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$migration$
""")
