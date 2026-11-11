"""Durable state and exact indexes for bounded recovery scans.

The module also builds the exact claim indexes and removes retired indexes
with the same concurrent machinery.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeAlias, assert_never

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


# The count of the workflow recovery limit: one row for each workflow whose
# recovery raised a persistent error. No foreign key: a varchar-born database
# creates this table before its offline cutover, and `horsies_workflows.id` is
# not uuid there. The retention pass deletes a row after its workflow is gone.
CREATE_WORKFLOW_RECOVERY_FAILURES_SQL = text("""
CREATE TABLE IF NOT EXISTS horsies_workflow_recovery_failures (
    workflow_id uuid PRIMARY KEY,
    failed_passes integer NOT NULL CHECK (failed_passes >= 1),
    last_pass_id uuid NOT NULL,
    first_failed_at timestamptz NOT NULL,
    last_failed_at timestamptz NOT NULL,
    last_case varchar(64) NOT NULL,
    last_task_index integer,
    last_exception_type varchar(255) NOT NULL
)
""")


@dataclass(frozen=True)
class RecoveryIndex:
    """One fixed recovery index and its expected owner.

    ``definition`` is the text of the index after ``ON <table>``: its key,
    its INCLUDE columns and its predicate.
    """

    name: str
    table: str
    definition: str

    @property
    def create_sql(self) -> str:
        """The statement that builds the index under its name, concurrently."""
        return self.build_sql(self.name)

    def build_sql(self, index_name: str) -> str:
        """The statement that builds this index under ``index_name``,
        concurrently."""
        return (
            f'CREATE INDEX CONCURRENTLY {_safe_index_name(index_name)} '
            f'ON {self.table} {self.definition}'
        )


RECOVERY_INDEXES = (
    RecoveryIndex(
        name='idx_horsies_workflows_running_recovery_scan',
        table='horsies_workflows',
        definition="(created_at, id) INCLUDE (name) WHERE status = 'RUNNING'",
    ),
    RecoveryIndex(
        name='idx_horsies_tasks_orphan_recovery_scan',
        table='horsies_tasks',
        definition=(
            '(created_at, id) '
            'WHERE is_workflow_task = TRUE '
            "AND status IN ('CLAIMED', 'PENDING')"
        ),
    ),
    RecoveryIndex(
        name='idx_horsies_workflow_tasks_nonterminal',
        table='horsies_workflow_tasks',
        definition=(
            '(workflow_id) '
            "WHERE status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')"
        ),
    ),
    # Retention, coverage and publication scans read only catalog records
    # whose leaf is not dropped; dropped records stay in the catalog and
    # grow by one per class per day.
    RecoveryIndex(
        name='idx_horsies_history_catalog_active',
        table='horsies_task_history_leaf_catalog',
        definition='(class_key, upper_anchor) WHERE dropped_at IS NULL',
    ),
)

_INDEX_NAME = re.compile(r'^[a-z][a-z0-9_]*$')
# An index on its way out: renamed from its name, then dropped concurrently.
_CLAIM_PREFIX = 'horsies_recovery_index_claim_'
# An index under construction: built concurrently, then renamed to its name.
_BUILD_PREFIX = 'horsies_recovery_index_build_'
# The declared form of an index, built on a temporary copy of its table's
# columns inside the transaction that compares it, and dropped at commit.
_EXPECTED_TABLE = 'horsies_expected_index'
_EXPECTED_INDEX = 'horsies_expected_index_idx'
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
  AND (relation.relname LIKE 'horsies_recovery_index_claim\\_%' ESCAPE '\\'
       OR relation.relname LIKE 'horsies_recovery_index_build\\_%' ESCAPE '\\')
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


@dataclass(frozen=True, slots=True)
class IndexAbsent:
    """No relation has the name of the index."""


@dataclass(frozen=True, slots=True)
class IndexMatches:
    """The table owns an index of the name, valid and of the declared form."""


@dataclass(frozen=True, slots=True)
class IndexDiffers:
    """The table owns an index of the name of another form, or an invalid one."""


IndexState: TypeAlias = IndexAbsent | IndexMatches | IndexDiffers


async def _inspect_index(
    engine: 'AsyncEngine',
    index: RecoveryIndex,
) -> IndexState:
    """Whether the table owns an index of the name, and whether it has the
    declared form (``_INDEX_FORMS_MATCH_SQL``).

    Changes nothing that outlives the transaction: the declared form is built
    on a temporary table that drops at commit. Raises ``RuntimeError`` when a
    relation other than ``index.table`` owns the name. Database errors
    propagate.
    """
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
                return IndexAbsent()
            case False:
                raise RuntimeError(
                    f'{index.name} belongs to a relation other than {index.table}'
                )
            case True:
                pass
        await connection.execute(
            text(
                f'CREATE TEMP TABLE {_EXPECTED_TABLE} (LIKE {index.table}) '
                'ON COMMIT DROP'
            )
        )
        await connection.execute(
            text(
                f'CREATE INDEX {_EXPECTED_INDEX} ON {_EXPECTED_TABLE} '
                f'{index.definition}'
            )
        )
        matches = (
            await connection.execute(
                _INDEX_FORMS_MATCH_SQL,
                {
                    'expected_index': _EXPECTED_INDEX,
                    'index_name': index.name,
                    'table_name': index.table,
                },
            )
        ).scalar_one()
        return IndexMatches() if bool(matches) else IndexDiffers()


async def _swap_in_built_index(
    engine: 'AsyncEngine',
    index: RecoveryIndex,
    build_name: str,
) -> str:
    """Rename the index of the name to a claim name and the built index to the
    name, in one transaction. Returns the claim name, which the caller drops.

    Readers of the table see the old index or the built one under the name,
    never neither. Raises ``RuntimeError`` when the table no longer owns an
    index of the name; nothing is renamed then, and the built index keeps its
    build name for ``_remove_abandoned_claims``. Database errors propagate.
    """
    async with engine.begin() as connection:
        owner = await _relation_owner(
            connection,
            index_name=index.name,
            table_name=index.table,
        )
        match owner:
            case True:
                pass
            case None | False:
                raise RuntimeError(
                    f'{index.name} is no longer an index of {index.table} '
                    'while the migration replaced it'
                )
        claim_name = f'{_CLAIM_PREFIX}{uuid.uuid4().hex}'
        await connection.execute(
            text(
                f'ALTER INDEX {_safe_index_name(index.name)} '
                f'RENAME TO {_safe_index_name(claim_name)}'
            )
        )
        claimed_owner = await _relation_owner(
            connection,
            index_name=claim_name,
            table_name=index.table,
        )
        if claimed_owner is not True:
            raise RuntimeError(
                f'{index.name} changed owner while the migration claimed it'
            )
        await connection.execute(
            text(
                f'ALTER INDEX {_safe_index_name(build_name)} '
                f'RENAME TO {_safe_index_name(index.name)}'
            )
        )
        return claim_name


async def _replace_index(
    engine: 'AsyncEngine',
    index: RecoveryIndex,
) -> None:
    """Build the declared index under a build name, swap it in under the
    name (``_swap_in_built_index``), and drop the old one concurrently.

    The table keeps an index of the name throughout. A failure leaves the
    old index under the name, or under a claim name next to the new one;
    ``_remove_abandoned_claims`` drops the leftover on a later install.
    Database errors propagate.
    """
    build_name = f'{_BUILD_PREFIX}{uuid.uuid4().hex}'
    await _create_index_concurrently(engine, index, build_name)
    claim_name = await _swap_in_built_index(engine, index, build_name)
    await _drop_index_concurrently(engine, claim_name)


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
    index_name: str,
) -> None:
    async with engine.connect() as connection:
        connection = await _autocommit(connection)
        await connection.execute(text(index.build_sql(index_name)))


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
        if not claim_name.startswith((_CLAIM_PREFIX, _BUILD_PREFIX)):
            raise RuntimeError(f'unexpected recovery index claim: {claim_name}')
        await _drop_index_concurrently(engine, claim_name)


async def _install_exact_indexes(
    engine: 'AsyncEngine',
    indexes: tuple[RecoveryIndex, ...],
) -> None:
    """Give each index its declared form under its name, outside a
    transaction.

    An index of the declared form is kept. An absent one is built
    concurrently under its name. One of another form, or an invalid one, is
    replaced (``_replace_index``), so readers keep an index of the name. Then
    each claim or build name left on the table by an interrupted install is
    dropped. Raises ``RuntimeError`` when another relation owns a name.
    Database errors propagate.
    """
    for index in indexes:
        match await _inspect_index(engine, index):
            case IndexMatches():
                pass
            case IndexAbsent():
                await _create_index_concurrently(engine, index, index.name)
            case IndexDiffers():
                await _replace_index(engine, index)
            case _ as unreachable:
                assert_never(unreachable)
        await _remove_abandoned_claims(engine, index)


async def install_recovery_indexes(engine: 'AsyncEngine') -> None:
    """Install the exact recovery indexes outside a transaction
    (``_install_exact_indexes``)."""
    await _install_exact_indexes(engine, RECOVERY_INDEXES)


# The class-keyed claim indexes. A claim stream asks for one queue and one
# execution class in (priority, enqueued_at, id) order, so the class is the
# second key: each stream is one ordered range, also when the class is rare
# or absent in the queue.
# - v54: the pending arm, over PENDING rows.
# - v55: the expired arm, over CLAIMED rows. It replaces
#   idx_horsies_tasks_claim_expired_ordered, whose key had no class: with
#   50,000 expired leases in one queue, a stream of an absent class read
#   7,848 buffers, against 142 on this index.
_CLAIM_INDEX_KEY: Final = '(queue_name, execution, priority, enqueued_at, id)'


@dataclass(frozen=True, slots=True)
class ClaimIndex:
    """One class-keyed claim index over horsies_tasks rows of one status."""

    name: str
    status: str

    def recovery_index(self) -> RecoveryIndex:
        return RecoveryIndex(
            name=self.name,
            table='horsies_tasks',
            definition=f"{_CLAIM_INDEX_KEY} WHERE status = '{self.status}'",
        )


CLAIM_INDEX_SPECS: Final[tuple[ClaimIndex, ...]] = (
    ClaimIndex(name='idx_horsies_tasks_claim_pending_class', status='PENDING'),
    ClaimIndex(name='idx_horsies_tasks_claim_expired_class', status='CLAIMED'),
)

CLAIM_INDEXES: Final[tuple[RecoveryIndex, ...]] = tuple(
    spec.recovery_index() for spec in CLAIM_INDEX_SPECS
)


async def install_claim_indexes(engine: 'AsyncEngine') -> None:
    """Install the exact claim indexes outside a transaction.

    As for the recovery indexes (``_install_exact_indexes``): an index of the
    declared form is kept, and one of another form is replaced while an index
    of its name stays on horsies_tasks, so running claims keep their index
    across a schema upgrade. Database errors propagate.
    """
    await _install_exact_indexes(engine, CLAIM_INDEXES)


_INDEX_SHAPE_SQL: Final = """jsonb_build_object(
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
           )"""


# Whether the index of the name on the table has the form of the expected
# index; false when the table owns no index of the name.
_INDEX_FORMS_MATCH_SQL = text(f"""
SELECT (
    SELECT {_INDEX_SHAPE_SQL}
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass(:expected_index)
) IS NOT DISTINCT FROM (
    SELECT {_INDEX_SHAPE_SQL}
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass(:index_name)
      AND i.indrelid = to_regclass(:table_name)
)
""")


def _claim_index_check(spec: ClaimIndex, position: int) -> str:
    """The catalog form of one claim index against the same index on a
    temporary table; any difference raises."""
    expected_table = f'horsies_expected_claim_index_{position}'
    expected_index = f'{expected_table}_idx'
    return f"""
    CREATE TEMP TABLE {expected_table}
        ON COMMIT DROP
        AS SELECT queue_name, execution, priority, enqueued_at, id, status
           FROM horsies_tasks WITH NO DATA;
    CREATE INDEX {expected_index}
        ON {expected_table} {_CLAIM_INDEX_KEY}
        WHERE status = '{spec.status}';

    SELECT {_INDEX_SHAPE_SQL}
    INTO v_expected
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass('{expected_index}');

    SELECT {_INDEX_SHAPE_SQL}
    INTO v_actual
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass('{spec.name}')
      AND i.indrelid = 'horsies_tasks'::regclass;

    IF v_actual IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            '{spec.name} is absent, invalid, or noncanonical'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
"""


# The catalog form of each claim index against the same index built on a
# temporary table. Any difference (columns, order, predicate, validity)
# fails the migration.
VALIDATE_CLAIM_INDEXES_SQL = text(
    'DO $migration$\n'
    'DECLARE\n'
    '    v_actual jsonb;\n'
    '    v_expected jsonb;\n'
    'BEGIN'
    + ''.join(
        _claim_index_check(spec, position)
        for position, spec in enumerate(CLAIM_INDEX_SPECS)
    )
    + 'END\n'
    '$migration$\n'
)


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
    CREATE TEMP TABLE horsies_expected_nonterminal_node_index
        ON COMMIT DROP
        AS SELECT workflow_id, status
           FROM horsies_workflow_tasks WITH NO DATA;
    CREATE INDEX horsies_expected_nonterminal_node_index_idx
        ON horsies_expected_nonterminal_node_index (workflow_id)
        WHERE status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING');

    SELECT jsonb_build_object(
               'method', am.amname,
               'valid', i.indisvalid,
               'ready', i.indisready,
               'live', i.indislive,
               'unique', i.indisunique,
               'exclusion', i.indisexclusion,
               'immediate', i.indimmediate,
               'key_count', i.indnkeyatts,
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
    WHERE ic.oid = to_regclass('horsies_expected_nonterminal_node_index_idx');

    SELECT jsonb_build_object(
               'method', am.amname,
               'valid', i.indisvalid,
               'ready', i.indisready,
               'live', i.indislive,
               'unique', i.indisunique,
               'exclusion', i.indisexclusion,
               'immediate', i.indimmediate,
               'key_count', i.indnkeyatts,
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
    WHERE ic.oid = to_regclass('idx_horsies_workflow_tasks_nonterminal')
      AND i.indrelid = 'horsies_workflow_tasks'::regclass;

    IF v_actual IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            'idx_horsies_workflow_tasks_nonterminal is absent, invalid, or noncanonical'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

END
$migration$
""")


@dataclass(frozen=True, slots=True)
class RetiredIndex:
    """One index to remove, and the table that must own it."""

    name: str
    table: str


RETIRED_INDEXES: Final[tuple[RetiredIndex, ...]] = (
    # v55: the claim reads one (queue, execution class) stream at a time on
    # the class-keyed claim indexes; these two have no class in their key.
    RetiredIndex(name='idx_horsies_tasks_claim_pending', table='horsies_tasks'),
    RetiredIndex(
        name='idx_horsies_tasks_claim_expired_ordered',
        table='horsies_tasks',
    ),
    # Terminal tasks move to history, so no retention delete reads the live
    # table by terminal age.
    RetiredIndex(name='idx_horsies_tasks_retention', table='horsies_tasks'),
    RetiredIndex(
        name='idx_horsies_tasks_queue_retention',
        table='horsies_tasks',
    ),
    # Every reader that filters nodes by node_id or dependencies also
    # filters workflow_id, which uq_horsies_workflow_task_index and the
    # workflow/status indexes serve; workflow_id alone is their prefix.
    RetiredIndex(
        name='ix_horsies_workflow_tasks_node_id',
        table='horsies_workflow_tasks',
    ),
    RetiredIndex(
        name='ix_horsies_workflow_tasks_workflow_id',
        table='horsies_workflow_tasks',
    ),
    RetiredIndex(
        name='idx_horsies_workflow_tasks_deps',
        table='horsies_workflow_tasks',
    ),
)


async def _claim_retired_index(
    engine: 'AsyncEngine',
    index: RetiredIndex,
) -> str | None:
    """Rename the index to a claim name when its table owns it.

    Returns the claim name, or None when no index of that name exists or
    a relation other than `index.table` owns the name. The rename and the
    owner checks share one transaction, so the claimed relation is the
    checked one. An interrupted removal leaves a recovery claim name on
    the table, which `install_recovery_indexes` removes.
    """
    async with engine.begin() as connection:
        owner = await _relation_owner(
            connection,
            index_name=index.name,
            table_name=index.table,
        )
        match owner:
            case None | False:
                return None
            case True:
                pass
        claim_name = f'{_CLAIM_PREFIX}{uuid.uuid4().hex}'
        await connection.execute(text(
            f'ALTER INDEX {_safe_index_name(index.name)} '
            f'RENAME TO {_safe_index_name(claim_name)}'
        ))
        claimed_owner = await _relation_owner(
            connection,
            index_name=claim_name,
            table_name=index.table,
        )
        if claimed_owner is not True:
            raise RuntimeError(
                f'{index.name} changed owner while it was claimed for removal'
            )
        return claim_name


async def remove_retired_indexes(engine: 'AsyncEngine') -> None:
    """Drop each retired index concurrently, outside a transaction.

    An index that is absent, or whose name belongs to another relation,
    is left alone. Database errors propagate; a removal that fails after
    its claim leaves the claim name for the recovery-index cleanup.
    """
    for index in RETIRED_INDEXES:
        claim_name = await _claim_retired_index(engine, index)
        if claim_name is not None:
            await _drop_index_concurrently(engine, claim_name)
