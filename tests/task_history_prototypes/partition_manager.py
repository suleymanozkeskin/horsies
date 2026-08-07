"""Partition lifecycle operations for disposable task-history schemas."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tests.task_history_prototypes.schema import PrototypeSchema


_IDENTIFIER = re.compile(r'^[a-z][a-z0-9_]{0,62}$')
_FINITE_PARENT = 'history_aggregate_finite'


class LeafState(StrEnum):
    READY = 'READY'
    NOT_EXPIRED = 'NOT_EXPIRED'
    PENDING_BLOCKED = 'PENDING_BLOCKED'
    DETACH_PENDING = 'DETACH_PENDING'
    DETACHED = 'DETACHED'
    MISSING = 'MISSING'
    CLASS_ABSENT = 'CLASS_ABSENT'
    FOREVER_CLASS = 'FOREVER_CLASS'
    CATALOG_CONFLICT = 'CATALOG_CONFLICT'
    DROPPED = 'DROPPED'


@dataclass(frozen=True, slots=True)
class LeafInspection:
    state: LeafState
    blocker_count: int
    expires_at: datetime | None
    relation_exists: bool
    attached: bool


@dataclass(frozen=True, slots=True)
class PartitionPrivileges:
    schema_create: bool
    owns_parent: bool

    @property
    def can_manage(self) -> bool:
        return self.schema_create and self.owns_parent


async def install_partition_manager_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    statements = (
        f"""
        CREATE TABLE {schema.sql}.history_leaf_catalog (
            leaf_name text PRIMARY KEY,
            parent_name text NOT NULL,
            class_key text NOT NULL
                REFERENCES {schema.sql}.retention_classes(class_key),
            lower_anchor timestamptz NOT NULL,
            upper_anchor timestamptz NOT NULL,
            index_schema_version smallint NOT NULL,
            id_index_name text NOT NULL,
            partition_bound text NOT NULL,
            created_at timestamptz NOT NULL,
            detached_at timestamptz,
            dropped_at timestamptz,
            CHECK (lower_anchor < upper_anchor),
            CHECK (dropped_at IS NULL OR detached_at IS NOT NULL),
            UNIQUE (class_key, lower_anchor, upper_anchor)
        )
        """,
        f"""
        INSERT INTO {schema.sql}.history_leaf_catalog (
            leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
            index_schema_version, id_index_name, partition_bound, created_at
        ) VALUES
            ('history_aggregate_finite_2026_06_01',
             'history_aggregate_finite', 'finite_30d_v1',
             '2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z', 1,
             'history_aggregate_finite_2026_06_01_id_idx',
             (SELECT pg_get_expr(c.relpartbound, c.oid)
              FROM pg_class AS c
              WHERE c.oid =
                '{schema.name}.history_aggregate_finite_2026_06_01'::regclass),
             statement_timestamp()),
            ('history_aggregate_finite_2026_08_05',
             'history_aggregate_finite', 'finite_30d_v1',
             '2026-08-05T00:00:00Z', '2026-08-06T00:00:00Z', 1,
             'history_aggregate_finite_2026_08_05_id_idx',
             (SELECT pg_get_expr(c.relpartbound, c.oid)
              FROM pg_class AS c
              WHERE c.oid =
                '{schema.name}.history_aggregate_finite_2026_08_05'::regclass),
             statement_timestamp())
        """,
        f"""
            CREATE FUNCTION {schema.sql}.history_leaf_lock_key(
                p_class_key text,
                p_anchor timestamptz
            ) RETURNS bigint
            LANGUAGE sql
            STABLE
            STRICT
            AS $function$
                SELECT hashtextextended(
                    p_class_key || chr(31) ||
                    extract(epoch FROM date_trunc('day', p_anchor, 'UTC'))
                        ::bigint::text,
                    1601
                )
            $function$
        """,
    )
    for statement in statements:
        await connection.execute(text(statement))


async def inspect_history_leaf(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    leaf_name: str,
    class_key: str,
    lower: datetime,
    upper: datetime,
) -> LeafInspection:
    _validate_leaf_inputs(leaf_name, class_key, lower, upper)
    duration = (
        await connection.execute(
            text(
                f"""
                SELECT duration
                FROM {schema.sql}.retention_classes
                WHERE class_key = :class_key
                """
            ),
            {'class_key': class_key},
        )
    ).scalar_one_or_none()
    class_exists = (
        await connection.execute(
            text(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM {schema.sql}.retention_classes
                    WHERE class_key = :class_key
                )
                """
            ),
            {'class_key': class_key},
        )
    ).scalar_one()
    if not class_exists:
        return LeafInspection(LeafState.CLASS_ABSENT, 0, None, False, False)
    if duration is None:
        return LeafInspection(LeafState.FOREVER_CLASS, 0, None, False, False)
    if not isinstance(duration, timedelta):
        raise TypeError('finite retention duration must decode as timedelta')

    catalog = (
        await connection.execute(
            text(
                f"""
                SELECT parent_name, class_key, lower_anchor, upper_anchor,
                       id_index_name, partition_bound, detached_at, dropped_at
                FROM {schema.sql}.history_leaf_catalog
                WHERE leaf_name = :leaf_name
                """
            ),
            {'leaf_name': leaf_name},
        )
    ).one_or_none()
    qualified_leaf = _qualified_relation(schema, leaf_name)
    qualified_parent = _qualified_relation(schema, _FINITE_PARENT)
    qualified_index = (
        _qualified_relation(schema, catalog.id_index_name)
        if catalog is not None
        else _qualified_relation(schema, f'{leaf_name}_task_idx')
    )
    relation = (
        await connection.execute(
            text(
                """
                SELECT to_regclass(:leaf)::oid AS leaf_oid,
                       to_regclass(:parent)::oid AS parent_oid,
                       to_regclass(:id_index) IS NOT NULL AS id_index_exists,
                       (SELECT pg_get_expr(c.relpartbound, c.oid)
                        FROM pg_class AS c
                        WHERE c.oid = to_regclass(:leaf)) AS partition_bound
                """
            ),
            {
                'leaf': qualified_leaf,
                'parent': qualified_parent,
                'id_index': qualified_index,
            },
        )
    ).one()
    if catalog is None:
        state = (
            LeafState.MISSING
            if relation.leaf_oid is None
            else LeafState.CATALOG_CONFLICT
        )
        return LeafInspection(
            state,
            0,
            upper + duration,
            relation.leaf_oid is not None,
            False,
        )
    if (
        catalog.parent_name != _FINITE_PARENT
        or catalog.class_key != class_key
        or catalog.lower_anchor != lower
        or catalog.upper_anchor != upper
    ):
        return LeafInspection(
            LeafState.CATALOG_CONFLICT,
            0,
            catalog.upper_anchor + duration,
            relation.leaf_oid is not None,
            False,
        )
    if relation.leaf_oid is None and catalog.dropped_at is not None:
        return LeafInspection(LeafState.DROPPED, 0, upper + duration, False, False)
    if relation.leaf_oid is None:
        return LeafInspection(LeafState.MISSING, 0, upper + duration, False, False)
    if relation.parent_oid is None:
        raise RuntimeError('finite history parent is absent')

    detach_pending = (
        await connection.execute(
            text(
                """
                SELECT inhdetachpending
                FROM pg_inherits
                WHERE inhparent = :parent_oid
                  AND inhrelid = :leaf_oid
                """
            ),
            {'parent_oid': relation.parent_oid, 'leaf_oid': relation.leaf_oid},
        )
    ).scalar_one_or_none()
    expires_at = upper + duration
    if detach_pending is not None and (
        relation.partition_bound != catalog.partition_bound
        or not relation.id_index_exists
    ):
        return LeafInspection(
            LeafState.CATALOG_CONFLICT,
            0,
            expires_at,
            True,
            True,
        )
    blocker_count = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.workflow_phase2_pending
                WHERE recovery_source = 'HISTORY'
                  AND history_class = :class_key
                  AND history_anchor >= :lower
                  AND history_anchor < :upper
                """
            ),
            {'class_key': class_key, 'lower': lower, 'upper': upper},
        )
    ).scalar_one()
    if blocker_count:
        return LeafInspection(
            LeafState.PENDING_BLOCKED,
            blocker_count,
            expires_at,
            True,
            detach_pending is not None,
        )
    if detach_pending is None:
        return LeafInspection(LeafState.DETACHED, 0, expires_at, True, False)
    if detach_pending:
        return LeafInspection(LeafState.DETACH_PENDING, 0, expires_at, True, True)

    database_now = (
        await connection.execute(text('SELECT statement_timestamp()'))
    ).scalar_one()
    state = LeafState.READY if expires_at <= database_now else LeafState.NOT_EXPIRED
    return LeafInspection(state, blocker_count, expires_at, True, True)


async def detach_history_leaf_concurrently(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    leaf_name: str,
    class_key: str,
    lower: datetime,
    upper: datetime,
    statement_timeout_ms: int | None = None,
) -> LeafInspection:
    _validate_leaf_inputs(leaf_name, class_key, lower, upper)
    if statement_timeout_ms is not None and statement_timeout_ms <= 0:
        raise ValueError('statement timeout must be positive')
    admin_engine = engine.execution_options(isolation_level='AUTOCOMMIT')
    async with admin_engine.connect() as connection:
        prior_statement_timeout: str | None = None
        await _acquire_leaf_lock(connection, schema, class_key, lower)
        try:
            inspection = await inspect_history_leaf(
                connection,
                schema,
                leaf_name=leaf_name,
                class_key=class_key,
                lower=lower,
                upper=upper,
            )
            if inspection.state is not LeafState.READY:
                return inspection
            if statement_timeout_ms is not None:
                prior_statement_timeout = (
                    await connection.execute(text('SHOW statement_timeout'))
                ).scalar_one()
                await connection.execute(
                    text("SELECT set_config('statement_timeout', :value, false)"),
                    {'value': f'{statement_timeout_ms}ms'},
                )
            await connection.execute(
                text(
                    f"""
                    ALTER TABLE {schema.sql}.{_FINITE_PARENT}
                    DETACH PARTITION {schema.sql}.{leaf_name} CONCURRENTLY
                    """
                )
            )
            await connection.execute(
                text(
                    f"""
                    UPDATE {schema.sql}.history_leaf_catalog
                    SET detached_at = COALESCE(
                        detached_at, statement_timestamp()
                    )
                    WHERE leaf_name = :leaf_name
                    """
                ),
                {'leaf_name': leaf_name},
            )
            return await inspect_history_leaf(
                connection,
                schema,
                leaf_name=leaf_name,
                class_key=class_key,
                lower=lower,
                upper=upper,
            )
        finally:
            await connection.rollback()
            if prior_statement_timeout is not None:
                await connection.execute(
                    text("SELECT set_config('statement_timeout', :value, false)"),
                    {'value': prior_statement_timeout},
                )
            await _release_leaf_lock(connection, schema, class_key, lower)


async def finalize_interrupted_detach(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    leaf_name: str,
    class_key: str,
    lower: datetime,
    upper: datetime,
) -> LeafInspection:
    _validate_leaf_inputs(leaf_name, class_key, lower, upper)
    admin_engine = engine.execution_options(isolation_level='AUTOCOMMIT')
    async with admin_engine.connect() as connection:
        await _acquire_leaf_lock(connection, schema, class_key, lower)
        try:
            inspection = await inspect_history_leaf(
                connection,
                schema,
                leaf_name=leaf_name,
                class_key=class_key,
                lower=lower,
                upper=upper,
            )
            if inspection.state is LeafState.DETACHED:
                await connection.execute(
                    text(
                        f"""
                        UPDATE {schema.sql}.history_leaf_catalog
                        SET detached_at = COALESCE(
                            detached_at, statement_timestamp()
                        )
                        WHERE leaf_name = :leaf_name
                        """
                    ),
                    {'leaf_name': leaf_name},
                )
                return await inspect_history_leaf(
                    connection,
                    schema,
                    leaf_name=leaf_name,
                    class_key=class_key,
                    lower=lower,
                    upper=upper,
                )
            if inspection.state is not LeafState.DETACH_PENDING:
                return inspection
            await connection.execute(
                text(
                    f"""
                    ALTER TABLE {schema.sql}.{_FINITE_PARENT}
                    DETACH PARTITION {schema.sql}.{leaf_name} FINALIZE
                    """
                )
            )
            await connection.execute(
                text(
                    f"""
                    UPDATE {schema.sql}.history_leaf_catalog
                    SET detached_at = COALESCE(
                        detached_at, statement_timestamp()
                    )
                    WHERE leaf_name = :leaf_name
                    """
                ),
                {'leaf_name': leaf_name},
            )
            return await inspect_history_leaf(
                connection,
                schema,
                leaf_name=leaf_name,
                class_key=class_key,
                lower=lower,
                upper=upper,
            )
        finally:
            await connection.rollback()
            await _release_leaf_lock(connection, schema, class_key, lower)


async def create_daily_history_leaf(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    leaf_name: str,
    class_key: str,
    lower: datetime,
    upper: datetime,
) -> None:
    _validate_leaf_inputs(leaf_name, class_key, lower, upper)
    if upper - lower != timedelta(days=1):
        raise ValueError('daily leaf bounds must span exactly one day')
    class_row = (
        await connection.execute(
            text(
                f"""
                SELECT duration, partition_interval
                FROM {schema.sql}.retention_classes
                WHERE class_key = :class_key
                """
            ),
            {'class_key': class_key},
        )
    ).one_or_none()
    if class_row is None:
        raise ValueError('retention class is absent')
    if class_row.duration is None:
        raise ValueError('forever retention does not use finite leaves')
    if class_row.partition_interval != timedelta(days=1):
        raise ValueError('retention class does not use daily leaves')
    await lock_history_leaf_for_transaction(
        connection,
        schema,
        class_key=class_key,
        anchor=lower,
    )
    relation_exists = (
        await connection.execute(
            text('SELECT to_regclass(:leaf) IS NOT NULL'),
            {'leaf': _qualified_relation(schema, leaf_name)},
        )
    ).scalar_one()
    catalog = (
        await connection.execute(
            text(
                f"""
                SELECT parent_name, class_key, lower_anchor, upper_anchor,
                       id_index_name, partition_bound, detached_at, dropped_at
                FROM {schema.sql}.history_leaf_catalog
                WHERE leaf_name = :leaf_name
                """
            ),
            {'leaf_name': leaf_name},
        )
    ).one_or_none()
    if relation_exists or catalog is not None:
        if not relation_exists or catalog is None:
            raise RuntimeError('leaf relation and catalog entry disagree')
        if (
            catalog.parent_name != _FINITE_PARENT
            or catalog.class_key != class_key
            or catalog.lower_anchor != lower
            or catalog.upper_anchor != upper
        ):
            raise RuntimeError('existing leaf metadata differs from request')
        if _IDENTIFIER.fullmatch(catalog.id_index_name) is None:
            raise RuntimeError('cataloged index name is unsafe')
        conformance = (
            await connection.execute(
                text(
                    """
                    SELECT
                        pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
                        to_regclass(:id_index) IS NOT NULL AS id_index_exists
                    FROM pg_class AS c
                    WHERE c.oid = to_regclass(:leaf)
                    """
                ),
                {
                    'leaf': _qualified_relation(schema, leaf_name),
                    'id_index': _qualified_relation(schema, catalog.id_index_name),
                },
            )
        ).one()
        if conformance.partition_bound != catalog.partition_bound:
            raise RuntimeError('attached leaf partition bound differs from catalog')
        if not conformance.id_index_exists:
            await connection.execute(
                text(
                    f"""
                    CREATE INDEX {catalog.id_index_name}
                        ON {schema.sql}.{leaf_name} (task_id)
                    """
                )
            )
            await connection.execute(text(f'ANALYZE {schema.sql}.{leaf_name}'))
        return
    await connection.execute(
        text(
            f"""
            CREATE TABLE {schema.sql}.{leaf_name}
                PARTITION OF {schema.sql}.{_FINITE_PARENT}
                FOR VALUES FROM ('{lower.isoformat()}') TO ('{upper.isoformat()}')
            """
        )
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_leaf_catalog (
                leaf_name, parent_name, class_key,
                lower_anchor, upper_anchor,
                index_schema_version, id_index_name, partition_bound,
                created_at
            ) VALUES (
                :leaf_name, :parent_name, :class_key,
                :lower, :upper, 1, :id_index_name,
                (SELECT pg_get_expr(c.relpartbound, c.oid)
                 FROM pg_class AS c
                 WHERE c.oid = to_regclass(:qualified_leaf)),
                statement_timestamp()
            )
            """
        ),
        {
            'leaf_name': leaf_name,
            'parent_name': _FINITE_PARENT,
            'class_key': class_key,
            'lower': lower,
            'upper': upper,
            'id_index_name': f'{leaf_name}_task_idx',
            'qualified_leaf': _qualified_relation(schema, leaf_name),
        },
    )
    await connection.execute(
        text(
            f"""
            CREATE INDEX {leaf_name}_task_idx
                ON {schema.sql}.{leaf_name} (task_id)
            """
        )
    )
    await connection.execute(text(f'ANALYZE {schema.sql}.{leaf_name}'))


async def drop_detached_history_leaf(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    leaf_name: str,
    class_key: str,
    lower: datetime,
    upper: datetime,
) -> LeafInspection:
    _validate_leaf_inputs(leaf_name, class_key, lower, upper)
    await lock_history_leaf_for_transaction(
        connection,
        schema,
        class_key=class_key,
        anchor=lower,
    )
    inspection = await inspect_history_leaf(
        connection,
        schema,
        leaf_name=leaf_name,
        class_key=class_key,
        lower=lower,
        upper=upper,
    )
    if inspection.state is not LeafState.DETACHED:
        return inspection
    await connection.execute(text(f'DROP TABLE {schema.sql}.{leaf_name}'))
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_leaf_catalog
            SET detached_at = COALESCE(detached_at, statement_timestamp()),
                dropped_at = statement_timestamp()
            WHERE leaf_name = :leaf_name
            """
        ),
        {'leaf_name': leaf_name},
    )
    return await inspect_history_leaf(
        connection,
        schema,
        leaf_name=leaf_name,
        class_key=class_key,
        lower=lower,
        upper=upper,
    )


async def lock_history_leaf_for_transaction(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    class_key: str,
    anchor: datetime,
) -> None:
    if not class_key:
        raise ValueError('class key must be non-empty')
    if anchor.tzinfo is None:
        raise ValueError('history anchor must be timezone-aware')
    await connection.execute(
        text(
            f"""
            SELECT pg_advisory_xact_lock(
                {schema.sql}.history_leaf_lock_key(:class_key, :anchor)
            )
            """
        ),
        {'class_key': class_key, 'anchor': anchor},
    )


async def partition_privileges(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> PartitionPrivileges:
    report = (
        await connection.execute(
            text(
                """
                SELECT
                    has_schema_privilege(current_user, :schema_name, 'CREATE')
                        AS schema_create,
                    pg_has_role(
                        current_user,
                        (SELECT relowner FROM pg_class
                         WHERE oid = to_regclass(:parent)),
                        'USAGE'
                    ) AS owns_parent
                """
            ),
            {
                'schema_name': schema.name,
                'parent': _qualified_relation(schema, _FINITE_PARENT),
            },
        )
    ).one()
    return PartitionPrivileges(
        schema_create=report.schema_create,
        owns_parent=report.owns_parent,
    )


async def _acquire_leaf_lock(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    class_key: str,
    lower: datetime,
) -> None:
    await connection.execute(
        text(
            f"""
            SELECT pg_advisory_lock(
                {schema.sql}.history_leaf_lock_key(:class_key, :lower)
            )
            """
        ),
        {'class_key': class_key, 'lower': lower},
    )


async def _release_leaf_lock(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    class_key: str,
    lower: datetime,
) -> None:
    released = (
        await connection.execute(
            text(
                f"""
                SELECT pg_advisory_unlock(
                    {schema.sql}.history_leaf_lock_key(:class_key, :lower)
                )
                """
            ),
            {'class_key': class_key, 'lower': lower},
        )
    ).scalar_one()
    if not released:
        raise RuntimeError('partition maintenance did not own the leaf lock')


def _validate_leaf_inputs(
    leaf_name: str,
    class_key: str,
    lower: datetime,
    upper: datetime,
) -> None:
    if _IDENTIFIER.fullmatch(leaf_name) is None:
        raise ValueError('leaf name must be a safe PostgreSQL identifier')
    if not class_key:
        raise ValueError('class key must be non-empty')
    if lower.tzinfo is None or upper.tzinfo is None:
        raise ValueError('leaf bounds must be timezone-aware')
    if lower >= upper:
        raise ValueError('leaf bounds must be increasing')


def _qualified_relation(schema: PrototypeSchema, relation: str) -> str:
    if _IDENTIFIER.fullmatch(relation) is None:
        raise ValueError('relation name must be a safe PostgreSQL identifier')
    return f'{schema.sql}.{relation}'
