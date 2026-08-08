"""Correctness gates for disposable task-history partition maintenance."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes.archive import (
    ARCHIVE_CODEC,
    ARCHIVE_VERSION,
    encode_attempts,
    encode_json_value,
    prototype_attempts,
)
from tests.task_history_prototypes.partition_manager import (
    LeafState,
    create_daily_history_leaf,
    detach_history_leaf_concurrently,
    drop_detached_history_leaf,
    finalize_interrupted_detach,
    inspect_history_leaf,
    install_partition_manager_prototype,
    lock_history_leaf_for_transaction,
    partition_privileges,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.workflow_schema import (
    install_workflow_recovery_prototype,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_CLASS_KEY = 'finite_30d_v1'
_OLD_LEAF = 'history_aggregate_finite_2026_06_01'
_OLD_LOWER = datetime(2026, 6, 1, tzinfo=timezone.utc)
_OLD_UPPER = datetime(2026, 6, 2, tzinfo=timezone.utc)
_CURRENT_LEAF = 'history_aggregate_finite_2026_08_05'
_CURRENT_LOWER = datetime(2026, 8, 5, tzinfo=timezone.utc)
_CURRENT_UPPER = datetime(2026, 8, 6, tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def partition_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[AsyncConnection]:
    schema = PrototypeSchema(f'history_partitions_{uuid4().hex[:12]}')
    connection = await engine.connect()
    try:
        await install_archive_candidates(connection, schema)
        await install_workflow_recovery_prototype(connection, schema)
        await install_partition_manager_prototype(connection, schema)
        await connection.commit()
        connection.info['task_history_schema'] = schema
        yield connection
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()
        await connection.close()


def _schema(connection: AsyncConnection) -> PrototypeSchema:
    schema = connection.info.get('task_history_schema')
    assert isinstance(schema, PrototypeSchema)
    return schema


async def _seed_pending_locator(connection: AsyncConnection) -> str:
    schema = _schema(connection)
    task_id = str(uuid4())
    workflow_id = str(uuid4())
    result = encode_json_value({'ok': {'value': 42}})
    attempts = encode_attempts(prototype_attempts(1))
    terminal_at = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority,
                command_fingerprint_version, command_fingerprint, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_content_type,
                result_payload, result_digest, retry_count, max_retries,
                workflow_id, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot_content_type,
                attempt_snapshot,
                attempt_snapshot_digest
            ) VALUES (
                :task_id, 'prototype.workflow_task', 'default', 100,
                1, decode(repeat('ab', 32), 'hex'),
                'COMPLETED', 'COMPLETE_LOCKED', :terminal_at, :terminal_at,
                :class_key, :terminal_at, :terminal_at,
                :version, :codec, 'application/json',
                :result_payload, :result_digest,
                0, 0, :workflow_id, TRUE,
                :version, :version, :codec, 'application/json',
                :attempts, :attempts_digest
            )
            """
        ),
        {
            'task_id': task_id,
            'terminal_at': terminal_at,
            'class_key': _CLASS_KEY,
            'version': ARCHIVE_VERSION,
            'codec': ARCHIVE_CODEC,
            'result_payload': result.payload,
            'result_digest': result.digest,
            'workflow_id': workflow_id,
            'attempts': attempts.payload,
            'attempts_digest': attempts.digest,
        },
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.phase2_workflows (workflow_id, status)
            VALUES (:workflow_id, 'RUNNING')
            """
        ),
        {'workflow_id': workflow_id},
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.phase2_nodes (
                workflow_id, node_id, task_id, status,
                requires_parent_propagation
            ) VALUES (
                :workflow_id, 'node-1', :task_id, 'RUNNING', FALSE
            )
            """
        ),
        {'workflow_id': workflow_id, 'task_id': task_id},
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.workflow_phase2_pending (
                task_id, workflow_id, workflow_node_row_id, terminal_status,
                terminal_at, terminalization_kind, recovery_source,
                history_class, history_anchor, history_schema_version,
                result_digest, phase2_generation, created_at
            ) VALUES (
                CAST(:task_id AS uuid),
                :workflow_id,
                (SELECT id FROM {schema.sql}.phase2_nodes
                 WHERE task_id = CAST(:task_id AS uuid)),
                'COMPLETED', :terminal_at,
                'COMPLETE_LOCKED', 'HISTORY', :class_key, :terminal_at,
                :version, :result_digest, :generation,
                statement_timestamp() - interval '90 days'
            )
            """
        ),
        {
            'task_id': task_id,
            'workflow_id': workflow_id,
            'terminal_at': terminal_at,
            'class_key': _CLASS_KEY,
            'version': ARCHIVE_VERSION,
            'result_digest': result.digest,
            'generation': str(uuid4()),
        },
    )
    await connection.commit()
    return task_id


async def test_expired_leaf_is_ready_without_pending_locator(
    partition_schema: AsyncConnection,
) -> None:
    inspection = await inspect_history_leaf(
        partition_schema,
        _schema(partition_schema),
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert inspection.state is LeafState.READY
    assert inspection.blocker_count == 0
    assert inspection.attached is True


async def test_unexpired_leaf_is_not_detachable(
    partition_schema: AsyncConnection,
) -> None:
    inspection = await inspect_history_leaf(
        partition_schema,
        _schema(partition_schema),
        leaf_name=_CURRENT_LEAF,
        class_key=_CLASS_KEY,
        lower=_CURRENT_LOWER,
        upper=_CURRENT_UPPER,
    )
    assert inspection.state is LeafState.NOT_EXPIRED


async def test_catalog_rejects_caller_supplied_bounds_that_do_not_match_leaf(
    partition_schema: AsyncConnection,
) -> None:
    inspection = await inspect_history_leaf(
        partition_schema,
        _schema(partition_schema),
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=datetime(2026, 6, 3, tzinfo=timezone.utc),
    )
    assert inspection.state is LeafState.CATALOG_CONFLICT


async def test_pending_locator_blocks_detach_until_verified_quarantine_repoint(
    partition_schema: AsyncConnection,
    engine: AsyncEngine,
) -> None:
    schema = _schema(partition_schema)
    task_id = await _seed_pending_locator(partition_schema)
    blocked = await detach_history_leaf_concurrently(
        engine,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert blocked.state is LeafState.PENDING_BLOCKED
    assert blocked.blocker_count == 1

    outcome = (
        await partition_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.quarantine_phase2_pending(
                    CAST(:task_id AS uuid), :lower, :upper,
                    interval '1 day', 'detach horizon exceeded'
                )::text
                """
            ),
            {'task_id': task_id, 'lower': _OLD_LOWER, 'upper': _OLD_UPPER},
        )
    ).scalar_one()
    assert outcome == 'REPOINTED'
    await partition_schema.commit()

    detached = await detach_history_leaf_concurrently(
        engine,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert detached.state is LeafState.DETACHED
    parent_count = (
        await partition_schema.execute(
            text(
                f"""
                SELECT count(*) FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    detached_count = (
        await partition_schema.execute(
            text(
                f"""
                SELECT count(*) FROM {schema.sql}.{_OLD_LEAF}
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert (parent_count, detached_count) == (0, 1)
    dropped = await drop_detached_history_leaf(
        partition_schema,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert dropped.state is LeafState.DROPPED


async def test_future_leaf_creation_is_idempotent_and_indexed(
    partition_schema: AsyncConnection,
) -> None:
    schema = _schema(partition_schema)
    leaf_name = 'history_aggregate_finite_2026_08_06'
    lower = datetime(2026, 8, 6, tzinfo=timezone.utc)
    upper = datetime(2026, 8, 7, tzinfo=timezone.utc)
    await create_daily_history_leaf(
        partition_schema,
        schema,
        leaf_name=leaf_name,
        class_key=_CLASS_KEY,
        lower=lower,
        upper=upper,
    )
    await create_daily_history_leaf(
        partition_schema,
        schema,
        leaf_name=leaf_name,
        class_key=_CLASS_KEY,
        lower=lower,
        upper=upper,
    )
    index_exists = (
        await partition_schema.execute(
            text(
                """
                SELECT to_regclass(:index_name) IS NOT NULL
                """
            ),
            {'index_name': f'{schema.sql}.{leaf_name}_task_idx'},
        )
    ).scalar_one()
    assert index_exists is True
    catalog = (
        await partition_schema.execute(
            text(
                f"""
                SELECT class_key, lower_anchor, upper_anchor
                FROM {schema.sql}.history_leaf_catalog
                WHERE leaf_name = :leaf_name
                """
            ),
            {'leaf_name': leaf_name},
        )
    ).one()
    assert tuple(catalog) == (_CLASS_KEY, lower, upper)
    await partition_schema.execute(
        text(f'DROP INDEX {schema.sql}.{leaf_name}_task_idx')
    )
    conflict = await inspect_history_leaf(
        partition_schema,
        schema,
        leaf_name=leaf_name,
        class_key=_CLASS_KEY,
        lower=lower,
        upper=upper,
    )
    assert conflict.state is LeafState.CATALOG_CONFLICT
    await create_daily_history_leaf(
        partition_schema,
        schema,
        leaf_name=leaf_name,
        class_key=_CLASS_KEY,
        lower=lower,
        upper=upper,
    )
    reconciled = await inspect_history_leaf(
        partition_schema,
        schema,
        leaf_name=leaf_name,
        class_key=_CLASS_KEY,
        lower=lower,
        upper=upper,
    )
    assert reconciled.state is LeafState.NOT_EXPIRED


async def test_interrupted_concurrent_detach_is_finalized(
    partition_schema: AsyncConnection,
    engine: AsyncEngine,
) -> None:
    schema = _schema(partition_schema)
    reader = await engine.connect()
    reader_transaction = await reader.begin()
    try:
        await reader.execute(
            text(f'SELECT count(*) FROM {schema.sql}.history_aggregate')
        )
        with pytest.raises(DBAPIError, match='statement timeout'):
            await detach_history_leaf_concurrently(
                engine,
                schema,
                leaf_name=_OLD_LEAF,
                class_key=_CLASS_KEY,
                lower=_OLD_LOWER,
                upper=_OLD_UPPER,
                statement_timeout_ms=100,
            )
    finally:
        await reader_transaction.rollback()
        await reader.close()

    pending = await inspect_history_leaf(
        partition_schema,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert pending.state is LeafState.DETACH_PENDING
    await partition_schema.rollback()
    finalized = await finalize_interrupted_detach(
        engine,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert finalized.state is LeafState.DETACHED


async def test_completed_detach_without_catalog_update_is_reconciled(
    partition_schema: AsyncConnection,
    engine: AsyncEngine,
) -> None:
    schema = _schema(partition_schema)
    await partition_schema.execute(
        text(
            f"""
            ALTER TABLE {schema.sql}.history_aggregate_finite
            DETACH PARTITION {schema.sql}.{_OLD_LEAF}
            """
        )
    )
    await partition_schema.commit()

    reconciled = await finalize_interrupted_detach(
        engine,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert reconciled.state is LeafState.DETACHED
    detached_at = (
        await partition_schema.execute(
            text(
                f"""
                SELECT detached_at
                FROM {schema.sql}.history_leaf_catalog
                WHERE leaf_name = :leaf_name
                """
            ),
            {'leaf_name': _OLD_LEAF},
        )
    ).scalar_one()
    assert detached_at is not None


async def test_drop_refuses_out_of_band_detach_with_pending_locator(
    partition_schema: AsyncConnection,
) -> None:
    schema = _schema(partition_schema)
    await _seed_pending_locator(partition_schema)
    await partition_schema.execute(
        text(
            f"""
            ALTER TABLE {schema.sql}.history_aggregate_finite
            DETACH PARTITION {schema.sql}.{_OLD_LEAF}
            """
        )
    )
    blocked = await drop_detached_history_leaf(
        partition_schema,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    assert blocked.state is LeafState.PENDING_BLOCKED
    assert blocked.blocker_count == 1
    assert blocked.attached is False
    assert (
        await partition_schema.execute(
            text('SELECT to_regclass(:leaf) IS NOT NULL'),
            {'leaf': f'{schema.sql}.{_OLD_LEAF}'},
        )
    ).scalar_one() is True


async def test_terminal_writer_waits_for_partition_maintenance_leaf_lock(
    partition_schema: AsyncConnection,
    engine: AsyncEngine,
) -> None:
    schema = _schema(partition_schema)
    admin_engine = engine.execution_options(isolation_level='AUTOCOMMIT')
    async with engine.connect() as writer:
        async with writer.begin():
            async with admin_engine.connect() as manager:
                await manager.execute(
                    text(
                        f"""
                        SELECT pg_advisory_lock(
                            {schema.sql}.history_leaf_lock_key(
                                :class_key, :anchor
                            )
                        )
                        """
                    ),
                    {'class_key': _CLASS_KEY, 'anchor': _OLD_LOWER},
                )
                writer_lock = asyncio.create_task(
                    lock_history_leaf_for_transaction(
                        writer,
                        schema,
                        class_key=_CLASS_KEY,
                        anchor=datetime(2026, 6, 1, 12, tzinfo=timezone.utc),
                    )
                )
                try:
                    waiting = 0
                    for _ in range(50):
                        waiting = (
                            await manager.execute(
                                text(
                                    """
                                    SELECT count(*)
                                    FROM pg_locks
                                    WHERE locktype = 'advisory' AND NOT granted
                                    """
                                )
                            )
                        ).scalar_one()
                        if waiting:
                            break
                        await asyncio.sleep(0.01)
                    assert waiting >= 1
                    assert writer_lock.done() is False
                finally:
                    released = (
                        await manager.execute(
                            text(
                                f"""
                                SELECT pg_advisory_unlock(
                                    {schema.sql}.history_leaf_lock_key(
                                        :class_key, :anchor
                                    )
                                )
                                """
                            ),
                            {'class_key': _CLASS_KEY, 'anchor': _OLD_LOWER},
                        )
                    ).scalar_one()
                    assert released is True
                await asyncio.wait_for(writer_lock, timeout=1)


async def test_installer_connection_has_partition_management_privileges(
    partition_schema: AsyncConnection,
) -> None:
    report = await partition_privileges(partition_schema, _schema(partition_schema))
    assert report.can_manage is True


async def test_worker_and_operator_partition_privilege_modes_are_distinct(
    partition_schema: AsyncConnection,
) -> None:
    schema = _schema(partition_schema)
    suffix = schema.name.rsplit('_', maxsplit=1)[-1]
    worker_role = f'history_worker_{suffix}'
    operator_role = f'history_operator_{suffix}'
    original_user = (
        await partition_schema.execute(text('SELECT current_user'))
    ).scalar_one()
    assert original_user.replace('_', '').isalnum()
    await partition_schema.execute(text(f'CREATE ROLE {worker_role} NOLOGIN'))
    await partition_schema.execute(text(f'CREATE ROLE {operator_role} NOLOGIN'))
    await partition_schema.execute(
        text(f'GRANT USAGE ON SCHEMA {schema.sql} TO {worker_role}, {operator_role}')
    )
    await partition_schema.execute(
        text(f'GRANT CREATE ON SCHEMA {schema.sql} TO {operator_role}')
    )
    await partition_schema.execute(
        text(
            f"""
            ALTER TABLE {schema.sql}.history_aggregate_finite
            OWNER TO {operator_role}
            """
        )
    )
    await partition_schema.commit()

    try:
        await partition_schema.execute(text(f'SET ROLE {worker_role}'))
        worker_report = await partition_privileges(partition_schema, schema)
        assert worker_report.can_manage is False
        with pytest.raises(DBAPIError, match='permission denied|must be owner'):
            await create_daily_history_leaf(
                partition_schema,
                schema,
                leaf_name='history_aggregate_finite_worker_probe',
                class_key=_CLASS_KEY,
                lower=datetime(2026, 8, 7, tzinfo=timezone.utc),
                upper=datetime(2026, 8, 8, tzinfo=timezone.utc),
            )
        await partition_schema.rollback()
        await partition_schema.execute(text('RESET ROLE'))

        await partition_schema.execute(text(f'SET ROLE {operator_role}'))
        operator_report = await partition_privileges(partition_schema, schema)
        assert operator_report.can_manage is True
        await partition_schema.execute(text('RESET ROLE'))
    finally:
        await partition_schema.rollback()
        await partition_schema.execute(text('RESET ROLE'))
        await partition_schema.execute(
            text(
                f"""
                ALTER TABLE {schema.sql}.history_aggregate_finite
                OWNER TO {original_user}
                """
            )
        )
        await partition_schema.execute(
            text(
                f"""
                REVOKE ALL ON SCHEMA {schema.sql}
                FROM {worker_role}, {operator_role}
                """
            )
        )
        await partition_schema.execute(text(f'DROP ROLE {worker_role}'))
        await partition_schema.execute(text(f'DROP ROLE {operator_role}'))
        await partition_schema.commit()


class TestBoundComparisonIsSessionIndependent:
    """A leaf's bound must compare equal across sessions in different zones.

    `pg_get_expr` renders timestamptz literals in the *session* timezone, and
    the leaf catalog stores that rendering as text. Comparing a fresh render
    against the stored one is therefore session-dependent: identical instants
    render as `FROM '2026-06-01 00:00:00+00'` under UTC and
    `FROM '2026-05-31 12:00:00-12'` twelve hours west, and the leaf reads as a
    catalog conflict having never changed.

    This is not a hazard the lifecycle can avoid by staying in one session.
    Concurrent detach runs on a **dedicated autocommit connection** by design,
    so capture and comparison are different sessions by construction — which
    is why the comparison had to become session-independent rather than merely
    careful.
    """

    @pytest.mark.parametrize(
        'capture_zone, compare_zone',
        (
            ('UTC', 'Europe/Berlin'),
            ('Europe/Berlin', 'UTC'),
            ('Etc/GMT+12', 'Pacific/Kiritimati'),
            ('Pacific/Kiritimati', 'Etc/GMT+12'),
        ),
    )
    async def test_leaf_reads_ready_across_session_zones(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs the base schema
        capture_zone: str,
        compare_zone: str,
    ) -> None:
        schema = PrototypeSchema(f'history_tz_{uuid4().hex[:12]}')
        capture = await engine.connect()
        try:
            await capture.execute(text(f"SET TIME ZONE '{capture_zone}'"))
            await install_archive_candidates(capture, schema)
            await install_workflow_recovery_prototype(capture, schema)
            await install_partition_manager_prototype(capture, schema)
            await capture.commit()

            compare = await engine.connect()
            try:
                await compare.execute(text(f"SET TIME ZONE '{compare_zone}'"))
                inspection = await inspect_history_leaf(
                    compare,
                    schema,
                    leaf_name=_OLD_LEAF,
                    class_key=_CLASS_KEY,
                    lower=_OLD_LOWER,
                    upper=_OLD_UPPER,
                )
            finally:
                await compare.rollback()
                await compare.execute(text('RESET TIME ZONE'))
                await compare.commit()
                await compare.close()

            assert inspection.state is LeafState.READY
            assert inspection.attached is True
        finally:
            await capture.rollback()
            await remove_archive_candidates(capture, schema)
            await capture.execute(text('RESET TIME ZONE'))
            await capture.commit()
            await capture.close()
