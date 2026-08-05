"""Correctness gates for task-history phase-2 pending and quarantine."""

from __future__ import annotations

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
    archive_digest,
    encode_attempts,
    encode_json_value,
    prototype_attempts,
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


@pytest_asyncio.fixture
async def recovery_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[AsyncConnection]:
    schema = PrototypeSchema(f'history_recovery_{uuid4().hex[:12]}')
    connection = await engine.connect()
    try:
        await install_archive_candidates(connection, schema)
        await install_workflow_recovery_prototype(connection, schema)
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


async def _seed_recovery(
    connection: AsyncConnection,
    *,
    workflow_status: str = 'RUNNING',
    node_status: str = 'RUNNING',
    pending_age: str = '90 days',
    requires_parent: bool = False,
    digest_override: bytes | None = None,
) -> tuple[str, str, str, bytes, bytes]:
    schema = _schema(connection)
    task_id = str(uuid4())
    workflow_id = str(uuid4())
    generation = str(uuid4())
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
                'finite_30d_v1', :terminal_at, :terminal_at,
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
            'version': ARCHIVE_VERSION,
            'codec': ARCHIVE_CODEC,
            'result_payload': result.payload,
            'result_digest': result.digest,
            'workflow_id': workflow_id,
            'attempts': attempts.payload,
            'attempts_digest': attempts.digest,
        },
    )
    parameters = {
        'workflow_id': workflow_id,
        'workflow_status': workflow_status,
        'task_id': task_id,
        'node_status': node_status,
        'requires_parent': requires_parent,
        'terminal_at': terminal_at,
        'version': ARCHIVE_VERSION,
        'pending_digest': digest_override or result.digest,
        'generation': generation,
        'pending_age': pending_age,
    }
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.phase2_workflows (workflow_id, status)
            VALUES (:workflow_id, :workflow_status)
            """
        ),
        parameters,
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.phase2_nodes (
                workflow_id, node_id, task_id, status,
                requires_parent_propagation
            ) VALUES (
                :workflow_id, 'node-1', :task_id, :node_status,
                :requires_parent
            )
            """
        ),
        parameters,
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
                CAST(:task_id AS varchar(36)),
                :workflow_id,
                (SELECT id FROM {schema.sql}.phase2_nodes
                 WHERE task_id = CAST(:task_id AS varchar(36))),
                'COMPLETED', :terminal_at,
                'COMPLETE_LOCKED', 'HISTORY', 'finite_30d_v1', :terminal_at,
                :version, :pending_digest, :generation,
                statement_timestamp() - CAST(:pending_age AS interval)
            )
            """
        ),
        parameters,
    )
    return task_id, workflow_id, generation, result.payload, result.digest


async def _quarantine(connection: AsyncConnection, task_id: str) -> str:
    schema = _schema(connection)
    return (
        await connection.execute(
            text(
                f"""
                SELECT {schema.sql}.quarantine_phase2_pending(
                    CAST(:task_id AS varchar(36)),
                    '2026-06-01T00:00:00Z'::timestamptz,
                    '2026-06-02T00:00:00Z'::timestamptz,
                    interval '1 day', 'detach horizon exceeded'
                )::text
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()


async def _apply(connection: AsyncConnection, task_id: str, generation: str) -> str:
    schema = _schema(connection)
    return (
        await connection.execute(
            text(
                f"""
                SELECT {schema.sql}.apply_phase2(
                    CAST(:task_id AS varchar(36)),
                    CAST(:generation AS varchar(36))
                )::text
                """
            ),
            {'task_id': task_id, 'generation': generation},
        )
    ).scalar_one()


async def test_quarantine_copy_verifies_and_repoints_atomically(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, _, result_payload, result_digest = await _seed_recovery(recovery_schema)
    assert await _quarantine(recovery_schema, task_id) == 'REPOINTED'
    pending = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT recovery_source::text, history_class, history_anchor,
                       quarantine_task_id
                FROM {schema.sql}.workflow_phase2_pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(pending) == ('QUARANTINE', None, None, task_id)
    quarantine = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT result_payload, result_digest, source_history_class,
                       source_history_anchor
                FROM {schema.sql}.workflow_phase2_quarantine
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert bytes(quarantine.result_payload) == result_payload
    assert bytes(quarantine.result_digest) == result_digest
    assert quarantine.source_history_class == 'finite_30d_v1'
    history_count = (
        await recovery_schema.execute(
            text(
                f'SELECT count(*) FROM {schema.sql}.history_aggregate '
                'WHERE task_id = :task_id'
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert history_count == 1


async def test_healthy_pending_is_not_quarantined_early(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(recovery_schema, pending_age='1 hour')
    assert await _quarantine(recovery_schema, task_id) == 'TOO_YOUNG'
    assert (
        await recovery_schema.execute(
            text(f'SELECT count(*) FROM {schema.sql}.workflow_phase2_quarantine')
        )
    ).scalar_one() == 0


async def test_quarantine_integrity_conflict_retains_history_locator(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(
        recovery_schema, digest_override=archive_digest(b'different')
    )
    assert await _quarantine(recovery_schema, task_id) == 'INTEGRITY_CONFLICT'
    source = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT recovery_source::text
                FROM {schema.sql}.workflow_phase2_pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert source == 'HISTORY'


async def test_quarantine_rejects_undecodable_archive_and_records_diagnostics(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(recovery_schema)
    invalid_payload = b'not-json'
    invalid_digest = archive_digest(invalid_payload)
    await recovery_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_payload = :payload, result_digest = :digest
            WHERE task_id = :task_id
            """
        ),
        {
            'task_id': task_id,
            'payload': invalid_payload,
            'digest': invalid_digest,
        },
    )
    await recovery_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.workflow_phase2_pending
            SET result_digest = :digest
            WHERE task_id = :task_id
            """
        ),
        {'task_id': task_id, 'digest': invalid_digest},
    )

    assert await _quarantine(recovery_schema, task_id) == 'INTEGRITY_CONFLICT'
    pending = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT recovery_source::text, attempt_count, last_failure_class
                FROM {schema.sql}.workflow_phase2_pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(pending) == ('HISTORY', 1, 'SOURCE_INTEGRITY')


async def test_quarantine_copy_rolls_back_when_repoint_fails(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(recovery_schema)
    await recovery_schema.execute(
        text(
            f"""
            CREATE FUNCTION {schema.sql}.reject_pending_repoint()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                IF NEW.recovery_source = 'QUARANTINE' THEN
                    RAISE EXCEPTION 'repoint disabled by test';
                END IF;
                RETURN NEW;
            END
            $function$
            """
        )
    )
    await recovery_schema.execute(
        text(
            f"""
            CREATE TRIGGER reject_pending_repoint
            BEFORE UPDATE ON {schema.sql}.workflow_phase2_pending
            FOR EACH ROW EXECUTE FUNCTION {schema.sql}.reject_pending_repoint()
            """
        )
    )

    with pytest.raises(DBAPIError, match='repoint disabled by test'):
        async with recovery_schema.begin_nested():
            await _quarantine(recovery_schema, task_id)

    state = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT
                    (SELECT recovery_source::text
                     FROM {schema.sql}.workflow_phase2_pending
                     WHERE task_id = :task_id) AS source,
                    (SELECT count(*)
                     FROM {schema.sql}.workflow_phase2_quarantine
                     WHERE task_id = :task_id) AS quarantine_count
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(state) == ('HISTORY', 0)


@pytest.mark.parametrize('workflow_status', ['RUNNING', 'PAUSED'])
async def test_phase2_applies_node_from_history_while_running_or_paused(
    recovery_schema: AsyncConnection,
    workflow_status: str,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, generation, result_payload, result_digest = await _seed_recovery(
        recovery_schema,
        workflow_status=workflow_status,
    )
    assert await _apply(recovery_schema, task_id, generation) == 'APPLIED_TO_NODE'
    node = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT status, result_payload, result_digest, phase2_generation
                FROM {schema.sql}.phase2_nodes
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert node.status == 'COMPLETED'
    assert bytes(node.result_payload) == result_payload
    assert bytes(node.result_digest) == result_digest
    assert node.phase2_generation == generation
    assert await _apply(recovery_schema, task_id, generation) == 'ALREADY_APPLIED'


async def test_phase2_from_quarantine_deletes_both_recovery_records(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, generation, *_ = await _seed_recovery(
        recovery_schema,
        requires_parent=True,
    )
    assert await _quarantine(recovery_schema, task_id) == 'REPOINTED'
    assert await _apply(recovery_schema, task_id, generation) == 'APPLIED_TO_NODE'
    counts = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT
                    (SELECT count(*) FROM {schema.sql}.workflow_phase2_pending)
                        AS pending,
                    (SELECT count(*) FROM {schema.sql}.workflow_phase2_quarantine)
                        AS quarantine,
                    (SELECT count(*)
                     FROM {schema.sql}.phase2_parent_responsibilities)
                        AS parent_responsibilities
                """
            )
        )
    ).one()
    assert tuple(counts) == (0, 0, 1)


async def test_phase2_delete_failure_rolls_back_all_durable_effects(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, generation, *_ = await _seed_recovery(
        recovery_schema,
        requires_parent=True,
    )
    assert await _quarantine(recovery_schema, task_id) == 'REPOINTED'
    await recovery_schema.execute(
        text(
            f"""
            CREATE FUNCTION {schema.sql}.reject_pending_delete()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                RAISE EXCEPTION 'pending delete disabled by test';
            END
            $function$
            """
        )
    )
    await recovery_schema.execute(
        text(
            f"""
            CREATE TRIGGER reject_pending_delete
            BEFORE DELETE ON {schema.sql}.workflow_phase2_pending
            FOR EACH ROW EXECUTE FUNCTION {schema.sql}.reject_pending_delete()
            """
        )
    )

    with pytest.raises(DBAPIError, match='pending delete disabled by test'):
        async with recovery_schema.begin_nested():
            await _apply(recovery_schema, task_id, generation)

    state = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT
                    (SELECT status FROM {schema.sql}.phase2_nodes
                     WHERE task_id = :task_id),
                    (SELECT count(*)
                     FROM {schema.sql}.workflow_phase2_pending
                     WHERE task_id = :task_id),
                    (SELECT count(*)
                     FROM {schema.sql}.workflow_phase2_quarantine
                     WHERE task_id = :task_id),
                    (SELECT count(*)
                     FROM {schema.sql}.phase2_parent_responsibilities)
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(state) == ('RUNNING', 1, 1, 0)


async def test_phase2_conflict_retains_pending_evidence(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, generation, *_ = await _seed_recovery(
        recovery_schema,
        node_status='CANCELLED',
    )
    assert await _apply(recovery_schema, task_id, generation) == 'SOURCE_STATE_CONFLICT'
    assert (
        await recovery_schema.execute(
            text(
                f'SELECT count(*) FROM {schema.sql}.workflow_phase2_pending '
                'WHERE task_id = :task_id'
            ),
            {'task_id': task_id},
        )
    ).scalar_one() == 1


async def test_phase2_rejects_unknown_result_version_and_records_diagnostics(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, generation, *_ = await _seed_recovery(recovery_schema)
    await recovery_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_envelope_version = 99
            WHERE task_id = :task_id
            """
        ),
        {'task_id': task_id},
    )

    assert await _apply(recovery_schema, task_id, generation) == 'SOURCE_STATE_CONFLICT'
    pending = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT attempt_count, last_failure_class
                FROM {schema.sql}.workflow_phase2_pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(pending) == (1, 'SOURCE_INTEGRITY')


async def test_terminal_workflow_supersedes_pending_and_cleans_recovery_source(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, _, generation, *_ = await _seed_recovery(
        recovery_schema,
        workflow_status='CANCELLED',
    )
    assert (
        await _apply(recovery_schema, task_id, generation)
        == 'SUPERSEDED_BY_WORKFLOW_TERMINAL'
    )
    remaining = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT
                    (SELECT count(*)
                     FROM {schema.sql}.workflow_phase2_pending
                     WHERE task_id = :task_id),
                    (SELECT count(*)
                     FROM {schema.sql}.workflow_phase2_quarantine
                     WHERE task_id = :task_id)
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(remaining) == (0, 0)


async def test_pending_fixed_projection_stays_within_declared_bound(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(recovery_schema)
    await recovery_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.workflow_phase2_pending
            SET history_class = repeat('h', 64),
                last_failure_class = repeat('f', 64),
                last_attempt_at = statement_timestamp(),
                attempt_count = 2147483647
            WHERE task_id = :task_id
            """
        ),
        {'task_id': task_id},
    )
    size = (
        await recovery_schema.execute(
            text(
                f"""
                SELECT pg_column_size(pending)
                FROM {schema.sql}.workflow_phase2_pending AS pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert size <= 512


async def test_pending_locator_structurally_pins_node_to_workflow(
    recovery_schema: AsyncConnection,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(recovery_schema)
    replacement_workflow_id = str(uuid4())
    await recovery_schema.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.phase2_workflows (workflow_id, status)
            VALUES (:workflow_id, 'RUNNING')
            """
        ),
        {'workflow_id': replacement_workflow_id},
    )
    with pytest.raises(DBAPIError, match='foreign key constraint'):
        await recovery_schema.execute(
            text(
                f"""
                UPDATE {schema.sql}.phase2_nodes
                SET workflow_id = :replacement_workflow_id
                WHERE task_id = :task_id
                """
            ),
            {
                'replacement_workflow_id': replacement_workflow_id,
                'task_id': task_id,
            },
        )


@pytest.mark.parametrize(
    ('column', 'value'),
    [
        ('history_class', 'h' * 65),
        ('history_class', 'é' * 33),
        ('terminalization_kind', 't' * 33),
        ('terminalization_kind', 'é' * 17),
        ('last_failure_class', 'f' * 65),
        ('last_failure_class', 'é' * 33),
    ],
)
async def test_pending_bounded_fields_reject_oversized_utf8_values(
    recovery_schema: AsyncConnection,
    column: str,
    value: str,
) -> None:
    schema = _schema(recovery_schema)
    task_id, *_ = await _seed_recovery(recovery_schema)
    with pytest.raises(DBAPIError, match='check constraint|value too long'):
        await recovery_schema.execute(
            text(
                f"""
                UPDATE {schema.sql}.workflow_phase2_pending
                SET {column} = :value
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id, 'value': value},
        )
