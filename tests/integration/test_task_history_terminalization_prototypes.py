"""Transaction-level gates for direct live-to-history completion."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import asyncio
import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedForeignTerminalization,
    ObservedStaleness,
    SourceStateConflict,
    TaskAbsent,
    decode_outcome_row,
)
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.task_history_prototypes.archive import (
    ArchiveDomain,
    DecodedArchiveValue,
    decode_attempts,
    decode_json_value,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.terminalization import (
    install_history_terminalization_prototype,
)
from tests.task_history_prototypes.transcode import (
    begin_archive_maintenance,
    finish_archive_maintenance,
    install_archive_transcode_prototype,
)
from tests.task_history_prototypes.workflow_schema import (
    install_workflow_recovery_prototype,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WORKER = 'history-prototype-worker'
_OTHER_WORKER = 'history-prototype-other-worker'
_GENERATION = datetime(2026, 8, 5, 9, tzinfo=timezone.utc)
_RESULT = '{"ok":{"value":42}}'


@pytest_asyncio.fixture
async def terminalization_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[AsyncConnection]:
    schema = PrototypeSchema(f'history_terminal_{uuid4().hex[:12]}')
    connection = await engine.connect()
    try:
        await install_archive_candidates(connection, schema)
        await install_archive_transcode_prototype(connection, schema)
        await install_workflow_recovery_prototype(connection, schema)
        await install_history_terminalization_prototype(connection, schema)
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


async def _seed_live_task(
    connection: AsyncConnection,
    *,
    is_workflow_task: bool = False,
    retention_class: str = 'forever',
    worker_id: str = _WORKER,
    claimed_at: datetime = _GENERATION,
) -> tuple[str, str | None]:
    schema = _schema(connection)
    task_id = str(uuid4())
    workflow_id = str(uuid4()) if is_workflow_task else None
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.live_tasks (
                id, task_name, queue_name, priority, status,
                args, kwargs, enqueue_sha, is_workflow_task,
                claimed, claimed_by_worker_id, claimed_at, started_at,
                retention_class_key
            ) VALUES (
                :task_id, 'prototype.complete', 'default', 100, 'RUNNING',
                '[]', '{{}}', repeat('a', 64), :is_workflow_task,
                TRUE, :worker_id, :claimed_at, :claimed_at,
                :retention_class
            )
            """
        ),
        {
            'task_id': task_id,
            'is_workflow_task': is_workflow_task,
            'worker_id': worker_id,
            'claimed_at': claimed_at,
            'retention_class': retention_class,
        },
    )
    if workflow_id is not None:
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
    await connection.commit()
    return task_id, workflow_id


async def _complete_fused(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker_id: str = _WORKER,
    claimed_at: datetime = _GENERATION,
) -> Applied | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent:
    schema = _schema(connection)
    row = (
        (
            await connection.execute(
                text(
                    f"""
                SELECT * FROM {schema.sql}.horsies_complete_task_fused(
                    CAST(:task_id AS varchar), :worker_id,
                    CAST(:claimed_at AS timestamptz), :result,
                    'task_queue_default', :task_id
                )
                """
                ),
                {
                    'task_id': task_id,
                    'worker_id': worker_id,
                    'claimed_at': claimed_at,
                    'result': _RESULT,
                },
            )
        )
        .mappings()
        .one()
    )
    return decode_outcome_row(_plain_mapping(row))


async def _complete_locked(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker_id: str = _WORKER,
) -> Applied | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent:
    schema = _schema(connection)
    row = (
        (
            await connection.execute(
                text(
                    f"""
                SELECT * FROM {schema.sql}.horsies_complete_locked_task(
                    CAST(:task_id AS varchar), :worker_id, :result
                )
                """
                ),
                {'task_id': task_id, 'worker_id': worker_id, 'result': _RESULT},
            )
        )
        .mappings()
        .one()
    )
    return decode_outcome_row(_plain_mapping(row))


async def _fail_locked(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker_id: str = _WORKER,
) -> Applied | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent:
    schema = _schema(connection)
    row = (
        (
            await connection.execute(
                text(
                    f"""
                SELECT * FROM {schema.sql}.horsies_fail_locked_task(
                    CAST(:task_id AS varchar), :worker_id, :result,
                    'TASK_EXCEPTION', 'final attempt failed'
                )
                """
                ),
                {'task_id': task_id, 'worker_id': worker_id, 'result': _RESULT},
            )
        )
        .mappings()
        .one()
    )
    return decode_outcome_row(_plain_mapping(row))


async def _fail_stale(
    connection: AsyncConnection,
    task_id: str,
    *,
    stale_after_ms: int,
    finalizing_stale_after_ms: int,
) -> Applied | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent:
    schema = _schema(connection)
    row = (
        (
            await connection.execute(
                text(
                    f"""
                SELECT * FROM {schema.sql}.horsies_fail_stale_task(
                    CAST(:task_id AS varchar), :stale_after_ms,
                    :finalizing_stale_after_ms, :result,
                    'WORKER_LOST', 'stale runner'
                )
                """
                ),
                {
                    'task_id': task_id,
                    'stale_after_ms': stale_after_ms,
                    'finalizing_stale_after_ms': finalizing_stale_after_ms,
                    'result': _RESULT,
                },
            )
        )
        .mappings()
        .one()
    )
    return decode_outcome_row(_plain_mapping(row))


async def _seed_attempt(
    connection: AsyncConnection,
    task_id: str,
    *,
    attempt: int = 1,
    outcome: str = 'FAILED',
    will_retry: bool = False,
) -> None:
    schema = _schema(connection)
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.live_attempts (
                task_id, attempt, outcome, will_retry,
                started_at, finished_at, error_code, error_message,
                failed_reason, worker_id
            ) VALUES (
                :task_id, :attempt, :outcome, :will_retry,
                :started_at, :finished_at, 'TASK_EXCEPTION',
                'attempt failed', 'final attempt failed', :worker_id
            )
            """
        ),
        {
            'task_id': task_id,
            'attempt': attempt,
            'outcome': outcome,
            'will_retry': will_retry,
            'started_at': _GENERATION,
            'finished_at': _GENERATION + timedelta(seconds=1),
            'worker_id': _WORKER,
        },
    )
    await connection.commit()


def _plain_mapping(row: Mapping[Any, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in row.items()}


async def _relation_counts(
    connection: AsyncConnection,
    task_id: str,
) -> tuple[int, int, int, int]:
    schema = _schema(connection)
    row = (
        await connection.execute(
            text(
                f"""
                SELECT
                    (SELECT count(*) FROM {schema.sql}.live_tasks
                     WHERE id = :task_id),
                    (SELECT count(*) FROM {schema.sql}.live_attempts
                     WHERE task_id = :task_id),
                    (SELECT count(*) FROM {schema.sql}.history_aggregate
                     WHERE task_id = :task_id),
                    (SELECT count(*) FROM {schema.sql}.workflow_phase2_pending
                     WHERE task_id = :task_id)
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    return tuple(int(value) for value in row)  # type: ignore[return-value]


async def test_fused_completion_moves_task_and_attempt_once(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)

    outcome = await _complete_fused(terminalization_schema, task_id)
    assert isinstance(outcome, Applied)
    assert outcome.kind is TerminalizationKind.COMPLETE_FUSED
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 0)

    history = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT result_envelope_version, result_codec, result_payload,
                       result_digest, attempt_archive_version,
                       attempt_snapshot_codec, attempt_snapshot,
                       attempt_snapshot_digest
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    result = decode_json_value(
        domain=ArchiveDomain.RESULT,
        version=history.result_envelope_version,
        codec=history.result_codec,
        payload=bytes(history.result_payload),
        digest=bytes(history.result_digest),
    )
    assert result == DecodedArchiveValue({'ok': {'value': 42}})
    attempts = decode_attempts(
        version=history.attempt_archive_version,
        codec=history.attempt_snapshot_codec,
        payload=bytes(history.attempt_snapshot),
        digest=bytes(history.attempt_snapshot_digest),
    )
    assert isinstance(attempts, DecodedArchiveValue)
    assert [(attempt.attempt, attempt.outcome) for attempt in attempts.value] == [
        (1, 'COMPLETED')
    ]


async def test_workflow_completion_creates_precise_pending_and_phase2_applies(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, workflow_id = await _seed_live_task(
        terminalization_schema, is_workflow_task=True
    )
    assert workflow_id is not None

    outcome = await _complete_locked(terminalization_schema, task_id)
    assert isinstance(outcome, Applied)
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 1)
    pending = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT workflow_id, node_id, recovery_source::text,
                       history_class, history_anchor, result_digest,
                       phase2_generation
                FROM {schema.sql}.workflow_phase2_pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert pending.workflow_id == workflow_id
    assert tuple(pending[:4]) == (workflow_id, 'node-1', 'HISTORY', 'forever')
    assert pending.history_anchor == outcome.terminal_at

    disposition = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.apply_phase2(
                    CAST(:task_id AS varchar(36)),
                    CAST(:generation AS varchar(36))
                )::text
                """
            ),
            {'task_id': task_id, 'generation': pending.phase2_generation},
        )
    ).scalar_one()
    assert disposition == 'APPLIED_TO_NODE'
    node = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT status, result_payload, result_digest
                FROM {schema.sql}.phase2_nodes
                WHERE workflow_id = :workflow_id AND node_id = 'node-1'
                """
            ),
            {'workflow_id': workflow_id},
        )
    ).one()
    assert node.status == 'COMPLETED'
    assert bytes(node.result_digest) == bytes(pending.result_digest)
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 0)


async def test_workflow_failure_archives_attempt_and_defers_phase2(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, workflow_id = await _seed_live_task(
        terminalization_schema, is_workflow_task=True
    )
    assert workflow_id is not None
    await _seed_attempt(terminalization_schema, task_id)

    outcome = await _fail_locked(terminalization_schema, task_id)
    assert isinstance(outcome, Applied)
    assert outcome.kind is TerminalizationKind.FAIL_RUNNING
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 1)
    history = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT status, error_code, final_failed_reason,
                       attempt_archive_version, attempt_snapshot_codec,
                       attempt_snapshot, attempt_snapshot_digest
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(history[:3]) == ('FAILED', 'TASK_EXCEPTION', 'final attempt failed')
    attempts = decode_attempts(
        version=history.attempt_archive_version,
        codec=history.attempt_snapshot_codec,
        payload=bytes(history.attempt_snapshot),
        digest=bytes(history.attempt_snapshot_digest),
    )
    assert isinstance(attempts, DecodedArchiveValue)
    assert [
        (item.attempt, item.outcome, item.will_retry) for item in attempts.value
    ] == [(1, 'FAILED', False)]
    pending = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT phase2_generation
                FROM {schema.sql}.workflow_phase2_pending
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    disposition = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.apply_phase2(
                    CAST(:task_id AS varchar(36)),
                    CAST(:generation AS varchar(36))
                )::text
                """
            ),
            {'task_id': task_id, 'generation': pending.phase2_generation},
        )
    ).scalar_one()
    assert disposition == 'APPLIED_TO_NODE'
    node_status = (
        await terminalization_schema.execute(
            text(
                f"""
                SELECT status FROM {schema.sql}.phase2_nodes
                WHERE workflow_id = :workflow_id AND node_id = 'node-1'
                """
            ),
            {'workflow_id': workflow_id},
        )
    ).scalar_one()
    assert node_status == 'FAILED'


async def test_stale_failure_reports_locked_guard_evidence(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)
    await terminalization_schema.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.live_heartbeats (
                task_id, sender_id, role, sent_at
            ) VALUES (:task_id, :worker_id, 'runner', NOW())
            """
        ),
        {'task_id': task_id, 'worker_id': _WORKER},
    )
    await terminalization_schema.commit()

    outcome = await _fail_stale(
        terminalization_schema,
        task_id,
        stale_after_ms=60_000,
        finalizing_stale_after_ms=60_000,
    )
    assert isinstance(outcome, SourceStateConflict)
    assert isinstance(outcome.evidence, ObservedStaleness)
    assert outcome.evidence.last_heartbeat_at is not None
    assert outcome.evidence.started_at == _GENERATION
    assert outcome.evidence.stale_after_ms == 60_000
    assert outcome.evidence.finalizing_stale_after_ms == 60_000
    assert outcome.evidence.evaluated_at >= outcome.evidence.last_heartbeat_at
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)


async def test_stale_failure_moves_eligible_task_directly_to_history(
    terminalization_schema: AsyncConnection,
) -> None:
    task_id, _ = await _seed_live_task(terminalization_schema)
    await _seed_attempt(terminalization_schema, task_id)

    outcome = await _fail_stale(
        terminalization_schema,
        task_id,
        stale_after_ms=0,
        finalizing_stale_after_ms=0,
    )
    assert isinstance(outcome, Applied)
    assert outcome.kind is TerminalizationKind.FAIL_STALE
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 0)


@pytest.mark.parametrize('first', ['fused', 'locked'])
async def test_completion_equivalence_replays_from_history(
    terminalization_schema: AsyncConnection,
    first: str,
) -> None:
    task_id, _ = await _seed_live_task(terminalization_schema)
    first_outcome = (
        await _complete_fused(terminalization_schema, task_id)
        if first == 'fused'
        else await _complete_locked(terminalization_schema, task_id)
    )
    assert isinstance(first_outcome, Applied)
    await terminalization_schema.commit()

    replay = (
        await _complete_locked(terminalization_schema, task_id)
        if first == 'fused'
        else await _complete_fused(terminalization_schema, task_id)
    )
    assert isinstance(replay, AlreadyApplied)
    assert replay.kind is first_outcome.kind
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 0)


async def test_claim_mismatch_is_typed_and_non_mutating(
    terminalization_schema: AsyncConnection,
) -> None:
    task_id, _ = await _seed_live_task(terminalization_schema)

    outcome = await _complete_fused(
        terminalization_schema, task_id, worker_id=_OTHER_WORKER
    )
    assert isinstance(outcome, LostClaim)
    assert outcome.observed.worker_id == _WORKER
    assert outcome.observed.claimed_at == _GENERATION
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)


async def test_absence_is_a_typed_outcome(
    terminalization_schema: AsyncConnection,
) -> None:
    outcome = await _complete_fused(terminalization_schema, str(uuid4()))
    assert isinstance(outcome, TaskAbsent)


async def test_foreign_history_kind_is_diagnosed(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)
    assert isinstance(await _complete_fused(terminalization_schema, task_id), Applied)
    await terminalization_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET terminalization_kind = 'FAIL_RUNNING', status = 'FAILED'
            WHERE task_id = :task_id
            """
        ),
        {'task_id': task_id},
    )
    await terminalization_schema.commit()

    outcome = await _complete_locked(terminalization_schema, task_id)
    assert isinstance(outcome, SourceStateConflict)
    assert isinstance(outcome.evidence, ObservedForeignTerminalization)
    assert outcome.evidence.committed_kind is TerminalizationKind.FAIL_RUNNING


async def test_missing_workflow_link_rolls_back_the_move(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, workflow_id = await _seed_live_task(
        terminalization_schema, is_workflow_task=True
    )
    assert workflow_id is not None
    await terminalization_schema.execute(
        text(
            f"""
            DELETE FROM {schema.sql}.phase2_nodes
            WHERE workflow_id = :workflow_id
            """
        ),
        {'workflow_id': workflow_id},
    )
    await terminalization_schema.commit()

    with pytest.raises(DBAPIError, match='no node linkage'):
        await _complete_locked(terminalization_schema, task_id)
    await terminalization_schema.rollback()
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)


async def test_missing_history_leaf_rolls_back_fused_attempt_and_live_delete(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(
        terminalization_schema, retention_class='forever'
    )
    await terminalization_schema.execute(
        text(f'DROP TABLE {schema.sql}.history_aggregate_forever')
    )
    await terminalization_schema.commit()

    with pytest.raises(DBAPIError, match='no partition'):
        await _complete_fused(terminalization_schema, task_id)
    await terminalization_schema.rollback()
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)


async def test_archive_maintenance_refuses_terminalization_before_mutation(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)
    await begin_archive_maintenance(
        terminalization_schema,
        schema,
        maintenance_id=str(uuid4()),
    )
    await terminalization_schema.commit()

    with pytest.raises(DBAPIError, match='archive maintenance is active'):
        await _complete_fused(terminalization_schema, task_id)
    await terminalization_schema.rollback()
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)


async def test_archive_maintenance_also_refuses_history_replay(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)
    assert isinstance(await _complete_fused(terminalization_schema, task_id), Applied)
    await terminalization_schema.commit()
    await begin_archive_maintenance(
        terminalization_schema,
        schema,
        maintenance_id=str(uuid4()),
    )
    await terminalization_schema.commit()

    with pytest.raises(DBAPIError, match='archive maintenance is active'):
        await _complete_locked(terminalization_schema, task_id)
    await terminalization_schema.rollback()
    assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 0)


async def test_maintenance_start_waits_for_terminalization_transaction(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)
    await terminalization_schema.execute(
        text(
            f"""
            SELECT singleton
            FROM {schema.sql}.archive_access_gate
            WHERE singleton IS TRUE
            FOR SHARE
            """
        )
    )
    maintenance_connection = await terminalization_schema.engine.connect()
    maintenance_id = str(uuid4())
    start = asyncio.create_task(
        begin_archive_maintenance(
            maintenance_connection,
            schema,
            maintenance_id=maintenance_id,
        )
    )
    try:
        await asyncio.sleep(0.1)
        assert not start.done()
        assert isinstance(
            await _complete_fused(terminalization_schema, task_id), Applied
        )
        await terminalization_schema.commit()
        session = await asyncio.wait_for(start, timeout=2)
        assert session.maintenance_id == maintenance_id
        await maintenance_connection.commit()
        assert await _relation_counts(terminalization_schema, task_id) == (0, 0, 1, 0)
        await finish_archive_maintenance(
            maintenance_connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await maintenance_connection.commit()
    finally:
        if not start.done():
            start.cancel()
        await maintenance_connection.rollback()
        await maintenance_connection.close()


async def test_fused_completion_rejects_workflow_task_without_losing_phase2(
    terminalization_schema: AsyncConnection,
) -> None:
    task_id, _ = await _seed_live_task(terminalization_schema, is_workflow_task=True)

    with pytest.raises(DBAPIError, match='cannot terminalize a workflow task'):
        await _complete_fused(terminalization_schema, task_id)
    await terminalization_schema.rollback()
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)


async def test_task_done_notification_is_transactional_raw_task_id(
    terminalization_schema: AsyncConnection,
) -> None:
    task_id, _ = await _seed_live_task(terminalization_schema)
    listener = await psycopg.AsyncConnection.connect(
        to_psycopg_url(DB_URL), autocommit=True
    )
    try:
        await listener.execute(sql.SQL('LISTEN {}').format(sql.Identifier('task_done')))
        assert isinstance(
            await _complete_fused(terminalization_schema, task_id), Applied
        )
        await terminalization_schema.rollback()
        notifications = listener.notifies()
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(anext(notifications), timeout=0.1)
        finally:
            await notifications.aclose()
        assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 0, 0)

        assert isinstance(
            await _complete_fused(terminalization_schema, task_id), Applied
        )
        await terminalization_schema.commit()
        notifications = listener.notifies()
        try:
            notification = await asyncio.wait_for(anext(notifications), timeout=2)
        finally:
            await notifications.aclose()
        assert notification.channel == 'task_done'
        assert notification.payload == task_id
    finally:
        await listener.close()


async def test_live_and_history_duplicate_is_rejected(
    terminalization_schema: AsyncConnection,
) -> None:
    schema = _schema(terminalization_schema)
    task_id, _ = await _seed_live_task(terminalization_schema)
    await terminalization_schema.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec,
                retry_count, is_workflow_task, history_schema_version,
                attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot, attempt_snapshot_digest
            ) VALUES (
                :task_id, 'duplicate', 'default', 100, 'COMPLETED',
                'COMPLETE_FUSED', NOW(), NOW(), 'forever', NOW(), NOW(),
                1, 'json-utf8', 0, FALSE, 1,
                1, 'json-utf8', convert_to('[]', 'UTF8'),
                sha256(convert_to('[]', 'UTF8'))
            )
            """
        ),
        {'task_id': task_id},
    )
    await terminalization_schema.commit()

    with pytest.raises(DBAPIError, match='multiple locations'):
        await _complete_fused(terminalization_schema, task_id)
    await terminalization_schema.rollback()
    assert await _relation_counts(terminalization_schema, task_id) == (1, 0, 1, 0)
