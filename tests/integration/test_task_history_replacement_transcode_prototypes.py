"""Correctness gates for replacement-partition archive transcoding."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
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
    store_inline_rerun_input,
)
from tests.task_history_prototypes.replacement_transcode import (
    ReplacementCopyBatch,
    ReplacementCopyRejected,
    ReplacementCopyRejectionKind,
    ReplacementReadyForVerification,
    ReplacementTranscodePlan,
    begin_replacement_archive_maintenance,
    finalize_replacement_archive_transcode,
    finish_replacement_archive_maintenance,
    install_replacement_archive_transcode_prototype,
    plan_replacement_archive_transcode,
    replacement_decoder_retirement_status,
    run_replacement_copy_batch,
    swap_verified_replacement_partitions,
    verify_replacement_archive_transcode,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import ArchiveComponent
from tests.task_history_prototypes.transcode import ARCHIVE_CODEC_V2, ARCHIVE_FRAME_V2

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(frozen=True, slots=True)
class _SeededRow:
    task_id: str
    result_payload: bytes
    result_digest: bytes
    attempt_payload: bytes
    attempt_digest: bytes


@pytest_asyncio.fixture
async def replacement_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[tuple[AsyncConnection, PrototypeSchema, str]]:
    schema = PrototypeSchema(f'history_replacement_{uuid4().hex[:10]}')
    connection = await engine.connect()
    maintenance_id = str(uuid4())
    try:
        await install_archive_candidates(connection, schema)
        await install_replacement_archive_transcode_prototype(connection, schema)
        await begin_replacement_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await connection.commit()
        yield connection, schema, maintenance_id
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()
        await connection.close()


async def _seed_history(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    finite_rows: int,
    forever_rows: int,
) -> tuple[_SeededRow, ...]:
    source_task_id = str(uuid4())
    root_task_id = str(uuid4())
    attempts = prototype_attempts(2)
    attempts = attempts[:-1] + (
        replace(
            attempts[-1],
            outcome='FAILED',
            error_code='FINAL_FAILURE',
            error_message='final failure',
            failed_reason='final worker failure',
        ),
    )
    attempt_snapshot = encode_attempts(attempts)
    rerun_input = store_inline_rerun_input(b'{"args":[1],"kwargs":{}}')
    terminal_at = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)
    seeded: list[_SeededRow] = []
    classes = ('finite_30d_v1',) * finite_rows + ('forever',) * forever_rows
    for ordinal, class_key in enumerate(classes, start=1):
        task_id = str(uuid4())
        result = encode_json_value(
            {'err': {'code': 'FINAL_FAILURE', 'ordinal': ordinal}}
        )
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.history_aggregate (
                    task_id, task_name, queue_name, priority,
                    command_fingerprint_version, command_fingerprint, status,
                    terminalization_kind, terminal_at, retention_anchor_at,
                    retention_class_key, enqueued_at, created_at,
                    result_envelope_version, result_codec, result_content_type,
                    result_payload, result_digest, error_code,
                    final_failed_reason, retry_count, max_retries,
                    rerun_of_task_id, rerun_root_task_id,
                    rerun_input_version, rerun_input_codec,
                    rerun_input_content_type, rerun_input_form,
                    rerun_input_digest, rerun_input_inline,
                    is_workflow_task, history_schema_version,
                    attempt_archive_version, attempt_snapshot_codec,
                    attempt_snapshot_content_type, attempt_snapshot,
                    attempt_snapshot_digest
                ) VALUES (
                    :task_id, 'prototype.replacement', 'default', 100,
                    1, decode(repeat('ab', 32), 'hex'), 'FAILED',
                    'FAIL_RUNNING', :terminal_at, :terminal_at,
                    :class_key, :terminal_at, :terminal_at,
                    :version, :codec, 'application/json',
                    :result_payload, :result_digest,
                    'FINAL_FAILURE', 'final worker failure', 1, 1,
                    :source_task_id, :root_task_id,
                    :version, :codec, 'application/json', 'INLINE',
                    :rerun_input_digest, :rerun_input, FALSE,
                    :version, :version, :codec, 'application/json',
                    :attempt_snapshot, :attempt_digest
                )
                """
            ),
            {
                'task_id': task_id,
                'terminal_at': terminal_at,
                'class_key': class_key,
                'version': ARCHIVE_VERSION,
                'codec': ARCHIVE_CODEC,
                'result_payload': result.payload,
                'result_digest': result.digest,
                'source_task_id': source_task_id,
                'root_task_id': root_task_id,
                'rerun_input_digest': rerun_input.digest,
                'rerun_input': rerun_input.inline_payload,
                'attempt_snapshot': attempt_snapshot.payload,
                'attempt_digest': attempt_snapshot.digest,
            },
        )
        seeded.append(
            _SeededRow(
                task_id=task_id,
                result_payload=result.payload,
                result_digest=result.digest,
                attempt_payload=attempt_snapshot.payload,
                attempt_digest=attempt_snapshot.digest,
            )
        )
    await connection.commit()
    return tuple(seeded)


async def _copy_all(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    batch_size: int,
) -> tuple[ReplacementCopyBatch, ...]:
    batches: list[ReplacementCopyBatch] = []
    while True:
        result = await run_replacement_copy_batch(
            connection,
            schema,
            job_id=job_id,
            batch_size=batch_size,
        )
        await connection.commit()
        match result:
            case ReplacementCopyBatch():
                batches.append(result)
            case ReplacementReadyForVerification():
                return tuple(batches)
            case ReplacementCopyRejected():
                raise AssertionError(f'replacement copy rejected: {result!r}')


async def _run_complete(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    component: ArchiveComponent,
    source_version: int = 1,
    target_version: int = 2,
    batch_size: int = 2,
) -> tuple[ReplacementTranscodePlan, str]:
    job_id = str(uuid4())
    planned = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=component,
        source_version=source_version,
        target_version=target_version,
    )
    assert isinstance(planned, ReplacementTranscodePlan)
    await connection.commit()
    await _copy_all(connection, schema, job_id=job_id, batch_size=batch_size)
    verification = await verify_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
    )
    assert verification.verified is True
    assert verification.source_relations_changed == 0
    assert verification.replacement_row_mismatches == 0
    await connection.commit()
    swap = await swap_verified_replacement_partitions(
        connection,
        schema,
        job_id=job_id,
    )
    assert swap.relations_swapped == planned.relation_count
    await connection.commit()
    final = await finalize_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
    )
    assert final.verified is True
    assert final.source_rows_remaining_after_swap == 0
    assert final.wal_bytes is not None
    await connection.commit()
    return planned, job_id


@pytest.mark.parametrize('component', tuple(ArchiveComponent))
async def test_replacement_copy_verifies_swaps_and_retires_decoder(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
    component: ArchiveComponent,
) -> None:
    connection, schema, _ = replacement_schema
    seeded = await _seed_history(
        connection,
        schema,
        finite_rows=2,
        forever_rows=2,
    )
    identity_before = (
        await connection.execute(
            text(
                f"""
                SELECT task_id, task_name, status, retention_class_key,
                       terminal_at, rerun_of_task_id, rerun_root_task_id
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()

    plan, job_id = await _run_complete(
        connection,
        schema,
        component=component,
    )

    assert plan.transformed_rows == len(seeded)
    assert plan.copied_rows == len(seeded)
    assert plan.relation_count == 2
    identity_after = (
        await connection.execute(
            text(
                f"""
                SELECT task_id, task_name, status, retention_class_key,
                       terminal_at, rerun_of_task_id, rerun_root_task_id
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert [tuple(row) for row in identity_after] == [
        tuple(row) for row in identity_before
    ]
    retirement = await replacement_decoder_retirement_status(
        connection,
        schema,
        component=component,
        version=1,
    )
    assert retirement.ready is True
    assert (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id AND state = 'COMPLETE'
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one() == 2
    indexes = (
        await connection.execute(
            text(
                f"""
                SELECT relations.source_relation_name,
                       count(indexes.indexrelid) AS index_count,
                       bool_and(
                           indexes.indisvalid
                           AND pg_get_indexdef(indexes.indexrelid)
                               LIKE '%(task_id)%'
                       ) AS valid_task_id_index
                FROM {schema.sql}.archive_replacement_relations AS relations
                JOIN pg_class AS leaves
                  ON leaves.relname = relations.source_relation_name
                JOIN pg_namespace AS namespaces
                  ON namespaces.oid = leaves.relnamespace
                 AND namespaces.nspname = :schema_name
                LEFT JOIN pg_index AS indexes
                  ON indexes.indrelid = leaves.oid
                WHERE relations.job_id = :job_id
                GROUP BY relations.source_relation_name
                ORDER BY relations.source_relation_name
                """
            ),
            {'job_id': job_id, 'schema_name': schema.name},
        )
    ).all()
    assert len(indexes) == 2
    assert all(row.index_count == 1 for row in indexes)
    assert all(row.valid_task_id_index for row in indexes)


async def test_copy_resumes_from_durable_cursor_across_connections(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
    engine: AsyncEngine,
) -> None:
    connection, schema, _ = replacement_schema
    await _seed_history(connection, schema, finite_rows=3, forever_rows=3)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    first = await run_replacement_copy_batch(
        connection,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    assert isinstance(first, ReplacementCopyBatch)
    assert first.rows_copied == 1
    await connection.commit()

    async with engine.connect() as resumed:
        batches = await _copy_all(
            resumed,
            schema,
            job_id=job_id,
            batch_size=1,
        )
        assert sum(batch.rows_copied for batch in batches) == 5
        verification = await verify_replacement_archive_transcode(
            resumed,
            schema,
            job_id=job_id,
        )
        assert verification.verified is True
        await resumed.commit()


async def test_mixed_version_leaf_counts_physical_copy_and_logical_transform(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    seeded = await _seed_history(connection, schema, finite_rows=2, forever_rows=0)
    target_payload = ARCHIVE_FRAME_V2 + seeded[0].result_payload
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_envelope_version = 2,
                result_codec = :target_codec,
                result_payload = :target_payload,
                result_digest = sha256(:target_payload)
            WHERE task_id = :task_id
            """
        ),
        {
            'target_codec': ARCHIVE_CODEC_V2,
            'target_payload': target_payload,
            'task_id': seeded[0].task_id,
        },
    )
    await connection.commit()
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    assert plan.transformed_rows == 1
    assert plan.copied_rows == 2
    await connection.commit()
    batches = await _copy_all(
        connection,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    assert sum(batch.rows_copied for batch in batches) == 2
    assert sum(batch.transformed_rows for batch in batches) == 1
    assert (
        await verify_replacement_archive_transcode(
            connection,
            schema,
            job_id=job_id,
        )
    ).verified


async def test_uncommitted_copy_batch_rolls_back_data_and_cursor(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    await _seed_history(connection, schema, finite_rows=2, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    first = await run_replacement_copy_batch(
        connection,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    assert isinstance(first, ReplacementCopyBatch)
    await connection.rollback()

    replay = await run_replacement_copy_batch(
        connection,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    assert isinstance(replay, ReplacementCopyBatch)
    assert replay.batch_number == first.batch_number
    assert replay.copied_rows_completed == first.copied_rows_completed


async def test_source_set_change_before_copy_is_rejected_without_replacement(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    seeded = await _seed_history(connection, schema, finite_rows=2, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    await connection.execute(
        text(f'DELETE FROM {schema.sql}.history_aggregate WHERE task_id = :task_id'),
        {'task_id': seeded[0].task_id},
    )
    await connection.commit()

    result = await run_replacement_copy_batch(
        connection,
        schema,
        job_id=job_id,
        batch_size=10,
    )
    assert result == ReplacementCopyRejected(
        job_id=job_id,
        relation_ordinal=1,
        kind=ReplacementCopyRejectionKind.SOURCE_SET_CHANGED,
        observed_rows=1,
    )
    assert (
        await connection.execute(
            text(
                f"""
                SELECT to_regclass(
                    :replacement_name
                ) IS NULL
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id AND relation_ordinal = 1
                """
            ),
            {
                'job_id': job_id,
                'replacement_name': (
                    f'{schema.name}.archive_replacement_'
                    f'{job_id.replace("-", "")[:12]}_1'
                ),
            },
        )
    ).scalar_one() is True


async def test_source_corruption_after_plan_is_rejected_before_copy(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    seeded = await _seed_history(connection, schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_digest = decode(repeat('00', 32), 'hex')
            WHERE task_id = :task_id
            """
        ),
        {'task_id': seeded[0].task_id},
    )
    await connection.commit()

    result = await run_replacement_copy_batch(
        connection,
        schema,
        job_id=job_id,
        batch_size=10,
    )
    assert result == ReplacementCopyRejected(
        job_id=job_id,
        relation_ordinal=1,
        kind=ReplacementCopyRejectionKind.SOURCE_CORRUPT,
        observed_rows=1,
    )


async def test_replacement_mutation_blocks_verification_and_swap(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    await _seed_history(connection, schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    await _copy_all(connection, schema, job_id=job_id, batch_size=10)
    relation_name = (
        await connection.execute(
            text(
                f"""
                SELECT replacement_relation_name
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one()
    await connection.execute(
        text(
            f'UPDATE {schema.sql}."{relation_name}" '
            "SET task_name = 'corrupt.replacement'"
        )
    )
    await connection.commit()

    verification = await verify_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
    )
    assert verification.verified is False
    assert verification.replacement_row_mismatches == 1
    with pytest.raises(RuntimeError, match='changed before binding swap'):
        await swap_verified_replacement_partitions(
            connection,
            schema,
            job_id=job_id,
        )


async def test_source_mutation_after_verification_blocks_locked_swap(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    seeded = await _seed_history(connection, schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    await _copy_all(connection, schema, job_id=job_id, batch_size=10)
    assert (
        await verify_replacement_archive_transcode(
            connection,
            schema,
            job_id=job_id,
        )
    ).verified
    await connection.commit()
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET task_name = 'changed.after.verification'
            WHERE task_id = :task_id
            """
        ),
        {'task_id': seeded[0].task_id},
    )
    await connection.commit()

    with pytest.raises(RuntimeError, match='changed before binding swap'):
        await swap_verified_replacement_partitions(
            connection,
            schema,
            job_id=job_id,
        )
    await connection.rollback()
    assert (
        await connection.execute(
            text(f'SELECT count(*) FROM {schema.sql}.history_aggregate')
        )
    ).scalar_one() == 1


async def test_binding_swap_is_atomic_and_replayable_after_commit(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    await _seed_history(connection, schema, finite_rows=1, forever_rows=1)
    job_id = str(uuid4())
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    await connection.commit()
    await _copy_all(connection, schema, job_id=job_id, batch_size=10)
    assert (
        await verify_replacement_archive_transcode(
            connection,
            schema,
            job_id=job_id,
        )
    ).verified
    await connection.commit()
    original_bound = (
        await connection.execute(
            text(
                f"""
                SELECT partition_bound
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id AND relation_ordinal = 2
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one()
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_relations
            SET partition_bound = 'FOR VALUES IN ('
            WHERE job_id = :job_id AND relation_ordinal = 2
            """
        ),
        {'job_id': job_id},
    )
    await connection.commit()
    with pytest.raises(DBAPIError):
        await swap_verified_replacement_partitions(
            connection,
            schema,
            job_id=job_id,
        )
    await connection.rollback()
    assert (
        await connection.execute(
            text(
                f"""
                SELECT state
                FROM {schema.sql}.archive_replacement_jobs
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one() == 'VERIFIED'
    assert (
        await connection.execute(
            text(f'SELECT count(*) FROM {schema.sql}.history_aggregate')
        )
    ).scalar_one() == 2
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_relations
            SET partition_bound = :bound
            WHERE job_id = :job_id AND relation_ordinal = 2
            """
        ),
        {'job_id': job_id, 'bound': original_bound},
    )
    await connection.commit()
    first = await swap_verified_replacement_partitions(
        connection,
        schema,
        job_id=job_id,
    )
    await connection.commit()
    replay = await swap_verified_replacement_partitions(
        connection,
        schema,
        job_id=job_id,
    )
    assert replay == first


@pytest.mark.parametrize('component', tuple(ArchiveComponent))
async def test_reverse_replacement_transcode_restores_exact_archive_rows(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
    component: ArchiveComponent,
) -> None:
    connection, schema, _ = replacement_schema
    await _seed_history(connection, schema, finite_rows=1, forever_rows=1)
    originals = (
        await connection.execute(
            text(
                f"""
                SELECT *
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    await _run_complete(
        connection,
        schema,
        component=component,
    )
    await _run_complete(
        connection,
        schema,
        component=component,
        source_version=2,
        target_version=1,
    )

    restored = (
        await connection.execute(
            text(
                f"""
                SELECT *
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert [tuple(row) for row in restored] == [tuple(row) for row in originals]


async def test_swapped_leaf_enforces_parent_constraints(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, _ = replacement_schema
    seeded = await _seed_history(connection, schema, finite_rows=1, forever_rows=0)
    await _run_complete(
        connection,
        schema,
        component=ArchiveComponent.RESULT,
    )
    with pytest.raises(DBAPIError):
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.history_aggregate
                SET priority = 101
                WHERE task_id = :task_id
                """
            ),
            {'task_id': seeded[0].task_id},
        )
    await connection.rollback()


async def test_maintenance_cannot_finish_until_replacement_job_completes(
    replacement_schema: tuple[AsyncConnection, PrototypeSchema, str],
) -> None:
    connection, schema, maintenance_id = replacement_schema
    await _seed_history(connection, schema, finite_rows=1, forever_rows=0)
    plan = await plan_replacement_archive_transcode(
        connection,
        schema,
        job_id=str(uuid4()),
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, ReplacementTranscodePlan)
    with pytest.raises(ValueError, match='unfinished replacement job'):
        await finish_replacement_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
