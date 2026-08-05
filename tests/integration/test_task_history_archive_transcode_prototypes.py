"""Correctness gates for disposable offline archive transcoding."""

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
    ArchiveDomain,
    DecodedArchiveValue,
    archive_digest,
    decode_attempts,
    decode_json_value,
    decode_rerun_input,
    encode_attempts,
    encode_json_value,
    prototype_attempts,
    store_inline_rerun_input,
    store_referenced_rerun_input,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import (
    ARCHIVE_CODEC_V2,
    ARCHIVE_FRAME_V2,
    ArchiveComponent,
    TranscodeBatch,
    TranscodeBatchRejected,
    TranscodeBatchRejectionKind,
    TranscodePlan,
    TranscodeReadyForVerification,
    TranscodeRejected,
    TranscodeRejectionKind,
    begin_archive_maintenance,
    decoder_retirement_status,
    finish_archive_maintenance,
    install_archive_transcode_prototype,
    inventory_archive_versions,
    plan_archive_transcode,
    run_archive_transcode_batch,
    verify_archive_transcode,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(frozen=True, slots=True)
class _SeededHistory:
    task_id: str
    result_payload: bytes
    result_digest: bytes
    attempt_payload: bytes
    attempt_digest: bytes


@pytest_asyncio.fixture
async def transcode_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[AsyncConnection]:
    schema = PrototypeSchema(f'history_transcode_{uuid4().hex[:12]}')
    connection = await engine.connect()
    try:
        await install_archive_candidates(connection, schema)
        await install_archive_transcode_prototype(connection, schema)
        maintenance = await begin_archive_maintenance(
            connection,
            schema,
            maintenance_id=str(uuid4()),
        )
        await connection.commit()
        connection.info['task_history_schema'] = schema
        connection.info['archive_maintenance_id'] = maintenance.maintenance_id
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


def _maintenance_id(connection: AsyncConnection) -> str:
    maintenance_id = connection.info.get('archive_maintenance_id')
    assert isinstance(maintenance_id, str)
    return maintenance_id


async def _seed_history(
    connection: AsyncConnection,
    *,
    finite_rows: int,
    forever_rows: int,
) -> tuple[_SeededHistory, ...]:
    schema = _schema(connection)
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
    seeded: list[_SeededHistory] = []
    for ordinal, class_key in enumerate(
        ('finite_30d_v1',) * finite_rows + ('forever',) * forever_rows,
        start=1,
    ):
        task_id = str(uuid4())
        result = encode_json_value(
            {'err': {'code': 'FINAL_FAILURE', 'ordinal': ordinal}}
        )
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.history_aggregate (
                    task_id, task_name, queue_name, priority, status,
                    terminalization_kind, terminal_at, retention_anchor_at,
                    retention_class_key, enqueued_at, created_at,
                    result_envelope_version, result_codec, result_payload,
                    result_digest, error_code, final_failed_reason,
                    retry_count, rerun_of_task_id, rerun_root_task_id,
                    rerun_input_version, rerun_input_codec,
                    rerun_input_form, rerun_input_digest,
                    rerun_input_inline, is_workflow_task,
                    history_schema_version, attempt_archive_version,
                    attempt_snapshot_codec, attempt_snapshot,
                    attempt_snapshot_digest
                ) VALUES (
                    :task_id, 'prototype.transcode', 'default', 100, 'FAILED',
                    'FAIL_LOCKED', :terminal_at, :terminal_at,
                    :class_key, :terminal_at, :terminal_at,
                    :version, :codec, :result_payload, :result_digest,
                    'FINAL_FAILURE', 'final worker failure', 1,
                    :source_task_id, :root_task_id,
                    :version, :codec, 'INLINE', :rerun_input_digest,
                    :rerun_input, FALSE,
                    :version, :version, :codec, :attempt_snapshot,
                    :attempt_digest
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
            _SeededHistory(
                task_id=task_id,
                result_payload=result.payload,
                result_digest=result.digest,
                attempt_payload=attempt_snapshot.payload,
                attempt_digest=attempt_snapshot.digest,
            )
        )
    await connection.commit()
    return tuple(seeded)


async def _run_to_completion(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    batch_size: int,
) -> None:
    while True:
        batch = await run_archive_transcode_batch(
            connection,
            schema,
            job_id=job_id,
            batch_size=batch_size,
        )
        assert isinstance(batch, TranscodeBatch)
        await connection.commit()
        if batch.rows_completed == batch.rows_total:
            return


async def test_dry_run_inventories_finite_and_forever_without_mutation(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=2, forever_rows=2)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    assert plan.affected_rows == 4
    assert plan.payload_rows == 4
    assert plan.relation_count == 2
    assert plan.projected_payload_bytes == plan.payload_bytes + 8
    assert plan.affected_relation_bytes > 0
    assert plan.peak_additional_disk_budget_bytes >= (
        plan.affected_relation_bytes * 5 / 4
    )
    assert plan.wal_budget_bytes >= plan.affected_relation_bytes * 3 / 2
    assert plan.rewrite_duration_limit_seconds == pytest.approx(4 / 20_000)
    assert plan.rollback_rows == 4
    assert plan.rollback_payload_bytes == plan.projected_payload_bytes
    assert (
        plan.rollback_peak_additional_disk_budget_bytes
        == plan.peak_additional_disk_budget_bytes
    )
    assert plan.rollback_wal_budget_bytes == plan.wal_budget_bytes
    assert plan.rollback_duration_limit_seconds == plan.rewrite_duration_limit_seconds
    inventory = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT relation_name, row_count
                FROM {schema.sql}.archive_transcode_inventory
                WHERE job_id = :job_id
                ORDER BY relation_name
                """
            ),
            {'job_id': job_id},
        )
    ).all()
    assert len(inventory) == 2
    assert sum(row.row_count for row in inventory) == 4
    assert any('finite_2026_08_05' in row.relation_name for row in inventory)
    assert any('forever' in row.relation_name for row in inventory)
    versions = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, result_envelope_version
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert {row.task_id for row in versions} == {row.task_id for row in seeded}
    assert {row.result_envelope_version for row in versions} == {1}


async def test_result_transcode_includes_named_administrative_prior_result(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    prior = seeded[0]
    await transcode_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET status = 'CANCELLED',
                terminalization_kind = 'CANCEL_ADMIN',
                prior_result_payload = result_payload,
                result_payload = NULL
            WHERE task_id = :task_id
            """
        ),
        {'task_id': prior.task_id},
    )
    await transcode_schema.commit()
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    assert (plan.affected_rows, plan.payload_rows) == (2, 2)
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    verified = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert verified.job_id == job_id
    row = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT result_envelope_version, result_codec,
                       result_payload, prior_result_payload, result_digest
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': prior.task_id},
        )
    ).one()
    assert row.result_payload is None
    assert bytes(row.prior_result_payload).startswith(ARCHIVE_FRAME_V2)
    decoded = decode_json_value(
        domain=ArchiveDomain.RESULT,
        version=row.result_envelope_version,
        codec=row.result_codec,
        payload=bytes(row.prior_result_payload),
        digest=bytes(row.result_digest),
    )
    assert isinstance(decoded, DecodedArchiveValue)


async def test_version_inventory_covers_all_domains_and_unknown_values(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    await transcode_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_envelope_version = 99,
                result_codec = 'unknown'
            WHERE task_id = :task_id
            """
        ),
        {'task_id': seeded[0].task_id},
    )
    inventory = await inventory_archive_versions(transcode_schema, schema)
    assert {entry.component for entry in inventory} == set(ArchiveComponent)
    result_inventory = {
        entry.version: entry
        for entry in inventory
        if entry.component is ArchiveComponent.RESULT
    }
    assert result_inventory[1].affected_rows == 1
    assert result_inventory[1].invalid_rows == 0
    assert result_inventory[99].codec == 'unknown'
    assert result_inventory[99].affected_rows == 1
    assert result_inventory[99].invalid_rows == 1
    history = next(
        entry for entry in inventory if entry.component is ArchiveComponent.HISTORY_ROW
    )
    assert history.affected_rows == 2
    assert history.payload_rows == 0
    assert history.payload_bytes == 0
    assert history.relation_count == 2


async def test_batches_resume_and_preserve_identity_across_connections(
    transcode_schema: AsyncConnection,
    engine: AsyncEngine,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=2, forever_rows=2)
    identity_before = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, task_name, status, retention_class_key,
                       terminal_at, rerun_of_task_id, rerun_root_task_id,
                       attempt_snapshot_digest
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    await transcode_schema.rollback()
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    first = await run_archive_transcode_batch(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=2,
    )
    assert isinstance(first, TranscodeBatch)
    assert (first.rows_rewritten, first.rows_completed) == (2, 2)
    await transcode_schema.commit()

    async with engine.connect() as resumed:
        second = await run_archive_transcode_batch(
            resumed,
            schema,
            job_id=job_id,
            batch_size=2,
        )
        assert isinstance(second, TranscodeBatch)
        assert (second.rows_rewritten, second.rows_completed) == (2, 4)
        await resumed.commit()
        verification = await verify_archive_transcode(
            resumed,
            schema,
            job_id=job_id,
        )
        assert verification.verified is True
        assert verification.source_rows_remaining == 0
        assert verification.invalid_target_rows == 0
        assert verification.wal_bytes >= 0
        await resumed.commit()

    identity_after = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, task_name, status, retention_class_key,
                       terminal_at, rerun_of_task_id, rerun_root_task_id,
                       attempt_snapshot_digest
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert [tuple(row) for row in identity_after] == [
        tuple(row) for row in identity_before
    ]
    values = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, result_envelope_version, result_codec,
                       result_payload, result_digest,
                       {schema.sql}.archive_component_value_is_valid(
                           'RESULT', result_envelope_version, result_codec,
                           result_payload, result_digest, NULL, NULL
                       ) AS valid
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert {row.task_id for row in values} == {row.task_id for row in seeded}
    assert all(row.result_codec == ARCHIVE_CODEC_V2 for row in values)
    assert all(bytes(row.result_payload).startswith(ARCHIVE_FRAME_V2) for row in values)
    assert all(row.valid for row in values)
    for row in values:
        decoded = decode_json_value(
            domain=ArchiveDomain.RESULT,
            version=row.result_envelope_version,
            codec=row.result_codec,
            payload=bytes(row.result_payload),
            digest=bytes(row.result_digest),
        )
        assert isinstance(decoded, DecodedArchiveValue)
    retirement = await decoder_retirement_status(
        transcode_schema,
        schema,
        component=ArchiveComponent.RESULT,
        version=1,
    )
    assert retirement.ready is True


async def test_uncommitted_plan_rolls_back_and_can_be_replanned(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    first = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(first, TranscodePlan)
    await transcode_schema.rollback()
    assert (
        await transcode_schema.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.archive_transcode_jobs
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one() == 0

    replanned = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(replanned, TranscodePlan)
    assert replanned.affected_rows == first.affected_rows


async def test_reverse_transcode_restores_exact_source_bytes_and_digests(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    originals = {row.task_id: (row.result_payload, row.result_digest) for row in seeded}
    forward_id = str(uuid4())
    forward = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=forward_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(forward, TranscodePlan)
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=forward_id,
        batch_size=10,
    )
    assert (
        await verify_archive_transcode(
            transcode_schema,
            schema,
            job_id=forward_id,
        )
    ).verified
    await transcode_schema.commit()

    reverse_id = str(uuid4())
    reverse = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=reverse_id,
        component=ArchiveComponent.RESULT,
        source_version=2,
        target_version=1,
    )
    assert isinstance(reverse, TranscodePlan)
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=reverse_id,
        batch_size=1,
    )
    assert (
        await verify_archive_transcode(
            transcode_schema,
            schema,
            job_id=reverse_id,
        )
    ).verified
    restored = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, result_payload, result_digest
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert {
        row.task_id: (bytes(row.result_payload), bytes(row.result_digest))
        for row in restored
    } == originals


async def test_attempt_snapshot_transcode_preserves_ordered_attempts(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.ATTEMPTS,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    assert (
        await verify_archive_transcode(
            transcode_schema,
            schema,
            job_id=job_id,
        )
    ).verified
    rows = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT attempt_snapshot, attempt_snapshot_digest,
                       result_envelope_version
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert all(row.result_envelope_version == 1 for row in rows)
    for row in rows:
        framed = bytes(row.attempt_snapshot)
        assert framed.startswith(ARCHIVE_FRAME_V2)
        decoded = decode_attempts(
            version=2,
            codec=ARCHIVE_CODEC_V2,
            payload=framed,
            digest=bytes(row.attempt_snapshot_digest),
        )
        assert isinstance(decoded, DecodedArchiveValue)
        assert [attempt.attempt for attempt in decoded.value] == [1, 2]


async def test_corrupt_source_rejects_plan_without_creating_job(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    invalid_payload = b'not-json'
    await transcode_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_payload = :payload, result_digest = sha256(:payload)
            WHERE task_id = :task_id
            """
        ),
        {'payload': invalid_payload, 'task_id': seeded[0].task_id},
    )
    outcome = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=str(uuid4()),
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert outcome == TranscodeRejected(
        TranscodeRejectionKind.SOURCE_CORRUPT,
        1,
    )
    assert (
        await transcode_schema.execute(
            text(f'SELECT count(*) FROM {schema.sql}.archive_transcode_jobs')
        )
    ).scalar_one() == 0


async def test_batch_failure_rolls_back_payload_version_and_progress(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    await transcode_schema.execute(
        text(
            f"""
            CREATE FUNCTION {schema.sql}.reject_result_transcode()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                IF NEW.result_envelope_version = 2 THEN
                    RAISE EXCEPTION 'result transcode disabled by test';
                END IF;
                RETURN NEW;
            END
            $function$
            """
        )
    )
    await transcode_schema.execute(
        text(
            f"""
            CREATE TRIGGER reject_result_transcode
            BEFORE UPDATE ON {schema.sql}.history_aggregate_finite_2026_08_05
            FOR EACH ROW EXECUTE FUNCTION {schema.sql}.reject_result_transcode()
            """
        )
    )
    await transcode_schema.commit()

    with pytest.raises(DBAPIError, match='result transcode disabled by test'):
        async with transcode_schema.begin_nested():
            await run_archive_transcode_batch(
                transcode_schema,
                schema,
                job_id=job_id,
                batch_size=10,
            )
    versions = (
        (
            await transcode_schema.execute(
                text(
                    f"""
                SELECT result_envelope_version
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
                )
            )
        )
        .scalars()
        .all()
    )
    progress = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT rows_completed
                FROM {schema.sql}.archive_transcode_jobs
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one()
    assert versions == [1, 1]
    assert progress == 0


async def test_progress_failure_rolls_back_payload_and_batch_record(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    await transcode_schema.execute(
        text(
            f"""
            CREATE FUNCTION {schema.sql}.reject_transcode_progress()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                IF NEW.rows_completed > OLD.rows_completed THEN
                    RAISE EXCEPTION 'transcode progress disabled by test';
                END IF;
                RETURN NEW;
            END
            $function$
            """
        )
    )
    await transcode_schema.execute(
        text(
            f"""
            CREATE TRIGGER reject_transcode_progress
            BEFORE UPDATE ON {schema.sql}.archive_transcode_jobs
            FOR EACH ROW EXECUTE FUNCTION
                {schema.sql}.reject_transcode_progress()
            """
        )
    )
    await transcode_schema.commit()

    with pytest.raises(DBAPIError, match='transcode progress disabled by test'):
        async with transcode_schema.begin_nested():
            await run_archive_transcode_batch(
                transcode_schema,
                schema,
                job_id=job_id,
                batch_size=1,
            )
    state = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT h.result_envelope_version, j.rows_completed,
                       (SELECT count(*)
                        FROM {schema.sql}.archive_transcode_batches
                        WHERE job_id = :job_id) AS batches
                FROM {schema.sql}.history_aggregate AS h
                CROSS JOIN {schema.sql}.archive_transcode_jobs AS j
                WHERE j.job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).one()
    assert tuple(state) == (1, 0, 0)


async def test_source_corruption_after_planning_blocks_batch_without_mutation(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    assert isinstance(
        await plan_archive_transcode(
            transcode_schema,
            schema,
            job_id=job_id,
            component=ArchiveComponent.RESULT,
            source_version=1,
            target_version=2,
        ),
        TranscodePlan,
    )
    await transcode_schema.commit()
    await transcode_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET result_digest = decode(repeat('00', 32), 'hex')
            WHERE task_id = :task_id
            """
        ),
        {'task_id': seeded[0].task_id},
    )
    outcome = await run_archive_transcode_batch(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=10,
    )
    assert outcome == TranscodeBatchRejected(
        job_id=job_id,
        kind=TranscodeBatchRejectionKind.SOURCE_CORRUPT,
        observed_rows=1,
    )
    version = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT result_envelope_version
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': seeded[0].task_id},
        )
    ).scalar_one()
    assert version == 1


async def test_referenced_rerun_input_is_transcoded_and_blocks_decoder_retirement(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    reference = 'payload://tenant-a/object-1'
    referenced = store_referenced_rerun_input(
        reference=reference,
        payload=b'payload held outside PostgreSQL',
    )
    await transcode_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET rerun_input_form = 'REFERENCE',
                rerun_input_digest = :digest,
                rerun_input_inline = NULL,
                rerun_input_reference = :reference
            WHERE task_id = :task_id
            """
        ),
        {
            'digest': referenced.digest,
            'reference': reference,
            'task_id': seeded[0].task_id,
        },
    )
    await transcode_schema.commit()
    originals = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, rerun_input_version, rerun_input_codec,
                       rerun_input_form, rerun_input_digest,
                       rerun_input_inline, rerun_input_reference
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()

    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RERUN_INPUT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    assert plan.affected_rows == 2
    assert plan.payload_rows == 1
    assert plan.projected_payload_bytes == plan.payload_bytes + 2
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    verification = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert verification.verified
    rows = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, rerun_input_version, rerun_input_codec,
                       rerun_input_form, rerun_input_digest,
                       rerun_input_inline, rerun_input_reference,
                       {schema.sql}.archive_component_value_is_valid(
                           'RERUN_INPUT', rerun_input_version,
                           rerun_input_codec, rerun_input_inline,
                           rerun_input_digest, rerun_input_form,
                           rerun_input_reference
                       ) AS valid
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert all(row.rerun_input_version == 2 for row in rows)
    assert all(row.rerun_input_codec == ARCHIVE_CODEC_V2 for row in rows)
    assert all(row.valid for row in rows)
    for row in rows:
        decoded = decode_rerun_input(
            version=row.rerun_input_version,
            codec=row.rerun_input_codec,
            form=row.rerun_input_form,
            digest=bytes(row.rerun_input_digest),
            inline_payload=(
                bytes(row.rerun_input_inline)
                if row.rerun_input_inline is not None
                else None
            ),
            reference=row.rerun_input_reference,
        )
        assert isinstance(decoded, DecodedArchiveValue)
    reference_row = next(row for row in rows if row.rerun_input_form == 'REFERENCE')
    assert reference_row.rerun_input_inline is None
    assert reference_row.rerun_input_reference == reference
    assert bytes(reference_row.rerun_input_digest) == referenced.digest
    inline_row = next(row for row in rows if row.rerun_input_form == 'INLINE')
    assert bytes(inline_row.rerun_input_inline).startswith(ARCHIVE_FRAME_V2)
    retirement = await decoder_retirement_status(
        transcode_schema,
        schema,
        component=ArchiveComponent.RERUN_INPUT,
        version=1,
    )
    assert retirement.ready
    await transcode_schema.commit()

    reverse_id = str(uuid4())
    reverse = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=reverse_id,
        component=ArchiveComponent.RERUN_INPUT,
        source_version=2,
        target_version=1,
    )
    assert isinstance(reverse, TranscodePlan)
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=reverse_id,
        batch_size=2,
    )
    assert (
        await verify_archive_transcode(
            transcode_schema,
            schema,
            job_id=reverse_id,
        )
    ).verified
    restored = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, rerun_input_version, rerun_input_codec,
                       rerun_input_form, rerun_input_digest,
                       rerun_input_inline, rerun_input_reference
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert [tuple(row) for row in restored] == [tuple(row) for row in originals]


async def test_source_set_change_after_plan_rejects_batch_before_mutation(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    await _seed_history(transcode_schema, finite_rows=0, forever_rows=1)

    outcome = await run_archive_transcode_batch(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=10,
    )
    assert outcome == TranscodeBatchRejected(
        job_id=job_id,
        kind=TranscodeBatchRejectionKind.SOURCE_SET_CHANGED,
        observed_rows=2,
    )
    versions = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT result_envelope_version
                FROM {schema.sql}.history_aggregate
                """
            )
        )
    ).scalars()
    assert list(versions) == [1, 1]


async def test_verification_replays_after_uncertain_commit(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    ready = await run_archive_transcode_batch(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    assert ready == TranscodeReadyForVerification(job_id=job_id, rows_total=1)
    assert (
        await transcode_schema.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.archive_transcode_batches
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one() == 1
    first = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert first.verified
    await transcode_schema.commit()

    replayed = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert replayed == first
    with pytest.raises(ValueError, match='verified transcode job is immutable'):
        await run_archive_transcode_batch(
            transcode_schema,
            schema,
            job_id=job_id,
            batch_size=1,
        )


@pytest.mark.parametrize(
    'malformed',
    [
        b'[{"attempt":1}]',
        b'[[1,"FAILED",false,1.5,2,null,null,null,null,null,null,null]]',
    ],
)
async def test_malformed_attempt_record_rejects_preflight(
    transcode_schema: AsyncConnection,
    malformed: bytes,
) -> None:
    schema = _schema(transcode_schema)
    seeded = await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    await transcode_schema.execute(
        text(
            f"""
            UPDATE {schema.sql}.history_aggregate
            SET attempt_snapshot = :payload,
                attempt_snapshot_digest = :digest
            WHERE task_id = :task_id
            """
        ),
        {
            'payload': malformed,
            'digest': archive_digest(malformed),
            'task_id': seeded[0].task_id,
        },
    )
    outcome = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=str(uuid4()),
        component=ArchiveComponent.ATTEMPTS,
        source_version=1,
        target_version=2,
    )
    assert outcome == TranscodeRejected(
        kind=TranscodeRejectionKind.SOURCE_CORRUPT,
        affected_rows=1,
    )


async def test_only_one_transcode_job_can_be_active_across_components(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    first = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=str(uuid4()),
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(first, TranscodePlan)
    second = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=str(uuid4()),
        component=ArchiveComponent.ATTEMPTS,
        source_version=1,
        target_version=2,
    )
    assert second == TranscodeRejected(
        kind=TranscodeRejectionKind.ACTIVE_JOB,
        affected_rows=1,
    )


async def test_history_row_schema_version_transcodes_without_payload_rewrite(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=1)
    before = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, task_name, terminal_at, result_payload,
                       result_digest, attempt_snapshot,
                       attempt_snapshot_digest, rerun_input_inline,
                       rerun_input_digest
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.HISTORY_ROW,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    assert plan.affected_rows == 2
    assert plan.payload_rows == 0
    assert plan.payload_bytes == 0
    assert plan.projected_payload_bytes == 0
    await transcode_schema.commit()
    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    verification = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert verification.verified
    after = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT task_id, task_name, terminal_at, result_payload,
                       result_digest, attempt_snapshot,
                       attempt_snapshot_digest, rerun_input_inline,
                       rerun_input_digest
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).all()
    assert [tuple(row) for row in after] == [tuple(row) for row in before]
    versions = (
        await transcode_schema.execute(
            text(
                f"""
                SELECT history_schema_version
                FROM {schema.sql}.history_aggregate
                ORDER BY task_id
                """
            )
        )
    ).scalars()
    assert list(versions) == [2, 2]
    retirement = await decoder_retirement_status(
        transcode_schema,
        schema,
        component=ArchiveComponent.HISTORY_ROW,
        version=1,
    )
    assert retirement.ready


async def test_maintenance_session_bounds_planning_and_execution(
    transcode_schema: AsyncConnection,
) -> None:
    schema = _schema(transcode_schema)
    await _seed_history(transcode_schema, finite_rows=1, forever_rows=0)
    job_id = str(uuid4())
    plan = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
        component=ArchiveComponent.RESULT,
        source_version=1,
        target_version=2,
    )
    assert isinstance(plan, TranscodePlan)
    await transcode_schema.commit()
    with pytest.raises(
        ValueError,
        match='archive maintenance has an unfinished transcode job',
    ):
        await finish_archive_maintenance(
            transcode_schema,
            schema,
            maintenance_id=_maintenance_id(transcode_schema),
        )
    await transcode_schema.rollback()

    await _run_to_completion(
        transcode_schema,
        schema,
        job_id=job_id,
        batch_size=1,
    )
    verified = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert verified.verified
    await transcode_schema.commit()
    await finish_archive_maintenance(
        transcode_schema,
        schema,
        maintenance_id=_maintenance_id(transcode_schema),
    )
    await transcode_schema.commit()

    replayed = await verify_archive_transcode(
        transcode_schema,
        schema,
        job_id=job_id,
    )
    assert replayed == verified
    rejected = await plan_archive_transcode(
        transcode_schema,
        schema,
        job_id=str(uuid4()),
        component=ArchiveComponent.ATTEMPTS,
        source_version=1,
        target_version=2,
    )
    assert rejected == TranscodeRejected(
        kind=TranscodeRejectionKind.MAINTENANCE_REQUIRED,
        affected_rows=0,
    )
