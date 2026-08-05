"""Operational evidence for offline archive transcoding."""

from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.archive import (
    ARCHIVE_CODEC,
    ARCHIVE_VERSION,
    archive_digest,
    encode_attempts,
    prototype_attempts,
)
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_operational_conditions,
)
from tests.task_history_prototypes.measurements import (
    RelationFootprint,
    partition_tree_footprint,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import (
    TRANSCODE_MINIMUM_ROWS_PER_SECOND,
    ArchiveComponent,
    ArchiveVersionInventory,
    TranscodeBatch,
    TranscodePlan,
    TranscodeVerification,
    begin_archive_maintenance,
    decoder_retirement_status,
    finish_archive_maintenance,
    install_archive_transcode_prototype,
    inventory_archive_versions,
    plan_archive_transcode,
    run_archive_transcode_batch,
    verify_archive_transcode,
)


@dataclass(frozen=True, slots=True)
class ArchiveTranscodeEvidence:
    conditions: EvidenceConditions
    component: ArchiveComponent
    workload: dict[str, int]
    batches: int
    inventory_before: tuple[ArchiveVersionInventory, ...]
    plan: TranscodePlan
    footprint_before: RelationFootprint
    peak_footprint: RelationFootprint
    footprint_after: RelationFootprint
    peak_additional_bytes: int
    rewrite_seconds: float
    rows_per_second: float
    verification: TranscodeVerification
    decoder_retirement_ready: bool
    throughput_passed: bool
    wal_passed: bool
    peak_disk_passed: bool


async def collect_archive_transcode_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    component: ArchiveComponent,
    rows: int,
    batch_size: int,
    payload_bytes: int,
    attempts_per_task: int,
) -> ArchiveTranscodeEvidence:
    _validate_workload(
        run_kind=run_kind,
        rows=rows,
        batch_size=batch_size,
        payload_bytes=payload_bytes,
        attempts_per_task=attempts_per_task,
    )
    conditions = await collect_operational_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture=(
            'explicit checkpoint before planning; operational first-run '
            'rewrite with committed batches'
        ),
        prepared_posture='parameterized set-wise seed and bounded batch program',
    )
    schema = PrototypeSchema(f'history_transcode_evidence_{uuid4().hex[:8]}')
    await install_archive_candidates(connection, schema)
    await install_archive_transcode_prototype(connection, schema)
    await connection.commit()
    try:
        await _seed_history(
            connection,
            schema,
            rows=rows,
            payload_bytes=payload_bytes,
            attempts_per_task=attempts_per_task,
        )
        await connection.commit()
        await connection.execute(text('CHECKPOINT'))
        footprint_before = await partition_tree_footprint(
            connection,
            f'{schema.name}.history_aggregate',
        )
        maintenance_id = str(uuid4())
        await begin_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await connection.commit()
        inventory_before = await inventory_archive_versions(connection, schema)
        job_id = str(uuid4())
        planned = await plan_archive_transcode(
            connection,
            schema,
            job_id=job_id,
            component=component,
            source_version=1,
            target_version=2,
        )
        if not isinstance(planned, TranscodePlan):
            raise RuntimeError(f'transcode planning rejected evidence: {planned!r}')
        _verify_planned_workload(
            planned,
            inventory=inventory_before,
            component=component,
            rows=rows,
        )
        await connection.commit()

        peak_footprint = footprint_before
        batches = 0
        started = time.perf_counter()
        while True:
            batch = await run_archive_transcode_batch(
                connection,
                schema,
                job_id=job_id,
                batch_size=batch_size,
            )
            if not isinstance(batch, TranscodeBatch):
                raise RuntimeError(f'transcode batch rejected evidence: {batch!r}')
            await connection.commit()
            batches += 1
            observed = await partition_tree_footprint(
                connection,
                f'{schema.name}.history_aggregate',
            )
            if observed.total_bytes > peak_footprint.total_bytes:
                peak_footprint = observed
            if batch.rows_completed == batch.rows_total:
                break
        rewrite_seconds = time.perf_counter() - started
        verification = await verify_archive_transcode(
            connection,
            schema,
            job_id=job_id,
        )
        await connection.commit()
        retirement = await decoder_retirement_status(
            connection,
            schema,
            component=component,
            version=1,
        )
        footprint_after = await partition_tree_footprint(
            connection,
            f'{schema.name}.history_aggregate',
        )
        if footprint_after.total_bytes > peak_footprint.total_bytes:
            peak_footprint = footprint_after
        await finish_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await connection.commit()

        peak_additional_bytes = max(
            0,
            peak_footprint.total_bytes - footprint_before.total_bytes,
        )
        rows_per_second = rows / rewrite_seconds
        return ArchiveTranscodeEvidence(
            conditions=conditions,
            component=component,
            workload={
                'rows': rows,
                'batch_size': batch_size,
                'payload_bytes': payload_bytes,
                'attempts_per_task': attempts_per_task,
            },
            batches=batches,
            inventory_before=inventory_before,
            plan=planned,
            footprint_before=footprint_before,
            peak_footprint=peak_footprint,
            footprint_after=footprint_after,
            peak_additional_bytes=peak_additional_bytes,
            rewrite_seconds=rewrite_seconds,
            rows_per_second=rows_per_second,
            verification=verification,
            decoder_retirement_ready=retirement.ready,
            throughput_passed=(
                rows_per_second >= TRANSCODE_MINIMUM_ROWS_PER_SECOND
            ),
            wal_passed=verification.wal_bytes <= planned.wal_budget_bytes,
            peak_disk_passed=(
                peak_additional_bytes
                <= planned.peak_additional_disk_budget_bytes
            ),
        )
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()


def _verify_planned_workload(
    plan: TranscodePlan,
    *,
    inventory: tuple[ArchiveVersionInventory, ...],
    component: ArchiveComponent,
    rows: int,
) -> None:
    if plan.affected_rows != rows:
        raise RuntimeError(
            'transcode plan does not cover the complete seeded workload: '
            f'expected {rows}, observed {plan.affected_rows}'
        )
    if plan.relation_count != 2:
        raise RuntimeError(
            'transcode evidence requires one finite and one forever relation: '
            f'observed {plan.relation_count}'
        )
    matching = tuple(
        item
        for item in inventory
        if item.component is component and item.version == 1
    )
    if len(matching) != 1 or matching[0].affected_rows != rows:
        raise RuntimeError(
            'transcode inventory does not cover the complete source version'
        )


def _validate_workload(
    *,
    run_kind: EvidenceRunKind,
    rows: int,
    batch_size: int,
    payload_bytes: int,
    attempts_per_task: int,
) -> None:
    if rows <= 0:
        raise ValueError('rows must be positive')
    if batch_size <= 0:
        raise ValueError('batch size must be positive')
    if payload_bytes < 8:
        raise ValueError('payload bytes must be at least 8')
    if attempts_per_task <= 0:
        raise ValueError('attempt count must be positive')
    if run_kind is EvidenceRunKind.GATE and rows < 1_000_000:
        raise ValueError('transcode gate evidence requires at least 1,000,000 rows')


async def _seed_history(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    rows: int,
    payload_bytes: int,
    attempts_per_task: int,
) -> None:
    payload = _small_json_payload(payload_bytes)
    attempts = encode_attempts(prototype_attempts(attempts_per_task))
    digest = archive_digest(payload)
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority,
                command_fingerprint_version, command_fingerprint, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_content_type,
                result_payload,
                result_digest, prior_result_payload, error_code,
                final_failed_reason, retry_count, max_retries,
                rerun_input_version,
                rerun_input_codec, rerun_input_content_type,
                rerun_input_form, rerun_input_digest,
                rerun_input_inline, rerun_input_reference,
                is_workflow_task, history_schema_version,
                attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot_content_type,
                attempt_snapshot, attempt_snapshot_digest
            )
            SELECT
                md5('transcode-' || series::text)::uuid::text,
                'prototype.transcode', 'default', 100, 1,
                sha256(convert_to('transcode-' || series::text, 'UTF8')),
                CASE WHEN mod(series, 2) = 0
                     THEN 'FAILED' ELSE 'CANCELLED' END,
                CASE WHEN mod(series, 2) = 0
                     THEN 'FAIL_RUNNING' ELSE 'CANCEL_ADMIN' END,
                '2026-08-05T12:00:00Z'::timestamptz,
                '2026-08-05T12:00:00Z'::timestamptz,
                CASE WHEN mod(series, 2) = 0
                     THEN 'finite_30d_v1' ELSE 'forever' END,
                '2026-08-05T11:59:00Z'::timestamptz,
                '2026-08-05T11:58:00Z'::timestamptz,
                :version, :codec, 'application/json',
                CASE WHEN mod(series, 2) = 0 THEN :payload END,
                :digest,
                CASE WHEN mod(series, 2) = 1 THEN :payload END,
                CASE WHEN mod(series, 2) = 0
                     THEN 'FINAL_FAILURE' ELSE 'TASK_CANCELLED' END,
                CASE WHEN mod(series, 2) = 0
                     THEN 'final worker failure'
                     ELSE 'Cancelled via monitoring API' END,
                :retry_count, :retry_count, :version, :codec,
                'application/json',
                CASE WHEN mod(series, 4) < 2 THEN 'INLINE' ELSE 'REFERENCE' END,
                :digest,
                CASE WHEN mod(series, 4) < 2 THEN :payload END,
                CASE WHEN mod(series, 4) >= 2
                     THEN 'sha256:' || encode(:digest, 'hex') END,
                FALSE, :version, :version, :codec, 'application/json',
                :attempts, :attempt_digest
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'version': ARCHIVE_VERSION,
            'codec': ARCHIVE_CODEC,
            'payload': payload,
            'digest': digest,
            'retry_count': attempts_per_task - 1,
            'attempts': attempts.payload,
            'attempt_digest': attempts.digest,
        },
    )


def _small_json_payload(size: int) -> bytes:
    body = b'x' * (size - 8)
    payload = b'{"v":"' + body + b'"}'
    if len(payload) != size:
        raise AssertionError('payload generator did not preserve requested size')
    return payload
