"""Measurement primitives shared by task-history archive evidence runs."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.archive import StoredArchiveValue
from tests.task_history_prototypes.archive import archive_digest
from tests.task_history_prototypes.schema import PrototypeSchema


@dataclass(frozen=True, slots=True)
class RelationFootprint:
    heap_bytes: int
    index_bytes: int
    toast_and_overhead_bytes: int
    total_bytes: int

    def __add__(self, other: RelationFootprint) -> RelationFootprint:
        return RelationFootprint(
            heap_bytes=self.heap_bytes + other.heap_bytes,
            index_bytes=self.index_bytes + other.index_bytes,
            toast_and_overhead_bytes=(
                self.toast_and_overhead_bytes + other.toast_and_overhead_bytes
            ),
            total_bytes=self.total_bytes + other.total_bytes,
        )


@dataclass(frozen=True, slots=True)
class ArchiveCandidateMeasurement:
    candidate: str
    rows: int
    attempts_per_task: int
    result_bytes: int
    attempt_snapshot_bytes: int | None
    load_seconds: float
    wal_bytes: int
    footprint: RelationFootprint


class RerunStorageCandidate(StrEnum):
    UNAVAILABLE = 'unavailable'
    INLINE = 'inline'
    REFERENCE = 'reference'


@dataclass(frozen=True, slots=True)
class RerunStorageMeasurement:
    candidate: RerunStorageCandidate
    rows: int
    payload_bytes: int
    result_bytes: int
    load_seconds: float
    wal_bytes: int
    footprint: RelationFootprint


class AdministrativeResultCandidate(StrEnum):
    EXCLUDE = 'exclude'
    NAMED_PRIOR_RESULT = 'named_prior_result'


@dataclass(frozen=True, slots=True)
class AdministrativeResultMeasurement:
    candidate: AdministrativeResultCandidate
    rows: int
    prior_result_bytes: int
    load_seconds: float
    wal_bytes: int
    footprint: RelationFootprint


async def measure_attempt_storage_candidates(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    rows: int,
    result: StoredArchiveValue,
    attempts: StoredArchiveValue,
    attempts_per_task: int,
) -> tuple[ArchiveCandidateMeasurement, ArchiveCandidateMeasurement]:
    if rows <= 0:
        raise ValueError('rows must be positive')
    if attempts_per_task <= 0:
        raise ValueError('attempts_per_task must be positive')

    aggregate = await _measure_aggregate(
        connection,
        schema,
        rows=rows,
        result=result,
        attempts=attempts,
        attempts_per_task=attempts_per_task,
    )
    await connection.execute(text(f'TRUNCATE {schema.sql}.history_aggregate'))
    await connection.commit()
    copartitioned = await _measure_copartitioned(
        connection,
        schema,
        rows=rows,
        result=result,
        attempts=attempts,
        attempts_per_task=attempts_per_task,
    )
    return aggregate, copartitioned


async def measure_rerun_storage_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    rows: int,
    result: bytes,
    rerun_input: bytes,
    candidate: RerunStorageCandidate,
) -> RerunStorageMeasurement:
    if rows <= 0:
        raise ValueError('rows must be positive')
    if not result:
        raise ValueError('result must be non-empty')
    if not rerun_input:
        raise ValueError('rerun input must be non-empty')

    match candidate:
        case RerunStorageCandidate.UNAVAILABLE:
            rerun_parameters: dict[str, object] = {
                'rerun_version': None,
                'rerun_codec': None,
                'rerun_form': None,
                'rerun_digest': None,
                'rerun_inline': None,
                'rerun_reference': None,
            }
        case RerunStorageCandidate.INLINE:
            rerun_parameters = {
                'rerun_version': 1,
                'rerun_codec': 'json-utf8',
                'rerun_form': 'INLINE',
                'rerun_digest': archive_digest(rerun_input),
                'rerun_inline': rerun_input,
                'rerun_reference': None,
            }
        case RerunStorageCandidate.REFERENCE:
            digest = archive_digest(rerun_input)
            rerun_parameters = {
                'rerun_version': 1,
                'rerun_codec': 'json-utf8',
                'rerun_form': 'REFERENCE',
                'rerun_digest': digest,
                'rerun_inline': None,
                'rerun_reference': f'sha256:{digest.hex()}',
            }

    await connection.execute(text(f'TRUNCATE {schema.sql}.history_aggregate'))
    await connection.commit()
    wal_start = await _wal_lsn(connection)
    started = time.perf_counter()
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, retry_count, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot,
                attempt_snapshot_digest, input_digest,
                rerun_input_version, rerun_input_codec, rerun_input_form,
                rerun_input_digest, rerun_input_inline,
                rerun_input_reference
            )
            SELECT
                md5('rerun-' || series::text)::uuid::text,
                'prototype.task', 'default', 100, 'FAILED',
                'FAIL_LOCKED', '2026-08-05T12:00:00Z'::timestamptz,
                '2026-08-05T12:00:00Z'::timestamptz,
                'finite_30d_v1', '2026-08-05T11:59:00Z'::timestamptz,
                '2026-08-05T11:58:00Z'::timestamptz,
                1, 'json-utf8', :result, :result_digest,
                0, FALSE, 1, 1, 'json-utf8', '[]'::bytea,
                :empty_attempt_digest, :input_digest,
                :rerun_version, :rerun_codec, :rerun_form,
                :rerun_digest, :rerun_inline, :rerun_reference
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'result': result,
            'result_digest': archive_digest(result),
            'empty_attempt_digest': archive_digest(b'[]'),
            'input_digest': archive_digest(rerun_input),
            **rerun_parameters,
        },
    )
    await connection.commit()
    elapsed = time.perf_counter() - started
    wal_bytes = await _wal_bytes_since(connection, wal_start)
    footprint = await partition_tree_footprint(
        connection, f'{schema.name}.history_aggregate'
    )
    return RerunStorageMeasurement(
        candidate=candidate,
        rows=rows,
        payload_bytes=len(rerun_input),
        result_bytes=len(result),
        load_seconds=elapsed,
        wal_bytes=wal_bytes,
        footprint=footprint,
    )


async def measure_administrative_result_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    rows: int,
    prior_result: bytes,
    candidate: AdministrativeResultCandidate,
) -> AdministrativeResultMeasurement:
    if rows <= 0:
        raise ValueError('rows must be positive')
    if not prior_result:
        raise ValueError('prior result must be non-empty')

    match candidate:
        case AdministrativeResultCandidate.EXCLUDE:
            prior_payload = None
            result_digest = None
        case AdministrativeResultCandidate.NAMED_PRIOR_RESULT:
            prior_payload = prior_result
            result_digest = archive_digest(prior_result)

    await connection.execute(text(f'TRUNCATE {schema.sql}.history_aggregate'))
    await connection.commit()
    wal_start = await _wal_lsn(connection)
    started = time.perf_counter()
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, prior_result_payload, retry_count,
                is_workflow_task, history_schema_version,
                attempt_archive_version, attempt_snapshot_codec,
                attempt_snapshot, attempt_snapshot_digest
            )
            SELECT
                md5('admin-' || series::text)::uuid::text,
                'prototype.task', 'default', 100, 'CANCELLED',
                'CANCEL_ADMIN', '2026-08-05T12:00:00Z'::timestamptz,
                '2026-08-05T12:00:00Z'::timestamptz,
                'finite_30d_v1', '2026-08-05T11:59:00Z'::timestamptz,
                '2026-08-05T11:58:00Z'::timestamptz,
                1, 'json-utf8', NULL, :result_digest, :prior_result,
                0, FALSE, 1, 1, 'json-utf8', '[]'::bytea,
                :empty_attempt_digest
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'result_digest': result_digest,
            'prior_result': prior_payload,
            'empty_attempt_digest': archive_digest(b'[]'),
        },
    )
    await connection.commit()
    elapsed = time.perf_counter() - started
    wal_bytes = await _wal_bytes_since(connection, wal_start)
    footprint = await partition_tree_footprint(
        connection, f'{schema.name}.history_aggregate'
    )
    return AdministrativeResultMeasurement(
        candidate=candidate,
        rows=rows,
        prior_result_bytes=len(prior_result),
        load_seconds=elapsed,
        wal_bytes=wal_bytes,
        footprint=footprint,
    )


async def _measure_aggregate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    rows: int,
    result: StoredArchiveValue,
    attempts: StoredArchiveValue,
    attempts_per_task: int,
) -> ArchiveCandidateMeasurement:
    await connection.execute(text(f'TRUNCATE {schema.sql}.history_aggregate'))
    await connection.commit()
    wal_start = await _wal_lsn(connection)
    started = time.perf_counter()
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, retry_count, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot,
                attempt_snapshot_digest
            )
            SELECT
                md5(series::text)::uuid::text,
                'prototype.task', 'default', 100, 'COMPLETED',
                'COMPLETE_LOCKED', '2026-08-05T12:00:00Z'::timestamptz,
                '2026-08-05T12:00:00Z'::timestamptz,
                'finite_30d_v1', '2026-08-05T11:59:00Z'::timestamptz,
                '2026-08-05T11:58:00Z'::timestamptz,
                :result_version, :result_codec, :result_payload,
                :result_digest, :retry_count, FALSE,
                1, :attempt_version, :attempt_codec, :attempt_payload,
                :attempt_digest
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'result_version': result.version,
            'result_codec': result.codec,
            'result_payload': result.payload,
            'result_digest': result.digest,
            'retry_count': attempts_per_task - 1,
            'attempt_version': attempts.version,
            'attempt_codec': attempts.codec,
            'attempt_payload': attempts.payload,
            'attempt_digest': attempts.digest,
        },
    )
    await connection.commit()
    elapsed = time.perf_counter() - started
    wal_bytes = await _wal_bytes_since(connection, wal_start)
    footprint = await partition_tree_footprint(
        connection, f'{schema.name}.history_aggregate'
    )
    return ArchiveCandidateMeasurement(
        candidate='aggregate_snapshot',
        rows=rows,
        attempts_per_task=attempts_per_task,
        result_bytes=len(result.payload),
        attempt_snapshot_bytes=len(attempts.payload),
        load_seconds=elapsed,
        wal_bytes=wal_bytes,
        footprint=footprint,
    )


async def _measure_copartitioned(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    rows: int,
    result: StoredArchiveValue,
    attempts: StoredArchiveValue,
    attempts_per_task: int,
) -> ArchiveCandidateMeasurement:
    await connection.execute(
        text(
            f'TRUNCATE {schema.sql}.history_copartitioned, '
            f'{schema.sql}.attempts_copartitioned'
        )
    )
    await connection.commit()
    wal_start = await _wal_lsn(connection)
    started = time.perf_counter()
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_copartitioned (
                task_id, task_name, queue_name, priority, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_payload,
                result_digest, retry_count, is_workflow_task,
                history_schema_version, attempt_archive_version
            )
            SELECT
                md5(series::text)::uuid::text,
                'prototype.task', 'default', 100, 'COMPLETED',
                'COMPLETE_LOCKED', '2026-08-05T12:00:00Z'::timestamptz,
                '2026-08-05T12:00:00Z'::timestamptz,
                'finite_30d_v1', '2026-08-05T11:59:00Z'::timestamptz,
                '2026-08-05T11:58:00Z'::timestamptz,
                :result_version, :result_codec, :result_payload,
                :result_digest, :retry_count, FALSE,
                1, :attempt_version
            FROM generate_series(1, :rows) AS series
            """
        ),
        {
            'rows': rows,
            'result_version': result.version,
            'result_codec': result.codec,
            'result_payload': result.payload,
            'result_digest': result.digest,
            'retry_count': attempts_per_task - 1,
            'attempt_version': attempts.version,
        },
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.attempts_copartitioned (
                task_id, retention_class_key, retention_anchor_at,
                attempt_archive_version, attempt, outcome, will_retry,
                started_at, finished_at, error_code, error_message,
                failed_reason, worker_id, worker_hostname, worker_pid,
                worker_process_name
            )
            SELECT
                md5(task_number::text)::uuid::text,
                'finite_30d_v1', '2026-08-05T12:00:00Z'::timestamptz,
                :attempt_version, attempt_number,
                CASE WHEN attempt_number = :attempt_count
                     THEN 'COMPLETED' ELSE 'FAILED' END,
                attempt_number < :attempt_count,
                '2026-08-05T11:00:00Z'::timestamptz
                    + attempt_number * interval '2 seconds',
                '2026-08-05T11:00:01Z'::timestamptz
                    + attempt_number * interval '2 seconds',
                CASE WHEN attempt_number < :attempt_count
                     THEN 'RETRYABLE' END,
                CASE WHEN attempt_number < :attempt_count
                     THEN 'retry' END,
                CASE WHEN attempt_number < :attempt_count
                     THEN 'worker failure' END,
                'worker-' || (attempt_number % 3)::text,
                'test-host', 1000 + attempt_number, 'test-process'
            FROM generate_series(1, :rows) AS task_number
            CROSS JOIN generate_series(1, :attempt_count) AS attempt_number
            """
        ),
        {
            'rows': rows,
            'attempt_count': attempts_per_task,
            'attempt_version': attempts.version,
        },
    )
    await connection.commit()
    elapsed = time.perf_counter() - started
    wal_bytes = await _wal_bytes_since(connection, wal_start)
    history = await partition_tree_footprint(
        connection, f'{schema.name}.history_copartitioned'
    )
    attempt_rows = await partition_tree_footprint(
        connection, f'{schema.name}.attempts_copartitioned'
    )
    return ArchiveCandidateMeasurement(
        candidate='copartitioned_attempts',
        rows=rows,
        attempts_per_task=attempts_per_task,
        result_bytes=len(result.payload),
        attempt_snapshot_bytes=None,
        load_seconds=elapsed,
        wal_bytes=wal_bytes,
        footprint=history + attempt_rows,
    )


async def partition_tree_footprint(
    connection: AsyncConnection,
    relation: str,
) -> RelationFootprint:
    row = (
        await connection.execute(
            text(
                """
                WITH leaves AS (
                    SELECT relid
                    FROM pg_partition_tree(CAST(:relation AS regclass))
                    WHERE isleaf
                )
                SELECT
                    COALESCE(SUM(pg_relation_size(relid)), 0)::bigint AS heap,
                    COALESCE(SUM(pg_indexes_size(relid)), 0)::bigint AS indexes,
                    COALESCE(SUM(
                        pg_total_relation_size(relid)
                        - pg_relation_size(relid)
                        - pg_indexes_size(relid)
                    ), 0)::bigint AS toast_and_overhead,
                    COALESCE(SUM(pg_total_relation_size(relid)), 0)::bigint AS total
                FROM leaves
                """
            ),
            {'relation': relation},
        )
    ).one()
    return RelationFootprint(
        heap_bytes=row.heap,
        index_bytes=row.indexes,
        toast_and_overhead_bytes=row.toast_and_overhead,
        total_bytes=row.total,
    )


async def relation_footprint(
    connection: AsyncConnection,
    relation: str,
) -> RelationFootprint:
    row = (
        await connection.execute(
            text(
                """
                SELECT pg_relation_size(CAST(:relation AS regclass))::bigint
                           AS heap,
                       pg_indexes_size(CAST(:relation AS regclass))::bigint
                           AS indexes,
                       (
                           pg_total_relation_size(CAST(:relation AS regclass))
                           - pg_relation_size(CAST(:relation AS regclass))
                           - pg_indexes_size(CAST(:relation AS regclass))
                       )::bigint AS toast_and_overhead,
                       pg_total_relation_size(CAST(:relation AS regclass))::bigint
                           AS total
                """
            ),
            {'relation': relation},
        )
    ).one()
    return RelationFootprint(
        heap_bytes=row.heap,
        index_bytes=row.indexes,
        toast_and_overhead_bytes=row.toast_and_overhead,
        total_bytes=row.total,
    )


async def _wal_lsn(connection: AsyncConnection) -> str:
    return (
        await connection.execute(text('SELECT pg_current_wal_insert_lsn()::text'))
    ).scalar_one()


async def _wal_bytes_since(connection: AsyncConnection, start_lsn: str) -> int:
    return (
        await connection.execute(
            text(
                """
                SELECT pg_wal_lsn_diff(
                    pg_current_wal_insert_lsn(), CAST(:start_lsn AS pg_lsn)
                )::bigint
                """
            ),
            {'start_lsn': start_lsn},
        )
    ).scalar_one()
