"""Operational live-table index evidence for UUID version and column type."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from typing import cast
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_operational_conditions,
    refresh_cumulative_statistics,
)
from tests.task_history_prototypes.measurements import (
    RelationFootprint,
    relation_footprint,
)
from tests.task_history_prototypes.schema import PrototypeSchema


class TaskIdIndexCandidate(StrEnum):
    UUID4_VARCHAR = 'uuid4_varchar'
    UUID7_VARCHAR = 'uuid7_varchar'
    UUID4_NATIVE = 'uuid4_native'
    UUID7_NATIVE = 'uuid7_native'


@dataclass(frozen=True, slots=True)
class TaskIdIndexMeasurement:
    candidate: TaskIdIndexCandidate
    repetition: int
    base_rows: int
    measured_rows: int
    batch_size: int
    insert_seconds: float
    rows_per_second: float
    table_footprint: RelationFootprint
    primary_index_bytes: int
    primary_index_bytes_per_row: float
    index_blocks_read: int
    index_blocks_hit: int
    index_cache_hit_ratio: float
    tree_level: int
    internal_pages: int
    leaf_pages: int
    average_leaf_density: float
    leaf_fragmentation: float
    wal_bytes: int
    wal_bytes_per_row: float
    wal_lsn_bytes: int
    wal_records: int
    wal_full_page_images: int


@dataclass(frozen=True, slots=True)
class TaskIdIndexEvidence:
    conditions: EvidenceConditions
    workload: dict[str, int | str]
    measurements: tuple[TaskIdIndexMeasurement, ...]


async def collect_task_id_index_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    base_rows: int,
    measured_rows: int,
    batch_size: int,
    repetitions: int,
) -> TaskIdIndexEvidence:
    _validate_workload(
        run_kind=run_kind,
        base_rows=base_rows,
        measured_rows=measured_rows,
        batch_size=batch_size,
        repetitions=repetitions,
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
            'one-million-row established primary index; explicit checkpoint '
            'before each measured sustained-enqueue window'
        ),
        prepared_posture=(
            'ordered 10,000-row INSERT SELECT batches; commit after every batch'
        ),
    )
    await connection.execute(text('CREATE EXTENSION IF NOT EXISTS pgstattuple'))
    await connection.commit()
    schema = PrototypeSchema(f'task_id_index_{uuid4().hex[:10]}')
    await connection.execute(text(f'CREATE SCHEMA {schema.sql}'))
    await connection.commit()
    measurements: list[TaskIdIndexMeasurement] = []
    try:
        for repetition in range(1, repetitions + 1):
            order = (
                tuple(TaskIdIndexCandidate)
                if repetition % 2 == 1
                else tuple(reversed(tuple(TaskIdIndexCandidate)))
            )
            for candidate in order:
                measurements.append(
                    await _measure_candidate(
                        connection,
                        schema,
                        candidate=candidate,
                        repetition=repetition,
                        base_rows=base_rows,
                        measured_rows=measured_rows,
                        batch_size=batch_size,
                    )
                )
    finally:
        await connection.rollback()
        await connection.execute(text(f'DROP SCHEMA {schema.sql} CASCADE'))
        await connection.commit()
    return TaskIdIndexEvidence(
        conditions=conditions,
        workload={
            'base_rows': base_rows,
            'measured_rows': measured_rows,
            'batch_size': batch_size,
            'repetitions': repetitions,
            'uuid7_ids_per_millisecond': 1_000,
            'uuid7_monotonic_field': 'rand_a',
        },
        measurements=tuple(measurements),
    )


async def _measure_candidate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    candidate: TaskIdIndexCandidate,
    repetition: int,
    base_rows: int,
    measured_rows: int,
    batch_size: int,
) -> TaskIdIndexMeasurement:
    relation = f'{candidate.value}_{repetition}'
    id_type = (
        'varchar(36)'
        if candidate in {
            TaskIdIndexCandidate.UUID4_VARCHAR,
            TaskIdIndexCandidate.UUID7_VARCHAR,
        }
        else 'uuid'
    )
    await connection.execute(
        text(
            f"""
            CREATE TABLE {schema.sql}.{relation} (
                task_id {id_type} PRIMARY KEY,
                queue_name text NOT NULL,
                created_at timestamptz NOT NULL,
                payload bytea NOT NULL
            ) WITH (autovacuum_enabled = false)
            """
        )
    )
    await _insert_range(
        connection,
        schema,
        relation=relation,
        candidate=candidate,
        first_row=1,
        last_row=base_rows,
    )
    await connection.commit()
    await connection.execute(text(f'ANALYZE {schema.sql}.{relation}'))
    await connection.commit()
    await connection.execute(text('CHECKPOINT'))
    await connection.execute(text('SELECT pg_stat_reset()'))
    await connection.commit()
    start_lsn = (
        await connection.execute(text('SELECT pg_current_wal_insert_lsn()'))
    ).scalar_one()
    await connection.commit()

    started = time.perf_counter()
    statement_wal_records = 0
    statement_wal_full_page_images = 0
    statement_wal_bytes = 0
    final_row = base_rows + measured_rows
    for first_row in range(base_rows + 1, final_row + 1, batch_size):
        last_row = min(first_row + batch_size - 1, final_row)
        records, full_page_images, wal_bytes = await _insert_range(
            connection,
            schema,
            relation=relation,
            candidate=candidate,
            first_row=first_row,
            last_row=last_row,
            explain_wal=True,
        )
        statement_wal_records += records
        statement_wal_full_page_images += full_page_images
        statement_wal_bytes += wal_bytes
        await connection.commit()
    insert_seconds = time.perf_counter() - started
    end_lsn = (
        await connection.execute(text('SELECT pg_current_wal_insert_lsn()'))
    ).scalar_one()
    await refresh_cumulative_statistics(connection)
    await connection.commit()
    wal_lsn_bytes = int(
        (
            await connection.execute(
                text('SELECT pg_wal_lsn_diff(:end_lsn, :start_lsn)'),
                {'end_lsn': end_lsn, 'start_lsn': start_lsn},
            )
        ).scalar_one()
    )
    index_name = f'{relation}_pkey'
    io = (
        await connection.execute(
            text(
                """
                SELECT idx_blks_read, idx_blks_hit
                FROM pg_statio_user_indexes
                WHERE schemaname = :schema_name
                  AND indexrelname = :index_name
                """
            ),
            {'schema_name': schema.name, 'index_name': index_name},
        )
    ).one()
    shape = (
        await connection.execute(
            text(
                f"""
                SELECT tree_level, internal_pages, leaf_pages,
                       avg_leaf_density, leaf_fragmentation
                FROM pgstatindex(
                    '{schema.name}.{index_name}'::regclass
                )
                """
            )
        )
    ).one()
    footprint = await relation_footprint(
        connection,
        f'{schema.name}.{relation}',
    )
    total_index_accesses = io.idx_blks_read + io.idx_blks_hit
    return TaskIdIndexMeasurement(
        candidate=candidate,
        repetition=repetition,
        base_rows=base_rows,
        measured_rows=measured_rows,
        batch_size=batch_size,
        insert_seconds=insert_seconds,
        rows_per_second=measured_rows / insert_seconds,
        table_footprint=footprint,
        primary_index_bytes=footprint.index_bytes,
        primary_index_bytes_per_row=footprint.index_bytes
        / (base_rows + measured_rows),
        index_blocks_read=io.idx_blks_read,
        index_blocks_hit=io.idx_blks_hit,
        index_cache_hit_ratio=(
            io.idx_blks_hit / total_index_accesses
            if total_index_accesses > 0
            else 1.0
        ),
        tree_level=shape.tree_level,
        internal_pages=shape.internal_pages,
        leaf_pages=shape.leaf_pages,
        average_leaf_density=float(shape.avg_leaf_density),
        leaf_fragmentation=float(shape.leaf_fragmentation),
        wal_bytes=statement_wal_bytes,
        wal_bytes_per_row=statement_wal_bytes / measured_rows,
        wal_lsn_bytes=wal_lsn_bytes,
        wal_records=statement_wal_records,
        wal_full_page_images=statement_wal_full_page_images,
    )


async def _insert_range(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation: str,
    candidate: TaskIdIndexCandidate,
    first_row: int,
    last_row: int,
    explain_wal: bool = False,
) -> tuple[int, int, int]:
    task_id = _task_id_expression(candidate)
    prefix = 'EXPLAIN (ANALYZE, WAL, FORMAT JSON) ' if explain_wal else ''
    result = await connection.execute(
        text(
            f"""
            {prefix}INSERT INTO {schema.sql}.{relation} (
                task_id, queue_name, created_at, payload
            )
            SELECT {task_id}, 'default',
                   TIMESTAMPTZ '2026-08-06T00:00:00Z'
                       + series * interval '1 millisecond',
                   decode(md5('payload-' || series::text), 'hex')
            FROM generate_series(
                CAST(:first_row AS bigint),
                CAST(:last_row AS bigint)
            ) AS series
            """
        ),
        {'first_row': first_row, 'last_row': last_row},
    )
    if not explain_wal:
        return 0, 0, 0
    document = cast(list[object], result.scalar_one())
    if not isinstance(document, list) or len(document) != 1:
        raise RuntimeError('UUID index WAL plan did not return one document')
    root = document[0]
    if not isinstance(root, dict):
        raise RuntimeError('UUID index WAL document is not an object')
    root_object = cast(dict[str, object], root)
    plan_value = root_object.get('Plan')
    if not isinstance(plan_value, dict):
        raise RuntimeError('UUID index WAL plan is missing its root')
    plan = cast(dict[str, object], plan_value)
    return (
        _plan_integer(plan, 'WAL Records'),
        _plan_integer(plan, 'WAL FPI'),
        _plan_integer(plan, 'WAL Bytes'),
    )


def _plan_integer(plan: dict[str, object], field: str) -> int:
    value = plan.get(field, 0)
    if not isinstance(value, int):
        raise RuntimeError(f'UUID index WAL plan field {field} is not an integer')
    return value


def _task_id_expression(candidate: TaskIdIndexCandidate) -> str:
    match candidate:
        case TaskIdIndexCandidate.UUID4_VARCHAR:
            return "md5('uuid4-' || series::text)::uuid::text"
        case TaskIdIndexCandidate.UUID4_NATIVE:
            return "md5('uuid4-' || series::text)::uuid"
        case (
            TaskIdIndexCandidate.UUID7_VARCHAR
            | TaskIdIndexCandidate.UUID7_NATIVE
        ):
            suffix = (
                "lpad(to_hex(1785974400000::bigint + "
                "((series - 1) / 1000)), 12, '0') "
                "|| '7' || lpad(to_hex(((series - 1) % 1000)::bigint), 3, '0') "
                "|| '8' || substring(md5('uuid7-' || series::text), 5, 15)"
            )
            cast = '::uuid::text' if candidate is TaskIdIndexCandidate.UUID7_VARCHAR else '::uuid'
            return f'({suffix}){cast}'


def _validate_workload(
    *,
    run_kind: EvidenceRunKind,
    base_rows: int,
    measured_rows: int,
    batch_size: int,
    repetitions: int,
) -> None:
    if min(base_rows, measured_rows, batch_size, repetitions) <= 0:
        raise ValueError('UUID index evidence sizes must be positive')
    if batch_size > measured_rows:
        raise ValueError('UUID index batch size must not exceed measured rows')
    if run_kind is EvidenceRunKind.GATE and (
        base_rows < 1_000_000
        or measured_rows < 1_000_000
        or repetitions < 2
    ):
        raise ValueError(
            'UUID index gate evidence requires one million established rows, '
            'one million measured rows, and two repetitions'
        )
