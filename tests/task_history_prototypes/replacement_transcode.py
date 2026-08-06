"""Replacement-partition executor for offline archive transcoding."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.lifecycle.operations import TerminalizationKind
from tests.task_history_prototypes.schema import PrototypeSchema
from tests.task_history_prototypes.transcode import (
    TRANSCODE_MINIMUM_ROWS_PER_SECOND,
    ArchiveComponent,
    ArchiveMaintenanceSession,
    DecoderRetirement,
    TranscodeRejectionKind,
    active_archive_maintenance_id,
    archive_codec_for_version,
    archive_component_columns,
    archive_transcode_direction,
    begin_archive_maintenance,
    decoder_retirement_status,
    install_archive_transcode_prototype,
    lock_archive_access_gate,
    lock_archive_transcode_program,
    ratio_ceiling,
)


class ReplacementJobState(StrEnum):
    PLANNED = 'PLANNED'
    COPYING = 'COPYING'
    COPIED = 'COPIED'
    VERIFIED = 'VERIFIED'
    SWAPPED = 'SWAPPED'
    COMPLETE = 'COMPLETE'


class ReplacementRelationState(StrEnum):
    PLANNED = 'PLANNED'
    COPYING = 'COPYING'
    COPIED = 'COPIED'
    VERIFIED = 'VERIFIED'
    SWAPPED = 'SWAPPED'
    COMPLETE = 'COMPLETE'


class ReplacementCopyRejectionKind(StrEnum):
    SOURCE_CORRUPT = 'SOURCE_CORRUPT'
    SOURCE_SET_CHANGED = 'SOURCE_SET_CHANGED'


@dataclass(frozen=True, slots=True)
class ReplacementTranscodePlan:
    job_id: str
    component: ArchiveComponent
    source_version: int
    target_version: int
    transformed_rows: int
    copied_rows: int
    payload_rows: int
    payload_bytes: int
    projected_payload_bytes: int
    affected_relation_bytes: int
    relation_count: int
    peak_additional_disk_budget_bytes: int
    wal_budget_bytes: int
    rewrite_duration_limit_seconds: float
    rollback_copied_rows: int
    rollback_peak_additional_disk_budget_bytes: int
    rollback_wal_budget_bytes: int
    rollback_duration_limit_seconds: float
    reversible: bool


@dataclass(frozen=True, slots=True)
class ReplacementTranscodeRejected:
    kind: TranscodeRejectionKind
    affected_rows: int


type PlanReplacementTranscodeOutcome = (
    ReplacementTranscodePlan | ReplacementTranscodeRejected
)


@dataclass(frozen=True, slots=True)
class ReplacementCopyBatch:
    job_id: str
    relation_ordinal: int
    batch_number: int
    rows_copied: int
    transformed_rows: int
    copied_rows_completed: int
    copied_rows_total: int


@dataclass(frozen=True, slots=True)
class ReplacementCopyRejected:
    job_id: str
    relation_ordinal: int
    kind: ReplacementCopyRejectionKind
    observed_rows: int


@dataclass(frozen=True, slots=True)
class ReplacementReadyForVerification:
    job_id: str
    copied_rows_total: int


type RunReplacementCopyOutcome = (
    ReplacementCopyBatch
    | ReplacementCopyRejected
    | ReplacementReadyForVerification
)


@dataclass(frozen=True, slots=True)
class ReplacementVerification:
    job_id: str
    verified: bool
    source_relations_changed: int
    replacement_row_mismatches: int
    source_rows_remaining_after_swap: int | None
    invalid_target_rows: int
    copied_rows_completed: int
    copied_rows_total: int
    wal_bytes: int | None


@dataclass(frozen=True, slots=True)
class ReplacementSwap:
    job_id: str
    relations_swapped: int


@dataclass(frozen=True, slots=True)
class _ReplacementJob:
    job_id: str
    maintenance_id: str
    component: str
    source_version: int
    target_version: int
    source_codec: str
    target_codec: str
    state: str
    copied_rows_total: int
    copied_rows_completed: int
    start_lsn: str
    wal_bytes: int | None
    relation_count: int


@dataclass(frozen=True, slots=True)
class _ReplacementRelation:
    job_id: str
    relation_ordinal: int
    source_relation_oid: int
    source_relation_name: str
    parent_relation_oid: int
    parent_relation_name: str
    partition_bound: str
    partition_constraint: str
    replacement_relation_name: str
    backup_relation_name: str
    state: str
    row_count: int
    transformed_rows: int
    rows_copied: int
    last_source_ctid: str | None


async def install_replacement_archive_transcode_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    await install_archive_transcode_prototype(connection, schema)
    for statement in _replacement_manifest(schema):
        await connection.execute(text(statement))


async def begin_replacement_archive_maintenance(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    maintenance_id: str,
) -> ArchiveMaintenanceSession:
    return await begin_archive_maintenance(
        connection,
        schema,
        maintenance_id=maintenance_id,
    )


async def finish_replacement_archive_maintenance(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    maintenance_id: str,
) -> None:
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    active_jobs = (
        await connection.execute(
            text(
                f"""
                SELECT (
                    SELECT count(*)
                    FROM {schema.sql}.archive_replacement_jobs
                    WHERE maintenance_id = :maintenance_id
                      AND state <> 'COMPLETE'
                ) + (
                    SELECT count(*)
                    FROM {schema.sql}.archive_transcode_jobs
                    WHERE maintenance_id = :maintenance_id
                      AND state IN ('PLANNED', 'RUNNING')
                )
                """
            ),
            {'maintenance_id': maintenance_id},
        )
    ).scalar_one()
    if active_jobs:
        raise ValueError('archive maintenance has an unfinished replacement job')
    ended = (
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_maintenance_sessions
                SET ended_at = statement_timestamp()
                WHERE maintenance_id = :maintenance_id
                  AND ended_at IS NULL
                RETURNING maintenance_id
                """
            ),
            {'maintenance_id': maintenance_id},
        )
    ).scalar_one_or_none()
    if ended is None:
        raise ValueError('archive maintenance session is not active')


async def plan_replacement_archive_transcode(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    component: ArchiveComponent,
    source_version: int,
    target_version: int,
) -> PlanReplacementTranscodeOutcome:
    direction = archive_transcode_direction(source_version, target_version)
    if direction is None:
        return ReplacementTranscodeRejected(
            TranscodeRejectionKind.UNSUPPORTED_DIRECTION,
            0,
        )
    columns = archive_component_columns(component)
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    maintenance_id = await active_archive_maintenance_id(connection, schema)
    if maintenance_id is None:
        return ReplacementTranscodeRejected(
            TranscodeRejectionKind.MAINTENANCE_REQUIRED,
            0,
        )
    active = (
        await connection.execute(
            text(
                f"""
                SELECT (
                    SELECT count(*)
                    FROM {schema.sql}.archive_transcode_jobs
                    WHERE state IN ('PLANNED', 'RUNNING')
                ) + (
                    SELECT count(*)
                    FROM {schema.sql}.archive_replacement_jobs
                    WHERE state <> 'COMPLETE'
                )
                """
            )
        )
    ).scalar_one()
    if active:
        return ReplacementTranscodeRejected(
            TranscodeRejectionKind.ACTIVE_JOB,
            active,
        )

    corrupt_rows = await _invalid_component_rows(
        connection,
        schema,
        component=component,
        version=source_version,
    )
    if corrupt_rows:
        return ReplacementTranscodeRejected(
            TranscodeRejectionKind.SOURCE_CORRUPT,
            corrupt_rows,
        )

    source_codec = archive_codec_for_version(component, source_version)
    target_codec = archive_codec_for_version(component, target_version)
    inventory = (
        await connection.execute(
            text(
                f"""
                SELECT history.tableoid::oid::bigint AS relation_oid,
                       child.relname AS relation_name,
                       parent.oid::bigint AS parent_oid,
                       parent.relname AS parent_name,
                       pg_get_expr(child.relpartbound, child.oid)
                           AS partition_bound,
                       pg_get_partition_constraintdef(child.oid)
                           AS partition_constraint,
                       count(*) AS row_count,
                       count(*) FILTER (
                           WHERE {columns.version} = :source_version
                             AND {columns.codec} = :source_codec
                             AND ({columns.presence_predicate})
                       ) AS transformed_rows,
                       count({columns.payload}) FILTER (
                           WHERE {columns.version} = :source_version
                             AND {columns.codec} = :source_codec
                             AND ({columns.presence_predicate})
                       ) AS payload_rows,
                       COALESCE(sum(octet_length({columns.payload})) FILTER (
                           WHERE {columns.version} = :source_version
                             AND {columns.codec} = :source_codec
                             AND ({columns.presence_predicate})
                       ), 0) AS payload_bytes,
                       pg_total_relation_size(history.tableoid)
                           AS relation_bytes,
                       count(DISTINCT task_id) AS distinct_task_ids
                FROM {schema.sql}.history_aggregate AS history
                JOIN pg_class AS child ON child.oid = history.tableoid
                JOIN pg_inherits AS inheritance
                  ON inheritance.inhrelid = child.oid
                JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
                GROUP BY history.tableoid, child.relname,
                         parent.oid, parent.relname, child.oid
                HAVING count(*) FILTER (
                    WHERE {columns.version} = :source_version
                      AND {columns.codec} = :source_codec
                      AND ({columns.presence_predicate})
                ) > 0
                ORDER BY child.relname
                """
            ),
            {
                'source_version': source_version,
                'source_codec': source_codec,
            },
        )
    ).all()
    if any(row.row_count != row.distinct_task_ids for row in inventory):
        return ReplacementTranscodeRejected(
            TranscodeRejectionKind.SOURCE_CORRUPT,
            sum(
                row.row_count - row.distinct_task_ids
                for row in inventory
                if row.row_count != row.distinct_task_ids
            ),
        )

    transformed_rows = sum(row.transformed_rows for row in inventory)
    copied_rows = sum(row.row_count for row in inventory)
    payload_rows = sum(row.payload_rows for row in inventory)
    payload_bytes = sum(row.payload_bytes for row in inventory)
    relation_bytes = sum(row.relation_bytes for row in inventory)
    projected_payload_bytes = (
        payload_bytes + 2 * payload_rows
        if direction == 'FORWARD'
        else payload_bytes - 2 * payload_rows
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.archive_replacement_jobs (
                job_id, maintenance_id, component,
                source_version, target_version,
                source_codec, target_codec, state,
                transformed_rows, copied_rows_total,
                copied_rows_completed, payload_rows,
                payload_bytes_before, projected_payload_bytes,
                affected_relation_bytes, started_at, start_lsn
            ) VALUES (
                :job_id, :maintenance_id, :component,
                :source_version, :target_version,
                :source_codec, :target_codec, 'PLANNED',
                :transformed_rows, :copied_rows, 0, :payload_rows,
                :payload_bytes, :projected_payload_bytes,
                :relation_bytes, statement_timestamp(),
                pg_current_wal_insert_lsn()
            )
            """
        ),
        {
            'job_id': job_id,
            'maintenance_id': maintenance_id,
            'component': component.value,
            'source_version': source_version,
            'target_version': target_version,
            'source_codec': source_codec,
            'target_codec': target_codec,
            'transformed_rows': transformed_rows,
            'copied_rows': copied_rows,
            'payload_rows': payload_rows,
            'payload_bytes': payload_bytes,
            'projected_payload_bytes': projected_payload_bytes,
            'relation_bytes': relation_bytes,
        },
    )
    for ordinal, row in enumerate(inventory, start=1):
        suffix = job_id.replace('-', '')[:12]
        replacement_name = f'archive_replacement_{suffix}_{ordinal}'
        backup_name = f'archive_replaced_{suffix}_{ordinal}'
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.archive_replacement_relations (
                    job_id, relation_ordinal, source_relation_oid,
                    source_relation_name, parent_relation_oid,
                    parent_relation_name, partition_bound,
                    partition_constraint, replacement_relation_name,
                    backup_relation_name, state, row_count,
                    transformed_rows, rows_copied, relation_bytes
                ) VALUES (
                    :job_id, :ordinal, :source_oid,
                    :source_name, :parent_oid, :parent_name,
                    :partition_bound, :partition_constraint,
                    :replacement_name, :backup_name, 'PLANNED',
                    :row_count, :transformed_rows, 0, :relation_bytes
                )
                """
            ),
            {
                'job_id': job_id,
                'ordinal': ordinal,
                'source_oid': row.relation_oid,
                'source_name': row.relation_name,
                'parent_oid': row.parent_oid,
                'parent_name': row.parent_name,
                'partition_bound': row.partition_bound,
                'partition_constraint': row.partition_constraint,
                'replacement_name': replacement_name,
                'backup_name': backup_name,
                'row_count': row.row_count,
                'transformed_rows': row.transformed_rows,
                'relation_bytes': row.relation_bytes,
            },
        )

    duration = copied_rows / TRANSCODE_MINIMUM_ROWS_PER_SECOND
    return ReplacementTranscodePlan(
        job_id=job_id,
        component=component,
        source_version=source_version,
        target_version=target_version,
        transformed_rows=transformed_rows,
        copied_rows=copied_rows,
        payload_rows=payload_rows,
        payload_bytes=payload_bytes,
        projected_payload_bytes=projected_payload_bytes,
        affected_relation_bytes=relation_bytes,
        relation_count=len(inventory),
        peak_additional_disk_budget_bytes=ratio_ceiling(
            relation_bytes,
            numerator=5,
            denominator=4,
        ),
        wal_budget_bytes=ratio_ceiling(
            relation_bytes,
            numerator=3,
            denominator=2,
        ),
        rewrite_duration_limit_seconds=duration,
        rollback_copied_rows=copied_rows,
        rollback_peak_additional_disk_budget_bytes=ratio_ceiling(
            relation_bytes,
            numerator=5,
            denominator=4,
        ),
        rollback_wal_budget_bytes=ratio_ceiling(
            relation_bytes,
            numerator=3,
            denominator=2,
        ),
        rollback_duration_limit_seconds=duration,
        reversible=True,
    )


async def run_replacement_copy_batch(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    batch_size: int,
) -> RunReplacementCopyOutcome:
    if batch_size <= 0:
        raise ValueError('batch size must be positive')
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    job = await _lock_replacement_job(connection, schema, job_id)
    if ReplacementJobState(job.state) not in {
        ReplacementJobState.PLANNED,
        ReplacementJobState.COPYING,
    }:
        if job.state == ReplacementJobState.COPIED:
            return ReplacementReadyForVerification(job_id, job.copied_rows_total)
        raise ValueError('replacement copy is not mutable in this job state')
    await _require_active_job_maintenance(connection, schema, job)
    relation_row = (
        await connection.execute(
            text(
                f"""
                SELECT *
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id
                  AND state IN ('PLANNED', 'COPYING')
                ORDER BY relation_ordinal
                LIMIT 1
                FOR UPDATE
                """
            ),
            {'job_id': job_id},
        )
    ).mappings().one_or_none()
    if relation_row is None:
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_replacement_jobs
                SET state = 'COPIED', copied_at = statement_timestamp()
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
        return ReplacementReadyForVerification(job_id, job.copied_rows_total)
    relation = _replacement_relation_from_row(relation_row)

    if relation.state == ReplacementRelationState.PLANNED:
        rejection = await _prepare_replacement_relation(
            connection,
            schema,
            job=job,
            relation=relation,
        )
        if rejection is not None:
            return rejection
        relation_row = (
            await connection.execute(
                text(
                    f"""
                    SELECT *
                    FROM {schema.sql}.archive_replacement_relations
                    WHERE job_id = :job_id
                      AND relation_ordinal = :ordinal
                    FOR UPDATE
                    """
                ),
                {'job_id': job_id, 'ordinal': relation.relation_ordinal},
            )
        ).mappings().one()
        relation = _replacement_relation_from_row(relation_row)

    source = _qualified(schema, relation.source_relation_name)
    replacement = _qualified(schema, relation.replacement_relation_name)
    columns = await _relation_columns(connection, relation.source_relation_oid)
    transformed_select = _transformed_select(
        columns,
        component=ArchiveComponent(job.component),
        source_version=job.source_version,
        source_codec=job.source_codec,
        target_version=job.target_version,
        target_codec=job.target_codec,
        alias='source',
    )
    encoded_select = _encoded_source_select(
        ArchiveComponent(job.component),
        alias='source',
        source_version=job.source_version,
        source_codec=job.source_codec,
        forward=job.target_version > job.source_version,
    )
    transformed_condition = _component_source_condition(
        ArchiveComponent(job.component),
        alias='source',
        source_version=job.source_version,
        source_codec=job.source_codec,
    )
    inserted = (
        await connection.execute(
            text(
                f"""
                WITH source_batch AS MATERIALIZED (
                    SELECT ctid AS source_ctid, source_table.*
                    FROM {source} AS source_table
                    WHERE (
                        CAST(:last_source_ctid AS tid) IS NULL
                        OR ctid > CAST(:last_source_ctid AS tid)
                    )
                    ORDER BY ctid
                    LIMIT :batch_size
                ), encoded AS MATERIALIZED (
                    SELECT {encoded_select}
                    FROM source_batch AS source
                ), inserted AS (
                    INSERT INTO {replacement} ({_column_list(columns)})
                    SELECT {transformed_select}
                    FROM encoded AS source
                    RETURNING task_id
                )
                SELECT count(*) AS rows_copied,
                       (
                           SELECT source_ctid::text
                           FROM source_batch
                           ORDER BY source_batch.source_ctid DESC
                           LIMIT 1
                       ) AS last_source_ctid,
                       (
                           SELECT count(*)
                           FROM source_batch AS source
                           WHERE {transformed_condition}
                       ) AS transformed_rows
                FROM inserted
                """
            ),
            {
                'last_source_ctid': relation.last_source_ctid,
                'batch_size': batch_size,
            },
        )
    ).one()
    if inserted.rows_copied == 0 and relation.rows_copied < relation.row_count:
        return ReplacementCopyRejected(
            job_id=job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=ReplacementCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=relation.rows_copied,
        )
    batch_number = (
        await connection.execute(
            text(
                f"""
                SELECT COALESCE(max(batch_number), 0) + 1
                FROM {schema.sql}.archive_replacement_batches
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one()
    rows_copied = relation.rows_copied + inserted.rows_copied
    relation_state = (
        ReplacementRelationState.COPIED
        if rows_copied == relation.row_count
        else ReplacementRelationState.COPYING
    )
    if rows_copied > relation.row_count:
        return ReplacementCopyRejected(
            job_id=job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=ReplacementCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=rows_copied,
        )
    if relation_state is ReplacementRelationState.COPIED:
        await _restore_source_constraints(
            connection,
            source_relation_oid=relation.source_relation_oid,
            replacement=replacement,
        )
        await connection.execute(
            text(
                f'CREATE INDEX '
                f'{_identifier(_replacement_index_name(job_id, relation.relation_ordinal))} '
                f'ON {replacement} (task_id)'
            )
        )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.archive_replacement_batches (
                job_id, batch_number, relation_ordinal,
                rows_copied, committed_at
            ) VALUES (
                :job_id, :batch_number, :ordinal,
                :rows_copied, statement_timestamp()
            )
            """
        ),
        {
            'job_id': job_id,
            'batch_number': batch_number,
            'ordinal': relation.relation_ordinal,
            'rows_copied': inserted.rows_copied,
        },
    )
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_relations
            SET state = :state, rows_copied = :rows_copied,
                last_source_ctid = CAST(:last_source_ctid AS tid),
                copied_at = CASE WHEN :state = 'COPIED'
                                 THEN statement_timestamp()
                                 ELSE copied_at END
            WHERE job_id = :job_id
              AND relation_ordinal = :ordinal
            """
        ),
        {
            'state': relation_state.value,
            'rows_copied': rows_copied,
            'last_source_ctid': inserted.last_source_ctid,
            'job_id': job_id,
            'ordinal': relation.relation_ordinal,
        },
    )
    completed = job.copied_rows_completed + inserted.rows_copied
    all_copied = completed == job.copied_rows_total
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_jobs
            SET state = :state,
                copied_rows_completed = :completed,
                last_batch_at = statement_timestamp(),
                copied_at = CASE WHEN :state = 'COPIED'
                                 THEN statement_timestamp()
                                 ELSE copied_at END
            WHERE job_id = :job_id
            """
        ),
        {
            'state': (
                ReplacementJobState.COPIED.value
                if all_copied
                else ReplacementJobState.COPYING.value
            ),
            'completed': completed,
            'job_id': job_id,
        },
    )
    return ReplacementCopyBatch(
        job_id=job_id,
        relation_ordinal=relation.relation_ordinal,
        batch_number=batch_number,
        rows_copied=inserted.rows_copied,
        transformed_rows=inserted.transformed_rows,
        copied_rows_completed=completed,
        copied_rows_total=job.copied_rows_total,
    )


async def verify_replacement_archive_transcode(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
) -> ReplacementVerification:
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    job = await _lock_replacement_job(connection, schema, job_id)
    state = ReplacementJobState(job.state)
    if state is ReplacementJobState.COMPLETE:
        return await _completed_verification(connection, schema, job)
    if state is ReplacementJobState.SWAPPED:
        return await _post_swap_verification(connection, schema, job)
    if state not in {
        ReplacementJobState.COPIED,
        ReplacementJobState.VERIFIED,
    }:
        raise ValueError('replacement relations are not ready for verification')
    await _require_active_job_maintenance(connection, schema, job)

    changed = 0
    mismatches = 0
    invalid_targets = 0
    relations = await _replacement_relations(connection, schema, job_id)
    for relation in relations:
        source = _qualified(schema, relation.source_relation_name)
        replacement = _qualified(schema, relation.replacement_relation_name)
        if not await _source_binding_matches(connection, schema, relation):
            changed += 1
            continue
        observed_source = (
            await connection.execute(text(f'SELECT count(*) FROM {source}'))
        ).scalar_one()
        if observed_source != relation.row_count:
            changed += 1
            continue
        columns = await _relation_columns(connection, relation.source_relation_oid)
        mismatch = await _replacement_mismatch_count(
            connection,
            source=source,
            replacement=replacement,
            columns=columns,
            component=ArchiveComponent(job.component),
            source_version=job.source_version,
            source_codec=job.source_codec,
            target_version=job.target_version,
            target_codec=job.target_codec,
        )
        mismatches += mismatch
        invalid_targets += await _invalid_component_rows_in_relation(
            connection,
            schema,
            relation_name=relation.replacement_relation_name,
            component=ArchiveComponent(job.component),
            version=job.target_version,
        )
        if mismatch == 0:
            await connection.execute(
                text(
                    f"""
                    UPDATE {schema.sql}.archive_replacement_relations
                    SET state = 'VERIFIED', verified_at = statement_timestamp()
                    WHERE job_id = :job_id
                      AND relation_ordinal = :ordinal
                    """
                ),
                {'job_id': job_id, 'ordinal': relation.relation_ordinal},
            )
    verified = changed == 0 and mismatches == 0 and invalid_targets == 0
    if verified and state is not ReplacementJobState.VERIFIED:
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_replacement_jobs
                SET state = 'VERIFIED', verified_at = statement_timestamp()
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    return ReplacementVerification(
        job_id=job_id,
        verified=verified,
        source_relations_changed=changed,
        replacement_row_mismatches=mismatches,
        source_rows_remaining_after_swap=None,
        invalid_target_rows=invalid_targets,
        copied_rows_completed=job.copied_rows_completed,
        copied_rows_total=job.copied_rows_total,
        wal_bytes=None,
    )


async def swap_verified_replacement_partitions(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
) -> ReplacementSwap:
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    job = await _lock_replacement_job(connection, schema, job_id)
    state = ReplacementJobState(job.state)
    if state in {ReplacementJobState.SWAPPED, ReplacementJobState.COMPLETE}:
        return ReplacementSwap(job_id, job.relation_count)
    if state not in {
        ReplacementJobState.COPIED,
        ReplacementJobState.VERIFIED,
    }:
        raise ValueError('replacement relations must be copied before binding swap')
    await _require_active_job_maintenance(connection, schema, job)
    relations = await _replacement_relations(connection, schema, job_id)
    for parent_name in sorted({row.parent_relation_name for row in relations}):
        await connection.execute(
            text(f'LOCK TABLE {_qualified(schema, parent_name)} '
                 'IN ACCESS EXCLUSIVE MODE')
        )
    for relation in relations:
        await connection.execute(
            text(
                f'LOCK TABLE '
                f'{_qualified(schema, relation.source_relation_name)}, '
                f'{_qualified(schema, relation.replacement_relation_name)} '
                'IN SHARE MODE'
            )
        )
    locked_verification = await verify_replacement_archive_transcode(
        connection,
        schema,
        job_id=job_id,
    )
    if not locked_verification.verified:
        raise RuntimeError(
            'replacement verification changed before binding swap: '
            'source_relations_changed='
            f'{locked_verification.source_relations_changed}, '
            'replacement_row_mismatches='
            f'{locked_verification.replacement_row_mismatches}, '
            f'invalid_target_rows={locked_verification.invalid_target_rows}'
        )
    for relation in relations:
        source = _qualified(schema, relation.source_relation_name)
        parent = _qualified(schema, relation.parent_relation_name)
        replacement = _qualified(schema, relation.replacement_relation_name)
        await connection.execute(
            text(f'ALTER TABLE {parent} DETACH PARTITION {source}')
        )
        await connection.execute(
            text(
                f'ALTER TABLE {source} RENAME TO '
                f'{_identifier(relation.backup_relation_name)}'
            )
        )
        await connection.execute(
            text(
                f'ALTER TABLE {replacement} RENAME TO '
                f'{_identifier(relation.source_relation_name)}'
            )
        )
        canonical = _qualified(schema, relation.source_relation_name)
        await connection.execute(
            text(
                f'ALTER TABLE {parent} ATTACH PARTITION {canonical} '
                f'{relation.partition_bound}'
            )
        )
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_replacement_relations
                SET state = 'SWAPPED', swapped_at = statement_timestamp()
                WHERE job_id = :job_id
                  AND relation_ordinal = :ordinal
                """
            ),
            {'job_id': job_id, 'ordinal': relation.relation_ordinal},
        )
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_jobs
            SET state = 'SWAPPED', swapped_at = statement_timestamp()
            WHERE job_id = :job_id
            """
        ),
        {'job_id': job_id},
    )
    return ReplacementSwap(job_id, len(relations))


async def finalize_replacement_archive_transcode(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
) -> ReplacementVerification:
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    job = await _lock_replacement_job(connection, schema, job_id)
    state = ReplacementJobState(job.state)
    if state is ReplacementJobState.COMPLETE:
        return await _completed_verification(connection, schema, job)
    if state is not ReplacementJobState.SWAPPED:
        raise ValueError('replacement partitions have not been swapped')
    await _require_active_job_maintenance(connection, schema, job)
    verification = await _post_swap_verification(connection, schema, job)
    if not verification.verified:
        return verification
    relations = await _replacement_relations(connection, schema, job_id)
    for relation in relations:
        await connection.execute(
            text(f'DROP TABLE {_qualified(schema, relation.backup_relation_name)}')
        )
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_replacement_relations
                SET state = 'COMPLETE', completed_at = statement_timestamp()
                WHERE job_id = :job_id
                  AND relation_ordinal = :ordinal
                """
            ),
            {'job_id': job_id, 'ordinal': relation.relation_ordinal},
        )
    wal_bytes = await _wal_bytes_since(connection, str(job.start_lsn))
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_jobs
            SET state = 'COMPLETE', completed_at = statement_timestamp(),
                wal_bytes = :wal_bytes
            WHERE job_id = :job_id
            """
        ),
        {'job_id': job_id, 'wal_bytes': wal_bytes},
    )
    return ReplacementVerification(
        job_id=job_id,
        verified=True,
        source_relations_changed=0,
        replacement_row_mismatches=0,
        source_rows_remaining_after_swap=0,
        invalid_target_rows=0,
        copied_rows_completed=job.copied_rows_completed,
        copied_rows_total=job.copied_rows_total,
        wal_bytes=wal_bytes,
    )


async def replacement_decoder_retirement_status(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    component: ArchiveComponent,
    version: int,
) -> DecoderRetirement:
    return await decoder_retirement_status(
        connection,
        schema,
        component=component,
        version=version,
    )


async def replacement_storage_bytes(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
) -> int:
    names = (
        await connection.execute(
            text(
                f"""
                SELECT source_relation_name, replacement_relation_name,
                       backup_relation_name, state
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id
                ORDER BY relation_ordinal
                """
            ),
            {'job_id': job_id},
        )
    ).all()
    relations: set[str] = set()
    for row in names:
        state = ReplacementRelationState(row.state)
        if state in {
            ReplacementRelationState.PLANNED,
            ReplacementRelationState.COPYING,
            ReplacementRelationState.COPIED,
            ReplacementRelationState.VERIFIED,
        }:
            relations.add(row.source_relation_name)
            if state is not ReplacementRelationState.PLANNED:
                relations.add(row.replacement_relation_name)
        elif state is ReplacementRelationState.SWAPPED:
            relations.add(row.source_relation_name)
            relations.add(row.backup_relation_name)
        else:
            relations.add(row.source_relation_name)
    total = 0
    for relation in relations:
        total += int(
            (
                await connection.execute(
                    text('SELECT pg_total_relation_size(CAST(:name AS regclass))'),
                    {'name': f'{schema.name}.{relation}'},
                )
            ).scalar_one()
        )
    return total


async def _prepare_replacement_relation(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job: _ReplacementJob,
    relation: _ReplacementRelation,
) -> ReplacementCopyRejected | None:
    source = _qualified(schema, relation.source_relation_name)
    if not await _source_binding_matches(connection, schema, relation):
        return ReplacementCopyRejected(
            job_id=job.job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=ReplacementCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=0,
        )
    observed = (
        await connection.execute(
            text(
                f"""
                SELECT count(*) AS row_count,
                       count(DISTINCT task_id) AS distinct_task_ids
                FROM {source}
                """
            )
        )
    ).one()
    if observed.row_count != relation.row_count or (
        observed.distinct_task_ids != relation.row_count
    ):
        return ReplacementCopyRejected(
            job_id=job.job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=ReplacementCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=observed.row_count,
        )
    invalid = await _invalid_component_rows_in_relation(
        connection,
        schema,
        relation_name=relation.source_relation_name,
        component=ArchiveComponent(job.component),
        version=job.source_version,
    )
    if invalid:
        return ReplacementCopyRejected(
            job_id=job.job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=ReplacementCopyRejectionKind.SOURCE_CORRUPT,
            observed_rows=invalid,
        )
    replacement = _qualified(schema, relation.replacement_relation_name)
    await connection.execute(
        text(
            f'CREATE TABLE {replacement} '
            f'(LIKE {source} INCLUDING ALL '
            f'EXCLUDING CONSTRAINTS EXCLUDING INDEXES)'
        )
    )
    await connection.execute(
        text(
            f'ALTER TABLE {replacement} ADD CONSTRAINT '
            f'{_identifier(_replacement_bound_name(job.job_id, relation.relation_ordinal))} '
            f'CHECK ({relation.partition_constraint})'
        )
    )
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_relations
            SET state = 'COPYING', prepared_at = statement_timestamp()
            WHERE job_id = :job_id
              AND relation_ordinal = :ordinal
            """
        ),
        {'job_id': job.job_id, 'ordinal': relation.relation_ordinal},
    )
    return None


async def _replacement_mismatch_count(
    connection: AsyncConnection,
    *,
    source: str,
    replacement: str,
    columns: tuple[str, ...],
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
) -> int:
    expected = _transformed_select(
        columns,
        component=component,
        source_version=source_version,
        source_codec=source_codec,
        target_version=target_version,
        target_codec=target_codec,
        alias='source',
    )
    encoded_select = _encoded_source_select(
        component,
        alias='source',
        source_version=source_version,
        source_codec=source_codec,
        forward=target_version > source_version,
    )
    expected_columns = ', '.join(
        f'expected.{_identifier(column)}' for column in columns
    )
    replacement_columns = ', '.join(
        f'replacement.{_identifier(column)}' for column in columns
    )
    return (
        await connection.execute(
            text(
                f"""
                WITH encoded AS MATERIALIZED (
                    SELECT {encoded_select}
                    FROM {source} AS source
                ), expected ({_column_list(columns)}) AS MATERIALIZED (
                    SELECT {expected}
                    FROM encoded AS source
                )
                SELECT count(*)
                FROM expected
                FULL OUTER JOIN {replacement} AS replacement USING (task_id)
                WHERE expected.task_id IS NULL
                   OR replacement.task_id IS NULL
                   OR ROW({expected_columns}) IS DISTINCT FROM
                      ROW({replacement_columns})
                """
            )
        )
    ).scalar_one()


async def _restore_source_constraints(
    connection: AsyncConnection,
    *,
    source_relation_oid: int,
    replacement: str,
) -> None:
    constraints = (
        await connection.execute(
            text(
                """
                SELECT conname, pg_get_constraintdef(oid, false) AS definition
                FROM pg_constraint
                WHERE conrelid = :source_relation_oid
                  AND contype = 'c'
                ORDER BY conname
                """
            ),
            {'source_relation_oid': source_relation_oid},
        )
    ).all()
    if not constraints:
        return
    actions = ', '.join(
        f'ADD CONSTRAINT {_identifier(row.conname)} '
        f'{_constraint_definition(row.conname, row.definition)}'
        for row in constraints
    )
    await connection.execute(text(f'ALTER TABLE {replacement} {actions}'))


def _constraint_definition(name: str, definition: str) -> str:
    if name.endswith('terminalization_kind_check'):
        values = ', '.join(f"'{kind.value}'" for kind in TerminalizationKind)
        return f'CHECK (terminalization_kind IN ({values}))'
    return definition


async def _post_swap_verification(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job: _ReplacementJob,
) -> ReplacementVerification:
    component = ArchiveComponent(job.component)
    source_remaining = (
        await decoder_retirement_status(
            connection,
            schema,
            component=component,
            version=job.source_version,
        )
    ).rows_requiring_decoder
    invalid_targets = await _invalid_component_rows(
        connection,
        schema,
        component=component,
        version=job.target_version,
    )
    return ReplacementVerification(
        job_id=job.job_id,
        verified=source_remaining == 0 and invalid_targets == 0,
        source_relations_changed=0,
        replacement_row_mismatches=0,
        source_rows_remaining_after_swap=source_remaining,
        invalid_target_rows=invalid_targets,
        copied_rows_completed=job.copied_rows_completed,
        copied_rows_total=job.copied_rows_total,
        wal_bytes=None,
    )


async def _completed_verification(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job: _ReplacementJob,
) -> ReplacementVerification:
    verification = await _post_swap_verification(connection, schema, job)
    if job.wal_bytes is None:
        raise RuntimeError('completed replacement transcode has no WAL measurement')
    return ReplacementVerification(
        job_id=verification.job_id,
        verified=verification.verified,
        source_relations_changed=verification.source_relations_changed,
        replacement_row_mismatches=verification.replacement_row_mismatches,
        source_rows_remaining_after_swap=(
            verification.source_rows_remaining_after_swap
        ),
        invalid_target_rows=verification.invalid_target_rows,
        copied_rows_completed=verification.copied_rows_completed,
        copied_rows_total=verification.copied_rows_total,
        wal_bytes=job.wal_bytes,
    )


async def _invalid_component_rows(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    component: ArchiveComponent,
    version: int,
) -> int:
    return await _invalid_component_rows_query(
        connection,
        schema,
        relation=f'{schema.sql}.history_aggregate',
        component=component,
        version=version,
    )


async def _invalid_component_rows_in_relation(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation_name: str,
    component: ArchiveComponent,
    version: int,
) -> int:
    return await _invalid_component_rows_query(
        connection,
        schema,
        relation=_qualified(schema, relation_name),
        component=component,
        version=version,
    )


async def _invalid_component_rows_query(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation: str,
    component: ArchiveComponent,
    version: int,
) -> int:
    columns = archive_component_columns(component)
    return (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {relation}
                WHERE {columns.version} = :version
                  AND ({columns.presence_predicate})
                  AND {schema.sql}.archive_component_value_is_valid(
                        :component, {columns.version}, {columns.codec},
                        {columns.content_type}, {columns.payload},
                        {columns.digest}, {columns.form}, {columns.reference}
                      ) IS NOT TRUE
                """
            ),
            {'component': component.value, 'version': version},
        )
    ).scalar_one()


async def _lock_replacement_job(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job_id: str,
) -> _ReplacementJob:
    row = (
        await connection.execute(
            text(
                f"""
                SELECT jobs.*,
                       (
                           SELECT count(*)
                           FROM {schema.sql}.archive_replacement_relations
                           WHERE job_id = jobs.job_id
                       ) AS relation_count
                FROM {schema.sql}.archive_replacement_jobs AS jobs
                WHERE job_id = :job_id
                FOR UPDATE
                """
            ),
            {'job_id': job_id},
        )
    ).mappings().one_or_none()
    if row is None:
        raise ValueError('unknown replacement transcode job')
    return _replacement_job_from_row(row)


async def _require_active_job_maintenance(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job: _ReplacementJob,
) -> None:
    active = (
        await connection.execute(
            text(
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM {schema.sql}.archive_maintenance_sessions
                    WHERE maintenance_id = :maintenance_id
                      AND ended_at IS NULL
                )
                """
            ),
            {'maintenance_id': job.maintenance_id},
        )
    ).scalar_one()
    if not active:
        raise RuntimeError('replacement job has no active maintenance session')


async def _replacement_relations(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job_id: str,
) -> tuple[_ReplacementRelation, ...]:
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT *
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id
                ORDER BY relation_ordinal
                """
            ),
            {'job_id': job_id},
        )
    ).mappings().all()
    return tuple(_replacement_relation_from_row(row) for row in rows)


def _replacement_job_from_row(row: RowMapping) -> _ReplacementJob:
    wal_value = row['wal_bytes']
    return _ReplacementJob(
        job_id=str(row['job_id']),
        maintenance_id=str(row['maintenance_id']),
        component=str(row['component']),
        source_version=int(row['source_version']),
        target_version=int(row['target_version']),
        source_codec=str(row['source_codec']),
        target_codec=str(row['target_codec']),
        state=str(row['state']),
        copied_rows_total=int(row['copied_rows_total']),
        copied_rows_completed=int(row['copied_rows_completed']),
        start_lsn=str(row['start_lsn']),
        wal_bytes=None if wal_value is None else int(wal_value),
        relation_count=int(row['relation_count']),
    )


def _replacement_relation_from_row(
    row: RowMapping,
) -> _ReplacementRelation:
    cursor = row['last_source_ctid']
    return _ReplacementRelation(
        job_id=str(row['job_id']),
        relation_ordinal=int(row['relation_ordinal']),
        source_relation_oid=int(row['source_relation_oid']),
        source_relation_name=str(row['source_relation_name']),
        parent_relation_oid=int(row['parent_relation_oid']),
        parent_relation_name=str(row['parent_relation_name']),
        partition_bound=str(row['partition_bound']),
        partition_constraint=str(row['partition_constraint']),
        replacement_relation_name=str(row['replacement_relation_name']),
        backup_relation_name=str(row['backup_relation_name']),
        state=str(row['state']),
        row_count=int(row['row_count']),
        transformed_rows=int(row['transformed_rows']),
        rows_copied=int(row['rows_copied']),
        last_source_ctid=None if cursor is None else str(cursor),
    )


async def _source_binding_matches(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    relation: _ReplacementRelation,
) -> bool:
    observed = (
        await connection.execute(
            text(
                """
                SELECT child.oid::bigint AS source_oid,
                       parent.oid::bigint AS parent_oid
                FROM pg_class AS child
                JOIN pg_namespace AS namespace
                  ON namespace.oid = child.relnamespace
                JOIN pg_inherits AS inheritance
                  ON inheritance.inhrelid = child.oid
                JOIN pg_class AS parent
                  ON parent.oid = inheritance.inhparent
                WHERE namespace.nspname = :schema_name
                  AND child.relname = :relation_name
                """
            ),
            {
                'schema_name': schema.name,
                'relation_name': relation.source_relation_name,
            },
        )
    ).one_or_none()
    return observed is not None and (
        observed.source_oid == relation.source_relation_oid
        and observed.parent_oid == relation.parent_relation_oid
    )


async def _relation_columns(
    connection: AsyncConnection,
    relation_oid: int,
) -> tuple[str, ...]:
    rows = (
        await connection.execute(
            text(
                """
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = :relation_oid
                  AND attnum > 0
                  AND NOT attisdropped
                ORDER BY attnum
                """
            ),
            {'relation_oid': relation_oid},
        )
    ).scalars()
    columns = tuple(rows)
    if not columns:
        raise RuntimeError('replacement source relation has no visible columns')
    return columns


def _transformed_select(
    columns: tuple[str, ...],
    *,
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
    alias: str,
) -> str:
    condition = _component_source_condition(
        component,
        alias=alias,
        source_version=source_version,
        source_codec=source_codec,
    )
    expressions = {
        column: f'{alias}.{_identifier(column)}'
        for column in columns
    }
    match component:
        case ArchiveComponent.HISTORY_ROW:
            expressions['history_schema_version'] = (
                f'CASE WHEN {condition} THEN {target_version} '
                f'ELSE {alias}.history_schema_version END'
            )
        case ArchiveComponent.RESULT:
            _apply_payload_transform(
                expressions,
                condition=condition,
                alias=alias,
                version_column='result_envelope_version',
                codec_column='result_codec',
                payload_columns=('result_payload', 'prior_result_payload'),
                digest_column='result_digest',
                target_version=target_version,
                target_codec=target_codec,
            )
        case ArchiveComponent.ATTEMPTS:
            _apply_payload_transform(
                expressions,
                condition=condition,
                alias=alias,
                version_column='attempt_archive_version',
                codec_column='attempt_snapshot_codec',
                payload_columns=('attempt_snapshot',),
                digest_column='attempt_snapshot_digest',
                target_version=target_version,
                target_codec=target_codec,
            )
        case ArchiveComponent.RERUN_INPUT:
            _apply_payload_transform(
                expressions,
                condition=condition,
                alias=alias,
                version_column='rerun_input_version',
                codec_column='rerun_input_codec',
                payload_columns=('rerun_input_inline',),
                digest_column='rerun_input_digest',
                target_version=target_version,
                target_codec=target_codec,
            )
    return ', '.join(expressions[column] for column in columns)


def _apply_payload_transform(
    expressions: dict[str, str],
    *,
    condition: str,
    alias: str,
    version_column: str,
    codec_column: str,
    payload_columns: tuple[str, ...],
    digest_column: str,
    target_version: int,
    target_codec: str,
) -> None:
    expressions[version_column] = (
        f'CASE WHEN {condition} THEN {target_version} '
        f'ELSE {alias}.{_identifier(version_column)} END'
    )
    expressions[codec_column] = (
        f"CASE WHEN {condition} THEN '{target_codec}' "
        f'ELSE {alias}.{_identifier(codec_column)} END'
    )
    transformed_payloads: list[str] = []
    for payload_column in payload_columns:
        transformed = f'{alias}.{_identifier(_encoded_payload_name(payload_column))}'
        expressions[payload_column] = transformed
        transformed_payloads.append(expressions[payload_column])
    payload = (
        transformed_payloads[0]
        if len(transformed_payloads) == 1
        else 'COALESCE(' + ', '.join(transformed_payloads) + ')'
    )
    expressions[digest_column] = (
        f'CASE WHEN {condition} AND {payload} IS NOT NULL '
        f'THEN sha256({payload}) ELSE {alias}.{_identifier(digest_column)} END'
    )


def _encoded_source_select(
    component: ArchiveComponent,
    *,
    alias: str,
    source_version: int,
    source_codec: str,
    forward: bool,
) -> str:
    condition = _component_source_condition(
        component,
        alias=alias,
        source_version=source_version,
        source_codec=source_codec,
    )
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return f'{alias}.*'
        case ArchiveComponent.RESULT:
            payload_columns = ('result_payload', 'prior_result_payload')
        case ArchiveComponent.ATTEMPTS:
            payload_columns = ('attempt_snapshot',)
        case ArchiveComponent.RERUN_INPUT:
            payload_columns = ('rerun_input_inline',)
    encoded = [f'{alias}.*']
    for payload_column in payload_columns:
        source = f'{alias}.{_identifier(payload_column)}'
        transformed = (
            f"decode('4832', 'hex') || {source}"
            if forward
            else f'substring({source} FROM 3)'
        )
        encoded.append(
            f'CASE WHEN {condition} AND {source} IS NOT NULL '
            f'THEN {transformed} ELSE {source} END AS '
            f'{_identifier(_encoded_payload_name(payload_column))}'
        )
    return ', '.join(encoded)


def _encoded_payload_name(payload_column: str) -> str:
    return f'archive_target_{payload_column}'


def _component_source_condition(
    component: ArchiveComponent,
    *,
    alias: str,
    source_version: int,
    source_codec: str,
) -> str:
    if component is ArchiveComponent.HISTORY_ROW:
        return f'{alias}.history_schema_version = {source_version}'
    columns = archive_component_columns(component)
    match component:
        case ArchiveComponent.RESULT:
            presence = (
                f'{alias}.result_payload IS NOT NULL '
                f'OR {alias}.prior_result_payload IS NOT NULL'
            )
        case ArchiveComponent.ATTEMPTS:
            presence = f'{alias}.attempt_snapshot IS NOT NULL'
        case ArchiveComponent.RERUN_INPUT:
            presence = f"{alias}.rerun_input_form IN ('INLINE', 'REFERENCE')"
    return (
        f'{alias}.{columns.version} = {source_version} '
        f"AND {alias}.{columns.codec} = '{source_codec}' "
        f'AND ({presence})'
    )


def _column_list(columns: tuple[str, ...]) -> str:
    return ', '.join(_identifier(column) for column in columns)


def _qualified(schema: PrototypeSchema, relation_name: str) -> str:
    return f'{schema.sql}.{_identifier(relation_name)}'


def _identifier(value: str) -> str:
    if not value or len(value) > 63 or not value.replace('_', '').isalnum():
        raise ValueError(f'unsafe PostgreSQL identifier: {value!r}')
    return '"' + value.replace('"', '""') + '"'


def _replacement_bound_name(job_id: str, relation_ordinal: int) -> str:
    suffix = job_id.replace('-', '')[:12]
    return f'archive_replacement_bound_{suffix}_{relation_ordinal}'


def _replacement_index_name(job_id: str, relation_ordinal: int) -> str:
    suffix = job_id.replace('-', '')[:12]
    return f'archive_replacement_id_{suffix}_{relation_ordinal}'


async def _wal_bytes_since(
    connection: AsyncConnection,
    start_lsn: str,
) -> int:
    return int(
        (
            await connection.execute(
                text(
                    """
                    SELECT pg_wal_lsn_diff(
                        pg_current_wal_insert_lsn(),
                        CAST(:start_lsn AS pg_lsn)
                    )
                    """
                ),
                {'start_lsn': start_lsn},
            )
        ).scalar_one()
    )


def _replacement_manifest(schema: PrototypeSchema) -> tuple[str, ...]:
    namespace = schema.sql
    return (
        f"""
        CREATE TABLE {namespace}.archive_replacement_jobs (
            job_id varchar(36) PRIMARY KEY,
            maintenance_id varchar(36) NOT NULL
                REFERENCES {namespace}.archive_maintenance_sessions(
                    maintenance_id
                ),
            component text NOT NULL CHECK (
                component IN (
                    'HISTORY_ROW', 'RESULT', 'ATTEMPTS', 'RERUN_INPUT'
                )
            ),
            source_version smallint NOT NULL,
            target_version smallint NOT NULL,
            source_codec text NOT NULL,
            target_codec text NOT NULL,
            state text NOT NULL CHECK (
                state IN (
                    'PLANNED', 'COPYING', 'COPIED',
                    'VERIFIED', 'SWAPPED', 'COMPLETE'
                )
            ),
            transformed_rows bigint NOT NULL CHECK (transformed_rows >= 0),
            copied_rows_total bigint NOT NULL CHECK (copied_rows_total >= 0),
            copied_rows_completed bigint NOT NULL CHECK (
                copied_rows_completed >= 0
                AND copied_rows_completed <= copied_rows_total
            ),
            payload_rows bigint NOT NULL CHECK (payload_rows >= 0),
            payload_bytes_before bigint NOT NULL CHECK (
                payload_bytes_before >= 0
            ),
            projected_payload_bytes bigint NOT NULL CHECK (
                projected_payload_bytes >= 0
            ),
            affected_relation_bytes bigint NOT NULL CHECK (
                affected_relation_bytes >= 0
            ),
            started_at timestamptz NOT NULL,
            last_batch_at timestamptz,
            copied_at timestamptz,
            verified_at timestamptz,
            swapped_at timestamptz,
            completed_at timestamptz,
            start_lsn pg_lsn NOT NULL,
            wal_bytes bigint CHECK (wal_bytes IS NULL OR wal_bytes >= 0),
            CHECK ((state = 'COMPLETE') = (completed_at IS NOT NULL)),
            CHECK ((state = 'COMPLETE') = (wal_bytes IS NOT NULL))
        )
        """,
        f"""
        CREATE UNIQUE INDEX archive_replacement_single_active_idx
            ON {namespace}.archive_replacement_jobs ((1))
            WHERE state <> 'COMPLETE'
        """,
        f"""
        CREATE TABLE {namespace}.archive_replacement_relations (
            job_id varchar(36) NOT NULL
                REFERENCES {namespace}.archive_replacement_jobs(job_id),
            relation_ordinal integer NOT NULL CHECK (relation_ordinal > 0),
            source_relation_oid bigint NOT NULL,
            source_relation_name text NOT NULL,
            parent_relation_oid bigint NOT NULL,
            parent_relation_name text NOT NULL,
            partition_bound text NOT NULL,
            partition_constraint text NOT NULL,
            replacement_relation_name text NOT NULL,
            backup_relation_name text NOT NULL,
            state text NOT NULL CHECK (
                state IN (
                    'PLANNED', 'COPYING', 'COPIED',
                    'VERIFIED', 'SWAPPED', 'COMPLETE'
                )
            ),
            row_count bigint NOT NULL CHECK (row_count >= 0),
            transformed_rows bigint NOT NULL CHECK (transformed_rows >= 0),
            rows_copied bigint NOT NULL CHECK (
                rows_copied >= 0 AND rows_copied <= row_count
            ),
            relation_bytes bigint NOT NULL CHECK (relation_bytes >= 0),
            last_source_ctid tid,
            prepared_at timestamptz,
            copied_at timestamptz,
            verified_at timestamptz,
            swapped_at timestamptz,
            completed_at timestamptz,
            PRIMARY KEY (job_id, relation_ordinal),
            UNIQUE (job_id, source_relation_name),
            UNIQUE (job_id, replacement_relation_name),
            UNIQUE (job_id, backup_relation_name)
        )
        """,
        f"""
        CREATE TABLE {namespace}.archive_replacement_batches (
            job_id varchar(36) NOT NULL
                REFERENCES {namespace}.archive_replacement_jobs(job_id),
            batch_number integer NOT NULL CHECK (batch_number > 0),
            relation_ordinal integer NOT NULL,
            rows_copied integer NOT NULL CHECK (rows_copied > 0),
            committed_at timestamptz NOT NULL,
            PRIMARY KEY (job_id, batch_number),
            FOREIGN KEY (job_id, relation_ordinal)
                REFERENCES {namespace}.archive_replacement_relations(
                    job_id, relation_ordinal
                )
        )
        """,
    )
