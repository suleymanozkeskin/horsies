"""Replacement-partition executor for offline archive transcoding."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import DBAPIError
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


class ReplacementSwapLockMode(StrEnum):
    PARENT = 'ACCESS_EXCLUSIVE'
    LEAVES = 'SHARE'


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
    metadata_copy_duration_limit_seconds: float | None
    rollback_copied_rows: int
    rollback_peak_additional_disk_budget_bytes: int
    rollback_wal_budget_bytes: int
    rollback_metadata_copy_duration_limit_seconds: float | None
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
class ReplacementSwapBlocker:
    pid: int
    state: str | None
    transaction_age_seconds: float | None
    query: str | None
    backend_type: str
    application_name: str
    relation_name: str
    held_lock_mode: str
    granted: bool


@dataclass(frozen=True, slots=True)
class ReplacementSwapBusy:
    job_id: str
    lock_mode: ReplacementSwapLockMode
    relation_names: tuple[str, ...]
    blockers: tuple[ReplacementSwapBlocker, ...] = ()


type ReplacementSwapOutcome = ReplacementSwap | ReplacementSwapBusy


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
    replacement_relation_oid: int | None
    backup_relation_name: str
    state: str
    row_count: int
    transformed_rows: int
    rows_copied: int
    last_source_ctid: str | None
    source_mutation_generation: int
    replacement_mutation_generation: int
    verified_source_generation: int | None
    verified_replacement_generation: int | None
    verified_source_filenode: int | None
    verified_replacement_filenode: int | None
    verified_source_schema_signature: str | None
    verified_replacement_schema_signature: str | None


@dataclass(frozen=True, slots=True)
class _RelationVerificationToken:
    source_generation: int
    replacement_generation: int
    source_filenode: int
    replacement_filenode: int
    source_schema_signature: str
    replacement_schema_signature: str


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

    metadata_duration = (
        copied_rows / TRANSCODE_MINIMUM_ROWS_PER_SECOND
        if component is ArchiveComponent.HISTORY_ROW
        else None
    )
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
        metadata_copy_duration_limit_seconds=metadata_duration,
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
        rollback_metadata_copy_duration_limit_seconds=metadata_duration,
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
        initial_token = await _relation_verification_token(
            connection,
            schema,
            relation=relation,
            lock_record=False,
        )
        if initial_token is None or not await _source_binding_matches(
            connection,
            schema,
            relation,
        ) or not await _replacement_binding_matches(
            connection,
            schema,
            relation,
        ):
            changed += 1
            await _clear_relation_verification(
                connection,
                schema,
                relation=relation,
            )
            continue
        observed_source = (
            await connection.execute(text(f'SELECT count(*) FROM {source}'))
        ).scalar_one()
        if observed_source != relation.row_count:
            changed += 1
            await _clear_relation_verification(
                connection,
                schema,
                relation=relation,
            )
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
        relation_invalid_targets = 0
        if mismatch:
            relation_invalid_targets = (
                await _invalid_component_rows_in_relation(
                    connection,
                    schema,
                    relation_name=relation.replacement_relation_name,
                    component=ArchiveComponent(job.component),
                    version=job.target_version,
                )
            )
        invalid_targets += relation_invalid_targets
        final_token = await _relation_verification_token(
            connection,
            schema,
            relation=relation,
            lock_record=True,
        )
        stable = final_token is not None and final_token == initial_token
        if not stable:
            changed += 1
        if (
            final_token is not None
            and mismatch == 0
            and relation_invalid_targets == 0
            and stable
        ):
            await connection.execute(
                text(
                    f"""
                    UPDATE {schema.sql}.archive_replacement_relations
                    SET state = 'VERIFIED',
                        verified_at = statement_timestamp(),
                        verified_source_generation = :source_generation,
                        verified_replacement_generation =
                            :replacement_generation,
                        verified_source_filenode = :source_filenode,
                        verified_replacement_filenode = :replacement_filenode,
                        verified_source_schema_signature =
                            :source_schema_signature,
                        verified_replacement_schema_signature =
                            :replacement_schema_signature
                    WHERE job_id = :job_id
                      AND relation_ordinal = :ordinal
                    """
                ),
                {
                    'job_id': job_id,
                    'ordinal': relation.relation_ordinal,
                    'source_generation': final_token.source_generation,
                    'replacement_generation': (
                        final_token.replacement_generation
                    ),
                    'source_filenode': final_token.source_filenode,
                    'replacement_filenode': final_token.replacement_filenode,
                    'source_schema_signature': (
                        final_token.source_schema_signature
                    ),
                    'replacement_schema_signature': (
                        final_token.replacement_schema_signature
                    ),
                },
            )
        else:
            await _clear_relation_verification(
                connection,
                schema,
                relation=relation,
            )
    verified = changed == 0 and mismatches == 0 and invalid_targets == 0
    if verified:
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
    else:
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_replacement_jobs
                SET state = 'COPIED', verified_at = NULL
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
) -> ReplacementSwapOutcome:
    await lock_archive_transcode_program(connection, schema)
    await lock_archive_access_gate(connection, schema)
    job = await _lock_replacement_job(connection, schema, job_id)
    state = ReplacementJobState(job.state)
    if state in {ReplacementJobState.SWAPPED, ReplacementJobState.COMPLETE}:
        return ReplacementSwap(job_id, job.relation_count)
    if state is not ReplacementJobState.VERIFIED:
        raise ValueError('replacement relations must be verified before binding swap')
    await _require_active_job_maintenance(connection, schema, job)
    relations = await _replacement_relations(connection, schema, job_id)
    busy = await _try_replacement_swap_locks(
        connection,
        schema,
        job_id=job_id,
        relations=relations,
    )
    if busy is not None:
        return busy
    changed = 0
    for relation in relations:
        if not await _verified_relation_token_matches(
            connection,
            schema,
            relation=relation,
        ):
            changed += 1
    if changed:
        raise RuntimeError(
            'replacement verification changed before binding swap: '
            f'source_relations_changed={changed}, '
            'replacement_row_mismatches=0, invalid_target_rows=0'
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


async def _try_replacement_swap_locks(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    relations: tuple[_ReplacementRelation, ...],
) -> ReplacementSwapBusy | None:
    parent_names = tuple(
        sorted({row.parent_relation_name for row in relations})
    )
    lock_mode = ReplacementSwapLockMode.PARENT
    relation_names = parent_names
    try:
        async with connection.begin_nested():
            for parent_name in parent_names:
                lock_mode = ReplacementSwapLockMode.PARENT
                relation_names = (parent_name,)
                await connection.execute(
                    text(
                        f'LOCK TABLE {_qualified(schema, parent_name)} '
                        'IN ACCESS EXCLUSIVE MODE NOWAIT'
                    )
                )
            for relation in relations:
                lock_mode = ReplacementSwapLockMode.LEAVES
                relation_names = (
                    relation.source_relation_name,
                    relation.replacement_relation_name,
                )
                await connection.execute(
                    text(
                        f'LOCK TABLE '
                        f'{_qualified(schema, relation.source_relation_name)}, '
                        f'{_qualified(schema, relation.replacement_relation_name)} '
                        'IN SHARE MODE NOWAIT'
                    )
                )
    except DBAPIError as error:
        if _sqlstate(error) != '55P03':
            raise
        qualified_relation_names = tuple(
            f'{schema.name}.{name}' for name in relation_names
        )
        return ReplacementSwapBusy(
            job_id=job_id,
            lock_mode=lock_mode,
            relation_names=qualified_relation_names,
            blockers=await _replacement_swap_blockers(
                connection,
                lock_mode=lock_mode,
                relation_names=qualified_relation_names,
            ),
        )
    return None


async def _replacement_swap_blockers(
    connection: AsyncConnection,
    *,
    lock_mode: ReplacementSwapLockMode,
    relation_names: tuple[str, ...],
) -> tuple[ReplacementSwapBlocker, ...]:
    await connection.execute(text('SELECT pg_stat_clear_snapshot()'))
    rows = (
        await connection.execute(
            text(
                """
                WITH requested AS (
                    SELECT relation_name,
                           to_regclass(relation_name)::oid AS relation_oid
                    FROM unnest(CAST(:relation_names AS text[]))
                         AS names(relation_name)
                )
                SELECT locks.pid,
                       activity.state,
                       EXTRACT(
                           EPOCH FROM clock_timestamp() - activity.xact_start
                       )::double precision AS transaction_age_seconds,
                       LEFT(activity.query, 2048) AS query,
                       activity.backend_type,
                       activity.application_name,
                       requested.relation_name,
                       locks.mode AS held_lock_mode,
                       locks.granted
                FROM requested
                JOIN pg_locks AS locks
                  ON locks.locktype = 'relation'
                 AND locks.relation = requested.relation_oid
                JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                WHERE locks.pid <> pg_backend_pid()
                  AND locks.granted
                  AND (
                      CAST(:requested_mode AS text) = 'ACCESS_EXCLUSIVE'
                      OR locks.mode = ANY(CAST(:share_conflicts AS text[]))
                  )
                ORDER BY locks.pid, requested.relation_name, locks.mode
                """
            ),
            {
                'relation_names': list(relation_names),
                'requested_mode': lock_mode.value,
                'share_conflicts': [
                    'RowExclusiveLock',
                    'ShareUpdateExclusiveLock',
                    'ShareRowExclusiveLock',
                    'ExclusiveLock',
                    'AccessExclusiveLock',
                ],
            },
        )
    ).all()
    return tuple(
        ReplacementSwapBlocker(
            pid=int(row.pid),
            state=row.state,
            transaction_age_seconds=(
                None
                if row.transaction_age_seconds is None
                else float(row.transaction_age_seconds)
            ),
            query=row.query,
            backend_type=str(row.backend_type),
            application_name=str(row.application_name),
            relation_name=str(row.relation_name),
            held_lock_mode=str(row.held_lock_mode),
            granted=bool(row.granted),
        )
        for row in rows
    )


def _sqlstate(error: DBAPIError) -> str | None:
    return getattr(error.orig, 'sqlstate', None) or getattr(
        error.orig,
        'pgcode',
        None,
    )


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
            text(
                f'DROP TRIGGER archive_replacement_target_guard ON '
                f'{_qualified(schema, relation.source_relation_name)}'
            )
        )
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
    await _install_source_mutation_guards(
        connection,
        schema,
        relation_name=relation.source_relation_name,
    )
    replacement = _qualified(schema, relation.replacement_relation_name)
    await connection.execute(
        text(
            f'CREATE TABLE {replacement} '
            f'(LIKE {source} INCLUDING ALL '
            f'EXCLUDING CONSTRAINTS EXCLUDING INDEXES)'
        )
    )
    replacement_oid = int(
        (
            await connection.execute(
                text('SELECT CAST(:relation_name AS regclass)::oid::bigint'),
                {
                    'relation_name': (
                        f'{schema.name}.{relation.replacement_relation_name}'
                    )
                },
            )
        ).scalar_one()
    )
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_relations
            SET replacement_relation_oid = :replacement_oid
            WHERE job_id = :job_id
              AND relation_ordinal = :ordinal
            """
        ),
        {
            'replacement_oid': replacement_oid,
            'job_id': job.job_id,
            'ordinal': relation.relation_ordinal,
        },
    )
    await _install_replacement_mutation_guards(
        connection,
        schema,
        relation_name=relation.replacement_relation_name,
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


async def _install_source_mutation_guards(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation_name: str,
) -> None:
    relation = _qualified(schema, relation_name)
    function = f'{schema.sql}.archive_replacement_note_mutation()'
    await connection.execute(
        text(
            f"""
            CREATE TRIGGER archive_replacement_source_row_guard
            AFTER INSERT OR UPDATE OR DELETE ON {relation}
            FOR EACH ROW EXECUTE FUNCTION {function}
            """
        )
    )
    await connection.execute(
        text(
            f"""
            CREATE TRIGGER archive_replacement_source_truncate_guard
            AFTER TRUNCATE ON {relation}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}
            """
        )
    )


async def _install_replacement_mutation_guards(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation_name: str,
) -> None:
    relation = _qualified(schema, relation_name)
    await connection.execute(
        text(
            f"""
            CREATE TRIGGER archive_replacement_target_guard
            AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation}
            FOR EACH STATEMENT EXECUTE FUNCTION
                {schema.sql}.archive_replacement_note_mutation()
            """
        )
    )


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
    relations = await _replacement_relations(connection, schema, job.job_id)
    changed = 0
    for relation in relations:
        if not await _post_swap_relation_token_matches(
            connection,
            schema,
            relation=relation,
        ):
            changed += 1
    source_remaining = (
        await decoder_retirement_status(
            connection,
            schema,
            component=component,
            version=job.source_version,
        )
    ).rows_requiring_decoder
    return ReplacementVerification(
        job_id=job.job_id,
        verified=changed == 0 and source_remaining == 0,
        source_relations_changed=changed,
        replacement_row_mismatches=0,
        source_rows_remaining_after_swap=source_remaining,
        invalid_target_rows=0,
        copied_rows_completed=job.copied_rows_completed,
        copied_rows_total=job.copied_rows_total,
        wal_bytes=None,
    )


async def _completed_verification(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job: _ReplacementJob,
) -> ReplacementVerification:
    if job.wal_bytes is None:
        raise RuntimeError('completed replacement transcode has no WAL measurement')
    source_remaining = (
        await decoder_retirement_status(
            connection,
            schema,
            component=ArchiveComponent(job.component),
            version=job.source_version,
        )
    ).rows_requiring_decoder
    return ReplacementVerification(
        job_id=job.job_id,
        verified=source_remaining == 0,
        source_relations_changed=0,
        replacement_row_mismatches=0,
        source_rows_remaining_after_swap=source_remaining,
        invalid_target_rows=0,
        copied_rows_completed=job.copied_rows_completed,
        copied_rows_total=job.copied_rows_total,
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
    replacement_oid = row['replacement_relation_oid']
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
        replacement_relation_oid=(
            None if replacement_oid is None else int(replacement_oid)
        ),
        backup_relation_name=str(row['backup_relation_name']),
        state=str(row['state']),
        row_count=int(row['row_count']),
        transformed_rows=int(row['transformed_rows']),
        rows_copied=int(row['rows_copied']),
        last_source_ctid=None if cursor is None else str(cursor),
        source_mutation_generation=int(row['source_mutation_generation']),
        replacement_mutation_generation=int(
            row['replacement_mutation_generation']
        ),
        verified_source_generation=(
            None
            if row['verified_source_generation'] is None
            else int(row['verified_source_generation'])
        ),
        verified_replacement_generation=(
            None
            if row['verified_replacement_generation'] is None
            else int(row['verified_replacement_generation'])
        ),
        verified_source_filenode=(
            None
            if row['verified_source_filenode'] is None
            else int(row['verified_source_filenode'])
        ),
        verified_replacement_filenode=(
            None
            if row['verified_replacement_filenode'] is None
            else int(row['verified_replacement_filenode'])
        ),
        verified_source_schema_signature=(
            None
            if row['verified_source_schema_signature'] is None
            else str(row['verified_source_schema_signature'])
        ),
        verified_replacement_schema_signature=(
            None
            if row['verified_replacement_schema_signature'] is None
            else str(row['verified_replacement_schema_signature'])
        ),
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


async def _replacement_binding_matches(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    relation: _ReplacementRelation,
) -> bool:
    if relation.replacement_relation_oid is None:
        return False
    observed = (
        await connection.execute(
            text(
                """
                SELECT relation.oid::bigint AS relation_oid
                FROM pg_class AS relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = :schema_name
                  AND relation.relname = :relation_name
                """
            ),
            {
                'schema_name': schema.name,
                'relation_name': relation.replacement_relation_name,
            },
        )
    ).scalar_one_or_none()
    return observed == relation.replacement_relation_oid


async def _relation_verification_token(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation: _ReplacementRelation,
    lock_record: bool,
) -> _RelationVerificationToken | None:
    if relation.replacement_relation_oid is None:
        return None
    lock_clause = 'FOR UPDATE' if lock_record else ''
    row = (
        await connection.execute(
            text(
                f"""
                SELECT source_mutation_generation,
                       replacement_mutation_generation,
                       pg_relation_filenode(
                           CAST(source_relation_oid AS oid)
                       )::bigint AS source_filenode,
                       pg_relation_filenode(
                           CAST(replacement_relation_oid AS oid)
                       )::bigint AS replacement_filenode
                FROM {schema.sql}.archive_replacement_relations
                WHERE job_id = :job_id
                  AND relation_ordinal = :ordinal
                {lock_clause}
                """
            ),
            {
                'job_id': relation.job_id,
                'ordinal': relation.relation_ordinal,
            },
        )
    ).one_or_none()
    if (
        row is None
        or row.source_filenode is None
        or row.replacement_filenode is None
    ):
        return None
    source_schema_signature = await _relation_schema_signature(
        connection,
        relation.source_relation_oid,
    )
    replacement_schema_signature = await _relation_schema_signature(
        connection,
        relation.replacement_relation_oid,
    )
    if (
        source_schema_signature is None
        or replacement_schema_signature is None
    ):
        return None
    return _RelationVerificationToken(
        source_generation=int(row.source_mutation_generation),
        replacement_generation=int(row.replacement_mutation_generation),
        source_filenode=int(row.source_filenode),
        replacement_filenode=int(row.replacement_filenode),
        source_schema_signature=source_schema_signature,
        replacement_schema_signature=replacement_schema_signature,
    )


# HAZARD, documented deliberately rather than fixed: the four deparse calls
# below — pg_get_expr over column defaults, pg_get_constraintdef,
# pg_get_indexdef, pg_get_triggerdef — all render in the CALLING SESSION's
# settings. This signature is captured during verification and compared inside
# the binding-swap transaction, so a session-timezone difference between those
# two points fails the swap on a relation that never changed, for any relation
# carrying a timestamptz default or a timestamptz CHECK or partition bound.
#
# Left as-is on purpose. This executor is a frozen qualified reference: it runs
# only in single-zone qualification dispatches, where the hazard is dormant,
# and editing its capture after qualification would drift the reference away
# from the shape that was qualified. The production executor carries the
# session-independent requirement instead, with its own multi-timezone pin.
#
# Classification, and the rest of the family's call sites, are enumerated in
# roadmap/catalog-deparse-session-dependence-sweep-2026-08-07.md (sites 12-15).
async def _relation_schema_signature(
    connection: AsyncConnection,
    relation_oid: int,
) -> str | None:
    return (
        await connection.execute(
            text(
                """
                SELECT encode(sha256(convert_to(
                    jsonb_build_object(
                        'relation', jsonb_build_array(
                            relation.relkind,
                            relation.relpersistence,
                            relation.relam,
                            relation.reloptions
                        ),
                        'columns', COALESCE((
                            SELECT jsonb_agg(
                                jsonb_build_array(
                                    attribute.attnum,
                                    attribute.attname,
                                    attribute.atttypid,
                                    attribute.atttypmod,
                                    attribute.attcollation,
                                    attribute.attnotnull,
                                    attribute.attidentity,
                                    attribute.attgenerated,
                                    pg_get_expr(defaults.adbin, defaults.adrelid)
                                ) ORDER BY attribute.attnum
                            )
                            FROM pg_attribute AS attribute
                            LEFT JOIN pg_attrdef AS defaults
                              ON defaults.adrelid = attribute.attrelid
                             AND defaults.adnum = attribute.attnum
                            WHERE attribute.attrelid = relation.oid
                              AND attribute.attnum > 0
                              AND NOT attribute.attisdropped
                        ), '[]'::jsonb),
                        'constraints', COALESCE((
                            SELECT jsonb_agg(
                                jsonb_build_array(
                                    constraints.conname,
                                    constraints.contype,
                                    constraints.convalidated,
                                    pg_get_constraintdef(
                                        constraints.oid,
                                        false
                                    )
                                ) ORDER BY constraints.conname
                            )
                            FROM pg_constraint AS constraints
                            WHERE constraints.conrelid = relation.oid
                        ), '[]'::jsonb),
                        'indexes', COALESCE((
                            SELECT jsonb_agg(
                                jsonb_build_array(
                                    indexes.indisvalid,
                                    indexes.indisready,
                                    pg_get_indexdef(indexes.indexrelid)
                                ) ORDER BY indexes.indexrelid
                            )
                            FROM pg_index AS indexes
                            WHERE indexes.indrelid = relation.oid
                        ), '[]'::jsonb),
                        'triggers', COALESCE((
                            SELECT jsonb_agg(
                                jsonb_build_array(
                                    triggers.tgenabled,
                                    pg_get_triggerdef(triggers.oid, false)
                                ) ORDER BY triggers.tgname
                            )
                            FROM pg_trigger AS triggers
                            WHERE triggers.tgrelid = relation.oid
                              AND NOT triggers.tgisinternal
                        ), '[]'::jsonb)
                    )::text,
                    'UTF8'
                )), 'hex')
                FROM pg_class AS relation
                WHERE relation.oid = CAST(:relation_oid AS oid)
                """
            ),
            {'relation_oid': relation_oid},
        )
    ).scalar_one_or_none()


async def _clear_relation_verification(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation: _ReplacementRelation,
) -> None:
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_replacement_relations
            SET state = 'COPIED',
                verified_at = NULL,
                verified_source_generation = NULL,
                verified_replacement_generation = NULL,
                verified_source_filenode = NULL,
                verified_replacement_filenode = NULL,
                verified_source_schema_signature = NULL,
                verified_replacement_schema_signature = NULL
            WHERE job_id = :job_id
              AND relation_ordinal = :ordinal
            """
        ),
        {
            'job_id': relation.job_id,
            'ordinal': relation.relation_ordinal,
        },
    )


async def _verified_relation_token_matches(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation: _ReplacementRelation,
) -> bool:
    if relation.state != ReplacementRelationState.VERIFIED.value:
        return False
    expected = (
        relation.verified_source_generation,
        relation.verified_replacement_generation,
        relation.verified_source_filenode,
        relation.verified_replacement_filenode,
        relation.verified_source_schema_signature,
        relation.verified_replacement_schema_signature,
    )
    if any(value is None for value in expected):
        return False
    if not await _source_binding_matches(connection, schema, relation):
        return False
    if not await _replacement_binding_matches(connection, schema, relation):
        return False
    observed = await _relation_verification_token(
        connection,
        schema,
        relation=relation,
        lock_record=True,
    )
    return observed is not None and (
        observed.source_generation == relation.verified_source_generation
        and observed.replacement_generation
        == relation.verified_replacement_generation
        and observed.source_filenode == relation.verified_source_filenode
        and observed.replacement_filenode
        == relation.verified_replacement_filenode
        and observed.source_schema_signature
        == relation.verified_source_schema_signature
        and observed.replacement_schema_signature
        == relation.verified_replacement_schema_signature
    )


async def _post_swap_relation_token_matches(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    relation: _ReplacementRelation,
) -> bool:
    expected = (
        relation.verified_source_generation,
        relation.verified_replacement_generation,
        relation.verified_source_filenode,
        relation.verified_replacement_filenode,
        relation.replacement_relation_oid,
        relation.verified_source_schema_signature,
        relation.verified_replacement_schema_signature,
    )
    if any(value is None for value in expected):
        return False
    row = (
        await connection.execute(
            text(
                f"""
                SELECT relations.source_mutation_generation,
                       relations.replacement_mutation_generation,
                       canonical.oid::bigint AS canonical_oid,
                       backup.oid::bigint AS backup_oid,
                       inheritance.inhparent::bigint AS parent_oid,
                       pg_relation_filenode(canonical.oid)::bigint
                           AS canonical_filenode,
                       pg_relation_filenode(backup.oid)::bigint
                           AS backup_filenode
                FROM {schema.sql}.archive_replacement_relations AS relations
                JOIN pg_class AS canonical
                  ON canonical.relname = relations.source_relation_name
                JOIN pg_namespace AS canonical_namespace
                  ON canonical_namespace.oid = canonical.relnamespace
                 AND canonical_namespace.nspname = :schema_name
                JOIN pg_class AS backup
                  ON backup.relname = relations.backup_relation_name
                JOIN pg_namespace AS backup_namespace
                  ON backup_namespace.oid = backup.relnamespace
                 AND backup_namespace.nspname = :schema_name
                JOIN pg_inherits AS inheritance
                  ON inheritance.inhrelid = canonical.oid
                WHERE relations.job_id = :job_id
                  AND relations.relation_ordinal = :ordinal
                  AND relations.state = 'SWAPPED'
                FOR UPDATE OF relations
                """
            ),
            {
                'schema_name': schema.name,
                'job_id': relation.job_id,
                'ordinal': relation.relation_ordinal,
            },
        )
    ).one_or_none()
    return row is not None and (
        row.source_mutation_generation == relation.verified_source_generation
        and row.replacement_mutation_generation
        == relation.verified_replacement_generation
        and row.canonical_oid == relation.replacement_relation_oid
        and row.backup_oid == relation.source_relation_oid
        and row.parent_oid == relation.parent_relation_oid
        and row.canonical_filenode == relation.verified_replacement_filenode
        and row.backup_filenode == relation.verified_source_filenode
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
            replacement_relation_oid bigint,
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
            source_mutation_generation bigint NOT NULL DEFAULT 0 CHECK (
                source_mutation_generation >= 0
            ),
            replacement_mutation_generation bigint NOT NULL DEFAULT 0 CHECK (
                replacement_mutation_generation >= 0
            ),
            verified_source_generation bigint CHECK (
                verified_source_generation IS NULL
                OR verified_source_generation >= 0
            ),
            verified_replacement_generation bigint CHECK (
                verified_replacement_generation IS NULL
                OR verified_replacement_generation >= 0
            ),
            verified_source_filenode bigint,
            verified_replacement_filenode bigint,
            verified_source_schema_signature text,
            verified_replacement_schema_signature text,
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
        f"""
        CREATE FUNCTION {namespace}.archive_replacement_note_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            changed_rows integer;
        BEGIN
            UPDATE {namespace}.archive_replacement_relations
            SET source_mutation_generation =
                    source_mutation_generation
                    + CASE WHEN source_relation_oid = TG_RELID
                           THEN 1 ELSE 0 END,
                replacement_mutation_generation =
                    replacement_mutation_generation
                    + CASE WHEN replacement_relation_oid = TG_RELID
                           THEN 1 ELSE 0 END
            WHERE state <> 'COMPLETE'
              AND (
                    source_relation_oid = TG_RELID
                    OR replacement_relation_oid = TG_RELID
                  );
            GET DIAGNOSTICS changed_rows = ROW_COUNT;
            IF changed_rows <> 1 THEN
                RAISE EXCEPTION
                    'archive replacement mutation guard has % owners for %',
                    changed_rows, TG_RELID;
            END IF;
            RETURN NULL;
        END
        $function$
        """,
    )


# The operational evidence collector uses the executor's physical-copy
# primitives so its control differs only in payload transformation.
replacement_column_list = _column_list
replacement_component_source_condition = _component_source_condition
replacement_constraint_definition = _constraint_definition
replacement_identifier = _identifier
replacement_qualified_relation = _qualified
replacement_relation_columns = _relation_columns
