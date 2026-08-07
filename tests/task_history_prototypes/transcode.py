"""Offline archive-transcoding program for disposable history schemas."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.archive import (
    ARCHIVE_CODEC,
    ARCHIVE_CODEC_V2,
    ARCHIVE_FRAME_V2,
)
from tests.task_history_prototypes.schema import PrototypeSchema


ARCHIVE_CODEC_V1 = ARCHIVE_CODEC
TRANSCODE_MINIMUM_ROWS_PER_SECOND = 20_000


class ArchiveComponent(StrEnum):
    HISTORY_ROW = 'HISTORY_ROW'
    RESULT = 'RESULT'
    ATTEMPTS = 'ATTEMPTS'
    RERUN_INPUT = 'RERUN_INPUT'


class TranscodeJobState(StrEnum):
    PLANNED = 'PLANNED'
    RUNNING = 'RUNNING'
    VERIFIED = 'VERIFIED'


class TranscodeRejectionKind(StrEnum):
    UNSUPPORTED_DIRECTION = 'UNSUPPORTED_DIRECTION'
    MAINTENANCE_REQUIRED = 'MAINTENANCE_REQUIRED'
    ACTIVE_JOB = 'ACTIVE_JOB'
    SOURCE_CORRUPT = 'SOURCE_CORRUPT'


class TranscodeBatchRejectionKind(StrEnum):
    SOURCE_CORRUPT = 'SOURCE_CORRUPT'
    SOURCE_SET_CHANGED = 'SOURCE_SET_CHANGED'


@dataclass(frozen=True, slots=True)
class TranscodePlan:
    job_id: str
    component: ArchiveComponent
    source_version: int
    target_version: int
    affected_rows: int
    payload_rows: int
    payload_bytes: int
    projected_payload_bytes: int
    affected_relation_bytes: int
    relation_count: int
    peak_additional_disk_budget_bytes: int
    wal_budget_bytes: int
    rewrite_duration_limit_seconds: float
    rollback_rows: int
    rollback_payload_bytes: int
    rollback_peak_additional_disk_budget_bytes: int
    rollback_wal_budget_bytes: int
    rollback_duration_limit_seconds: float
    reversible: bool


@dataclass(frozen=True, slots=True)
class TranscodeRejected:
    kind: TranscodeRejectionKind
    affected_rows: int


type PlanTranscodeOutcome = TranscodePlan | TranscodeRejected


@dataclass(frozen=True, slots=True)
class TranscodeBatch:
    job_id: str
    batch_number: int
    rows_rewritten: int
    payload_bytes_after: int
    rows_completed: int
    rows_total: int


@dataclass(frozen=True, slots=True)
class TranscodeBatchRejected:
    job_id: str
    kind: TranscodeBatchRejectionKind
    observed_rows: int


@dataclass(frozen=True, slots=True)
class TranscodeReadyForVerification:
    job_id: str
    rows_total: int


type RunTranscodeOutcome = (
    TranscodeBatch | TranscodeBatchRejected | TranscodeReadyForVerification
)


@dataclass(frozen=True, slots=True)
class TranscodeVerification:
    job_id: str
    verified: bool
    source_rows_remaining: int
    invalid_target_rows: int
    rows_completed: int
    rows_total: int
    wal_bytes: int


@dataclass(frozen=True, slots=True)
class DecoderRetirement:
    component: ArchiveComponent
    version: int
    rows_requiring_decoder: int

    @property
    def ready(self) -> bool:
        return self.rows_requiring_decoder == 0


@dataclass(frozen=True, slots=True)
class ArchiveMaintenanceSession:
    maintenance_id: str


@dataclass(frozen=True, slots=True)
class ArchiveVersionInventory:
    component: ArchiveComponent
    version: int
    codec: str | None
    affected_rows: int
    payload_rows: int
    payload_bytes: int
    relation_count: int
    invalid_rows: int


@dataclass(frozen=True, slots=True)
class _ComponentColumns:
    version: str
    codec: str
    content_type: str
    payload: str
    digest: str
    presence_predicate: str
    form: str
    reference: str
    metadata_only: bool
    payload_set: str
    updated_payload: str


async def install_archive_transcode_prototype(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    for statement in _transcode_manifest(schema):
        await connection.execute(text(statement))


async def begin_archive_maintenance(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    maintenance_id: str,
) -> ArchiveMaintenanceSession:
    await _lock_transcode_program(connection, schema)
    await _lock_archive_access_gate(connection, schema)
    active = (
        await connection.execute(
            text(
                f"""
                SELECT maintenance_id
                FROM {schema.sql}.archive_maintenance_sessions
                WHERE ended_at IS NULL
                """
            )
        )
    ).scalar_one_or_none()
    if active is not None:
        raise ValueError('archive maintenance is already active')
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.archive_maintenance_sessions (
                maintenance_id, started_at
            ) VALUES (:maintenance_id, statement_timestamp())
            """
        ),
        {'maintenance_id': maintenance_id},
    )
    return ArchiveMaintenanceSession(maintenance_id=maintenance_id)


async def finish_archive_maintenance(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    maintenance_id: str,
) -> None:
    await _lock_transcode_program(connection, schema)
    await _lock_archive_access_gate(connection, schema)
    active_jobs = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.archive_transcode_jobs
                WHERE maintenance_id = :maintenance_id
                  AND state IN ('PLANNED', 'RUNNING')
                """
            ),
            {'maintenance_id': maintenance_id},
        )
    ).scalar_one()
    if active_jobs:
        raise ValueError('archive maintenance has an unfinished transcode job')
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


async def inventory_archive_versions(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> tuple[ArchiveVersionInventory, ...]:
    await _lock_transcode_program(connection, schema)
    if await _active_maintenance_id(connection, schema) is None:
        raise RuntimeError('archive inventory requires active maintenance')
    inventory: list[ArchiveVersionInventory] = []
    for component in ArchiveComponent:
        columns = _component_columns(component)
        rows = (
            await connection.execute(
                text(
                    f"""
                    SELECT {columns.version} AS archive_version,
                           {columns.codec} AS archive_codec,
                           count(*) AS affected_rows,
                           count({columns.payload}) AS payload_rows,
                           COALESCE(
                               sum(octet_length({columns.payload})), 0
                           ) AS payload_bytes,
                           count(DISTINCT tableoid) AS relation_count,
                           count(*) FILTER (
                               WHERE {schema.sql}.
                                   archive_component_value_is_valid(
                                       :component, {columns.version},
                                       {columns.codec}, {columns.content_type},
                                       {columns.payload},
                                       {columns.digest}, {columns.form},
                                       {columns.reference}
                                   ) IS NOT TRUE
                           ) AS invalid_rows
                    FROM {schema.sql}.history_aggregate
                    WHERE {columns.presence_predicate}
                    GROUP BY {columns.version}, {columns.codec}
                    ORDER BY {columns.version}, {columns.codec}
                    """
                ),
                {'component': component.value},
            )
        ).all()
        inventory.extend(
            ArchiveVersionInventory(
                component=component,
                version=row.archive_version,
                codec=row.archive_codec,
                affected_rows=row.affected_rows,
                payload_rows=row.payload_rows,
                payload_bytes=row.payload_bytes,
                relation_count=row.relation_count,
                invalid_rows=row.invalid_rows,
            )
            for row in rows
        )
    return tuple(inventory)


async def plan_archive_transcode(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    component: ArchiveComponent,
    source_version: int,
    target_version: int,
) -> PlanTranscodeOutcome:
    direction = _direction(source_version, target_version)
    if direction is None:
        return TranscodeRejected(TranscodeRejectionKind.UNSUPPORTED_DIRECTION, 0)
    columns = _component_columns(component)
    await _lock_transcode_program(connection, schema)
    maintenance_id = await _active_maintenance_id(connection, schema)
    if maintenance_id is None:
        return TranscodeRejected(
            TranscodeRejectionKind.MAINTENANCE_REQUIRED,
            0,
        )
    active = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.archive_transcode_jobs
                WHERE state IN ('PLANNED', 'RUNNING')
                """
            )
        )
    ).scalar_one()
    if active:
        return TranscodeRejected(TranscodeRejectionKind.ACTIVE_JOB, active)

    source_codec = _codec_for_version(component, source_version)
    corrupt_rows = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.history_aggregate
                WHERE {columns.version} = :source_version
                  AND ({columns.presence_predicate})
                  AND {schema.sql}.archive_component_value_is_valid(
                        :component, {columns.version}, {columns.codec},
                        {columns.content_type}, {columns.payload},
                        {columns.digest},
                        {columns.form}, {columns.reference}
                      ) IS NOT TRUE
                """
            ),
            {
                'component': component.value,
                'source_version': source_version,
            },
        )
    ).scalar_one()
    if corrupt_rows:
        return TranscodeRejected(
            TranscodeRejectionKind.SOURCE_CORRUPT,
            corrupt_rows,
        )

    inventory = (
        await connection.execute(
            text(
                f"""
                SELECT tableoid::regclass::text AS relation_name,
                       count(*) AS row_count,
                       count({columns.payload}) AS payload_row_count,
                       COALESCE(sum(octet_length({columns.payload})), 0)
                           AS payload_bytes,
                       pg_total_relation_size(tableoid) AS relation_bytes
                FROM {schema.sql}.history_aggregate
                WHERE {columns.version} = :source_version
                  AND {columns.codec} = :source_codec
                  AND ({columns.presence_predicate})
                GROUP BY tableoid
                ORDER BY tableoid::regclass::text
                """
            ),
            {
                'source_version': source_version,
                'source_codec': source_codec,
            },
        )
    ).all()
    affected_rows = sum(row.row_count for row in inventory)
    payload_rows = sum(row.payload_row_count for row in inventory)
    payload_bytes = sum(row.payload_bytes for row in inventory)
    relation_bytes = sum(row.relation_bytes for row in inventory)
    projected_payload_bytes = (
        payload_bytes + len(ARCHIVE_FRAME_V2) * payload_rows
        if direction == 'FORWARD'
        else payload_bytes - len(ARCHIVE_FRAME_V2) * payload_rows
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.archive_transcode_jobs (
                job_id, maintenance_id, component,
                source_version, target_version,
                source_codec, target_codec, state, rows_total,
                payload_bytes_before, projected_payload_bytes,
                affected_relation_bytes, rows_completed,
                started_at, start_lsn
            ) VALUES (
                :job_id, :maintenance_id, :component,
                :source_version, :target_version,
                :source_codec, :target_codec, 'PLANNED', :rows_total,
                :payload_bytes, :projected_payload_bytes,
                :relation_bytes, 0, statement_timestamp(),
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
            'target_codec': _codec_for_version(component, target_version),
            'rows_total': affected_rows,
            'payload_bytes': payload_bytes,
            'projected_payload_bytes': projected_payload_bytes,
            'relation_bytes': relation_bytes,
        },
    )
    for row in inventory:
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.archive_transcode_inventory (
                    job_id, relation_name, row_count, payload_row_count,
                    payload_bytes, relation_bytes
                ) VALUES (
                    :job_id, :relation_name, :row_count, :payload_row_count,
                    :payload_bytes, :relation_bytes
                )
                """
            ),
            {
                'job_id': job_id,
                'relation_name': row.relation_name,
                'row_count': row.row_count,
                'payload_row_count': row.payload_row_count,
                'payload_bytes': row.payload_bytes,
                'relation_bytes': row.relation_bytes,
            },
        )
    return TranscodePlan(
        job_id=job_id,
        component=component,
        source_version=source_version,
        target_version=target_version,
        affected_rows=affected_rows,
        payload_rows=payload_rows,
        payload_bytes=payload_bytes,
        projected_payload_bytes=projected_payload_bytes,
        affected_relation_bytes=relation_bytes,
        relation_count=len(inventory),
        peak_additional_disk_budget_bytes=_ratio_ceiling(
            relation_bytes,
            numerator=5,
            denominator=4,
        ),
        wal_budget_bytes=_ratio_ceiling(
            relation_bytes,
            numerator=3,
            denominator=2,
        ),
        rewrite_duration_limit_seconds=(
            affected_rows / TRANSCODE_MINIMUM_ROWS_PER_SECOND
        ),
        rollback_rows=affected_rows,
        rollback_payload_bytes=projected_payload_bytes,
        rollback_peak_additional_disk_budget_bytes=_ratio_ceiling(
            relation_bytes,
            numerator=5,
            denominator=4,
        ),
        rollback_wal_budget_bytes=_ratio_ceiling(
            relation_bytes,
            numerator=3,
            denominator=2,
        ),
        rollback_duration_limit_seconds=(
            affected_rows / TRANSCODE_MINIMUM_ROWS_PER_SECOND
        ),
        reversible=True,
    )


async def run_archive_transcode_batch(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
    batch_size: int,
) -> RunTranscodeOutcome:
    if batch_size <= 0:
        raise ValueError('batch size must be positive')
    await _lock_transcode_program(connection, schema)
    job = await _lock_job(connection, schema, job_id)
    if job.state == TranscodeJobState.VERIFIED:
        raise ValueError('verified transcode job is immutable')
    if not await _job_maintenance_is_active(
        connection,
        schema,
        maintenance_id=str(job.maintenance_id),
    ):
        raise RuntimeError('transcode job has no active maintenance session')
    component = ArchiveComponent(job.component)
    columns = _component_columns(component)
    observed_source_rows = await _decoder_row_count(
        connection,
        schema,
        component=component,
        version=job.source_version,
    )
    expected_source_rows = job.rows_total - job.rows_completed
    if observed_source_rows != expected_source_rows:
        return TranscodeBatchRejected(
            job_id=job_id,
            kind=TranscodeBatchRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=observed_source_rows,
        )
    if expected_source_rows == 0:
        return TranscodeReadyForVerification(
            job_id=job_id,
            rows_total=job.rows_total,
        )
    corrupt_rows = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.history_aggregate
                WHERE {columns.version} = :source_version
                  AND ({columns.presence_predicate})
                  AND {schema.sql}.archive_component_value_is_valid(
                        :component, {columns.version}, {columns.codec},
                        {columns.content_type}, {columns.payload},
                        {columns.digest},
                        {columns.form}, {columns.reference}
                      ) IS NOT TRUE
                """
            ),
            {
                'component': component.value,
                'source_version': job.source_version,
            },
        )
    ).scalar_one()
    if corrupt_rows:
        return TranscodeBatchRejected(
            job_id=job_id,
            kind=TranscodeBatchRejectionKind.SOURCE_CORRUPT,
            observed_rows=corrupt_rows,
        )

    payload_expression = (
        'CASE WHEN target.source_payload IS NULL THEN NULL '
        "ELSE decode('4832', 'hex') || target.source_payload END"
        if _direction(job.source_version, job.target_version) == 'FORWARD'
        else (
            'CASE WHEN target.source_payload IS NULL THEN NULL '
            'ELSE substring(target.source_payload FROM 3) END'
        )
    )
    set_clause = f'{columns.version} = :target_version'
    payload_size_expression = '0'
    if not columns.metadata_only:
        set_clause += f""",
                        {columns.codec} = :target_codec,
                        {columns.payload_set},
                        {columns.digest} = CASE
                            WHEN encoded.target_payload IS NULL
                                THEN encoded.source_digest
                            ELSE sha256(encoded.target_payload)
                        END"""
        payload_size_expression = f'octet_length({columns.updated_payload})'
    rewritten = (
        await connection.execute(
            text(
                f"""
                WITH targets AS MATERIALIZED (
                    SELECT tableoid AS source_tableoid,
                           ctid AS source_ctid,
                           {columns.payload} AS source_payload,
                           {columns.digest} AS source_digest
                    FROM {schema.sql}.history_aggregate
                    WHERE {columns.version} = :source_version
                      AND {columns.codec} = :source_codec
                      AND ({columns.presence_predicate})
                    ORDER BY task_id
                    LIMIT :batch_size
                    FOR UPDATE
                ), encoded AS (
                    SELECT source_tableoid, source_ctid,
                           source_digest,
                           {payload_expression} AS target_payload
                    FROM targets AS target
                ), updated AS (
                    UPDATE {schema.sql}.history_aggregate AS history
                    SET {set_clause}
                    FROM encoded
                    WHERE history.tableoid = encoded.source_tableoid
                      AND history.ctid = encoded.source_ctid
                    RETURNING {payload_size_expression} AS payload_bytes
                )
                SELECT count(*) AS row_count,
                       COALESCE(sum(payload_bytes), 0) AS payload_bytes
                FROM updated
                """
            ),
            {
                'source_version': job.source_version,
                'source_codec': job.source_codec,
                'target_version': job.target_version,
                'target_codec': job.target_codec,
                'batch_size': batch_size,
            },
        )
    ).one()
    batch_number = (
        await connection.execute(
            text(
                f"""
                SELECT COALESCE(max(batch_number), 0) + 1
                FROM {schema.sql}.archive_transcode_batches
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id},
        )
    ).scalar_one()
    await connection.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.archive_transcode_batches (
                job_id, batch_number, rows_rewritten,
                payload_bytes_after, committed_at
            ) VALUES (
                :job_id, :batch_number, :row_count,
                :payload_bytes, statement_timestamp()
            )
            """
        ),
        {
            'job_id': job_id,
            'batch_number': batch_number,
            'row_count': rewritten.row_count,
            'payload_bytes': rewritten.payload_bytes,
        },
    )
    rows_completed = job.rows_completed + rewritten.row_count
    await connection.execute(
        text(
            f"""
            UPDATE {schema.sql}.archive_transcode_jobs
            SET state = 'RUNNING', rows_completed = :rows_completed,
                last_batch_at = statement_timestamp()
            WHERE job_id = :job_id
            """
        ),
        {'job_id': job_id, 'rows_completed': rows_completed},
    )
    return TranscodeBatch(
        job_id=job_id,
        batch_number=batch_number,
        rows_rewritten=rewritten.row_count,
        payload_bytes_after=rewritten.payload_bytes,
        rows_completed=rows_completed,
        rows_total=job.rows_total,
    )


async def verify_archive_transcode(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
) -> TranscodeVerification:
    await _lock_transcode_program(connection, schema)
    job = await _lock_job(connection, schema, job_id)
    if job.state != TranscodeJobState.VERIFIED and not await _job_maintenance_is_active(
        connection,
        schema,
        maintenance_id=str(job.maintenance_id),
    ):
        raise RuntimeError('transcode job has no active maintenance session')
    component = ArchiveComponent(job.component)
    columns = _component_columns(component)
    source_rows_remaining = await _decoder_row_count(
        connection,
        schema,
        component=component,
        version=job.source_version,
    )
    invalid_target_rows = (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.history_aggregate
                WHERE {columns.version} = :target_version
                  AND ({columns.presence_predicate})
                  AND {schema.sql}.archive_component_value_is_valid(
                        :component, {columns.version}, {columns.codec},
                        {columns.content_type}, {columns.payload},
                        {columns.digest},
                        {columns.form}, {columns.reference}
                      ) IS NOT TRUE
                """
            ),
            {
                'component': component.value,
                'target_version': job.target_version,
            },
        )
    ).scalar_one()
    verified = (
        source_rows_remaining == 0
        and invalid_target_rows == 0
        and job.rows_completed == job.rows_total
    )
    if job.state == TranscodeJobState.VERIFIED:
        if job.wal_bytes is None:
            raise RuntimeError('verified transcode job has no WAL measurement')
        wal_bytes = job.wal_bytes
    else:
        wal_bytes = int(
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
                    {'start_lsn': str(job.start_lsn)},
                )
            ).scalar_one()
        )
    if verified and job.state != TranscodeJobState.VERIFIED:
        await connection.execute(
            text(
                f"""
                UPDATE {schema.sql}.archive_transcode_jobs
                SET state = 'VERIFIED', verified_at = statement_timestamp(),
                    wal_bytes = :wal_bytes
                WHERE job_id = :job_id
                """
            ),
            {'job_id': job_id, 'wal_bytes': wal_bytes},
        )
    return TranscodeVerification(
        job_id=job_id,
        verified=verified,
        source_rows_remaining=source_rows_remaining,
        invalid_target_rows=invalid_target_rows,
        rows_completed=job.rows_completed,
        rows_total=job.rows_total,
        wal_bytes=wal_bytes,
    )


async def decoder_retirement_status(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    component: ArchiveComponent,
    version: int,
) -> DecoderRetirement:
    return DecoderRetirement(
        component=component,
        version=version,
        rows_requiring_decoder=await _decoder_row_count(
            connection,
            schema,
            component=component,
            version=version,
        ),
    )


async def _decoder_row_count(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    component: ArchiveComponent,
    version: int,
) -> int:
    columns = _component_columns(component)
    return (
        await connection.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.history_aggregate
                WHERE {columns.version} = :version
                  AND ({columns.presence_predicate})
                """
            ),
            {'version': version},
        )
    ).scalar_one()


async def _lock_job(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    job_id: str,
):
    job = (
        await connection.execute(
            text(
                f"""
                SELECT * FROM {schema.sql}.archive_transcode_jobs
                WHERE job_id = :job_id
                FOR UPDATE
                """
            ),
            {'job_id': job_id},
        )
    ).one_or_none()
    if job is None:
        raise ValueError('transcode job is absent')
    return job


async def _job_maintenance_is_active(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    maintenance_id: str,
) -> bool:
    return (
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
            {'maintenance_id': maintenance_id},
        )
    ).scalar_one()


async def _active_maintenance_id(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> str | None:
    return (
        await connection.execute(
            text(
                f"""
                SELECT maintenance_id
                FROM {schema.sql}.archive_maintenance_sessions
                WHERE ended_at IS NULL
                """
            )
        )
    ).scalar_one_or_none()


async def active_archive_maintenance_id(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> str | None:
    """Return the active archive-maintenance identity, if one exists."""
    return await _active_maintenance_id(connection, schema)


async def _lock_transcode_program(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    await connection.execute(
        text(
            f"""
            SELECT pg_advisory_xact_lock(
                hashtextextended(
                    :schema_name || chr(31) || 'archive_transcode',
                    1709
                )
            )
            """
        ),
        {'schema_name': schema.name},
    )


async def lock_archive_transcode_program(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    """Serialize archive-transcode command mutations for this schema."""
    await _lock_transcode_program(connection, schema)


async def _lock_archive_access_gate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    await connection.execute(
        text(
            f"""
            SELECT singleton
            FROM {schema.sql}.archive_access_gate
            WHERE singleton IS TRUE
            FOR UPDATE
            """
        )
    )


async def lock_archive_access_gate(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> None:
    """Lock the archive-access gate for an offline maintenance mutation."""
    await _lock_archive_access_gate(connection, schema)


def _direction(source_version: int, target_version: int) -> str | None:
    match source_version, target_version:
        case 1, 2:
            return 'FORWARD'
        case 2, 1:
            return 'REVERSE'
        case _:
            return None


def archive_transcode_direction(
    source_version: int,
    target_version: int,
) -> str | None:
    """Return the supported prototype direction for a version pair."""
    return _direction(source_version, target_version)


def _ratio_ceiling(value: int, *, numerator: int, denominator: int) -> int:
    return (value * numerator + denominator - 1) // denominator


def ratio_ceiling(value: int, *, numerator: int, denominator: int) -> int:
    """Return an integer ratio rounded toward positive infinity."""
    return _ratio_ceiling(value, numerator=numerator, denominator=denominator)


def _codec_for_version(component: ArchiveComponent, version: int) -> str:
    match component, version:
        case ArchiveComponent.HISTORY_ROW, 1:
            return 'row-v1'
        case ArchiveComponent.HISTORY_ROW, 2:
            return 'row-v2'
        case _, 1:
            return ARCHIVE_CODEC_V1
        case _, 2:
            return ARCHIVE_CODEC_V2
        case _:
            raise ValueError(
                f'{component.value} version {version} has no prototype codec'
            )


def archive_codec_for_version(component: ArchiveComponent, version: int) -> str:
    """Return the codec fixed to a prototype archive-domain version."""
    return _codec_for_version(component, version)


def _component_columns(component: ArchiveComponent) -> _ComponentColumns:
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return _ComponentColumns(
                version='history_schema_version',
                codec=(
                    'CASE history_schema_version '
                    "WHEN 1 THEN 'row-v1' WHEN 2 THEN 'row-v2' END"
                ),
                content_type='NULL::text',
                payload='NULL::bytea',
                digest='NULL::bytea',
                presence_predicate='TRUE',
                form='NULL::text',
                reference='NULL::text',
                metadata_only=True,
                payload_set='history_schema_version = :target_version',
                updated_payload='NULL::bytea',
            )
        case ArchiveComponent.RESULT:
            return _ComponentColumns(
                version='result_envelope_version',
                codec='result_codec',
                content_type='result_content_type',
                payload='COALESCE(result_payload, prior_result_payload)',
                digest='result_digest',
                presence_predicate=(
                    'result_payload IS NOT NULL ' 'OR prior_result_payload IS NOT NULL'
                ),
                form='NULL::text',
                reference='NULL::text',
                metadata_only=False,
                payload_set=(
                    'result_payload = CASE '
                    'WHEN history.result_payload IS NULL THEN NULL '
                    'ELSE encoded.target_payload END, '
                    'prior_result_payload = CASE '
                    'WHEN history.prior_result_payload IS NULL THEN NULL '
                    'ELSE encoded.target_payload END'
                ),
                updated_payload=(
                    'COALESCE(history.result_payload, ' 'history.prior_result_payload)'
                ),
            )
        case ArchiveComponent.ATTEMPTS:
            return _ComponentColumns(
                version='attempt_archive_version',
                codec='attempt_snapshot_codec',
                content_type='attempt_snapshot_content_type',
                payload='attempt_snapshot',
                digest='attempt_snapshot_digest',
                presence_predicate='attempt_snapshot IS NOT NULL',
                form='NULL::text',
                reference='NULL::text',
                metadata_only=False,
                payload_set='attempt_snapshot = encoded.target_payload',
                updated_payload='history.attempt_snapshot',
            )
        case ArchiveComponent.RERUN_INPUT:
            return _ComponentColumns(
                version='rerun_input_version',
                codec='rerun_input_codec',
                content_type='rerun_input_content_type',
                payload='rerun_input_inline',
                digest='rerun_input_digest',
                presence_predicate=("rerun_input_form IN ('INLINE', 'REFERENCE')"),
                form='rerun_input_form',
                reference='rerun_input_reference',
                metadata_only=False,
                payload_set='rerun_input_inline = encoded.target_payload',
                updated_payload='history.rerun_input_inline',
            )


def archive_component_columns(component: ArchiveComponent) -> _ComponentColumns:
    """Return the physical columns that implement one archive domain."""
    return _component_columns(component)


def _transcode_manifest(schema: PrototypeSchema) -> tuple[str, ...]:
    namespace = schema.sql
    return (
        f"""
        CREATE TABLE {namespace}.archive_access_gate (
            singleton boolean PRIMARY KEY CHECK (singleton IS TRUE)
        )
        """,
        f"""
        INSERT INTO {namespace}.archive_access_gate (singleton) VALUES (TRUE)
        """,
        f"""
        CREATE TABLE {namespace}.archive_maintenance_sessions (
            maintenance_id varchar(36) PRIMARY KEY,
            started_at timestamptz NOT NULL,
            ended_at timestamptz,
            CHECK (ended_at IS NULL OR ended_at >= started_at)
        )
        """,
        f"""
        CREATE UNIQUE INDEX archive_maintenance_single_active_idx
            ON {namespace}.archive_maintenance_sessions ((1))
            WHERE ended_at IS NULL
        """,
        f"""
        CREATE TABLE {namespace}.archive_transcode_jobs (
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
                state IN ('PLANNED', 'RUNNING', 'VERIFIED')
            ),
            rows_total bigint NOT NULL CHECK (rows_total >= 0),
            rows_completed bigint NOT NULL CHECK (
                rows_completed >= 0 AND rows_completed <= rows_total
            ),
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
            verified_at timestamptz,
            start_lsn pg_lsn NOT NULL,
            wal_bytes bigint CHECK (wal_bytes IS NULL OR wal_bytes >= 0),
            CHECK (
                (state = 'VERIFIED') =
                (verified_at IS NOT NULL AND wal_bytes IS NOT NULL)
            )
        )
        """,
        f"""
        CREATE UNIQUE INDEX archive_transcode_single_active_idx
            ON {namespace}.archive_transcode_jobs ((1))
            WHERE state IN ('PLANNED', 'RUNNING')
        """,
        f"""
        CREATE TABLE {namespace}.archive_transcode_inventory (
            job_id varchar(36) NOT NULL
                REFERENCES {namespace}.archive_transcode_jobs(job_id),
            relation_name text NOT NULL,
            row_count bigint NOT NULL CHECK (row_count >= 0),
            payload_row_count bigint NOT NULL CHECK (
                payload_row_count >= 0 AND payload_row_count <= row_count
            ),
            payload_bytes bigint NOT NULL CHECK (payload_bytes >= 0),
            relation_bytes bigint NOT NULL CHECK (relation_bytes >= 0),
            PRIMARY KEY (job_id, relation_name)
        )
        """,
        f"""
        CREATE TABLE {namespace}.archive_transcode_batches (
            job_id varchar(36) NOT NULL
                REFERENCES {namespace}.archive_transcode_jobs(job_id),
            batch_number integer NOT NULL CHECK (batch_number > 0),
            rows_rewritten integer NOT NULL CHECK (rows_rewritten > 0),
            payload_bytes_after bigint NOT NULL CHECK (payload_bytes_after >= 0),
            committed_at timestamptz NOT NULL,
            PRIMARY KEY (job_id, batch_number)
        )
        """,
        _archive_value_validator(namespace),
    )


def _archive_value_validator(namespace: str) -> str:
    return f"""
    CREATE FUNCTION {namespace}.archive_component_value_is_valid(
        p_component text,
        p_version smallint,
        p_codec text,
        p_content_type text,
        p_payload bytea,
        p_digest bytea,
        p_form text,
        p_reference text
    ) RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    AS $function$
    DECLARE
        v_value_bytes bytea;
        v_json jsonb;
        v_item jsonb;
        v_ordinality bigint;
        v_timestamp timestamptz;
        v_worker_pid integer;
    BEGIN
        IF p_component = 'HISTORY_ROW' THEN
            RETURN p_payload IS NULL
                AND p_digest IS NULL
                AND p_content_type IS NULL
                AND p_form IS NULL
                AND p_reference IS NULL
                AND (
                    (p_version = 1 AND p_codec = 'row-v1')
                    OR (p_version = 2 AND p_codec = 'row-v2')
                );
        END IF;
        IF p_content_type <> 'application/json' THEN
            RETURN FALSE;
        END IF;
        CASE p_version
            WHEN 1 THEN
                IF p_codec <> 'json-utf8' THEN
                    RETURN FALSE;
                END IF;
                v_value_bytes := p_payload;
            WHEN 2 THEN
                IF p_codec <> 'framed-json-v2' THEN
                    RETURN FALSE;
                END IF;
                IF p_payload IS NOT NULL
                   AND substring(p_payload FROM 1 FOR 2)
                       <> decode('4832', 'hex') THEN
                    RETURN FALSE;
                END IF;
                v_value_bytes := substring(p_payload FROM 3);
            ELSE
                RETURN FALSE;
        END CASE;
        CASE p_component
            WHEN 'RESULT' THEN
                IF p_payload IS NULL OR p_digest IS NULL
                   OR p_form IS NOT NULL OR p_reference IS NOT NULL
                   OR sha256(p_payload) <> p_digest THEN
                    RETURN FALSE;
                END IF;
                v_json := convert_from(v_value_bytes, 'UTF8')::jsonb;
                RETURN TRUE;
            WHEN 'RERUN_INPUT' THEN
                CASE p_form
                    WHEN 'INLINE' THEN
                        RETURN p_payload IS NOT NULL
                            AND p_reference IS NULL
                            AND p_digest IS NOT NULL
                            AND sha256(p_payload) = p_digest;
                    WHEN 'REFERENCE' THEN
                        RETURN p_payload IS NULL
                            AND p_reference IS NOT NULL
                            AND length(p_reference) > 0
                            AND p_digest IS NOT NULL
                            AND octet_length(p_digest) = 32;
                    ELSE
                        RETURN FALSE;
                END CASE;
            WHEN 'ATTEMPTS' THEN
                IF p_payload IS NULL OR p_digest IS NULL
                   OR p_form IS NOT NULL OR p_reference IS NOT NULL
                   OR sha256(p_payload) <> p_digest THEN
                    RETURN FALSE;
                END IF;
                v_json := convert_from(v_value_bytes, 'UTF8')::jsonb;
                IF jsonb_typeof(v_json) <> 'array' THEN
                    RETURN FALSE;
                END IF;
                FOR v_item, v_ordinality IN
                    SELECT value, ordinality
                    FROM jsonb_array_elements(v_json) WITH ORDINALITY
                LOOP
                    IF jsonb_typeof(v_item) <> 'array'
                       OR jsonb_array_length(v_item) <> 12
                       OR jsonb_typeof(v_item -> 0) <> 'number'
                       OR (v_item ->> 0)::bigint <> v_ordinality
                       OR jsonb_typeof(v_item -> 1) <> 'string'
                       OR jsonb_typeof(v_item -> 2) <> 'boolean'
                       OR jsonb_typeof(v_item -> 3) <> 'number'
                       OR (v_item ->> 3) !~ '^-?[0-9]+$'
                       OR jsonb_typeof(v_item -> 4) <> 'number'
                       OR (v_item ->> 4) !~ '^-?[0-9]+$'
                       OR jsonb_typeof(v_item -> 5)
                            NOT IN ('string', 'null')
                       OR jsonb_typeof(v_item -> 6)
                            NOT IN ('string', 'null')
                       OR jsonb_typeof(v_item -> 7)
                            NOT IN ('string', 'null')
                       OR jsonb_typeof(v_item -> 8)
                            NOT IN ('string', 'null')
                       OR jsonb_typeof(v_item -> 9)
                            NOT IN ('string', 'null')
                       OR jsonb_typeof(v_item -> 10)
                            NOT IN ('number', 'null')
                       OR jsonb_typeof(v_item -> 11)
                            NOT IN ('string', 'null') THEN
                        RETURN FALSE;
                    END IF;
                    v_timestamp := to_timestamp(
                        (v_item ->> 3)::numeric / 1000000
                    );
                    v_timestamp := to_timestamp(
                        (v_item ->> 4)::numeric / 1000000
                    );
                    IF jsonb_typeof(v_item -> 10) = 'number' THEN
                        v_worker_pid := (v_item ->> 10)::integer;
                        IF to_jsonb(v_worker_pid) <> v_item -> 10 THEN
                            RETURN FALSE;
                        END IF;
                    END IF;
                END LOOP;
                RETURN TRUE;
            ELSE
                RETURN FALSE;
        END CASE;
    EXCEPTION WHEN OTHERS THEN
            RETURN FALSE;
    END
    $function$
    """
