"""Operational evidence for replacement-partition archive transcoding."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_operational_conditions,
)
from tests.task_history_prototypes.replacement_transcode import (
    ReplacementCopyBatch,
    ReplacementReadyForVerification,
    ReplacementSwap,
    ReplacementSwapBusy,
    ReplacementTranscodePlan,
    ReplacementVerification,
    begin_replacement_archive_maintenance,
    finalize_replacement_archive_transcode,
    finish_replacement_archive_maintenance,
    install_replacement_archive_transcode_prototype,
    plan_replacement_archive_transcode,
    replacement_storage_bytes,
    run_replacement_copy_batch,
    swap_verified_replacement_partitions,
    verify_replacement_archive_transcode,
    replacement_column_list,
    replacement_component_source_condition,
    replacement_constraint_definition,
    replacement_identifier,
    replacement_qualified_relation,
    replacement_relation_columns,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import (
    TRANSCODE_MINIMUM_ROWS_PER_SECOND,
    ArchiveComponent,
    archive_codec_for_version,
    archive_component_columns,
)
from tests.task_history_prototypes.transcode_evidence import (
    seed_archive_transcode_workload,
    validate_archive_transcode_workload,
)


PAYLOAD_CONTROL_MINIMUM_RATIO = 0.50
REPLACEMENT_MAINTENANCE_MAX_SECONDS = 10 * 60
REPLACEMENT_SWAP_LOCK_MAX_SECONDS = 2.0
REPLACEMENT_SWAP_LOCK_MAX_ATTEMPTS = 120
REPLACEMENT_SWAP_RETRY_BACKOFF_SECONDS = 0.250


def replacement_throughput_passed(
    *,
    component: ArchiveComponent,
    candidate_rows_per_second: float,
    control_rows_per_second: float | None,
) -> bool:
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return (
                candidate_rows_per_second
                >= TRANSCODE_MINIMUM_ROWS_PER_SECOND
            )
        case (
            ArchiveComponent.RESULT
            | ArchiveComponent.ATTEMPTS
            | ArchiveComponent.RERUN_INPUT
        ):
            return control_rows_per_second is not None and (
                candidate_rows_per_second / control_rows_per_second
                >= PAYLOAD_CONTROL_MINIMUM_RATIO
            )


@dataclass(frozen=True, slots=True)
class PlainCopyHashControl:
    copied_rows: int
    payload_bytes_hashed: int
    batches: int
    copy_seconds: float
    copied_rows_per_second: float
    payload_bytes_per_second: float


@dataclass(frozen=True, slots=True)
class ReplacementArchiveTranscodeBudgets:
    metadata_tasks_per_second_minimum: int
    payload_control_ratio_minimum: float
    maintenance_seconds_maximum: int
    swap_lock_seconds_maximum: float
    swap_lock_attempts_maximum: int
    swap_retry_backoff_seconds: float


@dataclass(frozen=True, slots=True)
class ReplacementArchiveTranscodeEvidence:
    conditions: EvidenceConditions
    component: ArchiveComponent
    budgets: ReplacementArchiveTranscodeBudgets
    workload: dict[str, int]
    batches: int
    plan: ReplacementTranscodePlan
    baseline_relation_bytes: int
    peak_relation_bytes: int
    final_relation_bytes: int
    peak_additional_bytes: int
    copy_seconds: float
    copy_storage_probe_seconds: float
    control: PlainCopyHashControl | None
    candidate_control_ratio: float | None
    swap_lock_seconds: float
    swap_attempts: int
    swap_busy_attempts: int
    swap_busy_seconds: float
    swap_retry_sleep_seconds: float
    maintenance_seconds: float
    copied_rows_per_second: float
    verification: ReplacementVerification
    decoder_retirement_ready: bool
    throughput_passed: bool
    maintenance_duration_passed: bool
    swap_window_passed: bool
    wal_passed: bool
    peak_disk_passed: bool


@dataclass(frozen=True, slots=True)
class _ReplacementSwapExecution:
    swap: ReplacementSwap
    successful_lock_seconds: float
    attempts: int
    busy_attempts: int
    busy_seconds: float
    retry_sleep_seconds: float


async def collect_replacement_archive_transcode_evidence(
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
) -> ReplacementArchiveTranscodeEvidence:
    validate_archive_transcode_workload(
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
            'explicit checkpoint; payload controls receive an untimed source '
            'validation warmup before the paired control and candidate; '
            'replacement relations use committed task-ID batches'
        ),
        prepared_posture='parameterized seed and durable replacement-copy program',
    )
    schema = PrototypeSchema(f'history_replacement_evidence_{uuid4().hex[:8]}')
    await install_archive_candidates(connection, schema)
    await install_replacement_archive_transcode_prototype(connection, schema)
    await connection.commit()
    try:
        await seed_archive_transcode_workload(
            connection,
            schema,
            rows=rows,
            payload_bytes=payload_bytes,
            attempts_per_task=attempts_per_task,
        )
        await connection.commit()
        await connection.execute(text('CHECKPOINT'))
        control = (
            None
            if component is ArchiveComponent.HISTORY_ROW
            else await _measure_plain_copy_hash_control(
                connection,
                schema,
                component=component,
                source_version=1,
                batch_size=batch_size,
            )
        )
        await connection.execute(text('CHECKPOINT'))
        maintenance_started = time.perf_counter()
        maintenance_id = str(uuid4())
        await begin_replacement_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await connection.commit()
        job_id = str(uuid4())
        planned = await plan_replacement_archive_transcode(
            connection,
            schema,
            job_id=job_id,
            component=component,
            source_version=1,
            target_version=2,
        )
        if not isinstance(planned, ReplacementTranscodePlan):
            raise RuntimeError(
                f'replacement planning rejected evidence: {planned!r}'
            )
        _verify_planned_workload(planned, rows=rows)
        if control is not None and (
            control.copied_rows != planned.copied_rows
            or control.payload_bytes_hashed != planned.payload_bytes
        ):
            raise RuntimeError(
                'paired control did not copy and hash the candidate workload'
            )
        await connection.commit()

        baseline_bytes = await replacement_storage_bytes(
            connection,
            schema,
            job_id=job_id,
        )
        peak_bytes = baseline_bytes
        batches = 0
        storage_probe_seconds = 0.0
        copy_started = time.perf_counter()
        while True:
            copied = await run_replacement_copy_batch(
                connection,
                schema,
                job_id=job_id,
                batch_size=batch_size,
            )
            await connection.commit()
            storage_probe_started = time.perf_counter()
            observed_bytes = await replacement_storage_bytes(
                connection,
                schema,
                job_id=job_id,
            )
            storage_probe_seconds += (
                time.perf_counter() - storage_probe_started
            )
            peak_bytes = max(peak_bytes, observed_bytes)
            match copied:
                case ReplacementCopyBatch():
                    batches += 1
                case ReplacementReadyForVerification():
                    break
                case _:
                    raise RuntimeError(
                        f'replacement copy rejected evidence: {copied!r}'
                    )
        copy_seconds = (
            time.perf_counter() - copy_started - storage_probe_seconds
        )

        pre_swap_verification = await verify_replacement_archive_transcode(
            connection,
            schema,
            job_id=job_id,
        )
        if not pre_swap_verification.verified:
            raise RuntimeError(
                'replacement content verification failed before binding swap: '
                f'{pre_swap_verification!r}'
            )
        await connection.commit()

        swap_execution = await execute_replacement_binding_swap(
            connection,
            schema,
            job_id=job_id,
        )
        peak_bytes = max(
            peak_bytes,
            await replacement_storage_bytes(
                connection,
                schema,
                job_id=job_id,
            ),
        )
        verification = await finalize_replacement_archive_transcode(
            connection,
            schema,
            job_id=job_id,
        )
        await connection.commit()
        final_bytes = await replacement_storage_bytes(
            connection,
            schema,
            job_id=job_id,
        )
        await finish_replacement_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await connection.commit()
        maintenance_seconds = time.perf_counter() - maintenance_started

        if verification.wal_bytes is None:
            raise RuntimeError('replacement verification omitted WAL bytes')
        retirement_ready = (
            verification.source_rows_remaining_after_swap == 0
        )
        peak_additional = max(0, peak_bytes - baseline_bytes)
        copied_rows_per_second = planned.copied_rows / copy_seconds
        candidate_control_ratio = (
            None
            if control is None
            else copied_rows_per_second / control.copied_rows_per_second
        )
        throughput_passed = replacement_throughput_passed(
            component=component,
            candidate_rows_per_second=copied_rows_per_second,
            control_rows_per_second=(
                None if control is None else control.copied_rows_per_second
            ),
        )
        return ReplacementArchiveTranscodeEvidence(
            conditions=conditions,
            component=component,
            budgets=ReplacementArchiveTranscodeBudgets(
                metadata_tasks_per_second_minimum=(
                    TRANSCODE_MINIMUM_ROWS_PER_SECOND
                ),
                payload_control_ratio_minimum=PAYLOAD_CONTROL_MINIMUM_RATIO,
                maintenance_seconds_maximum=(
                    REPLACEMENT_MAINTENANCE_MAX_SECONDS
                ),
                swap_lock_seconds_maximum=REPLACEMENT_SWAP_LOCK_MAX_SECONDS,
                swap_lock_attempts_maximum=(
                    REPLACEMENT_SWAP_LOCK_MAX_ATTEMPTS
                ),
                swap_retry_backoff_seconds=(
                    REPLACEMENT_SWAP_RETRY_BACKOFF_SECONDS
                ),
            ),
            workload={
                'rows': rows,
                'batch_size': batch_size,
                'payload_bytes': payload_bytes,
                'attempts_per_task': attempts_per_task,
            },
            batches=batches,
            plan=planned,
            baseline_relation_bytes=baseline_bytes,
            peak_relation_bytes=peak_bytes,
            final_relation_bytes=final_bytes,
            peak_additional_bytes=peak_additional,
            copy_seconds=copy_seconds,
            copy_storage_probe_seconds=storage_probe_seconds,
            control=control,
            candidate_control_ratio=candidate_control_ratio,
            swap_lock_seconds=swap_execution.successful_lock_seconds,
            swap_attempts=swap_execution.attempts,
            swap_busy_attempts=swap_execution.busy_attempts,
            swap_busy_seconds=swap_execution.busy_seconds,
            swap_retry_sleep_seconds=swap_execution.retry_sleep_seconds,
            maintenance_seconds=maintenance_seconds,
            copied_rows_per_second=copied_rows_per_second,
            verification=verification,
            decoder_retirement_ready=retirement_ready,
            throughput_passed=throughput_passed,
            maintenance_duration_passed=(
                maintenance_seconds <= REPLACEMENT_MAINTENANCE_MAX_SECONDS
            ),
            swap_window_passed=(
                swap_execution.successful_lock_seconds
                <= REPLACEMENT_SWAP_LOCK_MAX_SECONDS
            ),
            wal_passed=(verification.wal_bytes <= planned.wal_budget_bytes),
            peak_disk_passed=(
                peak_additional <= planned.peak_additional_disk_budget_bytes
            ),
        )
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()


async def execute_replacement_binding_swap(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    job_id: str,
) -> _ReplacementSwapExecution:
    busy_attempts = 0
    busy_seconds = 0.0
    retry_sleep_seconds = 0.0
    for attempt in range(1, REPLACEMENT_SWAP_LOCK_MAX_ATTEMPTS + 1):
        attempt_started = time.perf_counter()
        outcome = await swap_verified_replacement_partitions(
            connection,
            schema,
            job_id=job_id,
        )
        match outcome:
            case ReplacementSwap():
                await connection.commit()
                return _ReplacementSwapExecution(
                    swap=outcome,
                    successful_lock_seconds=(
                        time.perf_counter() - attempt_started
                    ),
                    attempts=attempt,
                    busy_attempts=busy_attempts,
                    busy_seconds=busy_seconds,
                    retry_sleep_seconds=retry_sleep_seconds,
                )
            case ReplacementSwapBusy():
                await connection.rollback()
                busy_attempts += 1
                busy_seconds += time.perf_counter() - attempt_started
                if attempt == REPLACEMENT_SWAP_LOCK_MAX_ATTEMPTS:
                    raise RuntimeError(
                        'replacement binding swap remained busy after '
                        f'{attempt} non-queuing attempts: {outcome!r}'
                    )
                await asyncio.sleep(REPLACEMENT_SWAP_RETRY_BACKOFF_SECONDS)
                retry_sleep_seconds += REPLACEMENT_SWAP_RETRY_BACKOFF_SECONDS
    raise AssertionError('replacement binding-swap retry loop was not exhaustive')


async def _measure_plain_copy_hash_control(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    component: ArchiveComponent,
    source_version: int,
    batch_size: int,
) -> PlainCopyHashControl:
    if component is ArchiveComponent.HISTORY_ROW:
        raise ValueError('history-row metadata uses an absolute throughput gate')
    component_columns = archive_component_columns(component)
    source_codec = archive_codec_for_version(component, source_version)
    inventory = (
        await connection.execute(
            text(
                f"""
                SELECT history.tableoid::oid::bigint AS relation_oid,
                       child.relname AS relation_name,
                       pg_get_partition_constraintdef(child.oid)
                           AS partition_constraint,
                       count(*) AS row_count,
                       COALESCE(sum(octet_length({component_columns.payload}))
                           FILTER (
                               WHERE {component_columns.version} =
                                     :source_version
                                 AND {component_columns.codec} = :source_codec
                                 AND ({component_columns.presence_predicate})
                           ), 0) AS payload_bytes
                FROM {schema.sql}.history_aggregate AS history
                JOIN pg_class AS child ON child.oid = history.tableoid
                GROUP BY history.tableoid, child.relname, child.oid
                HAVING count(*) FILTER (
                    WHERE {component_columns.version} = :source_version
                      AND {component_columns.codec} = :source_codec
                      AND ({component_columns.presence_predicate})
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
    control_token = uuid4().hex[:12]
    control_relations: list[str] = []
    copied_rows = 0
    payload_bytes_hashed = sum(int(row.payload_bytes) for row in inventory)
    batches = 0
    for source_row in inventory:
        await _validate_plain_copy_source(
            connection,
            schema,
            source=replacement_qualified_relation(
                schema,
                source_row.relation_name,
            ),
            expected_rows=int(source_row.row_count),
            component=component,
            source_version=source_version,
        )
    await connection.rollback()
    copy_seconds = 0.0
    try:
        for ordinal, source_row in enumerate(inventory, start=1):
            relation_started = time.perf_counter()
            source = replacement_qualified_relation(
                schema,
                source_row.relation_name,
            )
            control_name = f'archive_copy_control_{control_token}_{ordinal}'
            control = replacement_qualified_relation(schema, control_name)
            control_relations.append(control_name)
            await _validate_plain_copy_source(
                connection,
                schema,
                source=source,
                expected_rows=int(source_row.row_count),
                component=component,
                source_version=source_version,
            )
            await connection.execute(
                text(
                    f'CREATE TABLE {control} '
                    f'(LIKE {source} INCLUDING ALL '
                    f'EXCLUDING CONSTRAINTS EXCLUDING INDEXES)'
                )
            )
            await connection.execute(
                text(
                    f'ALTER TABLE {control} ADD CONSTRAINT '
                    f'{replacement_identifier(f"archive_copy_control_bound_{control_token}_{ordinal}")} '
                    f'CHECK ({source_row.partition_constraint})'
                )
            )
            columns = await replacement_relation_columns(
                connection,
                int(source_row.relation_oid),
            )
            control_select = _plain_copy_hash_select(
                columns,
                component=component,
                source_version=source_version,
                source_codec=source_codec,
                alias='source',
            )
            last_source_ctid: str | None = None
            relation_rows = 0
            while relation_rows < int(source_row.row_count):
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
                            ), inserted AS (
                                INSERT INTO {control} (
                                    {replacement_column_list(columns)}
                                )
                                SELECT {control_select}
                                FROM source_batch AS source
                                RETURNING task_id
                            )
                            SELECT count(*) AS rows_copied,
                                   (
                                       SELECT source_ctid::text
                                       FROM source_batch
                                       ORDER BY source_batch.source_ctid DESC
                                       LIMIT 1
                                   ) AS last_source_ctid
                            FROM inserted
                            """
                        ),
                        {
                            'last_source_ctid': last_source_ctid,
                            'batch_size': batch_size,
                        },
                    )
                ).one()
                if inserted.rows_copied <= 0:
                    raise RuntimeError(
                        'plain copy-and-hash control stopped before source end'
                    )
                relation_rows += int(inserted.rows_copied)
                copied_rows += int(inserted.rows_copied)
                batches += 1
                last_source_ctid = str(inserted.last_source_ctid)
                await connection.commit()
            await _restore_plain_copy_constraints(
                connection,
                source_relation_oid=int(source_row.relation_oid),
                control=control,
            )
            await connection.execute(
                text(
                    f'CREATE INDEX '
                    f'{replacement_identifier(f"archive_copy_control_id_{control_token}_{ordinal}")} '
                    f'ON {control} (task_id)'
                )
            )
            await connection.commit()
            copy_seconds += time.perf_counter() - relation_started
            observed = (
                await connection.execute(
                    text(
                        f'SELECT count(*) AS rows, '
                        f'count(DISTINCT task_id) AS task_ids FROM {control}'
                    )
                )
            ).one()
            if (
                observed.rows != source_row.row_count
                or observed.task_ids != source_row.row_count
            ):
                raise RuntimeError(
                    'plain copy-and-hash control did not preserve the source set'
                )
    finally:
        await connection.rollback()
        for relation_name in control_relations:
            await connection.execute(
                text(
                    f'DROP TABLE IF EXISTS '
                    f'{replacement_qualified_relation(schema, relation_name)}'
                )
            )
        await connection.commit()
    return PlainCopyHashControl(
        copied_rows=copied_rows,
        payload_bytes_hashed=payload_bytes_hashed,
        batches=batches,
        copy_seconds=copy_seconds,
        copied_rows_per_second=copied_rows / copy_seconds,
        payload_bytes_per_second=payload_bytes_hashed / copy_seconds,
    )


async def _validate_plain_copy_source(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    *,
    source: str,
    expected_rows: int,
    component: ArchiveComponent,
    source_version: int,
) -> None:
    columns = archive_component_columns(component)
    observed = (
        await connection.execute(
            text(
                f"""
                SELECT count(*) AS rows,
                       count(DISTINCT task_id) AS task_ids,
                       count(*) FILTER (
                           WHERE {columns.version} = :source_version
                             AND ({columns.presence_predicate})
                             AND {schema.sql}.archive_component_value_is_valid(
                                   :component, {columns.version},
                                   {columns.codec}, {columns.content_type},
                                   {columns.payload}, {columns.digest},
                                   {columns.form}, {columns.reference}
                                 ) IS NOT TRUE
                       ) AS invalid_rows
                FROM {source}
                """
            ),
            {
                'component': component.value,
                'source_version': source_version,
            },
        )
    ).one()
    if (
        observed.rows != expected_rows
        or observed.task_ids != expected_rows
        or observed.invalid_rows != 0
    ):
        raise RuntimeError('plain copy-and-hash control source is invalid')


def _plain_copy_hash_select(
    columns: tuple[str, ...],
    *,
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    alias: str,
) -> str:
    condition = replacement_component_source_condition(
        component,
        alias=alias,
        source_version=source_version,
        source_codec=source_codec,
    )
    expressions = {
        column: f'{alias}.{replacement_identifier(column)}' for column in columns
    }
    match component:
        case ArchiveComponent.HISTORY_ROW:
            raise ValueError('history-row metadata has no payload control')
        case ArchiveComponent.RESULT:
            payload = (
                f'COALESCE({alias}.result_payload, '
                f'{alias}.prior_result_payload)'
            )
            digest_column = 'result_digest'
        case ArchiveComponent.ATTEMPTS:
            payload = f'{alias}.attempt_snapshot'
            digest_column = 'attempt_snapshot_digest'
        case ArchiveComponent.RERUN_INPUT:
            payload = f'{alias}.rerun_input_inline'
            digest_column = 'rerun_input_digest'
    expressions[digest_column] = (
        f'CASE WHEN {condition} AND {payload} IS NOT NULL '
        f'THEN sha256({payload}) '
        f'ELSE {alias}.{replacement_identifier(digest_column)} END'
    )
    return ', '.join(expressions[column] for column in columns)


async def _restore_plain_copy_constraints(
    connection: AsyncConnection,
    *,
    source_relation_oid: int,
    control: str,
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
        f'ADD CONSTRAINT {replacement_identifier(row.conname)} '
        f'{replacement_constraint_definition(row.conname, row.definition)}'
        for row in constraints
    )
    await connection.execute(text(f'ALTER TABLE {control} {actions}'))


def _verify_planned_workload(
    plan: ReplacementTranscodePlan,
    *,
    rows: int,
) -> None:
    if plan.transformed_rows != rows:
        raise RuntimeError(
            'replacement plan does not cover the complete source version: '
            f'expected {rows}, observed {plan.transformed_rows}'
        )
    if plan.copied_rows != rows:
        raise RuntimeError(
            'replacement evidence requires every seeded row to be copied: '
            f'expected {rows}, observed {plan.copied_rows}'
        )
    if plan.relation_count != 2:
        raise RuntimeError(
            'replacement evidence requires one finite and one forever relation: '
            f'observed {plan.relation_count}'
        )
