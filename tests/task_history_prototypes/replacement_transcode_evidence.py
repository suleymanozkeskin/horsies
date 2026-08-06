"""Operational evidence for replacement-partition archive transcoding."""

from __future__ import annotations

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
    ReplacementTranscodePlan,
    ReplacementVerification,
    begin_replacement_archive_maintenance,
    finalize_replacement_archive_transcode,
    finish_replacement_archive_maintenance,
    install_replacement_archive_transcode_prototype,
    plan_replacement_archive_transcode,
    replacement_decoder_retirement_status,
    replacement_storage_bytes,
    run_replacement_copy_batch,
    swap_verified_replacement_partitions,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import (
    TRANSCODE_MINIMUM_ROWS_PER_SECOND,
    ArchiveComponent,
)
from tests.task_history_prototypes.transcode_evidence import (
    seed_archive_transcode_workload,
    validate_archive_transcode_workload,
)


@dataclass(frozen=True, slots=True)
class ReplacementArchiveTranscodeEvidence:
    conditions: EvidenceConditions
    component: ArchiveComponent
    workload: dict[str, int]
    batches: int
    plan: ReplacementTranscodePlan
    baseline_relation_bytes: int
    peak_relation_bytes: int
    final_relation_bytes: int
    peak_additional_bytes: int
    copy_seconds: float
    swap_lock_seconds: float
    total_seconds: float
    copied_rows_per_second: float
    verification: ReplacementVerification
    decoder_retirement_ready: bool
    throughput_passed: bool
    duration_passed: bool
    wal_passed: bool
    peak_disk_passed: bool


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
            'explicit checkpoint before planning; replacement relations '
            'copied in committed task-ID batches'
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
        await connection.commit()

        baseline_bytes = await replacement_storage_bytes(
            connection,
            schema,
            job_id=job_id,
        )
        peak_bytes = baseline_bytes
        batches = 0
        total_started = time.perf_counter()
        copy_started = total_started
        while True:
            copied = await run_replacement_copy_batch(
                connection,
                schema,
                job_id=job_id,
                batch_size=batch_size,
            )
            await connection.commit()
            observed_bytes = await replacement_storage_bytes(
                connection,
                schema,
                job_id=job_id,
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
        copy_seconds = time.perf_counter() - copy_started

        swap_started = time.perf_counter()
        await swap_verified_replacement_partitions(
            connection,
            schema,
            job_id=job_id,
        )
        await connection.commit()
        swap_lock_seconds = time.perf_counter() - swap_started
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
        total_seconds = time.perf_counter() - total_started
        final_bytes = await replacement_storage_bytes(
            connection,
            schema,
            job_id=job_id,
        )
        retirement = await replacement_decoder_retirement_status(
            connection,
            schema,
            component=component,
            version=1,
        )
        await finish_replacement_archive_maintenance(
            connection,
            schema,
            maintenance_id=maintenance_id,
        )
        await connection.commit()

        if verification.wal_bytes is None:
            raise RuntimeError('replacement verification omitted WAL bytes')
        peak_additional = max(0, peak_bytes - baseline_bytes)
        copied_rows_per_second = planned.copied_rows / copy_seconds
        return ReplacementArchiveTranscodeEvidence(
            conditions=conditions,
            component=component,
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
            swap_lock_seconds=swap_lock_seconds,
            total_seconds=total_seconds,
            copied_rows_per_second=copied_rows_per_second,
            verification=verification,
            decoder_retirement_ready=retirement.ready,
            throughput_passed=(
                copied_rows_per_second >= TRANSCODE_MINIMUM_ROWS_PER_SECOND
            ),
            duration_passed=(
                copy_seconds <= planned.rewrite_duration_limit_seconds
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
