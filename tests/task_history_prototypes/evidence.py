"""Condition capture and archive-shape evidence for disposable prototypes."""

from __future__ import annotations

import json
import os
import platform
import random
from dataclasses import asdict, dataclass
from enum import Enum, StrEnum
from typing import Any, cast
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes.archive import (
    ARCHIVE_CODEC,
    ARCHIVE_CONTENT_TYPE,
    ARCHIVE_VERSION,
    StoredArchiveValue,
    archive_digest,
    encode_attempts,
    prototype_attempts,
)
from tests.task_history_prototypes.measurements import (
    AdministrativeResultCandidate,
    AdministrativeResultMeasurement,
    ArchiveCandidateMeasurement,
    RerunStorageCandidate,
    RerunStorageMeasurement,
    measure_administrative_result_candidate,
    measure_attempt_storage_candidates,
    measure_rerun_storage_candidate,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)


_MICRO_SETTINGS = {
    'autovacuum': 'off',
    'fsync': 'off',
    'full_page_writes': 'off',
    'synchronous_commit': 'off',
}
_OPERATIONAL_SETTINGS = {
    'autovacuum': 'on',
    'fsync': 'on',
    'full_page_writes': 'on',
    'synchronous_commit': 'on',
}


class PayloadShape(StrEnum):
    COMPRESSIBLE = 'compressible'
    INCOMPRESSIBLE = 'incompressible'


class EvidenceRunKind(StrEnum):
    SMOKE = 'smoke'
    GATE = 'gate'


@dataclass(frozen=True, slots=True)
class EvidenceConditions:
    commit: str
    run_kind: EvidenceRunKind
    server_image: str
    postgres_version: str
    postgres_major: int
    settings: dict[str, str]
    host_system: str
    host_machine: str
    host_cpu_count: int | None
    host_description: str
    storage_description: str
    demo_quiesced: bool
    durability_mode: str
    cache_posture: str
    prepared_posture: str


@dataclass(frozen=True, slots=True)
class AttemptStorageEvidence:
    conditions: EvidenceConditions
    workload: dict[str, int | str]
    aggregate: ArchiveCandidateMeasurement
    copartitioned: ArchiveCandidateMeasurement
    aggregate_logical_payload_ratio: float
    aggregate_logical_payload_passed: bool


@dataclass(frozen=True, slots=True)
class RerunStorageEvidence:
    conditions: EvidenceConditions
    workload: dict[str, int | str]
    measurements: tuple[RerunStorageMeasurement, ...]


@dataclass(frozen=True, slots=True)
class AdministrativeResultEvidence:
    conditions: EvidenceConditions
    workload: dict[str, int | str]
    measurements: tuple[AdministrativeResultMeasurement, ...]


type ArchiveStorageEvidence = (
    AttemptStorageEvidence | RerunStorageEvidence | AdministrativeResultEvidence
)


async def collect_conditions(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    cache_posture: str,
    prepared_posture: str,
) -> EvidenceConditions:
    return await _collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture=cache_posture,
        prepared_posture=prepared_posture,
        expected_settings=_MICRO_SETTINGS,
        durability_mode='paired-micro',
    )


async def collect_operational_conditions(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    cache_posture: str,
    prepared_posture: str,
) -> EvidenceConditions:
    return await _collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture=cache_posture,
        prepared_posture=prepared_posture,
        expected_settings=_OPERATIONAL_SETTINGS,
        durability_mode='operational',
    )


async def refresh_cumulative_statistics(
    connection: AsyncConnection,
) -> None:
    """Publish this backend's pending counters and discard cached snapshots."""
    force_flush_available = bool(
        (
            await connection.execute(
                text(
                    "SELECT to_regprocedure("
                    "'pg_catalog.pg_stat_force_next_flush()'"
                    ") IS NOT NULL"
                )
            )
        ).scalar_one()
    )
    if force_flush_available:
        await connection.execute(
            text('SELECT pg_catalog.pg_stat_force_next_flush()')
        )
    else:
        # PostgreSQL 14 reports per-backend counters to its collector before
        # going idle, at most once per 500 ms. This command spans that interval;
        # the following command boundary publishes the pending counters.
        await connection.execute(text('SELECT pg_sleep(0.6)'))
    await connection.execute(text('SELECT pg_stat_clear_snapshot()'))


async def _collect_conditions(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    cache_posture: str,
    prepared_posture: str,
    expected_settings: dict[str, str],
    durability_mode: str,
) -> EvidenceConditions:
    for label, value in (
        ('commit', commit),
        ('server image', server_image),
        ('host description', host_description),
        ('storage description', storage_description),
        ('cache posture', cache_posture),
        ('prepared posture', prepared_posture),
    ):
        if not value.strip():
            raise ValueError(f'{label} must be non-empty')
    row = (
        await connection.execute(
            text(
                """
                SELECT current_setting('server_version') AS postgres_version,
                       current_setting('server_version_num')::integer
                           AS version_number,
                       current_setting('autovacuum') AS autovacuum,
                       current_setting('fsync') AS fsync,
                       current_setting('full_page_writes') AS full_page_writes,
                       current_setting('synchronous_commit')
                           AS synchronous_commit,
                       current_setting('shared_buffers') AS shared_buffers,
                       current_setting('effective_cache_size')
                           AS effective_cache_size,
                       current_setting('autovacuum_vacuum_threshold')
                           AS autovacuum_vacuum_threshold,
                       current_setting('autovacuum_vacuum_scale_factor')
                           AS autovacuum_vacuum_scale_factor,
                       current_setting('autovacuum_analyze_threshold')
                           AS autovacuum_analyze_threshold,
                       current_setting('autovacuum_analyze_scale_factor')
                           AS autovacuum_analyze_scale_factor,
                       current_setting('autovacuum_naptime')
                           AS autovacuum_naptime,
                       current_setting('autovacuum_max_workers')
                           AS autovacuum_max_workers,
                       current_setting('checkpoint_timeout')
                           AS checkpoint_timeout,
                       current_setting('max_wal_size') AS max_wal_size
                """
            )
        )
    ).one()
    settings = {
        'autovacuum': row.autovacuum,
        'fsync': row.fsync,
        'full_page_writes': row.full_page_writes,
        'synchronous_commit': row.synchronous_commit,
        'shared_buffers': row.shared_buffers,
        'effective_cache_size': row.effective_cache_size,
        'autovacuum_vacuum_threshold': row.autovacuum_vacuum_threshold,
        'autovacuum_vacuum_scale_factor': row.autovacuum_vacuum_scale_factor,
        'autovacuum_analyze_threshold': row.autovacuum_analyze_threshold,
        'autovacuum_analyze_scale_factor': row.autovacuum_analyze_scale_factor,
        'autovacuum_naptime': row.autovacuum_naptime,
        'autovacuum_max_workers': row.autovacuum_max_workers,
        'checkpoint_timeout': row.checkpoint_timeout,
        'max_wal_size': row.max_wal_size,
    }
    violations = {
        name: (expected, settings[name])
        for name, expected in expected_settings.items()
        if settings[name] != expected
    }
    if violations:
        detail = ', '.join(
            f'{name} expected {expected}, got {observed}'
            for name, (expected, observed) in sorted(violations.items())
        )
        raise RuntimeError(
            f'{durability_mode} evidence conditions do not match: {detail}'
        )
    if not demo_quiesced:
        raise RuntimeError(
            f'{durability_mode} evidence requires an explicitly quiesced host'
        )
    return EvidenceConditions(
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        postgres_version=row.postgres_version,
        postgres_major=row.version_number // 10_000,
        settings=settings,
        host_system=platform.system(),
        host_machine=platform.machine(),
        host_cpu_count=os.cpu_count(),
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        durability_mode=durability_mode,
        cache_posture=cache_posture,
        prepared_posture=prepared_posture,
    )


async def collect_attempt_storage_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    rows: int,
    result_bytes: int,
    attempts_per_task: int,
    payload_shape: PayloadShape,
    detail_observations: int,
    bootstrap_resamples: int,
    seed: int,
) -> AttemptStorageEvidence:
    _validate_storage_evidence_rows(run_kind, rows)
    _validate_sample_authority(
        run_kind,
        observations=detail_observations,
        bootstrap_resamples=bootstrap_resamples,
    )
    conditions = await collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture='steady-state after candidate-local truncate',
        prepared_posture=(
            'one parameterized bulk insert per candidate; detail lookup '
            'prepared by ten warm-up executions'
        ),
    )
    schema = PrototypeSchema(f'history_evidence_{uuid4().hex[:12]}')
    await install_archive_candidates(connection, schema)
    await connection.commit()
    try:
        result_payload = _json_payload(result_bytes, payload_shape, seed=seed)
        result = StoredArchiveValue(
            version=ARCHIVE_VERSION,
            codec=ARCHIVE_CODEC,
            content_type=ARCHIVE_CONTENT_TYPE,
            payload=result_payload,
            digest=archive_digest(result_payload),
        )
        attempts = encode_attempts(prototype_attempts(attempts_per_task))
        aggregate, copartitioned = await measure_attempt_storage_candidates(
            connection,
            schema,
            rows=rows,
            result=result,
            attempts=attempts,
            attempts_per_task=attempts_per_task,
            detail_observations=detail_observations,
            bootstrap_resamples=bootstrap_resamples,
            seed=seed,
        )
        aggregate_logical_payload_ratio = (
            aggregate.logical_attempt_bytes
            / copartitioned.logical_attempt_bytes
        )
        return AttemptStorageEvidence(
            conditions=conditions,
            workload={
                'rows': rows,
                'result_bytes': result_bytes,
                'attempts_per_task': attempts_per_task,
                'attempt_snapshot_bytes': len(attempts.payload),
                'detail_observations': detail_observations,
                'bootstrap_resamples': bootstrap_resamples,
                'payload_shape': payload_shape.value,
                'seed': seed,
            },
            aggregate=aggregate,
            copartitioned=copartitioned,
            aggregate_logical_payload_ratio=aggregate_logical_payload_ratio,
            aggregate_logical_payload_passed=(
                aggregate_logical_payload_ratio <= 1.2
            ),
        )
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()


async def collect_rerun_storage_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    rows: int,
    result_bytes: int,
    rerun_input_bytes: int,
    payload_shape: PayloadShape,
    seed: int,
) -> RerunStorageEvidence:
    _validate_storage_evidence_rows(run_kind, rows)
    conditions = await collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture='steady-state after candidate-local truncate',
        prepared_posture='one parameterized bulk insert per candidate',
    )
    schema = PrototypeSchema(f'history_evidence_{uuid4().hex[:12]}')
    await install_archive_candidates(connection, schema)
    await connection.commit()
    try:
        result = _json_payload(result_bytes, payload_shape, seed=seed)
        rerun_input = _json_payload(
            rerun_input_bytes,
            payload_shape,
            seed=seed + 1,
        )
        measurements = tuple(
            [
                await measure_rerun_storage_candidate(
                    connection,
                    schema,
                    rows=rows,
                    result=result,
                    rerun_input=rerun_input,
                    candidate=candidate,
                )
                for candidate in RerunStorageCandidate
            ]
        )
        return RerunStorageEvidence(
            conditions=conditions,
            workload={
                'rows': rows,
                'result_bytes': result_bytes,
                'rerun_input_bytes': rerun_input_bytes,
                'payload_shape': payload_shape.value,
                'seed': seed,
            },
            measurements=measurements,
        )
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()


async def collect_administrative_result_evidence(
    connection: AsyncConnection,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    rows: int,
    prior_result_bytes: int,
    payload_shape: PayloadShape,
    seed: int,
) -> AdministrativeResultEvidence:
    _validate_storage_evidence_rows(run_kind, rows)
    conditions = await collect_conditions(
        connection,
        commit=commit,
        run_kind=run_kind,
        server_image=server_image,
        host_description=host_description,
        storage_description=storage_description,
        demo_quiesced=demo_quiesced,
        cache_posture='steady-state after candidate-local truncate',
        prepared_posture='one parameterized bulk insert per candidate',
    )
    schema = PrototypeSchema(f'history_evidence_{uuid4().hex[:12]}')
    await install_archive_candidates(connection, schema)
    await connection.commit()
    try:
        prior_result = _json_payload(
            prior_result_bytes,
            payload_shape,
            seed=seed,
        )
        measurements = tuple(
            [
                await measure_administrative_result_candidate(
                    connection,
                    schema,
                    rows=rows,
                    prior_result=prior_result,
                    candidate=candidate,
                )
                for candidate in AdministrativeResultCandidate
            ]
        )
        return AdministrativeResultEvidence(
            conditions=conditions,
            workload={
                'rows': rows,
                'prior_result_bytes': prior_result_bytes,
                'payload_shape': payload_shape.value,
                'seed': seed,
            },
            measurements=measurements,
        )
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()


def evidence_json(value: object) -> str:
    return json.dumps(
        asdict(cast(Any, value)),
        indent=2,
        sort_keys=True,
        default=_json_default,
    )


def _json_payload(size: int, shape: PayloadShape, *, seed: int) -> bytes:
    if size < 8:
        raise ValueError('JSON payload size must be at least 8 bytes')
    body_size = size - 8
    match shape:
        case PayloadShape.COMPRESSIBLE:
            body = 'x' * body_size
        case PayloadShape.INCOMPRESSIBLE:
            alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
            generator = random.Random(seed)
            body = ''.join(generator.choices(alphabet, k=body_size))
    payload = f'{{"v":"{body}"}}'.encode()
    if len(payload) != size:
        raise AssertionError('payload generator did not preserve requested size')
    return payload


def _json_default(value: Any) -> str:
    match value:
        case Enum():
            return value.value
        case _:
            raise TypeError(f'cannot serialize {type(value).__name__}')


def _validate_storage_evidence_rows(
    run_kind: EvidenceRunKind,
    rows: int,
) -> None:
    if rows <= 0:
        raise ValueError('rows must be positive')
    if run_kind is EvidenceRunKind.GATE and rows < 1_000_000:
        raise ValueError('storage gate evidence requires at least 1,000,000 rows')


def _validate_sample_authority(
    run_kind: EvidenceRunKind,
    *,
    observations: int,
    bootstrap_resamples: int,
) -> None:
    if observations <= 0:
        raise ValueError('observations must be positive')
    if bootstrap_resamples <= 0:
        raise ValueError('bootstrap resamples must be positive')
    if run_kind is EvidenceRunKind.GATE and observations < 10_000:
        raise ValueError('latency gate evidence requires 10,000 observations')
    if run_kind is EvidenceRunKind.GATE and bootstrap_resamples < 1_000:
        raise ValueError('latency gate evidence requires 1,000 bootstrap resamples')
