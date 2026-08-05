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
                           AS effective_cache_size
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
    }
    violations = {
        name: (expected, settings[name])
        for name, expected in _MICRO_SETTINGS.items()
        if settings[name] != expected
    }
    if violations:
        detail = ', '.join(
            f'{name} expected {expected}, got {observed}'
            for name, (expected, observed) in sorted(violations.items())
        )
        raise RuntimeError(f'micro evidence conditions do not match: {detail}')
    if not demo_quiesced:
        raise RuntimeError('micro evidence requires an explicitly quiesced host')
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
        durability_mode='paired-micro',
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
    seed: int,
) -> AttemptStorageEvidence:
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
        result_payload = _json_payload(result_bytes, payload_shape, seed=seed)
        result = StoredArchiveValue(
            version=ARCHIVE_VERSION,
            codec=ARCHIVE_CODEC,
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
        )
        return AttemptStorageEvidence(
            conditions=conditions,
            workload={
                'rows': rows,
                'result_bytes': result_bytes,
                'attempts_per_task': attempts_per_task,
                'attempt_snapshot_bytes': len(attempts.payload),
                'payload_shape': payload_shape.value,
                'seed': seed,
            },
            aggregate=aggregate,
            copartitioned=copartitioned,
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
