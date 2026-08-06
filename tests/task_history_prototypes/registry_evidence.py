"""Paired-micro qualification for the idempotency-only key registry."""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tests.perf.statistics import percentile_ms
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_conditions,
)
from tests.task_history_prototypes.identity_schema import (
    install_identity_candidates,
)
from tests.task_history_prototypes.measurements import (
    RelationFootprint,
    relation_footprint,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)


_FINGERPRINT = hashlib.sha256(b'prototype-keyed-command').digest()


@dataclass(frozen=True, slots=True)
class KeyedEnqueueMeasurement:
    keyed_percent: int
    operations: int
    producers: int
    seconds: float
    operations_per_second: float
    p99_ms: float
    applied: int
    reservation_rows: int


@dataclass(frozen=True, slots=True)
class SameKeyMeasurement:
    producers: int
    seconds: float
    p99_ms: float
    applied: int
    replays: int
    conflicts: int
    committed_task_ids: int


@dataclass(frozen=True, slots=True)
class KeyRegistryEvidence:
    conditions: EvidenceConditions
    workload: dict[str, int]
    keyed_workloads: tuple[KeyedEnqueueMeasurement, ...]
    same_fingerprint: SameKeyMeasurement
    different_fingerprint: SameKeyMeasurement
    registry_footprint: RelationFootprint
    different_key_throughput_ratio: float
    different_key_throughput_passed: bool
    same_key_p99_passed: bool
    exact_outcomes_passed: bool


async def collect_key_registry_evidence(
    engine: AsyncEngine,
    *,
    dsn: str,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    operations: int,
    producers: int,
    seed: int,
) -> KeyRegistryEvidence:
    _validate_workload(
        run_kind=run_kind,
        operations=operations,
        producers=producers,
    )
    schema = PrototypeSchema(f'key_registry_evidence_{uuid4().hex[:10]}')
    async with engine.connect() as connection:
        conditions = await collect_conditions(
            connection,
            commit=commit,
            run_kind=run_kind,
            server_image=server_image,
            host_description=host_description,
            storage_description=storage_description,
            demo_quiesced=demo_quiesced,
            cache_posture=(
                'candidate-local schema; each workload follows committed '
                'cleanup and one untimed enqueue warmup'
            ),
            prepared_posture=(
                'one database function call and one transaction per request; '
                'bounded asynchronous producer pool'
            ),
        )
        await install_archive_candidates(connection, schema)
        await install_identity_candidates(connection, schema)
        await connection.commit()

    concurrent_engine = create_async_engine(
        dsn,
        pool_size=producers,
        max_overflow=0,
    )
    try:
        await _warm_connection_pool(concurrent_engine, producers=producers)
        await _warm_candidate(
            concurrent_engine,
            engine,
            schema,
            producers=producers,
        )
        measurements: list[KeyedEnqueueMeasurement] = []
        for keyed_percent in (0, 1, 10, 100):
            await _truncate_candidate(engine, schema)
            measurement = await _measure_distinct_keys(
                concurrent_engine,
                engine,
                schema,
                keyed_percent=keyed_percent,
                operations=operations,
                producers=producers,
                seed=seed + keyed_percent,
            )
            measurements.append(measurement)

        async with engine.connect() as connection:
            footprint = await relation_footprint(
                connection,
                f'{schema.name}.key_reservations',
            )
        await _truncate_candidate(engine, schema)
        same_fingerprint = await _measure_same_key(
            concurrent_engine,
            schema,
            producers=producers,
            mixed_fingerprints=False,
        )
        await _truncate_candidate(engine, schema)
        different_fingerprint = await _measure_same_key(
            concurrent_engine,
            schema,
            producers=producers,
            mixed_fingerprints=True,
        )
        ordinary = next(
            item for item in measurements if item.keyed_percent == 0
        )
        fully_keyed = next(
            item for item in measurements if item.keyed_percent == 100
        )
        ratio = (
            fully_keyed.operations_per_second / ordinary.operations_per_second
        )
        exact_outcomes = (
            same_fingerprint.applied == 1
            and same_fingerprint.replays == producers - 1
            and same_fingerprint.conflicts == 0
            and same_fingerprint.committed_task_ids == 1
            and different_fingerprint.applied == 1
            and different_fingerprint.replays
            + different_fingerprint.conflicts
            == producers - 1
            and different_fingerprint.conflicts > 0
            and different_fingerprint.committed_task_ids == 1
        )
        return KeyRegistryEvidence(
            conditions=conditions,
            workload={
                'operations_per_ratio': operations,
                'producers': producers,
                'seed': seed,
            },
            keyed_workloads=tuple(measurements),
            same_fingerprint=same_fingerprint,
            different_fingerprint=different_fingerprint,
            registry_footprint=footprint,
            different_key_throughput_ratio=ratio,
            different_key_throughput_passed=ratio >= 0.7,
            same_key_p99_passed=(
                same_fingerprint.p99_ms <= 1_000
                and different_fingerprint.p99_ms <= 1_000
            ),
            exact_outcomes_passed=exact_outcomes,
        )
    finally:
        await concurrent_engine.dispose()
        async with engine.connect() as connection:
            await connection.rollback()
            await remove_archive_candidates(connection, schema)
            await connection.commit()


async def _measure_distinct_keys(
    concurrent_engine: AsyncEngine,
    inspection_engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    keyed_percent: int,
    operations: int,
    producers: int,
    seed: int,
) -> KeyedEnqueueMeasurement:
    generator = random.Random(seed)
    key_flags = [index < operations * keyed_percent // 100 for index in range(operations)]
    generator.shuffle(key_flags)
    semaphore = asyncio.Semaphore(producers)

    async def apply(index: int, keyed: bool) -> tuple[str, float]:
        async with semaphore:
            started = time.perf_counter()
            outcome, _ = await _enqueue_one(
                concurrent_engine,
                schema,
                task_id=str(uuid4()),
                key_digest=(
                    hashlib.sha256(f'key-{keyed_percent}-{index}'.encode()).digest()
                    if keyed
                    else None
                ),
                fingerprint=_FINGERPRINT,
            )
            return outcome, (time.perf_counter() - started) * 1_000

    started = time.perf_counter()
    results = await asyncio.gather(
        *(apply(index, keyed) for index, keyed in enumerate(key_flags))
    )
    seconds = time.perf_counter() - started
    async with inspection_engine.connect() as connection:
        reservation_rows = int(
            (
                await connection.execute(
                    text(f'SELECT count(*) FROM {schema.sql}.key_reservations')
                )
            ).scalar_one()
        )
    return KeyedEnqueueMeasurement(
        keyed_percent=keyed_percent,
        operations=operations,
        producers=producers,
        seconds=seconds,
        operations_per_second=operations / seconds,
        p99_ms=percentile_ms([latency for _, latency in results], 99),
        applied=sum(outcome == 'APPLIED' for outcome, _ in results),
        reservation_rows=reservation_rows,
    )


async def _measure_same_key(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    producers: int,
    mixed_fingerprints: bool,
) -> SameKeyMeasurement:
    key_digest = hashlib.sha256(
        b'mixed-key' if mixed_fingerprints else b'same-key'
    ).digest()

    async def apply(index: int) -> tuple[str, str, float]:
        fingerprint = (
            hashlib.sha256(f'fingerprint-{index % 2}'.encode()).digest()
            if mixed_fingerprints
            else _FINGERPRINT
        )
        started = time.perf_counter()
        outcome, task_id = await _enqueue_one(
            engine,
            schema,
            task_id=str(uuid4()),
            key_digest=key_digest,
            fingerprint=fingerprint,
        )
        return outcome, task_id, (time.perf_counter() - started) * 1_000

    started = time.perf_counter()
    results = await asyncio.gather(*(apply(index) for index in range(producers)))
    seconds = time.perf_counter() - started
    return SameKeyMeasurement(
        producers=producers,
        seconds=seconds,
        p99_ms=percentile_ms([latency for _, _, latency in results], 99),
        applied=sum(outcome == 'APPLIED' for outcome, _, _ in results),
        replays=sum(outcome == 'REPLAY' for outcome, _, _ in results),
        conflicts=sum(outcome == 'CONFLICT' for outcome, _, _ in results),
        committed_task_ids=len({task_id for _, task_id, _ in results}),
    )


async def _enqueue_one(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    task_id: str,
    key_digest: bytes | None,
    fingerprint: bytes,
) -> tuple[str, str]:
    async with engine.begin() as connection:
        row = (
            await connection.execute(
                text(
                    f"""
                    SELECT (applied.outcome).outcome::text AS outcome,
                           (applied.outcome).task_id AS task_id
                    FROM (
                        SELECT {schema.sql}.enqueue_key_registry(
                            CAST(:task_id AS varchar(36)),
                            CAST('prototype.task' AS text),
                            CAST(:key_digest AS bytea),
                            CAST(:key_scope_version AS smallint),
                            CAST(:key_window AS interval),
                            CAST(1 AS smallint),
                            CAST(:fingerprint AS bytea),
                            CAST('finite_30d_v1' AS text)
                        ) AS outcome
                    ) AS applied
                    """
                ),
                {
                    'task_id': task_id,
                    'key_digest': key_digest,
                    'key_scope_version': 1 if key_digest is not None else None,
                    'key_window': '24 hours' if key_digest is not None else None,
                    'fingerprint': fingerprint,
                },
            )
        ).one()
    return str(row.outcome), str(row.task_id)


async def _truncate_candidate(
    engine: AsyncEngine,
    schema: PrototypeSchema,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f'TRUNCATE {schema.sql}.key_registry_live, '
                f'{schema.sql}.key_registry_history, '
                f'{schema.sql}.key_reservations CASCADE'
            )
        )


async def _warm_connection_pool(engine: AsyncEngine, *, producers: int) -> None:
    async def probe() -> None:
        async with engine.connect() as connection:
            await connection.execute(text('SELECT 1'))

    await asyncio.gather(*(probe() for _ in range(producers)))


async def _warm_candidate(
    concurrent_engine: AsyncEngine,
    inspection_engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    producers: int,
) -> None:
    await asyncio.gather(
        *(
            _enqueue_one(
                concurrent_engine,
                schema,
                task_id=str(uuid4()),
                key_digest=(
                    hashlib.sha256(f'warm-key-{index}'.encode()).digest()
                    if index % 2
                    else None
                ),
                fingerprint=_FINGERPRINT,
            )
            for index in range(producers)
        )
    )
    await _truncate_candidate(inspection_engine, schema)


def _validate_workload(
    *,
    run_kind: EvidenceRunKind,
    operations: int,
    producers: int,
) -> None:
    if operations <= 0 or producers <= 0:
        raise ValueError('registry workload sizes must be positive')
    if run_kind is EvidenceRunKind.GATE and (
        operations < 4_096 or producers != 64
    ):
        raise ValueError(
            'registry gate evidence requires at least 4,096 operations per '
            'key ratio and exactly 64 producers'
        )
