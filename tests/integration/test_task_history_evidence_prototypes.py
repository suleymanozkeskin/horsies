"""Executable checks for the task-history evidence collectors."""

from __future__ import annotations

from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes import (
    identity_evidence,
    recovery_evidence,
    transcode_evidence,
)
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    PayloadShape,
    collect_attempt_storage_evidence,
    collect_operational_conditions,
)
from tests.task_history_prototypes.identity_evidence import (
    IdentityCandidate,
    LookupCategory,
    LookupPosture,
    collect_identity_evidence,
)
from tests.task_history_prototypes.measurements import relation_footprint
from tests.task_history_prototypes.recovery_evidence import (
    collect_pending_locator_evidence,
)
from tests.task_history_prototypes.schema import PrototypeSchema
from tests.task_history_prototypes.transcode import ArchiveComponent
from tests.task_history_prototypes.transcode_evidence import (
    collect_archive_transcode_evidence,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_operational_evidence_rejects_micro_durability(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
) -> None:
    async with engine.connect() as connection:
        await connection.execute(text('SET synchronous_commit TO off'))
        with pytest.raises(RuntimeError, match='operational evidence conditions'):
            await collect_operational_conditions(
                connection,
                commit='test-head',
                run_kind=EvidenceRunKind.SMOKE,
                server_image='test-image',
                host_description='test host',
                storage_description='test storage',
                demo_quiesced=True,
                cache_posture='test cache',
                prepared_posture='test prepared posture',
            )


async def test_transcode_gate_rejects_subqualification_workload(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as connection:
        with pytest.raises(
            ValueError,
            match='requires at least 1,000,000 rows',
        ):
            await collect_archive_transcode_evidence(
                connection,
                commit='test-head',
                run_kind=EvidenceRunKind.GATE,
                server_image='test-image',
                host_description='test host',
                storage_description='test storage',
                demo_quiesced=True,
                component=ArchiveComponent.RESULT,
                rows=999_999,
                batch_size=10_000,
                payload_bytes=200,
                attempts_per_task=4,
            )


async def test_attempt_gate_rejects_subqualification_sample_count(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as connection:
        with pytest.raises(
            ValueError,
            match='requires 10,000 observations',
        ):
            await collect_attempt_storage_evidence(
                connection,
                commit='test-head',
                run_kind=EvidenceRunKind.GATE,
                server_image='test-image',
                host_description='test host',
                storage_description='test storage',
                demo_quiesced=True,
                rows=1_000_000,
                result_bytes=200,
                attempts_per_task=4,
                payload_shape=PayloadShape.COMPRESSIBLE,
                detail_observations=9_999,
                bootstrap_resamples=1_000,
                seed=20260805,
            )


async def _test_conditions(
    *_args: object,
    **_kwargs: object,
) -> EvidenceConditions:
    return EvidenceConditions(
        commit='test-head',
        run_kind=EvidenceRunKind.SMOKE,
        server_image='test-image',
        postgres_version='test-version',
        postgres_major=16,
        settings={
            'autovacuum': 'off',
            'fsync': 'off',
            'full_page_writes': 'off',
            'synchronous_commit': 'off',
        },
        host_system='test-system',
        host_machine='test-machine',
        host_cpu_count=1,
        host_description='test host',
        storage_description='test storage',
        demo_quiesced=True,
        durability_mode='paired-micro',
        cache_posture='test cache',
        prepared_posture='test prepared posture',
    )


async def _test_operational_conditions(
    *_args: object,
    **_kwargs: object,
) -> EvidenceConditions:
    return replace(
        await _test_conditions(),
        settings={
            'autovacuum': 'on',
            'fsync': 'on',
            'full_page_writes': 'on',
            'synchronous_commit': 'on',
        },
        durability_mode='operational',
    )


async def _small_ballast(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> int:
    await connection.execute(
        text(
            f"""
            CREATE UNLOGGED TABLE {schema.sql}.lookup_ballast AS
            SELECT series::bigint,
                   md5('a-' || series::text) AS a,
                   md5('b-' || series::text) AS b,
                   md5('c-' || series::text) AS c,
                   md5('d-' || series::text) AS d
            FROM generate_series(1, 10) AS series
            """
        )
    )
    await connection.commit()
    footprint = await relation_footprint(
        connection,
        f'{schema.name}.lookup_ballast',
    )
    return footprint.heap_bytes


async def test_identity_evidence_collector_exercises_every_shape_and_posture(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(identity_evidence, 'collect_conditions', _test_conditions)
    monkeypatch.setattr(identity_evidence, '_install_ballast', _small_ballast)
    async with engine.connect() as connection:
        evidence = await collect_identity_evidence(
            connection,
            commit='test-head',
            run_kind=EvidenceRunKind.SMOKE,
            server_image='test-image',
            host_description='test host',
            storage_description='test storage',
            demo_quiesced=True,
            live_rows=50,
            finite_history_rows=100,
            forever_history_rows=50,
            attached_finite_leaves=8,
            keyed_percent=10,
            warm_observations_per_category=2,
            cold_observations_per_category=1,
            bootstrap_resamples=20,
            seed=20260805,
        )

    assert tuple(candidate.candidate for candidate in evidence.candidates) == tuple(
        IdentityCandidate
    )
    expected_pairs = {
        (category, posture)
        for category in LookupCategory
        for posture in LookupPosture
    }
    for candidate in evidence.candidates:
        assert {(result.category, result.posture) for result in candidate.lookup} == (
            expected_pairs
        )
        assert all(result.observations > 0 for result in candidate.lookup)
        assert candidate.live_footprint.total_bytes > 0
        assert candidate.history_footprint.total_bytes > 0
    assert evidence.conditions.demo_quiesced is True


async def test_pending_locator_evidence_compares_wide_and_compact_shapes(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(recovery_evidence, 'collect_conditions', _test_conditions)
    async with engine.connect() as connection:
        evidence = await collect_pending_locator_evidence(
            connection,
            commit='test-head',
            run_kind=EvidenceRunKind.SMOKE,
            server_image='test-image',
            host_description='test host',
            storage_description='test storage',
            demo_quiesced=True,
        )

    assert evidence.wide_locator_bytes > evidence.byte_budget
    assert evidence.compact_history_locator_bytes <= evidence.byte_budget
    assert evidence.compact_quarantine_locator_bytes <= evidence.byte_budget
    assert evidence.compact_candidate_passed is True


async def test_attempt_evidence_applies_logical_payload_budget(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'tests.task_history_prototypes.evidence.collect_conditions',
        _test_conditions,
    )
    async with engine.connect() as connection:
        evidence = await collect_attempt_storage_evidence(
            connection,
            commit='test-head',
            run_kind=EvidenceRunKind.SMOKE,
            server_image='test-image',
            host_description='test host',
            storage_description='test storage',
            demo_quiesced=True,
            rows=100,
            result_bytes=200,
            attempts_per_task=21,
            payload_shape=PayloadShape.COMPRESSIBLE,
            detail_observations=10,
            bootstrap_resamples=20,
            seed=20260805,
        )

    assert evidence.aggregate_logical_payload_ratio <= 1.2
    assert evidence.aggregate_logical_payload_passed is True


@pytest.mark.parametrize('component', tuple(ArchiveComponent))
async def test_transcode_evidence_measures_finite_and_forever_rewrite(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    monkeypatch: pytest.MonkeyPatch,
    component: ArchiveComponent,
) -> None:
    monkeypatch.setattr(
        transcode_evidence,
        'collect_operational_conditions',
        _test_operational_conditions,
    )
    async with engine.connect() as connection:
        evidence = await collect_archive_transcode_evidence(
            connection,
            commit='test-head',
            run_kind=EvidenceRunKind.SMOKE,
            server_image='test-image',
            host_description='test host',
            storage_description='test storage',
            demo_quiesced=True,
            component=component,
            rows=100,
            batch_size=17,
            payload_bytes=200,
            attempts_per_task=4,
        )

    assert evidence.plan.affected_rows == 100
    assert evidence.plan.relation_count == 2
    assert evidence.workload == {
        'rows': 100,
        'batch_size': 17,
        'payload_bytes': 200,
        'attempts_per_task': 4,
    }
    assert evidence.batches == 6
    assert evidence.verification.verified is True
    assert evidence.verification.source_rows_remaining == 0
    assert evidence.verification.invalid_target_rows == 0
    assert evidence.decoder_retirement_ready is True
    assert evidence.peak_additional_bytes >= 0
