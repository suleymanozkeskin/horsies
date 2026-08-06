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
    replacement_transcode_evidence,
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
from tests.task_history_prototypes.replacement_transcode_evidence import (
    collect_replacement_archive_transcode_evidence,
    replacement_throughput_passed,
)
from tests.task_history_prototypes.schema import PrototypeSchema
from tests.task_history_prototypes.transcode import ArchiveComponent
from tests.task_history_prototypes.transcode_evidence import (
    collect_archive_transcode_evidence,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize(
    ('component', 'candidate', 'control', 'passed'),
    (
        (ArchiveComponent.HISTORY_ROW, 19_999.0, None, False),
        (ArchiveComponent.HISTORY_ROW, 20_000.0, None, True),
        (ArchiveComponent.RESULT, 499.0, 1_000.0, False),
        (ArchiveComponent.ATTEMPTS, 500.0, 1_000.0, True),
        (ArchiveComponent.RERUN_INPUT, 1_000.0, None, False),
    ),
)
async def test_replacement_throughput_contract_is_component_exhaustive(
    component: ArchiveComponent,
    candidate: float,
    control: float | None,
    passed: bool,
) -> None:
    assert replacement_throughput_passed(
        component=component,
        candidate_rows_per_second=candidate,
        control_rows_per_second=control,
    ) is passed


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


async def test_replacement_transcode_gate_rejects_subqualification_workload(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as connection:
        with pytest.raises(
            ValueError,
            match='requires at least 1,000,000 rows',
        ):
            await collect_replacement_archive_transcode_evidence(
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


@pytest.mark.parametrize('component', tuple(ArchiveComponent))
async def test_replacement_transcode_evidence_measures_copy_verify_and_swap(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    monkeypatch: pytest.MonkeyPatch,
    component: ArchiveComponent,
) -> None:
    monkeypatch.setattr(
        replacement_transcode_evidence,
        'collect_operational_conditions',
        _test_operational_conditions,
    )
    async with engine.connect() as connection:
        evidence = await collect_replacement_archive_transcode_evidence(
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

    assert evidence.plan.transformed_rows == 100
    assert evidence.plan.copied_rows == 100
    assert evidence.plan.relation_count == 2
    assert evidence.workload == {
        'rows': 100,
        'batch_size': 17,
        'payload_bytes': 200,
        'attempts_per_task': 4,
    }
    assert evidence.batches == 6
    assert evidence.verification.verified is True
    assert evidence.verification.source_rows_remaining_after_swap == 0
    assert evidence.decoder_retirement_ready is True
    assert evidence.peak_additional_bytes >= 0
    assert evidence.copy_storage_probe_seconds >= 0
    assert evidence.swap_lock_seconds >= 0
    assert evidence.maintenance_seconds >= evidence.swap_lock_seconds
    assert evidence.maintenance_duration_passed is True
    assert evidence.swap_window_passed is True
    assert evidence.budgets.metadata_tasks_per_second_minimum == 20_000
    assert evidence.budgets.payload_control_ratio_minimum == 0.5
    assert evidence.budgets.maintenance_seconds_maximum == 600
    assert evidence.budgets.swap_lock_seconds_maximum == 2.0
    if component is ArchiveComponent.HISTORY_ROW:
        assert evidence.control is None
        assert evidence.candidate_control_ratio is None
    else:
        assert evidence.control is not None
        assert evidence.control.copied_rows == 100
        assert evidence.control.payload_bytes_hashed == evidence.plan.payload_bytes
        assert evidence.control.batches == 6
        assert evidence.control.copy_seconds > 0
        assert evidence.control.copied_rows_per_second > 0
        assert evidence.control.payload_bytes_per_second > 0
        assert evidence.candidate_control_ratio is not None
        assert evidence.candidate_control_ratio > 0
