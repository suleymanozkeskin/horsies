"""Executable checks for the task-history evidence collectors."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes import identity_evidence
from tests.task_history_prototypes.evidence import EvidenceConditions, EvidenceRunKind
from tests.task_history_prototypes.identity_evidence import (
    IdentityCandidate,
    LookupCategory,
    LookupPosture,
    collect_identity_evidence,
)
from tests.task_history_prototypes.measurements import relation_footprint
from tests.task_history_prototypes.schema import PrototypeSchema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


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
