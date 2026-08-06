"""Qualification sampling, checkpoint, progress, and runner contracts."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.task_history_prototypes import identity_evidence
from tests.task_history_prototypes.evidence import EvidenceConditions, EvidenceRunKind
from tests.task_history_prototypes.identity_evidence import (
    AbsoluteVerdict,
    IdentityCandidate,
    IdentityCheckpointStatus,
    LookupCategory,
    LookupLatency,
    LookupPosture,
)
from tests.task_history_prototypes.qualification_io import (
    AtomicEvidenceWriter,
    QualificationProgress,
    QualificationProgressReporter,
    bounded_observation_milestones,
)
from tests.task_history_prototypes.schema import PrototypeSchema


def _conditions() -> EvidenceConditions:
    return EvidenceConditions(
        commit='test-head',
        run_kind=EvidenceRunKind.GATE,
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
        host_description='test-host',
        storage_description='test-storage',
        demo_quiesced=True,
        durability_mode='paired-micro',
        cache_posture='test-cache',
        prepared_posture='test-prepared',
    )


def _lookup_result(
    observations: int,
    verdict: AbsoluteVerdict,
) -> LookupLatency:
    return LookupLatency(
        category=LookupCategory.LIVE,
        posture=LookupPosture.BUFFER_COLD,
        observations=observations,
        p50_ms=1.0,
        p95_ms=1.0,
        p99_ms=1.0,
        p99_ci_low_ms=0.5,
        p99_ci_high_ms=1.5,
        maximum_ms=2.0,
        budget_ms=25.0,
        verdict=verdict,
    )


@pytest.mark.parametrize('maximum', (9_999, 10_001))
def test_gate_cold_ladder_rejects_any_other_declared_maximum(maximum: int) -> None:
    with pytest.raises(ValueError, match='500/2,000/10,000'):
        identity_evidence._cold_observation_ladder(
            EvidenceRunKind.GATE,
            maximum=maximum,
        )


def test_gate_and_smoke_cold_ladders_are_explicit() -> None:
    assert identity_evidence._cold_observation_ladder(
        EvidenceRunKind.GATE,
        maximum=10_000,
    ) == (500, 2_000, 10_000)
    assert identity_evidence._cold_observation_ladder(
        EvidenceRunKind.SMOKE,
        maximum=50,
    ) == (50,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('verdicts', 'expected_ranges', 'expected_observations', 'expected_verdict'),
    (
        (
            (AbsoluteVerdict.PASS,),
            ((0, 2),),
            2,
            AbsoluteVerdict.PASS,
        ),
        (
            (AbsoluteVerdict.FAIL,),
            ((0, 2),),
            2,
            AbsoluteVerdict.FAIL,
        ),
        (
            (AbsoluteVerdict.INCONCLUSIVE, AbsoluteVerdict.PASS),
            ((0, 2), (2, 4)),
            4,
            AbsoluteVerdict.PASS,
        ),
        (
            (
                AbsoluteVerdict.INCONCLUSIVE,
                AbsoluteVerdict.INCONCLUSIVE,
                AbsoluteVerdict.INCONCLUSIVE,
            ),
            ((0, 2), (2, 4), (4, 6)),
            6,
            AbsoluteVerdict.INCONCLUSIVE,
        ),
    ),
)
async def test_cold_ladder_only_escalates_inconclusive_cells(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verdicts: tuple[AbsoluteVerdict, ...],
    expected_ranges: tuple[tuple[int, int], ...],
    expected_observations: int,
    expected_verdict: AbsoluteVerdict,
) -> None:
    checkpoint = tmp_path / 'identity.partial.json'
    observer = identity_evidence._IdentityRunObserver(
        conditions=_conditions(),
        workload={'cold_observation_ladder': (2, 4, 6)},
        writer=AtomicEvidenceWriter(checkpoint),
        progress=QualificationProgressReporter(),
    )
    ranges: list[tuple[int, int]] = []
    verdict_iterator: Iterator[AbsoluteVerdict] = iter(verdicts)

    async def fake_cold_timings(
        _connection: AsyncConnection,
        _schema: PrototypeSchema,
        _candidate: IdentityCandidate,
        _category: LookupCategory,
        task_ids: tuple[str, ...],
        *,
        observation_offset: int,
        observation_target: int,
        ballast_bytes: int,
        observer: identity_evidence._IdentityRunObserver,
    ) -> list[float]:
        assert ballast_bytes == 1
        ranges.append((observation_offset, observation_target))
        observer.record_observations(observation_target)
        return [1.0] * len(task_ids)

    def fake_latency_result(
        _category: LookupCategory,
        _posture: LookupPosture,
        samples: list[float],
        _budget_ms: float,
        *,
        resamples: int,
        seed: int,
    ) -> LookupLatency:
        assert resamples == 1_000
        assert seed == 7
        return _lookup_result(len(samples), next(verdict_iterator))

    monkeypatch.setattr(identity_evidence, '_cold_timings', fake_cold_timings)
    monkeypatch.setattr(identity_evidence, '_latency_result', fake_latency_result)
    result = await identity_evidence._measure_cold_ladder(
        cast(AsyncConnection, object()),
        PrototypeSchema('cold_ladder_test'),
        IdentityCandidate.NO_DIRECTORY,
        1,
        LookupCategory.LIVE,
        3,
        tuple(str(index) for index in range(6)),
        (2, 4, 6),
        ballast_bytes=1,
        bootstrap_resamples=1_000,
        bootstrap_seed=7,
        observer=observer,
    )

    assert ranges == list(expected_ranges)
    assert result.observations == expected_observations
    assert result.verdict is expected_verdict
    assert len(observer.finalized_cells) == 1
    partial = json.loads(checkpoint.read_text())
    assert partial['active_cell'] is None
    assert partial['finalized_cells'][0]['result']['observations'] == (
        expected_observations
    )
    assert partial['finalized_cells'][0]['result']['verdict'] == expected_verdict


def test_cell_start_and_final_verdict_are_atomically_checkpointed(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / 'identity.partial.json'
    observer = identity_evidence._IdentityRunObserver(
        conditions=_conditions(),
        workload={'cold_observation_ladder': (500, 2_000, 10_000)},
        writer=AtomicEvidenceWriter(checkpoint),
        progress=QualificationProgressReporter(),
    )
    observer.start_cell(
        candidate=IdentityCandidate.NO_DIRECTORY,
        candidate_index=1,
        category=LookupCategory.NEVER_SEEN,
        posture=LookupPosture.BUFFER_COLD,
        cell_index=18,
        rung=1,
        observations_completed=0,
        observation_target=500,
    )
    observer.record_observations(250)

    partial = json.loads(checkpoint.read_text())
    assert partial['status'] == IdentityCheckpointStatus.RUNNING
    assert partial['active_cell']['candidate'] == IdentityCandidate.NO_DIRECTORY
    assert partial['active_cell']['category'] == LookupCategory.NEVER_SEEN
    assert partial['active_cell']['rung'] == 1
    assert partial['active_cell']['observations_completed'] == 0

    observer.finalize_cell(
        LookupLatency(
            category=LookupCategory.NEVER_SEEN,
            posture=LookupPosture.BUFFER_COLD,
            observations=250,
            p50_ms=1.0,
            p95_ms=1.0,
            p99_ms=1.0,
            p99_ci_low_ms=0.5,
            p99_ci_high_ms=1.5,
            maximum_ms=2.0,
            budget_ms=150.0,
            verdict=AbsoluteVerdict.PASS,
        )
    )
    finalized = json.loads(checkpoint.read_text())
    assert finalized['active_cell'] is None
    assert finalized['finalized_cells'][0]['result']['observations'] == 250
    assert finalized['finalized_cells'][0]['result']['verdict'] == 'PASS'
    assert not tuple(tmp_path.glob('.*.tmp'))


def test_progress_fd_is_separate_and_line_buffered() -> None:
    read_fd, write_fd = os.pipe()
    reporter = QualificationProgressReporter.from_fd(write_fd)
    try:
        reporter.emit(
            QualificationProgress(
                scenario='identity-lookup',
                phase='lookup-cell',
                status='running',
                candidate='no_directory',
                observations=50,
                observation_target=500,
            )
        )
        reporter.close()
        os.close(write_fd)
        output = os.read(read_fd, 4_096).decode()
    finally:
        os.close(read_fd)

    assert output == (
        'qualification scenario=identity-lookup phase=lookup-cell '
        'status=running candidate=no_directory observations=50 '
        'observation_target=500\n'
    )


def test_observation_progress_is_bounded() -> None:
    milestones = bounded_observation_milestones(10_000)
    assert len(milestones) == 10
    assert min(milestones) == 1_000
    assert max(milestones) == 10_000


def test_boundary_workflow_preserves_partial_evidence_and_progress() -> None:
    repository = Path(__file__).parents[2]
    workflow = (repository / '.github/workflows/performance.yml').read_text()
    boundary_job = workflow.split('  identity-lookup-boundary:', maxsplit=1)[1]

    assert 'timeout-minutes: 345' in boundary_job
    assert 'timeout --foreground --signal=TERM --kill-after=30s 330m' in boundary_job
    assert '--partial-evidence-path "$partial_output"' in boundary_job
    assert '--progress-fd 3' in boundary_job
    assert 'cold_observation_ladder: [500, 2000, 10000]' in boundary_job
    assert 'partial_evidence: $partial_evidence' in boundary_job


def test_transcode_workflow_preserves_typed_swap_failure_evidence() -> None:
    repository = Path(__file__).parents[2]
    workflow = (repository / '.github/workflows/performance.yml').read_text()
    transcode_job = workflow.split(
        '  replacement-archive-transcode:',
        maxsplit=1,
    )[1].split('  identity-lookup-boundary:', maxsplit=1)[0]

    assert '--partial-evidence-path "$partial_output"' in transcode_job
    assert 'failure_evidence="$(jq -c . "$partial_output")"' in transcode_job
    assert 'failure_evidence: $failure_evidence' in transcode_job
